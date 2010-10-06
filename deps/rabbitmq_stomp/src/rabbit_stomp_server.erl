%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

%% rabbit_stomp implements STOMP messaging semantics, as per protocol
%% "version 1.0", at http://stomp.codehaus.org/Protocol

-module(rabbit_stomp_server).

-export([start/1,
         listener_started/2, listener_stopped/2, start_client/1,
         start_link/0, init/1, mainloop/1]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_stomp_frame.hrl").

-record(state, {socket, session_id, channel, connection, parse_state, 
                subscriptions}).

start(Listeners) ->
    {ok, Pid} = supervisor:start_child(
               rabbit_stomp_sup,
               {rabbit_stomp_client_sup,
                {tcp_client_sup, start_link,
                 [{local, rabbit_stomp_client_sup},
                  {?MODULE, start_link,[]}]},
                transient, infinity, supervisor, [tcp_client_sup]}),
    ok = start_listeners(Listeners),
    {ok, Pid}.

start_listeners([]) ->
    ok;
start_listeners([{Host, Port} | More]) ->
    {IPAddress, Name} = rabbit_networking:check_tcp_listener_address(
                          rabbit_stomp_listener_sup,
                          Host,
                          Port),
    {ok,_} = supervisor:start_child(
               rabbit_stomp_sup,
               {Name,
                {tcp_listener_sup, start_link,
                 [IPAddress, Port,
                  [{packet, raw},
                   {reuseaddr, true}],
                  {?MODULE, listener_started, []},
                  {?MODULE, listener_stopped, []},
                  {?MODULE, start_client, []}, "STOMP Listener"]},
                transient, infinity, supervisor, [tcp_listener_sup]}),
    start_listeners(More).

listener_started(_IPAddress, _Port) ->
    ok.

listener_stopped(_IPAddress, _Port) ->
    ok.

start_client(Sock) ->
    {ok, Child} = supervisor:start_child(rabbit_stomp_client_sup, []),
    ok = gen_tcp:controlling_process(Sock, Child),
    Child ! {go, Sock},
    Child.

start_link() ->
    {ok, proc_lib:spawn_link(?MODULE, init, [self()])}.

init(_Parent) ->
    receive
        {go, Sock} ->
            ok = inet:setopts(Sock, [{active, once}]),
            process_flag(trap_exit, true),

            {ok, {PeerAddress, PeerPort}} = inet:peername(Sock),
            PeerAddressS = inet_parse:ntoa(PeerAddress),
            error_logger:info_msg("starting STOMP connection ~p from ~s:~p~n",
                                  [self(), PeerAddressS, PeerPort]),
            ParseState = rabbit_stomp_frame:initial_state(),
            try
                ?MODULE:mainloop(#state{socket        = Sock,
                                        channel       = none,
                                        parse_state   = ParseState,
                                        subscriptions = dict:new()})
            after
                error_logger:info_msg("ending STOMP connection ~p from ~s:~p~n",
                                      [self(), PeerAddressS, PeerPort])
            end
    end.

mainloop(State) ->
    receive
        {'EXIT', Pid, Reason} ->
            handle_exit(Pid, Reason, State);
        {tcp, Sock, Bytes} ->
            inet:setopts(Sock, [{active, once}]),
            process_received_bytes(Bytes, State);
        {tcp_closed, _Sock} ->
            shutdown_channel_and_connection(State),
            done;
        #'basic.consume_ok'{} ->
            %% just being notified that we got made a successful
            %% subscription.
            mainloop(State);
        {Delivery = #'basic.deliver'{}, 
         #amqp_msg{props = Props, payload = Payload}} ->
             mainloop(send_delivery(Delivery, Props, Payload, State));
        Data ->
            send_priv_error("Error", "Internal error in mainloop\n",
                            Data, State),
            done
    end.

handle_exit(_Who, normal, _State) ->
    done;
handle_exit(Who, Reason, State) ->
    send_priv_error("Error", "~p exited\n", [Who], Reason, State),
    done.

process_received_bytes([], State) ->
    ?MODULE:mainloop(State);
process_received_bytes(Bytes, State = #state{parse_state = ParseState}) ->
    case rabbit_stomp_frame:parse(Bytes, ParseState) of
        {more, ParseState1} ->
            ?MODULE:mainloop(State#state{parse_state = ParseState1});
        {ok, Frame = #stomp_frame{command = Command}, Rest} ->
            PS = rabbit_stomp_frame:initial_state(),
            case catch process_frame(Command, Frame,
                                     State#state{parse_state = PS}) of
                {'EXIT', 
                 {{server_initiated_close, ReplyCode, Explanation}, _}} ->
                    explain_amqp_death(ReplyCode, Explanation, State),
                    done;
                {'EXIT', Reason} ->
                    send_priv_error("Processing error", "Processing error\n",
                                    Reason, State),
                    done;
                {ok, NewState} ->
                    process_received_bytes(Rest, NewState);
                stop ->
                    done
            end;
        {error, Reason} ->
            send_priv_error("Invalid frame", "Could not parse frame\n",
                            Reason, State),
            done
    end.

explain_amqp_death(ReplyCode, Explanation, State) ->
    ErrorName = ?PROTOCOL:amqp_exception(ReplyCode),
    send_error(atom_to_list(ErrorName), "~s\n",
               [Explanation], State).

maybe_header(_Key, undefined) ->
    [];
maybe_header(Key, Value) when is_binary(Value) ->
    [{Key, binary_to_list(Value)}];
maybe_header(Key, Value) when is_integer(Value) ->
    [{Key, integer_to_list(Value)}];
maybe_header(_Key, _Value) ->
    [].

send_delivery(#'basic.deliver'{consumer_tag = ConsumerTag,
                            delivery_tag = DeliveryTag},
              #'P_basic'{headers          = Headers,
                         content_type     = ContentType,
                         content_encoding = ContentEncoding,
                         delivery_mode    = DeliveryMode,
                         priority         = Priority,
                         correlation_id   = CorrelationId,
                         reply_to         = ReplyTo,
                         message_id       = MessageId},
              Body, State = #state{session_id    = SessionId, 
                                   subscriptions = Subs}) ->
   {ok, Destination} = dict:find(ConsumerTag, Subs),
   send_frame(
      "MESSAGE",
      [{"destination", Destination},
       %% TODO append ContentEncoding as ContentType;
       %% charset=ContentEncoding?  The STOMP SEND handler could also
       %% parse "content-type" to split it, perhaps?
       {"message-id", SessionId ++ "_" ++ integer_to_list(DeliveryTag)}]
      ++ maybe_header("content-type", ContentType)
      ++ maybe_header("content-encoding", ContentEncoding)
      ++ case ConsumerTag of
             <<"Q_",  _/binary>> -> [];
             <<"T_", Id/binary>> -> [{"subscription", binary_to_list(Id)}]
         end
      ++ adhoc_convert_headers(case Headers of
                                   undefined -> [];
                                   _         -> Headers
                               end)
      ++ maybe_header("delivery-mode", DeliveryMode)
      ++ maybe_header("priority", Priority)
      ++ maybe_header("correlation-id", CorrelationId)
      ++ maybe_header("reply-to", ReplyTo)
      ++ maybe_header("amqp-message-id", MessageId),
      Body,
      State).

adhoc_convert_headers(Headers) ->
    lists:foldr(fun ({K, longstr, V}, Acc) ->
                        [{"X-" ++ binary_to_list(K), binary_to_list(V)} | Acc];
                    ({K, signedint, V}, Acc) ->
                        [{"X-" ++ binary_to_list(K), integer_to_list(V)} | Acc];
                    (_, Acc) ->
                        Acc
                end, [], Headers).

send_frame(Frame, State = #state{socket = Sock}) ->
    %% We ignore certain errors here, as we will be receiving an
    %% asynchronous notification of the same (or a related) fault
    %% shortly anyway. See bug 21365.
    %% io:format("Sending ~p~n", [Frame]),
    case gen_tcp:send(Sock, rabbit_stomp_frame:serialize(Frame)) of
        ok -> State;
        {error, closed} -> State;
        {error, enotconn} -> State;
        {error, Code} ->
            error_logger:error_msg("Error sending STOMP frame ~p: ~p~n",
                                   [Frame#stomp_frame.command,
                                    Code]),
            State
    end.

send_frame(Command, Headers, BodyFragments, State) ->
    send_frame(#stomp_frame{command = Command,
                            headers = Headers,
                            body_iolist = BodyFragments},
               State).

send_priv_error(Message, Detail, ServerPrivateDetail, State) ->
    error_logger:error_msg("STOMP error frame sent:~n" ++
                           "Message: ~p~n" ++
                           "Detail: ~p~n" ++
                           "Server private detail: ~p~n",
                           [Message, Detail, ServerPrivateDetail]),
    send_frame("ERROR", [{"message", Message},
                         {"content-type", "text/plain"}], Detail, State).

send_priv_error(Message, Format, Args, ServerPrivateDetail, State) ->
    send_priv_error(Message, lists:flatten(io_lib:format(Format, Args)),
                    ServerPrivateDetail, State).

send_error(Message, Detail, State) ->
    send_priv_error(Message, Detail, none, State).

send_error(Message, Format, Args, State) ->
    send_priv_error(Message, Format, Args, none, State).

shutdown_channel_and_connection(State) ->
    amqp_channel:close(State#state.channel),
    amqp_connection:close(State#state.connection),
    State#state{channel = none, connection = none}.

process_frame("CONNECT", Frame, State = #state{channel = none}) ->
    {ok, DefaultVHost} = application:get_env(rabbit, default_vhost),
    {ok, State1} = do_login(rabbit_stomp_frame:header(Frame, "login"),
                            rabbit_stomp_frame:header(Frame, "passcode"),
                            rabbit_stomp_frame:header(Frame, "virtual-host",
                                               binary_to_list(DefaultVHost)),
                            State),
     
    send_method(#'basic.qos'{prefetch_size = 0,
                             prefetch_count = 1,
                             global = false}, State1),

    {ok, State1};
process_frame("DISCONNECT", Frame, State) ->
    receipt_if_necessary(Frame, shutdown_channel_and_connection(State)),
    stop;
process_frame(_Command, _Frame, State = #state{channel = none}) ->
    {ok, send_error("Illegal command",
                    "You must log in using CONNECT first\n",
                    State)};
process_frame(Command, Frame, State) ->
    case process_command(Command, Frame, State) of
        {ok, State1} -> {ok, receipt_if_necessary(Frame, State1)};
        stop         -> stop
    end.

receipt_if_necessary(Frame, State) ->
    case rabbit_stomp_frame:header(Frame, "receipt") of
        {ok, Id}  -> send_frame("RECEIPT", [{"receipt-id", Id}], "", State);
        not_found -> State
    end.

send_method(Method, State = #state{channel = Channel}) ->
    amqp_channel:call(Channel, Method),
    State.

send_method(Method, Properties, BodyFragments, 
            State = #state{channel = Channel}) ->
    amqp_channel:call(Channel,Method, #amqp_msg{
                                props = Properties,
                                payload = lists:reverse(BodyFragments)}),
    State.

do_login({ok, Login}, {ok, Passcode}, VirtualHost, State) ->
    {ok, Connection} = amqp_connection:start(direct, #amqp_params{
					       username		= list_to_binary(Login),
					       password		= list_to_binary(Passcode),
					       virtual_host	= list_to_binary(VirtualHost)}),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    SessionId = rabbit_guid:string_guid("session"),
    {ok, send_frame("CONNECTED",
                    [{"session", SessionId}],
                    "",
                    State#state{session_id	= SessionId, 
				channel		= Channel, 
				connection	= Connection})};
do_login(_, _, _, State) ->
    {ok, send_error("Bad CONNECT", "Missing login or passcode header(s)\n",
                    State)}.

longstr_field(K, V) ->
    {list_to_binary(K), longstr, list_to_binary(V)}.

transactional(Frame) ->
    case rabbit_stomp_frame:header(Frame, "transaction") of
        {ok, Transaction} ->
            {yes, Transaction};
        not_found ->
            no
    end.

transactional_action(Frame, Name, Fun, State) ->
    case transactional(Frame) of
        {yes, Transaction} ->
            Fun(Transaction, State);
        no ->
            {ok, send_error("Missing transaction",
                            Name ++ " must include a 'transaction' header\n",
                            State)}
    end.

with_transaction(Transaction, State, Fun) ->
    case get({transaction, Transaction}) of
        undefined ->
            {ok, send_error("Bad transaction",
                            "Invalid transaction identifier: ~p\n",
                            [Transaction],
                            State)};
        Actions ->
            Fun(Actions, State)
    end.

begin_transaction(Transaction, State) ->
    put({transaction, Transaction}, []),
    {ok, State}.

extend_transaction(Transaction, Action, State0) ->
    with_transaction(
      Transaction, State0,
      fun (Actions, State) ->
              put({transaction, Transaction}, [Action | Actions]),
              {ok, State}
      end).

commit_transaction(Transaction, State0) ->
    with_transaction(
      Transaction, State0,
      fun (Actions, State) ->
              FinalState = lists:foldr(fun perform_transaction_action/2,
                                       State,
                                       Actions),
              erase({transaction, Transaction}),
              {ok, FinalState}
      end).

abort_transaction(Transaction, State0) ->
    with_transaction(
      Transaction, State0,
      fun (_Actions, State) ->
              erase({transaction, Transaction}),
              {ok, State}
      end).

perform_transaction_action({Method}, State) ->
    send_method(Method, State);
perform_transaction_action({Method, Props, BodyFragments}, State) ->
    send_method(Method, Props, BodyFragments, State).

process_command("BEGIN", Frame, State) ->
    transactional_action(Frame, "BEGIN", fun begin_transaction/2, State);
process_command("SEND",
                Frame = #stomp_frame{headers = Headers, body_iolist = BodyFragments},
                State) ->
    BinH = fun(K, V) -> rabbit_stomp_frame:binary_header(Frame, K, V) end,
    IntH = fun(K, V) -> rabbit_stomp_frame:integer_header(Frame, K, V) end,
    case rabbit_stomp_frame:header(Frame, "destination") of
        {ok, DestHeader} ->
            {ok, Destination} = 
                rabbit_stomp_destination_parser:parse_destination(DestHeader),

            {ok, _Q} = create_queue_if_needed(send, Destination, State),

            Props = #'P_basic'{
              content_type     = BinH("content-type",     <<"text/plain">>),
              content_encoding = BinH("content-encoding", undefined),
              delivery_mode    = IntH("delivery-mode",    undefined),
              priority         = IntH("priority",         undefined),
              correlation_id   = BinH("correlation-id",   undefined),
              reply_to         = BinH("reply-to",         undefined),
              message_id       = BinH("amqp-message-id",  undefined),
              headers          = [longstr_field(K, V) ||
                                     {"X-" ++ K, V} <- Headers]},

            {Exchange, RoutingKey} = parse_routing_information(Destination),

            Method = #'basic.publish'{
              exchange = list_to_binary(Exchange),
              routing_key = list_to_binary(RoutingKey),
              mandatory = false,
              immediate = false},

            case transactional(Frame) of
                {yes, Transaction} ->
                    extend_transaction(Transaction, {Method, Props, BodyFragments},
                                       State);
                no ->
                    {ok, send_method(Method, Props, BodyFragments, State)}
            end;
        not_found ->
            {ok, send_error("Missing destination",
                            "SEND must include a 'destination', "
                            "and optional 'exchange' header\n",
                            State)}
    end;
process_command("ACK", Frame, State = #state{session_id = SessionId}) ->
    case rabbit_stomp_frame:header(Frame, "message-id") of
        {ok, IdStr} ->
            IdPrefix = SessionId ++ "_",
            case string:substr(IdStr, 1, length(IdPrefix)) of
                IdPrefix ->
                    DeliveryTag = list_to_integer(
                                    string:substr(IdStr, length(IdPrefix) + 1)),
                    Method = #'basic.ack'{delivery_tag = DeliveryTag,
                                          multiple = false},
                    case transactional(Frame) of
                        {yes, Transaction} ->
                            extend_transaction(Transaction, {Method}, State);
                        no ->
                            {ok, send_method(Method, State)}
                    end;
                _ ->
                    rabbit_misc:die(command_invalid, 'basic.ack')
            end;
        not_found ->
            {ok, send_error("Missing message-id",
                            "ACK must include a 'message-id' header\n",
                            State)}
    end;

process_command("COMMIT", Frame, State) ->
    transactional_action(Frame, "COMMIT", fun commit_transaction/2, State);
process_command("ABORT", Frame, State) ->
    transactional_action(Frame, "ABORT", fun abort_transaction/2, State);
process_command("SUBSCRIBE", Frame,
                State = #state{channel = Channel, subscriptions = Subs}) ->
    AckMode = case rabbit_stomp_frame:header(Frame, "ack", "auto") of
                  "auto" -> auto;
                  "client" -> client
              end,
    case rabbit_stomp_frame:header(Frame, "destination") of
        {ok, DestHeader} ->
            {ok, Destination} = 
                rabbit_stomp_destination_parser:parse_destination(DestHeader),

            {ok, Queue} = create_queue_if_needed(subscribe, Destination, State),

            ConsumerTag = case rabbit_stomp_frame:header(Frame, "id") of
                              {ok, Str} ->
                                  list_to_binary("T_" ++ Str);
                              not_found ->
                                  list_to_binary("Q_" ++ DestHeader)
                          end,            
            
            amqp_channel:subscribe(Channel, 
                                   #'basic.consume'{
                                     queue        = Queue, 
                                     consumer_tag = ConsumerTag,
                                     no_local     = false,
                                     no_ack       = (AckMode == auto),
                                     exclusive    = false},
                                   self()),

            ok = bind_queue_if_needed(Queue, Destination, State),

            {ok, State#state{subscriptions = 
                            dict:store(ConsumerTag, DestHeader, Subs)}};
        not_found ->
            {ok, send_error("Missing destination",
                            "SUBSCRIBE must include a 'destination' header\n",
                            State)}
    end;
process_command("UNSUBSCRIBE", Frame, State = #state{subscriptions = Subs}) ->
    ConsumerTag = case rabbit_stomp_frame:header(Frame, "id") of
                      {ok, IdStr} ->
                          list_to_binary("T_" ++ IdStr);
                      not_found ->
                          case rabbit_stomp_frame:header(Frame, "destination") of
                              {ok, QueueStr} ->
                                  list_to_binary("Q_" ++ QueueStr);
                              not_found ->
                                  missing
                          end
                  end,
    if
        ConsumerTag == missing ->
            {ok, send_error("Missing destination or id",
                            "UNSUBSCRIBE must include a 'destination' "
                            "or 'id' header\n",
                            State)};
        true ->
            {ok, send_method(#'basic.cancel'{consumer_tag = ConsumerTag,
                                             nowait       = true},
                            State#state{subscriptions = 
                                            dict:erase(ConsumerTag, Subs)})}
    end;
process_command(Command, _Frame, State) ->
    {ok, send_error("Bad command",
                    "Could not interpret command " ++ Command ++ "\n",
                    State)}.

parse_routing_information({exchange, {Name, undefined}}) ->
    {Name, ""};
parse_routing_information({exchange, {Name, Pattern}}) ->
    {Name, Pattern};
parse_routing_information({queue, Name}) ->
    {"", Name};
parse_routing_information({topic, Name}) ->
    {"amq.topic", Name}.


create_queue_if_needed(subscribe, {exchange, _}, #state{channel = Channel}) ->
    %% Create anonymous queue for SUBSCRIBE on /exchange destinations
    #'queue.declare_ok'{queue = Queue} = 
        amqp_channel:call(Channel, #'queue.declare'{auto_delete = true}),
    {ok, Queue};
create_queue_if_needed(send, {exchange, _}, _State) ->
    %% Don't create queues on SEND for /exchange destinations
    {ok, undefined};
create_queue_if_needed(_, {queue, Name}, #state{channel = Channel}) ->
    %% Always create named queue for /queue destinations
    Queue = list_to_binary(Name),
    #'queue.declare_ok'{queue = Queue} = 
        amqp_channel:call(Channel, 
                          #'queue.declare'{durable = true, 
                                           queue   = Queue}),
    {ok, Queue};
create_queue_if_needed(subscribe, {topic, _}, #state{channel = Channel}) ->
    %% Create anonymous, exclusive queue for SUBSCRIBE on /topic destinations
    #'queue.declare_ok'{queue = Queue} = 
        amqp_channel:call(Channel, #'queue.declare'{exclusive = true}),
    {ok, Queue};
create_queue_if_needed(send, {topic, _}, _State) ->
    %% Don't create queues on SEND for /topic destinations
    {ok, undefined}.

bind_queue_if_needed(Queue, {exchange, {Name, Pattern}},
                     #state{channel = Channel}) ->
    RoutingKey = case Pattern of 
                     undefined -> "";
                     _         -> Pattern
                 end,
    #'queue.bind_ok'{} = 
        amqp_channel:call(Channel, 
                          #'queue.bind'{
                            queue       = Queue,
                            exchange    = list_to_binary(Name),
                            routing_key = list_to_binary(RoutingKey)}),    
    ok;
bind_queue_if_needed(_Queue, {queue, _}, _State) ->
    %% rely on default binding for /queue
    ok;
bind_queue_if_needed(Queue, {topic, Name}, #state{channel = Channel}) ->
    #'queue.bind_ok'{} = 
        amqp_channel:call(Channel, 
                          #'queue.bind'{
                            queue       = Queue,
                            exchange    = list_to_binary("amq.topic"),
                            routing_key = list_to_binary(Name)}),    
    ok.                       
    
    
