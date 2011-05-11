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
-module(rabbit_stomp_processor).
-behaviour(gen_server2).

-export([start_link/2, process_frame/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_stomp_frame.hrl").

-record(state, {socket, session_id, channel,
                connection, subscriptions, version,
                start_heartbeat_fun, pending_receipts}).

-record(subscription, {dest_hdr, channel, multi_ack, description}).

-define(SUPPORTED_VERSIONS, ["1.0", "1.1"]).
-define(DEFAULT_QUEUE_PREFETCH, 1).
-define(FLUSH_TIMEOUT, 60000).

%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------
start_link(Sock, StartHeartbeatFun) ->
    gen_server2:start_link(?MODULE, [Sock, StartHeartbeatFun], []).

process_frame(Pid, Frame = #stomp_frame{command = Command}) ->
    gen_server2:cast(Pid, {Command, Frame}).

%%----------------------------------------------------------------------------
%% Basic gen_server2 callbacks
%%----------------------------------------------------------------------------

init([Sock, StartHeartbeatFun]) ->
    process_flag(trap_exit, true),
    {ok,
     #state {
       socket              = Sock,
       session_id          = none,
       channel             = none,
       connection          = none,
       subscriptions       = dict:new(),
       version             = none,
       start_heartbeat_fun = StartHeartbeatFun,
       pending_receipts    = undefined},
     hibernate,
     {backoff, 1000, 1000, 10000}
    }.

terminate(_Reason, State) ->
    shutdown_channel_and_connection(State).

handle_cast({"STOMP", Frame}, State) ->
    handle_cast({"CONNECT", Frame}, State);

handle_cast({"CONNECT", Frame}, State = #state{channel = none,
                                               socket  = Sock}) ->
    process_request(
      fun(StateN) ->
              case negotiate_version(Frame) of
                  {ok, Version} ->
                      {ok, DefaultVHost} =
                          application:get_env(rabbit, default_vhost),
                      do_login(rabbit_stomp_frame:header(Frame, "login"),
                               rabbit_stomp_frame:header(Frame, "passcode"),
                               rabbit_stomp_frame:header(Frame, "host",
                                                         binary_to_list(
                                                           DefaultVHost)),
                               rabbit_stomp_frame:header(Frame, "heartbeat",
                                                         "0,0"),
                               adapter_info(Sock, Version),
                               Version,
                               StateN);
                  {error, no_common_version} ->
                      error("Version mismatch",
                            "Supported versions are ~s\n",
                            [string:join(?SUPPORTED_VERSIONS, ",")],
                            StateN)
              end
      end,
      fun(StateM) -> StateM end,
      State);

handle_cast(_Request, State = #state{channel = none}) ->
    {noreply,
     send_error("Illegal command",
                "You must log in using CONNECT first\n",
                State),
     hibernate};

handle_cast({Command, Frame}, State) ->
    process_request(
      fun(StateN) ->
              handle_frame(Command, Frame, StateN)
      end,
      fun(StateM) ->
              ensure_receipt(Frame, StateM)
      end,
      State);

handle_cast(client_timeout, State) ->
    {stop, client_timeout, State}.

handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State, hibernate};
handle_info(#'basic.cancel_ok'{}, State) ->
    {noreply, State, hibernate};
handle_info(#'basic.ack'{delivery_tag = Tag, multiple = IsMulti}, State) ->
    {noreply, flush_pending_receipts(Tag, IsMulti, State), hibernate};
handle_info({Delivery = #'basic.deliver'{},
             #amqp_msg{props = Props, payload = Payload}}, State) ->
    {noreply, send_delivery(Delivery, Props, Payload, State), hibernate};
handle_info({inet_reply, _, ok}, State) ->
    {noreply, State, hibernate};
handle_info({inet_reply, _, Status}, State) ->
    {stop, Status, State}.

process_request(ProcessFun, SuccessFun, State) ->
    Res = case catch ProcessFun(State) of
              {'EXIT',
               {{shutdown,
                 {server_initiated_close, ReplyCode, Explanation}}, _}} ->
                  amqp_death(ReplyCode, Explanation, State);
              {'EXIT', Reason} ->
                  priv_error("Processing error", "Processing error\n",
                              Reason, State);
              Result ->
                  Result
          end,
    case Res of
        {ok, Frame, NewState} ->
            case Frame of
                none -> ok;
                _    -> send_frame(Frame, NewState)
            end,
            {noreply, SuccessFun(NewState), hibernate};
        {error, Message, Detail, NewState} ->
            {noreply, send_error(Message, Detail, NewState), hibernate};
        {stop, normal, NewState} ->
            {stop, normal, SuccessFun(NewState)};
        {stop, R, NewState} ->
            {stop, R, NewState}
    end.

%%----------------------------------------------------------------------------
%% Frame handlers
%%----------------------------------------------------------------------------

handle_frame("DISCONNECT", _Frame, State) ->
    %% We'll get to shutdown the channels in terminate
    {stop, normal, shutdown_channel_and_connection(State)};

handle_frame("SUBSCRIBE", Frame, State) ->
    with_destination("SUBSCRIBE", Frame, State, fun do_subscribe/4);

handle_frame("UNSUBSCRIBE", Frame, State) ->
    ConsumerTag = rabbit_stomp_util:consumer_tag(Frame),
    cancel_subscription(ConsumerTag, State);

handle_frame("SEND", Frame, State) ->
    with_destination("SEND", Frame, State, fun do_send/4);

handle_frame("ACK", Frame, State) ->
    ack_action("ACK", Frame, State, fun create_ack_method/2);

handle_frame("NACK", Frame, State) ->
    ack_action("NACK", Frame, State, fun create_nack_method/2);

handle_frame("BEGIN", Frame, State) ->
    transactional_action(Frame, "BEGIN", fun begin_transaction/2, State);

handle_frame("COMMIT", Frame, State) ->
    transactional_action(Frame, "COMMIT", fun commit_transaction/2, State);

handle_frame("ABORT", Frame, State) ->
    transactional_action(Frame, "ABORT", fun abort_transaction/2, State);

handle_frame(Command, _Frame, State) ->
    error("Bad command",
          "Could not interpret command " ++ Command ++ "\n",
          State).

%%----------------------------------------------------------------------------
%% Internal helpers for processing frames callbacks
%%----------------------------------------------------------------------------

ack_action(Command, Frame,
           State = #state{subscriptions = Subs}, MethodFun) ->
    case rabbit_stomp_frame:header(Frame, "message-id") of
        {ok, IdStr} ->
            case rabbit_stomp_util:parse_message_id(IdStr) of
                {ok, {ConsumerTag, _SessionId, DeliveryTag}} ->
                    Subscription = #subscription{channel = SubChannel}
                        = dict:fetch(ConsumerTag, Subs),
                    Method = MethodFun(DeliveryTag, Subscription),
                    case transactional(Frame) of
                        {yes, Transaction} ->
                            extend_transaction(Transaction,
                                               {SubChannel, Method},
                                               State);
                        no ->
                            amqp_channel:call(SubChannel, Method),
                            ok(State)
                    end;
                _ ->
                   error("Invalid message-id",
                         "~p must include a valid 'message-id' header\n",
                         [Command],
                         State)
            end;
        not_found ->
            error("Missing message-id",
                  "~p must include a 'message-id' header\n",
                  [Command],
                  State)
    end.

%%----------------------------------------------------------------------------
%% Internal helpers for processing frames callbacks
%%----------------------------------------------------------------------------

cancel_subscription({error, _}, State) ->
    error("Missing destination or id",
          "UNSUBSCRIBE must include a 'destination' or 'id' header\n",
          State);

cancel_subscription({ok, ConsumerTag, Description},
                    State = #state{channel       = MainChannel,
                                   subscriptions = Subs}) ->
    case dict:find(ConsumerTag, Subs) of
        error ->
            error("No subscription found",
                  "UNSUBSCRIBE must refer to an existing subscription.\n"
                  "Subscription to ~p not found.\n",
                  [Description],
                  State);
        {ok, #subscription{channel = SubChannel}} ->
            case amqp_channel:call(SubChannel,
                                   #'basic.cancel'{
                                     consumer_tag = ConsumerTag}) of
                #'basic.cancel_ok'{consumer_tag = ConsumerTag} ->
                    NewSubs = dict:erase(ConsumerTag, Subs),
                    ensure_subchannel_closed(SubChannel,
                                             MainChannel,
                                             State#state{
                                               subscriptions = NewSubs});
                _ ->
                    error("Failed to cancel subscription",
                          "UNSUBSCRIBE to ~p failed.\n",
                          [Description],
                          State)
            end
    end.

ensure_subchannel_closed(SubChannel, MainChannel, State)
  when SubChannel == MainChannel ->
    ok(State);

ensure_subchannel_closed(SubChannel, _MainChannel, State) ->
    amqp_channel:close(SubChannel),
    ok(State).

with_destination(Command, Frame, State, Fun) ->
    case rabbit_stomp_frame:header(Frame, "destination") of
        {ok, DestHdr} ->
            case rabbit_stomp_util:parse_destination(DestHdr) of
                {ok, Destination} ->
                    Fun(Destination, DestHdr, Frame, State);
                {error, {invalid_destination, Type, Content}} ->
                    error("Invalid destination",
                          "'~s' is not a valid ~p destination\n",
                          [Content, Type],
                          State);
                {error, {unknown_destination, Content}} ->
                    error("Unknown destination",
                          "'~s' is not a valid destination.\n" ++
                              "Valid destination types are: " ++
                              "/exchange, /topic or /queue.\n",
                          [Content],
                          State)
            end;
        not_found ->
            error("Missing destination",
                  "~p must include a 'destination' header\n",
                  [Command],
                  State)
    end.

do_login({ok, Username0}, {ok, Password0}, VirtualHost0, Heartbeat, AdapterInfo,
         Version, State) ->
    Username = list_to_binary(Username0),
    Password = list_to_binary(Password0),
    VirtualHost = list_to_binary(VirtualHost0),
    case rabbit_access_control:check_user_pass_login(Username, Password) of
        {ok, _User} ->
            case amqp_connection:start(
                   #amqp_params_direct{username     = Username,
                                       virtual_host = VirtualHost,
                                       adapter_info = AdapterInfo}) of
                {ok, Connection} ->
                    {ok, Channel} = amqp_connection:open_channel(Connection),
                    SessionId = rabbit_guid:string_guid("session"),
                    {{SendTimeout, ReceiveTimeout}, State1} =
                        ensure_heartbeats(Heartbeat, State),
                    ok("CONNECTED",
                       [{"session", SessionId},
                        {"heartbeat", io_lib:format("~B,~B", [SendTimeout,
                                                              ReceiveTimeout])},
                        {"version", Version}],
                       "",
                       State1#state{session_id = SessionId,
                                    channel    = Channel,
                                    connection = Connection});
                {error, auth_failure} ->
                    error("Bad CONNECT", "Authentication failure\n", State);
                {error, access_refused} ->
                    error("Bad CONNECT", "Authentication failure\n", State)
            end;
        {refused, _Msg, _Args} ->
            error("Bad CONNECT", "Authentication failure\n", State)
    end;

do_login(_, _, _, _, _, _, State) ->
    error("Bad CONNECT", "Missing login or passcode header(s)\n", State).

adapter_info(Sock, Version) ->
    {ok, {Addr,     Port}}     = rabbit_net:sockname(Sock),
    {ok, {PeerAddr, PeerPort}} = rabbit_net:peername(Sock),
    #adapter_info{protocol     = {'STOMP', Version},
                  address      = Addr,
                  port         = Port,
                  peer_address = PeerAddr,
                  peer_port    = PeerPort}.

do_subscribe(Destination, DestHdr, Frame,
             State = #state{subscriptions = Subs,
                            connection    = Connection,
                            channel       = MainChannel}) ->
    Prefetch = rabbit_stomp_frame:integer_header(Frame, "prefetch-count",
                                                 default_prefetch(Destination)),

    Channel = case Prefetch of
                  undefined ->
                      MainChannel;
                  _ ->
                      {ok, Channel1} = amqp_connection:open_channel(Connection),
                      amqp_channel:call(Channel1,
                                        #'basic.qos'{prefetch_size  = 0,
                                                     prefetch_count = Prefetch,
                                                     global         = false}),
                      Channel1
              end,

    {AckMode, IsMulti} = rabbit_stomp_util:ack_mode(Frame),

    {ok, Queue} = ensure_queue(subscribe, Destination, Channel),

    {ok, ConsumerTag, Description} = rabbit_stomp_util:consumer_tag(Frame),

    amqp_channel:subscribe(Channel,
                           #'basic.consume'{
                             queue        = Queue,
                             consumer_tag = ConsumerTag,
                             no_local     = false,
                             no_ack       = (AckMode == auto),
                             exclusive    = false},
                           self()),
    ExchangeAndKey = rabbit_stomp_util:parse_routing_information(Destination),
    ok = ensure_queue_binding(Queue, ExchangeAndKey, Channel),

    ok(State#state{subscriptions =
                       dict:store(ConsumerTag,
                                  #subscription{dest_hdr    = DestHdr,
                                                channel     = Channel,
                                                multi_ack   = IsMulti,
                                                description = Description},
                                  Subs)}).

do_send(Destination, _DestHdr,
        Frame = #stomp_frame{body_iolist = BodyFragments},
        State = #state{channel = Channel}) ->
    {ok, _Q} = ensure_queue(send, Destination, Channel),

    Props = rabbit_stomp_util:message_properties(Frame),

    {Exchange, RoutingKey} =
        rabbit_stomp_util:parse_routing_information(Destination),

    Method = #'basic.publish'{
      exchange = list_to_binary(Exchange),
      routing_key = list_to_binary(RoutingKey),
      mandatory = false,
      immediate = false},

    case transactional(Frame) of
        {yes, Transaction} ->
            extend_transaction(Transaction,
                               fun(StateN) ->
                                       maybe_record_receipt(Frame, StateN)
                               end,
                               {Method, Props, BodyFragments},
                               State);
        no ->
            ok(send_method(Method, Props, BodyFragments,
                           maybe_record_receipt(Frame, State)))
    end.

create_ack_method(DeliveryTag, #subscription{multi_ack = IsMulti}) ->
    #'basic.ack'{delivery_tag = DeliveryTag,
                 multiple     = IsMulti}.

create_nack_method(DeliveryTag, #subscription{multi_ack = IsMulti}) ->
    #'basic.nack'{delivery_tag = DeliveryTag,
                  multiple     = IsMulti}.

negotiate_version(Frame) ->
    ClientVers = re:split(
                   rabbit_stomp_frame:header(Frame, "accept-version", "1.0"),
                   ",",
                   [{return, list}]),
    rabbit_stomp_util:negotiate_version(ClientVers, ?SUPPORTED_VERSIONS).


send_delivery(Delivery = #'basic.deliver'{consumer_tag = ConsumerTag},
              Properties, Body,
              State = #state{session_id    = SessionId,
                             subscriptions = Subs}) ->
    case dict:find(ConsumerTag, Subs) of
        {ok, #subscription{dest_hdr = Destination}} ->
            send_frame(
              "MESSAGE",
              rabbit_stomp_util:message_headers(Destination, SessionId,
                                                Delivery, Properties),
              Body,
              State);
        error ->
            send_error("Subscription not found",
                       "There is no current subscription with tag '~s'.",
                       [ConsumerTag],
                       State)
    end.

send_method(Method, Channel, State) ->
    amqp_channel:call(Channel, Method),
    State.

send_method(Method, State = #state{channel = Channel}) ->
    send_method(Method, Channel, State).

send_method(Method, Properties, BodyFragments,
            State = #state{channel = Channel}) ->
    send_method(Method, Channel, Properties, BodyFragments, State).

send_method(Method, Channel, Properties, BodyFragments, State) ->
    amqp_channel:call(
      Channel, Method,
      #amqp_msg{props   = Properties,
                payload = list_to_binary(BodyFragments)}),
    State.

shutdown_channel_and_connection(State = #state{channel = none}) ->
    State;
shutdown_channel_and_connection(State = #state{channel       = Channel,
                                               connection    = Connection,
                                               subscriptions = Subs}) ->
    dict:fold(
      fun(_ConsumerTag, #subscription{channel = SubChannel}, Acc) ->
              case SubChannel of
                  Channel -> Acc;
                  _ ->
                      amqp_channel:close(SubChannel),
                      Acc
              end
      end, 0, Subs),

    amqp_channel:close(Channel),
    amqp_connection:close(Connection),
    State#state{channel = none, connection = none, subscriptions = none}.

default_prefetch({queue, _}) ->
    ?DEFAULT_QUEUE_PREFETCH;
default_prefetch(_) ->
    undefined.

%%----------------------------------------------------------------------------
%% Receipt Handling
%%----------------------------------------------------------------------------

ensure_receipt(Frame = #stomp_frame{command = Command}, State) ->
    case rabbit_stomp_frame:header(Frame, "receipt") of
        {ok, Id}  -> do_receipt(Command, Id, State);
        not_found -> State
    end.

do_receipt("SEND", _, State) ->
    %% SEND frame receipts are handled when messages are confirmed
    State;
do_receipt(_Frame, ReceiptId, State) ->
    send_frame("RECEIPT", [{"receipt-id", ReceiptId}], "", State).

maybe_record_receipt(Frame, State = #state{channel          = Channel,
                                           pending_receipts = PR}) ->
    case rabbit_stomp_frame:header(Frame, "receipt") of
        {ok, Id} ->
            PR1 = case PR of
                      undefined ->
                          amqp_channel:register_confirm_handler(
                            Channel, self()),
                          #'confirm.select_ok'{} =
                              amqp_channel:call(Channel, #'confirm.select'{}),
                          gb_trees:empty();
                      _ ->
                          PR
                  end,
            SeqNo = amqp_channel:next_publish_seqno(Channel),
            State#state{pending_receipts = gb_trees:insert(SeqNo, Id, PR1)};
        not_found ->
            State
    end.

flush_pending_receipts(DeliveryTag, IsMulti,
                       State = #state{pending_receipts = PR}) ->
    {Receipts, PR1} = accumulate_receipts(DeliveryTag, IsMulti, PR),
    State1 = lists:foldl(fun(ReceiptId, StateN) ->
                                 do_receipt(none, ReceiptId, StateN)
                         end, State, Receipts),
    State1#state{pending_receipts = PR1}.

accumulate_receipts(DeliveryTag, false, PR) ->
    case gb_trees:lookup(DeliveryTag, PR) of
        {value, ReceiptId} -> {[ReceiptId], gb_trees:delete(DeliveryTag, PR)};
        none               -> {[], PR}
    end;
accumulate_receipts(DeliveryTag, true, PR) ->
    {Key, Value, PR1} = gb_trees:take_smallest(PR),
    case DeliveryTag >= Key of
        true  -> accumulate_receipts1(DeliveryTag, {Key, Value, PR1}, []);
        false -> {[], PR}
    end.

accumulate_receipts1(DeliveryTag, {DeliveryTag, Value, PR}, Acc) ->
    {lists:reverse([Value | Acc]), PR};
accumulate_receipts1(DeliveryTag, {_Key, Value, PR}, Acc) ->
    accumulate_receipts1(DeliveryTag,
                         gb_trees:take_smallest(PR), [Value | Acc]).

%%----------------------------------------------------------------------------
%% Transaction Support
%%----------------------------------------------------------------------------

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
            error("Missing transaction",
                  Name ++ " must include a 'transaction' header\n",
                  State)
    end.

with_transaction(Transaction, State, Fun) ->
    case get({transaction, Transaction}) of
        undefined ->
            error("Bad transaction",
                  "Invalid transaction identifier: ~p\n",
                  [Transaction],
                  State);
        Actions ->
            Fun(Actions, State)
    end.

begin_transaction(Transaction, State) ->
    put({transaction, Transaction}, []),
    ok(State).

extend_transaction(Transaction, Callback, Action, State) ->
    extend_transaction(Transaction, {callback, Callback, Action}, State).

extend_transaction(Transaction, Action, State0) ->
    with_transaction(
      Transaction, State0,
      fun (Actions, State) ->
              put({transaction, Transaction}, [Action | Actions]),
              ok(State)
      end).

commit_transaction(Transaction, State0) ->
    with_transaction(
      Transaction, State0,
      fun (Actions, State) ->
              FinalState = lists:foldr(fun perform_transaction_action/2,
                                       State,
                                       Actions),
              erase({transaction, Transaction}),
              ok(FinalState)
      end).

abort_transaction(Transaction, State0) ->
    with_transaction(
      Transaction, State0,
      fun (_Actions, State) ->
              erase({transaction, Transaction}),
              ok(State)
      end).

perform_transaction_action({callback, Callback, Action}, State) ->
    perform_transaction_action(Action, Callback(State));
perform_transaction_action({Method}, State) ->
    send_method(Method, State);
perform_transaction_action({Channel, Method}, State) ->
    send_method(Method, Channel, State);
perform_transaction_action({Method, Props, BodyFragments}, State) ->
    send_method(Method, Props, BodyFragments, State).

%%--------------------------------------------------------------------
%% Heartbeat Management
%%--------------------------------------------------------------------

ensure_heartbeats(Heartbeats,
                  State = #state{socket = Sock, start_heartbeat_fun = SHF}) ->
    [CX, CY] = [list_to_integer(X) ||
                   X <- re:split(Heartbeats, ",", [{return, list}])],

    SendFun = fun() ->
                      catch gen_tcp:send(Sock, <<0>>)
              end,

    Pid = self(),
    ReceiveFun = fun() ->
                         gen_server2:cast(Pid, client_timeout)
                 end,

    {SendTimeout, ReceiveTimeout} =
        {millis_to_seconds(CY), millis_to_seconds(CX)},

    SHF(Sock, SendTimeout, SendFun, ReceiveTimeout, ReceiveFun),

    {{SendTimeout * 1000 , ReceiveTimeout * 1000}, State}.

millis_to_seconds(M) when M =< 0 ->
    0;
millis_to_seconds(M) ->
    case M < 1000 of
        true  -> 1;
        false -> M div 1000
    end.

%%----------------------------------------------------------------------------
%% Queue and Binding Setup
%%----------------------------------------------------------------------------

ensure_queue(subscribe, {exchange, _}, Channel) ->
    %% Create anonymous, exclusive queue for SUBSCRIBE on /exchange destinations
    #'queue.declare_ok'{queue = Queue} =
        amqp_channel:call(Channel, #'queue.declare'{auto_delete = true,
                                                    exclusive = true}),
    {ok, Queue};
ensure_queue(send, {exchange, _}, _Channel) ->
    %% Don't create queues on SEND for /exchange destinations
    {ok, undefined};
ensure_queue(_, {queue, Name}, Channel) ->
    %% Always create named queue for /queue destinations
    Queue = list_to_binary(Name),
    amqp_channel:cast(Channel,
                      #'queue.declare'{durable = true,
                                       queue   = Queue,
                                       nowait  = true}),
    {ok, Queue};
ensure_queue(subscribe, {topic, _}, Channel) ->
    %% Create anonymous, exclusive queue for SUBSCRIBE on /topic destinations
    #'queue.declare_ok'{queue = Queue} =
        amqp_channel:call(Channel, #'queue.declare'{auto_delete = true,
                                                    exclusive = true}),
    {ok, Queue};
ensure_queue(send, {topic, _}, _Channel) ->
    %% Don't create queues on SEND for /topic destinations
    {ok, undefined}.

ensure_queue_binding(QueueBin, {"", Queue}, _Channel) ->
    %% i.e., we should only be asked to bind to the default exchange a
    %% queue with its own name
    QueueBin = list_to_binary(Queue),
    ok;
ensure_queue_binding(Queue, {Exchange, RoutingKey}, Channel) ->
    #'queue.bind_ok'{} =
        amqp_channel:call(Channel,
                          #'queue.bind'{
                            queue       = Queue,
                            exchange    = list_to_binary(Exchange),
                            routing_key = list_to_binary(RoutingKey)}),
    ok.
%%----------------------------------------------------------------------------
%% Success/error handling
%%----------------------------------------------------------------------------

ok(State) ->
    {ok, none, State}.

ok(Command, Headers, BodyFragments, State) ->
    {ok, #stomp_frame{command     = Command,
                      headers     = Headers,
                      body_iolist = BodyFragments}, State}.

amqp_death(ReplyCode, Explanation, State) ->
    ErrorName = ?PROTOCOL:amqp_exception(ReplyCode),
    {stop, amqp_death,
     send_error(atom_to_list(ErrorName),
                format_detail("~s~n", [Explanation]),
                State)}.

error(Message, Detail, State) ->
    priv_error(Message, Detail, none, State).

error(Message, Format, Args, State) ->
    priv_error(Message, Format, Args, none, State).

priv_error(Message, Detail, ServerPrivateDetail, State) ->
    error_logger:error_msg("STOMP error frame sent:~n" ++
                           "Message: ~p~n" ++
                           "Detail: ~p~n" ++
                           "Server private detail: ~p~n",
                           [Message, Detail, ServerPrivateDetail]),
    {error, Message, Detail, State}.

priv_error(Message, Format, Args, ServerPrivateDetail, State) ->
    priv_error(Message, format_detail(Format, Args),
                    ServerPrivateDetail, State).

format_detail(Format, Args) ->
    lists:flatten(io_lib:format(Format, Args)).
%%----------------------------------------------------------------------------
%% Frame sending utilities
%%----------------------------------------------------------------------------
send_frame(Command, Headers, BodyFragments, State) ->
    send_frame(#stomp_frame{command     = Command,
                            headers     = Headers,
                            body_iolist = BodyFragments},
               State).

send_frame(Frame, State = #state{socket = Sock}) ->
    %% We ignore certain errors here, as we will be receiving an
    %% asynchronous notification of the same (or a related) fault
    %% shortly anyway. See bug 21365.
    rabbit_net:port_command(Sock, rabbit_stomp_frame:serialize(Frame)),
    State.

send_error(Message, Detail, State) ->
    send_frame("ERROR", [{"message", Message},
                         {"content-type", "text/plain"},
                         {"version", string:join(?SUPPORTED_VERSIONS, ",")}],
                         Detail, State).

send_error(Message, Format, Args, State) ->
    send_error(Message, format_detail(Format, Args), State).

%%----------------------------------------------------------------------------
%% Skeleton gen_server2 callbacks
%%----------------------------------------------------------------------------
handle_call(_Msg, _From, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

