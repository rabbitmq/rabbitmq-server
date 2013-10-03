%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_stomp_processor).
-behaviour(gen_server2).

-export([start_link/1, init_arg/2, process_frame/2, flush_and_die/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("amqp_client/include/rabbit_routing_prefixes.hrl").
-include("rabbit_stomp_frame.hrl").
-include("rabbit_stomp.hrl").
-include("rabbit_stomp_headers.hrl").

-record(state, {session_id, channel, connection, subscriptions,
                version, start_heartbeat_fun, pending_receipts,
                config, route_state, reply_queues, frame_transformer,
                adapter_info, send_fun, ssl_login_name}).

-record(subscription, {dest_hdr, channel, ack_mode, multi_ack, description}).

-define(FLUSH_TIMEOUT, 60000).

%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------
start_link(Args) ->
    gen_server2:start_link(?MODULE, Args, []).

init_arg(ProcessorPid, InitArgs) ->
    gen_server2:cast(ProcessorPid, {init, InitArgs}).

process_frame(Pid, Frame = #stomp_frame{command = "SEND"}) ->
    credit_flow:send(Pid),
    gen_server2:cast(Pid, {"SEND", Frame, self()});
process_frame(Pid, Frame = #stomp_frame{command = Command}) ->
    gen_server2:cast(Pid, {Command, Frame, noflow}).

flush_and_die(Pid) ->
    gen_server2:cast(Pid, flush_and_die).

%%----------------------------------------------------------------------------
%% Basic gen_server2 callbacks
%%----------------------------------------------------------------------------

init(Configuration) ->
    process_flag(trap_exit, true),
    {ok,
     #state {
       session_id          = none,
       channel             = none,
       connection          = none,
       subscriptions       = dict:new(),
       version             = none,
       pending_receipts    = undefined,
       config              = Configuration,
       route_state         = rabbit_routing_util:init_state(),
       reply_queues        = dict:new(),
       frame_transformer   = undefined},
     hibernate,
     {backoff, 1000, 1000, 10000}
    }.

terminate(_Reason, State) ->
    close_connection(State).

handle_cast({init, [SendFun, AdapterInfo, StartHeartbeatFun, SSLLoginName]},
            State) ->
    {noreply, State #state { send_fun            = SendFun,
                             adapter_info        = AdapterInfo,
                             start_heartbeat_fun = StartHeartbeatFun,
                             ssl_login_name      = SSLLoginName }};

handle_cast(flush_and_die, State) ->
    {stop, normal, close_connection(State)};

handle_cast({"STOMP", Frame, noflow}, State) ->
    process_connect(no_implicit, Frame, State);

handle_cast({"CONNECT", Frame, noflow}, State) ->
    process_connect(no_implicit, Frame, State);

handle_cast(Request, State = #state{channel = none,
                                     config = #stomp_configuration{
                                      implicit_connect = true}}) ->
    {noreply, State1 = #state{channel = Ch}, _} =
        process_connect(implicit, #stomp_frame{headers = []}, State),
    case Ch of
        none -> {stop, normal, State1};
        _    -> handle_cast(Request, State1)
    end;

handle_cast(_Request, State = #state{channel = none,
                                     config = #stomp_configuration{
                                      implicit_connect = false}}) ->
    {noreply,
     send_error("Illegal command",
                "You must log in using CONNECT first",
                State),
     hibernate};

handle_cast({Command, Frame, FlowPid},
            State = #state{frame_transformer = FT}) ->
    case FlowPid of
        noflow -> ok;
        _      -> credit_flow:ack(FlowPid)
    end,
    Frame1 = FT(Frame),
    process_request(
      fun(StateN) ->
              case validate_frame(Command, Frame1, StateN) of
                  R = {error, _, _, _} -> R;
                  _                    -> handle_frame(Command, Frame1, StateN)
              end
      end,
      fun(StateM) -> ensure_receipt(Frame1, StateM) end,
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
handle_info(#'basic.cancel'{consumer_tag = Ctag}, State) ->
    process_request(
      fun(StateN) -> server_cancel_consumer(Ctag, StateN) end, State);
handle_info({'EXIT', Conn,
             {shutdown, {server_initiated_close, Code, Explanation}}},
            State = #state{connection = Conn}) ->
    amqp_death(Code, Explanation, State);
handle_info({'EXIT', Conn, Reason}, State = #state{connection = Conn}) ->
    send_error("AMQP connection died", "Reason: ~p", [Reason], State),
    {stop, {conn_died, Reason}, State};
handle_info({inet_reply, _, ok}, State) ->
    {noreply, State, hibernate};
handle_info({bump_credit, Msg}, State) ->
    credit_flow:handle_bump_msg(Msg),
    {noreply, State, hibernate};
handle_info({inet_reply, _, Status}, State) ->
    {stop, Status, State}.

process_request(ProcessFun, State) ->
    process_request(ProcessFun, fun (StateM) -> StateM end, State).

process_request(ProcessFun, SuccessFun, State) ->
    Res = case catch ProcessFun(State) of
              {'EXIT',
               {{shutdown,
                 {server_initiated_close, ReplyCode, Explanation}}, _}} ->
                  amqp_death(ReplyCode, Explanation, State);
              {'EXIT', Reason} ->
                  priv_error("Processing error", "Processing error",
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

process_connect(Implicit, Frame,
                State = #state{channel        = none,
                               config         = Config,
                               ssl_login_name = SSLLoginName,
                               adapter_info   = AdapterInfo}) ->
    process_request(
      fun(StateN) ->
              case negotiate_version(Frame) of
                  {ok, Version} ->
                      FT = frame_transformer(Version),
                      Frame1 = FT(Frame),
                      {Username, Passwd} = creds(Frame1, SSLLoginName, Config),
                      {ok, DefaultVHost} = application:get_env(
                                             rabbitmq_stomp, default_vhost),
                      {ProtoName, _} = AdapterInfo#amqp_adapter_info.protocol,
                      Res = do_login(
                              Username, Passwd,
                              login_header(Frame1, ?HEADER_HOST, DefaultVHost),
                              login_header(Frame1, ?HEADER_HEART_BEAT, "0,0"),
                              AdapterInfo#amqp_adapter_info{
                                protocol = {ProtoName, Version}}, Version,
                              StateN#state{frame_transformer = FT}),
                      case {Res, Implicit} of
                          {{ok, _, StateN1}, implicit} -> ok(StateN1);
                          _                            -> Res
                      end;
                  {error, no_common_version} ->
                      error("Version mismatch",
                            "Supported versions are ~s~n",
                            [string:join(?SUPPORTED_VERSIONS, ",")],
                            StateN)
              end
      end,
      State).

creds(Frame, SSLLoginName,
      #stomp_configuration{default_login    = DefLogin,
                           default_passcode = DefPasscode}) ->
    PasswordCreds = {login_header(Frame, ?HEADER_LOGIN,    DefLogin),
                     login_header(Frame, ?HEADER_PASSCODE, DefPasscode)},
    case {rabbit_stomp_frame:header(Frame, ?HEADER_LOGIN), SSLLoginName} of
        {not_found, none}    -> PasswordCreds;
        {not_found, SSLName} -> {SSLName, none};
        _                    -> PasswordCreds
    end.

login_header(Frame, Key, Default) when is_binary(Default) ->
    login_header(Frame, Key, binary_to_list(Default));
login_header(Frame, Key, Default) ->
    case rabbit_stomp_frame:header(Frame, Key, Default) of
        undefined -> undefined;
        Hdr       -> list_to_binary(Hdr)
    end.

%%----------------------------------------------------------------------------
%% Frame Transformation
%%----------------------------------------------------------------------------
frame_transformer("1.0") -> fun rabbit_stomp_util:trim_headers/1;
frame_transformer(_) -> fun(Frame) -> Frame end.

%%----------------------------------------------------------------------------
%% Frame Validation
%%----------------------------------------------------------------------------

validate_frame(Command, Frame, State)
  when Command =:= "SUBSCRIBE" orelse Command =:= "UNSUBSCRIBE" ->
    Hdr = fun(Name) -> rabbit_stomp_frame:header(Frame, Name) end,
    case {Hdr(?HEADER_PERSISTENT), Hdr(?HEADER_ID)} of
        {{ok, "true"}, not_found} ->
            error("Missing Header",
                  "Header 'id' is required for durable subscriptions", State);
        _ ->
            ok(State)
    end;
validate_frame(_Command, _Frame, State) ->
    ok(State).

%%----------------------------------------------------------------------------
%% Frame handlers
%%----------------------------------------------------------------------------

handle_frame("DISCONNECT", _Frame, State) ->
    {stop, normal, close_connection(State)};

handle_frame("SUBSCRIBE", Frame, State) ->
    with_destination("SUBSCRIBE", Frame, State, fun do_subscribe/4);

handle_frame("UNSUBSCRIBE", Frame, State) ->
    ConsumerTag = rabbit_stomp_util:consumer_tag(Frame),
    cancel_subscription(ConsumerTag, Frame, State);

handle_frame("SEND", Frame, State) ->
    without_headers(?HEADERS_NOT_ON_SEND, "SEND", Frame, State,
        fun (_Command, Frame1, State1) ->
            with_destination("SEND", Frame1, State1, fun do_send/4)
        end);

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
          "Could not interpret command ~p~n",
          [Command],
          State).

%%----------------------------------------------------------------------------
%% Internal helpers for processing frames callbacks
%%----------------------------------------------------------------------------

ack_action(Command, Frame,
           State = #state{subscriptions = Subs,
                          version       = Version}, MethodFun) ->
    AckHeader = rabbit_stomp_util:ack_header_name(Version),
    case rabbit_stomp_frame:header(Frame, AckHeader) of
        {ok, AckValue} ->
            case rabbit_stomp_util:parse_message_id(AckValue) of
                {ok, {ConsumerTag, _SessionId, DeliveryTag}} ->
                    case dict:find(ConsumerTag, Subs) of
                        {ok, Sub = #subscription{channel = SubChannel}} ->
                            Method = MethodFun(DeliveryTag, Sub),
                            case transactional(Frame) of
                                {yes, Transaction} ->
                                    extend_transaction(Transaction,
                                                       {SubChannel, Method},
                                                       State);
                                no ->
                                    amqp_channel:call(SubChannel, Method),
                                    ok(State)
                            end;
                        error ->
                            error("Subscription not found",
                                  "Message with id ~p has no subscription",
                                  [AckValue],
                                  State)
                    end;
                _ ->
                   error("Invalid header",
                         "~p must include a valid ~p header~n",
                         [Command, AckHeader],
                         State)
            end;
        not_found ->
            error("Missing header",
                  "~p must include the ~p header~n",
                  [Command, AckHeader],
                  State)
    end.

%%----------------------------------------------------------------------------
%% Internal helpers for processing frames callbacks
%%----------------------------------------------------------------------------
server_cancel_consumer(ConsumerTag, State = #state{subscriptions = Subs}) ->
    case dict:find(ConsumerTag, Subs) of
        error ->
            error("Server cancelled unknown subscription",
                  "Consumer tag ~p is not associated with a subscription.~n",
                  [ConsumerTag],
                  State);
        {ok, Subscription = #subscription{description = Description}} ->
            Id = case rabbit_stomp_util:tag_to_id(ConsumerTag) of
                     {ok,    {_, Id1}} -> Id1;
                     {error, {_, Id1}} -> "Unknown[" ++ Id1 ++ "]"
                 end,
            send_error_frame("Server cancelled subscription",
                             [{?HEADER_SUBSCRIPTION, Id}],
                             "The server has canceled a subscription.~n"
                             "No more messages will be delivered for ~p.~n",
                             [Description],
                             State),
            tidy_canceled_subscription(ConsumerTag, Subscription,
                                       #stomp_frame{}, State)
    end.

cancel_subscription({error, invalid_prefix}, _Frame, State) ->
    error("Invalid id",
          "UNSUBSCRIBE 'id' may not start with ~s~n",
          [?TEMP_QUEUE_ID_PREFIX],
          State);

cancel_subscription({error, _}, _Frame, State) ->
    error("Missing destination or id",
          "UNSUBSCRIBE must include a 'destination' or 'id' header",
          State);

cancel_subscription({ok, ConsumerTag, Description}, Frame,
                    State = #state{subscriptions = Subs}) ->
    case dict:find(ConsumerTag, Subs) of
        error ->
            error("No subscription found",
                  "UNSUBSCRIBE must refer to an existing subscription.~n"
                  "Subscription to ~p not found.~n",
                  [Description],
                  State);
        {ok, Subscription = #subscription{channel = SubChannel,
                                          description = Descr}} ->
            case amqp_channel:call(SubChannel,
                                   #'basic.cancel'{
                                     consumer_tag = ConsumerTag}) of
                #'basic.cancel_ok'{consumer_tag = ConsumerTag} ->
                    tidy_canceled_subscription(ConsumerTag, Subscription,
                                               Frame, State);
                _ ->
                    error("Failed to cancel subscription",
                          "UNSUBSCRIBE to ~p failed.~n",
                          [Descr],
                          State)
            end
    end.

tidy_canceled_subscription(ConsumerTag, #subscription{dest_hdr = DestHdr,
                                                      channel = SubChannel},
                           Frame, State = #state{channel = MainChannel,
                                                 subscriptions = Subs}) ->
    ok = ensure_subchannel_closed(SubChannel, MainChannel),
    Subs1 = dict:erase(ConsumerTag, Subs),
    {ok, Dest} = rabbit_routing_util:parse_endpoint(DestHdr),
    maybe_delete_durable_sub(Dest, Frame, State#state{subscriptions = Subs1}).

maybe_delete_durable_sub({topic, Name}, Frame,
                         State = #state{channel = Channel}) ->
    case rabbit_stomp_frame:boolean_header(Frame,
                                           ?HEADER_PERSISTENT, false) of
        true ->
            {ok, Id} = rabbit_stomp_frame:header(Frame, ?HEADER_ID),
            QName = rabbit_stomp_util:durable_subscription_queue(Name, Id),
            amqp_channel:call(Channel,
                              #'queue.delete'{queue  = list_to_binary(QName),
                                              nowait = false}),
            ok(State);
        false ->
            ok(State)
    end;
maybe_delete_durable_sub(_Destination, _Frame, State) ->
    ok(State).

ensure_subchannel_closed(SubChannel, MainChannel)
  when SubChannel == MainChannel ->
    ok;

ensure_subchannel_closed(SubChannel, _MainChannel) ->
    amqp_channel:close(SubChannel),
    ok.

with_destination(Command, Frame, State, Fun) ->
    case rabbit_stomp_frame:header(Frame, ?HEADER_DESTINATION) of
        {ok, DestHdr} ->
            case rabbit_routing_util:parse_endpoint(DestHdr) of
                {ok, Destination} ->
                    case Fun(Destination, DestHdr, Frame, State) of
                        {error, invalid_endpoint} ->
                            error("Invalid destination",
                                  "'~s' is not a valid destination for '~s'~n",
                                  [DestHdr, Command],
                                  State);
                        {error, Reason} ->
                            throw(Reason);
                        Result ->
                            Result
                    end;
                {error, {invalid_destination, Type, Content}} ->
                    error("Invalid destination",
                          "'~s' is not a valid ~p destination~n",
                          [Content, Type],
                          State);
                {error, {unknown_destination, Content}} ->
                    error("Unknown destination",
                          "'~s' is not a valid destination.~n"
                          "Valid destination types are: ~s.~n",
                          [Content,
                           string:join(rabbit_routing_util:all_dest_prefixes(),
                                       ", ")], State)
            end;
        not_found ->
            error("Missing destination",
                  "~p must include a 'destination' header~n",
                  [Command],
                  State)
    end.

without_headers([Hdr | Hdrs], Command, Frame, State, Fun) ->
    case rabbit_stomp_frame:header(Frame, Hdr) of
        {ok, _} ->
            error("Invalid header",
                  "'~s' is not allowed on '~s'.~n",
                  [Hdr, Command],
                  State);
        not_found ->
            without_headers(Hdrs, Command, Frame, State, Fun)
    end;
without_headers([], Command, Frame, State, Fun) ->
    Fun(Command, Frame, State).

do_login(undefined, _, _, _, _, _, State) ->
    error("Bad CONNECT", "Missing login or passcode header(s)", State);
do_login(Username, Passwd, VirtualHost, Heartbeat, AdapterInfo, Version,
         State) ->
    case amqp_connection:start(
           #amqp_params_direct{username     = Username,
                               password     = Passwd,
                               virtual_host = VirtualHost,
                               adapter_info = AdapterInfo}) of
        {ok, Connection} ->
            link(Connection),
            {ok, Channel} = amqp_connection:open_channel(Connection),
            SessionId = rabbit_guid:string(rabbit_guid:gen_secure(), "session"),
            {{SendTimeout, ReceiveTimeout}, State1} =
                ensure_heartbeats(Heartbeat, State),
            ok("CONNECTED",
               [{?HEADER_SESSION, SessionId},
                {?HEADER_HEART_BEAT,
                 io_lib:format("~B,~B", [SendTimeout, ReceiveTimeout])},
                {?HEADER_SERVER, server_header()},
                {?HEADER_VERSION, Version}],
               "",
               State1#state{session_id = SessionId,
                            channel    = Channel,
                            connection = Connection,
                            version    = Version});
        {error, {auth_failure, _}} ->
            rabbit_log:warning("STOMP login failed for user ~p~n",
                               [binary_to_list(Username)]),
            error("Bad CONNECT", "Access refused for user '" ++
                  binary_to_list(Username) ++ "'~n", [], State);
        {error, access_refused} ->
            rabbit_log:warning("STOMP login failed - access_refused "
                               "(vhost access not allowed)~n"),
            error("Bad CONNECT", "Virtual host '" ++
                                 binary_to_list(VirtualHost) ++
                                 "' access denied", State)
    end.

server_header() ->
    {ok, Product} = application:get_key(rabbit, id),
    {ok, Version} = application:get_key(rabbit, vsn),
    rabbit_misc:format("~s/~s", [Product, Version]).

do_subscribe(Destination, DestHdr, Frame,
             State = #state{subscriptions = Subs,
                            route_state   = RouteState,
                            connection    = Connection,
                            channel       = MainChannel}) ->
    Prefetch =
        rabbit_stomp_frame:integer_header(Frame, ?HEADER_PREFETCH_COUNT,
                                          undefined),
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
    case ensure_endpoint(source, Destination, Frame, Channel, RouteState) of
        {ok, Queue, RouteState1} ->
            {ok, ConsumerTag, Description} =
                rabbit_stomp_util:consumer_tag(Frame),
            amqp_channel:subscribe(Channel,
                                   #'basic.consume'{
                                     queue        = Queue,
                                     consumer_tag = ConsumerTag,
                                     no_local     = false,
                                     no_ack       = (AckMode == auto),
                                     exclusive    = false},
                                   self()),
            ExchangeAndKey = rabbit_routing_util:parse_routing(Destination),
            ok = rabbit_routing_util:ensure_binding(
                   Queue, ExchangeAndKey, Channel),
            ok(State#state{subscriptions =
                               dict:store(
                                 ConsumerTag,
                                 #subscription{dest_hdr    = DestHdr,
                                               channel     = Channel,
                                               ack_mode    = AckMode,
                                               multi_ack   = IsMulti,
                                               description = Description},
                                 Subs),
                           route_state = RouteState1});
        {error, _} = Err ->
            Err
    end.

do_send(Destination, _DestHdr,
        Frame = #stomp_frame{body_iolist = BodyFragments},
        State = #state{channel = Channel, route_state = RouteState}) ->

    case ensure_endpoint(dest, Destination, Frame, Channel, RouteState) of

        {ok, _Q, RouteState1} ->

            {Frame1, State1} =
                ensure_reply_to(Frame, State#state{route_state = RouteState1}),

            Props = rabbit_stomp_util:message_properties(Frame1),

            {Exchange, RoutingKey} =
                rabbit_routing_util:parse_routing(Destination),

            Method = #'basic.publish'{
              exchange = list_to_binary(Exchange),
              routing_key = list_to_binary(RoutingKey),
              mandatory = false,
              immediate = false},

            case transactional(Frame1) of
                {yes, Transaction} ->
                    extend_transaction(
                      Transaction,
                      fun(StateN) ->
                              maybe_record_receipt(Frame1, StateN)
                      end,
                      {Method, Props, BodyFragments},
                      State1);
                no ->
                    ok(send_method(Method, Props, BodyFragments,
                                   maybe_record_receipt(Frame1, State1)))
            end;

        {error, _} = Err ->

            Err
    end.

create_ack_method(DeliveryTag, #subscription{multi_ack = IsMulti}) ->
    #'basic.ack'{delivery_tag = DeliveryTag,
                 multiple     = IsMulti}.

create_nack_method(DeliveryTag, #subscription{multi_ack = IsMulti}) ->
    #'basic.nack'{delivery_tag = DeliveryTag,
                  multiple     = IsMulti}.

negotiate_version(Frame) ->
    ClientVers = re:split(rabbit_stomp_frame:header(
                            Frame, ?HEADER_ACCEPT_VERSION, "1.0"),
                          ",", [{return, list}]),
    rabbit_stomp_util:negotiate_version(ClientVers, ?SUPPORTED_VERSIONS).


send_delivery(Delivery = #'basic.deliver'{consumer_tag = ConsumerTag},
              Properties, Body,
              State = #state{session_id    = SessionId,
                             subscriptions = Subs,
                             version       = Version}) ->
    case dict:find(ConsumerTag, Subs) of
        {ok, #subscription{ack_mode = AckMode}} ->
            send_frame(
              "MESSAGE",
              rabbit_stomp_util:headers(SessionId, Delivery, Properties,
                                        AckMode, Version),
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

send_method(Method = #'basic.publish'{}, Channel, Properties, BodyFragments,
            State) ->
    amqp_channel:cast_flow(
      Channel, Method,
      #amqp_msg{props   = Properties,
                payload = list_to_binary(BodyFragments)}),
    State.

close_connection(State = #state{connection = none}) ->
    State;
%% Closing the connection will close the channel and subchannels
close_connection(State = #state{connection = Connection}) ->
    %% ignore noproc or other exceptions to avoid debris
    catch amqp_connection:close(Connection),
    State#state{channel = none, connection = none, subscriptions = none}.

%%----------------------------------------------------------------------------
%% Reply-To
%%----------------------------------------------------------------------------
ensure_reply_to(Frame = #stomp_frame{headers = Headers}, State) ->
    case rabbit_stomp_frame:header(Frame, ?HEADER_REPLY_TO) of
        not_found ->
            {Frame, State};
        {ok, ReplyTo} ->
            {ok, Destination} = rabbit_routing_util:parse_endpoint(ReplyTo),
            case rabbit_routing_util:dest_temp_queue(Destination) of
                none ->
                    {Frame, State};
                TempQueueId ->
                    {ReplyQueue, State1} =
                        ensure_reply_queue(TempQueueId, State),
                    {Frame#stomp_frame{
                       headers = lists:keyreplace(
                                   ?HEADER_REPLY_TO, 1, Headers,
                                   {?HEADER_REPLY_TO, ReplyQueue})},
                     State1}
            end
    end.

ensure_reply_queue(TempQueueId, State = #state{channel       = Channel,
                                               reply_queues  = RQS,
                                               subscriptions = Subs}) ->
    case dict:find(TempQueueId, RQS) of
        {ok, RQ} ->
            {binary_to_list(RQ), State};
        error ->
            #'queue.declare_ok'{queue = Queue} =
                amqp_channel:call(Channel,
                                  #'queue.declare'{auto_delete = true,
                                                   exclusive   = true}),

            ConsumerTag = rabbit_stomp_util:consumer_tag_reply_to(TempQueueId),
            #'basic.consume_ok'{} =
                amqp_channel:subscribe(Channel,
                                       #'basic.consume'{
                                         queue        = Queue,
                                         consumer_tag = ConsumerTag,
                                         no_ack       = true,
                                         nowait       = false},
                                       self()),

            Destination = binary_to_list(Queue),

            %% synthesise a subscription to the reply queue destination
            Subs1 = dict:store(ConsumerTag,
                               #subscription{dest_hdr    = Destination,
                                             channel     = Channel,
                                             multi_ack   = false},
                               Subs),

            {Destination, State#state{
                            reply_queues  = dict:store(TempQueueId, Queue, RQS),
                            subscriptions = Subs1}}
    end.

%%----------------------------------------------------------------------------
%% Receipt Handling
%%----------------------------------------------------------------------------

ensure_receipt(Frame = #stomp_frame{command = Command}, State) ->
    case rabbit_stomp_frame:header(Frame, ?HEADER_RECEIPT) of
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
    case rabbit_stomp_frame:header(Frame, ?HEADER_RECEIPT) of
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
    case gb_trees:is_empty(PR) of
        true  -> {[], PR};
        false -> accumulate_receipts1(DeliveryTag,
                                      gb_trees:take_smallest(PR), [])
    end.

accumulate_receipts1(DeliveryTag, {Key, Value, PR}, Acc)
  when Key > DeliveryTag ->
    {lists:reverse(Acc), gb_trees:insert(Key, Value, PR)};
accumulate_receipts1(DeliveryTag, {_Key, Value, PR}, Acc) ->
    Acc1 = [Value | Acc],
    case gb_trees:is_empty(PR) of
        true  -> {lists:reverse(Acc1), PR};
        false -> accumulate_receipts1(DeliveryTag,
                                      gb_trees:take_smallest(PR), Acc1)
    end.


%%----------------------------------------------------------------------------
%% Transaction Support
%%----------------------------------------------------------------------------

transactional(Frame) ->
    case rabbit_stomp_frame:header(Frame, ?HEADER_TRANSACTION) of
        {ok, Transaction} -> {yes, Transaction};
        not_found         -> no
    end.

transactional_action(Frame, Name, Fun, State) ->
    case transactional(Frame) of
        {yes, Transaction} ->
            Fun(Transaction, State);
        no ->
            error("Missing transaction",
                  "~p must include a 'transaction' header~n",
                  [Name],
                  State)
    end.

with_transaction(Transaction, State, Fun) ->
    case get({transaction, Transaction}) of
        undefined ->
            error("Bad transaction",
                  "Invalid transaction identifier: ~p~n",
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
                  State = #state{start_heartbeat_fun = SHF,
                                 send_fun            = RawSendFun}) ->
    [CX, CY] = [list_to_integer(X) ||
                   X <- re:split(Heartbeats, ",", [{return, list}])],

    SendFun = fun() -> RawSendFun(sync, <<$\n>>) end,
    Pid = self(),
    ReceiveFun = fun() -> gen_server2:cast(Pid, client_timeout) end,

    {SendTimeout, ReceiveTimeout} =
        {millis_to_seconds(CY), millis_to_seconds(CX)},

    SHF(SendTimeout, SendFun, ReceiveTimeout, ReceiveFun),

    {{SendTimeout * 1000 , ReceiveTimeout * 1000}, State}.

millis_to_seconds(M) when M =< 0   -> 0;
millis_to_seconds(M) when M < 1000 -> 1;
millis_to_seconds(M)               -> M div 1000.

%%----------------------------------------------------------------------------
%% Queue Setup
%%----------------------------------------------------------------------------

ensure_endpoint(source, EndPoint, Frame, Channel, State) ->
    Params =
        case rabbit_stomp_frame:boolean_header(
               Frame, ?HEADER_PERSISTENT, false) of
            true ->
                [{subscription_queue_name_gen,
                    fun () ->
                        {ok, Id} = rabbit_stomp_frame:header(Frame, ?HEADER_ID),
                        {_, Name} = rabbit_routing_util:parse_routing(EndPoint),
                        list_to_binary(
                          rabbit_stomp_util:durable_subscription_queue(Name,
                                                                       Id))
                    end},
                 {durable, true}];
            false ->
                [{durable, false}]
        end,
    rabbit_routing_util:ensure_endpoint(source, Channel, EndPoint, Params, State);

ensure_endpoint(Direction, Endpoint, _Frame, Channel, State) ->
    rabbit_routing_util:ensure_endpoint(Direction, Channel, Endpoint, State).

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
    ErrorName = amqp_connection:error_atom(ReplyCode),
    ErrorDesc = rabbit_misc:format("~s~n", [Explanation]),
    log_error(ErrorName, ErrorDesc, none),
    {stop, normal, send_error(atom_to_list(ErrorName), ErrorDesc, State)}.

error(Message, Detail, State) ->
    priv_error(Message, Detail, none, State).

error(Message, Format, Args, State) ->
    priv_error(Message, Format, Args, none, State).

priv_error(Message, Detail, ServerPrivateDetail, State) ->
    log_error(Message, Detail, ServerPrivateDetail),
    {error, Message, Detail, State}.

priv_error(Message, Format, Args, ServerPrivateDetail, State) ->
    priv_error(Message, rabbit_misc:format(Format, Args), ServerPrivateDetail,
               State).

log_error(Message, Detail, ServerPrivateDetail) ->
    rabbit_log:error("STOMP error frame sent:~n"
                     "Message: ~p~n"
                     "Detail: ~p~n"
                     "Server private detail: ~p~n",
                     [Message, Detail, ServerPrivateDetail]).

%%----------------------------------------------------------------------------
%% Frame sending utilities
%%----------------------------------------------------------------------------
send_frame(Command, Headers, BodyFragments, State) ->
    send_frame(#stomp_frame{command     = Command,
                            headers     = Headers,
                            body_iolist = BodyFragments},
               State).

send_frame(Frame, State = #state{send_fun = SendFun}) ->
    SendFun(async, rabbit_stomp_frame:serialize(Frame)),
    State.

send_error_frame(Message, ExtraHeaders, Format, Args, State) ->
    send_error_frame(Message, ExtraHeaders, rabbit_misc:format(Format, Args),
                     State).

send_error_frame(Message, ExtraHeaders, Detail, State) ->
    send_frame("ERROR", [{"message", Message},
                         {"content-type", "text/plain"},
                         {"version", string:join(?SUPPORTED_VERSIONS, ",")}] ++
                        ExtraHeaders,
                        Detail, State).

send_error(Message, Detail, State) ->
    send_error_frame(Message, [], Detail, State).

send_error(Message, Format, Args, State) ->
    send_error(Message, rabbit_misc:format(Format, Args), State).

%%----------------------------------------------------------------------------
%% Skeleton gen_server2 callbacks
%%----------------------------------------------------------------------------
handle_call(_Msg, _From, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
