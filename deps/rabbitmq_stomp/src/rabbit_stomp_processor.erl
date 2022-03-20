%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stomp_processor).

-compile({no_auto_import, [error/3]}).

-export([initial_state/2, process_frame/2, flush_and_die/1]).
-export([flush_pending_receipts/3,
         handle_exit/3,
         cancel_consumer/2,
         send_delivery/5]).

-export([adapter_name/1]).
-export([info/2]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("amqp_client/include/rabbit_routing_prefixes.hrl").
-include("rabbit_stomp_frame.hrl").
-include("rabbit_stomp.hrl").
-include("rabbit_stomp_headers.hrl").

-record(proc_state, {session_id, channel, connection, subscriptions,
                version, start_heartbeat_fun, pending_receipts,
                config, route_state, reply_queues, frame_transformer,
                adapter_info, send_fun, ssl_login_name, peer_addr,
                %% see rabbitmq/rabbitmq-stomp#39
                trailing_lf, auth_mechanism, auth_login,
                default_topic_exchange, default_nack_requeue}).

-record(subscription, {dest_hdr, ack_mode, multi_ack, description}).

-define(FLUSH_TIMEOUT, 60000).

adapter_name(State) ->
  #proc_state{adapter_info = #amqp_adapter_info{name = Name}} = State,
  Name.

%%----------------------------------------------------------------------------

-spec initial_state(
  #stomp_configuration{},
  {SendFun, AdapterInfo, SSLLoginName, PeerAddr})
    -> #proc_state{}
  when SendFun :: fun((atom(), binary()) -> term()),
       AdapterInfo :: #amqp_adapter_info{},
       SSLLoginName :: atom() | binary(),
       PeerAddr :: inet:ip_address().

-type process_frame_result() ::
    {ok, term(), #proc_state{}} |
    {stop, term(), #proc_state{}}.

-spec process_frame(#stomp_frame{}, #proc_state{}) ->
    process_frame_result().

-spec flush_and_die(#proc_state{}) -> #proc_state{}.

-spec command({Command, Frame}, State) -> process_frame_result()
    when Command :: string(),
         Frame   :: #stomp_frame{},
         State   :: #proc_state{}.

-type process_fun() :: fun((#proc_state{}) ->
        {ok, #stomp_frame{}, #proc_state{}}  |
        {error, string(), string(), #proc_state{}} |
        {stop, term(), #proc_state{}}).
-spec process_request(process_fun(), fun((#proc_state{}) -> #proc_state{}), #proc_state{}) ->
    process_frame_result().

-spec flush_pending_receipts(DeliveryTag, IsMulti, State) -> State
    when State :: #proc_state{},
         DeliveryTag :: term(),
         IsMulti :: boolean().

-spec handle_exit(From, Reason, State) -> unknown_exit | {stop, Reason, State}
    when State  :: #proc_state{},
         From   :: pid(),
         Reason :: term().

-spec cancel_consumer(binary(), #proc_state{}) -> process_frame_result().

-spec send_delivery(#'basic.deliver'{}, term(), term(), term(),
                    #proc_state{}) -> #proc_state{}.

%%----------------------------------------------------------------------------


%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------

process_frame(Frame = #stomp_frame{command = Command}, State) ->
    command({Command, Frame}, State).

flush_and_die(State) ->
    close_connection(State).

info(session_id, #proc_state{session_id = Val}) ->
    Val;
info(channel, #proc_state{channel = Val}) -> Val;
info(version, #proc_state{version = Val}) -> Val;
info(implicit_connect, #proc_state{config = #stomp_configuration{implicit_connect = Val}}) ->  Val;
info(auth_login, #proc_state{auth_login = Val}) ->  Val;
info(auth_mechanism, #proc_state{auth_mechanism = Val}) ->  Val;
info(peer_addr, #proc_state{peer_addr = Val}) -> Val;
info(host, #proc_state{adapter_info = #amqp_adapter_info{host = Val}}) -> Val;
info(port, #proc_state{adapter_info = #amqp_adapter_info{port = Val}}) -> Val;
info(peer_host, #proc_state{adapter_info = #amqp_adapter_info{peer_host = Val}}) -> Val;
info(peer_port, #proc_state{adapter_info = #amqp_adapter_info{peer_port = Val}}) -> Val;
info(protocol, #proc_state{adapter_info = #amqp_adapter_info{protocol = Val}}) ->
    case Val of
        {Proto, Version} -> {Proto, rabbit_data_coercion:to_binary(Version)};
        Other -> Other
    end;
info(channels, PState) -> additional_info(channels, PState);
info(channel_max, PState) -> additional_info(channel_max, PState);
info(frame_max, PState) -> additional_info(frame_max, PState);
info(client_properties, PState) -> additional_info(client_properties, PState);
info(ssl, PState) -> additional_info(ssl, PState);
info(ssl_protocol, PState) -> additional_info(ssl_protocol, PState);
info(ssl_key_exchange, PState) -> additional_info(ssl_key_exchange, PState);
info(ssl_cipher, PState) -> additional_info(ssl_cipher, PState);
info(ssl_hash, PState) -> additional_info(ssl_hash, PState).

initial_state(Configuration,
    {SendFun, AdapterInfo0 = #amqp_adapter_info{additional_info = Extra},
     SSLLoginName, PeerAddr}) ->
  %% STOMP connections use exactly one channel. The frame max is not
  %% applicable and there is no way to know what client is used.
  AdapterInfo = AdapterInfo0#amqp_adapter_info{additional_info=[
       {channels, 1},
       {channel_max, 1},
       {frame_max, 0},
       %% TODO: can we use a header to make it possible for clients
       %%       to override this value?
       {client_properties, [{<<"product">>, longstr, <<"STOMP client">>}]}
       |Extra]},
  #proc_state {
       send_fun            = SendFun,
       adapter_info        = AdapterInfo,
       ssl_login_name      = SSLLoginName,
       peer_addr           = PeerAddr,
       session_id          = none,
       channel             = none,
       connection          = none,
       subscriptions       = #{},
       version             = none,
       pending_receipts    = undefined,
       config              = Configuration,
       route_state         = rabbit_routing_util:init_state(),
       reply_queues        = #{},
       frame_transformer   = undefined,
       trailing_lf         = application:get_env(rabbitmq_stomp, trailing_lf, true),
       default_topic_exchange = application:get_env(rabbitmq_stomp, default_topic_exchange, <<"amq.topic">>),
       default_nack_requeue = application:get_env(rabbitmq_stomp, default_nack_requeue, true)}.


command({"STOMP", Frame}, State) ->
    process_connect(no_implicit, Frame, State);

command({"CONNECT", Frame}, State) ->
    process_connect(no_implicit, Frame, State);

command(Request, State = #proc_state{channel = none,
                             config = #stomp_configuration{
                             implicit_connect = true}}) ->
    {ok, State1 = #proc_state{channel = Ch}, _} =
        process_connect(implicit, #stomp_frame{headers = []}, State),
    case Ch of
        none -> {stop, normal, State1};
        _    -> command(Request, State1)
    end;

command(_Request, State = #proc_state{channel = none,
                              config = #stomp_configuration{
                              implicit_connect = false}}) ->
    {ok, send_error("Illegal command",
                    "You must log in using CONNECT first",
                    State), none};

command({Command, Frame}, State = #proc_state{frame_transformer = FT}) ->
    Frame1 = FT(Frame),
    process_request(
      fun(StateN) ->
          case validate_frame(Command, Frame1, StateN) of
              R = {error, _, _, _} -> R;
              _                    -> handle_frame(Command, Frame1, StateN)
          end
      end,
      fun(StateM) -> ensure_receipt(Frame1, StateM) end,
      State).

cancel_consumer(Ctag, State) ->
  process_request(
    fun(StateN) -> server_cancel_consumer(Ctag, StateN) end,
    State).

handle_exit(Conn, {shutdown, {server_initiated_close, Code, Explanation}},
            State = #proc_state{connection = Conn}) ->
    amqp_death(Code, Explanation, State);
handle_exit(Conn, {shutdown, {connection_closing,
                    {server_initiated_close, Code, Explanation}}},
            State = #proc_state{connection = Conn}) ->
    amqp_death(Code, Explanation, State);
handle_exit(Conn, Reason, State = #proc_state{connection = Conn}) ->
    _ = send_error("AMQP connection died", "Reason: ~p", [Reason], State),
    {stop, {conn_died, Reason}, State};

handle_exit(Ch, {shutdown, {server_initiated_close, Code, Explanation}},
            State = #proc_state{channel = Ch}) ->
    amqp_death(Code, Explanation, State);

handle_exit(Ch, Reason, State = #proc_state{channel = Ch}) ->
    _ = send_error("AMQP channel died", "Reason: ~p", [Reason], State),
    {stop, {channel_died, Reason}, State};
handle_exit(Ch, {shutdown, {server_initiated_close, Code, Explanation}},
            State = #proc_state{channel = Ch}) ->
    amqp_death(Code, Explanation, State);
handle_exit(_, _, _) -> unknown_exit.


process_request(ProcessFun, State) ->
    process_request(ProcessFun, fun (StateM) -> StateM end, State).


process_request(ProcessFun, SuccessFun, State) ->
    Res = case catch ProcessFun(State) of
              {'EXIT',
               {{shutdown,
                 {server_initiated_close, ReplyCode, Explanation}}, _}} ->
                  amqp_death(ReplyCode, Explanation, State);
              {'EXIT', {amqp_error, access_refused, Msg, _}} ->
                  amqp_death(access_refused, Msg, State);
              {'EXIT', Reason} ->
                  priv_error("Processing error", "Processing error",
                              Reason, State);
              Result ->
                  Result
          end,
    case Res of
        {ok, Frame, NewState = #proc_state{connection = Conn}} ->
            _ = case Frame of
                    none -> ok;
                    _    -> send_frame(Frame, NewState)
                end,
            {ok, SuccessFun(NewState), Conn};
        {error, Message, Detail, NewState = #proc_state{connection = Conn}} ->
            {ok, send_error(Message, Detail, NewState), Conn};
        {stop, normal, NewState} ->
            {stop, normal, SuccessFun(NewState)};
        {stop, R, NewState} ->
            {stop, R, NewState}
    end.

process_connect(Implicit, Frame,
                State = #proc_state{channel        = none,
                               config         = Config,
                               ssl_login_name = SSLLoginName,
                               adapter_info   = AdapterInfo}) ->
    process_request(
      fun(StateN) ->
              case negotiate_version(Frame) of
                  {ok, Version} ->
                      FT = frame_transformer(Version),
                      Frame1 = FT(Frame),
                      {Auth, {Username, Passwd}} = creds(Frame1, SSLLoginName, Config),
                      {ok, DefaultVHost} = application:get_env(
                                             rabbitmq_stomp, default_vhost),
                      {ProtoName, _} = AdapterInfo#amqp_adapter_info.protocol,
                      Res = do_login(
                              Username, Passwd,
                              login_header(Frame1, ?HEADER_HOST, DefaultVHost),
                              login_header(Frame1, ?HEADER_HEART_BEAT, "0,0"),
                              AdapterInfo#amqp_adapter_info{
                                protocol = {ProtoName, Version}}, Version,
                              StateN#proc_state{frame_transformer = FT,
                                                auth_mechanism = Auth,
                                                auth_login = Username}),
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

creds(_, _, #stomp_configuration{default_login       = DefLogin,
                                 default_passcode    = DefPasscode,
                                 force_default_creds = true}) ->
    {config, {iolist_to_binary(DefLogin), iolist_to_binary(DefPasscode)}};
creds(Frame, SSLLoginName,
      #stomp_configuration{default_login    = DefLogin,
                           default_passcode = DefPasscode}) ->
    PasswordCreds = {login_header(Frame, ?HEADER_LOGIN,    DefLogin),
                     login_header(Frame, ?HEADER_PASSCODE, DefPasscode)},
    case {rabbit_stomp_frame:header(Frame, ?HEADER_LOGIN), SSLLoginName} of
        {not_found, none}    -> {config, PasswordCreds};
        {not_found, SSLName} -> {ssl, {SSLName, none}};
        _                    -> {stomp_headers, PasswordCreds}
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

report_missing_id_header(State) ->
    error("Missing Header",
          "Header 'id' is required for durable subscriptions", State).

validate_frame(Command, Frame, State)
  when Command =:= "SUBSCRIBE" orelse Command =:= "UNSUBSCRIBE" ->
    Hdr = fun(Name) -> rabbit_stomp_frame:header(Frame, Name) end,
    case {Hdr(?HEADER_DURABLE), Hdr(?HEADER_PERSISTENT), Hdr(?HEADER_ID)} of
        {{ok, "true"}, _, not_found} ->
            report_missing_id_header(State);
        {_, {ok, "true"}, not_found} ->
            report_missing_id_header(State);
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
    ack_action("ACK", Frame, State, fun create_ack_method/3);

handle_frame("NACK", Frame, State) ->
    ack_action("NACK", Frame, State, fun create_nack_method/3);

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
           State = #proc_state{subscriptions = Subs,
                          channel              = Channel,
                          version              = Version,
                          default_nack_requeue = DefaultNackRequeue}, MethodFun) ->
    AckHeader = rabbit_stomp_util:ack_header_name(Version),
    case rabbit_stomp_frame:header(Frame, AckHeader) of
        {ok, AckValue} ->
            case rabbit_stomp_util:parse_message_id(AckValue) of
                {ok, {ConsumerTag, _SessionId, DeliveryTag}} ->
                    case maps:find(ConsumerTag, Subs) of
                        {ok, Sub} ->
                            Requeue = rabbit_stomp_frame:boolean_header(Frame, "requeue", DefaultNackRequeue),
                            Method = MethodFun(DeliveryTag, Sub, Requeue),
                            case transactional(Frame) of
                                {yes, Transaction} ->
                                    extend_transaction(
                                      Transaction, {Method}, State);
                                no ->
                                    amqp_channel:call(Channel, Method),
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

server_cancel_consumer(ConsumerTag, State = #proc_state{subscriptions = Subs}) ->
    case maps:find(ConsumerTag, Subs) of
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
            _ = send_error_frame("Server cancelled subscription",
                                 [{?HEADER_SUBSCRIPTION, Id}],
                                 "The server has canceled a subscription.~n"
                                 "No more messages will be delivered for ~p.~n",
                                 [Description],
                                 State),
            tidy_canceled_subscription(ConsumerTag, Subscription,
                                       undefined, State)
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
                    State = #proc_state{subscriptions = Subs,
                                   channel       = Channel}) ->
    case maps:find(ConsumerTag, Subs) of
        error ->
            error("No subscription found",
                  "UNSUBSCRIBE must refer to an existing subscription.~n"
                  "Subscription to ~p not found.~n",
                  [Description],
                  State);
        {ok, Subscription = #subscription{description = Descr}} ->
            case amqp_channel:call(Channel,
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

%% Server-initiated cancelations will pass an undefined instead of a
%% STOMP frame. In this case we know that the queue was deleted and
%% thus we don't have to clean it up.
tidy_canceled_subscription(ConsumerTag, _Subscription,
                           undefined, State = #proc_state{subscriptions = Subs}) ->
    Subs1 = maps:remove(ConsumerTag, Subs),
    ok(State#proc_state{subscriptions = Subs1});

%% Client-initiated cancelations will pass an actual frame
tidy_canceled_subscription(ConsumerTag, #subscription{dest_hdr = DestHdr},
                           Frame, State = #proc_state{subscriptions = Subs}) ->
    Subs1 = maps:remove(ConsumerTag, Subs),
    {ok, Dest} = rabbit_routing_util:parse_endpoint(DestHdr),
    maybe_delete_durable_sub(Dest, Frame, State#proc_state{subscriptions = Subs1}).

maybe_delete_durable_sub({topic, Name}, Frame,
                         State = #proc_state{channel = Channel}) ->
    case rabbit_stomp_util:has_durable_header(Frame) of
        true ->
            {ok, Id} = rabbit_stomp_frame:header(Frame, ?HEADER_ID),
            QName = rabbit_stomp_util:subscription_queue_name(Name, Id, Frame),
            amqp_channel:call(Channel,
                              #'queue.delete'{queue  = list_to_binary(QName),
                                              nowait = false}),
            ok(State);
        false ->
            ok(State)
    end;
maybe_delete_durable_sub(_Destination, _Frame, State) ->
    ok(State).

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
                        {error, {invalid_destination, Msg}} ->
                            error("Invalid destination",
                                  "~s",
                                  [Msg],
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
         State = #proc_state{peer_addr = Addr}) ->
    case start_connection(
           #amqp_params_direct{username     = Username,
                               password     = Passwd,
                               virtual_host = VirtualHost,
                               adapter_info = AdapterInfo}, Username, Addr) of
        {ok, Connection} ->
            link(Connection),
            {ok, Channel} = amqp_connection:open_channel(Connection),
            link(Channel),
            amqp_channel:enable_delivery_flow_control(Channel),
            SessionId = rabbit_guid:string(rabbit_guid:gen_secure(), "session"),
            {SendTimeout, ReceiveTimeout} = ensure_heartbeats(Heartbeat),

          Headers = [{?HEADER_SESSION, SessionId},
                     {?HEADER_HEART_BEAT,
                      io_lib:format("~B,~B", [SendTimeout, ReceiveTimeout])},
                     {?HEADER_VERSION, Version}],
          ok("CONNECTED",
              case application:get_env(rabbitmq_stomp, hide_server_info, false) of
                true  -> Headers;
                false -> [{?HEADER_SERVER, server_header()} | Headers]
              end,
               "",
               State#proc_state{session_id = SessionId,
                                channel    = Channel,
                                connection = Connection,
                                version    = Version});
        {error, {auth_failure, _}} ->
            rabbit_log:warning("STOMP login failed for user '~s': authentication failed", [Username]),
            error("Bad CONNECT", "Access refused for user '" ++
                  binary_to_list(Username) ++ "'~n", [], State);
        {error, not_allowed} ->
            rabbit_log:warning("STOMP login failed for user '~s': "
                               "virtual host access not allowed", [Username]),
            error("Bad CONNECT", "Virtual host '" ++
                                 binary_to_list(VirtualHost) ++
                                 "' access denied", State);
        {error, access_refused} ->
            rabbit_log:warning("STOMP login failed for user '~s': "
                               "virtual host access not allowed", [Username]),
            error("Bad CONNECT", "Virtual host '" ++
                                 binary_to_list(VirtualHost) ++
                                 "' access denied", State);
        {error, not_loopback} ->
            rabbit_log:warning("STOMP login failed for user '~s': "
                               "this user's access is restricted to localhost", [Username]),
            error("Bad CONNECT", "non-loopback access denied", State)
    end.

start_connection(Params, Username, Addr) ->
    case amqp_connection:start(Params) of
        {ok, Conn} -> case rabbit_access_control:check_user_loopback(
                             Username, Addr) of
                          ok          -> {ok, Conn};
                          not_allowed -> amqp_connection:close(Conn),
                                         {error, not_loopback}
                      end;
        {error, E} -> {error, E}
    end.

server_header() ->
    {ok, Product} = application:get_key(rabbit, description),
    {ok, Version} = application:get_key(rabbit, vsn),
    rabbit_misc:format("~s/~s", [Product, Version]).

do_subscribe(Destination, DestHdr, Frame,
             State = #proc_state{subscriptions = Subs,
                                 route_state   = RouteState,
                                 channel       = Channel,
                                 default_topic_exchange = DfltTopicEx}) ->
    check_subscription_access(Destination, State),
    Prefetch =
        rabbit_stomp_frame:integer_header(Frame, ?HEADER_PREFETCH_COUNT,
                                          undefined),
    {AckMode, IsMulti} = rabbit_stomp_util:ack_mode(Frame),
    case ensure_endpoint(source, Destination, Frame, Channel, RouteState) of
        {ok, Queue, RouteState1} ->
            {ok, ConsumerTag, Description} =
                rabbit_stomp_util:consumer_tag(Frame),
            case Prefetch of
                undefined -> ok;
                _         -> amqp_channel:call(
                               Channel, #'basic.qos'{prefetch_count = Prefetch})
            end,
            case maps:find(ConsumerTag, Subs) of
                {ok, _} ->
                    Message = "Duplicated subscription identifier",
                    Detail = "A subscription identified by '~s' already exists.",
                    _ = error(Message, Detail, [ConsumerTag], State),
                    _ = send_error(Message, Detail, [ConsumerTag], State),
                    {stop, normal, close_connection(State)};
                error ->
                    ExchangeAndKey = parse_routing(Destination, DfltTopicEx),
                    StreamOffset = rabbit_stomp_frame:stream_offset_header(Frame, undefined),
                    Arguments = case StreamOffset of
                                    undefined ->
                                        [];
                                    {Type, Value} ->
                                        [{<<"x-stream-offset">>, Type, Value}]
                                end,
                    try
                        amqp_channel:subscribe(Channel,
                                               #'basic.consume'{
                                                  queue        = Queue,
                                                  consumer_tag = ConsumerTag,
                                                  no_local     = false,
                                                  no_ack       = (AckMode == auto),
                                                  exclusive    = false,
                                                  arguments    = Arguments},
                                               self()),
                        ok = rabbit_routing_util:ensure_binding(
                               Queue, ExchangeAndKey, Channel)
                    catch exit:Err ->
                            %% it's safe to delete this queue, it
                            %% was server-named and declared by us
                            case Destination of
                                {exchange, _} ->
                                    ok = maybe_clean_up_queue(Queue, State);
                                {topic, _} ->
                                    ok = maybe_clean_up_queue(Queue, State);
                                _ ->
                                    ok
                            end,
                            exit(Err)
                    end,
                    ok(State#proc_state{subscriptions =
                                       maps:put(
                                         ConsumerTag,
                                         #subscription{dest_hdr    = DestHdr,
                                                       ack_mode    = AckMode,
                                                       multi_ack   = IsMulti,
                                                       description = Description},
                                         Subs),
                                   route_state = RouteState1})
            end;
        {error, _} = Err ->
            Err
    end.

check_subscription_access(Destination = {topic, _Topic},
                          #proc_state{auth_login = _User,
                                      connection = Connection,
                                      default_topic_exchange = DfltTopicEx}) ->
    [{amqp_params, AmqpParams}, {internal_user, InternalUser = #user{username = Username}}] =
        amqp_connection:info(Connection, [amqp_params, internal_user]),
    #amqp_params_direct{virtual_host = VHost} = AmqpParams,
    {Exchange, RoutingKey} = parse_routing(Destination, DfltTopicEx),
    Resource = #resource{virtual_host = VHost,
        kind = topic,
        name = rabbit_data_coercion:to_binary(Exchange)},
    Context = #{routing_key  => rabbit_data_coercion:to_binary(RoutingKey),
                variable_map => #{<<"vhost">> => VHost, <<"username">> => Username}
    },
    rabbit_access_control:check_topic_access(InternalUser, Resource, read, Context);
check_subscription_access(_, _) ->
    authorized.

maybe_clean_up_queue(Queue, #proc_state{connection = Connection}) ->
    {ok, Channel} = amqp_connection:open_channel(Connection),
    catch amqp_channel:call(Channel, #'queue.delete'{queue = Queue}),
    catch amqp_channel:close(Channel),
    ok.

do_send(Destination, _DestHdr,
        Frame = #stomp_frame{body_iolist = BodyFragments},
        State = #proc_state{channel = Channel,
                            route_state = RouteState,
                            default_topic_exchange = DfltTopicEx}) ->
    case ensure_endpoint(dest, Destination, Frame, Channel, RouteState) of

        {ok, _Q, RouteState1} ->

            {Frame1, State1} =
                ensure_reply_to(Frame, State#proc_state{route_state = RouteState1}),

            Props = rabbit_stomp_util:message_properties(Frame1),

            {Exchange, RoutingKey} = parse_routing(Destination, DfltTopicEx),

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

create_ack_method(DeliveryTag, #subscription{multi_ack = IsMulti}, _) ->
    #'basic.ack'{delivery_tag = DeliveryTag,
                 multiple     = IsMulti}.

create_nack_method(DeliveryTag, #subscription{multi_ack = IsMulti}, Requeue) ->
    #'basic.nack'{delivery_tag = DeliveryTag,
                  multiple     = IsMulti,
                  requeue      = Requeue}.

negotiate_version(Frame) ->
    ClientVers = re:split(rabbit_stomp_frame:header(
                            Frame, ?HEADER_ACCEPT_VERSION, "1.0"),
                          ",", [{return, list}]),
    rabbit_stomp_util:negotiate_version(ClientVers, ?SUPPORTED_VERSIONS).


send_delivery(Delivery = #'basic.deliver'{consumer_tag = ConsumerTag},
              Properties, Body, DeliveryCtx,
              State = #proc_state{
                          session_id  = SessionId,
                          subscriptions = Subs,
                          version       = Version}) ->
    NewState = case maps:find(ConsumerTag, Subs) of
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
    end,
    notify_received(DeliveryCtx),
    NewState.

notify_received(undefined) ->
  %% no notification for quorum queues and streams
  ok;
notify_received(DeliveryCtx) ->
  %% notification for flow control
  amqp_channel:notify_received(DeliveryCtx).

send_method(Method, Channel, State) ->
    amqp_channel:call(Channel, Method),
    State.

send_method(Method, State = #proc_state{channel = Channel}) ->
    send_method(Method, Channel, State).

send_method(Method, Properties, BodyFragments,
            State = #proc_state{channel = Channel}) ->
    send_method(Method, Channel, Properties, BodyFragments, State).

send_method(Method = #'basic.publish'{}, Channel, Properties, BodyFragments,
            State) ->
    amqp_channel:cast_flow(
      Channel, Method,
      #amqp_msg{props   = Properties,
                payload = list_to_binary(BodyFragments)}),
    State.

close_connection(State = #proc_state{connection = none}) ->
    State;
%% Closing the connection will close the channel and subchannels
close_connection(State = #proc_state{connection = Connection}) ->
    %% ignore noproc or other exceptions to avoid debris
    catch amqp_connection:close(Connection),
    State#proc_state{channel = none, connection = none, subscriptions = none};
close_connection(undefined) ->
    rabbit_log:debug("~s:close_connection: undefined state", [?MODULE]),
    #proc_state{channel = none, connection = none, subscriptions = none}.

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

ensure_reply_queue(TempQueueId, State = #proc_state{channel       = Channel,
                                               reply_queues  = RQS,
                                               subscriptions = Subs}) ->
    case maps:find(TempQueueId, RQS) of
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
            Subs1 = maps:put(ConsumerTag,
                             #subscription{dest_hdr  = Destination,
                                           multi_ack = false},
                             Subs),

            {Destination, State#proc_state{
                            reply_queues  = maps:put(TempQueueId, Queue, RQS),
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

maybe_record_receipt(Frame, State = #proc_state{channel          = Channel,
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
            State#proc_state{pending_receipts = gb_trees:insert(SeqNo, Id, PR1)};
        not_found ->
            State
    end.

flush_pending_receipts(DeliveryTag, IsMulti,
                       State = #proc_state{pending_receipts = PR}) ->
    {Receipts, PR1} = accumulate_receipts(DeliveryTag, IsMulti, PR),
    State1 = lists:foldl(fun(ReceiptId, StateN) ->
                                 do_receipt(none, ReceiptId, StateN)
                         end, State, Receipts),
    State1#proc_state{pending_receipts = PR1}.

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
perform_transaction_action({Method, Props, BodyFragments}, State) ->
    send_method(Method, Props, BodyFragments, State).

%%--------------------------------------------------------------------
%% Heartbeat Management
%%--------------------------------------------------------------------

ensure_heartbeats(Heartbeats) ->

    [CX, CY] = [list_to_integer(X) ||
                   X <- re:split(Heartbeats, ",", [{return, list}])],

    {SendTimeout, ReceiveTimeout} =
        {millis_to_seconds(CY), millis_to_seconds(CX)},

    _ = rabbit_stomp_reader:start_heartbeats(self(), {SendTimeout, ReceiveTimeout}),
    {SendTimeout * 1000 , ReceiveTimeout * 1000}.

millis_to_seconds(M) when M =< 0   -> 0;
millis_to_seconds(M) when M < 1000 -> 1;
millis_to_seconds(M)               -> M div 1000.

%%----------------------------------------------------------------------------
%% Queue Setup
%%----------------------------------------------------------------------------

ensure_endpoint(_Direction, {queue, []}, _Frame, _Channel, _State) ->
    {error, {invalid_destination, "Destination cannot be blank"}};

ensure_endpoint(source, EndPoint, {_, _, Headers, _} = Frame, Channel, State) ->
    Params =
        [{subscription_queue_name_gen,
          fun () ->
              Id = build_subscription_id(Frame),
              % Note: we discard the exchange here so there's no need to use
              % the default_topic_exchange configuration key
              {_, Name} = rabbit_routing_util:parse_routing(EndPoint),
              list_to_binary(rabbit_stomp_util:subscription_queue_name(Name, Id, Frame))
          end
         }] ++ rabbit_stomp_util:build_params(EndPoint, Headers),
    Arguments = rabbit_stomp_util:build_arguments(Headers),
    rabbit_routing_util:ensure_endpoint(source, Channel, EndPoint,
                                        [Arguments | Params], State);

ensure_endpoint(Direction, EndPoint, {_, _, Headers, _}, Channel, State) ->
    Params = rabbit_stomp_util:build_params(EndPoint, Headers),
    Arguments = rabbit_stomp_util:build_arguments(Headers),
    rabbit_routing_util:ensure_endpoint(Direction, Channel, EndPoint,
                                        [Arguments | Params], State).

build_subscription_id(Frame) ->
    case rabbit_stomp_util:has_durable_header(Frame) of
        true ->
            {ok, Id} = rabbit_stomp_frame:header(Frame, ?HEADER_ID),
            Id;
        false ->
            rabbit_guid:gen_secure()
    end.

%%----------------------------------------------------------------------------
%% Success/error handling
%%----------------------------------------------------------------------------

ok(State) ->
    {ok, none, State}.

ok(Command, Headers, BodyFragments, State) ->
    {ok, #stomp_frame{command     = Command,
                      headers     = Headers,
                      body_iolist = BodyFragments}, State}.

amqp_death(access_refused = ErrorName, Explanation, State) ->
    ErrorDesc = rabbit_misc:format("~s", [Explanation]),
    log_error(ErrorName, ErrorDesc, none),
    {stop, normal, close_connection(send_error(atom_to_list(ErrorName), ErrorDesc, State))};
amqp_death(ReplyCode, Explanation, State) ->
    ErrorName = amqp_connection:error_atom(ReplyCode),
    ErrorDesc = rabbit_misc:format("~s", [Explanation]),
    log_error(ErrorName, ErrorDesc, none),
    {stop, normal, close_connection(send_error(atom_to_list(ErrorName), ErrorDesc, State))}.

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
                     "Server private detail: ~p",
                     [Message, Detail, ServerPrivateDetail]).

%%----------------------------------------------------------------------------
%% Frame sending utilities
%%----------------------------------------------------------------------------

send_frame(Command, Headers, BodyFragments, State) ->
    send_frame(#stomp_frame{command     = Command,
                            headers     = Headers,
                            body_iolist = BodyFragments},
               State).

send_frame(Frame, State = #proc_state{send_fun = SendFun,
                                 trailing_lf = TrailingLF}) ->
    SendFun(async, rabbit_stomp_frame:serialize(Frame, TrailingLF)),
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

additional_info(Key,
                #proc_state{adapter_info =
                                #amqp_adapter_info{additional_info = AddInfo}}) ->
    proplists:get_value(Key, AddInfo).

parse_routing(Destination, DefaultTopicExchange) ->
    {Exchange0, RoutingKey} = rabbit_routing_util:parse_routing(Destination),
    Exchange1 = maybe_apply_default_topic_exchange(Exchange0, DefaultTopicExchange),
    {Exchange1, RoutingKey}.

maybe_apply_default_topic_exchange("amq.topic"=Exchange, <<"amq.topic">>=_DefaultTopicExchange) ->
    %% This is the case where the destination is the same
    %% as the default of amq.topic
    Exchange;
maybe_apply_default_topic_exchange("amq.topic"=_Exchange, DefaultTopicExchange) ->
    %% This is the case where the destination would have been
    %% amq.topic but we have configured a different default
    binary_to_list(DefaultTopicExchange);
maybe_apply_default_topic_exchange(Exchange, _DefaultTopicExchange) ->
    %% This is the case where the destination is different than
    %% amq.topic, so it must have been specified in the
    %% message headers
    Exchange.
