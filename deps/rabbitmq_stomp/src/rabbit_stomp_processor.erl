%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stomp_processor).

-compile({no_auto_import, [error/3]}).

-export([initial_state/2, process_frame/2, flush_and_die/1]).
-export([flush_pending_receipts/3,
         handle_exit/3,
         cancel_consumer/2,
         handle_queue_event/2]).

-export([adapter_name/1]).
-export([info/2]).

-include("rabbit_stomp_frame.hrl").
-include("rabbit_stomp.hrl").
-include("rabbit_stomp_headers.hrl").
-include_lib("rabbit/include/amqqueue.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-record(proc_state, {session_id, channel, connection, subscriptions,
                     version, start_heartbeat_fun, pending_receipts,
                     config, route_state, reply_queues, frame_transformer,
                     adapter_info, send_fun, ssl_login_name, peer_addr,
                     %% see rabbitmq/rabbitmq-stomp#39
                     trailing_lf, authz_context, auth_mechanism, auth_login,
                     user, queue_states, delivery_tag = 0,
                     default_topic_exchange, default_nack_requeue}).

-record(subscription, {dest_hdr, ack_mode, multi_ack, description}).

-define(FLUSH_TIMEOUT, 60000).

-define(MAX_PERMISSION_CACHE_SIZE, 12).

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
       route_state         = routing_init_state(),
       reply_queues        = #{},
       frame_transformer   = undefined,
       queue_states        = rabbit_queue_type:init(),
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
    _ = send_error("AMQP connection died", "Reason: ~tp", [Reason], State),
    {stop, {conn_died, Reason}, State};

handle_exit(Ch, {shutdown, {server_initiated_close, Code, Explanation}},
            State = #proc_state{channel = Ch}) ->
    amqp_death(Code, Explanation, State);

handle_exit(Ch, Reason, State = #proc_state{channel = Ch}) ->
    _ = send_error("AMQP channel died", "Reason: ~tp", [Reason], State),
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
              {'EXIT', {amqp_error, Name, Msg, _}} ->
                  amqp_death(Name, Msg, State);
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
                      {Auth, {Username, Passwd}} = Creds= creds(Frame1, SSLLoginName, Config),
                      {ok, User} = do_native_login(Creds, State),
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
                                                auth_login = Username,
                                                authz_context = #{},  %% TODO: client_id? else?
                                                user = User}),
                      case {Res, Implicit} of
                          {{ok, _, StateN1}, implicit} -> ok(StateN1);
                          _                            -> Res
                      end;
                  {error, no_common_version} ->
                      error("Version mismatch",
                            "Supported versions are ~ts~n",
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
    maybe_with_transaction(
      Frame,
      fun(State0) ->
	      ensure_no_headers(?HEADERS_NOT_ON_SEND, "SEND", Frame, State0,
				fun (_Command, Frame1, State1) ->
					with_destination("SEND", Frame1, State1, fun do_send/4)
				end)
      end, State);

handle_frame("ACK", Frame, State) ->
    maybe_with_transaction(
      Frame,
      fun(State0) ->
	      ack_action("ACK", Frame, State, fun create_ack_method/3)
      end,
      State);

handle_frame("NACK", Frame, State) ->
    maybe_with_transaction(
      Frame,
      fun(State0) ->
	      ack_action("NACK", Frame, State, fun create_nack_method/3)
      end,
      State);

handle_frame("BEGIN", Frame, State) ->
    transactional_action(Frame, "BEGIN", fun begin_transaction/2, State);

handle_frame("COMMIT", Frame, State) ->
    transactional_action(Frame, "COMMIT", fun commit_transaction/2, State);

handle_frame("ABORT", Frame, State) ->
    transactional_action(Frame, "ABORT", fun abort_transaction/2, State);

handle_frame(Command, _Frame, State) ->
    error("Bad command",
          "Could not interpret command ~tp~n",
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
			    amqp_channel:call(Channel, Method),
			    ok(State);
                        error ->
                            error("Subscription not found",
                                  "Message with id ~tp has no subscription",
                                  [AckValue],
                                  State)
                    end;
                _ ->
                   error("Invalid header",
                         "~tp must include a valid ~tp header~n",
                         [Command, AckHeader],
                         State)
            end;
        not_found ->
            error("Missing header",
                  "~tp must include the ~tp header~n",
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
                  "Consumer tag ~tp is not associated with a subscription.~n",
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
                                 "No more messages will be delivered for ~tp.~n",
                                 [Description],
                                 State),
            tidy_canceled_subscription(ConsumerTag, Subscription,
                                       undefined, State)
    end.

cancel_subscription({error, invalid_prefix}, _Frame, State) ->
    error("Invalid id",
          "UNSUBSCRIBE 'id' may not start with ~ts~n",
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
                  "Subscription to ~tp not found.~n",
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
                          "UNSUBSCRIBE to ~tp failed.~n",
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
    {ok, Dest} = parse_endpoint(DestHdr),
    maybe_delete_durable_sub(Dest, Frame, State#proc_state{subscriptions = Subs1}).

maybe_delete_durable_sub({topic, Name}, Frame,
                         State = #proc_state{auth_login = Username}) ->
    case rabbit_stomp_util:has_durable_header(Frame) of
        true ->
            {ok, Id} = rabbit_stomp_frame:header(Frame, ?HEADER_ID),
            QName = rabbit_stomp_util:subscription_queue_name(Name, Id, Frame),
            delete_queue(list_to_binary(QName), Username),
            ok(State);
        false ->
            ok(State)
    end;
maybe_delete_durable_sub(_Destination, _Frame, State) ->
    ok(State).

with_destination(Command, Frame, State, Fun) ->
    case rabbit_stomp_frame:header(Frame, ?HEADER_DESTINATION) of
        {ok, DestHdr} ->
            case parse_endpoint(DestHdr) of
                {ok, Destination} ->
                    case Fun(Destination, DestHdr, Frame, State) of
                        {error, invalid_endpoint} ->
                            error("Invalid destination",
                                  "'~ts' is not a valid destination for '~ts'~n",
                                  [DestHdr, Command],
                                  State);
                        {error, {invalid_destination, Msg}} ->
                            error("Invalid destination",
                                  "~ts",
                                  [Msg],
                                  State);
                        {error, Reason} ->
                            throw(Reason);
                        Result ->
                            Result
                    end;
                {error, {invalid_destination, Type, Content}} ->
                    error("Invalid destination",
                          "'~ts' is not a valid ~tp destination~n",
                          [Content, Type],
                          State);
                {error, {unknown_destination, Content}} ->
                    error("Unknown destination",
                          "'~ts' is not a valid destination.~n"
                          "Valid destination types are: ~ts.~n",
                          [Content,
                           string:join(?ALL_DEST_PREFIXES, ", ")], State)
            end;
        not_found ->
            error("Missing destination",
                  "~tp must include a 'destination' header~n",
                  [Command],
                  State)
    end.

ensure_no_headers([Hdr | Hdrs], Command, Frame, State, Fun) -> %
    case rabbit_stomp_frame:header(Frame, Hdr) of
        {ok, _} ->
            error("Invalid header",
                  "'~ts' is not allowed on '~ts'.~n",
                  [Hdr, Command],
                  State);
        not_found ->
            ensure_no_headers(Hdrs, Command, Frame, State, Fun)
    end;
ensure_no_headers([], Command, Frame, State, Fun) ->
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
            rabbit_log:warning("STOMP login failed for user '~ts': authentication failed", [Username]),
            error("Bad CONNECT", "Access refused for user '" ++
                  binary_to_list(Username) ++ "'", [], State);
        {error, not_allowed} ->
            rabbit_log:warning("STOMP login failed for user '~ts': "
                               "virtual host access not allowed", [Username]),
            error("Bad CONNECT", "Virtual host '" ++
                                 binary_to_list(VirtualHost) ++
                                 "' access denied", State);
        {error, access_refused} ->
            rabbit_log:warning("STOMP login failed for user '~ts': "
                               "virtual host access not allowed", [Username]),
            error("Bad CONNECT", "Virtual host '" ++
                                 binary_to_list(VirtualHost) ++
                                 "' access denied", State);
        {error, not_loopback} ->
            rabbit_log:warning("STOMP login failed for user '~ts': "
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
    rabbit_misc:format("~ts/~ts", [Product, Version]).

do_subscribe(Destination, DestHdr, Frame,
             State0 = #proc_state{subscriptions = Subs,
                                  default_topic_exchange = DfltTopicEx}) ->
    check_subscription_access(Destination, State0),
    Prefetch =
        rabbit_stomp_frame:integer_header(Frame, ?HEADER_PREFETCH_COUNT,
                                          application:get_env(rabbit, default_consumer_prefetch)),
    {AckMode, IsMulti} = rabbit_stomp_util:ack_mode(Frame),
    case ensure_endpoint(source, Destination, Frame, State0) of
        {ok, Queue, State} ->
            {ok, ConsumerTag, Description} = rabbit_stomp_util:consumer_tag(Frame),
            case maps:find(ConsumerTag, Subs) of
                {ok, _} ->
                    Message = "Duplicated subscription identifier",
                    Detail = "A subscription identified by '~ts' already exists.",
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
                        consume_queue(Queue, #{no_ack => (AckMode == auto),
                                               prefetch_count => Prefetch,
                                               consumer_tag => ConsumerTag,
                                               exclusive_consume => false,
                                               args => Arguments},
                                      State),
                        ok = ensure_binding(Queue, ExchangeAndKey, State)
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
                                         Subs)})
            end;
        {error, _} = Err ->
            Err
    end.

check_subscription_access(Destination = {topic, _Topic},
                          #proc_state{user = #user{username = Username} = User,
                                      default_topic_exchange = DfltTopicEx}) ->
    {ok, DefaultVHost} = application:get_env(rabbitmq_stomp, default_vhost),
    {Exchange, RoutingKey} = parse_routing(Destination, DfltTopicEx),
    Resource = #resource{virtual_host = DefaultVHost,
        kind = topic,
        name = rabbit_data_coercion:to_binary(Exchange)},
    Context = #{routing_key  => rabbit_data_coercion:to_binary(RoutingKey),
                variable_map => #{<<"vhost">> => DefaultVHost, <<"username">> => Username}
    },
    rabbit_access_control:check_topic_access(User, Resource, read, Context);
check_subscription_access(_, _) ->
    authorized.

maybe_clean_up_queue(Queue, #proc_state{connection = Connection,
                                       auth_login = Username}) ->
    {ok, Channel} = amqp_connection:open_channel(Connection),
    catch delete_queue(Queue, Username),
    catch amqp_channel:close(Channel),
    ok.

do_send(Destination, _DestHdr,
        Frame = #stomp_frame{body_iolist = BodyFragments},
        State0 = #proc_state{default_topic_exchange = DfltTopicEx}) ->
    case ensure_endpoint(dest, Destination, Frame, State0) of

        {ok, _Q, State} ->

            {Frame1, State1} =
                ensure_reply_to(Frame, State),

            Props = rabbit_stomp_util:message_properties(Frame1),

            {Exchange, RoutingKey} = parse_routing(Destination, DfltTopicEx),

            Method = #'basic.publish'{
              exchange = list_to_binary(Exchange),
              routing_key = list_to_binary(RoutingKey),
              mandatory = false,
              immediate = false},

	    ok(send_method(Method, Props, BodyFragments,
			   maybe_record_receipt(Frame1, State1)));
		
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


deliver_to_client(ConsumerTag, Ack, Msgs, State) ->
    lists:foldl(fun(Msg, S) ->
                       deliver_one_to_client(ConsumerTag, Ack, Msg, S)
                end, State, Msgs).

deliver_one_to_client(ConsumerTag, _Ack, {QName, QPid, _MsgId, Redelivered,
                                         #basic_message{exchange_name = ExchangeName,
                                                        routing_keys  = [RoutingKey | _CcRoutes],
                                                        content       = Content}},
                      State = #proc_state{queue_states = QStates,
                                          delivery_tag = DeliveryTag}) ->
    Delivery = #'basic.deliver'{consumer_tag = ConsumerTag,
                                delivery_tag = DeliveryTag,
                                redelivered  = Redelivered,
                                exchange     = ExchangeName#resource.name,
                                routing_key  = RoutingKey},


    {Props, Payload} = rabbit_basic_common:from_content(Content),


    DeliveryCtx = case rabbit_queue_type:module(QName, QStates) of
                     {ok, rabbit_classic_queue} ->
                         {ok, QPid, ok};
                     _ -> undefined
                 end,

    State1 = send_delivery(Delivery, Props, Payload, DeliveryCtx, State),

    State1#proc_state{delivery_tag = DeliveryTag + 1}.


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
                       "There is no current subscription with tag '~ts'.",
                       [ConsumerTag],
                       State)
    end,
    maybe_notify_sent(DeliveryCtx),
    NewState.

maybe_notify_sent(undefined) ->
    ok;
maybe_notify_sent({_, QPid, _}) ->
       ok = rabbit_amqqueue:notify_sent(QPid, self()).

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
    rabbit_log:debug("~ts:close_connection: undefined state", [?MODULE]),
    #proc_state{channel = none, connection = none, subscriptions = none}.

%%----------------------------------------------------------------------------
%% Reply-To
%%----------------------------------------------------------------------------

ensure_reply_to(Frame = #stomp_frame{headers = Headers}, State) ->
    case rabbit_stomp_frame:header(Frame, ?HEADER_REPLY_TO) of
        not_found ->
            {Frame, State};
        {ok, ReplyTo} ->
            {ok, Destination} = parse_endpoint(ReplyTo),
            case dest_temp_queue(Destination) of
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

ensure_reply_queue(TempQueueId, State = #proc_state{reply_queues  = RQS,
                                                    subscriptions = Subs}) ->
    case maps:find(TempQueueId, RQS) of
        {ok, RQ} ->
            {binary_to_list(RQ), State};
        error ->
            {ok, Queue} = create_queue(State),
            #resource{name = QNameBin} = QName = amqqueue:get_name(Queue),

            ConsumerTag = rabbit_stomp_util:consumer_tag_reply_to(TempQueueId),
            Spec = #{no_ack => true,
                     prefetch_count => application:get_env(rabbit, default_consumer_prefetch),
                     consumer_tag => ConsumerTag,
                     exclusive_consume => false,
                     args => []},
            {ok, State1} = consume_queue(QName, Spec, State),
            Destination = binary_to_list(QNameBin),

            %% synthesise a subscription to the reply queue destination
            Subs1 = maps:put(ConsumerTag,
                             #subscription{dest_hdr  = Destination,
                                           multi_ack = false},
                             Subs),

            {Destination, State1#proc_state{
                            reply_queues  = maps:put(TempQueueId, QNameBin, RQS),
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
                  "~tp must include a 'transaction' header~n",
                  [Name],
                  State)
    end.

maybe_with_transaction(Frame, Fun, State) ->
    case transactional(Frame) of
	{yes, Transaction} ->
	    extend_transaction(
	      Transaction,
	      Fun,
	      State);
	no ->
	    Fun(State)
	end.

with_transaction(Transaction, State, Fun) ->
    case get({transaction, Transaction}) of
        undefined ->
            error("Bad transaction",
                  "Invalid transaction identifier: ~tp~n",
                  [Transaction],
                  State);
        Actions ->
            Fun(Actions, State)
    end.

begin_transaction(Transaction, State) ->
    put({transaction, Transaction}, []),
    ok(State).

extend_transaction(Transaction, Fun, State0) ->
    with_transaction(
      Transaction, State0,
      fun (Funs, State) ->
              put({transaction, Transaction}, [Fun | Funs]),
              ok(State)
      end).

commit_transaction(Transaction, State0) ->
    with_transaction(
      Transaction, State0,
      fun (Funs, State) ->
              FinalState = lists:foldr(fun perform_transaction_action/2,
                                       State,
                                       Funs),
              erase({transaction, Transaction}),
              ok(FinalState)
      end).

abort_transaction(Transaction, State0) ->
    with_transaction(
      Transaction, State0,
      fun (_Frames, State) ->
              erase({transaction, Transaction}),
              ok(State)
      end).

perform_transaction_action(Fun, State) ->
    Fun(State).

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

ensure_endpoint(_Direction, {queue, []}, _Frame, _State) ->
    {error, {invalid_destination, "Destination cannot be blank"}};

ensure_endpoint(source, EndPoint, {_, _, Headers, _} = Frame, State) ->
    Params =
        [{subscription_queue_name_gen,
          fun () ->
              Id = build_subscription_id(Frame),
              % Note: we discard the exchange here so there's no need to use
              % the default_topic_exchange configuration key
              {_, Name} = parse_routing(EndPoint),
              list_to_binary(rabbit_stomp_util:subscription_queue_name(Name, Id, Frame))
          end
         }] ++ rabbit_stomp_util:build_params(EndPoint, Headers),
    Arguments = rabbit_stomp_util:build_arguments(Headers),
    util_ensure_endpoint(source, EndPoint, [Arguments | Params], State);

ensure_endpoint(Direction, EndPoint, {_, _, Headers, _}, State) ->
    Params = rabbit_stomp_util:build_params(EndPoint, Headers),
    Arguments = rabbit_stomp_util:build_arguments(Headers),
    util_ensure_endpoint(Direction, EndPoint, [Arguments | Params], State).

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

amqp_death(ErrorName, Explanation, State) when is_atom(ErrorName) ->
    ErrorDesc = rabbit_misc:format("~ts", [Explanation]),
    log_error(ErrorName, ErrorDesc, none),
    {stop, normal, close_connection(send_error(atom_to_list(ErrorName), ErrorDesc, State))};
amqp_death(ReplyCode, Explanation, State) ->
    ErrorName = amqp_connection:error_atom(ReplyCode),
    ErrorDesc = rabbit_misc:format("~ts", [Explanation]),
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
                     "Message: ~tp~n"
                     "Detail: ~tp~n"
                     "Server private detail: ~tp",
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
    {Exchange0, RoutingKey} = parse_routing(Destination),
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

create_queue(_State = #proc_state{authz_context = AuthzCtx, user = #user{username = Username} = User}) ->

    {ok, VHost} = application:get_env(rabbitmq_stomp, default_vhost),
    QNameBin = rabbit_guid:binary(rabbit_guid:gen_secure(), "stomp.gen"),
    QName = rabbit_misc:r(VHost, queue, QNameBin),

        %% configure access to queue required for queue.declare
        ok = check_resource_access(User, QName, configure, AuthzCtx),
        case rabbit_vhost_limit:is_over_queue_limit(VHost) of
            false ->
                rabbit_core_metrics:queue_declared(QName),

                case rabbit_amqqueue:declare(QName, _Durable = false, _AutoDelete = true,
                                             [], self(), Username) of
                    {new, Q} when ?is_amqqueue(Q) ->
                        rabbit_core_metrics:queue_created(QName),
                        {ok, Q};
                    Other ->
                        log_error(rabbit_misc:format("Failed to declare ~s: ~p", [rabbit_misc:rs(QName)]), Other, none),
                        {error, queue_declare}
                end;
            {true, Limit} ->
                log_error(rabbit_misc:format("cannot declare ~s because ", [rabbit_misc:rs(QName)]),
                          rabbit_misc:format("queue limit ~p in vhost '~s' is reached",  [Limit, VHost]),
                          none),
                {error, queue_limit_exceeded}
        end.

delete_queue(QName, Username) ->
    {ok, DefaultVHost} = application:get_env(rabbitmq_stomp, default_vhost),
    Queue = rabbit_misc:r(DefaultVHost, queue, QName),
    case rabbit_amqqueue:with(
           Queue,
           fun (Q) ->
                   rabbit_queue_type:delete(Q, false, false, Username)
           end,
           fun (not_found) ->
                   ok;
               ({absent, Q, crashed}) ->
                   rabbit_classic_queue:delete_crashed(Q, Username);
               ({absent, Q, stopped}) ->
                   rabbit_classic_queue:delete_crashed(Q, Username);
               ({absent, _Q, _Reason}) ->
                   ok
           end) of
        {ok, _N} ->
            ok;
        ok ->
            ok
    end.

ensure_binding(QueueBin, {"", Queue}, _State) ->
    %% i.e., we should only be asked to bind to the default exchange a
    %% queue with its own name
    QueueBin = list_to_binary(Queue),
    ok;
ensure_binding(Queue, {Exchange, RoutingKey}, _State = #proc_state{auth_login = Username}) ->
    {ok, DefaultVHost} = application:get_env(rabbitmq_stomp, default_vhost),
    Binding = #binding{source = rabbit_misc:r(DefaultVHost, exchange, list_to_binary(Exchange)),
                       destination = rabbit_misc:r(DefaultVHost, queue, Queue),
                       key = list_to_binary(RoutingKey)},
    case rabbit_binding:add(Binding, Username) of
        {error, {resources_missing, [{not_found, Name} | _]}} ->
            rabbit_amqqueue:not_found(Name);
        {error, {resources_missing, [{absent, Q, Reason} | _]}} ->
            rabbit_amqqueue:absent(Q, Reason);
        {error, {binding_invalid, Fmt, Args}} ->
            rabbit_misc:protocol_error(precondition_failed, Fmt, Args);
        {error, #amqp_error{} = Error} ->
            rabbit_misc:protocol_error(Error);
        ok ->
            ok
    end.

check_resource_access(User, Resource, Perm, Context) ->
    V = {Resource, Context, Perm},
    Cache = case get(permission_cache) of
                undefined -> [];
                Other     -> Other
            end,
    case lists:member(V, Cache) of
        true ->
            ok;
        false ->
            try rabbit_access_control:check_resource_access(User, Resource, Perm, Context) of
                ok ->
                    CacheTail = lists:sublist(Cache, ?MAX_PERMISSION_CACHE_SIZE-1),
                    put(permission_cache, [V | CacheTail]),
                    ok
            catch
                exit:#amqp_error{name = access_refused,
                                 explanation = Msg} ->
                    log_error(access_refused, Msg, none),
                    {error, access_refused}
            end
    end.

do_native_login(Creds, State) ->
    {ok, DefaultVHost} = application:get_env(rabbitmq_stomp, default_vhost),
    {Username, AuthProps} = case Creds of
                                {ssl, {Username0, none}}-> {Username0, []};
                                {_, {Username0, Password}} -> {Username0, [{password, Password},
                                                                           {vhost, DefaultVHost}]}
                            end,

     case rabbit_access_control:check_user_login(Username, AuthProps) of
        {ok, User} ->
            {ok, User};
        {refused, Username1, _Msg, _Args} ->
             rabbit_log:warning("STOMP login failed for user '~ts': authentication failed", [Username1]),
             error("Bad CONNECT", "Access refused for user '" ++
                      binary_to_list(Username1) ++ "'", [], State)
     end.

handle_queue_event({queue_event, QRef, Evt}, #proc_state{queue_states  = QStates0} = State) ->
    case rabbit_queue_type:handle_event(QRef, Evt, QStates0) of
        {ok, QState1, Actions} ->
            State1 = State#proc_state{queue_states = QState1},
            State = handle_queue_actions(Actions, State1),
            {ok, State};
        %% {eol, Actions} ->
        %%     State1 = handle_queue_actions(Actions, State0),
        %%     State2 = handle_consuming_queue_down_or_eol(QRef, State1),
        %%     {ConfirmMXs, UC1} =
        %%         rabbit_confirms:remove_queue(QRef, State2#ch.unconfirmed),
        %%     %% Deleted queue is a special case.
        %%     %% Do not nack the "rejected" messages.
        %%     State3 = record_confirms(ConfirmMXs,
        %%                              State2#ch{unconfirmed = UC1}),
        %%     _ = erase_queue_stats(QRef),
        %%     noreply_coalesce(
        %%       State3#ch{queue_states = rabbit_queue_type:remove(QRef, QueueStates0)});
        {protocol_error, Type, Reason, ReasonArgs} = Error ->
            log_error(Type, Reason, ReasonArgs),
            {error, Error, State}
    end.

handle_queue_actions(Actions, #proc_state{} = State0) ->
    lists:foldl(
      fun ({deliver, ConsumerTag, Ack, Msgs}, S) ->
              deliver_to_client(ConsumerTag, Msgs, Ack, S)%% ;
          %% ({settled, QName, PktIds}, S = #state{unacked_client_pubs = U0}) ->
          %%     {ConfirmPktIds, U} = rabbit_mqtt_confirms:confirm(PktIds, QName, U0),
          %%     send_puback(ConfirmPktIds, S),
          %%     S#state{unacked_client_pubs = U};
          %% ({rejected, _QName, PktIds}, S = #state{unacked_client_pubs = U0}) ->
          %%     %% Negative acks are supported in MQTT v5 only.
          %%     %% Therefore, in MQTT v3 and v4 we ignore rejected messages.
          %%     U = lists:foldl(
          %%           fun(PktId, Acc0) ->
          %%                   case rabbit_mqtt_confirms:reject(PktId, Acc0) of
          %%                       {ok, Acc} -> Acc;
          %%                       {error, not_found} -> Acc0
          %%                   end
          %%           end, U0, PktIds),
          %%     S#state{unacked_client_pubs = U};
          %% ({block, QName}, S = #state{queues_soft_limit_exceeded = QSLE}) ->
          %%     S#state{queues_soft_limit_exceeded = sets:add_element(QName, QSLE)};
          %% ({unblock, QName}, S = #state{queues_soft_limit_exceeded = QSLE}) ->
          %%     S#state{queues_soft_limit_exceeded = sets:del_element(QName, QSLE)};
          %% ({queue_down, QName}, S) ->
          %%     handle_queue_down(QName, S)
      end, State0, Actions).


parse_endpoint(Destination) ->
    parse_endpoint(Destination, false).

parse_endpoint(undefined, AllowAnonymousQueue) ->
    parse_endpoint("/queue", AllowAnonymousQueue);

parse_endpoint(Destination, AllowAnonymousQueue) when is_binary(Destination) ->
    parse_endpoint(unicode:characters_to_list(Destination),
                                              AllowAnonymousQueue);
parse_endpoint(Destination, AllowAnonymousQueue) when is_list(Destination) ->
    case re:split(Destination, "/", [{return, list}]) of
        [Name] ->
            {ok, {queue, unescape(Name)}};
        ["", Type | Rest]
            when Type =:= "exchange" orelse Type =:= "queue" orelse
                 Type =:= "topic"    orelse Type =:= "temp-queue" ->
            parse_endpoint0(atomise(Type), Rest, AllowAnonymousQueue);
        ["", "amq", "queue" | Rest] ->
            parse_endpoint0(amqqueue, Rest, AllowAnonymousQueue);
        ["", "reply-queue" = Prefix | [_|_]] ->
            parse_endpoint0(reply_queue,
                            [lists:nthtail(2 + length(Prefix), Destination)],
                            AllowAnonymousQueue);
        _ ->
            {error, {unknown_destination, Destination}}
    end.

parse_endpoint0(exchange, ["" | _] = Rest,    _) ->
    {error, {invalid_destination, exchange, to_url(Rest)}};
parse_endpoint0(exchange, [Name],             _) ->
    {ok, {exchange, {unescape(Name), undefined}}};
parse_endpoint0(exchange, [Name, Pattern],    _) ->
    {ok, {exchange, {unescape(Name), unescape(Pattern)}}};
parse_endpoint0(queue,    [],                 false) ->
    {error, {invalid_destination, queue, []}};
parse_endpoint0(queue,    [],                 true) ->
    {ok, {queue, undefined}};
parse_endpoint0(Type,     [[_|_]] = [Name],   _) ->
    {ok, {Type, unescape(Name)}};
parse_endpoint0(Type,     Rest,               _) ->
    {error, {invalid_destination, Type, to_url(Rest)}}.

%% --------------------------------------------------------------------------

util_ensure_endpoint(source, {exchange, {Name, _}}, Params, State) ->
    ExchangeName = rabbit_misc:r(Name, exchange,  proplists:get_value(vhost, Params)),
    check_exchange(ExchangeName, proplists:get_value(check_exchange, Params, false)),
    Amqqueue = new_amqqueue(undefined, exchange, Params, State),
    {ok, Queue} = create_queue(Amqqueue, State),
    {ok, Queue, State};

util_ensure_endpoint(source, {topic, _}, Params, State) ->
    Amqqueue = new_amqqueue(undefined, topic, Params, State),
    {ok, Queue} = create_queue(Amqqueue, State),
    {ok, Queue, State};

util_ensure_endpoint(_Dir, {queue, undefined}, _Params, State) ->
    {ok, undefined, State};

util_ensure_endpoint(_, {queue, Name}, Params, State=#proc_state{route_state = RoutingState}) ->
    Params1 = rabbit_misc:pmerge(durable, true, Params),
    Queue = list_to_binary(Name),
    RState1 = case sets:is_element(Queue, RoutingState) of
                  true -> State;
                  _    -> Amqqueue = new_amqqueue(Queue, queue, Params1, State),
                          {ok, Queue} = create_queue(Amqqueue, State),
                          #resource{name = QNameBin} = amqqueue:get_name(Queue),
                          sets:add_element(QNameBin, RoutingState)
              end,
    {ok, Queue, State#proc_state{route_state = RState1}};

util_ensure_endpoint(dest, {exchange, {Name, _}}, Params, State) ->
    ExchangeName = rabbit_misc:r(Name, exchange,  proplists:get_value(vhost, Params)),
    check_exchange(ExchangeName, proplists:get_value(check_exchange, Params, false)),
    {ok, undefined, State};

util_ensure_endpoint(dest, {topic, _}, _Params, State) ->
    {ok, undefined, State};

util_ensure_endpoint(_, {amqqueue, Name}, _Params, State) ->
  {ok, list_to_binary(Name), State};

util_ensure_endpoint(_, {reply_queue, Name}, _Params, State) ->
  {ok, list_to_binary(Name), State};

util_ensure_endpoint(_Direction, _Endpoint, _Params, _State) ->
    {error, invalid_endpoint}.


%% --------------------------------------------------------------------------

parse_routing({exchange, {Name, undefined}}) ->
    {Name, ""};
parse_routing({exchange, {Name, Pattern}}) ->
    {Name, Pattern};
parse_routing({topic, Name}) ->
    {"amq.topic", Name};
parse_routing({Type, Name})
  when Type =:= queue orelse Type =:= reply_queue orelse Type =:= amqqueue ->
    {"", Name}.

dest_temp_queue({temp_queue, Name}) -> Name;
dest_temp_queue(_)                  -> none.

%% --------------------------------------------------------------------------

check_exchange(_,            false) ->
    ok;
check_exchange(ExchangeName, true) ->
    _ = rabbit_exchange:lookup_or_die(ExchangeName),
    ok.

new_amqqueue(QNameBin0, Type, Params0, _State = #proc_state{user = #user{username = Username}}) ->
    QNameBin = case  {Type, proplists:get_value(subscription_queue_name_gen, Params0)} of
                   {topic, SQNG} when is_function(SQNG) ->
                      SQNG();
                   {exchange, SQNG} when is_function(SQNG) ->
                       SQNG();
                   _ ->
                       QNameBin0
               end,
    QName = rabbit_misc:r(proplists:get_value(vhost, Params0), queue, QNameBin),
    %% defaults
    Params = case proplists:get_value(durable, Params0, false) of
                  false -> [{auto_delete, true}, {exclusive, true} | Params0];
                  true  -> Params0
              end,

    amqqueue:new(QName,
                 none,
                 proplists:get_value(durable, Params, false),
                 proplists:get_value(auto_delete, Params, false),
                 case proplists:get_value(exclusive, Params, false) of
                     false -> none;
                     true -> self()
                 end,
                 proplists:get_value(arguments, Params, []),
                 application:get_env(rabbitmq_stomp, default_vhost),
                 #{user => Username}).


to_url([])  -> [];
to_url(Lol) -> "/" ++ string:join(Lol, "/").

atomise(Name) when is_list(Name) ->
    list_to_atom(re:replace(Name, "-", "_", [{return,list}, global])).

unescape(Str) -> unescape(Str, []).

unescape("%2F" ++ Str, Acc) -> unescape(Str, [$/ | Acc]);
unescape([C | Str],    Acc) -> unescape(Str, [C | Acc]);
unescape([],           Acc) -> lists:reverse(Acc).


consume_queue(QName, Spec0, State = #proc_state{user = #user{username = Username} = User,
                                                authz_context = AuthzCtx,
                                                queue_states  = QStates0}) ->
    case check_resource_access(User, QName, read, AuthzCtx) of
        ok ->
            Spec = Spec0#{channel_pid => self(),
                          limiter_pid => none,
                          limiter_active => false,
                          ok_msg => undefined,
                          acting_user => Username},
            rabbit_amqqueue:with(
              QName,
              fun(Q1) ->
                      case rabbit_queue_type:consume(Q1, Spec, QStates0) of
                          {ok, QStates} ->
                              State1 = State#proc_state{queue_states = QStates},
                              {ok, State1};
                          {error, Reason} ->
                              error("Failed to consume from ~s: ~p",
                                    [rabbit_misc:rs(QName), Reason],
                                              State)
                      end
              end);
        {error, access_refused} ->
            error("Failed to consume from ~s: no read access",
                  [rabbit_misc:rs(QName)],
                  State)
    end.


create_queue(Amqqueue, _State = #proc_state{authz_context = AuthzCtx, user = User}) ->
    {ok, VHost} = application:get_env(rabbitmq_stomp, default_vhost),
    QName = amqqueue:get_name(Amqqueue),

    %% configure access to queue required for queue.declare
    ok = check_resource_access(User, QName, configure, AuthzCtx),
        case rabbit_vhost_limit:is_over_queue_limit(VHost) of
            false ->
                rabbit_core_metrics:queue_declared(QName),

                case rabbit_amqqueue:declare(Amqqueue, node()) of
                    {new, Q} when ?is_amqqueue(Q) ->
                        rabbit_core_metrics:queue_created(QName),
                        {ok, Q};
                    Other ->
                        log_error(rabbit_misc:format("Failed to declare ~s: ~p", [rabbit_misc:rs(QName)]), Other, none),
                        {error, queue_declare}
                end;
            {true, Limit} ->
                log_error(rabbit_misc:format("cannot declare ~s because ", [rabbit_misc:rs(QName)]),
                          rabbit_misc:format("queue limit ~p in vhost '~s' is reached",  [Limit, VHost]),
                          none),
                {error, queue_limit_exceeded}
        end.

routing_init_state() -> sets:new().
