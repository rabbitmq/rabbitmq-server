%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stomp_processor).

-feature(maybe_expr, enable).

-compile({no_auto_import, [error/3]}).

-export([initial_state/2, process_frame/2, flush_and_die/1]).
-export([flush_pending_receipts/3,
         cancel_consumer/2,
         handle_down/2,
         handle_queue_event/2]).

-export([adapter_name/1]).
-export([info/2]).

-include("rabbit_stomp_frame.hrl").
-include("rabbit_stomp.hrl").
-include("rabbit_stomp_headers.hrl").
-include_lib("rabbit/include/amqqueue.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-record(proc_state, {session_id, subscriptions,
                     version, start_heartbeat_fun, pending_receipts,
                     config, route_state, reply_queues, frame_transformer,
                     adapter_info, send_fun, ssl_login_name, peer_addr,
                     %% see rabbitmq/rabbitmq-stomp#39
                     trailing_lf, authz_context, auth_mechanism, auth_login,
                     confirmed, rejected, unconfirmed,
                     %% a map of queue names to consumer tag lists
                     queue_consumers, unacked_message_q, vhost,
                     user, queue_states, delivery_tag = 0, msg_seq_no, delivery_flow,
                     default_topic_exchange, default_nack_requeue}).

-record(subscription, {dest_hdr, ack_mode, multi_ack, description, queue_name}).

-record(pending_ack, {
                      %% delivery identifier used by clients
                      %% to acknowledge and reject deliveries
                      delivery_tag,
                      %% consumer tag
                      tag,
                      delivered_at,
                      %% queue name
                      queue,
                      %% message ID used by queue and message store implementations
                      msg_id
                    }).

-define(FLUSH_TIMEOUT, 60000).

-define(MAX_PERMISSION_CACHE_SIZE, 12).
-define(QUEUE, lqueue).

-import(rabbit_misc, [maps_put_truthy/3]).

adapter_name(State) ->
  #proc_state{adapter_info = #amqp_adapter_info{name = Name}} = State,
  Name.

%%----------------------------------------------------------------------------

%% -spec initial_state(
%%   #stomp_configuration{},
%%   {SendFun, AdapterInfo, SSLLoginName, PeerAddr})
%%     -> #proc_state{}
%%   when SendFun :: fun((atom(), binary()) -> term()),
%%        AdapterInfo :: #amqp_adapter_info{},
%%        SSLLoginName :: atom() | binary(),
%%        PeerAddr :: inet:ip_address().

%% -type process_frame_result() ::
%%     {ok, term(), #proc_state{}} |
%%     {stop, term(), #proc_state{}}.

%% -spec process_frame(#stomp_frame{}, #proc_state{}) ->
%%           process_frame_result().

%% -spec flush_and_die(#proc_state{}) -> #proc_state{}.

%% -spec command({Command, Frame}, State) -> process_frame_result()
%%     when Command :: string(),
%%          Frame   :: #stomp_frame{},
%%          State   :: #proc_state{}.

%% -type process_fun() :: fun((#proc_state{}) ->
%%         {ok, #stomp_frame{}, #proc_state{}}  |
%%         {error, string(), string(), #proc_state{}} |
%%         {stop, term(), #proc_state{}}).
%% -spec process_request(process_fun(), fun((#proc_state{}) -> #proc_state{}), #proc_state{}) ->
%%     process_frame_result().

%% -spec flush_pending_receipts(DeliveryTag, IsMulti, State) -> State
%%     when State :: #proc_state{},
%%          DeliveryTag :: term(),
%%          IsMulti :: boolean().

%% -spec cancel_consumer(binary(), #proc_state{}) -> process_frame_result().

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
info(version, #proc_state{version = Val}) -> Val;
info(implicit_connect, #proc_state{config = #stomp_configuration{implicit_connect = Val}}) ->  Val;
info(auth_login, #proc_state{auth_login = Val}) ->  Val;
info(auth_mechanism, #proc_state{auth_mechanism = Val}) ->  Val;
info(peer_addr, #proc_state{peer_addr = Val}) -> Val;
info(host, #proc_state{adapter_info = #amqp_adapter_info{host = Val}}) -> Val;
info(port, #proc_state{adapter_info = #amqp_adapter_info{port = Val}}) -> Val;
info(peer_host, #proc_state{adapter_info = #amqp_adapter_info{peer_host = Val}}) -> Val;
info(peer_port, #proc_state{adapter_info = #amqp_adapter_info{peer_port = Val}}) -> Val;
info(protocol, #proc_state{version = Version}) ->
    VersionTuple = case Version of
                       "1.0" -> {1, 0};
                       "1.1" -> {1, 1};
                       "1.2" -> {1, 2};
                       _ -> none
                   end,
    {'STOMP', VersionTuple};
info(user, #proc_state{user = undefined}) -> undefined;
info(user, #proc_state{user = #user{username = Username}}) -> Username;
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

    %% io:format("AdapterInfo0 ~p~n", [AdapterInfo0]),
    %% io:format("PeerAddr ~p~n", [PeerAddr]),

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

    Flow = case rabbit_misc:get_env(rabbit, mirroring_flow_control, true) of
               true   -> flow;
               false  -> noflow
           end,
  #proc_state {
       send_fun            = SendFun,
       adapter_info        = AdapterInfo,
       ssl_login_name      = SSLLoginName,
       peer_addr           = PeerAddr,
       session_id          = none,
       subscriptions       = #{},
       queue_consumers     = #{},
       version             = none,
       config              = Configuration,
       route_state         = routing_init_state(),
       reply_queues        = #{},
       frame_transformer   = undefined,
       msg_seq_no          = 1,
       delivery_flow       = Flow,
       unconfirmed         = rabbit_confirms:init(),
       confirmed           = [],
       unacked_message_q   = ?QUEUE:new(),
       rejected            = [],
       queue_states        = rabbit_queue_type:init(),
       pending_receipts    = gb_trees:empty(),
       trailing_lf         = application:get_env(rabbitmq_stomp, trailing_lf, true),
       default_topic_exchange = application:get_env(rabbitmq_stomp, default_topic_exchange, <<"amq.topic">>),
       default_nack_requeue = application:get_env(rabbitmq_stomp, default_nack_requeue, true)}.


command({"STOMP", Frame}, State) ->
    process_connect(no_implicit, Frame, State);

command({"CONNECT", Frame}, State) ->
    process_connect(no_implicit, Frame, State);

command(Request, State = #proc_state{user = undefined,
                                     config = #stomp_configuration{
                                                 implicit_connect = true}}) ->

    case process_connect(implicit, #stomp_frame{headers = []}, State) of
        {ok, State1 = #proc_state{user = undefined}} ->
            {stop, normal, State1};
        {ok, State1 = #proc_state{user = _User}} ->
            command(Request, State1);
        Res -> Res
    end;

command(_Request, State = #proc_state{user = undefined,
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

handle_consuming_queue_down_or_eol(QName,
                                   State = #proc_state{queue_consumers = QCons}) ->
    %% io:format("DOE QName ~p~n", [QName]),
    %% io:format("DOE QCons ~p~n", [QCons]),
    ConsumerTags = case maps:find(QName, QCons) of
                       error       -> gb_sets:new();
                       {ok, CTags} -> CTags
                   end,
    %% io:format("DOE ConsumerTags ~p~n", [ConsumerTags]),
    gb_sets:fold(
      fun (CTag, StateN) ->
              {ok, S} = cancel_consumer(CTag, StateN),
              S
      end, State#proc_state{queue_consumers = maps:remove(QName, QCons)}, ConsumerTags).

cancel_consumer(CTag, State) ->
  process_request(
    fun(StateN) -> server_cancel_consumer(CTag, StateN) end,
    State).

process_request(ProcessFun, State) ->
    process_request(ProcessFun, fun (StateM) -> StateM end, State).


process_request(ProcessFun, SuccessFun, State) ->
    Res = case catch ProcessFun(State) of
              {'EXIT',
               {{shutdown,
                 {server_initiated_close, ReplyCode, Explanation}}, _}} ->
                  amqp_death(ReplyCode, Explanation, State);
              {'EXIT', {amqp_error, Name, Msg, _}} ->
                  %% io:format("amqp_error ~p, ~p~n", [Name, Msg]),
                  amqp_death(Name, Msg, State);
              {'EXIT', Reason} ->
                  priv_error("Processing error", "Processing error",
                              Reason, State);
              Result ->
                  %% io:format("ProcessFun: ~p~n", [Result]),
                  Result
          end,
    case Res of
        {ok, Frame, NewState} ->
            _ = case Frame of
                    none -> ok;
                    _    -> send_frame(Frame, NewState)
                end,
            {ok, SuccessFun(NewState)};
        {ok, NewState} ->
            {ok, SuccessFun(NewState)};
        {error, Message, Detail, NewState} ->
            {ok, send_error(Message, Detail, NewState)};
        {stop, normal, NewState} ->
            {stop, normal, SuccessFun(NewState)};
        {stop, R, NewState} ->
            {stop, R, NewState}
    end.

process_connect(Implicit, Frame,
                State = #proc_state{user           = undefined,
                                    config         = Config,
                                    ssl_login_name = SSLLoginName,
                                    adapter_info   = AdapterInfo}) ->
    process_request(
      fun(StateN) ->
              case negotiate_version(Frame) of
                  {ok, Version} ->
                      %% io:format("Version ~p~n", [Version]),
                      FT = frame_transformer(Version),
                      Frame1 = FT(Frame),
                      {Auth, {Username, _}} = Creds = creds(Frame1, SSLLoginName, Config),
                      {ok, DefaultVHost} = application:get_env(
                                             rabbitmq_stomp, default_vhost),
                      VHost = login_header(Frame1, ?HEADER_HOST, DefaultVHost),
                      Heartbeat = login_header(Frame1, ?HEADER_HEART_BEAT, "0,0"),
                      {ProtoName, _} = AdapterInfo#amqp_adapter_info.protocol,
                      Res = do_native_login(Creds, Heartbeat, Version, StateN#proc_state{vhost = VHost,
                                                                                         adapter_info = AdapterInfo#amqp_adapter_info{
                                                                                                          protocol = {ProtoName, Version}},
                                                                                         frame_transformer = FT,
                                                                                         auth_mechanism = Auth,
                                                                                         auth_login = Username}),
                      case {Res, Implicit} of
                          {{ok, _, StateN1}, implicit} ->
                              self() ! connection_created, ok(StateN1);
                          _                            ->
                              self() ! connection_created, Res
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
	      ack_action("ACK", Frame, State0, fun handle_ack/4)
      end,
      State);

handle_frame("NACK", Frame, State) ->
    maybe_with_transaction(
      Frame,
      fun(State0) ->
	      ack_action("NACK", Frame, State0, fun handle_nack/4)
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
           State = #proc_state{subscriptions        = Subs,
                               version              = Version,
                               default_nack_requeue = DefaultNackRequeue}, Fun) ->
    AckHeader = rabbit_stomp_util:ack_header_name(Version),
    case rabbit_stomp_frame:header(Frame, AckHeader) of
        {ok, AckValue} ->
            case rabbit_stomp_util:parse_message_id(AckValue) of
                {ok, {ConsumerTag, _SessionId, DeliveryTag}} ->
                    %% io:format("ConsumerTag ~p, DeliveryTag ~p~n", [ConsumerTag, DeliveryTag]),
                    case maps:find(ConsumerTag, Subs) of
                        {ok, Sub} ->
                            %% io:format("Sub ~p~n", [Sub]),
                            Requeue = rabbit_stomp_frame:boolean_header(Frame, "requeue", DefaultNackRequeue),
                            State1 = Fun(DeliveryTag, Sub, Requeue, State),
			    ok(State1);
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
                                        user = #user{username = Username},
                                        queue_states  = QueueStates0}) ->
    case maps:find(ConsumerTag, Subs) of
        error ->
            error("No subscription found",
                  "UNSUBSCRIBE must refer to an existing subscription.~n"
                  "Subscription to ~tp not found.~n",
                  [Description],
                  State);
        {ok, Subscription = #subscription{queue_name = Queue}} ->

            case rabbit_misc:with_exit_handler(
                   fun () -> {error, not_found} end,
                   fun () ->
                           %% default NoWait is false, so was the basic.cancel here
                           %% however there is no cancel.ok in the STOMP world
                           %% so OkMsg is undefined
                            rabbit_amqqueue:with_or_die(
                              Queue,
                              fun(Q1) ->
                                      rabbit_queue_type:cancel(
                                        Q1, ConsumerTag, undefined,
                                        Username, QueueStates0)
                              end)
                   end) of
                {ok, QueueStates} ->
                    %% rabbit_global_counters:consumer_deleted('STOMP'),

                    {ok, _, NewState} = tidy_canceled_subscription(ConsumerTag, Subscription,
                                                                   Frame, State#proc_state{queue_states = QueueStates}),
                    {ok, NewState};
                {error, not_found} ->
                    %% rabbit_global_counters:consumer_deleted('STOMP'),

                    {ok, _, NewState} = tidy_canceled_subscription(ConsumerTag, Subscription,
                                                                   Frame, State),
                    {ok, NewState}
            end
    end.

%% Server-initiated cancelations will pass an undefined instead of a
%% STOMP frame. In this case we know that the queue was deleted and
%% thus we don't have to clean it up.
tidy_canceled_subscription(ConsumerTag, Subscription,
                           undefined, State) ->
    tidy_canceled_subscription_state(ConsumerTag, Subscription, State);

%% Client-initiated cancelations will pass an actual frame
tidy_canceled_subscription(ConsumerTag, Subscription = #subscription{dest_hdr = DestHdr},
                           Frame, State0) ->
    {ok, State1} = tidy_canceled_subscription_state(ConsumerTag, Subscription, State0),
    {ok, Dest} = parse_endpoint(DestHdr),
    maybe_delete_durable_sub_queue(Dest, Frame, State1).

tidy_canceled_subscription_state(ConsumerTag,
                                 _Subscription = #subscription{queue_name = QName},
                                 State = #proc_state{subscriptions = Subs,
                                                     queue_consumers = QCons}) ->
    Subs1 = maps:remove(ConsumerTag, Subs),
    QCons1 =
        case maps:find(QName, QCons) of
            error       -> QCons;
            {ok, CTags} -> CTags1 = gb_sets:delete(ConsumerTag, CTags),
                           case gb_sets:is_empty(CTags1) of
                               true  -> maps:remove(QName, QCons);
                               false -> maps:put(QName, CTags1, QCons)
                           end
        end,
    {ok, State#proc_state{subscriptions = Subs1,
                          queue_consumers = QCons1}}.

maybe_delete_durable_sub_queue({topic, Name}, Frame,
                         State = #proc_state{auth_login = Username,
                                             vhost = VHost}) ->
    case rabbit_stomp_util:has_durable_header(Frame) of
        true ->
            {ok, Id} = rabbit_stomp_frame:header(Frame, ?HEADER_ID),
            QName = rabbit_stomp_util:subscription_queue_name(Name, Id, Frame),
            QRes = rabbit_misc:r(VHost, queue, list_to_binary(QName)),
            io:format("Durable QRes: ~p~n", [QRes]),
            delete_queue(QRes, Username),
            ok(State);
        false ->
            ok(State)
    end;
maybe_delete_durable_sub_queue(_Destination, _Frame, State) ->
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

server_header() ->
    {ok, Product} = application:get_key(rabbit, description),
    {ok, Version} = application:get_key(rabbit, vsn),
    rabbit_misc:format("~ts/~ts", [Product, Version]).

do_subscribe(Destination, DestHdr, Frame,
             State0 = #proc_state{subscriptions = Subs,
                                  default_topic_exchange = DfltTopicEx,
                                  queue_consumers = QCons}) ->
    check_subscription_access(Destination, State0),

    {ok, {_Global, DefaultPrefetch}} = application:get_env(rabbit, default_consumer_prefetch),
    Prefetch =
        rabbit_stomp_frame:integer_header(Frame, ?HEADER_PREFETCH_COUNT, DefaultPrefetch),
    %% io:format("Prefetch: ~p~n", [Prefetch]),
    {AckMode, IsMulti} = rabbit_stomp_util:ack_mode(Frame),
    case ensure_endpoint(source, Destination, Frame, State0) of
        {ok, QueueName, State} ->
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
                        {ok, State1} = consume_queue(QueueName, #{no_ack => (AckMode == auto),
                                                                  prefetch_count => Prefetch,
                                                                  consumer_tag => ConsumerTag,
                                                                  exclusive_consume => false,
                                                                  args => Arguments},
                                                     State),
                        ok = ensure_binding(QueueName, ExchangeAndKey, State1),
                        CTags1 = case maps:find(QueueName, QCons) of
                                     {ok, CTags} -> gb_sets:insert(ConsumerTag, CTags);
                                     error -> gb_sets:singleton(ConsumerTag)
                                 end,
                        QCons1 = maps:put(QueueName, CTags1, QCons),
                        ok(State1#proc_state{subscriptions = maps:put(
                                                               ConsumerTag,
                                                               #subscription{dest_hdr    = DestHdr,
                                                                             ack_mode    = AckMode,
                                                                             multi_ack   = IsMulti,
                                                                             description = Description,
                                                                             queue_name  = QueueName},
                                                               Subs),
                                             queue_consumers = QCons1})
                    catch exit:Err ->
                            %% it's safe to delete this queue, it
                            %% was server-named and declared by us
                            case Destination of
                                {exchange, _} ->
                                    ok = maybe_clean_up_queue(QueueName, State);
                                {topic, _} ->
                                    ok = maybe_clean_up_queue(QueueName, State);
                                _ ->
                                    ok
                            end,
                            exit(Err)
                    end
            end;
        {error, _} = Err ->
            Err
    end.

check_subscription_access(Destination = {topic, _Topic},
                          #proc_state{user = #user{username = Username} = User,
                                      default_topic_exchange = DfltTopicEx,
                                      vhost = VHost}) ->
    {Exchange, RoutingKey} = parse_routing(Destination, DfltTopicEx),
    Resource = #resource{virtual_host = VHost,
        kind = topic,
        name = rabbit_data_coercion:to_binary(Exchange)},
    Context = #{routing_key  => rabbit_data_coercion:to_binary(RoutingKey),
                variable_map => #{<<"vhost">> => VHost, <<"username">> => Username}
    },
    rabbit_access_control:check_topic_access(User, Resource, read, Context);
check_subscription_access(_, _) ->
    authorized.

maybe_clean_up_queue(Queue, #proc_state{auth_login = Username}) ->
    catch delete_queue(Queue, Username),
    ok.

do_send(Destination, _DestHdr,
        Frame = #stomp_frame{body_iolist = BodyFragments},
        State0 = #proc_state{default_topic_exchange = DfltTopicEx,
                             delivery_flow = Flow,
                             user = User,
                             authz_context = AuthzCtx,
                             vhost = VHost}) ->
    case ensure_endpoint(dest, Destination, Frame, State0) of

        {ok, _Q, State} ->
            {Frame1, State1} =
                ensure_reply_to(Frame, State),

            Props = rabbit_stomp_util:message_properties(Frame1),

            {ExchangeNameList, RoutingKeyList} = parse_routing(Destination, DfltTopicEx),
            %% io:format("Parse_routing: ~p~n", [{ExchangeNameList, RoutingKeyList}]),
            RoutingKey = list_to_binary(RoutingKeyList),


            rabbit_global_counters:messages_received(stomp, 1),

            ExchangeName = rabbit_misc:r(VHost, exchange, list_to_binary(ExchangeNameList)),
            check_resource_access(User, ExchangeName, write, AuthzCtx),
            Exchange = rabbit_exchange:lookup_or_die(ExchangeName),
            check_internal_exchange(Exchange),
            check_topic_authorisation(Exchange, User, RoutingKey, AuthzCtx, write),

            {DeliveryOptions, _MsgSeqNo, State2} =
                case rabbit_stomp_frame:header(Frame, ?HEADER_RECEIPT) of
                    not_found ->
                        {maps_put_truthy(flow, Flow, #{}), undefined, State1};
                    {ok, Id} ->
                        rabbit_global_counters:messages_received_confirm(stomp, 1),
                        SeqNo = State1#proc_state.msg_seq_no,
                        %% I think it's safe to just add it here because
                        %% if there is an error down the road process dies
                        StateRR = record_receipt(true, SeqNo, Id, State1),
                        Opts = maps_put_truthy(flow, Flow, #{correlation => SeqNo}),
                        {Opts, SeqNo, StateRR#proc_state{msg_seq_no = SeqNo + 1}}
                end,

            {ClassId, _MethodId} = rabbit_framing_amqp_0_9_1:method_id('basic.publish'),

            Content0 = #content{
                          class_id = ClassId,
                          properties = Props,
                          properties_bin = none,
                          protocol = none,
                          payload_fragments_rev = [BodyFragments]
                         },

            Message0 = mc_amqpl:message(ExchangeName, RoutingKey, Content0),

            Message = rabbit_message_interceptor:intercept(Message0),

            %% {ok, BasicMessage} = rabbit_basic:message(ExchangeName, RoutingKey, Content),

            %% Delivery = #delivery{
            %%               mandatory = false,
            %%               confirm = DoConfirm,
            %%               sender = self(),
            %%               message = BasicMessage,
            %%               msg_seq_no = MsgSeqNo,
            %%               flow = Flow
            %%              },
            QNames = rabbit_exchange:route(Exchange, Message, #{return_binding_keys => true}),
            %% io:format("QNames ~p~n", [QNames]),

            Delivery = {Message, DeliveryOptions, QNames},
            %% io:format("Delivery: ~p~n", [Delivery]),
            deliver_to_queues(ExchangeName, Delivery, State2);
        {error, _} = Err ->
            %% io:format("Err ~p~n", [Err]),
            Err
    end.

deliver_to_queues(_XName,
                  {_Message, Options, _RoutedToQueues = []},
                  State)
  when not is_map_key(correlation, Options) -> %% optimisation when there are no queues
    %%?INCR_STATS(exchange_stats, XName, 1, publish, State),
    rabbit_global_counters:messages_unroutable_dropped(stomp, 1),
    %%?INCR_STATS(exchange_stats, XName, 1, drop_unroutable, State),
    {ok, State};

deliver_to_queues(XName,
                  {Message, Options, RoutedToQNames},
                  State0 = #proc_state{queue_states = QStates0}) ->
    Qs0 = rabbit_amqqueue:lookup_many(RoutedToQNames),
    Qs = rabbit_amqqueue:prepend_extra_bcc(Qs0),
    MsgSeqNo = maps:get(correlation, Options, undefined),
    %% io:format("Qs: ~p~n", [Qs]),
    case rabbit_queue_type:deliver(Qs, Message, Options, QStates0) of
        {ok, QStates, Actions} ->
            rabbit_global_counters:messages_routed(stomp, length(Qs)),
            QueueNames = rabbit_amqqueue:queue_names(Qs),
            State1 = process_routing_confirm(MsgSeqNo, QueueNames, XName, State0),
            %% Actions must be processed after registering confirms as actions may
            %% contain rejections of publishes.
            {ok, handle_queue_actions(Actions, State1#proc_state{queue_states = QStates})};
        {error, Reason} ->
            log_error("Failed to deliver message with packet_id=~p to queues: ~p",
                      [MsgSeqNo, Reason], none),
            {error, Reason, State0}
    end.


record_rejects([], State) ->
    State;
record_rejects(MXs, State = #proc_state{rejected = R%% , tx = Tx
                                       }) ->
    %% Tx1 = case Tx of
    %%     none -> none;
    %%     _    -> failed
    %% end,
    State#proc_state{rejected = [MXs | R]%% , tx = Tx1
                    }.

record_confirms([], State) ->
    State;
record_confirms(MXs, State = #proc_state{confirmed = C}) ->
    State#proc_state{confirmed = [MXs | C]}.

process_routing_confirm(undefined, _, _, State) ->
    State;
process_routing_confirm(MsgSeqNo, [], XName, State) ->
    record_confirms([{MsgSeqNo, XName}], State);
process_routing_confirm(MsgSeqNo, QRefs, XName, State) ->
    State#proc_state{unconfirmed =
        rabbit_confirms:insert(MsgSeqNo, QRefs, XName, State#proc_state.unconfirmed)}.

confirm(MsgSeqNos, QRef, State = #proc_state{unconfirmed = UC}) ->
    %% NOTE: if queue name does not exist here it's likely that the ref also
    %% does not exist in unconfirmed messages.
    %% Neither does the 'ignore' atom, so it's a reasonable fallback.
    {ConfirmMXs, UC1} = rabbit_confirms:confirm(MsgSeqNos, QRef, UC),
    %% NB: don't call noreply/1 since we don't want to send confirms.
    record_confirms(ConfirmMXs, State#proc_state{unconfirmed = UC1}).

send_confirms_and_nacks(State = #proc_state{%% tx = none,
                                            confirmed = [], rejected = []}) ->
    State;
send_confirms_and_nacks(State = #proc_state{%% tx = none,
                                            confirmed = C, rejected = R}) ->
    case rabbit_node_monitor:pause_partition_guard() of
        ok      ->
            Confirms = lists:append(C),
            %% rabbit_global_counters:messages_confirmed('STOMP', length(Confirms)),
            Rejects = lists:append(R),
            ConfirmMsgSeqNos =
                lists:foldl(
                    fun ({MsgSeqNo, _XName}, MSNs) ->
                        %% ?INCR_STATS(exchange_stats, XName, 1, confirm, State), %% TODO: what to do with stats
                        [MsgSeqNo | MSNs]
                    end, [], Confirms),
            RejectMsgSeqNos = [MsgSeqNo || {MsgSeqNo, _} <- Rejects],

            State1 = send_confirms(ConfirmMsgSeqNos,
                                   RejectMsgSeqNos,
                                   State#proc_state{confirmed = []}),
            %% TODO: we don't have server-originated nacks in STOMP unfortunately
            %% TODO: msg seq nos, same as for confirms. Need to implement
            %% nack rates first.
            %% send_nacks(RejectMsgSeqNos,
            %%            ConfirmMsgSeqNos,
            %%            State1#proc_state{rejected = []});
            State1#proc_state{rejected = []};
        pausing -> State
    end.

%% TODO: in stomp we can only ERROR, there is no commit_ok :-(
%% send_confirms_and_nacks(State) ->
%%     case rabbit_node_monitor:pause_partition_guard() of
%%         ok      -> maybe_complete_tx(State);
%%         pausing -> State
%%     end
%%        .

%% TODO: in stomp there is no nacks, only ERROR, shall I send error here??
%% send_nacks([], _, State) ->
%%     State;
%% send_nacks(_Rs, _, State = #ch{cfg = #conf{state = closing}}) -> %% optimisation
%%     State;
%% send_nacks(Rs, Cs, State) ->
%%     coalesce_and_send(Rs, Cs,
%%                       fun(MsgSeqNo, Multiple) ->
%%                               #'basic.nack'{delivery_tag = MsgSeqNo,
%%                                             multiple     = Multiple}
%%                       end, State).

send_confirms([], _, State) ->
    State;
%% TODO: implement connection states
%% send_confirms(_Cs, _, State = #ch{cfg = #conf{state = closing}}) -> %% optimisation
%%     State;
send_confirms([MsgSeqNo], _, State) ->
    State1 = flush_pending_receipts(MsgSeqNo, false, State),
    State1;
send_confirms(Cs, Rs, State) ->
    coalesce_and_send(Cs, Rs,
                      fun(MsgSeqNo, Multiple, StateN) ->
                              flush_pending_receipts(MsgSeqNo, Multiple, StateN)
                      end, State).

coalesce_and_send(MsgSeqNos, NegativeMsgSeqNos, MkMsgFun, State = #proc_state{unconfirmed = UC}) ->
    SMsgSeqNos = lists:usort(MsgSeqNos),
    UnconfirmedCutoff = case rabbit_confirms:is_empty(UC) of
                 true  -> lists:last(SMsgSeqNos) + 1;
                 false -> rabbit_confirms:smallest(UC)
             end,
    Cutoff = lists:min([UnconfirmedCutoff | NegativeMsgSeqNos]),
    {Ms, Ss} = lists:splitwith(fun(X) -> X < Cutoff end, SMsgSeqNos),
    State1 = case Ms of
                 [] -> State;
                 _  -> MkMsgFun(lists:last(Ms), true, State)
             end,
    lists:foldl(fun(SeqNo, StateN) ->
                        MkMsgFun(SeqNo, false, StateN)
                end, State1, Ss).

%% ack_len(Acks) -> lists:sum([length(L) || {ack, L} <- Acks]).

handle_ack(DeliveryTag, #subscription{multi_ack = IsMulti}, _, State = #proc_state{unacked_message_q = UAMQ}) ->
    %% io:format("UAMQ ~p~n", [UAMQ]),
    {Acked, Remaining} = collect_acks(UAMQ, DeliveryTag, IsMulti),
    %% io:format("Acked ~p~n, Remaining ~p~n", [Acked, Remaining]),
    State1 = State#proc_state{unacked_message_q = Remaining},
    {State2, Actions} = settle_acks(Acked, State1),
    handle_queue_actions(Actions, State2).

handle_nack(DeliveryTag, #subscription{multi_ack = IsMulti}, Requeue, State = #proc_state{unacked_message_q = UAMQ}) ->
    %% io:format("UAMQ ~p~n", [UAMQ]),
    {Acked, Remaining} = collect_acks(UAMQ, DeliveryTag, IsMulti),
    %% io:format("Acked ~p~n, Remaining ~p~n", [Acked, Remaining]),
    State1 = State#proc_state{unacked_message_q = Remaining},
    {State2, Actions} = internal_reject(Requeue, Acked, State1),
    handle_queue_actions(Actions, State2).

%% Records a client-sent acknowledgement. Handles both single delivery acks
%% and multi-acks.
%%
%% Returns a tuple of acknowledged pending acks and remaining pending acks.
%% Sorts each group in the youngest-first order (descending by delivery tag).
collect_acks(UAMQ, DeliveryTag, Multiple) ->
    collect_acks([], [], UAMQ, DeliveryTag, Multiple).

collect_acks(AcknowledgedAcc, RemainingAcc, UAMQ, DeliveryTag, Multiple) ->
    case ?QUEUE:out(UAMQ) of
        {{value, UnackedMsg = #pending_ack{delivery_tag = CurrentDT}},
         UAMQTail} ->
            if CurrentDT == DeliveryTag ->
                   {[UnackedMsg | AcknowledgedAcc],
                    case RemainingAcc of
                        [] -> UAMQTail;
                        _  -> ?QUEUE:join(
                                 ?QUEUE:from_list(lists:reverse(RemainingAcc)),
                                 UAMQTail)
                    end};
               Multiple ->
                    collect_acks([UnackedMsg | AcknowledgedAcc], RemainingAcc,
                                 UAMQTail, DeliveryTag, Multiple);
               true ->
                    collect_acks(AcknowledgedAcc, [UnackedMsg | RemainingAcc],
                                 UAMQTail, DeliveryTag, Multiple)
            end;
        {empty, _} ->
            error("Unknown delivery tag",
                  "unknown delivery tag ~w", [DeliveryTag])
    end.

%% foreach_per_queue(_F, [], Acc) ->
%%     Acc;
foreach_per_queue(F, [#pending_ack{tag = CTag,
                                   queue = QName,
                                   msg_id = MsgId}], Acc) ->
    %% quorum queue, needs the consumer tag
    F({QName, CTag}, [MsgId], Acc);
foreach_per_queue(F, UAL, Acc) ->
    T = lists:foldl(fun (#pending_ack{tag = CTag,
                                      queue = QName,
                                      msg_id = MsgId}, T) ->
                            rabbit_misc:gb_trees_cons({QName, CTag}, MsgId, T)
                    end, gb_trees:empty(), UAL),
    rabbit_misc:gb_trees_fold(fun (Key, Val, Acc0) -> F(Key, Val, Acc0) end, Acc, T).

settle_acks(Acks, State = #proc_state{queue_states = QueueStates0}) ->
    {QueueStates, Actions} =
        foreach_per_queue(
          fun ({QRef, CTag}, MsgIds, {Acc0, ActionsAcc0}) ->
                  case rabbit_queue_type:settle(QRef, complete, CTag,
                                                MsgIds, Acc0) of
                      {ok, Acc, ActionsAcc} ->
                          %% incr_queue_stats(QRef, MsgIds, State),
                          {Acc, ActionsAcc0 ++ ActionsAcc};
                      {protocol_error, ErrorType, Reason, ReasonArgs} ->
                          rabbit_misc:protocol_error(ErrorType, Reason, ReasonArgs)
                  end
          end, Acks, {QueueStates0, []}),
    {State#proc_state{queue_states = QueueStates}, Actions}.

%% NB: Acked is in youngest-first order
internal_reject(Requeue, Acked,
                State = #proc_state{queue_states = QueueStates0}) ->
    {QueueStates, Actions} =
        foreach_per_queue(
          fun({QRef, CTag}, MsgIds, {Acc0, Actions0}) ->
                  Op = case Requeue of
                           false -> discard;
                           true -> requeue
                       end,
                  case rabbit_queue_type:settle(QRef, Op, CTag, MsgIds, Acc0) of
                      {ok, Acc, Actions} ->
                          {Acc, Actions0 ++ Actions};
                      {protocol_error, ErrorType, Reason, ReasonArgs} ->
                          rabbit_misc:protocol_error(ErrorType, Reason, ReasonArgs)
                  end
          end, Acked, {QueueStates0, []}),
    {State#proc_state{queue_states = QueueStates}, Actions}.

negotiate_version(Frame) ->
    ClientVers = re:split(rabbit_stomp_frame:header(
                            Frame, ?HEADER_ACCEPT_VERSION, "1.0"),
                          ",", [{return, list}]),
    rabbit_stomp_util:negotiate_version(ClientVers, ?SUPPORTED_VERSIONS).


deliver_to_client(ConsumerTag, Ack, Msgs, State) ->
    lists:foldl(fun(Msg, S) ->
                       deliver_one_to_client(ConsumerTag, Ack, Msg, S)
                end, State, Msgs).

deliver_one_to_client(ConsumerTag, _Ack, {QName, QPid, MsgId, Redelivered, MsgCont0} = _Msg,
                      State = #proc_state{queue_states = QStates,
                                          delivery_tag = DeliveryTag}) ->

    [RoutingKey | _] = mc:get_annotation(routing_keys, MsgCont0),
    ExchangeNameBin = mc:get_annotation(exchange, MsgCont0),
    MsgCont = mc:convert(mc_amqpl, MsgCont0),
    Content = mc:protocol_state(MsgCont),
    Delivery = #'basic.deliver'{consumer_tag = ConsumerTag,
                                delivery_tag = DeliveryTag,
                                redelivered  = Redelivered,
                                exchange     = ExchangeNameBin,
                                routing_key  = RoutingKey},


    {Props, Payload} = rabbit_basic_common:from_content(Content),


    DeliveryCtx = case rabbit_queue_type:module(QName, QStates) of
                     {ok, rabbit_classic_queue} ->
                         {ok, QPid, ok};
                     _ -> undefined
                 end,

    State1 = send_delivery(QName, MsgId, Delivery, Props, Payload, DeliveryCtx, State),

    State1#proc_state{delivery_tag = DeliveryTag + 1}.


send_delivery(QName, MsgId, Delivery = #'basic.deliver'{consumer_tag = ConsumerTag,
                                                        delivery_tag = DeliveryTag},
              Properties, Body, DeliveryCtx,
              State = #proc_state{
                         session_id  = SessionId,
                         subscriptions = Subs,
                         version       = Version,
                         unacked_message_q = UAMQ}) ->
    %% io:format("SD Subs ~p~n", [Subs]),
    case maps:find(ConsumerTag, Subs) of
        {ok, #subscription{ack_mode = AckMode}} ->
            NewState = send_frame(
                          "MESSAGE",
                          rabbit_stomp_util:headers(SessionId, Delivery, Properties,
                                                    AckMode, Version),
                         Body,
                          State),
            maybe_notify_sent(DeliveryCtx),
            case AckMode of
                client ->
                    DeliveredAt = os:system_time(millisecond),
                    %% io:format("Send delivery state: ~p~n", [NewState#proc_state{unacked_message_q =
                    %%                                                                 ?QUEUE:in(#pending_ack{delivery_tag = DeliveryTag,
                    %%                                                                                        tag = ConsumerTag,
                    %%                                                                                        delivered_at = DeliveredAt,
                    %%                                                                                        queue = QName,
                    %%                                                                                        msg_id = MsgId}, UAMQ)}]),
                    NewState#proc_state{unacked_message_q =
                                            ?QUEUE:in(#pending_ack{delivery_tag = DeliveryTag,
                                                                   tag = ConsumerTag,
                                                                   delivered_at = DeliveredAt,
                                                                   queue = QName,
                                                                   msg_id = MsgId}, UAMQ)};
                _ -> NewState
            end;
        error ->
            send_error("Subscription not found",
                       "There is no current subscription with tag '~ts'.",
                       [ConsumerTag],
                       State)
    end.

maybe_notify_sent(undefined) ->
    ok;
maybe_notify_sent({_, QPid, _}) ->
       ok = rabbit_amqqueue:notify_sent(QPid, self()).

close_connection(State) ->
    %% TODO: I feel like there has to be a cleanup,
    %% maybe delete queues we created?
    %% notify queues that subsriptions are shutdown?
    %% (they probably monitor self() after consume anyway)
    State.

%% close_connection(State = #proc_state{connection = none}) ->
%%     State;
%% %% Closing the connection will close the channel and subchannels
%% close_connection(State = #proc_state{connection = Connection}) ->
%%     %% ignore noproc or other exceptions to avoid debris
%%     catch amqp_connection:close(Connection),
%%     State#proc_state{channel = none, connection = none, subscriptions = none};
%% close_connection(undefined) ->
%%     rabbit_log:debug("~ts:close_connection: undefined state", [?MODULE]),
%%     #proc_state{channel = none, connection = none, subscriptions = none}.

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

record_receipt(_DoConfirm = true, MsgSeqNo, ReceiptId, State = #proc_state{pending_receipts = PR}) ->
    State#proc_state{pending_receipts = gb_trees:insert(MsgSeqNo, ReceiptId, PR)}.

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
              Frame,
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

extend_transaction(Transaction, Fun, Frame, State0) ->
    with_transaction(
      Transaction, State0,
      fun (Funs, State) ->
              put({transaction, Transaction}, [{Frame, Fun} | Funs]),
              ok(State)
      end).

commit_transaction(Transaction, State0) ->
    with_transaction(
      Transaction, State0,
      fun (Funs, State) ->
              FinalState = lists:foldr(fun perform_transaction_action/2,
                                       {ok, State},
                                       Funs),
              erase({transaction, Transaction}),
              FinalState
      end).

abort_transaction(Transaction, State0) ->
    with_transaction(
      Transaction, State0,
      fun (_Frames, State) ->
              erase({transaction, Transaction}),
              ok(State)
      end).

perform_transaction_action(_, {stop, _, _} = Res) ->
    Res;
perform_transaction_action({Frame, Fun}, {ok, State}) ->
    process_request(
      Fun,
      fun(StateM) -> ensure_receipt(Frame, StateM) end,
      State).

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

create_queue(_State = #proc_state{authz_context = AuthzCtx,
                                  user = #user{username = Username} = User,
                                  vhost = VHost}) ->
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

delete_queue(QRes, Username) ->
    case rabbit_amqqueue:with(
           QRes,
           fun (Q) ->
                   io:format("Delete queue ~p~n", [rabbit_queue_type:delete(Q, false, false, Username)])
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

ensure_binding(#resource{name = QueueBin}, {"", Queue}, _State) ->
    %% i.e., we should only be asked to bind to the default exchange a
    %% queue with its own name
    QueueBin = list_to_binary(Queue),
    ok;
ensure_binding(QName, {Exchange, RoutingKey}, _State = #proc_state{auth_login = Username,
                                                                   vhost = VHost}) ->
    Binding = #binding{source = rabbit_misc:r(VHost, exchange, list_to_binary(Exchange)),
                       destination = QName,
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
            rabbit_access_control:check_resource_access(User, Resource, Perm, Context),
            CacheTail = lists:sublist(Cache, ?MAX_PERMISSION_CACHE_SIZE-1),
            put(permission_cache, [V | CacheTail]),
            ok
    end.

do_native_login(Creds, Heartbeat, Version,  State = #proc_state{peer_addr = Addr,
                                                                vhost = VHost}) ->
    {Username, AuthProps} = case Creds of
                                {ssl, {Username0, none}}-> {Username0, []};
                                {_, {Username0, Password}} -> {Username0, [{password, Password},
                                                                           {vhost, VHost}]}
                            end,

    maybe
        {ok, User} ?= rabbit_access_control:check_user_login(Username, AuthProps),
        {ok, AuthzCtx} ?= check_vhost_access(VHost, User, Addr),
        ok ?= check_user_loopback(Username, Addr),
        rabbit_core_metrics:auth_attempt_succeeded(Addr, Username, stomp),
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
                            version    = Version,
                            user = User,
                            authz_context = AuthzCtx})
    else
        {error, not_allowed} ->
            rabbit_log:warning("STOMP login failed for user '~ts': "
                               "virtual host access not allowed", [Username]),
            error("Bad CONNECT", "Virtual host '" ++
                      binary_to_list(VHost) ++
                      "' access denied", State);
        {refused, Username1, _Msg, _Args} ->
            rabbit_log:warning("STOMP login failed for user '~ts': authentication failed", [Username1]),
            error("Bad CONNECT", "Access refused for user '" ++
                      binary_to_list(Username1) ++ "'", [], State);
        {error, not_loopback} ->
            rabbit_log:warning("STOMP login failed for user '~ts': "
                               "this user's access is restricted to localhost", [Username]),
            error("Bad CONNECT", "non-loopback access denied", State)
    end.

handle_down({{'DOWN', QName}, _MRef, process, QPid, Reason},
            State0 =  #proc_state{queue_states  = QStates0} = State) ->
    credit_flow:peer_down(QPid),
    case rabbit_queue_type:handle_down(QPid, QName, Reason, QStates0) of
        {ok, QStates1, Actions} ->
            State1 = State0#proc_state{queue_states = QStates1},
            State2 = handle_queue_actions(Actions, State1),
            {ok, State2};
        {eol, QStates1, QRef} ->
            State1 = handle_consuming_queue_down_or_eol(QRef, State#proc_state{queue_states = QStates1}),
            {ConfirmMXs, UC1} =
                rabbit_confirms:remove_queue(QRef, State1#proc_state.unconfirmed),
            State2 = record_confirms(ConfirmMXs,
                                     State1#proc_state{unconfirmed = UC1}),
            _ = erase_queue_stats(QRef),
            {ok, State2#proc_state{queue_states = rabbit_queue_type:remove(QRef, State2#proc_state.queue_states)}}
    end.

handle_queue_event({queue_event, QRef, Evt}, #proc_state{queue_states  = QStates0} = State) ->
    %% io:format("Event: ~p~n", [Evt]),
    %% io:format("QStates: ~p~n", [QStates0]),
    case rabbit_queue_type:handle_event(QRef, Evt, QStates0) of
        {ok, QState1, Actions} ->
            %% io:format("ActionsEv ~p~n", [Actions]),
            State1 = State#proc_state{queue_states = QState1},
            State2 = handle_queue_actions(Actions, State1),
            {ok, State2};
        {eol, Actions} ->
            State1 = handle_queue_actions(Actions, State),
            State2 = handle_consuming_queue_down_or_eol(QRef, State1),
            {ConfirmMXs, UC1} =
                rabbit_confirms:remove_queue(QRef, State1#proc_state.unconfirmed),
            %% Deleted queue is a special case.
            %% Do not nack the "rejected" messages.
            State3 = record_confirms(ConfirmMXs,
                                     State2#proc_state{unconfirmed = UC1}),
            {ok, State3#proc_state{queue_states = rabbit_queue_type:remove(QRef, QStates0)}};
        {protocol_error, Type, Reason, ReasonArgs} = Error ->
            log_error(Type, Reason, ReasonArgs),
            {error, Error, State}
    end.

handle_queue_actions(Actions, #proc_state{} = State0) ->
    %% io:format("Actions: ~p~n", [Actions]),
    lists:foldl(
      fun ({deliver, ConsumerTag, Ack, Msgs}, S) ->
              deliver_to_client(ConsumerTag, Ack, Msgs, S);
          ({settled, QRef, MsgSeqNos}, S0) ->
              S = confirm(MsgSeqNos, QRef, S0),
              send_confirms_and_nacks(S);
          ({rejected, _QRef, MsgSeqNos}, S0) ->
              {U, Rej} =
              lists:foldr(
                fun(SeqNo, {U1, Acc}) ->
                        case rabbit_confirms:reject(SeqNo, U1) of
                            {ok, MX, U2} ->
                                {U2, [MX | Acc]};
                            {error, not_found} ->
                                {U1, Acc}
                        end
                end, {S0#proc_state.unconfirmed, []}, MsgSeqNos),
              S = S0#proc_state{unconfirmed = U},
              %% Don't send anything, no nacks in STOMP
              record_rejects(Rej, S);
          ({queue_down, QRef}, S0) ->
              handle_consuming_queue_down_or_eol(QRef, S0);
          %% TODO: I have no idea about the scope of credit_flow
          ({block, QName}, S0) ->
              credit_flow:block(QName),
              S0;
          ({unblock, QName}, S0) ->
              credit_flow:unblock(QName),
              S0;
          %% TODO: in rabbit_channel there code for handling
          %% send_drained and send_credit_reply
          %% I'm doing catch all here to not crash?
          (_, S0) ->
              S0
      end, State0, Actions).



parse_endpoint(undefined) ->
    parse_endpoint("/queue");
parse_endpoint(Destination) when is_binary(Destination) ->
    parse_endpoint(unicode:characters_to_list(Destination));
parse_endpoint(Destination) when is_list(Destination) ->
    case re:split(Destination, "/", [{return, list}]) of
        [Name] ->
            {ok, {queue, unescape(Name)}};
        ["", Type | Rest]
            when Type =:= "exchange" orelse Type =:= "queue" orelse
                 Type =:= "topic"    orelse Type =:= "temp-queue" ->
            parse_endpoint0(atomise(Type), Rest);
        ["", "amq", "queue" | Rest] ->
            parse_endpoint0(amqqueue, Rest);
        ["", "reply-queue" = Prefix | [_|_]] ->
            parse_endpoint0(reply_queue,
                            [lists:nthtail(2 + length(Prefix), Destination)]);
        _ ->
            {error, {unknown_destination, Destination}}
    end.

parse_endpoint0(exchange, ["" | _] = Rest) ->
    {error, {invalid_destination, exchange, to_url(Rest)}};
parse_endpoint0(exchange, [Name]) ->
    {ok, {exchange, {unescape(Name), undefined}}};
parse_endpoint0(exchange, [Name, Pattern]) ->
    {ok, {exchange, {unescape(Name), unescape(Pattern)}}};
parse_endpoint0(queue,    []) ->
    {error, {invalid_destination, queue, []}};
parse_endpoint0(Type,     [[_|_]] = [Name]) ->
    {ok, {Type, unescape(Name)}};
parse_endpoint0(Type,     Rest) ->
    {error, {invalid_destination, Type, to_url(Rest)}}.

%% --------------------------------------------------------------------------

util_ensure_endpoint(source, {exchange, {Name, _}}, Params, State = #proc_state{vhost = VHost}) ->
    ExchangeName = rabbit_misc:r(Name, exchange, VHost),
    check_exchange(ExchangeName, proplists:get_value(check_exchange, Params, false)),
    Amqqueue = new_amqqueue(undefined, exchange, Params, State),
    {ok, Queue} = create_queue(Amqqueue, State),
    {ok, amqqueue:get_name(Queue), State};

util_ensure_endpoint(source, {topic, _}, Params, State) ->
    Amqqueue = new_amqqueue(undefined, topic, Params, State),
    {ok, Queue} = create_queue(Amqqueue, State),
    {ok, amqqueue:get_name(Queue), State};

util_ensure_endpoint(_Dir, {queue, undefined}, _Params, State) ->
    {ok, undefined, State};

util_ensure_endpoint(_, {queue, Name}, Params, State=#proc_state{route_state = RoutingState,
                                                                 vhost = VHost}) ->
    Params1 = rabbit_misc:pmerge(durable, true, Params),
    QueueNameBin = list_to_binary(Name),
    RState1 = case sets:is_element(Params1, RoutingState) of
                  true -> State;
                  _    -> Amqqueue = new_amqqueue(QueueNameBin, queue, Params1, State),
                          {ok, Queue} = create_queue(Amqqueue, State),
                          #resource{name = QNameBin} = amqqueue:get_name(Queue),
                          sets:add_element(QNameBin, RoutingState)
              end,
    {ok,  rabbit_misc:r(VHost, queue, QueueNameBin), State#proc_state{route_state = RState1}};

util_ensure_endpoint(dest, {exchange, {Name, _}}, Params, State = #proc_state{vhost = VHost}) ->
    ExchangeName = rabbit_misc:r(Name, exchange, VHost),
    check_exchange(ExchangeName, proplists:get_value(check_exchange, Params, false)),
    {ok, undefined, State};

util_ensure_endpoint(dest, {topic, _}, _Params, State) ->
    {ok, undefined, State};

util_ensure_endpoint(_, {amqqueue, Name}, _Params, State = #proc_state{vhost = VHost}) ->
  {ok, rabbit_misc:r(VHost, queue, list_to_binary(Name)), State};

util_ensure_endpoint(_, {reply_queue, Name}, _Params, State = #proc_state{vhost = VHost}) ->
  {ok, rabbit_misc:r(VHost, queue, list_to_binary(Name)), State};

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

new_amqqueue(QNameBin0, Type, Params0, _State = #proc_state{user = #user{username = Username},
                                                            vhost = VHost}) ->
    QNameBin = case  {Type, proplists:get_value(subscription_queue_name_gen, Params0)} of
                   {topic, SQNG} when is_function(SQNG) ->
                      SQNG();
                   {exchange, SQNG} when is_function(SQNG) ->
                       SQNG();
                   _ ->
                       QNameBin0
               end,
    QName = rabbit_misc:r(VHost, queue, QNameBin),
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
                 VHost,
                 #{user => Username}).


to_url([])  -> [];
to_url(Lol) -> "/" ++ string:join(Lol, "/").

atomise(Name) when is_list(Name) ->
    list_to_atom(re:replace(Name, "-", "_", [{return,list}, global])).

unescape(Str) -> unescape(Str, []).

unescape("%2F" ++ Str, Acc) -> unescape(Str, [$/ | Acc]);
unescape([C | Str],    Acc) -> unescape(Str, [C | Acc]);
unescape([],           Acc) -> lists:reverse(Acc).


consume_queue(QRes, Spec0, State = #proc_state{user = #user{username = Username} = User,
                                                   authz_context = AuthzCtx,
                                                   queue_states  = QStates0})->
    check_resource_access(User, QRes, read, AuthzCtx),
    Spec = Spec0#{channel_pid => self(),
                  limiter_pid => none,
                  limiter_active => false,
                  ok_msg => undefined,
                  acting_user => Username},
    rabbit_amqqueue:with_or_die(
      QRes,
      fun(Q1) ->
              case rabbit_queue_type:consume(Q1, Spec, QStates0) of
                  {ok, QStates} ->
                      %% io:format("Consume QStates ~p ~n", [QStates]),
                      %% rabbit_global_counters:consumer_created('STOMP'),
                      State1 = State#proc_state{queue_states = QStates},
                              {ok, State1};
                  {error, Reason} ->
                      error("Failed to consume from ~s: ~p",
                                    [rabbit_misc:rs(QRes), Reason],
                            State)
              end
      end).

create_queue(Amqqueue, _State = #proc_state{authz_context = AuthzCtx,
                                            user = User,
                                            vhost = VHost}) ->
    QName = amqqueue:get_name(Amqqueue),

    %% configure access to queue required for queue.declare
    ok = check_resource_access(User, QName, configure, AuthzCtx),

    case rabbit_vhost_limit:is_over_queue_limit(VHost) of
        false ->
            rabbit_core_metrics:queue_declared(QName),

            case rabbit_queue_type:declare(Amqqueue, node()) of
                {new, Q} when ?is_amqqueue(Q) ->
                        rabbit_core_metrics:queue_created(QName),
                    {ok, Q};
                {existing, Q} when ?is_amqqueue(Q) ->
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

check_internal_exchange(#exchange{name = Name, internal = true}) ->
    rabbit_misc:protocol_error(access_refused,
                               "cannot publish to internal ~ts",
                               [rabbit_misc:rs(Name)]);
check_internal_exchange(_) ->
    ok.


check_topic_authorisation(#exchange{name = Name = #resource{virtual_host = VHost}, type = topic},
                             User = #user{username = Username},
                          RoutingKey, AuthzContext, Permission) ->
    Resource = Name#resource{kind = topic},
    VariableMap = build_topic_variable_map(AuthzContext, VHost, Username),
    Context = #{routing_key  => RoutingKey,
                variable_map => VariableMap},
    Cache = case get(topic_permission_cache) of
                undefined -> [];
                Other     -> Other
            end,
    case lists:member({Resource, Context, Permission}, Cache) of
        true  -> ok;
        false -> ok = rabbit_access_control:check_topic_access(
            User, Resource, Permission, Context),
            CacheTail = lists:sublist(Cache, ?MAX_PERMISSION_CACHE_SIZE-1),
            put(topic_permission_cache, [{Resource, Context, Permission} | CacheTail])
    end;
check_topic_authorisation(_, _, _, _, _) ->
    ok.


build_topic_variable_map(AuthzContext, VHost, Username) when is_map(AuthzContext) ->
    maps:merge(AuthzContext, #{<<"vhost">> => VHost, <<"username">> => Username});
build_topic_variable_map(AuthzContext, VHost, Username) ->
    maps:merge(extract_variable_map_from_amqp_params(AuthzContext), #{<<"vhost">> => VHost, <<"username">> => Username}).

%% Use tuple representation of amqp_params to avoid a dependency on amqp_client.
%% Extracts variable map only from amqp_params_direct, not amqp_params_network.
%% amqp_params_direct records are usually used by plugins (e.g. STOMP)
extract_variable_map_from_amqp_params({amqp_params, {amqp_params_direct, _, _, _, _,
                                                        {amqp_adapter_info, _,_,_,_,_,_,AdditionalInfo}, _}}) ->
    proplists:get_value(variable_map, AdditionalInfo, #{});
extract_variable_map_from_amqp_params({amqp_params_direct, _, _, _, _,
                                             {amqp_adapter_info, _,_,_,_,_,_,AdditionalInfo}, _}) ->
    proplists:get_value(variable_map, AdditionalInfo, #{});
extract_variable_map_from_amqp_params([Value]) ->
    extract_variable_map_from_amqp_params(Value);
extract_variable_map_from_amqp_params(_) ->
    #{}.


check_vhost_access(VHost, User = #user{username = Username}, PeerIp) ->
    AuthzCtx = #{},
    try rabbit_access_control:check_vhost_access(
          User, VHost, {ip, PeerIp}, AuthzCtx) of
        ok ->
            {ok, AuthzCtx}
    catch exit:#amqp_error{name = not_allowed} ->
            rabbit_core_metrics:auth_attempt_failed(PeerIp, Username, stomp),
            {error, not_allowed}
    end.

check_user_loopback(Username, PeerIp) ->
    case rabbit_access_control:check_user_loopback(Username, PeerIp) of
        ok ->
            ok;
        not_allowed ->
            rabbit_core_metrics:auth_attempt_failed(PeerIp, Username, stomp),
            {error, not_loopback}
    end.

erase_queue_stats(QName) ->
    rabbit_core_metrics:channel_queue_down({self(), QName}),
    erase({queue_stats, QName}),
    [begin
	 rabbit_core_metrics:channel_queue_exchange_down({self(), QX}),
	 erase({queue_exchange_stats, QX})
     end || {{queue_exchange_stats, QX = {QName0, _}}, _} <- get(),
	    QName0 =:= QName].
