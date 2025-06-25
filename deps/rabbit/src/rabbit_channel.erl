%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_channel).

%% rabbit_channel processes represent an AMQP 0-9-1 channels.
%%
%% Connections parse protocol frames coming from clients and
%% dispatch them to channel processes.
%% Channels are responsible for implementing the logic behind
%% the various protocol methods, involving other processes as
%% needed:
%%
%%  * Routing messages (using functions in various exchange type
%%    modules) to queue processes.
%%  * Managing queues, exchanges, and bindings.
%%  * Keeping track of consumers
%%  * Keeping track of unacknowledged deliveries to consumers
%%  * Keeping track of publisher confirms
%%  * Transaction management
%%  * Authorisation (enforcing permissions)
%%  * Publishing trace events if tracing is enabled
%%
%% Every channel has a number of dependent processes:
%%
%%  * A writer which is responsible for sending frames to clients.
%%  * A limiter which controls how many messages can be delivered
%%    to consumers according to active QoS prefetch and internal
%%    flow control logic.
%%
%% Channels are also aware of their connection's queue collector.
%% When a queue is declared as exclusive on a channel, the channel
%% will notify queue collector of that queue.

-include_lib("kernel/include/logger.hrl").

-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_misc.hrl").

-include("amqqueue.hrl").

-behaviour(gen_server2).

-export([start_link/11, start_link/12, do/2, do/3, do_flow/3, flush/1, shutdown/1]).
-export([send_command/2]).
-export([list/0, info_keys/0, info/1, info/2, info_all/0, info_all/1,
         emit_info_all/4, info_local/1]).
-export([refresh_config_local/0, ready_for_close/1]).
-export([refresh_interceptors/0]).
-export([force_event_refresh/1]).
-export([update_user_state/2]).

-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
         handle_info/2, handle_pre_hibernate/1, handle_post_hibernate/1,
         prioritise_call/4, prioritise_cast/3, prioritise_info/3,
         format_message_queue/2]).

%% Internal
-export([list_local/0, emit_info_local/3, deliver_reply_local/3]).
-export([get_vhost/1, get_user/1]).
%% For testing
-export([build_topic_variable_map/3]).
-export([list_queue_states/1]).

%% Mgmt HTTP API refactor
-export([handle_method/6,
         binding_action/4]).

-import(rabbit_misc, [maps_put_truthy/3]).

-record(conf, {
          %% starting | running | flow | closing
          state,
          %% same as reader's protocol. Used when instantiating
          %% (protocol) exceptions.
          protocol,
          %% channel number
          channel,
          %% reader process
          reader_pid,
          %% writer process
          writer_pid,
          %%
          conn_pid,
          %% same as reader's name, see #v1.name
          %% in rabbit_reader
          conn_name,
          %% same as #v1.user in the reader, used in
          %% authorisation checks
          user,
          virtual_host,
          %% when queue.bind's queue field is empty,
          %% this name will be used instead
          most_recently_declared_queue,
          %% when a queue is declared as exclusive, queue
          %% collector must be notified.
          %% see rabbit_queue_collector for more info.
          queue_collector_pid,

          %% same as capabilities in the reader
          capabilities,
          trace_state :: rabbit_trace:state(),
          consumer_prefetch,
          consumer_timeout,
          authz_context,
          max_consumers,  % taken from rabbit.consumer_max_per_channel
          %% defines how ofter gc will be executed
          writer_gc_threshold,
          msg_interceptor_ctx :: rabbit_msg_interceptor:context()
         }).

-record(pending_ack, {
                      %% delivery identifier used by clients
                      %% to acknowledge and reject deliveries
                      delivery_tag,
                      %% consumer tag
                      tag,
                      delivered_at :: integer(),
                      %% queue name
                      queue,
                      %% message ID used by queue and message store implementations
                      msg_id
                    }).

-record(ch, {cfg :: #conf{},
             %% limiter state, see rabbit_limiter
             limiter,
             %% none | {Msgs, Acks} | committing | failed |
             tx,
             %% (consumer) delivery tag sequence
             next_tag,
             %% messages pending consumer acknowledgement
             unacked_message_q,
             %% a map of consumer tags to
             %% consumer details: #amqqueue record, acknowledgement mode,
             %% consumer exclusivity, etc
             consumer_mapping,
             %% a map of queue names to consumer tag lists
             queue_consumers,
             %% timer used to emit statistics
             stats_timer,
             %% are publisher confirms enabled for this channel?
             confirm_enabled,
             %% publisher confirm delivery tag sequence
             publish_seqno,
             %% an unconfirmed_messages data structure used to track unconfirmed
             %% (to publishers) messages
             unconfirmed,
             %% a list of tags for published messages that were
             %% delivered but are yet to be confirmed to the client
             confirmed,
             %% a list of tags for published messages that were
             %% rejected but are yet to be sent to the client
             rejected,
             %% used by "one shot RPC" (amq.
             reply_consumer :: none | {rabbit_types:ctag(), binary(), binary()},
             %% see rabbitmq-server#114
             delivery_flow :: flow | noflow,
             interceptor_state,
             queue_states,
             tick_timer,
             publishing_mode = false :: boolean()
            }).

-define(QUEUE, lqueue).

-define(MAX_PERMISSION_CACHE_SIZE, 12).

-define(REFRESH_TIMEOUT, 15000).

-define(STATISTICS_KEYS,
        [reductions,
         pid,
         transactional,
         confirm,
         consumer_count,
         messages_unacknowledged,
         messages_unconfirmed,
         messages_uncommitted,
         acks_uncommitted,
         pending_raft_commands,
         cached_segments,
         prefetch_count,
         state,
         garbage_collection]).


-define(CREATION_EVENT_KEYS,
        [pid,
         name,
         connection,
         number,
         user,
         vhost,
         user_who_performed_action]).

-define(INFO_KEYS, ?CREATION_EVENT_KEYS ++ ?STATISTICS_KEYS -- [pid]).

-define(INCR_STATS(Type, Key, Inc, Measure, State),
        case rabbit_event:stats_level(State, #ch.stats_timer) of
            fine ->
                rabbit_core_metrics:channel_stats(Type, Measure, {self(), Key}, Inc),
                %% Keys in the process dictionary are used to clean up the core metrics
                put({Type, Key}, none);
            _ ->
                ok
        end).

-define(INCR_STATS(Type, Key, Inc, Measure),
        begin
            rabbit_core_metrics:channel_stats(Type, Measure, {self(), Key}, Inc),
            %% Keys in the process dictionary are used to clean up the core metrics
            put({Type, Key}, none)
        end).

%%----------------------------------------------------------------------------

-export_type([channel_number/0]).

-type channel_number() :: rabbit_types:channel_number().

-export_type([channel/0]).

-type channel() :: #ch{}.

%%----------------------------------------------------------------------------

-rabbit_deprecated_feature(
   {global_qos,
    #{deprecation_phase => permitted_by_default,
      doc_url => "https://blog.rabbitmq.com/posts/2021/08/4.0-deprecation-announcements/#removal-of-global-qos"
     }}).

-spec start_link
        (channel_number(), pid(), pid(), pid(), string(), rabbit_types:protocol(),
         rabbit_types:user(), rabbit_types:vhost(), rabbit_framing:amqp_table(),
         pid(), pid()) ->
            rabbit_types:ok_pid_or_error().

start_link(Channel, ReaderPid, WriterPid, ConnPid, ConnName, Protocol, User,
           VHost, Capabilities, CollectorPid, Limiter) ->
    start_link(Channel, ReaderPid, WriterPid, ConnPid, ConnName, Protocol, User,
           VHost, Capabilities, CollectorPid, Limiter, undefined).

-spec start_link
        (channel_number(), pid(), pid(), pid(), string(), rabbit_types:protocol(),
         rabbit_types:user(), rabbit_types:vhost(), rabbit_framing:amqp_table(),
         pid(), pid(), any()) ->
            rabbit_types:ok_pid_or_error().

start_link(Channel, ReaderPid, WriterPid, ConnPid, ConnName, Protocol, User,
           VHost, Capabilities, CollectorPid, Limiter, AmqpParams) ->
    gen_server2:start_link(
      ?MODULE, [Channel, ReaderPid, WriterPid, ConnPid, ConnName, Protocol,
                User, VHost, Capabilities, CollectorPid, Limiter, AmqpParams], []).

-spec do(pid(), rabbit_framing:amqp_method_record()) -> 'ok'.

do(Pid, Method) ->
    rabbit_channel_common:do(Pid, Method).

-spec do
        (pid(), rabbit_framing:amqp_method_record(),
         rabbit_types:'maybe'(rabbit_types:content())) ->
            'ok'.

do(Pid, Method, Content) ->
    rabbit_channel_common:do(Pid, Method, Content).

-spec do_flow
        (pid(), rabbit_framing:amqp_method_record(),
         rabbit_types:'maybe'(rabbit_types:content())) ->
            'ok'.

do_flow(Pid, Method, Content) ->
    rabbit_channel_common:do_flow(Pid, Method, Content).

-spec flush(pid()) -> 'ok'.

flush(Pid) ->
    gen_server2:call(Pid, flush, infinity).

-spec shutdown(pid()) -> 'ok'.

shutdown(Pid) ->
    gen_server2:cast(Pid, terminate).

-spec send_command(pid(), rabbit_framing:amqp_method_record()) -> 'ok'.

send_command(Pid, Msg) ->
    gen_server2:cast(Pid,  {command, Msg}).


-spec deliver_reply(binary(), mc:state()) -> 'ok'.
deliver_reply(<<"amq.rabbitmq.reply-to.", EncodedBin/binary>>, Message) ->
    Nodes = rabbit_nodes:all_running_with_hashes(),
    case rabbit_direct_reply_to:decode_reply_to(EncodedBin, Nodes) of
        {ok, Pid, Key} ->
            delegate:invoke_no_result(
              Pid, {?MODULE, deliver_reply_local, [Key, Message]});
        {error, _} ->
            ok
    end.

%% We want to ensure people can't use this mechanism to send a message
%% to an arbitrary process and kill it!

-spec deliver_reply_local(pid(), binary(), mc:state()) -> 'ok'.

deliver_reply_local(Pid, Key, Message) ->
    case pg_local:in_group(rabbit_channels, Pid) of
        true  -> gen_server2:cast(Pid, {deliver_reply, Key, Message});
        false -> ok
    end.

declare_fast_reply_to(<<"amq.rabbitmq.reply-to">>) ->
    exists;
declare_fast_reply_to(<<"amq.rabbitmq.reply-to.", EncodedBin/binary>>) ->
    Nodes = rabbit_nodes:all_running_with_hashes(),
    case rabbit_direct_reply_to:decode_reply_to(EncodedBin, Nodes) of
        {ok, Pid, Key} ->
            Msg = {declare_fast_reply_to, Key},
            rabbit_misc:with_exit_handler(
              rabbit_misc:const(not_found),
              fun() -> gen_server2:call(Pid, Msg, infinity) end);
        {error, _} ->
            not_found
    end;
declare_fast_reply_to(_) ->
    not_found.

-spec list() -> [pid()].

list() ->
    Nodes = rabbit_nodes:list_running(),
    rabbit_misc:append_rpc_all_nodes(Nodes, rabbit_channel, list_local, [], ?RPC_TIMEOUT).

-spec list_local() -> [pid()].

list_local() ->
    pg_local:get_members(rabbit_channels).

-spec info_keys() -> rabbit_types:info_keys().

info_keys() -> ?INFO_KEYS.

-spec info(pid()) -> rabbit_types:infos().

info(Pid) ->
    {Timeout, Deadline} = get_operation_timeout_and_deadline(),
    try
        case gen_server2:call(Pid, {info, Deadline}, Timeout) of
            {ok, Res}      -> Res;
            {error, Error} -> throw(Error)
        end
    catch
        exit:{timeout, _} ->
            rabbit_log:error("Timed out getting channel ~tp info", [Pid]),
            throw(timeout)
    end.

-spec info(pid(), rabbit_types:info_keys()) -> rabbit_types:infos().

info(Pid, Items) ->
    {Timeout, Deadline} = get_operation_timeout_and_deadline(),
    try
        case gen_server2:call(Pid, {{info, Items}, Deadline}, Timeout) of
            {ok, Res}      -> Res;
            {error, Error} -> throw(Error)
        end
    catch
        exit:{timeout, _} ->
            rabbit_log:error("Timed out getting channel ~tp info", [Pid]),
            throw(timeout)
    end.

-spec info_all() -> [rabbit_types:infos()].

info_all() ->
    rabbit_misc:filter_exit_map(fun (C) -> info(C) end, list()).

-spec info_all(rabbit_types:info_keys()) -> [rabbit_types:infos()].

info_all(Items) ->
    rabbit_misc:filter_exit_map(fun (C) -> info(C, Items) end, list()).

info_local(Items) ->
    rabbit_misc:filter_exit_map(fun (C) -> info(C, Items) end, list_local()).

emit_info_all(Nodes, Items, Ref, AggregatorPid) ->
    Pids = [ spawn_link(Node, rabbit_channel, emit_info_local, [Items, Ref, AggregatorPid]) || Node <- Nodes ],
    rabbit_control_misc:await_emitters_termination(Pids).

emit_info_local(Items, Ref, AggregatorPid) ->
    emit_info(list_local(), Items, Ref, AggregatorPid).

emit_info(PidList, InfoItems, Ref, AggregatorPid) ->
    rabbit_control_misc:emitting_map_with_exit_handler(
      AggregatorPid, Ref, fun(C) -> info(C, InfoItems) end, PidList).

-spec refresh_config_local() -> 'ok'.

refresh_config_local() ->
    _ = rabbit_misc:upmap(
      fun (C) ->
        try
          gen_server2:call(C, refresh_config, infinity)
        catch _:Reason ->
          rabbit_log:error("Failed to refresh channel config "
                           "for channel ~tp. Reason ~tp",
                           [C, Reason])
        end
      end,
      list_local()),
    ok.

refresh_interceptors() ->
    _ = rabbit_misc:upmap(
      fun (C) ->
        try
          gen_server2:call(C, refresh_interceptors, ?REFRESH_TIMEOUT)
        catch _:Reason ->
          rabbit_log:error("Failed to refresh channel interceptors "
                           "for channel ~tp. Reason ~tp",
                           [C, Reason])
        end
      end,
      list_local()),
    ok.

-spec ready_for_close(pid()) -> 'ok'.

ready_for_close(Pid) ->
    rabbit_channel_common:ready_for_close(Pid).

-spec force_event_refresh(reference()) -> 'ok'.

% Note: https://www.pivotaltracker.com/story/show/166962656
% This event is necessary for the stats timer to be initialized with
% the correct values once the management agent has started
force_event_refresh(Ref) ->
    [gen_server2:cast(C, {force_event_refresh, Ref}) || C <- list()],
    ok.

list_queue_states(Pid) ->
    gen_server2:call(Pid, list_queue_states).

-spec update_user_state(pid(), rabbit_types:user()) -> 'ok' | {error, channel_terminated}.

update_user_state(Pid, UserState) when is_pid(Pid) ->
    case erlang:is_process_alive(Pid) of
        true  -> Pid ! {update_user_state, UserState},
                 ok;
        false -> {error, channel_terminated}
    end.

%%---------------------------------------------------------------------------

init([Channel, ReaderPid, WriterPid, ConnPid, ConnName, Protocol, User, VHost,
      Capabilities, CollectorPid, LimiterPid, AmqpParams]) ->
    process_flag(trap_exit, true),
    rabbit_process_flag:adjust_for_message_handling_proc(),

    ?LG_PROCESS_TYPE(channel),
    ?store_proc_name({ConnName, Channel}),
    ok = pg_local:join(rabbit_channels, self()),
    Flow = case rabbit_misc:get_env(rabbit, classic_queue_flow_control, true) of
             true   -> flow;
             false  -> noflow
           end,
    {ok, {Global0, Prefetch}} = application:get_env(rabbit, default_consumer_prefetch),
    Limiter0 = rabbit_limiter:new(LimiterPid),
    Global = Global0 andalso is_global_qos_permitted(),
    Limiter = case {Global, Prefetch} of
                  {true, 0} ->
                      rabbit_limiter:unlimit_prefetch(Limiter0);
                  {true, _} ->
                      rabbit_limiter:limit_prefetch(Limiter0, Prefetch, 0);
                  _ ->
                      Limiter0
              end,
    %% Process dictionary is used here because permission cache already uses it. MK.
    put(permission_cache_can_expire, rabbit_access_control:permission_cache_can_expire(User)),
    ConsumerTimeout = get_consumer_timeout(),
    OptionalVariables = extract_variable_map_from_amqp_params(AmqpParams),
    {ok, GCThreshold} = application:get_env(rabbit, writer_gc_threshold),
    MaxConsumers = application:get_env(rabbit, consumer_max_per_channel, infinity),
    MsgIcptCtx = #{protocol => amqp091,
                   vhost => VHost,
                   username => User#user.username,
                   connection_name => ConnName},
    State = #ch{cfg = #conf{state = starting,
                            protocol = Protocol,
                            channel = Channel,
                            reader_pid = ReaderPid,
                            writer_pid = WriterPid,
                            conn_pid = ConnPid,
                            conn_name = ConnName,
                            user = User,
                            virtual_host = VHost,
                            most_recently_declared_queue = <<>>,
                            queue_collector_pid = CollectorPid,
                            capabilities = Capabilities,
                            trace_state = rabbit_trace:init(VHost),
                            consumer_prefetch = Prefetch,
                            consumer_timeout = ConsumerTimeout,
                            authz_context = OptionalVariables,
                            max_consumers = MaxConsumers,
                            writer_gc_threshold = GCThreshold,
                            msg_interceptor_ctx = MsgIcptCtx},
                limiter = Limiter,
                tx                      = none,
                next_tag                = 1,
                unacked_message_q       = ?QUEUE:new(),
                consumer_mapping        = #{},
                queue_consumers         = #{},
                confirm_enabled         = false,
                publish_seqno           = 1,
                unconfirmed             = rabbit_confirms:init(),
                rejected                = [],
                confirmed               = [],
                reply_consumer          = none,
                delivery_flow           = Flow,
                interceptor_state       = undefined,
                queue_states            = rabbit_queue_type:init()
               },
    State1 = State#ch{
               interceptor_state = rabbit_channel_interceptor:init(State)},
    State2 = rabbit_event:init_stats_timer(State1, #ch.stats_timer),
    Infos = infos(?CREATION_EVENT_KEYS, State2),
    rabbit_core_metrics:channel_created(self(), Infos),
    rabbit_event:notify(channel_created, Infos),
    rabbit_event:if_enabled(State2, #ch.stats_timer,
                            fun() -> emit_stats(State2) end),
    put_operation_timeout(),
    State3 = init_tick_timer(State2),
    {ok, State3, hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

prioritise_call(Msg, _From, _Len, _State) ->
    case Msg of
        info           -> 9;
        {info, _Items} -> 9;
        _              -> 0
    end.

prioritise_cast(Msg, _Len, _State) ->
    case Msg of
        {confirm,            _MsgSeqNos, _QPid} -> 5;
        {reject_publish,     _MsgSeqNos, _QPid} -> 5;
        _                                       -> 0
    end.

prioritise_info(Msg, _Len, _State) ->
    case Msg of
        emit_stats                   -> 7;
        _                            -> 0
    end.

handle_call(flush, _From, State) ->
    reply(ok, State);

handle_call({info, Deadline}, _From, State) ->
    try
        reply({ok, infos(?INFO_KEYS, Deadline, State)}, State)
    catch
        Error ->
            reply({error, Error}, State)
    end;

handle_call({{info, Items}, Deadline}, _From, State) ->
    try
        reply({ok, infos(Items, Deadline, State)}, State)
    catch
        Error ->
            reply({error, Error}, State)
    end;

handle_call(refresh_config, _From,
            State = #ch{cfg = #conf{virtual_host = VHost} = Cfg}) ->
    reply(ok, State#ch{cfg = Cfg#conf{trace_state = rabbit_trace:init(VHost)}});

handle_call(refresh_interceptors, _From, State) ->
    IState = rabbit_channel_interceptor:init(State),
    reply(ok, State#ch{interceptor_state = IState});

handle_call({declare_fast_reply_to, Key}, _From,
            State = #ch{reply_consumer = Consumer}) ->
    reply(case Consumer of
              {_, _, Key} -> exists;
              _           -> not_found
          end, State);

handle_call(list_queue_states, _From, State = #ch{queue_states = QueueStates}) ->
    %% For testing of cleanup only
    %% HACK
    {reply, maps:keys(element(2, QueueStates)), State};

handle_call(_Request, _From, State) ->
    noreply(State).

handle_cast({method, Method, Content, Flow},
            State = #ch{cfg = #conf{reader_pid = Reader},
                        interceptor_state = IState}) ->
    case Flow of
        %% We are going to process a message from the rabbit_reader
        %% process, so here we ack it. In this case we are accessing
        %% the rabbit_channel process dictionary.
        flow   -> credit_flow:ack(Reader);
        noflow -> ok
    end,
    try handle_method(rabbit_channel_interceptor:intercept_in(
                        expand_shortcuts(Method, State), Content, IState),
                      State) of
        {reply, Reply, NewState} ->
            ok = send(Reply, NewState),
            noreply(NewState);
        {noreply, NewState} ->
            noreply(NewState);
        stop ->
            {stop, normal, State}
    catch
        exit:Reason = #amqp_error{} ->
            MethodName = rabbit_misc:method_record_type(Method),
            handle_exception(Reason#amqp_error{method = MethodName}, State);
        _:Reason:Stacktrace ->
            {stop, {Reason, Stacktrace}, State}
    end;

handle_cast(ready_for_close,
            State = #ch{cfg = #conf{state = closing,
                                    writer_pid = WriterPid}}) ->
    ok = rabbit_writer:send_command_sync(WriterPid, #'channel.close_ok'{}),
    {stop, normal, State};

handle_cast(terminate, State = #ch{cfg = #conf{writer_pid = WriterPid}}) ->
    try
       ok = rabbit_writer:flush(WriterPid)
    catch
        _Class:Reason ->
            rabbit_log:debug("Failed to flush pending writes on a terminating connection, reason: ~tp", [Reason])
    end,
    {stop, normal, State};

handle_cast({command, #'basic.consume_ok'{consumer_tag = CTag} = Msg},
            #ch{consumer_mapping = CMap} = State)
  when is_map_key(CTag, CMap) ->
    ok = send(Msg, State),
    noreply(consumer_monitor(CTag, State));
handle_cast({command, #'basic.consume_ok'{}}, State) ->
    %% a consumer was not found so just ignore this
    noreply(State);
handle_cast({command, Msg}, State) ->
    ok = send(Msg, State),
    noreply(State);

handle_cast({deliver_reply, _K, _Del},
            State = #ch{cfg = #conf{state = closing}}) ->
    noreply(State);
handle_cast({deliver_reply, _K, _Msg}, State = #ch{reply_consumer = none}) ->
    noreply(State);
handle_cast({deliver_reply, Key, Mc},
            State = #ch{cfg = #conf{writer_pid = WriterPid,
                                    msg_interceptor_ctx = MsgIcptCtx},
                        next_tag = DeliveryTag,
                        reply_consumer = {ConsumerTag, _Suffix, Key}}) ->
    ExchName = mc:exchange(Mc),
    [RoutingKey | _] = mc:routing_keys(Mc),
    Content = outgoing_content(Mc, MsgIcptCtx),
    ok = rabbit_writer:send_command(
           WriterPid,
           #'basic.deliver'{consumer_tag = ConsumerTag,
                            delivery_tag = DeliveryTag,
                            redelivered = false,
                            exchange = ExchName,
                            routing_key = RoutingKey},
           Content),
    noreply(State);
handle_cast({deliver_reply, _K1, _}, State=#ch{reply_consumer = {_, _, _K2}}) ->
    noreply(State);

% Note: https://www.pivotaltracker.com/story/show/166962656
% This event is necessary for the stats timer to be initialized with
% the correct values once the management agent has started
handle_cast({force_event_refresh, Ref}, State) ->
    rabbit_event:notify(channel_created, infos(?CREATION_EVENT_KEYS, State),
                        Ref),
    noreply(rabbit_event:init_stats_timer(State, #ch.stats_timer));

handle_cast({queue_event, QRef, Evt},
            #ch{queue_states = QueueStates0} = State0) ->
    case rabbit_queue_type:handle_event(QRef, Evt, QueueStates0) of
        {ok, QState1, Actions} ->
            State1 = State0#ch{queue_states = QState1},
            State = handle_queue_actions(Actions, State1),
            noreply_coalesce(State);
        {eol, Actions} ->
            State = handle_queue_actions(Actions, State0),
            handle_eol(QRef, State);
        {protocol_error, Type, Reason, ReasonArgs} ->
            rabbit_misc:protocol_error(Type, Reason, ReasonArgs)
    end.

handle_info({bump_credit, Msg}, State) ->
    %% A rabbit_amqqueue_process is granting credit to our channel. If
    %% our channel was being blocked by this process, and no other
    %% process is blocking our channel, then this channel will be
    %% unblocked. This means that any credit that was deferred will be
    %% sent to rabbit_reader processs that might be blocked by this
    %% particular channel.
    credit_flow:handle_bump_msg(Msg),
    noreply(State);

handle_info(timeout, State) ->
    noreply(State);

handle_info(emit_stats, State) ->
    emit_stats(State),
    State1 = rabbit_event:reset_stats_timer(State, #ch.stats_timer),
    %% NB: don't call noreply/1 since we don't want to kick off the
    %% stats timer.
    {noreply, send_confirms_and_nacks(State1), hibernate};

handle_info({{'DOWN', QName}, _MRef, process, QPid, Reason},
            #ch{queue_states = QStates0} = State0) ->
    credit_flow:peer_down(QPid),
    case rabbit_queue_type:handle_down(QPid, QName, Reason, QStates0) of
        {ok, QState1, Actions} ->
            State1 = State0#ch{queue_states = QState1},
            State = handle_queue_actions(Actions, State1),
            noreply_coalesce(State);
        {eol, QState1, QRef} ->
            State = State0#ch{queue_states = QState1},
            handle_eol(QRef, State)
    end;

handle_info({'DOWN', _MRef, process, Pid, normal}, State) ->
    ?LOG_DEBUG("Process ~0p monitored by channel ~0p exited", [Pid, self()]),
    {noreply, State};

handle_info({'EXIT', _Pid, Reason}, State) ->
    {stop, Reason, State};

handle_info({{Ref, Node}, LateAnswer},
            State = #ch{cfg = #conf{channel = Channel}})
  when is_reference(Ref) ->
    rabbit_log_channel:warning("Channel ~tp ignoring late answer ~tp from ~tp",
        [Channel, LateAnswer, Node]),
    noreply(State);

handle_info(tick, State0 = #ch{queue_states = QueueStates0}) ->
    case get(permission_cache_can_expire) of
      true  -> ok = clear_permission_cache();
      _     -> ok
    end,
    case evaluate_consumer_timeout(State0#ch{queue_states = QueueStates0}) of
        {noreply, State} ->
            noreply(init_tick_timer(reset_tick_timer(State)));
        Return ->
            Return
    end;
handle_info({update_user_state, User}, State = #ch{cfg = Cfg}) ->
    noreply(State#ch{cfg = Cfg#conf{user = User}}).


handle_pre_hibernate(State0) ->
    ok = clear_permission_cache(),
    State = maybe_cancel_tick_timer(State0),
    rabbit_event:if_enabled(
      State, #ch.stats_timer,
      fun () -> emit_stats(State,
                           [{idle_since,
                             os:system_time(millisecond)}])
      end),
    {hibernate, rabbit_event:stop_stats_timer(State, #ch.stats_timer)}.

handle_post_hibernate(State0) ->
    State = init_tick_timer(State0),
    {noreply, State}.

terminate(_Reason,
          State = #ch{cfg = #conf{user = #user{username = Username}},
                      consumer_mapping = CM,
                      queue_states = QueueCtxs}) ->
    rabbit_queue_type:close(QueueCtxs),
    {_Res, _State1} = notify_queues(State),
    pg_local:leave(rabbit_channels, self()),
    rabbit_event:if_enabled(State, #ch.stats_timer,
                            fun() -> emit_stats(State) end),
    [delete_stats(Tag) || {Tag, _} <- get()],
    maybe_decrease_global_publishers(State),
    maps:foreach(
      fun (_, _) ->
              rabbit_global_counters:consumer_deleted(amqp091)
      end, CM),
    rabbit_core_metrics:channel_closed(self()),
    rabbit_event:notify(channel_closed, [{pid, self()},
                                         {user_who_performed_action, Username},
                                         {consumer_count, maps:size(CM)}]),
    case rabbit_confirms:size(State#ch.unconfirmed) of
        0 -> ok;
        NumConfirms ->
            rabbit_log:warning("Channel is stopping with ~b pending publisher confirms",
                               [NumConfirms])
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_message_queue(Opt, MQ) -> rabbit_misc:format_message_queue(Opt, MQ).

get_consumer_timeout() ->
    case application:get_env(rabbit, consumer_timeout) of
        {ok, MS} when is_integer(MS) ->
            MS;
        _ ->
            undefined
    end.

%%---------------------------------------------------------------------------

reply(Reply, NewState) -> {reply, Reply, next_state(NewState), hibernate}.

noreply(NewState) -> {noreply, next_state(NewState), hibernate}.

next_state(State) -> ensure_stats_timer(send_confirms_and_nacks(State)).

noreply_coalesce(#ch{confirmed = [], rejected = []} = State) ->
    {noreply, ensure_stats_timer(State), hibernate};
noreply_coalesce(#ch{} = State) ->
    % Immediately process 'timeout' info message
    {noreply, ensure_stats_timer(State), 0}.

ensure_stats_timer(State) ->
    rabbit_event:ensure_stats_timer(State, #ch.stats_timer, emit_stats).

return_ok(State, true, _Msg)  -> {noreply, State};
return_ok(State, false, Msg)  -> {reply, Msg, State}.

ok_msg(true, _Msg) -> undefined;
ok_msg(false, Msg) -> Msg.

send(_Command, #ch{cfg = #conf{state = closing}}) ->
    ok;
send(Command, #ch{cfg = #conf{writer_pid = WriterPid}}) ->
    ok = rabbit_writer:send_command(WriterPid, Command).

format_soft_error(#amqp_error{name = N, explanation = E, method = M}) ->
    io_lib:format("operation ~ts caused a channel exception ~ts: ~ts", [M, N, E]).

handle_exception(Reason, State = #ch{cfg = #conf{protocol = Protocol,
                                                 channel = Channel,
                                                 writer_pid = WriterPid,
                                                 reader_pid = ReaderPid,
                                                 conn_pid = ConnPid,
                                                 conn_name = ConnName,
                                                 virtual_host = VHost,
                                                 user = User
                                                }}) ->
    %% something bad's happened: notify_queues may not be 'ok'
    {_Result, State1} = notify_queues(State),
    case rabbit_binary_generator:map_exception(Channel, Reason, Protocol) of
        {Channel, CloseMethod} ->
            rabbit_log_channel:error(
                "Channel error on connection ~tp (~ts, vhost: '~ts',"
                " user: '~ts'), channel ~tp:~n~ts",
                [ConnPid, ConnName, VHost, User#user.username,
                 Channel, format_soft_error(Reason)]),
            ok = rabbit_writer:send_command(WriterPid, CloseMethod),
            {noreply, State1};
        {0, _} ->
            ReaderPid ! {channel_exit, Channel, Reason},
            {stop, normal, State1}
    end.

return_queue_declare_ok(#resource{name = ActualName},
                        NoWait, MessageCount, ConsumerCount,
                        #ch{cfg = Cfg} = State) ->
    return_ok(State#ch{cfg = Cfg#conf{most_recently_declared_queue = ActualName}},
              NoWait, #'queue.declare_ok'{queue          = ActualName,
                                          message_count  = MessageCount,
                                          consumer_count = ConsumerCount}).

check_resource_access(User, Resource, Perm, Context) ->
    V = {Resource, Context, Perm},

    Cache = case get(permission_cache) of
                undefined -> [];
                Other     -> Other
            end,
    case lists:member(V, Cache) of
        true  -> ok;
        false -> ok = rabbit_access_control:check_resource_access(
                        User, Resource, Perm, Context),
                 CacheTail = lists:sublist(Cache, ?MAX_PERMISSION_CACHE_SIZE-1),
                 put(permission_cache, [V | CacheTail])
    end.

clear_permission_cache() -> erase(permission_cache),
                            erase(topic_permission_cache),
                            ok.

check_configure_permitted(Resource, User, Context) ->
    check_resource_access(User, Resource, configure, Context).

check_write_permitted(Resource, User, Context) ->
    check_resource_access(User, Resource, write, Context).

check_read_permitted(Resource, User, Context) ->
    check_resource_access(User, Resource, read, Context).

check_write_permitted_on_topics(#exchange{type = topic} = Resource, User, Mc, AuthzContext) ->
    lists:foreach(
      fun(RoutingKey) ->
              check_topic_authorisation(Resource, User, RoutingKey, AuthzContext, write)
      end, mc:routing_keys(Mc));
check_write_permitted_on_topics(_, _, _, _) ->
    ok.

check_read_permitted_on_topic(Resource, User, RoutingKey, AuthzContext) ->
    check_topic_authorisation(Resource, User, RoutingKey, AuthzContext, read).

check_user_id_header(Msg, User) ->
    case rabbit_access_control:check_user_id(Msg, User) of
        ok ->
            ok;
        {refused, Reason, Args} ->
            rabbit_misc:precondition_failed(Reason, Args)
    end.

check_expiration_header(Props) ->
    case rabbit_basic:parse_expiration(Props) of
        {ok, _}    -> ok;
        {error, E} -> rabbit_misc:precondition_failed("invalid expiration '~ts': ~tp",
                                                      [Props#'P_basic'.expiration, E])
    end.

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

check_msg_size(Content, GCThreshold) ->
    MaxMessageSize = persistent_term:get(max_message_size),
    Size = rabbit_basic:maybe_gc_large_msg(Content, GCThreshold),
    case Size =< MaxMessageSize of
        true ->
            rabbit_msg_size_metrics:observe(amqp091, Size);
        false ->
            Fmt = case MaxMessageSize of
                      ?MAX_MSG_SIZE ->
                          "message size ~B is larger than max size ~B";
                      _ ->
                          "message size ~B is larger than configured max size ~B"
                  end,
            rabbit_misc:precondition_failed(
              Fmt, [Size, MaxMessageSize])
    end.

qbin_to_resource(QueueNameBin, VHostPath) ->
    name_to_resource(queue, QueueNameBin, VHostPath).

name_to_resource(Type, NameBin, VHostPath) ->
    rabbit_misc:r(VHostPath, Type, NameBin).

expand_queue_name_shortcut(<<>>, #ch{cfg = #conf{most_recently_declared_queue = <<>>}}) ->
    rabbit_misc:protocol_error(not_found, "no previously declared queue", []);
expand_queue_name_shortcut(<<>>, #ch{cfg = #conf{most_recently_declared_queue = MRDQ}}) ->
    MRDQ;
expand_queue_name_shortcut(QueueNameBin, _) ->
    QueueNameBin.

expand_routing_key_shortcut(<<>>, <<>>,
                            #ch{cfg = #conf{most_recently_declared_queue = <<>>}}) ->
    rabbit_misc:protocol_error(not_found, "no previously declared queue", []);
expand_routing_key_shortcut(<<>>, <<>>,
                            #ch{cfg = #conf{most_recently_declared_queue = MRDQ}}) ->
    MRDQ;
expand_routing_key_shortcut(_QueueNameBin, RoutingKey, _State) ->
    RoutingKey.

expand_shortcuts(#'basic.get'    {queue = Q} = M, State) ->
    M#'basic.get'    {queue = expand_queue_name_shortcut(Q, State)};
expand_shortcuts(#'basic.consume'{queue = Q} = M, State) ->
    M#'basic.consume'{queue = expand_queue_name_shortcut(Q, State)};
expand_shortcuts(#'queue.delete' {queue = Q} = M, State) ->
    M#'queue.delete' {queue = expand_queue_name_shortcut(Q, State)};
expand_shortcuts(#'queue.purge'  {queue = Q} = M, State) ->
    M#'queue.purge'  {queue = expand_queue_name_shortcut(Q, State)};
expand_shortcuts(#'queue.bind'   {queue = Q, routing_key = K} = M, State) ->
    M#'queue.bind'   {queue       = expand_queue_name_shortcut(Q, State),
                      routing_key = expand_routing_key_shortcut(Q, K, State)};
expand_shortcuts(#'queue.unbind' {queue = Q, routing_key = K} = M, State) ->
    M#'queue.unbind' {queue       = expand_queue_name_shortcut(Q, State),
                      routing_key = expand_routing_key_shortcut(Q, K, State)};
expand_shortcuts(M, _State) ->
    M.

check_not_default_exchange(#resource{kind = exchange, name = <<"">>}) ->
    rabbit_misc:protocol_error(
      access_refused, "operation not permitted on the default exchange", []);
check_not_default_exchange(_) ->
    ok.

check_exchange_deletion(XName = #resource{name = <<"amq.", _/binary>>,
                                          kind = exchange}) ->
    rabbit_misc:protocol_error(
      access_refused, "deletion of system ~ts not allowed",
      [rabbit_misc:rs(XName)]);
check_exchange_deletion(_) ->
    ok.

%% check that an exchange/queue name does not contain the reserved
%% "amq."  prefix.
%%
%% As per the AMQP 0-9-1 spec, the exclusion of "amq." prefixed names
%% only applies on actual creation, and not in the cases where the
%% entity already exists or passive=true.
%%
%% NB: We deliberately do not enforce the other constraints on names
%% required by the spec.
check_name(Kind, NameBin = <<"amq.", _/binary>>) ->
    rabbit_misc:protocol_error(
      access_refused,
      "~ts name '~ts' contains reserved prefix 'amq.*'",[Kind, NameBin]);
check_name(_Kind, NameBin) ->
    NameBin.

strip_cr_lf(NameBin) ->
  binary:replace(NameBin, [<<"\n">>, <<"\r">>], <<"">>, [global]).


maybe_set_fast_reply_to(
  C = #content{properties = P = #'P_basic'{reply_to =
                                               <<"amq.rabbitmq.reply-to">>}},
  #ch{reply_consumer = ReplyConsumer}) ->
    case ReplyConsumer of
        none         -> rabbit_misc:protocol_error(
                          precondition_failed,
                          "fast reply consumer does not exist", []);
        {_, Suf, _K} -> Rep = <<"amq.rabbitmq.reply-to.", Suf/binary>>,
                        rabbit_binary_generator:clear_encoded_content(
                          C#content{properties = P#'P_basic'{reply_to = Rep}})
    end;
maybe_set_fast_reply_to(C, _State) ->
    C.

record_rejects([], State) ->
    State;
record_rejects(MXs, State = #ch{rejected = R, tx = Tx}) ->
    Tx1 = case Tx of
        none -> none;
        _    -> failed
    end,
    State#ch{rejected = [MXs | R], tx = Tx1}.

record_confirms([], State) ->
    State;
record_confirms(MXs, State = #ch{confirmed = C}) ->
    State#ch{confirmed = [MXs | C]}.

handle_method({Method, Content}, State) ->
    handle_method(Method, Content, State).

handle_method(#'channel.open'{}, _,
              State = #ch{cfg = #conf{state = starting} = Cfg}) ->
    %% Don't leave "starting" as the state for 5s. TODO is this TRTTD?
    State1 = State#ch{cfg = Cfg#conf{state = running}},
    rabbit_event:if_enabled(State1, #ch.stats_timer,
                            fun() -> emit_stats(State1) end),
    {reply, #'channel.open_ok'{}, State1};

handle_method(#'channel.open'{}, _, _State) ->
    rabbit_misc:protocol_error(
      channel_error, "second 'channel.open' seen", []);

handle_method(_Method, _, #ch{cfg = #conf{state = starting}}) ->
    rabbit_misc:protocol_error(channel_error, "expected 'channel.open'", []);

handle_method(#'channel.close_ok'{}, _, #ch{cfg = #conf{state = closing}}) ->
    stop;

handle_method(#'channel.close'{}, _,
              State = #ch{cfg = #conf{state = closing,
                                      writer_pid = WriterPid}}) ->
    ok = rabbit_writer:send_command(WriterPid, #'channel.close_ok'{}),
    {noreply, State};

handle_method(_Method, _, State = #ch{cfg = #conf{state = closing}}) ->
    {noreply, State};

handle_method(#'channel.close'{}, _,
              State = #ch{cfg = #conf{reader_pid = ReaderPid}}) ->
    {_Result, State1} = notify_queues(State),
    %% We issue the channel.close_ok response after a handshake with
    %% the reader, the other half of which is ready_for_close. That
    %% way the reader forgets about the channel before we send the
    %% response (and this channel process terminates). If we didn't do
    %% that, a channel.open for the same channel number, which a
    %% client is entitled to send as soon as it has received the
    %% close_ok, might be received by the reader before it has seen
    %% the termination and hence be sent to the old, now dead/dying
    %% channel process, instead of a new process, and thus lost.
    ReaderPid ! {channel_closing, self()},
    {noreply, State1};

%% Even though the spec prohibits the client from sending commands
%% while waiting for the reply to a synchronous command, we generally
%% do allow this...except in the case of a pending tx.commit, where
%% it could wreak havoc.
handle_method(_Method, _, #ch{tx = Tx})
  when Tx =:= committing orelse Tx =:= failed ->
    rabbit_misc:protocol_error(
      channel_error, "unexpected command while processing 'tx.commit'", []);

handle_method(#'access.request'{},_, State) ->
    {reply, #'access.request_ok'{ticket = 1}, State};

handle_method(#'basic.publish'{immediate = true}, _Content, _State) ->
    rabbit_misc:protocol_error(not_implemented, "immediate=true", []);

handle_method(#'basic.publish'{exchange    = ExchangeNameBin,
                               routing_key = RoutingKey,
                               mandatory   = Mandatory},
              Content, State0 = #ch{cfg = #conf{channel = ChannelNum,
                                                conn_name = ConnName,
                                                virtual_host = VHostPath,
                                                user = #user{username = Username} = User,
                                                trace_state = TraceState,
                                                authz_context = AuthzContext,
                                                writer_gc_threshold = GCThreshold,
                                                msg_interceptor_ctx = MsgIcptCtx
                                               },
                                   tx               = Tx,
                                   confirm_enabled  = ConfirmEnabled,
                                   delivery_flow    = Flow
                                   }) ->
    State1 = maybe_increase_global_publishers(State0),
    rabbit_global_counters:messages_received(amqp091, 1),
    check_msg_size(Content, GCThreshold),
    ExchangeName = rabbit_misc:r(VHostPath, exchange, ExchangeNameBin),
    check_write_permitted(ExchangeName, User, AuthzContext),
    Exchange = rabbit_exchange:lookup_or_die(ExchangeName),
    check_internal_exchange(Exchange),
    %% We decode the content's properties here because we're almost
    %% certain to want to look at delivery-mode and priority.
    DecodedContent = #content {properties = Props} =
        maybe_set_fast_reply_to(
          rabbit_binary_parser:ensure_content_decoded(Content), State1),
    check_expiration_header(Props),
    DoConfirm = Tx =/= none orelse ConfirmEnabled,
    {DeliveryOptions, State} =
        case DoConfirm of
            false ->
                {maps_put_truthy(flow, Flow, #{mandatory => Mandatory}), State1};
            true  ->
                rabbit_global_counters:messages_received_confirm(amqp091, 1),
                SeqNo = State1#ch.publish_seqno,
                Opts = maps_put_truthy(flow, Flow, #{correlation => SeqNo,
                                                     mandatory => Mandatory}),
                {Opts, State1#ch{publish_seqno = SeqNo + 1}}
        end,

    case mc_amqpl:message(ExchangeName,
                          RoutingKey,
                          DecodedContent) of
        {error, Reason}  ->
            rabbit_misc:precondition_failed("invalid message: ~tp", [Reason]);
        {ok, Message0} ->
            check_write_permitted_on_topics(Exchange, User, Message0, AuthzContext),
            check_user_id_header(Message0, User),
            Message = rabbit_msg_interceptor:intercept_incoming(Message0, MsgIcptCtx),
            QNames = rabbit_exchange:route(Exchange, Message, #{return_binding_keys => true}),
            [deliver_reply(RK, Message) || {virtual_reply_queue, RK} <- QNames],
            Queues = rabbit_amqqueue:lookup_many(QNames),
            rabbit_trace:tap_in(Message, QNames, ConnName, ChannelNum,
                                Username, TraceState),
            %% TODO: call delivery_to_queues with plain args
            Delivery = {Message, DeliveryOptions, Queues},
            {noreply, case Tx of
                          none ->
                              deliver_to_queues(ExchangeName, Delivery, State);
                          {Msgs, Acks} ->
                              Msgs1 = ?QUEUE:in(Delivery, Msgs),
                              State#ch{tx = {Msgs1, Acks}}
                      end}
    end;

handle_method(#'basic.nack'{delivery_tag = DeliveryTag,
                            multiple     = Multiple,
                            requeue      = Requeue}, _, State) ->
    reject(DeliveryTag, Requeue, Multiple, State);

handle_method(#'basic.ack'{delivery_tag = DeliveryTag,
                           multiple     = Multiple},
              _, State = #ch{unacked_message_q = UAMQ, tx = Tx}) ->
    {Acked, Remaining} = collect_acks(UAMQ, DeliveryTag, Multiple),
    State1 = State#ch{unacked_message_q = Remaining},
    {noreply, case Tx of
                  none         -> {State2, Actions} = settle_acks(Acked, State1),
                                  handle_queue_actions(Actions, State2);
                  {Msgs, Acks} -> Acks1 = ack_cons(ack, Acked, Acks),
                                  State1#ch{tx = {Msgs, Acks1}}
              end};

handle_method(#'basic.get'{queue = QueueNameBin, no_ack = NoAck},
              _, State = #ch{cfg = #conf{writer_pid = WriterPid,
                                         conn_pid = ConnPid,
                                         user = User,
                                         virtual_host = VHostPath,
                                         authz_context = AuthzContext
                                        },
                             limiter = Limiter,
                             next_tag   = DeliveryTag,
                             queue_states = QueueStates0}) ->
    QueueName = qbin_to_resource(QueueNameBin, VHostPath),
    check_read_permitted(QueueName, User, AuthzContext),
    case rabbit_amqqueue:with_exclusive_access_or_die(
           QueueName, ConnPid,
           %% Use the delivery tag as consumer tag for quorum queues
           fun (Q) ->
                   rabbit_queue_type:dequeue(
                     Q, NoAck, rabbit_limiter:pid(Limiter),
                     DeliveryTag, QueueStates0)
           end) of
        {ok, MessageCount, Msg, QueueStates} ->
            {ok, QueueType} = rabbit_queue_type:module(QueueName, QueueStates),
            handle_basic_get(WriterPid, DeliveryTag, NoAck, MessageCount, Msg,
                             QueueType, State#ch{queue_states = QueueStates});
        {empty, QueueStates} ->
            {ok, QueueType} = rabbit_queue_type:module(QueueName, QueueStates),
            rabbit_global_counters:messages_get_empty(amqp091, QueueType, 1),
            ?INCR_STATS(queue_stats, QueueName, 1, get_empty, State),
            {reply, #'basic.get_empty'{}, State#ch{queue_states = QueueStates}};
        {error, {unsupported, single_active_consumer}} ->
             rabbit_amqqueue:with_or_die(QueueName, fun unsupported_single_active_consumer_error/1);
        {error, Reason} ->
            %% TODO add queue type to error message
            rabbit_misc:protocol_error(internal_error,
                                       "Cannot get a message from queue '~ts': ~tp",
                                       [rabbit_misc:rs(QueueName), Reason]);
        {protocol_error, Type, Reason, ReasonArgs} ->
            rabbit_misc:protocol_error(Type, Reason, ReasonArgs)
    end;

handle_method(#'basic.consume'{queue        = <<"amq.rabbitmq.reply-to">>,
                               consumer_tag = CTag0,
                               no_ack       = NoAck,
                               nowait       = NoWait},
              _, State = #ch{reply_consumer   = ReplyConsumer,
                             cfg = #conf{max_consumers = MaxConsumers},
                             consumer_mapping = ConsumerMapping}) ->
    CurrentConsumers = maps:size(ConsumerMapping),
    case maps:find(CTag0, ConsumerMapping) of
        error when CurrentConsumers >= MaxConsumers ->  % false when MaxConsumers is 'infinity'
            rabbit_misc:protocol_error(
              not_allowed, "reached maximum (~B) of consumers per channel", [MaxConsumers]);
        error ->
            case {ReplyConsumer, NoAck} of
                {none, true} ->
                    CTag = case CTag0 of
                               <<>>  -> rabbit_guid:binary(
                                          rabbit_guid:gen_secure(), "amq.ctag");
                               Other -> Other
                           end,
                    %% Precalculate both suffix and key
                    {Key, Suffix} = rabbit_direct_reply_to:compute_key_and_suffix(self()),
                    Consumer = {CTag, Suffix, Key},
                    State1 = State#ch{reply_consumer = Consumer},
                    case NoWait of
                        true  -> {noreply, State1};
                        false -> Rep = #'basic.consume_ok'{consumer_tag = CTag},
                                 {reply, Rep, State1}
                    end;
                {_, false} ->
                    rabbit_misc:protocol_error(
                      precondition_failed,
                      "reply consumer cannot acknowledge", []);
                _ ->
                    rabbit_misc:protocol_error(
                      precondition_failed, "reply consumer already set", [])
            end;
        {ok, _} ->
            %% Attempted reuse of consumer tag.
            rabbit_misc:protocol_error(
              not_allowed, "attempt to reuse consumer tag '~ts'", [CTag0])
    end;

handle_method(#'basic.cancel'{consumer_tag = ConsumerTag, nowait = NoWait},
              _, State = #ch{reply_consumer = {ConsumerTag, _, _}}) ->
    State1 = State#ch{reply_consumer = none},
    case NoWait of
        true  -> {noreply, State1};
        false -> Rep = #'basic.cancel_ok'{consumer_tag = ConsumerTag},
                 {reply, Rep, State1}
    end;

handle_method(#'basic.consume'{queue        = QueueNameBin,
                               consumer_tag = ConsumerTag,
                               no_local     = _, % FIXME: implement
                               no_ack       = NoAck,
                               exclusive    = ExclusiveConsume,
                               nowait       = NoWait,
                               arguments    = Args},
              _, State = #ch{cfg = #conf{consumer_prefetch = ConsumerPrefetch,
                                         max_consumers = MaxConsumers,
                                         user = User,
                                         virtual_host = VHostPath,
                                         authz_context = AuthzContext},
                             consumer_mapping  = ConsumerMapping
                            }) ->
    CurrentConsumers = maps:size(ConsumerMapping),
    case maps:find(ConsumerTag, ConsumerMapping) of
        error when CurrentConsumers >= MaxConsumers ->  % false when MaxConsumers is 'infinity'
            rabbit_misc:protocol_error(not_allowed,
                                       "reached maximum (~B) of consumers per channel",
                                       [MaxConsumers]);
        error ->
            QueueName = qbin_to_resource(QueueNameBin, VHostPath),
            check_read_permitted(QueueName, User, AuthzContext),
            ActualTag = case ConsumerTag of
                            <<>> ->
                                rabbit_guid:binary(
                                  rabbit_guid:gen_secure(), "amq.ctag");
                            _ ->
                                ConsumerTag
                        end,
            basic_consume(QueueName, NoAck, ConsumerPrefetch, ActualTag,
                          ExclusiveConsume, Args, NoWait, State);
        {ok, _} ->
            %% Attempted reuse of consumer tag.
            rabbit_misc:protocol_error(not_allowed,
                                       "attempt to reuse consumer tag '~ts'",
                                       [ConsumerTag])
    end;

handle_method(#'basic.cancel'{consumer_tag = ConsumerTag, nowait = NoWait},
              _, State = #ch{cfg = #conf{user = #user{username = Username}},
                             consumer_mapping = ConsumerMapping,
                             queue_consumers  = QCons,
                             queue_states     = QueueStates0}) ->
    OkMsg = #'basic.cancel_ok'{consumer_tag = ConsumerTag},
    case maps:find(ConsumerTag, ConsumerMapping) of
        error ->
            %% Spec requires we ignore this situation.
            return_ok(State, NoWait, OkMsg);
        {ok, {Q, _CParams}} when ?is_amqqueue(Q) ->
            QName = amqqueue:get_name(Q),

            ConsumerMapping1 = maps:remove(ConsumerTag, ConsumerMapping),
            QCons1 =
                case maps:find(QName, QCons) of
                    error       -> QCons;
                    {ok, CTags} -> CTags1 = gb_sets:delete(ConsumerTag, CTags),
                                   case gb_sets:is_empty(CTags1) of
                                       true  -> maps:remove(QName, QCons);
                                       false -> maps:put(QName, CTags1, QCons)
                                   end
                end,
            NewState = State#ch{consumer_mapping = ConsumerMapping1,
                                queue_consumers  = QCons1},
            %% In order to ensure that no more messages are sent to
            %% the consumer after the cancel_ok has been sent, we get
            %% the queue process to send the cancel_ok on our
            %% behalf. If we were sending the cancel_ok ourselves it
            %% might overtake a message sent previously by the queue.
            case rabbit_misc:with_exit_handler(
                   fun () -> {error, not_found} end,
                   fun () ->
                           rabbit_queue_type:cancel(
                             Q, #{consumer_tag => ConsumerTag,
                                  ok_msg => ok_msg(NoWait, OkMsg),
                                  user => Username}, QueueStates0)
                   end) of
                {ok, QueueStates} ->
                    rabbit_global_counters:consumer_deleted(amqp091),
                    {noreply, NewState#ch{queue_states = QueueStates}};
                {error, not_found} ->
                    %% Spec requires we ignore this situation.
                    return_ok(NewState, NoWait, OkMsg)
            end
    end;

handle_method(#'basic.qos'{prefetch_size = Size}, _, _State) when Size /= 0 ->
    rabbit_misc:protocol_error(not_implemented,
                               "prefetch_size!=0 (~w)", [Size]);

handle_method(#'basic.qos'{global         = false,
                           prefetch_count = PrefetchCount},
              _, State = #ch{cfg = Cfg,
                             limiter = Limiter}) ->
    %% Ensures that if default was set, it's overridden
    Limiter1 = rabbit_limiter:unlimit_prefetch(Limiter),
    case rabbit_limiter:is_active(Limiter) of
        true  -> rabbit_amqqueue:deactivate_limit_all(
                   classic_consumer_queue_pids(State#ch.consumer_mapping), self());
        false -> ok
    end,
    {reply, #'basic.qos_ok'{}, State#ch{cfg = Cfg#conf{consumer_prefetch = PrefetchCount},
                                        limiter = Limiter1}};

handle_method(#'basic.qos'{global         = true,
                           prefetch_count = 0} = Method,
              Content,
              State = #ch{limiter = Limiter}) ->
    case is_global_qos_permitted() of
        true ->
            Limiter1 = rabbit_limiter:unlimit_prefetch(Limiter),
            case rabbit_limiter:is_active(Limiter) of
                true  -> rabbit_amqqueue:deactivate_limit_all(
                           classic_consumer_queue_pids(State#ch.consumer_mapping), self());
                false -> ok
            end,
            {reply, #'basic.qos_ok'{}, State#ch{limiter = Limiter1}};
        false ->
            Method1 = Method#'basic.qos'{global = false},
            handle_method(Method1, Content, State)
    end;

handle_method(#'basic.qos'{global         = true,
                           prefetch_count = PrefetchCount} = Method,
              Content,
              State = #ch{limiter = Limiter, unacked_message_q = UAMQ}) ->
    case is_global_qos_permitted() of
        true ->
            %% TODO ?QUEUE:len(UAMQ) is not strictly right since that counts
            %% unacked messages from basic.get too. Pretty obscure though.
            Limiter1 = rabbit_limiter:limit_prefetch(Limiter,
                                                     PrefetchCount, ?QUEUE:len(UAMQ)),
            case ((not rabbit_limiter:is_active(Limiter)) andalso
                  rabbit_limiter:is_active(Limiter1)) of
                true  -> rabbit_amqqueue:activate_limit_all(
                           classic_consumer_queue_pids(State#ch.consumer_mapping), self());
                false -> ok
            end,
            {reply, #'basic.qos_ok'{}, State#ch{limiter = Limiter1}};
        false ->
            Method1 = Method#'basic.qos'{global = false},
            handle_method(Method1, Content, State)
    end;

handle_method(#'basic.recover_async'{requeue = true},
              _, State = #ch{unacked_message_q = UAMQ,
                             limiter = Limiter,
                             queue_states = QueueStates0}) ->
    OkFun = fun () -> ok end,
    UAMQL = ?QUEUE:to_list(UAMQ),

    {QueueStates, Actions} =
        foreach_per_queue(
          fun ({QRef, CTag}, MsgIds, {Acc0, Actions0}) ->
                  rabbit_misc:with_exit_handler(
                    OkFun,
                    fun () ->
                            {ok, Acc, Act} = rabbit_queue_type:settle(
                                               QRef, requeue, CTag, MsgIds, Acc0),
                            {Acc, Act ++ Actions0}
                    end)
          end, lists:reverse(UAMQL), {QueueStates0, []}),
    ok = notify_limiter(Limiter, UAMQL),
    State1 = handle_queue_actions(Actions, State#ch{unacked_message_q = ?QUEUE:new(),
                                                    queue_states = QueueStates}),
    %% No answer required - basic.recover is the newer, synchronous
    %% variant of this method
    {noreply, State1};

handle_method(#'basic.recover_async'{requeue = false}, _, _State) ->
    rabbit_misc:protocol_error(not_implemented, "requeue=false", []);

handle_method(#'basic.recover'{requeue = Requeue}, Content, State) ->
    {noreply, State1} = handle_method(#'basic.recover_async'{requeue = Requeue},
                                      Content, State),
    {reply, #'basic.recover_ok'{}, State1};

handle_method(#'basic.reject'{delivery_tag = DeliveryTag, requeue = Requeue},
              _, State) ->
    reject(DeliveryTag, Requeue, false, State);

handle_method(#'exchange.declare'{nowait = NoWait} = Method,
              _, State = #ch{cfg = #conf{virtual_host = VHostPath,
                                         user = User,
                                         queue_collector_pid = CollectorPid,
                                         conn_pid = ConnPid,
                                         authz_context = AuthzContext}}) ->
    handle_method(Method, ConnPid, AuthzContext, CollectorPid, VHostPath, User),
    return_ok(State, NoWait, #'exchange.declare_ok'{});

handle_method(#'exchange.delete'{nowait = NoWait} = Method,
              _, State = #ch{cfg = #conf{conn_pid = ConnPid,
                                         authz_context = AuthzContext,
                                         virtual_host = VHostPath,
                                         queue_collector_pid = CollectorPid,
                                         user = User}}) ->
    handle_method(Method, ConnPid, AuthzContext, CollectorPid, VHostPath, User),
    return_ok(State, NoWait,  #'exchange.delete_ok'{});

handle_method(#'exchange.bind'{nowait = NoWait} = Method,
              _, State = #ch{cfg = #conf{virtual_host = VHostPath,
                                         conn_pid = ConnPid,
                                         authz_context = AuthzContext,
                                         queue_collector_pid = CollectorPid,
                                         user = User}}) ->
    handle_method(Method, ConnPid, AuthzContext, CollectorPid, VHostPath, User),
    return_ok(State, NoWait, #'exchange.bind_ok'{});

handle_method(#'exchange.unbind'{nowait = NoWait} = Method,
              _, State = #ch{cfg = #conf{virtual_host = VHostPath,
                                         conn_pid = ConnPid,
                                         authz_context = AuthzContext,
                                         queue_collector_pid = CollectorPid,
                                         user = User}}) ->
    handle_method(Method, ConnPid, AuthzContext, CollectorPid, VHostPath, User),
    return_ok(State, NoWait, #'exchange.unbind_ok'{});

handle_method(#'queue.declare'{nowait = NoWait} = Method,
              _, State = #ch{cfg = #conf{virtual_host = VHostPath,
                                         conn_pid = ConnPid,
                                         authz_context = AuthzContext,
                                         queue_collector_pid = CollectorPid,
                                         user = User}}) ->
    {ok, QueueName, MessageCount, ConsumerCount} =
        handle_method(Method, ConnPid, AuthzContext, CollectorPid, VHostPath, User),
    return_queue_declare_ok(QueueName, NoWait, MessageCount,
                            ConsumerCount, State);

handle_method(#'queue.delete'{nowait = NoWait} = Method, _,
              State = #ch{cfg = #conf{conn_pid = ConnPid,
                                      authz_context = AuthzContext,
                                      virtual_host = VHostPath,
                                      queue_collector_pid = CollectorPid,
                                      user = User}}) ->
    {ok, PurgedMessageCount} =
        handle_method(Method, ConnPid, AuthzContext, CollectorPid, VHostPath, User),
    return_ok(State, NoWait,
              #'queue.delete_ok'{message_count = PurgedMessageCount});

handle_method(#'queue.bind'{nowait = NoWait} = Method, _,
              State = #ch{cfg = #conf{conn_pid = ConnPid,
                                      authz_context = AuthzContext,
                                      user = User,
                                      queue_collector_pid = CollectorPid,
                                      virtual_host = VHostPath}}) ->
    handle_method(Method, ConnPid, AuthzContext, CollectorPid, VHostPath, User),
    return_ok(State, NoWait, #'queue.bind_ok'{});

handle_method(#'queue.unbind'{} = Method, _,
              State = #ch{cfg = #conf{conn_pid = ConnPid,
                                      authz_context = AuthzContext,
                                      user = User,
                                      queue_collector_pid = CollectorPid,
                                      virtual_host = VHostPath}}) ->
    handle_method(Method, ConnPid, AuthzContext, CollectorPid, VHostPath, User),
    return_ok(State, false, #'queue.unbind_ok'{});

handle_method(#'queue.purge'{nowait = NoWait} = Method,
              _, State = #ch{cfg = #conf{conn_pid = ConnPid,
                                         authz_context = AuthzContext,
                                         user = User,
                                         queue_collector_pid = CollectorPid,
                                         virtual_host = VHostPath}}) ->
    case handle_method(Method, ConnPid, AuthzContext, CollectorPid,
                       VHostPath, User) of
        {ok, PurgedMessageCount} ->
            return_ok(State, NoWait,
                      #'queue.purge_ok'{message_count = PurgedMessageCount})
    end;

handle_method(#'tx.select'{}, _, #ch{confirm_enabled = true}) ->
    rabbit_misc:precondition_failed("cannot switch from confirm to tx mode");

handle_method(#'tx.select'{}, _, State = #ch{tx = none}) ->
    {reply, #'tx.select_ok'{}, State#ch{tx = new_tx()}};

handle_method(#'tx.select'{}, _, State) ->
    {reply, #'tx.select_ok'{}, State};

handle_method(#'tx.commit'{}, _, #ch{tx = none}) ->
    rabbit_misc:precondition_failed("channel is not transactional");

handle_method(#'tx.commit'{}, _, State = #ch{tx      = {Deliveries, Acks},
                                             limiter = Limiter}) ->
    State1 = ?QUEUE:fold(fun deliver_to_queues/2, State, Deliveries),
    Rev = fun (X) -> lists:reverse(lists:sort(X)) end,
    {State2, Actions2} =
        lists:foldl(fun ({ack,     A}, {Acc, Actions}) ->
                            {Acc0, Actions0} = settle_acks(Rev(A), Acc),
                            {Acc0, Actions ++ Actions0};
                        ({Requeue, A}, {Acc, Actions}) ->
                            {Acc0, Actions0} = internal_reject(Requeue, Rev(A), Limiter, Acc),
                            {Acc0, Actions ++ Actions0}
                    end, {State1, []}, lists:reverse(Acks)),
    State3 = handle_queue_actions(Actions2, State2),
    {noreply, maybe_complete_tx(State3#ch{tx = committing})};

handle_method(#'tx.rollback'{}, _, #ch{tx = none}) ->
    rabbit_misc:precondition_failed("channel is not transactional");

handle_method(#'tx.rollback'{}, _, State = #ch{unacked_message_q = UAMQ,
                                               tx = {_Msgs, Acks}}) ->
    AcksL = lists:append(lists:reverse([lists:reverse(L) || {_, L} <- Acks])),
    UAMQ1 = ?QUEUE:from_list(lists:usort(AcksL ++ ?QUEUE:to_list(UAMQ))),
    {reply, #'tx.rollback_ok'{}, State#ch{unacked_message_q = UAMQ1,
                                          tx                = new_tx()}};

handle_method(#'confirm.select'{}, _, #ch{tx = {_, _}}) ->
    rabbit_misc:precondition_failed("cannot switch from tx to confirm mode");

handle_method(#'confirm.select'{nowait = NoWait}, _, State) ->
    return_ok(State#ch{confirm_enabled = true},
              NoWait, #'confirm.select_ok'{});

handle_method(#'channel.flow'{active = true}, _, State) ->
    {reply, #'channel.flow_ok'{active = true}, State};

handle_method(#'channel.flow'{active = false}, _, _State) ->
    rabbit_misc:protocol_error(not_implemented, "active=false", []);

handle_method(_MethodRecord, _Content, _State) ->
    rabbit_misc:protocol_error(
      command_invalid, "unimplemented method", []).

%%----------------------------------------------------------------------------

%% We get the queue process to send the consume_ok on our behalf. This
%% is for symmetry with basic.cancel - see the comment in that method
%% for why.
basic_consume(QueueName, NoAck, ConsumerPrefetch, ActualConsumerTag,
              ExclusiveConsume, Args, NoWait,
              State0 = #ch{cfg = #conf{conn_pid = ConnPid,
                                       user = #user{username = Username}},
                           limiter = Limiter,
                           consumer_mapping = ConsumerMapping,
                           queue_states = QueueStates0}) ->
    case rabbit_amqqueue:with_exclusive_access_or_die(
           QueueName, ConnPid,
           fun (Q) ->
                   {rabbit_amqqueue:basic_consume(
                      Q, NoAck, self(),
                      rabbit_limiter:pid(Limiter),
                      rabbit_limiter:is_active(Limiter),
                      ConsumerPrefetch, ActualConsumerTag,
                      ExclusiveConsume, Args,
                      ok_msg(NoWait, #'basic.consume_ok'{
                                        consumer_tag = ActualConsumerTag}),
                      Username, QueueStates0),
                    Q}
           end) of
        {{ok, QueueStates}, Q} when ?is_amqqueue(Q) ->
            rabbit_global_counters:consumer_created(amqp091),
            CM1 = maps:put(
                    ActualConsumerTag,
                    {Q, {NoAck, ConsumerPrefetch, ExclusiveConsume, Args}},
                    ConsumerMapping),
            State1 = State0#ch{consumer_mapping = CM1,
                               queue_states = QueueStates},
            State = case NoWait of
                        true ->
                            consumer_monitor(ActualConsumerTag, State1);
                        false ->
                            State1
                    end,
            {noreply, State};
        {{error, Type, Reason, ReasonArgs}, _Q} ->
            rabbit_misc:protocol_error(Type, Reason, ReasonArgs)
    end.

maybe_stat(false, Q) -> rabbit_amqqueue:stat(Q);
maybe_stat(true, _Q) -> {ok, 0, 0}.

consumer_monitor(ConsumerTag,
                 State = #ch{consumer_mapping = ConsumerMapping,
                             queue_consumers  = QCons}) ->
    {Q, _} = maps:get(ConsumerTag, ConsumerMapping),
    QRef = amqqueue:get_name(Q),
    CTags1 = case maps:find(QRef, QCons) of
                 {ok, CTags} -> gb_sets:insert(ConsumerTag, CTags);
                 error -> gb_sets:singleton(ConsumerTag)
             end,
    QCons1 = maps:put(QRef, CTags1, QCons),
    State#ch{queue_consumers = QCons1}.

handle_consuming_queue_down_or_eol(QName,
                                   State = #ch{queue_consumers = QCons}) ->
    ConsumerTags = case maps:find(QName, QCons) of
                       error       -> gb_sets:new();
                       {ok, CTags} -> CTags
                   end,
    gb_sets:fold(
      fun (CTag, StateN = #ch{}) ->
              cancel_consumer(CTag, QName, StateN)
      end, State#ch{queue_consumers = maps:remove(QName, QCons)}, ConsumerTags).

%% [0] There is a slight danger here that if a queue is deleted and
%% then recreated again the reconsume will succeed even though it was
%% not an HA failover. But the likelihood is not great and most users
%% are unlikely to care.

cancel_consumer(CTag, QName,
                State = #ch{cfg = #conf{capabilities = Capabilities},
                            consumer_mapping = CMap}) ->
    case rabbit_misc:table_lookup(
           Capabilities, <<"consumer_cancel_notify">>) of
        {bool, true} -> ok = send(#'basic.cancel'{consumer_tag = CTag,
                                                  nowait       = true}, State);
        _            -> ok
    end,
    rabbit_global_counters:consumer_deleted(amqp091),
    rabbit_event:notify(consumer_deleted, [{consumer_tag, CTag},
                                           {channel,      self()},
                                           {queue,        QName}]),
    State#ch{consumer_mapping = maps:remove(CTag, CMap)}.

binding_action_with_checks(
  Action, SourceNameBin0, DestinationType, DestinationNameBin0,
  RoutingKey, Arguments, VHostPath, ConnPid, AuthzContext,
  #user{username = Username} = User) ->
    ExchangeNameBin = strip_cr_lf(SourceNameBin0),
    DestinationNameBin = strip_cr_lf(DestinationNameBin0),
    DestinationName = name_to_resource(DestinationType, DestinationNameBin, VHostPath),
    check_write_permitted(DestinationName, User, AuthzContext),
    ExchangeName = rabbit_misc:r(VHostPath, exchange, ExchangeNameBin),
    [check_not_default_exchange(N) || N <- [DestinationName, ExchangeName]],
    check_read_permitted(ExchangeName, User, AuthzContext),
    case rabbit_exchange:lookup(ExchangeName) of
        {error, not_found} ->
            ok;
        {ok, Exchange}     ->
            check_read_permitted_on_topic(Exchange, User, RoutingKey, AuthzContext)
    end,
    Binding = #binding{source = ExchangeName,
                       destination = DestinationName,
                       key = RoutingKey,
                       args = Arguments},
    binding_action(Action, Binding, Username, ConnPid).

-spec binding_action(add | remove,
                     rabbit_types:binding(),
                     rabbit_types:username(),
                     pid()) -> ok.
binding_action(Action, Binding, Username, ConnPid) ->
    case rabbit_binding:Action(
           Binding,
           fun (_X, Q) when ?is_amqqueue(Q) ->
                   try rabbit_amqqueue:check_exclusive_access(Q, ConnPid)
                   catch exit:Reason -> {error, Reason}
                   end;
               (_X, #exchange{}) ->
                   ok
           end,
           Username) of
        {error, {resources_missing, [{not_found, Name} | _]}} ->
            rabbit_amqqueue:not_found(Name);
        {error, {resources_missing, [{absent, Q, Reason} | _]}} ->
            rabbit_amqqueue:absent(Q, Reason);
        {error, {binding_invalid, Fmt, Args}} ->
            rabbit_misc:protocol_error(precondition_failed, Fmt, Args);
        {error, #amqp_error{} = Error} ->
            rabbit_misc:protocol_error(Error);
        {error, timeout} ->
            rabbit_misc:protocol_error(
              internal_error, "Could not ~s binding due to timeout", [Action]);
        ok ->
            ok
    end.

basic_return(Content, RoutingKey, XNameBin,
             #ch{cfg = #conf{protocol = Protocol,
                             writer_pid = WriterPid}},
             Reason) ->
    {_Close, ReplyCode, ReplyText} = Protocol:lookup_amqp_exception(Reason),
    ok = rabbit_writer:send_command(WriterPid,
                                    #'basic.return'{reply_code = ReplyCode,
                                                    reply_text = ReplyText,
                                                    exchange = XNameBin,
                                                    routing_key = RoutingKey},
                                    Content).

reject(DeliveryTag, Requeue, Multiple,
       State = #ch{unacked_message_q = UAMQ, tx = Tx}) ->
    {Acked, Remaining} = collect_acks(UAMQ, DeliveryTag, Multiple),
    State1 = State#ch{unacked_message_q = Remaining},
    {noreply, case Tx of
                  none ->
                      {State2, Actions} = internal_reject(Requeue, Acked, State1#ch.limiter, State1),
                      handle_queue_actions(Actions, State2);
                  {Msgs, Acks} ->
                      Acks1 = ack_cons(Requeue, Acked, Acks),
                      State1#ch{tx = {Msgs, Acks1}}
              end}.

%% NB: Acked is in youngest-first order
internal_reject(Requeue, Acked, Limiter,
                State = #ch{queue_states = QueueStates0}) ->
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
    ok = notify_limiter(Limiter, Acked),
    {State#ch{queue_states = QueueStates}, Actions}.

record_sent(Type, QueueType, Tag, AckRequired,
            Msg = {QName, _QPid, MsgId, Redelivered, _Message},
            State = #ch{cfg = #conf{channel = ChannelNum,
                                    trace_state = TraceState,
                                    user = #user{username = Username},
                                    conn_name = ConnName
                                   },
                        unacked_message_q = UAMQ,
                        next_tag          = DeliveryTag
                       }) ->
    rabbit_global_counters:messages_delivered(amqp091, QueueType, 1),
    ?INCR_STATS(queue_stats, QName, 1,
                case {Type, AckRequired} of
                    {get, true} ->
                        rabbit_global_counters:messages_delivered_get_manual_ack(amqp091, QueueType, 1),
                        get;
                    {get, false} ->
                        rabbit_global_counters:messages_delivered_get_auto_ack(amqp091, QueueType, 1),
                        get_no_ack;
                    {deliver, true} ->
                        rabbit_global_counters:messages_delivered_consume_manual_ack(amqp091, QueueType, 1),
                        deliver;
                    {deliver, false} ->
                        rabbit_global_counters:messages_delivered_consume_auto_ack(amqp091, QueueType, 1),
                        deliver_no_ack
                end, State),
    case Redelivered of
        true ->
            rabbit_global_counters:messages_redelivered(amqp091, QueueType, 1),
            ?INCR_STATS(queue_stats, QName, 1, redeliver, State);
        false ->
            ok
    end,
    rabbit_trace:tap_out(Msg, ConnName, ChannelNum, Username, TraceState),
    UAMQ1 = case AckRequired of
                true ->
                    DeliveredAt = erlang:monotonic_time(millisecond),
                    ?QUEUE:in(#pending_ack{delivery_tag = DeliveryTag,
                                           tag = Tag,
                                           delivered_at = DeliveredAt,
                                           queue = QName,
                                           msg_id = MsgId}, UAMQ);
                false ->
                    UAMQ
            end,
    State#ch{unacked_message_q = UAMQ1, next_tag = DeliveryTag + 1}.

%% Records a client-sent acknowledgement. Handles both single delivery acks
%% and multi-acks.
%%
%% Returns a tuple of acknowledged pending acks and remaining pending acks.
%% Sorts each group in the youngest-first order (descending by delivery tag).
%% The special case for 0 comes from the AMQP 0-9-1 spec: if the multiple field is set to 1 (true),
%% and the delivery tag is 0, this indicates acknowledgement of all outstanding messages (by a client).
collect_acks(UAMQ, 0, true) ->
    {lists:reverse(?QUEUE:to_list(UAMQ)), ?QUEUE:new()};
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
            rabbit_misc:precondition_failed("unknown delivery tag ~w", [DeliveryTag])
    end.

%% Settles (acknowledges) messages at the queue replica process level.
%% This happens in the oldest-first order (ascending by delivery tag).
settle_acks(Acks, State = #ch{queue_states = QueueStates0}) ->
    {QueueStates, Actions} =
        foreach_per_queue(
          fun ({QRef, CTag}, MsgIds, {Acc0, ActionsAcc0}) ->
                  case rabbit_queue_type:settle(QRef, complete, CTag,
                                                MsgIds, Acc0) of
                      {ok, Acc, ActionsAcc} ->
                          incr_queue_stats(QRef, MsgIds, State),
                          {Acc, ActionsAcc0 ++ ActionsAcc};
                      {protocol_error, ErrorType, Reason, ReasonArgs} ->
                          rabbit_misc:protocol_error(ErrorType, Reason, ReasonArgs)
                  end
          end, Acks, {QueueStates0, []}),
    ok = notify_limiter(State#ch.limiter, Acks),
    {State#ch{queue_states = QueueStates}, Actions}.

incr_queue_stats(QName, MsgIds, State = #ch{queue_states = QueueStates}) ->
    Count = length(MsgIds),
    case rabbit_queue_type:module(QName, QueueStates) of
        {ok, QueueType} ->
            rabbit_global_counters:messages_acknowledged(amqp091, QueueType, Count);
        _ ->
            noop
    end,
    ?INCR_STATS(queue_stats, QName, Count, ack, State).

%% {Msgs, Acks}
%%
%% Msgs is a queue.
%%
%% Acks looks s.t. like this:
%% [{false,[5,4]},{true,[3]},{ack,[2,1]}, ...]
%%
%% Each element is a pair consisting of a tag and a list of
%% ack'ed/reject'ed msg ids. The tag is one of 'ack' (to ack), 'true'
%% (reject w requeue), 'false' (reject w/o requeue). The msg ids, as
%% well as the list overall, are in "most-recent (generally youngest)
%% ack first" order.
new_tx() -> {?QUEUE:new(), []}.

notify_queues(State = #ch{cfg = #conf{state = closing}}) ->
    {ok, State};
notify_queues(State = #ch{consumer_mapping  = Consumers,
                          cfg = Cfg}) ->
    QPids = classic_consumer_queue_pids(Consumers),
    Timeout = get_operation_timeout(),
    {rabbit_amqqueue:notify_down_all(QPids, self(), Timeout),
     State#ch{cfg = Cfg#conf{state = closing}}}.

foreach_per_queue(_F, [], Acc) ->
    Acc;
foreach_per_queue(F, [#pending_ack{tag = CTag,
                                   queue = QName,
                                   msg_id = MsgId}], Acc) ->
    %% TODO: fix this abstraction leak
    %% quorum queue, needs the consumer tag
    F({QName, CTag}, [MsgId], Acc);
foreach_per_queue(F, UAL, Acc) ->
    T = lists:foldl(fun (#pending_ack{tag = CTag,
                                      queue = QName,
                                      msg_id = MsgId}, T) ->
                            rabbit_misc:gb_trees_cons({QName, CTag}, MsgId, T)
                    end, gb_trees:empty(), UAL),
    rabbit_misc:gb_trees_fold(fun (Key, Val, Acc0) -> F(Key, Val, Acc0) end, Acc, T).

%% hack to patch up missing queue type behaviour for classic queue
classic_consumer_queue_pids(Consumers) ->
    lists:usort([amqqueue:get_pid(Q)
                 || {Q, _CParams} <- maps:values(Consumers),
                    amqqueue:get_type(Q) == rabbit_classic_queue]).

%% tell the limiter about the number of acks that have been received
%% for messages delivered to subscribed consumers, but not acks for
%% messages sent in a response to a basic.get (identified by their
%% consumer tag as an integer (the same as the delivery tag, required
%% quorum queues))
notify_limiter(Limiter, Acked) ->
    %% optimisation: avoid the potentially expensive 'foldl' in the
    %% common case.
     case rabbit_limiter:is_active(Limiter) of
        false -> ok;
        true  -> case lists:foldl(fun (#pending_ack{tag = CTag}, Acc) when is_integer(CTag) ->
                                          %% TODO: fix absctraction leak
                                          %% Quorum queues use integer CTags
                                          %% classic queues use binaries
                                          %% Quorum queues do not interact
                                          %% with limiters
                                          Acc;
                                      (_, Acc) -> Acc + 1
                                  end, 0, Acked) of
                     0     -> ok;
                     Count -> rabbit_limiter:ack(Limiter, Count)
                 end
    end.

deliver_to_queues({Message, _Options, _RoutedToQueues} = Delivery,
                  #ch{cfg = #conf{virtual_host = VHost}} = State) ->
    XName = rabbit_misc:r(VHost, exchange, mc:exchange(Message)),
    deliver_to_queues(XName, Delivery, State).

deliver_to_queues(XName,
                  {_Message, #{mandatory := false} = Options, _RoutedToQueues = []},
                  State)
  when not is_map_key(correlation, Options) -> %% optimisation when there are no queues
    ?INCR_STATS(exchange_stats, XName, 1, publish, State),
    rabbit_global_counters:messages_unroutable_dropped(amqp091, 1),
    ?INCR_STATS(exchange_stats, XName, 1, drop_unroutable, State),
    State;
deliver_to_queues(XName,
                  {Message, Options0, RoutedToQueues},
                  State0 = #ch{queue_states = QueueStates0}) ->
    {Mandatory, Options} = maps:take(mandatory, Options0),
    Qs = rabbit_amqqueue:prepend_extra_bcc(RoutedToQueues),
    case rabbit_queue_type:deliver(Qs, Message, Options, QueueStates0) of
        {ok, QueueStates, Actions} ->
            rabbit_global_counters:messages_routed(amqp091, length(Qs)),
            QueueNames = rabbit_amqqueue:queue_names(Qs),
            %% NB: the order here is important since basic.returns must be
            %% sent before confirms.
            ok = process_routing_mandatory(Mandatory, RoutedToQueues, Message, XName, State0),
            MsgSeqNo = maps:get(correlation, Options, undefined),
            State1 = process_routing_confirm(MsgSeqNo, QueueNames, XName, State0),
            %% Actions must be processed after registering confirms as actions may
            %% contain rejections of publishes
            State = handle_queue_actions(Actions, State1#ch{queue_states = QueueStates}),
            case rabbit_event:stats_level(State, #ch.stats_timer) of
                fine ->
                    ?INCR_STATS(exchange_stats, XName, 1, publish),
                    lists:foreach(fun(QName) ->
                                          ?INCR_STATS(queue_exchange_stats, {QName, XName}, 1, publish)
                                  end, QueueNames);
                _ ->
                    ok
            end,
            State;
        {error, {stream_not_found, Resource}} ->
            rabbit_misc:protocol_error(
              resource_error,
              "Stream not found for ~ts",
              [rabbit_misc:rs(Resource)]);
        {error, {coordinator_unavailable, Resource}} ->
            rabbit_misc:protocol_error(
              resource_error,
              "Stream coordinator unavailable for ~ts",
              [rabbit_misc:rs(Resource)])
    end.

process_routing_mandatory(_Mandatory = true,
                          _RoutedToQs = [],
                          Msg,
                          XName,
                          State) ->
    rabbit_global_counters:messages_unroutable_returned(amqp091, 1),
    ?INCR_STATS(exchange_stats, XName, 1, return_unroutable, State),
    Content = mc:protocol_state(Msg),
    [RoutingKey | _] = mc:routing_keys(Msg),
    ok = basic_return(Content, RoutingKey, XName#resource.name, State, no_route);
process_routing_mandatory(_Mandatory = false,
                          _RoutedToQs = [],
                          _Msg,
                          XName,
                          State) ->
    rabbit_global_counters:messages_unroutable_dropped(amqp091, 1),
    ?INCR_STATS(exchange_stats, XName, 1, drop_unroutable, State),
    ok;
process_routing_mandatory(_, _, _, _, _) ->
    ok.

process_routing_confirm(undefined, _, _, State) ->
    State;
process_routing_confirm(MsgSeqNo, [], XName, State)
  when is_integer(MsgSeqNo) ->
    record_confirms([{MsgSeqNo, XName}], State);
process_routing_confirm(MsgSeqNo, QRefs, XName, State)
  when is_integer(MsgSeqNo) ->
    State#ch{unconfirmed =
             rabbit_confirms:insert(MsgSeqNo, QRefs, XName, State#ch.unconfirmed)}.

confirm(MsgSeqNos, QRef, State = #ch{unconfirmed = UC}) ->
    %% NOTE: if queue name does not exist here it's likely that the ref also
    %% does not exist in unconfirmed messages.
    %% Neither does the 'ignore' atom, so it's a reasonable fallback.
    {ConfirmMXs, UC1} = rabbit_confirms:confirm(MsgSeqNos, QRef, UC),
    %% NB: don't call noreply/1 since we don't want to send confirms.
    record_confirms(ConfirmMXs, State#ch{unconfirmed = UC1}).

send_confirms_and_nacks(State = #ch{tx = none, confirmed = [], rejected = []}) ->
    State;
send_confirms_and_nacks(State = #ch{tx = none, confirmed = C, rejected = R}) ->
    case rabbit_node_monitor:pause_partition_guard() of
        ok      ->
            Confirms = lists:append(C),
            rabbit_global_counters:messages_confirmed(amqp091, length(Confirms)),
            Rejects = lists:append(R),
            ConfirmMsgSeqNos =
                lists:foldl(
                    fun ({MsgSeqNo, XName}, MSNs) ->
                        ?INCR_STATS(exchange_stats, XName, 1, confirm, State),
                        [MsgSeqNo | MSNs]
                    end, [], Confirms),
            RejectMsgSeqNos = [MsgSeqNo || {MsgSeqNo, _} <- Rejects],

            State1 = send_confirms(ConfirmMsgSeqNos,
                                   RejectMsgSeqNos,
                                   State#ch{confirmed = []}),
            %% TODO: msg seq nos, same as for confirms. Need to implement
            %% nack rates first.
            send_nacks(RejectMsgSeqNos,
                       ConfirmMsgSeqNos,
                       State1#ch{rejected = []});
        pausing -> State
    end;
send_confirms_and_nacks(State) ->
    case rabbit_node_monitor:pause_partition_guard() of
        ok      -> maybe_complete_tx(State);
        pausing -> State
    end.

send_nacks([], _, State) ->
    State;
send_nacks(_Rs, _, State = #ch{cfg = #conf{state = closing}}) -> %% optimisation
    State;
send_nacks(Rs, Cs, State) ->
    coalesce_and_send(Rs, Cs,
                      fun(MsgSeqNo, Multiple) ->
                              #'basic.nack'{delivery_tag = MsgSeqNo,
                                            multiple     = Multiple}
                      end, State).

send_confirms([], _, State) ->
    State;
send_confirms(_Cs, _, State = #ch{cfg = #conf{state = closing}}) -> %% optimisation
    State;
send_confirms([MsgSeqNo], _, State) ->
    ok = send(#'basic.ack'{delivery_tag = MsgSeqNo}, State),
    State;
send_confirms(Cs, Rs, State) ->
    coalesce_and_send(Cs, Rs,
                      fun(MsgSeqNo, Multiple) ->
                                  #'basic.ack'{delivery_tag = MsgSeqNo,
                                               multiple     = Multiple}
                      end, State).

coalesce_and_send(MsgSeqNos, NegativeMsgSeqNos, MkMsgFun, State = #ch{unconfirmed = UC}) ->
    SMsgSeqNos = lists:usort(MsgSeqNos),
    UnconfirmedCutoff = case rabbit_confirms:is_empty(UC) of
                 true  -> lists:last(SMsgSeqNos) + 1;
                 false -> rabbit_confirms:smallest(UC)
             end,
    Cutoff = lists:min([UnconfirmedCutoff | NegativeMsgSeqNos]),
    {Ms, Ss} = lists:splitwith(fun(X) -> X < Cutoff end, SMsgSeqNos),
    case Ms of
        [] -> ok;
        _  -> ok = send(MkMsgFun(lists:last(Ms), true), State)
    end,
    [ok = send(MkMsgFun(SeqNo, false), State) || SeqNo <- Ss],
    State.

ack_cons(Tag, Acked, [{Tag, Acks} | L]) -> [{Tag, Acked ++ Acks} | L];
ack_cons(Tag, Acked, Acks)              -> [{Tag, Acked} | Acks].

ack_len(Acks) -> lists:sum([length(L) || {ack, L} <- Acks]).

maybe_complete_tx(State = #ch{tx = {_, _}}) ->
    State;
maybe_complete_tx(State = #ch{unconfirmed = UC}) ->
    case rabbit_confirms:is_empty(UC) of
        false -> State;
        true  -> complete_tx(State#ch{confirmed = []})
    end.

complete_tx(State = #ch{tx = committing}) ->
    ok = send(#'tx.commit_ok'{}, State),
    State#ch{tx = new_tx()};
complete_tx(State = #ch{tx = failed}) ->
    {noreply, State1} = handle_exception(
                          rabbit_misc:amqp_error(
                            precondition_failed, "partial tx completion", [],
                            'tx.commit'),
                          State),
    State1#ch{tx = new_tx()}.

infos(Items, State) -> [{Item, i(Item, State)} || Item <- Items].

infos(Items, Deadline, State) ->
    [begin
         Now = now_millis(),
         if
             Now > Deadline ->
                 throw(timeout);
             true ->
                {Item, i(Item, State)}
         end
     end || Item <- Items].

i(pid,            _)                               -> self();
i(connection,     #ch{cfg = #conf{conn_pid = ConnPid}}) -> ConnPid;
i(number,         #ch{cfg =  #conf{channel = Channel}}) -> Channel;
i(user,           #ch{cfg = #conf{user = User}}) -> User#user.username;
i(user_who_performed_action, Ch) -> i(user, Ch);
i(vhost,          #ch{cfg = #conf{virtual_host = VHost}}) -> VHost;
i(transactional,  #ch{tx               = Tx})      -> Tx =/= none;
i(confirm,        #ch{confirm_enabled  = CE})      -> CE;
i(name,           State)                           -> name(State);
i(consumer_count,          #ch{consumer_mapping = CM})    -> maps:size(CM);
i(messages_unconfirmed,    #ch{unconfirmed = UC})         -> rabbit_confirms:size(UC);
i(messages_unacknowledged, #ch{unacked_message_q = UAMQ}) -> ?QUEUE:len(UAMQ);
i(messages_uncommitted,    #ch{tx = {Msgs, _Acks}})       -> ?QUEUE:len(Msgs);
i(messages_uncommitted,    #ch{})                         -> 0;
i(acks_uncommitted,        #ch{tx = {_Msgs, Acks}})       -> ack_len(Acks);
i(acks_uncommitted,        #ch{})                         -> 0;
i(pending_raft_commands,   #ch{queue_states = QS}) ->
    pending_raft_commands(QS);
i(cached_segments,   #ch{queue_states = QS}) ->
    cached_segments(QS);
i(state,                   #ch{cfg = #conf{state = running}}) -> credit_flow:state();
i(state,                   #ch{cfg = #conf{state = State}}) -> State;
i(prefetch_count,          #ch{cfg = #conf{consumer_prefetch = C}})    -> C;
%% Retained for backwards compatibility e.g. in mixed version clusters,
%% can be removed starting with 4.2. MK.
i(global_prefetch_count, #ch{limiter = Limiter}) ->
    rabbit_limiter:get_prefetch_limit(Limiter);
i(interceptors, #ch{interceptor_state = IState}) ->
    IState;
i(garbage_collection, _State) ->
    rabbit_misc:get_gc_info(self());
i(reductions, _State) ->
    {reductions, Reductions} = erlang:process_info(self(), reductions),
    Reductions;
i(Item, _) ->
    throw({bad_argument, Item}).

pending_raft_commands(QStates) ->
    Fun = fun(_, V, Acc) ->
                  case rabbit_queue_type:state_info(V) of
                      #{pending_raft_commands := P} ->
                          Acc + P;
                      _ ->
                          Acc
                  end
          end,
    rabbit_queue_type:fold_state(Fun, 0, QStates).

cached_segments(QStates) ->
    Fun = fun(_, V, Acc) ->
                  case rabbit_queue_type:state_info(V) of
                      #{cached_segments := P} ->
                          Acc + P;
                      _ ->
                          Acc
                  end
          end,
    rabbit_queue_type:fold_state(Fun, 0, QStates).

name(#ch{cfg = #conf{conn_name = ConnName, channel = Channel}}) ->
    list_to_binary(rabbit_misc:format("~ts (~tp)", [ConnName, Channel])).

emit_stats(State) -> emit_stats(State, []).

emit_stats(State, Extra) ->
    [{reductions, Red} | Coarse0] = infos(?STATISTICS_KEYS, State),
    %% First metric must be `idle_since` (if available), as expected by
    %% `rabbit_mgmt_format:format_channel_stats`. This is a performance
    %% optimisation that avoids traversing the whole list when only
    %% one element has to be formatted.
    rabbit_core_metrics:channel_stats(self(), Extra ++ Coarse0),
    rabbit_core_metrics:channel_stats(reductions, self(), Red).

erase_queue_stats(QName) ->
    rabbit_core_metrics:channel_queue_down({self(), QName}),
    erase({queue_stats, QName}),
    [begin
	 rabbit_core_metrics:channel_queue_exchange_down({self(), QX}),
	 erase({queue_exchange_stats, QX})
     end || {{queue_exchange_stats, QX = {QName0, _}}, _} <- get(),
	    QName0 =:= QName].

get_vhost(#ch{cfg = #conf{virtual_host = VHost}}) -> VHost.

get_user(#ch{cfg = #conf{user = User}}) -> User.

delete_stats({queue_stats, QName}) ->
    rabbit_core_metrics:channel_queue_down({self(), QName});
delete_stats({exchange_stats, XName}) ->
    rabbit_core_metrics:channel_exchange_down({self(), XName});
delete_stats({queue_exchange_stats, QX}) ->
    rabbit_core_metrics:channel_queue_exchange_down({self(), QX});
delete_stats(_) ->
    ok.

put_operation_timeout() ->
    put(channel_operation_timeout, ?CHANNEL_OPERATION_TIMEOUT).

get_operation_timeout() ->
    get(channel_operation_timeout).

%% Refactored and exported to allow direct calls from the HTTP API,
%% avoiding the usage of AMQP 0-9-1 from the management.

handle_method(#'exchange.bind'{destination = DestinationNameBin,
                               source      = SourceNameBin,
                               routing_key = RoutingKey,
                               arguments   = Arguments},
              ConnPid, AuthzContext, _CollectorId, VHostPath, User) ->
    binding_action_with_checks(
      add, SourceNameBin, exchange, DestinationNameBin,
      RoutingKey, Arguments, VHostPath, ConnPid, AuthzContext, User);
handle_method(#'exchange.unbind'{destination = DestinationNameBin,
                                 source      = SourceNameBin,
                                 routing_key = RoutingKey,
                                 arguments   = Arguments},
              ConnPid, AuthzContext, _CollectorId, VHostPath, User) ->
    binding_action_with_checks(
      remove, SourceNameBin, exchange, DestinationNameBin,
      RoutingKey, Arguments, VHostPath, ConnPid, AuthzContext, User);
handle_method(#'queue.unbind'{queue       = QueueNameBin,
                              exchange    = ExchangeNameBin,
                              routing_key = RoutingKey,
                              arguments   = Arguments},
              ConnPid, AuthzContext, _CollectorId, VHostPath, User) ->
    binding_action_with_checks(
      remove, ExchangeNameBin, queue, QueueNameBin,
      RoutingKey, Arguments, VHostPath, ConnPid, AuthzContext, User);
handle_method(#'queue.bind'{queue       = QueueNameBin,
                            exchange    = ExchangeNameBin,
                            routing_key = RoutingKey,
                            arguments   = Arguments},
              ConnPid, AuthzContext, _CollectorId, VHostPath, User) ->
    binding_action_with_checks(
      add, ExchangeNameBin, queue, QueueNameBin,
      RoutingKey, Arguments, VHostPath, ConnPid, AuthzContext, User);
%% Note that all declares to these are effectively passive. If it
%% exists it by definition has one consumer.
handle_method(#'queue.declare'{queue   = <<"amq.rabbitmq.reply-to",
                                           _/binary>> = QueueNameBin},
              _ConnPid, _AuthzContext, _CollectorPid, VHost, _User) ->
    StrippedQueueNameBin = strip_cr_lf(QueueNameBin),
    QueueName = rabbit_misc:r(VHost, queue, StrippedQueueNameBin),
    case declare_fast_reply_to(StrippedQueueNameBin) of
        exists    -> {ok, QueueName, 0, 1};
        not_found -> rabbit_amqqueue:not_found(QueueName)
    end;
handle_method(#'queue.declare'{queue       = QueueNameBin,
                               passive     = false,
                               durable     = DurableDeclare,
                               exclusive   = ExclusiveDeclare,
                               auto_delete = AutoDelete,
                               nowait      = NoWait,
                               arguments   = Args0} = Declare,
              ConnPid, AuthzContext, CollectorPid, VHostPath,
              #user{username = Username} = User) ->
    Owner = case ExclusiveDeclare of
                true  -> ConnPid;
                false -> none
            end,
    Args = rabbit_amqqueue:augment_declare_args(VHostPath,
                                                DurableDeclare,
                                                ExclusiveDeclare,
                                                AutoDelete,
                                                Args0),
    StrippedQueueNameBin = strip_cr_lf(QueueNameBin),
    Durable = DurableDeclare andalso not ExclusiveDeclare,
    Kind = queue,
    ActualNameBin = case StrippedQueueNameBin of
                        <<>> ->
                            case rabbit_amqqueue:is_server_named_allowed(Args) of
                                true ->
                                    rabbit_guid:binary(rabbit_guid:gen_secure(), "amq.gen");
                                false ->
                                    rabbit_misc:protocol_error(
                                      precondition_failed,
                                      "Cannot declare a server-named queue for type ~tp",
                                      [rabbit_amqqueue:get_queue_type(Args)])
                            end;
                        Other -> check_name(Kind, Other)
                    end,
    QueueName = rabbit_misc:r(VHostPath, Kind, ActualNameBin),
    check_configure_permitted(QueueName, User, AuthzContext),
    rabbit_core_metrics:queue_declared(QueueName),
    case rabbit_amqqueue:with(
           QueueName,
           fun (Q) -> ok = rabbit_amqqueue:assert_equivalence(
                             Q, Durable, AutoDelete, Args, Owner),
                      maybe_stat(NoWait, Q)
           end) of
        {ok, MessageCount, ConsumerCount} ->
            {ok, QueueName, MessageCount, ConsumerCount};
        {error, not_found} ->
            %% enforce the limit for newly declared queues only
            DlxKey = <<"x-dead-letter-exchange">>,
            case rabbit_misc:r_arg(VHostPath, exchange, Args, DlxKey) of
               undefined ->
                   ok;
               {error, {invalid_type, Type}} ->
                    rabbit_misc:precondition_failed(
                      "invalid type '~ts' for arg '~ts' in ~ts",
                      [Type, DlxKey, rabbit_misc:rs(QueueName)]);
               DLX ->
                   check_read_permitted(QueueName, User, AuthzContext),
                   check_write_permitted(DLX, User, AuthzContext),
                   ok
            end,
            case rabbit_amqqueue:declare(QueueName, Durable, AutoDelete,
                                         Args, Owner, Username) of
                {new, Q} when ?is_amqqueue(Q) ->
                    %% We need to notify the reader within the channel
                    %% process so that we can be sure there are no
                    %% outstanding exclusive queues being declared as
                    %% the connection shuts down.
                    QPid = amqqueue:get_pid(Q),
                    ok = case {Owner, CollectorPid} of
                             {none, _} -> ok;
                             {_, none} -> ok; %% Supports call from mgmt API
                             _    -> rabbit_queue_collector:register(
                                       CollectorPid, QPid)
                         end,
                    rabbit_core_metrics:queue_created(QueueName),
                    {ok, QueueName, 0, 0};
                {existing, _Q} ->
                    %% must have been created between the stat and the
                    %% declare. Loop around again.
                    handle_method(Declare, ConnPid, AuthzContext, CollectorPid, VHostPath,
                                  User);
                {absent, Q, Reason} ->
                    rabbit_amqqueue:absent(Q, Reason);
                {owner_died, _Q} ->
                    %% Presumably our own days are numbered since the
                    %% connection has died. Pretend the queue exists though,
                    %% just so nothing fails.
                    {ok, QueueName, 0, 0};
                {error, queue_limit_exceeded, Reason, ReasonArgs} ->
                    rabbit_misc:precondition_failed(Reason, ReasonArgs);
                {protocol_error, ErrorType, Reason, ReasonArgs} ->
                    rabbit_misc:protocol_error(ErrorType, Reason, ReasonArgs)
            end;
        {error, {absent, Q, Reason}} ->
            rabbit_amqqueue:absent(Q, Reason)
    end;
handle_method(#'queue.declare'{queue   = QueueNameBin,
                               nowait  = NoWait,
                               passive = true},
              ConnPid, _AuthzContext, _CollectorPid, VHostPath, _User) ->
    StrippedQueueNameBin = strip_cr_lf(QueueNameBin),
    QueueName = rabbit_misc:r(VHostPath, queue, StrippedQueueNameBin),
    Fun = fun (Q0) ->
              QStat = maybe_stat(NoWait, Q0),
              {QStat, Q0}
          end,
    %% Note: no need to check if Q is an #amqqueue, with_or_die does it
    {{ok, MessageCount, ConsumerCount}, Q} = rabbit_amqqueue:with_or_die(QueueName, Fun),
    ok = rabbit_amqqueue:check_exclusive_access(Q, ConnPid),
    {ok, QueueName, MessageCount, ConsumerCount};
handle_method(#'queue.delete'{queue     = QueueNameBin,
                              if_unused = IfUnused,
                              if_empty  = IfEmpty},
              ConnPid, AuthzContext, _CollectorPid, VHostPath,
              User = #user{username = Username}) ->
    StrippedQueueNameBin = strip_cr_lf(QueueNameBin),
    QueueName = qbin_to_resource(StrippedQueueNameBin, VHostPath),

    check_configure_permitted(QueueName, User, AuthzContext),
    rabbit_amqqueue:delete_with(QueueName, ConnPid, IfUnused, IfEmpty, Username, true);
handle_method(#'exchange.delete'{exchange  = ExchangeNameBin,
                                 if_unused = IfUnused},
              _ConnPid, AuthzContext, _CollectorPid, VHostPath,
              User = #user{username = Username}) ->
    StrippedExchangeNameBin = strip_cr_lf(ExchangeNameBin),
    ExchangeName = rabbit_misc:r(VHostPath, exchange, StrippedExchangeNameBin),
    check_not_default_exchange(ExchangeName),
    check_exchange_deletion(ExchangeName),
    check_configure_permitted(ExchangeName, User, AuthzContext),
    case rabbit_exchange:ensure_deleted(ExchangeName, IfUnused, Username) of
        ok ->
            ok;
        {error, in_use} ->
            rabbit_misc:precondition_failed("~ts in use", [rabbit_misc:rs(ExchangeName)]);
        {error, timeout} ->
            rabbit_misc:protocol_error(
              internal_error,
              "failed to delete ~ts due to a timeout",
              [rabbit_misc:rs(ExchangeName)])
    end;
handle_method(#'queue.purge'{queue = QueueNameBin},
              ConnPid, AuthzContext, _CollectorPid, VHostPath, User) ->
    QueueName = qbin_to_resource(QueueNameBin, VHostPath),
    check_read_permitted(QueueName, User, AuthzContext),
    rabbit_amqqueue:with_exclusive_access_or_die(
      QueueName, ConnPid,
      fun (Q) ->
              case rabbit_queue_type:purge(Q) of
                  {ok, _} = Res ->
                      Res;
                  {error, not_supported} ->
                      rabbit_misc:protocol_error(
                        not_implemented,
                        "queue.purge not supported by stream queues ~ts",
                        [rabbit_misc:rs(amqqueue:get_name(Q))])
              end
      end);
handle_method(#'exchange.declare'{exchange    = XNameBin,
                                  type        = TypeNameBin,
                                  passive     = false,
                                  durable     = Durable,
                                  auto_delete = AutoDelete,
                                  internal    = Internal,
                                  arguments   = Args},
              _ConnPid, AuthzContext, _CollectorPid, VHostPath,
              #user{username = Username} = User) ->
    CheckedType = rabbit_exchange:check_type(TypeNameBin),
    XNameBinStripped = strip_cr_lf(XNameBin),
    ExchangeName = rabbit_misc:r(VHostPath, exchange, XNameBinStripped),
    check_not_default_exchange(ExchangeName),
    check_configure_permitted(ExchangeName, User, AuthzContext),
    X = case rabbit_exchange:lookup(ExchangeName) of
            {ok, FoundX} -> FoundX;
            {error, not_found} ->
                _ = check_name('exchange', XNameBinStripped),
                AeKey = <<"alternate-exchange">>,
                case rabbit_misc:r_arg(VHostPath, exchange, Args, AeKey) of
                    undefined -> ok;
                    {error, {invalid_type, Type}} ->
                        rabbit_misc:precondition_failed(
                          "invalid type '~ts' for arg '~ts' in ~ts",
                          [Type, AeKey, rabbit_misc:rs(ExchangeName)]);
                    AName     -> check_read_permitted(ExchangeName, User, AuthzContext),
                                 check_write_permitted(AName, User, AuthzContext),
                                 ok
                end,
                case rabbit_exchange:declare(ExchangeName,
                                             CheckedType,
                                             Durable,
                                             AutoDelete,
                                             Internal,
                                             Args,
                                             Username) of
                    {ok, DeclaredX} ->
                        DeclaredX;
                    {error, timeout} ->
                        rabbit_misc:protocol_error(
                          internal_error,
                          "failed to declare ~ts because the operation "
                          "timed out",
                          [rabbit_misc:rs(ExchangeName)])
                end
        end,
    ok = rabbit_exchange:assert_equivalence(X, CheckedType, Durable,
                                            AutoDelete, Internal, Args);
handle_method(#'exchange.declare'{exchange    = ExchangeNameBin,
                                  passive     = true},
              _ConnPid, _AuthzContext, _CollectorPid, VHostPath, _User) ->
    ExchangeName = rabbit_misc:r(VHostPath, exchange, strip_cr_lf(ExchangeNameBin)),
    check_not_default_exchange(ExchangeName),
    _ = rabbit_exchange:lookup_or_die(ExchangeName).

handle_deliver(CTag, Ack, Msgs, State) when is_list(Msgs) ->
    lists:foldl(fun(Msg, S) ->
                        handle_deliver0(CTag, Ack, Msg, S)
                end, State, Msgs).

handle_deliver0(ConsumerTag, AckRequired,
                {QName, QPid, _MsgId, Redelivered, Mc} = Msg,
               State = #ch{cfg = #conf{writer_pid = WriterPid,
                                       writer_gc_threshold = GCThreshold,
                                       msg_interceptor_ctx = MsgIcptCtx},
                           next_tag   = DeliveryTag,
                           queue_states = Qs}) ->
    Exchange = mc:exchange(Mc),
    [RoutingKey | _] = mc:routing_keys(Mc),
    Content = outgoing_content(Mc, MsgIcptCtx),
    Deliver = #'basic.deliver'{consumer_tag = ConsumerTag,
                               delivery_tag = DeliveryTag,
                               redelivered  = Redelivered,
                               exchange     = Exchange,
                               routing_key  = RoutingKey},
    {ok, QueueType} = rabbit_queue_type:module(QName, Qs),
    case QueueType of
        rabbit_classic_queue ->
            ok = rabbit_writer:send_command_and_notify(
                   WriterPid, QPid, self(), Deliver, Content);
        _ ->
            ok = rabbit_writer:send_command(WriterPid, Deliver, Content)
    end,
    _ = case GCThreshold of
        undefined -> ok;
        _         -> rabbit_basic:maybe_gc_large_msg(Content, GCThreshold)
    end,
    record_sent(deliver, QueueType, ConsumerTag, AckRequired, Msg, State).

handle_basic_get(WriterPid, DeliveryTag, NoAck, MessageCount,
                 Msg0 = {_QName, _QPid, _MsgId, Redelivered, Mc},
                 QueueType, State) ->
    Exchange = mc:exchange(Mc),
    [RoutingKey | _] = mc:routing_keys(Mc),
    Content = outgoing_content(Mc, State#ch.cfg#conf.msg_interceptor_ctx),
    ok = rabbit_writer:send_command(
           WriterPid,
           #'basic.get_ok'{delivery_tag  = DeliveryTag,
                           redelivered   = Redelivered,
                           exchange      = Exchange,
                           routing_key   = RoutingKey,
                           message_count = MessageCount},
           Content),
    {noreply, record_sent(get, QueueType, DeliveryTag, not(NoAck), Msg0, State)}.

outgoing_content(Mc, MsgIcptCtx) ->
    Mc1 = rabbit_msg_interceptor:intercept_outgoing(Mc, MsgIcptCtx),
    Mc2 = mc:convert(mc_amqpl, Mc1),
    mc:protocol_state(Mc2).

init_tick_timer(State = #ch{tick_timer = undefined}) ->
    {ok, Interval} = application:get_env(rabbit, channel_tick_interval),
    State#ch{tick_timer = erlang:send_after(Interval, self(), tick)};
init_tick_timer(State) ->
    State.

reset_tick_timer(State) ->
    State#ch{tick_timer = undefined}.

maybe_cancel_tick_timer(#ch{tick_timer = undefined} = State) ->
    State;
maybe_cancel_tick_timer(#ch{tick_timer = TRef,
                            unacked_message_q = UMQ} = State) ->
    case ?QUEUE:len(UMQ) of
        0 ->
            %% we can only cancel the tick timer if the unacked messages
            %% queue is empty.
            _ = erlang:cancel_timer(TRef),
            State#ch{tick_timer = undefined};
        _ ->
            %% let the timer continue
            State
    end.

now_millis() ->
    erlang:monotonic_time(millisecond).

get_operation_timeout_and_deadline() ->
    % NB: can't use get_operation_timeout because
    % this code may not be running via the channel Pid
    Timeout = ?CHANNEL_OPERATION_TIMEOUT,
    Deadline =  now_millis() + Timeout,
    {Timeout, Deadline}.

get_queue_consumer_timeout(_PA = #pending_ack{queue = QName},
			   _State = #ch{cfg = #conf{consumer_timeout = GCT}}) ->
    case rabbit_amqqueue:lookup(QName) of
	{ok, Q} -> %% should we account for different queue states here?
	    case rabbit_queue_type_util:args_policy_lookup(<<"consumer-timeout">>,
							   fun (X, Y) -> erlang:min(X, Y) end, Q) of
		    undefined -> GCT;
		    Val -> Val
	    end;
	_ ->
	    GCT
    end.

get_consumer_timeout(PA, State) ->
    get_queue_consumer_timeout(PA, State).

evaluate_consumer_timeout(State = #ch{unacked_message_q = UAMQ}) ->
    case ?QUEUE:get(UAMQ, empty) of
	    empty ->
	        {noreply, State};
	    PA ->  evaluate_consumer_timeout1(PA, State)
    end.

evaluate_consumer_timeout1(PA = #pending_ack{delivered_at = Time},
                           State) ->
    Now = erlang:monotonic_time(millisecond),
    case get_consumer_timeout(PA, State) of
        Timeout when is_integer(Timeout)
                     andalso Time < Now - Timeout ->
            handle_consumer_timed_out(Timeout, PA, State);
        _ ->
            {noreply, State}
    end.

handle_consumer_timed_out(Timeout,#pending_ack{delivery_tag = DeliveryTag, tag = ConsumerTag, queue = QName},
			  State = #ch{cfg = #conf{channel = Channel}}) ->
    rabbit_log_channel:warning("Consumer '~ts' on channel ~w and ~ts has timed out "
			       "waiting for a consumer acknowledgement of a delivery with delivery tag = ~b. Timeout used: ~tp ms. "
			       "This timeout value can be configured, see consumers doc guide to learn more",
			       [ConsumerTag,
                    Channel,
                    rabbit_misc:rs(QName),
                    DeliveryTag, Timeout]),
    Ex = rabbit_misc:amqp_error(precondition_failed,
				"delivery acknowledgement on channel ~w timed out. "
				"Timeout value used: ~tp ms. "
				"This timeout value can be configured, see consumers doc guide to learn more",
				[Channel, Timeout], none),
    handle_exception(Ex, State).

handle_queue_actions(Actions, State) ->
    lists:foldl(
      fun({settled, QRef, MsgSeqNos}, S0) ->
              confirm(MsgSeqNos, QRef, S0);
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
                end, {S0#ch.unconfirmed, []}, MsgSeqNos),
              S = S0#ch{unconfirmed = U},
              record_rejects(Rej, S);
         ({deliver, CTag, AckRequired, Msgs}, S0) ->
              handle_deliver(CTag, AckRequired, Msgs, S0);
         ({queue_down, QRef}, S0) ->
              handle_consuming_queue_down_or_eol(QRef, S0);
         ({block, QName}, S0) ->
              credit_flow:block(QName),
              S0;
         ({unblock, QName}, S0) ->
              credit_flow:unblock(QName),
              S0
      end, State, Actions).

handle_eol(QName, State0) ->
    State1 = handle_consuming_queue_down_or_eol(QName, State0),
    {ConfirmMXs, Unconfirmed} = rabbit_confirms:remove_queue(QName, State1#ch.unconfirmed),
    State2 = State1#ch{unconfirmed = Unconfirmed},
    %% Deleted queue is a special case.
    %% Do not nack the "rejected" messages.
    State3 = record_confirms(ConfirmMXs, State2),
    _ = erase_queue_stats(QName),
    QStates = rabbit_queue_type:remove(QName, State3#ch.queue_states),
    State = State3#ch{queue_states = QStates},
    noreply_coalesce(State).

maybe_increase_global_publishers(#ch{publishing_mode = true} = State0) ->
    State0;
maybe_increase_global_publishers(State0) ->
    rabbit_global_counters:publisher_created(amqp091),
    State0#ch{publishing_mode = true}.

maybe_decrease_global_publishers(#ch{publishing_mode = false}) ->
    ok;
maybe_decrease_global_publishers(#ch{publishing_mode = true}) ->
    rabbit_global_counters:publisher_deleted(amqp091).

is_global_qos_permitted() ->
    rabbit_deprecated_features:is_permitted(global_qos).

-spec unsupported_single_active_consumer_error(amqqueue:amqqueue()) -> no_return().
unsupported_single_active_consumer_error(Q) ->
    rabbit_misc:protocol_error(
      resource_locked,
      "cannot obtain access to locked ~ts. basic.get operations "
      "are not supported by ~p queues with single active consumer",
      [rabbit_misc:rs(amqqueue:get_name(Q)),
       rabbit_queue_type:short_alias_of(amqqueue:get_type(Q))]).
