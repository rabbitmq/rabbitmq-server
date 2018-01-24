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
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
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
%%  * Keeping track of mandatory message routing confirmations
%%    and returns
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

-include("rabbit_framing.hrl").
-include("rabbit.hrl").

-behaviour(gen_server2).

-export([start_link/11, do/2, do/3, do_flow/3, flush/1, shutdown/1]).
-export([send_command/2, deliver/4, deliver_reply/2,
         send_credit_reply/2, send_drained/2]).
-export([list/0, info_keys/0, info/1, info/2, info_all/0, info_all/1,
         emit_info_all/4, info_local/1]).
-export([refresh_config_local/0, ready_for_close/1]).
-export([refresh_interceptors/0]).
-export([force_event_refresh/1]).

-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
         handle_info/2, handle_pre_hibernate/1, prioritise_call/4,
         prioritise_cast/3, prioritise_info/3, format_message_queue/2]).
%% Internal
-export([list_local/0, emit_info_local/3, deliver_reply_local/3]).
-export([get_vhost/1, get_user/1]).
%% For testing
-export([build_topic_variable_map/3]).

%% Mgmt HTTP API refactor
-export([handle_method/5]).

-record(ch, {
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
  %% limiter pid, see rabbit_limiter
  limiter,
  %% none | {Msgs, Acks} | committing | failed |
  tx,
  %% (consumer) delivery tag sequence
  next_tag,
  %% messages pending consumer acknowledgement
  unacked_message_q,
  %% same as #v1.user in the reader, used in
  %% authorisation checks
  user,
  %% same as #v1.user in the reader
  virtual_host,
  %% when queue.bind's queue field is empty,
  %% this name will be used instead
  most_recently_declared_queue,
  %% a map of queue pid to queue name
  queue_names,
  %% queue processes are monitored to update
  %% queue names
  queue_monitors,
  %% a map of consumer tags to
  %% consumer details: #amqqueue record, acknowledgement mode,
  %% consumer exclusivity, etc
  consumer_mapping,
  %% a map of queue pids to consumer tag lists
  queue_consumers,
  %% a set of pids of queues that have unacknowledged
  %% deliveries
  delivering_queues,
  %% when a queue is declared as exclusive, queue
  %% collector must be notified.
  %% see rabbit_queue_collector for more info.
  queue_collector_pid,
  %% timer used to emit statistics
  stats_timer,
  %% are publisher confirms enabled for this channel?
  confirm_enabled,
  %% publisher confirm delivery tag sequence
  publish_seqno,
  %% a dtree used to track unconfirmed
  %% (to publishers) messages
  unconfirmed,
  %% a list of tags for published messages that were
  %% delivered but are yet to be confirmed to the client
  confirmed,
  %% a list of tags for published messages that were
  %% rejected but are yet to be sent to the client
  rejected,
  %% a dtree used to track oustanding notifications
  %% for messages published as mandatory
  mandatory,
  %% same as capabilities in the reader
  capabilities,
  %% tracing exchange resource if tracing is enabled,
  %% 'none' otherwise
  trace_state,
  consumer_prefetch,
  %% used by "one shot RPC" (amq.
  reply_consumer,
  %% flow | noflow, see rabbitmq-server#114
  delivery_flow,
  interceptor_state
}).


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
         prefetch_count,
         global_prefetch_count,
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

-type channel_number() :: non_neg_integer().

-export_type([channel/0]).

-type channel() :: #ch{}.

-spec start_link
        (channel_number(), pid(), pid(), pid(), string(), rabbit_types:protocol(),
         rabbit_types:user(), rabbit_types:vhost(), rabbit_framing:amqp_table(),
         pid(), pid()) ->
            rabbit_types:ok_pid_or_error().
-spec do(pid(), rabbit_framing:amqp_method_record()) -> 'ok'.
-spec do
        (pid(), rabbit_framing:amqp_method_record(),
         rabbit_types:maybe(rabbit_types:content())) ->
            'ok'.
-spec do_flow
        (pid(), rabbit_framing:amqp_method_record(),
         rabbit_types:maybe(rabbit_types:content())) ->
            'ok'.
-spec flush(pid()) -> 'ok'.
-spec shutdown(pid()) -> 'ok'.
-spec send_command(pid(), rabbit_framing:amqp_method_record()) -> 'ok'.
-spec deliver
        (pid(), rabbit_types:ctag(), boolean(), rabbit_amqqueue:qmsg()) -> 'ok'.
-spec deliver_reply(binary(), rabbit_types:delivery()) -> 'ok'.
-spec deliver_reply_local(pid(), binary(), rabbit_types:delivery()) -> 'ok'.
-spec send_credit_reply(pid(), non_neg_integer()) -> 'ok'.
-spec send_drained(pid(), [{rabbit_types:ctag(), non_neg_integer()}]) -> 'ok'.
-spec list() -> [pid()].
-spec list_local() -> [pid()].
-spec info_keys() -> rabbit_types:info_keys().
-spec info(pid()) -> rabbit_types:infos().
-spec info(pid(), rabbit_types:info_keys()) -> rabbit_types:infos().
-spec info_all() -> [rabbit_types:infos()].
-spec info_all(rabbit_types:info_keys()) -> [rabbit_types:infos()].
-spec refresh_config_local() -> 'ok'.
-spec ready_for_close(pid()) -> 'ok'.
-spec force_event_refresh(reference()) -> 'ok'.

%%----------------------------------------------------------------------------

start_link(Channel, ReaderPid, WriterPid, ConnPid, ConnName, Protocol, User,
           VHost, Capabilities, CollectorPid, Limiter) ->
    gen_server2:start_link(
      ?MODULE, [Channel, ReaderPid, WriterPid, ConnPid, ConnName, Protocol,
                User, VHost, Capabilities, CollectorPid, Limiter], []).

do(Pid, Method) ->
    rabbit_channel_common:do(Pid, Method).

do(Pid, Method, Content) ->
    rabbit_channel_common:do(Pid, Method, Content).

do_flow(Pid, Method, Content) ->
    rabbit_channel_common:do_flow(Pid, Method, Content).

flush(Pid) ->
    gen_server2:call(Pid, flush, infinity).

shutdown(Pid) ->
    gen_server2:cast(Pid, terminate).

send_command(Pid, Msg) ->
    gen_server2:cast(Pid,  {command, Msg}).

deliver(Pid, ConsumerTag, AckRequired, Msg) ->
    gen_server2:cast(Pid, {deliver, ConsumerTag, AckRequired, Msg}).

deliver_reply(<<"amq.rabbitmq.reply-to.", Rest/binary>>, Delivery) ->
    case decode_fast_reply_to(Rest) of
        {ok, Pid, Key} ->
            delegate:invoke_no_result(
              Pid, {?MODULE, deliver_reply_local, [Key, Delivery]});
        error ->
            ok
    end.

%% We want to ensure people can't use this mechanism to send a message
%% to an arbitrary process and kill it!
deliver_reply_local(Pid, Key, Delivery) ->
    case pg_local:in_group(rabbit_channels, Pid) of
        true  -> gen_server2:cast(Pid, {deliver_reply, Key, Delivery});
        false -> ok
    end.

declare_fast_reply_to(<<"amq.rabbitmq.reply-to">>) ->
    exists;
declare_fast_reply_to(<<"amq.rabbitmq.reply-to.", Rest/binary>>) ->
    case decode_fast_reply_to(Rest) of
        {ok, Pid, Key} ->
            Msg = {declare_fast_reply_to, Key},
            rabbit_misc:with_exit_handler(
              rabbit_misc:const(not_found),
              fun() -> gen_server2:call(Pid, Msg, infinity) end);
        error ->
            not_found
    end;
declare_fast_reply_to(_) ->
    not_found.

decode_fast_reply_to(Rest) ->
    case string:tokens(binary_to_list(Rest), ".") of
        [PidEnc, Key] -> Pid = binary_to_term(base64:decode(PidEnc)),
                         {ok, Pid, Key};
        _             -> error
    end.

send_credit_reply(Pid, Len) ->
    gen_server2:cast(Pid, {send_credit_reply, Len}).

send_drained(Pid, CTagCredit) ->
    gen_server2:cast(Pid, {send_drained, CTagCredit}).

list() ->
    rabbit_misc:append_rpc_all_nodes(rabbit_mnesia:cluster_nodes(running),
                                     rabbit_channel, list_local, []).

list_local() ->
    pg_local:get_members(rabbit_channels).

info_keys() -> ?INFO_KEYS.

info(Pid) ->
    gen_server2:call(Pid, info, infinity).

info(Pid, Items) ->
    case gen_server2:call(Pid, {info, Items}, infinity) of
        {ok, Res}      -> Res;
        {error, Error} -> throw(Error)
    end.

info_all() ->
    rabbit_misc:filter_exit_map(fun (C) -> info(C) end, list()).

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

refresh_config_local() ->
    rabbit_misc:upmap(
      fun (C) ->
        try
          gen_server2:call(C, refresh_config, infinity)
        catch _:Reason ->
          rabbit_log:error("Failed to refresh channel config "
                           "for channel ~p. Reason ~p",
                           [C, Reason])
        end
      end,
      list_local()),
    ok.

refresh_interceptors() ->
    rabbit_misc:upmap(
      fun (C) ->
        try
          gen_server2:call(C, refresh_interceptors, ?REFRESH_TIMEOUT)
        catch _:Reason ->
          rabbit_log:error("Failed to refresh channel interceptors "
                           "for channel ~p. Reason ~p",
                           [C, Reason])
        end
      end,
      list_local()),
    ok.

ready_for_close(Pid) ->
    rabbit_channel_common:ready_for_close(Pid).

force_event_refresh(Ref) ->
    [gen_server2:cast(C, {force_event_refresh, Ref}) || C <- list()],
    ok.

%%---------------------------------------------------------------------------

init([Channel, ReaderPid, WriterPid, ConnPid, ConnName, Protocol, User, VHost,
      Capabilities, CollectorPid, LimiterPid]) ->
    process_flag(trap_exit, true),
    ?LG_PROCESS_TYPE(channel),
    ?store_proc_name({ConnName, Channel}),
    ok = pg_local:join(rabbit_channels, self()),
    Flow = case rabbit_misc:get_env(rabbit, mirroring_flow_control, true) of
             true   -> flow;
             false  -> noflow
           end,
    {ok, {Global, Prefetch}} = application:get_env(rabbit, default_consumer_prefetch),
    Limiter0 = rabbit_limiter:new(LimiterPid),
    Limiter = case {Global, Prefetch} of
                  {true, 0} ->
                      rabbit_limiter:unlimit_prefetch(Limiter0);
                  {true, _} ->
                      rabbit_limiter:limit_prefetch(Limiter0, Prefetch, 0);
                  _ ->
                      Limiter0
              end,
    State = #ch{state                   = starting,
                protocol                = Protocol,
                channel                 = Channel,
                reader_pid              = ReaderPid,
                writer_pid              = WriterPid,
                conn_pid                = ConnPid,
                conn_name               = ConnName,
                limiter                 = Limiter,
                tx                      = none,
                next_tag                = 1,
                unacked_message_q       = queue:new(),
                user                    = User,
                virtual_host            = VHost,
                most_recently_declared_queue = <<>>,
                queue_names             = #{},
                queue_monitors          = pmon:new(),
                consumer_mapping        = #{},
                queue_consumers         = #{},
                delivering_queues       = sets:new(),
                queue_collector_pid     = CollectorPid,
                confirm_enabled         = false,
                publish_seqno           = 1,
                unconfirmed             = dtree:empty(),
                rejected                = [],
                confirmed               = [],
                mandatory               = dtree:empty(),
                capabilities            = Capabilities,
                trace_state             = rabbit_trace:init(VHost),
                consumer_prefetch       = Prefetch,
                reply_consumer          = none,
                delivery_flow           = Flow,
                interceptor_state       = undefined},
    State1 = State#ch{
               interceptor_state = rabbit_channel_interceptor:init(State)},
    State2 = rabbit_event:init_stats_timer(State1, #ch.stats_timer),
    Infos = infos(?CREATION_EVENT_KEYS, State2),
    rabbit_core_metrics:channel_created(self(), Infos),
    rabbit_event:notify(channel_created, Infos),
    rabbit_event:if_enabled(State2, #ch.stats_timer,
                            fun() -> emit_stats(State2) end),
    put_operation_timeout(),
    {ok, State2, hibernate,
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
        {mandatory_received, _MsgSeqNo,  _QPid} -> 5;
        _                                       -> 0
    end.

prioritise_info(Msg, _Len, _State) ->
    case Msg of
        emit_stats                   -> 7;
        _                            -> 0
    end.

handle_call(flush, _From, State) ->
    reply(ok, State);

handle_call(info, _From, State) ->
    reply(infos(?INFO_KEYS, State), State);

handle_call({info, Items}, _From, State) ->
    try
        reply({ok, infos(Items, State)}, State)
    catch Error -> reply({error, Error}, State)
    end;

handle_call(refresh_config, _From, State = #ch{virtual_host = VHost}) ->
    reply(ok, State#ch{trace_state = rabbit_trace:init(VHost)});

handle_call(refresh_interceptors, _From, State) ->
    IState = rabbit_channel_interceptor:init(State),
    reply(ok, State#ch{interceptor_state = IState});

handle_call({declare_fast_reply_to, Key}, _From,
            State = #ch{reply_consumer = Consumer}) ->
    reply(case Consumer of
              {_, _, Key} -> exists;
              _           -> not_found
          end, State);

handle_call(_Request, _From, State) ->
    noreply(State).

handle_cast({method, Method, Content, Flow},
            State = #ch{reader_pid        = Reader,
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
        _:Reason ->
            {stop, {Reason, erlang:get_stacktrace()}, State}
    end;

handle_cast(ready_for_close, State = #ch{state      = closing,
                                         writer_pid = WriterPid}) ->
    ok = rabbit_writer:send_command_sync(WriterPid, #'channel.close_ok'{}),
    {stop, normal, State};

handle_cast(terminate, State = #ch{writer_pid = WriterPid}) ->
    ok = rabbit_writer:flush(WriterPid),
    {stop, normal, State};

handle_cast({command, #'basic.consume_ok'{consumer_tag = CTag} = Msg}, State) ->
    ok = send(Msg, State),
    noreply(consumer_monitor(CTag, State));

handle_cast({command, Msg}, State) ->
    ok = send(Msg, State),
    noreply(State);

handle_cast({deliver, _CTag, _AckReq, _Msg}, State = #ch{state = closing}) ->
    noreply(State);
handle_cast({deliver, ConsumerTag, AckRequired,
             Msg = {_QName, QPid, _MsgId, Redelivered,
                    #basic_message{exchange_name = ExchangeName,
                                   routing_keys  = [RoutingKey | _CcRoutes],
                                   content       = Content}}},
            State = #ch{writer_pid = WriterPid,
                        next_tag   = DeliveryTag}) ->
    ok = rabbit_writer:send_command_and_notify(
           WriterPid, QPid, self(),
           #'basic.deliver'{consumer_tag = ConsumerTag,
                            delivery_tag = DeliveryTag,
                            redelivered  = Redelivered,
                            exchange     = ExchangeName#resource.name,
                            routing_key  = RoutingKey},
           Content),
    rabbit_basic:maybe_gc_large_msg(Content),
    noreply(record_sent(ConsumerTag, AckRequired, Msg, State));

handle_cast({deliver_reply, _K, _Del}, State = #ch{state = closing}) ->
    noreply(State);
handle_cast({deliver_reply, _K, _Del}, State = #ch{reply_consumer = none}) ->
    noreply(State);
handle_cast({deliver_reply, Key, #delivery{message =
                    #basic_message{exchange_name = ExchangeName,
                                   routing_keys  = [RoutingKey | _CcRoutes],
                                   content       = Content}}},
            State = #ch{writer_pid     = WriterPid,
                        next_tag       = DeliveryTag,
                        reply_consumer = {ConsumerTag, _Suffix, Key}}) ->
    ok = rabbit_writer:send_command(
           WriterPid,
           #'basic.deliver'{consumer_tag = ConsumerTag,
                            delivery_tag = DeliveryTag,
                            redelivered  = false,
                            exchange     = ExchangeName#resource.name,
                            routing_key  = RoutingKey},
           Content),
    noreply(State);
handle_cast({deliver_reply, _K1, _}, State=#ch{reply_consumer = {_, _, _K2}}) ->
    noreply(State);

handle_cast({send_credit_reply, Len}, State = #ch{writer_pid = WriterPid}) ->
    ok = rabbit_writer:send_command(
           WriterPid, #'basic.credit_ok'{available = Len}),
    noreply(State);

handle_cast({send_drained, CTagCredit}, State = #ch{writer_pid = WriterPid}) ->
    [ok = rabbit_writer:send_command(
            WriterPid, #'basic.credit_drained'{consumer_tag   = ConsumerTag,
                                               credit_drained = CreditDrained})
     || {ConsumerTag, CreditDrained} <- CTagCredit],
    noreply(State);

handle_cast({force_event_refresh, Ref}, State) ->
    rabbit_event:notify(channel_created, infos(?CREATION_EVENT_KEYS, State),
                        Ref),
    noreply(rabbit_event:init_stats_timer(State, #ch.stats_timer));

handle_cast({mandatory_received, MsgSeqNo}, State = #ch{mandatory = Mand}) ->
    %% NB: don't call noreply/1 since we don't want to send confirms.
    noreply_coalesce(State#ch{mandatory = dtree:drop(MsgSeqNo, Mand)});

handle_cast({reject_publish, MsgSeqNo, _QPid}, State = #ch{unconfirmed = UC}) ->
    %% It does not matter which queue rejected the message,
    %% if any queue rejected it - it should not be confirmed.
    {MXs, UC1} = dtree:take_one(MsgSeqNo, UC),
    %% NB: don't call noreply/1 since we don't want to send confirms.
    noreply_coalesce(record_rejects(MXs, State#ch{unconfirmed = UC1}));

handle_cast({confirm, MsgSeqNos, QPid}, State = #ch{unconfirmed = UC}) ->
    {MXs, UC1} = dtree:take(MsgSeqNos, QPid, UC),
    %% NB: don't call noreply/1 since we don't want to send confirms.
    noreply_coalesce(record_confirms(MXs, State#ch{unconfirmed = UC1})).

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

handle_info({'DOWN', _MRef, process, QPid, Reason}, State) ->
    State1 = handle_publishing_queue_down(QPid, Reason, State),
    State3 = handle_consuming_queue_down(QPid, State1),
    State4 = handle_delivering_queue_down(QPid, State3),
    %% A rabbit_amqqueue_process has died. If our channel was being
    %% blocked by this process, and no other process is blocking our
    %% channel, then this channel will be unblocked. This means that
    %% any credit that was deferred will be sent to the rabbit_reader
    %% processs that might be blocked by this particular channel.
    credit_flow:peer_down(QPid),
    #ch{queue_names = QNames, queue_monitors = QMons} = State4,
    case maps:find(QPid, QNames) of
        {ok, QName} -> erase_queue_stats(QName);
        error       -> ok
    end,
    noreply(State4#ch{queue_names    = maps:remove(QPid, QNames),
                      queue_monitors = pmon:erase(QPid, QMons)});

handle_info({'EXIT', _Pid, Reason}, State) ->
    {stop, Reason, State};

handle_info({{Ref, Node}, LateAnswer}, State = #ch{channel = Channel})
  when is_reference(Ref) ->
    rabbit_log_channel:warning("Channel ~p ignoring late answer ~p from ~p",
        [Channel, LateAnswer, Node]),
    noreply(State).

handle_pre_hibernate(State) ->
    ok = clear_permission_cache(),
    rabbit_event:if_enabled(
      State, #ch.stats_timer,
      fun () -> emit_stats(State,
                           [{idle_since,
                             os:system_time(milli_seconds)}])
                end),
    {hibernate, rabbit_event:stop_stats_timer(State, #ch.stats_timer)}.

terminate(_Reason, State = #ch{user = #user{username = Username}}) ->
    {_Res, _State1} = notify_queues(State),
    pg_local:leave(rabbit_channels, self()),
    rabbit_event:if_enabled(State, #ch.stats_timer,
                            fun() -> emit_stats(State) end),
    [delete_stats(Tag) || {Tag, _} <- get()],
    rabbit_core_metrics:channel_closed(self()),
    rabbit_event:notify(channel_closed, [{pid, self()},
                                         {user_who_performed_action, Username}]).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_message_queue(Opt, MQ) -> rabbit_misc:format_message_queue(Opt, MQ).

%%---------------------------------------------------------------------------

reply(Reply, NewState) -> {reply, Reply, next_state(NewState), hibernate}.

noreply(NewState) -> {noreply, next_state(NewState), hibernate}.

next_state(State) -> ensure_stats_timer(send_confirms_and_nacks(State)).

noreply_coalesce(State = #ch{confirmed = C, rejected = R}) ->
    Timeout = case {C, R} of {[], []} -> hibernate; _ -> 0 end,
    {noreply, ensure_stats_timer(State), Timeout}.

ensure_stats_timer(State) ->
    rabbit_event:ensure_stats_timer(State, #ch.stats_timer, emit_stats).

return_ok(State, true, _Msg)  -> {noreply, State};
return_ok(State, false, Msg)  -> {reply, Msg, State}.

ok_msg(true, _Msg) -> undefined;
ok_msg(false, Msg) -> Msg.

send(_Command, #ch{state = closing}) ->
    ok;
send(Command, #ch{writer_pid = WriterPid}) ->
    ok = rabbit_writer:send_command(WriterPid, Command).

format_soft_error(#amqp_error{name = N, explanation = E, method = M}) ->
    io_lib:format("operation ~s caused a channel exception ~s: ~ts", [M, N, E]);
format_soft_error(Reason) ->
    Reason.

handle_exception(Reason, State = #ch{protocol     = Protocol,
                                     channel      = Channel,
                                     writer_pid   = WriterPid,
                                     reader_pid   = ReaderPid,
                                     conn_pid     = ConnPid,
                                     conn_name    = ConnName,
                                     virtual_host = VHost,
                                     user         = User}) ->
    %% something bad's happened: notify_queues may not be 'ok'
    {_Result, State1} = notify_queues(State),
    case rabbit_binary_generator:map_exception(Channel, Reason, Protocol) of
        {Channel, CloseMethod} ->
            rabbit_log_channel:error(
                "Channel error on connection ~p (~s, vhost: '~s',"
                " user: '~s'), channel ~p:~n~s~n",
                [ConnPid, ConnName, VHost, User#user.username,
                 Channel, format_soft_error(Reason)]),
            ok = rabbit_writer:send_command(WriterPid, CloseMethod),
            {noreply, State1};
        {0, _} ->
            ReaderPid ! {channel_exit, Channel, Reason},
            {stop, normal, State1}
    end.

-spec precondition_failed(string()) -> no_return().

precondition_failed(Format) -> precondition_failed(Format, []).

-spec precondition_failed(string(), [any()]) -> no_return().

precondition_failed(Format, Params) ->
    rabbit_misc:protocol_error(precondition_failed, Format, Params).

return_queue_declare_ok(#resource{name = ActualName},
                        NoWait, MessageCount, ConsumerCount, State) ->
    return_ok(State#ch{most_recently_declared_queue = ActualName}, NoWait,
              #'queue.declare_ok'{queue          = ActualName,
                                  message_count  = MessageCount,
                                  consumer_count = ConsumerCount}).

check_resource_access(User, Resource, Perm) ->
    V = {Resource, Perm},
    Cache = case get(permission_cache) of
                undefined -> [];
                Other     -> Other
            end,
    case lists:member(V, Cache) of
        true  -> ok;
        false -> ok = rabbit_access_control:check_resource_access(
                        User, Resource, Perm),
                 CacheTail = lists:sublist(Cache, ?MAX_PERMISSION_CACHE_SIZE-1),
                 put(permission_cache, [V | CacheTail])
    end.

clear_permission_cache() -> erase(permission_cache),
                            erase(topic_permission_cache),
                            ok.

check_configure_permitted(Resource, User) ->
    check_resource_access(User, Resource, configure).

check_write_permitted(Resource, User) ->
    check_resource_access(User, Resource, write).

check_read_permitted(Resource, User) ->
    check_resource_access(User, Resource, read).

check_write_permitted_on_topic(Resource, User, ConnPid, RoutingKey) ->
    check_topic_authorisation(Resource, User, ConnPid, RoutingKey, write).

check_read_permitted_on_topic(Resource, User, ConnPid, RoutingKey) ->
    check_topic_authorisation(Resource, User, ConnPid, RoutingKey, read).

check_user_id_header(#'P_basic'{user_id = undefined}, _) ->
    ok;
check_user_id_header(#'P_basic'{user_id = Username},
                     #ch{user = #user{username = Username}}) ->
    ok;
check_user_id_header(
  #'P_basic'{}, #ch{user = #user{authz_backends =
                                     [{rabbit_auth_backend_dummy, _}]}}) ->
    ok;
check_user_id_header(#'P_basic'{user_id = Claimed},
                     #ch{user = #user{username = Actual,
                                      tags     = Tags}}) ->
    case lists:member(impersonator, Tags) of
        true  -> ok;
        false -> precondition_failed(
                   "user_id property set to '~s' but authenticated user was "
                   "'~s'", [Claimed, Actual])
    end.

check_expiration_header(Props) ->
    case rabbit_basic:parse_expiration(Props) of
        {ok, _}    -> ok;
        {error, E} -> precondition_failed("invalid expiration '~s': ~p",
                                          [Props#'P_basic'.expiration, E])
    end.

check_internal_exchange(#exchange{name = Name, internal = true}) ->
    rabbit_misc:protocol_error(access_refused,
                               "cannot publish to internal ~s",
                               [rabbit_misc:rs(Name)]);
check_internal_exchange(_) ->
    ok.

check_topic_authorisation(#exchange{name = Name = #resource{virtual_host = VHost}, type = topic},
                          User = #user{username = Username},
                          ConnPid,
                          RoutingKey,
                          Permission) ->
    Resource = Name#resource{kind = topic},
    Timeout = get_operation_timeout(),
    AmqpParams = case ConnPid of
                     none ->
                         %% Called from outside the channel by mgmt API
                         [];
                     _ ->
                         rabbit_amqp_connection:amqp_params(ConnPid, Timeout)
                 end,
    VariableMap = build_topic_variable_map(AmqpParams, VHost, Username),
    Context = #{routing_key   => RoutingKey,
                variable_map  => VariableMap
    },
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

build_topic_variable_map(AmqpParams, VHost, Username) ->
    VariableFromAmqpParams = extract_topic_variable_map_from_amqp_params(AmqpParams),
    maps:merge(VariableFromAmqpParams, #{<<"vhost">> => VHost, <<"username">> => Username}).

%% use tuple representation of amqp_params to avoid coupling.
%% get variable map only from amqp_params_direct, not amqp_params_network.
%% amqp_params_direct are usually used from plugins (e.g. MQTT, STOMP)
extract_topic_variable_map_from_amqp_params([{amqp_params, {amqp_params_direct, _, _, _, _,
                                             {amqp_adapter_info, _,_,_,_,_,_,AdditionalInfo}, _}}]) ->
    proplists:get_value(variable_map, AdditionalInfo, #{});
extract_topic_variable_map_from_amqp_params(_) ->
    #{}.

check_msg_size(Content) ->
    Size = rabbit_basic:maybe_gc_large_msg(Content),
    case Size > ?MAX_MSG_SIZE of
        true  -> precondition_failed("message size ~B larger than max size ~B",
                                     [Size, ?MAX_MSG_SIZE]);
        false -> ok
    end.

check_vhost_queue_limit(#resource{name = QueueName}, VHost) ->
  case rabbit_vhost_limit:is_over_queue_limit(VHost) of
    false         -> ok;
    {true, Limit} -> precondition_failed("cannot declare queue '~s': "
                               "queue limit in vhost '~s' (~p) is reached",
                               [QueueName, VHost, Limit])

  end.

qbin_to_resource(QueueNameBin, VHostPath) ->
    name_to_resource(queue, QueueNameBin, VHostPath).

name_to_resource(Type, NameBin, VHostPath) ->
    rabbit_misc:r(VHostPath, Type, NameBin).

expand_queue_name_shortcut(<<>>, #ch{most_recently_declared_queue = <<>>}) ->
    rabbit_misc:protocol_error(not_found, "no previously declared queue", []);
expand_queue_name_shortcut(<<>>, #ch{most_recently_declared_queue = MRDQ}) ->
    MRDQ;
expand_queue_name_shortcut(QueueNameBin, _) ->
    QueueNameBin.

expand_routing_key_shortcut(<<>>, <<>>,
                            #ch{most_recently_declared_queue = <<>>}) ->
    rabbit_misc:protocol_error(not_found, "no previously declared queue", []);
expand_routing_key_shortcut(<<>>, <<>>,
                            #ch{most_recently_declared_queue = MRDQ}) ->
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
      access_refused, "deletion of system ~s not allowed",
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
      "~s name '~s' contains reserved prefix 'amq.*'",[Kind, NameBin]);
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

handle_method(#'channel.open'{}, _, State = #ch{state = starting}) ->
    %% Don't leave "starting" as the state for 5s. TODO is this TRTTD?
    State1 = State#ch{state = running},
    rabbit_event:if_enabled(State1, #ch.stats_timer,
                            fun() -> emit_stats(State1) end),
    {reply, #'channel.open_ok'{}, State1};

handle_method(#'channel.open'{}, _, _State) ->
    rabbit_misc:protocol_error(
      channel_error, "second 'channel.open' seen", []);

handle_method(_Method, _, #ch{state = starting}) ->
    rabbit_misc:protocol_error(channel_error, "expected 'channel.open'", []);

handle_method(#'channel.close_ok'{}, _, #ch{state = closing}) ->
    stop;

handle_method(#'channel.close'{}, _, State = #ch{writer_pid = WriterPid,
                                                 state      = closing}) ->
    ok = rabbit_writer:send_command(WriterPid, #'channel.close_ok'{}),
    {noreply, State};

handle_method(_Method, _, State = #ch{state = closing}) ->
    {noreply, State};

handle_method(#'channel.close'{}, _, State = #ch{reader_pid = ReaderPid}) ->
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
              Content, State = #ch{virtual_host    = VHostPath,
                                   tx              = Tx,
                                   channel         = ChannelNum,
                                   confirm_enabled = ConfirmEnabled,
                                   trace_state     = TraceState,
                                   user            = #user{username = Username} = User,
                                   conn_name       = ConnName,
                                   delivery_flow   = Flow,
                                   conn_pid        = ConnPid}) ->
    check_msg_size(Content),
    ExchangeName = rabbit_misc:r(VHostPath, exchange, ExchangeNameBin),
    check_write_permitted(ExchangeName, User),
    Exchange = rabbit_exchange:lookup_or_die(ExchangeName),
    check_internal_exchange(Exchange),
    check_write_permitted_on_topic(Exchange, User, ConnPid, RoutingKey),
    %% We decode the content's properties here because we're almost
    %% certain to want to look at delivery-mode and priority.
    DecodedContent = #content {properties = Props} =
        maybe_set_fast_reply_to(
          rabbit_binary_parser:ensure_content_decoded(Content), State),
    check_user_id_header(Props, State),
    check_expiration_header(Props),
    DoConfirm = Tx =/= none orelse ConfirmEnabled,
    {MsgSeqNo, State1} =
        case DoConfirm orelse Mandatory of
            false -> {undefined, State};
            true  -> SeqNo = State#ch.publish_seqno,
                     {SeqNo, State#ch{publish_seqno = SeqNo + 1}}
        end,
    case rabbit_basic:message(ExchangeName, RoutingKey, DecodedContent) of
        {ok, Message} ->
            Delivery = rabbit_basic:delivery(
                         Mandatory, DoConfirm, Message, MsgSeqNo),
            QNames = rabbit_exchange:route(Exchange, Delivery),
            rabbit_trace:tap_in(Message, QNames, ConnName, ChannelNum,
                                Username, TraceState),
            DQ = {Delivery#delivery{flow = Flow}, QNames},
            {noreply, case Tx of
                          none         -> deliver_to_queues(DQ, State1);
                          {Msgs, Acks} -> Msgs1 = queue:in(DQ, Msgs),
                                          State1#ch{tx = {Msgs1, Acks}}
                      end};
        {error, Reason} ->
            precondition_failed("invalid message: ~p", [Reason])
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
                  none         -> ack(Acked, State1),
                                  State1;
                  {Msgs, Acks} -> Acks1 = ack_cons(ack, Acked, Acks),
                                  State1#ch{tx = {Msgs, Acks1}}
              end};

handle_method(#'basic.get'{queue = QueueNameBin, no_ack = NoAck},
              _, State = #ch{writer_pid = WriterPid,
                             conn_pid   = ConnPid,
                             limiter    = Limiter,
                             next_tag   = DeliveryTag,
                             user       = User,
                             virtual_host = VHostPath}) ->
    QueueName = qbin_to_resource(QueueNameBin, VHostPath),
    check_read_permitted(QueueName, User),
    case rabbit_amqqueue:with_exclusive_access_or_die(
           QueueName, ConnPid,
           fun (Q) -> rabbit_amqqueue:basic_get(
                        Q, self(), NoAck, rabbit_limiter:pid(Limiter))
           end) of
        {ok, MessageCount,
         Msg = {QName, QPid, _MsgId, Redelivered,
                #basic_message{exchange_name = ExchangeName,
                               routing_keys  = [RoutingKey | _CcRoutes],
                               content       = Content}}} ->
            ok = rabbit_writer:send_command(
                   WriterPid,
                   #'basic.get_ok'{delivery_tag  = DeliveryTag,
                                   redelivered   = Redelivered,
                                   exchange      = ExchangeName#resource.name,
                                   routing_key   = RoutingKey,
                                   message_count = MessageCount},
                   Content),
            State1 = monitor_delivering_queue(NoAck, QPid, QName, State),
            {noreply, record_sent(none, not(NoAck), Msg, State1)};
        empty ->
            {reply, #'basic.get_empty'{}, State}
    end;

handle_method(#'basic.consume'{queue        = <<"amq.rabbitmq.reply-to">>,
                               consumer_tag = CTag0,
                               no_ack       = NoAck,
                               nowait       = NoWait},
              _, State = #ch{reply_consumer   = ReplyConsumer,
                             consumer_mapping = ConsumerMapping}) ->
    case maps:find(CTag0, ConsumerMapping) of
        error ->
            case {ReplyConsumer, NoAck} of
                {none, true} ->
                    CTag = case CTag0 of
                               <<>>  -> rabbit_guid:binary(
                                          rabbit_guid:gen_secure(), "amq.ctag");
                               Other -> Other
                           end,
                    %% Precalculate both suffix and key; base64 encoding is
                    %% expensive
                    Key = base64:encode(rabbit_guid:gen_secure()),
                    PidEnc = base64:encode(term_to_binary(self())),
                    Suffix = <<PidEnc/binary, ".", Key/binary>>,
                    Consumer = {CTag, Suffix, binary_to_list(Key)},
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
              not_allowed, "attempt to reuse consumer tag '~s'", [CTag0])
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
              _, State = #ch{consumer_prefetch = ConsumerPrefetch,
                             consumer_mapping  = ConsumerMapping,
                             user              = User,
                             virtual_host      = VHostPath}) ->
    case maps:find(ConsumerTag, ConsumerMapping) of
        error ->
            QueueName = qbin_to_resource(QueueNameBin, VHostPath),
            check_read_permitted(QueueName, User),
            ActualConsumerTag =
                case ConsumerTag of
                    <<>>  -> rabbit_guid:binary(rabbit_guid:gen_secure(),
                                                "amq.ctag");
                    Other -> Other
                end,
            case basic_consume(
                   QueueName, NoAck, ConsumerPrefetch, ActualConsumerTag,
                   ExclusiveConsume, Args, NoWait, State) of
                {ok, State1} ->
                    {noreply, State1};
                {error, exclusive_consume_unavailable} ->
                    rabbit_misc:protocol_error(
                      access_refused, "~s in exclusive use",
                      [rabbit_misc:rs(QueueName)])
            end;
        {ok, _} ->
            %% Attempted reuse of consumer tag.
            rabbit_misc:protocol_error(
              not_allowed, "attempt to reuse consumer tag '~s'", [ConsumerTag])
    end;

handle_method(#'basic.cancel'{consumer_tag = ConsumerTag, nowait = NoWait},
              _, State = #ch{consumer_mapping = ConsumerMapping,
                             queue_consumers  = QCons,
                             user             = #user{username = Username}}) ->
    OkMsg = #'basic.cancel_ok'{consumer_tag = ConsumerTag},
    case maps:find(ConsumerTag, ConsumerMapping) of
        error ->
            %% Spec requires we ignore this situation.
            return_ok(State, NoWait, OkMsg);
        {ok, {Q = #amqqueue{pid = QPid}, _CParams}} ->
            ConsumerMapping1 = maps:remove(ConsumerTag, ConsumerMapping),
            QCons1 =
                case maps:find(QPid, QCons) of
                    error       -> QCons;
                    {ok, CTags} -> CTags1 = gb_sets:delete(ConsumerTag, CTags),
                                   case gb_sets:is_empty(CTags1) of
                                       true  -> maps:remove(QPid, QCons);
                                       false -> maps:put(QPid, CTags1, QCons)
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
                           rabbit_amqqueue:basic_cancel(
                             Q, self(), ConsumerTag, ok_msg(NoWait, OkMsg),
                             Username)
                   end) of
                ok ->
                    {noreply, NewState};
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
              _, State = #ch{limiter = Limiter}) ->
    %% Ensures that if default was set, it's overriden
    Limiter1 = rabbit_limiter:unlimit_prefetch(Limiter),
    {reply, #'basic.qos_ok'{}, State#ch{consumer_prefetch = PrefetchCount,
                                        limiter = Limiter1}};

handle_method(#'basic.qos'{global         = true,
                           prefetch_count = 0},
              _, State = #ch{limiter = Limiter}) ->
    Limiter1 = rabbit_limiter:unlimit_prefetch(Limiter),
    {reply, #'basic.qos_ok'{}, State#ch{limiter = Limiter1}};

handle_method(#'basic.qos'{global         = true,
                           prefetch_count = PrefetchCount},
              _, State = #ch{limiter = Limiter, unacked_message_q = UAMQ}) ->
    %% TODO queue:len(UAMQ) is not strictly right since that counts
    %% unacked messages from basic.get too. Pretty obscure though.
    Limiter1 = rabbit_limiter:limit_prefetch(Limiter,
                                             PrefetchCount, queue:len(UAMQ)),
    case ((not rabbit_limiter:is_active(Limiter)) andalso
          rabbit_limiter:is_active(Limiter1)) of
        true  -> rabbit_amqqueue:activate_limit_all(
                   consumer_queues(State#ch.consumer_mapping), self());
        false -> ok
    end,
    {reply, #'basic.qos_ok'{}, State#ch{limiter = Limiter1}};

handle_method(#'basic.recover_async'{requeue = true},
              _, State = #ch{unacked_message_q = UAMQ, limiter = Limiter}) ->
    OkFun = fun () -> ok end,
    UAMQL = queue:to_list(UAMQ),
    foreach_per_queue(
      fun (QPid, MsgIds) ->
              rabbit_misc:with_exit_handler(
                OkFun,
                fun () -> rabbit_amqqueue:requeue(QPid, MsgIds, self()) end)
      end, lists:reverse(UAMQL)),
    ok = notify_limiter(Limiter, UAMQL),
    %% No answer required - basic.recover is the newer, synchronous
    %% variant of this method
    {noreply, State#ch{unacked_message_q = queue:new()}};

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
              _, State = #ch{virtual_host = VHostPath,
                             user = User,
                             queue_collector_pid = CollectorPid,
                             conn_pid = ConnPid}) ->
    handle_method(Method, ConnPid, CollectorPid, VHostPath, User),
    return_ok(State, NoWait, #'exchange.declare_ok'{});

handle_method(#'exchange.delete'{nowait = NoWait} = Method,
              _, State = #ch{conn_pid = ConnPid,
                             virtual_host = VHostPath,
                             queue_collector_pid = CollectorPid,
                             user = User}) ->
    handle_method(Method, ConnPid, CollectorPid, VHostPath, User),
    return_ok(State, NoWait,  #'exchange.delete_ok'{});

handle_method(#'exchange.bind'{nowait = NoWait} = Method,
              _, State = #ch{virtual_host        = VHostPath,
                             conn_pid            = ConnPid,
                             queue_collector_pid = CollectorPid,
                             user = User}) ->
    handle_method(Method, ConnPid, CollectorPid, VHostPath, User),
    return_ok(State, NoWait, #'exchange.bind_ok'{});

handle_method(#'exchange.unbind'{nowait = NoWait} = Method,
              _, State = #ch{virtual_host        = VHostPath,
                             conn_pid            = ConnPid,
                             queue_collector_pid = CollectorPid,
                             user = User}) ->
    handle_method(Method, ConnPid, CollectorPid, VHostPath, User),
    return_ok(State, NoWait, #'exchange.unbind_ok'{});

handle_method(#'queue.declare'{nowait = NoWait} = Method,
              _, State = #ch{virtual_host        = VHostPath,
                             conn_pid            = ConnPid,
                             queue_collector_pid = CollectorPid,
                             user = User}) ->
    {ok, QueueName, MessageCount, ConsumerCount} =
        handle_method(Method, ConnPid, CollectorPid, VHostPath, User),
    return_queue_declare_ok(QueueName, NoWait, MessageCount,
                            ConsumerCount, State);

handle_method(#'queue.delete'{nowait = NoWait} = Method, _,
              State = #ch{conn_pid     = ConnPid,
                          virtual_host = VHostPath,
                          queue_collector_pid = CollectorPid,
                          user         = User}) ->
    {ok, PurgedMessageCount} = handle_method(Method, ConnPid, CollectorPid,
                                             VHostPath, User),
    return_ok(State, NoWait,
              #'queue.delete_ok'{message_count = PurgedMessageCount});

handle_method(#'queue.bind'{nowait = NoWait} = Method, _,
              State = #ch{conn_pid = ConnPid,
                          user     = User,
                          queue_collector_pid = CollectorPid,
                          virtual_host = VHostPath}) ->
    handle_method(Method, ConnPid, CollectorPid, VHostPath, User),
    return_ok(State, NoWait, #'queue.bind_ok'{});

handle_method(#'queue.unbind'{} = Method, _,
              State = #ch{conn_pid = ConnPid,
                          user     = User,
                          queue_collector_pid = CollectorPid,
                          virtual_host = VHostPath}) ->
    handle_method(Method, ConnPid, CollectorPid, VHostPath, User),
    return_ok(State, false, #'queue.unbind_ok'{});

handle_method(#'queue.purge'{nowait = NoWait} = Method,
              _, State = #ch{conn_pid = ConnPid,
                             user     = User,
                             queue_collector_pid = CollectorPid,
                             virtual_host = VHostPath}) ->
    {ok, PurgedMessageCount} = handle_method(Method, ConnPid, CollectorPid,
                                             VHostPath, User),
    return_ok(State, NoWait,
              #'queue.purge_ok'{message_count = PurgedMessageCount});

handle_method(#'tx.select'{}, _, #ch{confirm_enabled = true}) ->
    precondition_failed("cannot switch from confirm to tx mode");

handle_method(#'tx.select'{}, _, State = #ch{tx = none}) ->
    {reply, #'tx.select_ok'{}, State#ch{tx = new_tx()}};

handle_method(#'tx.select'{}, _, State) ->
    {reply, #'tx.select_ok'{}, State};

handle_method(#'tx.commit'{}, _, #ch{tx = none}) ->
    precondition_failed("channel is not transactional");

handle_method(#'tx.commit'{}, _, State = #ch{tx      = {Msgs, Acks},
                                             limiter = Limiter}) ->
    State1 = rabbit_misc:queue_fold(fun deliver_to_queues/2, State, Msgs),
    Rev = fun (X) -> lists:reverse(lists:sort(X)) end,
    lists:foreach(fun ({ack,     A}) -> ack(Rev(A), State1);
                      ({Requeue, A}) -> reject(Requeue, Rev(A), Limiter)
                  end, lists:reverse(Acks)),
    {noreply, maybe_complete_tx(State1#ch{tx = committing})};

handle_method(#'tx.rollback'{}, _, #ch{tx = none}) ->
    precondition_failed("channel is not transactional");

handle_method(#'tx.rollback'{}, _, State = #ch{unacked_message_q = UAMQ,
                                               tx = {_Msgs, Acks}}) ->
    AcksL = lists:append(lists:reverse([lists:reverse(L) || {_, L} <- Acks])),
    UAMQ1 = queue:from_list(lists:usort(AcksL ++ queue:to_list(UAMQ))),
    {reply, #'tx.rollback_ok'{}, State#ch{unacked_message_q = UAMQ1,
                                          tx                = new_tx()}};

handle_method(#'confirm.select'{}, _, #ch{tx = {_, _}}) ->
    precondition_failed("cannot switch from tx to confirm mode");

handle_method(#'confirm.select'{nowait = NoWait}, _, State) ->
    return_ok(State#ch{confirm_enabled = true},
              NoWait, #'confirm.select_ok'{});

handle_method(#'channel.flow'{active = true}, _, State) ->
    {reply, #'channel.flow_ok'{active = true}, State};

handle_method(#'channel.flow'{active = false}, _, _State) ->
    rabbit_misc:protocol_error(not_implemented, "active=false", []);

handle_method(#'basic.credit'{consumer_tag = CTag,
                              credit       = Credit,
                              drain        = Drain},
              _, State = #ch{consumer_mapping = Consumers}) ->
    case maps:find(CTag, Consumers) of
        {ok, {Q, _CParams}} -> ok = rabbit_amqqueue:credit(
                                      Q, self(), CTag, Credit, Drain),
                               {noreply, State};
        error               -> precondition_failed(
                                 "unknown consumer tag '~s'", [CTag])
    end;

handle_method(_MethodRecord, _Content, _State) ->
    rabbit_misc:protocol_error(
      command_invalid, "unimplemented method", []).

%%----------------------------------------------------------------------------

%% We get the queue process to send the consume_ok on our behalf. This
%% is for symmetry with basic.cancel - see the comment in that method
%% for why.
basic_consume(QueueName, NoAck, ConsumerPrefetch, ActualConsumerTag,
              ExclusiveConsume, Args, NoWait,
              State = #ch{conn_pid          = ConnPid,
                          limiter           = Limiter,
                          consumer_mapping  = ConsumerMapping,
                          user              = #user{username = Username}}) ->
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
                      Username),
                    Q}
           end) of
        {ok, Q = #amqqueue{pid = QPid, name = QName}} ->
            CM1 = maps:put(
                    ActualConsumerTag,
                    {Q, {NoAck, ConsumerPrefetch, ExclusiveConsume, Args}},
                    ConsumerMapping),
            State1 = monitor_delivering_queue(
                       NoAck, QPid, QName,
                       State#ch{consumer_mapping = CM1}),
            {ok, case NoWait of
                     true  -> consumer_monitor(ActualConsumerTag, State1);
                     false -> State1
                 end};
        {{error, exclusive_consume_unavailable} = E, _Q} ->
            E
    end.

maybe_stat(false, Q) -> rabbit_amqqueue:stat(Q);
maybe_stat(true, _Q) -> {ok, 0, 0}.

consumer_monitor(ConsumerTag,
                 State = #ch{consumer_mapping = ConsumerMapping,
                             queue_monitors   = QMons,
                             queue_consumers  = QCons}) ->
    {#amqqueue{pid = QPid}, _CParams} =
        maps:get(ConsumerTag, ConsumerMapping),
    CTags1 = case maps:find(QPid, QCons) of
        {ok, CTags} -> gb_sets:insert(ConsumerTag, CTags);
        error -> gb_sets:singleton(ConsumerTag)
    end,
    QCons1 = maps:put(QPid, CTags1, QCons),
    State#ch{queue_monitors  = pmon:monitor(QPid, QMons),
             queue_consumers = QCons1}.

monitor_delivering_queue(NoAck, QPid, QName,
                         State = #ch{queue_names       = QNames,
                                     queue_monitors    = QMons,
                                     delivering_queues = DQ}) ->
    State#ch{queue_names       = maps:put(QPid, QName, QNames),
             queue_monitors    = pmon:monitor(QPid, QMons),
             delivering_queues = case NoAck of
                                     true  -> DQ;
                                     false -> sets:add_element(QPid, DQ)
                                 end}.

handle_publishing_queue_down(QPid, Reason, State = #ch{unconfirmed = UC,
                                                       mandatory   = Mand}) ->
    {MMsgs, Mand1} = dtree:take(QPid, Mand),
    [basic_return(Msg, State, no_route) || {_, Msg} <- MMsgs],
    State1 = State#ch{mandatory = Mand1},
    case rabbit_misc:is_abnormal_exit(Reason) of
        true  -> {MXs, UC1} = dtree:take_all(QPid, UC),
                 send_nacks(MXs, State1#ch{unconfirmed = UC1});
        false -> {MXs, UC1} = dtree:take(QPid, UC),
                              record_confirms(MXs, State1#ch{unconfirmed = UC1})

    end.

handle_consuming_queue_down(QPid, State = #ch{queue_consumers  = QCons,
                                              queue_names      = QNames}) ->
    ConsumerTags = case maps:find(QPid, QCons) of
                       error       -> gb_sets:new();
                       {ok, CTags} -> CTags
                   end,
    gb_sets:fold(
      fun (CTag, StateN = #ch{consumer_mapping = CMap}) ->
              QName = maps:get(QPid, QNames),
              case queue_down_consumer_action(CTag, CMap) of
                  remove ->
                      cancel_consumer(CTag, QName, StateN);
                  {recover, {NoAck, ConsumerPrefetch, Exclusive, Args}} ->
                      case catch basic_consume( %% [0]
                                   QName, NoAck, ConsumerPrefetch, CTag,
                                   Exclusive, Args, true, StateN) of
                          {ok, StateN1} -> StateN1;
                          _             -> cancel_consumer(CTag, QName, StateN)
                      end
              end
      end, State#ch{queue_consumers = maps:remove(QPid, QCons)}, ConsumerTags).

%% [0] There is a slight danger here that if a queue is deleted and
%% then recreated again the reconsume will succeed even though it was
%% not an HA failover. But the likelihood is not great and most users
%% are unlikely to care.

cancel_consumer(CTag, QName, State = #ch{capabilities     = Capabilities,
                                         consumer_mapping = CMap}) ->
    case rabbit_misc:table_lookup(
           Capabilities, <<"consumer_cancel_notify">>) of
        {bool, true} -> ok = send(#'basic.cancel'{consumer_tag = CTag,
                                                  nowait       = true}, State);
        _            -> ok
    end,
    rabbit_event:notify(consumer_deleted, [{consumer_tag, CTag},
                                           {channel,      self()},
                                           {queue,        QName}]),
    State#ch{consumer_mapping = maps:remove(CTag, CMap)}.

queue_down_consumer_action(CTag, CMap) ->
    {_, {_, _, _, Args} = ConsumeSpec} = maps:get(CTag, CMap),
    case rabbit_misc:table_lookup(Args, <<"x-cancel-on-ha-failover">>) of
        {bool, true} -> remove;
        _            -> {recover, ConsumeSpec}
    end.

handle_delivering_queue_down(QPid, State = #ch{delivering_queues = DQ}) ->
    State#ch{delivering_queues = sets:del_element(QPid, DQ)}.

binding_action(Fun, SourceNameBin0, DestinationType, DestinationNameBin0,
               RoutingKey, Arguments, VHostPath, ConnPid,
               #user{username = Username} = User) ->
    ExchangeNameBin = strip_cr_lf(SourceNameBin0),
    DestinationNameBin = strip_cr_lf(DestinationNameBin0),
    DestinationName = name_to_resource(DestinationType, DestinationNameBin, VHostPath),
    check_write_permitted(DestinationName, User),
    ExchangeName = rabbit_misc:r(VHostPath, exchange, ExchangeNameBin),
    [check_not_default_exchange(N) || N <- [DestinationName, ExchangeName]],
    check_read_permitted(ExchangeName, User),
    ExchangeLookup = rabbit_exchange:lookup(ExchangeName),
    case ExchangeLookup of
        {error, not_found} ->
            %% no-op
            ExchangeLookup;
        {ok, Exchange}     ->
            check_read_permitted_on_topic(Exchange, User, ConnPid, RoutingKey),
            ExchangeLookup
    end,
    case Fun(#binding{source      = ExchangeName,
                      destination = DestinationName,
                      key         = RoutingKey,
                      args        = Arguments},
             fun (_X, Q = #amqqueue{}) ->
                     try rabbit_amqqueue:check_exclusive_access(Q, ConnPid)
                     catch exit:Reason -> {error, Reason}
                     end;
                 (_X, #exchange{}) ->
                     ok
             end,
             Username) of
        {error, {resources_missing, [{not_found, Name} | _]}} ->
            rabbit_misc:not_found(Name);
        {error, {resources_missing, [{absent, Q, Reason} | _]}} ->
            rabbit_misc:absent(Q, Reason);
        {error, binding_not_found} ->
            rabbit_misc:protocol_error(
              not_found, "no binding ~s between ~s and ~s",
              [RoutingKey, rabbit_misc:rs(ExchangeName),
               rabbit_misc:rs(DestinationName)]);
        {error, {binding_invalid, Fmt, Args}} ->
            rabbit_misc:protocol_error(precondition_failed, Fmt, Args);
        {error, #amqp_error{} = Error} ->
            rabbit_misc:protocol_error(Error);
        ok ->
            ok
    end.

basic_return(#basic_message{exchange_name = ExchangeName,
                            routing_keys  = [RoutingKey | _CcRoutes],
                            content       = Content},
             State = #ch{protocol = Protocol, writer_pid = WriterPid},
             Reason) ->
    ?INCR_STATS(exchange_stats, ExchangeName, 1, return_unroutable, State),
    {_Close, ReplyCode, ReplyText} = Protocol:lookup_amqp_exception(Reason),
    ok = rabbit_writer:send_command(
           WriterPid,
           #'basic.return'{reply_code  = ReplyCode,
                           reply_text  = ReplyText,
                           exchange    = ExchangeName#resource.name,
                           routing_key = RoutingKey},
           Content).

reject(DeliveryTag, Requeue, Multiple,
       State = #ch{unacked_message_q = UAMQ, tx = Tx}) ->
    {Acked, Remaining} = collect_acks(UAMQ, DeliveryTag, Multiple),
    State1 = State#ch{unacked_message_q = Remaining},
    {noreply, case Tx of
                  none         -> reject(Requeue, Acked, State1#ch.limiter),
                                  State1;
                  {Msgs, Acks} -> Acks1 = ack_cons(Requeue, Acked, Acks),
                                  State1#ch{tx = {Msgs, Acks1}}
              end}.

%% NB: Acked is in youngest-first order
reject(Requeue, Acked, Limiter) ->
    foreach_per_queue(
      fun (QPid, MsgIds) ->
              rabbit_amqqueue:reject(QPid, Requeue, MsgIds, self())
      end, Acked),
    ok = notify_limiter(Limiter, Acked).

record_sent(ConsumerTag, AckRequired,
            Msg = {QName, QPid, MsgId, Redelivered, _Message},
            State = #ch{unacked_message_q = UAMQ,
                        next_tag          = DeliveryTag,
                        trace_state       = TraceState,
                        user              = #user{username = Username},
                        conn_name         = ConnName,
                        channel           = ChannelNum}) ->
    ?INCR_STATS(queue_stats, QName, 1, case {ConsumerTag, AckRequired} of
                                           {none,  true} -> get;
                                           {none, false} -> get_no_ack;
                                           {_   ,  true} -> deliver;
                                           {_   , false} -> deliver_no_ack
                                       end, State),
    case Redelivered of
        true  -> ?INCR_STATS(queue_stats, QName, 1, redeliver, State);
        false -> ok
    end,
    rabbit_trace:tap_out(Msg, ConnName, ChannelNum, Username, TraceState),
    UAMQ1 = case AckRequired of
                true  -> queue:in({DeliveryTag, ConsumerTag, {QPid, MsgId}},
                                  UAMQ);
                false -> UAMQ
            end,
    State#ch{unacked_message_q = UAMQ1, next_tag = DeliveryTag + 1}.

%% NB: returns acks in youngest-first order
collect_acks(Q, 0, true) ->
    {lists:reverse(queue:to_list(Q)), queue:new()};
collect_acks(Q, DeliveryTag, Multiple) ->
    collect_acks([], [], Q, DeliveryTag, Multiple).

collect_acks(ToAcc, PrefixAcc, Q, DeliveryTag, Multiple) ->
    case queue:out(Q) of
        {{value, UnackedMsg = {CurrentDeliveryTag, _ConsumerTag, _Msg}},
         QTail} ->
            if CurrentDeliveryTag == DeliveryTag ->
                    {[UnackedMsg | ToAcc],
                     case PrefixAcc of
                         [] -> QTail;
                         _  -> queue:join(
                                 queue:from_list(lists:reverse(PrefixAcc)),
                                 QTail)
                     end};
               Multiple ->
                    collect_acks([UnackedMsg | ToAcc], PrefixAcc,
                                 QTail, DeliveryTag, Multiple);
               true ->
                    collect_acks(ToAcc, [UnackedMsg | PrefixAcc],
                                 QTail, DeliveryTag, Multiple)
            end;
        {empty, _} ->
            precondition_failed("unknown delivery tag ~w", [DeliveryTag])
    end.

%% NB: Acked is in youngest-first order
ack(Acked, State = #ch{queue_names = QNames}) ->
    foreach_per_queue(
      fun (QPid, MsgIds) ->
              ok = rabbit_amqqueue:ack(QPid, MsgIds, self()),
              case maps:find(QPid, QNames) of
                  {ok, QName} -> Count = length(MsgIds),
                                 ?INCR_STATS(queue_stats, QName, Count, ack, State);
                  error       -> ok
              end
      end, Acked),
    ok = notify_limiter(State#ch.limiter, Acked).

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
new_tx() -> {queue:new(), []}.

notify_queues(State = #ch{state = closing}) ->
    {ok, State};
notify_queues(State = #ch{consumer_mapping  = Consumers,
                          delivering_queues = DQ }) ->
    QPids = sets:to_list(
              sets:union(sets:from_list(consumer_queues(Consumers)), DQ)),
    Timeout = get_operation_timeout(),
    {rabbit_amqqueue:notify_down_all(QPids, self(), Timeout),
     State#ch{state = closing}}.

foreach_per_queue(_F, []) ->
    ok;
foreach_per_queue(F, [{_DTag, _CTag, {QPid, MsgId}}]) -> %% common case
    F(QPid, [MsgId]);
%% NB: UAL should be in youngest-first order; the tree values will
%% then be in oldest-first order
foreach_per_queue(F, UAL) ->
    T = lists:foldl(fun ({_DTag, _CTag, {QPid, MsgId}}, T) ->
                            rabbit_misc:gb_trees_cons(QPid, MsgId, T)
                    end, gb_trees:empty(), UAL),
    rabbit_misc:gb_trees_foreach(F, T).

consumer_queues(Consumers) ->
    lists:usort([QPid || {_Key, {#amqqueue{pid = QPid}, _CParams}}
                             <- maps:to_list(Consumers)]).

%% tell the limiter about the number of acks that have been received
%% for messages delivered to subscribed consumers, but not acks for
%% messages sent in a response to a basic.get (identified by their
%% 'none' consumer tag)
notify_limiter(Limiter, Acked) ->
    %% optimisation: avoid the potentially expensive 'foldl' in the
    %% common case.
     case rabbit_limiter:is_active(Limiter) of
        false -> ok;
        true  -> case lists:foldl(fun ({_, none, _}, Acc) -> Acc;
                                      ({_,    _, _}, Acc) -> Acc + 1
                                  end, 0, Acked) of
                     0     -> ok;
                     Count -> rabbit_limiter:ack(Limiter, Count)
                 end
    end.

deliver_to_queues({#delivery{message   = #basic_message{exchange_name = XName},
                             confirm   = false,
                             mandatory = false},
                   []}, State) -> %% optimisation
    ?INCR_STATS(exchange_stats, XName, 1, publish, State),
    State;
deliver_to_queues({Delivery = #delivery{message    = Message = #basic_message{
                                                       exchange_name = XName},
                                        mandatory  = Mandatory,
                                        confirm    = Confirm,
                                        msg_seq_no = MsgSeqNo},
                   DelQNames}, State = #ch{queue_names    = QNames,
                                           queue_monitors = QMons}) ->
    Qs = rabbit_amqqueue:lookup(DelQNames),
    DeliveredQPids = rabbit_amqqueue:deliver(Qs, Delivery),
    %% The pmon:monitor_all/2 monitors all queues to which we
    %% delivered. But we want to monitor even queues we didn't deliver
    %% to, since we need their 'DOWN' messages to clean
    %% queue_names. So we also need to monitor each QPid from
    %% queues. But that only gets the masters (which is fine for
    %% cleaning queue_names), so we need the union of both.
    %%
    %% ...and we need to add even non-delivered queues to queue_names
    %% since alternative algorithms to update queue_names less
    %% frequently would in fact be more expensive in the common case.
    {QNames1, QMons1} =
        lists:foldl(fun (#amqqueue{pid = QPid, name = QName},
                         {QNames0, QMons0}) ->
                            {case maps:is_key(QPid, QNames0) of
                                 true  -> QNames0;
                                 false -> maps:put(QPid, QName, QNames0)
                             end, pmon:monitor(QPid, QMons0)}
                    end, {QNames, pmon:monitor_all(DeliveredQPids, QMons)}, Qs),
    State1 = State#ch{queue_names    = QNames1,
                      queue_monitors = QMons1},
    %% NB: the order here is important since basic.returns must be
    %% sent before confirms.
    State2 = process_routing_mandatory(Mandatory, DeliveredQPids, MsgSeqNo,
                                       Message, State1),
    State3 = process_routing_confirm(  Confirm,   DeliveredQPids, MsgSeqNo,
                                       XName,   State2),
    case rabbit_event:stats_level(State3, #ch.stats_timer) of
        fine ->
            ?INCR_STATS(exchange_stats, XName, 1, publish),
            [?INCR_STATS(queue_exchange_stats, {QName, XName}, 1, publish) ||
                QPid        <- DeliveredQPids,
                {ok, QName} <- [maps:find(QPid, QNames1)]];
        _ ->
            ok
    end,
    State3.

process_routing_mandatory(false,     _, _MsgSeqNo, _Msg, State) ->
    State;
process_routing_mandatory(true,     [], _MsgSeqNo,  Msg, State) ->
    ok = basic_return(Msg, State, no_route),
    State;
process_routing_mandatory(true,  QPids,  MsgSeqNo,  Msg, State) ->
    State#ch{mandatory = dtree:insert(MsgSeqNo, QPids, Msg,
                                      State#ch.mandatory)}.

process_routing_confirm(false,    _, _MsgSeqNo, _XName, State) ->
    State;
process_routing_confirm(true,    [],  MsgSeqNo,  XName, State) ->
    record_confirms([{MsgSeqNo, XName}], State);
process_routing_confirm(true, QPids,  MsgSeqNo,  XName, State) ->
    State#ch{unconfirmed = dtree:insert(MsgSeqNo, QPids, XName,
                                        State#ch.unconfirmed)}.

send_nacks([], State) ->
    State;
send_nacks(_MXs, State = #ch{state = closing,
                             tx    = none}) -> %% optimisation
    State;
send_nacks(MXs, State = #ch{tx = none}) ->
    coalesce_and_send([MsgSeqNo || {MsgSeqNo, _} <- MXs],
                      fun(MsgSeqNo, Multiple) ->
                              #'basic.nack'{delivery_tag = MsgSeqNo,
                                            multiple     = Multiple}
                      end, State);
send_nacks(_MXs, State = #ch{state = closing}) -> %% optimisation
    State#ch{tx = failed};
send_nacks(_, State) ->
    maybe_complete_tx(State#ch{tx = failed}).

send_confirms_and_nacks(State = #ch{tx = none, confirmed = [], rejected = []}) ->
    State;
send_confirms_and_nacks(State = #ch{tx = none, confirmed = C, rejected = R}) ->
    case rabbit_node_monitor:pause_partition_guard() of
        ok      -> ConfirmMsgSeqNos =
                       lists:foldl(
                         fun ({MsgSeqNo, XName}, MSNs) ->
                                 ?INCR_STATS(exchange_stats, XName, 1,
                                             confirm, State),
                                 [MsgSeqNo | MSNs]
                         end, [], lists:append(C)),
                   State1 = send_confirms(ConfirmMsgSeqNos, State#ch{confirmed = []}),
                   %% TODO: msg seq nos, same as for confirms. Need to implement
                   %% nack rates first.
                   send_nacks(lists:append(R), State1#ch{rejected = []});
        pausing -> State
    end;
send_confirms_and_nacks(State) ->
    case rabbit_node_monitor:pause_partition_guard() of
        ok      -> maybe_complete_tx(State);
        pausing -> State
    end.

send_confirms([], State) ->
    State;
send_confirms(_Cs, State = #ch{state = closing}) -> %% optimisation
    State;
send_confirms([MsgSeqNo], State) ->
    ok = send(#'basic.ack'{delivery_tag = MsgSeqNo}, State),
    State;
send_confirms(Cs, State) ->
    coalesce_and_send(Cs, fun(MsgSeqNo, Multiple) ->
                                  #'basic.ack'{delivery_tag = MsgSeqNo,
                                               multiple     = Multiple}
                          end, State).

coalesce_and_send(MsgSeqNos, MkMsgFun, State = #ch{unconfirmed = UC}) ->
    SMsgSeqNos = lists:usort(MsgSeqNos),
    CutOff = case dtree:is_empty(UC) of
                 true  -> lists:last(SMsgSeqNos) + 1;
                 false -> {SeqNo, _XName} = dtree:smallest(UC), SeqNo
             end,
    {Ms, Ss} = lists:splitwith(fun(X) -> X < CutOff end, SMsgSeqNos),
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
    case dtree:is_empty(UC) of
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

i(pid,            _)                               -> self();
i(connection,     #ch{conn_pid         = ConnPid}) -> ConnPid;
i(number,         #ch{channel          = Channel}) -> Channel;
i(user,           #ch{user             = User})    -> User#user.username;
i(user_who_performed_action, Ch) -> i(user, Ch);
i(vhost,          #ch{virtual_host     = VHost})   -> VHost;
i(transactional,  #ch{tx               = Tx})      -> Tx =/= none;
i(confirm,        #ch{confirm_enabled  = CE})      -> CE;
i(name,           State)                           -> name(State);
i(consumer_count,          #ch{consumer_mapping = CM})    -> maps:size(CM);
i(messages_unconfirmed,    #ch{unconfirmed = UC})         -> dtree:size(UC);
i(messages_unacknowledged, #ch{unacked_message_q = UAMQ}) -> queue:len(UAMQ);
i(messages_uncommitted,    #ch{tx = {Msgs, _Acks}})       -> queue:len(Msgs);
i(messages_uncommitted,    #ch{})                         -> 0;
i(acks_uncommitted,        #ch{tx = {_Msgs, Acks}})       -> ack_len(Acks);
i(acks_uncommitted,        #ch{})                         -> 0;
i(state,                   #ch{state = running})          -> credit_flow:state();
i(state,                   #ch{state = State})            -> State;
i(prefetch_count,          #ch{consumer_prefetch = C})    -> C;
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

name(#ch{conn_name = ConnName, channel = Channel}) ->
    list_to_binary(rabbit_misc:format("~s (~p)", [ConnName, Channel])).

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

get_vhost(#ch{virtual_host = VHost}) -> VHost.

get_user(#ch{user = User}) -> User.

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
              ConnPid, _CollectorId, VHostPath, User) ->
    binding_action(fun rabbit_binding:add/3,
                   SourceNameBin, exchange, DestinationNameBin,
                   RoutingKey, Arguments, VHostPath, ConnPid, User);
handle_method(#'exchange.unbind'{destination = DestinationNameBin,
                                 source      = SourceNameBin,
                                 routing_key = RoutingKey,
                                 arguments   = Arguments},
             ConnPid, _CollectorId, VHostPath, User) ->
    binding_action(fun rabbit_binding:remove/3,
                       SourceNameBin, exchange, DestinationNameBin,
                       RoutingKey, Arguments, VHostPath, ConnPid, User);
handle_method(#'queue.unbind'{queue       = QueueNameBin,
                              exchange    = ExchangeNameBin,
                              routing_key = RoutingKey,
                              arguments   = Arguments},
              ConnPid, _CollectorId, VHostPath, User) ->
    binding_action(fun rabbit_binding:remove/3,
                   ExchangeNameBin, queue, QueueNameBin,
                   RoutingKey, Arguments, VHostPath, ConnPid, User);
handle_method(#'queue.bind'{queue       = QueueNameBin,
                            exchange    = ExchangeNameBin,
                            routing_key = RoutingKey,
                            arguments   = Arguments},
             ConnPid, _CollectorId, VHostPath, User) ->
    binding_action(fun rabbit_binding:add/3,
                   ExchangeNameBin, queue, QueueNameBin,
                   RoutingKey, Arguments, VHostPath, ConnPid, User);
%% Note that all declares to these are effectively passive. If it
%% exists it by definition has one consumer.
handle_method(#'queue.declare'{queue   = <<"amq.rabbitmq.reply-to",
                                           _/binary>> = QueueNameBin},
              _ConnPid, _CollectorPid, VHost, _User) ->
    StrippedQueueNameBin = strip_cr_lf(QueueNameBin),
    QueueName = rabbit_misc:r(VHost, queue, StrippedQueueNameBin),
    case declare_fast_reply_to(StrippedQueueNameBin) of
        exists    -> {ok, QueueName, 0, 1};
        not_found -> rabbit_misc:not_found(QueueName)
    end;
handle_method(#'queue.declare'{queue       = QueueNameBin,
                               passive     = false,
                               durable     = DurableDeclare,
                               exclusive   = ExclusiveDeclare,
                               auto_delete = AutoDelete,
                               nowait      = NoWait,
                               arguments   = Args} = Declare,
              ConnPid, CollectorPid, VHostPath, #user{username = Username} = User) ->
    Owner = case ExclusiveDeclare of
                true  -> ConnPid;
                false -> none
            end,
    StrippedQueueNameBin = strip_cr_lf(QueueNameBin),
    Durable = DurableDeclare andalso not ExclusiveDeclare,
    ActualNameBin = case StrippedQueueNameBin of
                        <<>>  -> rabbit_guid:binary(rabbit_guid:gen_secure(),
                                                    "amq.gen");
                        Other -> check_name('queue', Other)
                    end,
    QueueName = rabbit_misc:r(VHostPath, queue, ActualNameBin),
    check_configure_permitted(QueueName, User),
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
            check_vhost_queue_limit(QueueName, VHostPath),
            DlxKey = <<"x-dead-letter-exchange">>,
            case rabbit_misc:r_arg(VHostPath, exchange, Args, DlxKey) of
               undefined ->
                   ok;
               {error, {invalid_type, Type}} ->
                    precondition_failed(
                      "invalid type '~s' for arg '~s' in ~s",
                      [Type, DlxKey, rabbit_misc:rs(QueueName)]);
               DLX ->
                   check_read_permitted(QueueName, User),
                   check_write_permitted(DLX, User),
                   ok
            end,
            case rabbit_amqqueue:declare(QueueName, Durable, AutoDelete,
                                         Args, Owner, Username) of
                {new, #amqqueue{pid = QPid}} ->
                    %% We need to notify the reader within the channel
                    %% process so that we can be sure there are no
                    %% outstanding exclusive queues being declared as
                    %% the connection shuts down.
                    ok = case {Owner, CollectorPid} of
                             {none, _} -> ok;
                             {_, none} -> ok; %% Supports call from mgmt API
                             _    -> rabbit_queue_collector:register(
                                       CollectorPid, QPid)
                         end,
                    {ok, QueueName, 0, 0};
                {existing, _Q} ->
                    %% must have been created between the stat and the
                    %% declare. Loop around again.
                    handle_method(Declare, ConnPid, CollectorPid, VHostPath, User);
                {absent, Q, Reason} ->
                    rabbit_misc:absent(Q, Reason);
                {owner_died, _Q} ->
                    %% Presumably our own days are numbered since the
                    %% connection has died. Pretend the queue exists though,
                    %% just so nothing fails.
                    {ok, QueueName, 0, 0}
            end;
        {error, {absent, Q, Reason}} ->
            rabbit_misc:absent(Q, Reason)
    end;
handle_method(#'queue.declare'{queue   = QueueNameBin,
                               nowait  = NoWait,
                               passive = true},
              ConnPid, _CollectorPid, VHostPath, _User) ->
    StrippedQueueNameBin = strip_cr_lf(QueueNameBin),
    QueueName = rabbit_misc:r(VHostPath, queue, StrippedQueueNameBin),
    {{ok, MessageCount, ConsumerCount}, #amqqueue{} = Q} =
        rabbit_amqqueue:with_or_die(
          QueueName, fun (Q) -> {maybe_stat(NoWait, Q), Q} end),
    ok = rabbit_amqqueue:check_exclusive_access(Q, ConnPid),
    {ok, QueueName, MessageCount, ConsumerCount};
handle_method(#'queue.delete'{queue     = QueueNameBin,
                              if_unused = IfUnused,
                              if_empty  = IfEmpty},
              ConnPid, _CollectorPid, VHostPath, User = #user{username = Username}) ->
    StrippedQueueNameBin = strip_cr_lf(QueueNameBin),
    QueueName = qbin_to_resource(StrippedQueueNameBin, VHostPath),

    check_configure_permitted(QueueName, User),
    case rabbit_amqqueue:with(
           QueueName,
           fun (Q) ->
                   rabbit_amqqueue:check_exclusive_access(Q, ConnPid),
                   rabbit_amqqueue:delete(Q, IfUnused, IfEmpty, Username)
           end,
           fun (not_found)            -> {ok, 0};
               ({absent, Q, crashed}) -> rabbit_amqqueue:delete_crashed(Q, Username),
                                         {ok, 0};
               ({absent, Q, stopped}) -> rabbit_amqqueue:delete_crashed(Q, Username),
                                         {ok, 0};
               ({absent, Q, Reason})  -> rabbit_misc:absent(Q, Reason)
           end) of
        {error, in_use} ->
            precondition_failed("~s in use", [rabbit_misc:rs(QueueName)]);
        {error, not_empty} ->
            precondition_failed("~s not empty", [rabbit_misc:rs(QueueName)]);
        {ok, _Count} = OK ->
            OK
    end;
handle_method(#'exchange.delete'{exchange  = ExchangeNameBin,
                                 if_unused = IfUnused},
              _ConnPid, _CollectorPid, VHostPath, User = #user{username = Username}) ->
    StrippedExchangeNameBin = strip_cr_lf(ExchangeNameBin),
    ExchangeName = rabbit_misc:r(VHostPath, exchange, StrippedExchangeNameBin),
    check_not_default_exchange(ExchangeName),
    check_exchange_deletion(ExchangeName),
    check_configure_permitted(ExchangeName, User),
    case rabbit_exchange:delete(ExchangeName, IfUnused, Username) of
        {error, not_found} ->
            ok;
        {error, in_use} ->
            precondition_failed("~s in use", [rabbit_misc:rs(ExchangeName)]);
        ok ->
            ok
    end;
handle_method(#'queue.purge'{queue = QueueNameBin},
              ConnPid, _CollectorPid, VHostPath, User) ->
    QueueName = qbin_to_resource(QueueNameBin, VHostPath),
    check_read_permitted(QueueName, User),
    rabbit_amqqueue:with_exclusive_access_or_die(
      QueueName, ConnPid,
      fun (Q) -> rabbit_amqqueue:purge(Q) end);
handle_method(#'exchange.declare'{exchange    = ExchangeNameBin,
                                  type        = TypeNameBin,
                                  passive     = false,
                                  durable     = Durable,
                                  auto_delete = AutoDelete,
                                  internal    = Internal,
                                  arguments   = Args},
              _ConnPid, _CollectorPid, VHostPath, #user{username = Username} = User) ->
    CheckedType = rabbit_exchange:check_type(TypeNameBin),
    ExchangeName = rabbit_misc:r(VHostPath, exchange, strip_cr_lf(ExchangeNameBin)),
    check_not_default_exchange(ExchangeName),
    check_configure_permitted(ExchangeName, User),
    X = case rabbit_exchange:lookup(ExchangeName) of
            {ok, FoundX} -> FoundX;
            {error, not_found} ->
                check_name('exchange', strip_cr_lf(ExchangeNameBin)),
                AeKey = <<"alternate-exchange">>,
                case rabbit_misc:r_arg(VHostPath, exchange, Args, AeKey) of
                    undefined -> ok;
                    {error, {invalid_type, Type}} ->
                        precondition_failed(
                          "invalid type '~s' for arg '~s' in ~s",
                          [Type, AeKey, rabbit_misc:rs(ExchangeName)]);
                    AName     -> check_read_permitted(ExchangeName, User),
                                 check_write_permitted(AName, User),
                                 ok
                end,
                rabbit_exchange:declare(ExchangeName,
                                        CheckedType,
                                        Durable,
                                        AutoDelete,
                                        Internal,
                                        Args,
                                        Username)
        end,
    ok = rabbit_exchange:assert_equivalence(X, CheckedType, Durable,
                                            AutoDelete, Internal, Args);
handle_method(#'exchange.declare'{exchange    = ExchangeNameBin,
                                  passive     = true},
              _ConnPid, _CollectorPid, VHostPath, _User) ->
    ExchangeName = rabbit_misc:r(VHostPath, exchange, strip_cr_lf(ExchangeNameBin)),
    check_not_default_exchange(ExchangeName),
    _ = rabbit_exchange:lookup_or_die(ExchangeName).
