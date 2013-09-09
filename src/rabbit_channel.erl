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

-module(rabbit_channel).
-include("rabbit_framing.hrl").
-include("rabbit.hrl").

-behaviour(gen_server2).

-export([start_link/11, do/2, do/3, do_flow/3, flush/1, shutdown/1]).
-export([send_command/2, deliver/4, send_credit_reply/2, send_drained/2,
         flushed/2]).
-export([list/0, info_keys/0, info/1, info/2, info_all/0, info_all/1]).
-export([refresh_config_local/0, ready_for_close/1]).
-export([force_event_refresh/0]).

-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
         handle_info/2, handle_pre_hibernate/1, prioritise_call/4,
         prioritise_cast/3, prioritise_info/3, format_message_queue/2]).
%% Internal
-export([list_local/0]).

-record(ch, {state, protocol, channel, reader_pid, writer_pid, conn_pid,
             conn_name, limiter, tx, next_tag, unacked_message_q, user,
             virtual_host, most_recently_declared_queue,
             queue_names, queue_monitors, consumer_mapping,
             blocking, queue_consumers, delivering_queues,
             queue_collector_pid, stats_timer, confirm_enabled, publish_seqno,
             unconfirmed, confirmed, capabilities, trace_state}).

-define(MAX_PERMISSION_CACHE_SIZE, 12).

-define(STATISTICS_KEYS,
        [pid,
         transactional,
         confirm,
         consumer_count,
         messages_unacknowledged,
         messages_unconfirmed,
         messages_uncommitted,
         acks_uncommitted,
         prefetch_count,
         client_flow_blocked]).

-define(CREATION_EVENT_KEYS,
        [pid,
         name,
         connection,
         number,
         user,
         vhost]).

-define(INFO_KEYS, ?CREATION_EVENT_KEYS ++ ?STATISTICS_KEYS -- [pid]).

-define(INCR_STATS(Incs, Measure, State),
        case rabbit_event:stats_level(State, #ch.stats_timer) of
            fine -> incr_stats(Incs, Measure);
            _    -> ok
        end).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([channel_number/0]).

-type(channel_number() :: non_neg_integer()).

-spec(start_link/11 ::
        (channel_number(), pid(), pid(), pid(), string(),
         rabbit_types:protocol(), rabbit_types:user(), rabbit_types:vhost(),
         rabbit_framing:amqp_table(), pid(), pid()) ->
                            rabbit_types:ok_pid_or_error()).
-spec(do/2 :: (pid(), rabbit_framing:amqp_method_record()) -> 'ok').
-spec(do/3 :: (pid(), rabbit_framing:amqp_method_record(),
               rabbit_types:maybe(rabbit_types:content())) -> 'ok').
-spec(do_flow/3 :: (pid(), rabbit_framing:amqp_method_record(),
                    rabbit_types:maybe(rabbit_types:content())) -> 'ok').
-spec(flush/1 :: (pid()) -> 'ok').
-spec(shutdown/1 :: (pid()) -> 'ok').
-spec(send_command/2 :: (pid(), rabbit_framing:amqp_method_record()) -> 'ok').
-spec(deliver/4 ::
        (pid(), rabbit_types:ctag(), boolean(), rabbit_amqqueue:qmsg())
        -> 'ok').
-spec(send_credit_reply/2 :: (pid(), non_neg_integer()) -> 'ok').
-spec(send_drained/2 :: (pid(), [{rabbit_types:ctag(), non_neg_integer()}])
                        -> 'ok').
-spec(flushed/2 :: (pid(), pid()) -> 'ok').
-spec(list/0 :: () -> [pid()]).
-spec(list_local/0 :: () -> [pid()]).
-spec(info_keys/0 :: () -> rabbit_types:info_keys()).
-spec(info/1 :: (pid()) -> rabbit_types:infos()).
-spec(info/2 :: (pid(), rabbit_types:info_keys()) -> rabbit_types:infos()).
-spec(info_all/0 :: () -> [rabbit_types:infos()]).
-spec(info_all/1 :: (rabbit_types:info_keys()) -> [rabbit_types:infos()]).
-spec(refresh_config_local/0 :: () -> 'ok').
-spec(ready_for_close/1 :: (pid()) -> 'ok').
-spec(force_event_refresh/0 :: () -> 'ok').

-endif.

%%----------------------------------------------------------------------------

start_link(Channel, ReaderPid, WriterPid, ConnPid, ConnName, Protocol, User,
           VHost, Capabilities, CollectorPid, Limiter) ->
    gen_server2:start_link(
      ?MODULE, [Channel, ReaderPid, WriterPid, ConnPid, ConnName, Protocol,
                User, VHost, Capabilities, CollectorPid, Limiter], []).

do(Pid, Method) ->
    do(Pid, Method, none).

do(Pid, Method, Content) ->
    gen_server2:cast(Pid, {method, Method, Content, noflow}).

do_flow(Pid, Method, Content) ->
    credit_flow:send(Pid),
    gen_server2:cast(Pid, {method, Method, Content, flow}).

flush(Pid) ->
    gen_server2:call(Pid, flush, infinity).

shutdown(Pid) ->
    gen_server2:cast(Pid, terminate).

send_command(Pid, Msg) ->
    gen_server2:cast(Pid,  {command, Msg}).

deliver(Pid, ConsumerTag, AckRequired, Msg) ->
    gen_server2:cast(Pid, {deliver, ConsumerTag, AckRequired, Msg}).

send_credit_reply(Pid, Len) ->
    gen_server2:cast(Pid, {send_credit_reply, Len}).

send_drained(Pid, CTagCredit) ->
    gen_server2:cast(Pid, {send_drained, CTagCredit}).

flushed(Pid, QPid) ->
    gen_server2:cast(Pid, {flushed, QPid}).

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

refresh_config_local() ->
    rabbit_misc:upmap(
      fun (C) -> gen_server2:call(C, refresh_config) end, list_local()),
    ok.

ready_for_close(Pid) ->
    gen_server2:cast(Pid, ready_for_close).

force_event_refresh() ->
    [gen_server2:cast(C, force_event_refresh) || C <- list()],
    ok.

%%---------------------------------------------------------------------------

init([Channel, ReaderPid, WriterPid, ConnPid, ConnName, Protocol, User, VHost,
      Capabilities, CollectorPid, LimiterPid]) ->
    process_flag(trap_exit, true),
    ok = pg_local:join(rabbit_channels, self()),
    State = #ch{state                   = starting,
                protocol                = Protocol,
                channel                 = Channel,
                reader_pid              = ReaderPid,
                writer_pid              = WriterPid,
                conn_pid                = ConnPid,
                conn_name               = ConnName,
                limiter                 = rabbit_limiter:new(LimiterPid),
                tx                      = none,
                next_tag                = 1,
                unacked_message_q       = queue:new(),
                user                    = User,
                virtual_host            = VHost,
                most_recently_declared_queue = <<>>,
                queue_names             = dict:new(),
                queue_monitors          = pmon:new(),
                consumer_mapping        = dict:new(),
                blocking                = sets:new(),
                queue_consumers         = dict:new(),
                delivering_queues       = sets:new(),
                queue_collector_pid     = CollectorPid,
                confirm_enabled         = false,
                publish_seqno           = 1,
                unconfirmed             = dtree:empty(),
                confirmed               = [],
                capabilities            = Capabilities,
                trace_state             = rabbit_trace:init(VHost)},
    State1 = rabbit_event:init_stats_timer(State, #ch.stats_timer),
    rabbit_event:notify(channel_created, infos(?CREATION_EVENT_KEYS, State1)),
    rabbit_event:if_enabled(State1, #ch.stats_timer,
                            fun() -> emit_stats(State1) end),
    {ok, State1, hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

prioritise_call(Msg, _From, _Len, _State) ->
    case Msg of
        info           -> 9;
        {info, _Items} -> 9;
        _              -> 0
    end.

prioritise_cast(Msg, _Len, _State) ->
    case Msg of
        {confirm, _MsgSeqNos, _QPid} -> 5;
        _                            -> 0
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

handle_call(_Request, _From, State) ->
    noreply(State).

handle_cast({method, Method, Content, Flow},
            State = #ch{reader_pid = Reader}) ->
    case Flow of
        flow   -> credit_flow:ack(Reader);
        noflow -> ok
    end,
    try handle_method(Method, Content, State) of
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

handle_cast({flushed, QPid}, State) ->
    {noreply, queue_blocked(QPid, State), hibernate};

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
    noreply(record_sent(ConsumerTag, AckRequired, Msg, State));

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

handle_cast(force_event_refresh, State) ->
    rabbit_event:notify(channel_created, infos(?CREATION_EVENT_KEYS, State)),
    noreply(State);

handle_cast({confirm, MsgSeqNos, From}, State) ->
    State1 = #ch{confirmed = C} = confirm(MsgSeqNos, From, State),
    Timeout = case C of [] -> hibernate; _ -> 0 end,
    %% NB: don't call noreply/1 since we don't want to send confirms.
    {noreply, ensure_stats_timer(State1), Timeout}.

handle_info({bump_credit, Msg}, State) ->
    credit_flow:handle_bump_msg(Msg),
    noreply(State);

handle_info(timeout, State) ->
    noreply(State);

handle_info(emit_stats, State) ->
    emit_stats(State),
    State1 = rabbit_event:reset_stats_timer(State, #ch.stats_timer),
    %% NB: don't call noreply/1 since we don't want to kick off the
    %% stats timer.
    {noreply, send_confirms(State1), hibernate};

handle_info({'DOWN', _MRef, process, QPid, Reason}, State) ->
    State1 = handle_publishing_queue_down(QPid, Reason, State),
    State2 = queue_blocked(QPid, State1),
    State3 = handle_consuming_queue_down(QPid, State2),
    State4 = handle_delivering_queue_down(QPid, State3),
    credit_flow:peer_down(QPid),
    #ch{queue_names = QNames, queue_monitors = QMons} = State4,
    case dict:find(QPid, QNames) of
        {ok, QName} -> erase_queue_stats(QName);
        error       -> ok
    end,
    noreply(State4#ch{queue_names    = dict:erase(QPid, QNames),
                      queue_monitors = pmon:erase(QPid, QMons)});

handle_info({'EXIT', _Pid, Reason}, State) ->
    {stop, Reason, State}.

handle_pre_hibernate(State) ->
    ok = clear_permission_cache(),
    rabbit_event:if_enabled(
      State, #ch.stats_timer,
      fun () -> emit_stats(State, [{idle_since, now()}]) end),
    {hibernate, rabbit_event:stop_stats_timer(State, #ch.stats_timer)}.

terminate(Reason, State) ->
    {Res, _State1} = notify_queues(State),
    case Reason of
        normal            -> ok = Res;
        shutdown          -> ok = Res;
        {shutdown, _Term} -> ok = Res;
        _                 -> ok
    end,
    pg_local:leave(rabbit_channels, self()),
    rabbit_event:if_enabled(State, #ch.stats_timer,
                            fun() -> emit_stats(State) end),
    rabbit_event:notify(channel_closed, [{pid, self()}]).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_message_queue(Opt, MQ) -> rabbit_misc:format_message_queue(Opt, MQ).

%%---------------------------------------------------------------------------

reply(Reply, NewState) -> {reply, Reply, next_state(NewState), hibernate}.

noreply(NewState) -> {noreply, next_state(NewState), hibernate}.

next_state(State) -> ensure_stats_timer(send_confirms(State)).

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

handle_exception(Reason, State = #ch{protocol   = Protocol,
                                     channel    = Channel,
                                     writer_pid = WriterPid,
                                     reader_pid = ReaderPid,
                                     conn_pid   = ConnPid}) ->
    %% something bad's happened: notify_queues may not be 'ok'
    {_Result, State1} = notify_queues(State),
    case rabbit_binary_generator:map_exception(Channel, Reason, Protocol) of
        {Channel, CloseMethod} ->
            rabbit_log:error("connection ~p, channel ~p - soft error:~n~p~n",
                             [ConnPid, Channel, Reason]),
            ok = rabbit_writer:send_command(WriterPid, CloseMethod),
            {noreply, State1};
        {0, _} ->
            ReaderPid ! {channel_exit, Channel, Reason},
            {stop, normal, State1}
    end.

-ifdef(use_specs).
-spec(precondition_failed/1 :: (string()) -> no_return()).
-endif.
precondition_failed(Format) -> precondition_failed(Format, []).

-ifdef(use_specs).
-spec(precondition_failed/2 :: (string(), [any()]) -> no_return()).
-endif.
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

clear_permission_cache() ->
    erase(permission_cache),
    ok.

check_configure_permitted(Resource, #ch{user = User}) ->
    check_resource_access(User, Resource, configure).

check_write_permitted(Resource, #ch{user = User}) ->
    check_resource_access(User, Resource, write).

check_read_permitted(Resource, #ch{user = User}) ->
    check_resource_access(User, Resource, read).

check_user_id_header(#'P_basic'{user_id = undefined}, _) ->
    ok;
check_user_id_header(#'P_basic'{user_id = Username},
                     #ch{user = #user{username = Username}}) ->
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

expand_queue_name_shortcut(<<>>, #ch{most_recently_declared_queue = <<>>}) ->
    rabbit_misc:protocol_error(
      not_found, "no previously declared queue", []);
expand_queue_name_shortcut(<<>>, #ch{virtual_host = VHostPath,
                                     most_recently_declared_queue = MRDQ}) ->
    rabbit_misc:r(VHostPath, queue, MRDQ);
expand_queue_name_shortcut(QueueNameBin, #ch{virtual_host = VHostPath}) ->
    rabbit_misc:r(VHostPath, queue, QueueNameBin).

expand_routing_key_shortcut(<<>>, <<>>,
                            #ch{most_recently_declared_queue = <<>>}) ->
    rabbit_misc:protocol_error(
      not_found, "no previously declared queue", []);
expand_routing_key_shortcut(<<>>, <<>>,
                            #ch{most_recently_declared_queue = MRDQ}) ->
    MRDQ;
expand_routing_key_shortcut(_QueueNameBin, RoutingKey, _State) ->
    RoutingKey.

expand_binding(queue, DestinationNameBin, RoutingKey, State) ->
    {expand_queue_name_shortcut(DestinationNameBin, State),
     expand_routing_key_shortcut(DestinationNameBin, RoutingKey, State)};
expand_binding(exchange, DestinationNameBin, RoutingKey, State) ->
    {rabbit_misc:r(State#ch.virtual_host, exchange, DestinationNameBin),
     RoutingKey}.

check_not_default_exchange(#resource{kind = exchange, name = <<"">>}) ->
    rabbit_misc:protocol_error(
      access_refused, "operation not permitted on the default exchange", []);
check_not_default_exchange(_) ->
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

queue_blocked(QPid, State = #ch{blocking = Blocking}) ->
    case sets:is_element(QPid, Blocking) of
        false -> State;
        true  -> maybe_send_flow_ok(
                   State#ch{blocking = sets:del_element(QPid, Blocking)})
    end.

maybe_send_flow_ok(State = #ch{blocking = Blocking}) ->
    case sets:size(Blocking) of
        0 -> ok = send(#'channel.flow_ok'{active = false}, State);
        _ -> ok
    end,
    State.

record_confirms([], State) ->
    State;
record_confirms(MXs, State = #ch{confirmed = C}) ->
    State#ch{confirmed = [MXs | C]}.

confirm([], _QPid, State) ->
    State;
confirm(MsgSeqNos, QPid, State = #ch{unconfirmed = UC}) ->
    {MXs, UC1} = dtree:take(MsgSeqNos, QPid, UC),
    record_confirms(MXs, State#ch{unconfirmed = UC1}).

handle_method(#'channel.open'{}, _, State = #ch{state = starting}) ->
    {reply, #'channel.open_ok'{}, State#ch{state = running}};

handle_method(#'channel.open'{}, _, _State) ->
    rabbit_misc:protocol_error(
      command_invalid, "second 'channel.open' seen", []);

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
    {ok, State1} = notify_queues(State),
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
                                   confirm_enabled = ConfirmEnabled,
                                   trace_state     = TraceState}) ->
    ExchangeName = rabbit_misc:r(VHostPath, exchange, ExchangeNameBin),
    check_write_permitted(ExchangeName, State),
    Exchange = rabbit_exchange:lookup_or_die(ExchangeName),
    check_internal_exchange(Exchange),
    %% We decode the content's properties here because we're almost
    %% certain to want to look at delivery-mode and priority.
    DecodedContent = #content {properties = Props} =
        rabbit_binary_parser:ensure_content_decoded(Content),
    check_user_id_header(Props, State),
    check_expiration_header(Props),
    {MsgSeqNo, State1} =
        case {Tx, ConfirmEnabled} of
            {none, false} -> {undefined, State};
            {_, _}        -> SeqNo = State#ch.publish_seqno,
                             {SeqNo, State#ch{publish_seqno = SeqNo + 1}}
        end,
    case rabbit_basic:message(ExchangeName, RoutingKey, DecodedContent) of
        {ok, Message} ->
            rabbit_trace:tap_in(Message, TraceState),
            Delivery = rabbit_basic:delivery(Mandatory, Message, MsgSeqNo),
            QNames = rabbit_exchange:route(Exchange, Delivery),
            DQ = {Delivery, QNames},
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
                            requeue      = Requeue},
              _, State) ->
    reject(DeliveryTag, Requeue, Multiple, State);

handle_method(#'basic.ack'{delivery_tag = DeliveryTag,
                           multiple = Multiple},
              _, State = #ch{unacked_message_q = UAMQ, tx = Tx}) ->
    {Acked, Remaining} = collect_acks(UAMQ, DeliveryTag, Multiple),
    State1 = State#ch{unacked_message_q = Remaining},
    {noreply, case Tx of
                  none         -> ack(Acked, State1),
                                  State1;
                  {Msgs, Acks} -> Acks1 = ack_cons(ack, Acked, Acks),
                                  State1#ch{tx = {Msgs, Acks1}}
              end};

handle_method(#'basic.get'{queue = QueueNameBin,
                           no_ack = NoAck},
              _, State = #ch{writer_pid = WriterPid,
                             conn_pid   = ConnPid,
                             limiter    = Limiter,
                             next_tag   = DeliveryTag}) ->
    QueueName = expand_queue_name_shortcut(QueueNameBin, State),
    check_read_permitted(QueueName, State),
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

handle_method(#'basic.consume'{queue        = QueueNameBin,
                               consumer_tag = ConsumerTag,
                               no_local     = _, % FIXME: implement
                               no_ack       = NoAck,
                               exclusive    = ExclusiveConsume,
                               nowait       = NoWait,
                               arguments    = Args},
              _, State = #ch{conn_pid          = ConnPid,
                             limiter           = Limiter,
                             consumer_mapping  = ConsumerMapping}) ->
    case dict:find(ConsumerTag, ConsumerMapping) of
        error ->
            QueueName = expand_queue_name_shortcut(QueueNameBin, State),
            check_read_permitted(QueueName, State),
            ActualConsumerTag =
                case ConsumerTag of
                    <<>>  -> rabbit_guid:binary(rabbit_guid:gen_secure(),
                                                "amq.ctag");
                    Other -> Other
                end,

            %% We get the queue process to send the consume_ok on our
            %% behalf. This is for symmetry with basic.cancel - see
            %% the comment in that method for why.
            case rabbit_amqqueue:with_exclusive_access_or_die(
                   QueueName, ConnPid,
                   fun (Q) ->
                           {CreditArgs, OtherArgs} = parse_credit_args(Args),
                           {rabbit_amqqueue:basic_consume(
                              Q, NoAck, self(),
                              rabbit_limiter:pid(Limiter),
                              rabbit_limiter:is_active(Limiter),
                              ActualConsumerTag, ExclusiveConsume,
                              CreditArgs, OtherArgs,
                              ok_msg(NoWait, #'basic.consume_ok'{
                                       consumer_tag = ActualConsumerTag})),
                            Q}
                   end) of
                {ok, Q = #amqqueue{pid = QPid, name = QName}} ->
                    CM1 = dict:store(ActualConsumerTag, Q, ConsumerMapping),
                    State1 = monitor_delivering_queue(
                               NoAck, QPid, QName,
                               State#ch{consumer_mapping = CM1}),
                    {noreply,
                     case NoWait of
                         true  -> consumer_monitor(ActualConsumerTag, State1);
                         false -> State1
                     end};
                {{error, exclusive_consume_unavailable}, _Q} ->
                    rabbit_misc:protocol_error(
                      access_refused, "~s in exclusive use",
                      [rabbit_misc:rs(QueueName)])
            end;
        {ok, _} ->
            %% Attempted reuse of consumer tag.
            rabbit_misc:protocol_error(
              not_allowed, "attempt to reuse consumer tag '~s'", [ConsumerTag])
    end;

handle_method(#'basic.cancel'{consumer_tag = ConsumerTag,
                              nowait = NoWait},
              _, State = #ch{consumer_mapping = ConsumerMapping,
                             queue_consumers  = QCons}) ->
    OkMsg = #'basic.cancel_ok'{consumer_tag = ConsumerTag},
    case dict:find(ConsumerTag, ConsumerMapping) of
        error ->
            %% Spec requires we ignore this situation.
            return_ok(State, NoWait, OkMsg);
        {ok, Q = #amqqueue{pid = QPid}} ->
            ConsumerMapping1 = dict:erase(ConsumerTag, ConsumerMapping),
            QCons1 =
                case dict:find(QPid, QCons) of
                    error       -> QCons;
                    {ok, CTags} -> CTags1 = gb_sets:delete(ConsumerTag, CTags),
                                   case gb_sets:is_empty(CTags1) of
                                       true  -> dict:erase(QPid, QCons);
                                       false -> dict:store(QPid, CTags1, QCons)
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
                             Q, self(), ConsumerTag, ok_msg(NoWait, OkMsg))
                   end) of
                ok ->
                    {noreply, NewState};
                {error, not_found} ->
                    %% Spec requires we ignore this situation.
                    return_ok(NewState, NoWait, OkMsg)
            end
    end;

handle_method(#'basic.qos'{global = true}, _, _State) ->
    rabbit_misc:protocol_error(not_implemented, "global=true", []);

handle_method(#'basic.qos'{prefetch_size = Size}, _, _State) when Size /= 0 ->
    rabbit_misc:protocol_error(not_implemented,
                               "prefetch_size!=0 (~w)", [Size]);

handle_method(#'basic.qos'{prefetch_count = 0}, _,
              State = #ch{limiter = Limiter}) ->
    Limiter1 = rabbit_limiter:unlimit_prefetch(Limiter),
    {reply, #'basic.qos_ok'{}, State#ch{limiter = Limiter1}};

handle_method(#'basic.qos'{prefetch_count = PrefetchCount}, _,
              State = #ch{limiter = Limiter, unacked_message_q = UAMQ}) ->
    %% TODO queue:len(UAMQ) is not strictly right since that counts
    %% unacked messages from basic.get too. Pretty obscure though.
    Limiter1 = rabbit_limiter:limit_prefetch(Limiter,
                                             PrefetchCount, queue:len(UAMQ)),
    {reply, #'basic.qos_ok'{},
     maybe_limit_queues(Limiter, Limiter1, State#ch{limiter = Limiter1})};

handle_method(#'basic.recover_async'{requeue = true},
              _, State = #ch{unacked_message_q = UAMQ,
                             limiter = Limiter}) ->
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

handle_method(#'basic.reject'{delivery_tag = DeliveryTag,
                              requeue = Requeue},
              _, State) ->
    reject(DeliveryTag, Requeue, false, State);

handle_method(#'exchange.declare'{exchange = ExchangeNameBin,
                                  type = TypeNameBin,
                                  passive = false,
                                  durable = Durable,
                                  auto_delete = AutoDelete,
                                  internal = Internal,
                                  nowait = NoWait,
                                  arguments = Args},
              _, State = #ch{virtual_host = VHostPath}) ->
    CheckedType = rabbit_exchange:check_type(TypeNameBin),
    ExchangeName = rabbit_misc:r(VHostPath, exchange, ExchangeNameBin),
    check_not_default_exchange(ExchangeName),
    check_configure_permitted(ExchangeName, State),
    X = case rabbit_exchange:lookup(ExchangeName) of
            {ok, FoundX} -> FoundX;
            {error, not_found} ->
                check_name('exchange', ExchangeNameBin),
                AeKey = <<"alternate-exchange">>,
                case rabbit_misc:r_arg(VHostPath, exchange, Args, AeKey) of
                    undefined -> ok;
                    {error, {invalid_type, Type}} ->
                        precondition_failed(
                          "invalid type '~s' for arg '~s' in ~s",
                          [Type, AeKey, rabbit_misc:rs(ExchangeName)]);
                    AName     -> check_read_permitted(ExchangeName, State),
                                 check_write_permitted(AName, State),
                                 ok
                end,
                rabbit_exchange:declare(ExchangeName,
                                        CheckedType,
                                        Durable,
                                        AutoDelete,
                                        Internal,
                                        Args)
        end,
    ok = rabbit_exchange:assert_equivalence(X, CheckedType, Durable,
                                            AutoDelete, Internal, Args),
    return_ok(State, NoWait, #'exchange.declare_ok'{});

handle_method(#'exchange.declare'{exchange = ExchangeNameBin,
                                  passive = true,
                                  nowait = NoWait},
              _, State = #ch{virtual_host = VHostPath}) ->
    ExchangeName = rabbit_misc:r(VHostPath, exchange, ExchangeNameBin),
    check_not_default_exchange(ExchangeName),
    _ = rabbit_exchange:lookup_or_die(ExchangeName),
    return_ok(State, NoWait, #'exchange.declare_ok'{});

handle_method(#'exchange.delete'{exchange = ExchangeNameBin,
                                 if_unused = IfUnused,
                                 nowait = NoWait},
              _, State = #ch{virtual_host = VHostPath}) ->
    ExchangeName = rabbit_misc:r(VHostPath, exchange, ExchangeNameBin),
    check_not_default_exchange(ExchangeName),
    check_configure_permitted(ExchangeName, State),
    case rabbit_exchange:delete(ExchangeName, IfUnused) of
        {error, not_found} ->
            return_ok(State, NoWait,  #'exchange.delete_ok'{});
        {error, in_use} ->
            precondition_failed("~s in use", [rabbit_misc:rs(ExchangeName)]);
        ok ->
            return_ok(State, NoWait,  #'exchange.delete_ok'{})
    end;

handle_method(#'exchange.bind'{destination = DestinationNameBin,
                               source = SourceNameBin,
                               routing_key = RoutingKey,
                               nowait = NoWait,
                               arguments = Arguments}, _, State) ->
    binding_action(fun rabbit_binding:add/2,
                   SourceNameBin, exchange, DestinationNameBin, RoutingKey,
                   Arguments, #'exchange.bind_ok'{}, NoWait, State);

handle_method(#'exchange.unbind'{destination = DestinationNameBin,
                                 source = SourceNameBin,
                                 routing_key = RoutingKey,
                                 nowait = NoWait,
                                 arguments = Arguments}, _, State) ->
    binding_action(fun rabbit_binding:remove/2,
                   SourceNameBin, exchange, DestinationNameBin, RoutingKey,
                   Arguments, #'exchange.unbind_ok'{}, NoWait, State);

handle_method(#'queue.declare'{queue       = QueueNameBin,
                               passive     = false,
                               durable     = Durable,
                               exclusive   = ExclusiveDeclare,
                               auto_delete = AutoDelete,
                               nowait      = NoWait,
                               arguments   = Args} = Declare,
              _, State = #ch{virtual_host        = VHostPath,
                             conn_pid            = ConnPid,
                             queue_collector_pid = CollectorPid}) ->
    Owner = case ExclusiveDeclare of
                true  -> ConnPid;
                false -> none
            end,
    ActualNameBin = case QueueNameBin of
                        <<>>  -> rabbit_guid:binary(rabbit_guid:gen_secure(),
                                                    "amq.gen");
                        Other -> check_name('queue', Other)
                    end,
    QueueName = rabbit_misc:r(VHostPath, queue, ActualNameBin),
    check_configure_permitted(QueueName, State),
    case rabbit_amqqueue:with(
           QueueName,
           fun (Q) -> ok = rabbit_amqqueue:assert_equivalence(
                             Q, Durable, AutoDelete, Args, Owner),
                      rabbit_amqqueue:stat(Q)
           end) of
        {ok, MessageCount, ConsumerCount} ->
            return_queue_declare_ok(QueueName, NoWait, MessageCount,
                                    ConsumerCount, State);
        {error, not_found} ->
            DlxKey = <<"x-dead-letter-exchange">>,
            case rabbit_misc:r_arg(VHostPath, exchange, Args, DlxKey) of
               undefined ->
                   ok;
               {error, {invalid_type, Type}} ->
                    precondition_failed(
                      "invalid type '~s' for arg '~s' in ~s",
                      [Type, DlxKey, rabbit_misc:rs(QueueName)]);
               DLX ->
                   check_read_permitted(QueueName, State),
                   check_write_permitted(DLX, State),
                   ok
            end,
            case rabbit_amqqueue:declare(QueueName, Durable, AutoDelete,
                                         Args, Owner) of
                {new, #amqqueue{pid = QPid}} ->
                    %% We need to notify the reader within the channel
                    %% process so that we can be sure there are no
                    %% outstanding exclusive queues being declared as
                    %% the connection shuts down.
                    ok = case Owner of
                             none -> ok;
                             _    -> rabbit_queue_collector:register(
                                       CollectorPid, QPid)
                         end,
                    return_queue_declare_ok(QueueName, NoWait, 0, 0, State);
                {existing, _Q} ->
                    %% must have been created between the stat and the
                    %% declare. Loop around again.
                    handle_method(Declare, none, State);
                {absent, Q} ->
                    rabbit_misc:absent(Q)
            end;
        {error, {absent, Q}} ->
            rabbit_misc:absent(Q)
    end;

handle_method(#'queue.declare'{queue   = QueueNameBin,
                               passive = true,
                               nowait  = NoWait},
              _, State = #ch{virtual_host = VHostPath,
                             conn_pid     = ConnPid}) ->
    QueueName = rabbit_misc:r(VHostPath, queue, QueueNameBin),
    {{ok, MessageCount, ConsumerCount}, #amqqueue{} = Q} =
        rabbit_amqqueue:with_or_die(
          QueueName, fun (Q) -> {rabbit_amqqueue:stat(Q), Q} end),
    ok = rabbit_amqqueue:check_exclusive_access(Q, ConnPid),
    return_queue_declare_ok(QueueName, NoWait, MessageCount, ConsumerCount,
                            State);

handle_method(#'queue.delete'{queue = QueueNameBin,
                              if_unused = IfUnused,
                              if_empty = IfEmpty,
                              nowait = NoWait},
              _, State = #ch{conn_pid = ConnPid}) ->
    QueueName = expand_queue_name_shortcut(QueueNameBin, State),
    check_configure_permitted(QueueName, State),
    case rabbit_amqqueue:with(
           QueueName,
           fun (Q) ->
                   rabbit_amqqueue:check_exclusive_access(Q, ConnPid),
                   rabbit_amqqueue:delete(Q, IfUnused, IfEmpty)
           end,
           fun (not_found)   -> {ok, 0};
               ({absent, Q}) -> rabbit_misc:absent(Q)
           end) of
        {error, in_use} ->
            precondition_failed("~s in use", [rabbit_misc:rs(QueueName)]);
        {error, not_empty} ->
            precondition_failed("~s not empty", [rabbit_misc:rs(QueueName)]);
        {ok, PurgedMessageCount} ->
            return_ok(State, NoWait,
                      #'queue.delete_ok'{message_count = PurgedMessageCount})
    end;

handle_method(#'queue.bind'{queue = QueueNameBin,
                            exchange = ExchangeNameBin,
                            routing_key = RoutingKey,
                            nowait = NoWait,
                            arguments = Arguments}, _, State) ->
    binding_action(fun rabbit_binding:add/2,
                   ExchangeNameBin, queue, QueueNameBin, RoutingKey, Arguments,
                   #'queue.bind_ok'{}, NoWait, State);

handle_method(#'queue.unbind'{queue = QueueNameBin,
                              exchange = ExchangeNameBin,
                              routing_key = RoutingKey,
                              arguments = Arguments}, _, State) ->
    binding_action(fun rabbit_binding:remove/2,
                   ExchangeNameBin, queue, QueueNameBin, RoutingKey, Arguments,
                   #'queue.unbind_ok'{}, false, State);

handle_method(#'queue.purge'{queue = QueueNameBin,
                             nowait = NoWait},
              _, State = #ch{conn_pid = ConnPid}) ->
    QueueName = expand_queue_name_shortcut(QueueNameBin, State),
    check_read_permitted(QueueName, State),
    {ok, PurgedMessageCount} = rabbit_amqqueue:with_exclusive_access_or_die(
                                 QueueName, ConnPid,
                                 fun (Q) -> rabbit_amqqueue:purge(Q) end),
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

handle_method(#'channel.flow'{active = true}, _,
              State = #ch{limiter = Limiter}) ->
    Limiter1 = rabbit_limiter:unblock(Limiter),
    {reply, #'channel.flow_ok'{active = true},
     maybe_limit_queues(Limiter, Limiter1, State#ch{limiter = Limiter1})};

handle_method(#'channel.flow'{active = false}, _,
              State = #ch{consumer_mapping = Consumers,
                          limiter          = Limiter}) ->
    case rabbit_limiter:is_blocked(Limiter) of
        true  -> {noreply, maybe_send_flow_ok(State)};
        false -> Limiter1 = rabbit_limiter:block(Limiter),
                 State1 = maybe_limit_queues(Limiter, Limiter1,
                                             State#ch{limiter = Limiter1}),
                 %% The semantics of channel.flow{active=false}
                 %% require that no messages are delivered after the
                 %% channel.flow_ok has been sent. We accomplish that
                 %% by "flushing" all messages in flight from the
                 %% consumer queues to us. To do this we tell all the
                 %% queues to invoke rabbit_channel:flushed/2, which
                 %% will send us a {flushed, ...} message that appears
                 %% *after* all the {deliver, ...} messages. We keep
                 %% track of all the QPids thus asked, and once all of
                 %% them have responded (or died) we send the
                 %% channel.flow_ok.
                 QPids = consumer_queues(Consumers),
                 ok = rabbit_amqqueue:flush_all(QPids, self()),
                 {noreply, maybe_send_flow_ok(
                             State1#ch{blocking = sets:from_list(QPids)})}
    end;

handle_method(#'basic.credit'{consumer_tag = CTag,
                              credit       = Credit,
                              drain        = Drain}, _,
              State = #ch{consumer_mapping = Consumers}) ->
    case dict:find(CTag, Consumers) of
        {ok, Q} -> ok = rabbit_amqqueue:credit(
                          Q, self(), CTag, Credit, Drain),
                   {noreply, State};
        error   -> precondition_failed("unknown consumer tag '~s'", [CTag])
    end;

handle_method(_MethodRecord, _Content, _State) ->
    rabbit_misc:protocol_error(
      command_invalid, "unimplemented method", []).

%%----------------------------------------------------------------------------

consumer_monitor(ConsumerTag,
                 State = #ch{consumer_mapping = ConsumerMapping,
                             queue_monitors   = QMons,
                             queue_consumers  = QCons,
                             capabilities     = Capabilities}) ->
    case rabbit_misc:table_lookup(
           Capabilities, <<"consumer_cancel_notify">>) of
        {bool, true} ->
            #amqqueue{pid = QPid} = dict:fetch(ConsumerTag, ConsumerMapping),
            QCons1 = dict:update(QPid,
                                 fun (CTags) ->
                                         gb_sets:insert(ConsumerTag, CTags)
                                 end,
                                 gb_sets:singleton(ConsumerTag),
                                 QCons),
            State#ch{queue_monitors  = pmon:monitor(QPid, QMons),
                     queue_consumers = QCons1};
        _ ->
            State
    end.

monitor_delivering_queue(NoAck, QPid, QName,
                         State = #ch{queue_names       = QNames,
                                     queue_monitors    = QMons,
                                     delivering_queues = DQ}) ->
    State#ch{queue_names       = dict:store(QPid, QName, QNames),
             queue_monitors    = pmon:monitor(QPid, QMons),
             delivering_queues = case NoAck of
                                     true  -> DQ;
                                     false -> sets:add_element(QPid, DQ)
                                 end}.

handle_publishing_queue_down(QPid, Reason, State = #ch{unconfirmed = UC}) ->
    case rabbit_misc:is_abnormal_exit(Reason) of
        true  -> {MXs, UC1} = dtree:take_all(QPid, UC),
                 send_nacks(MXs, State#ch{unconfirmed = UC1});
        false -> {MXs, UC1} = dtree:take(QPid, UC),
                 record_confirms(MXs, State#ch{unconfirmed = UC1})
    end.

handle_consuming_queue_down(QPid,
                            State = #ch{consumer_mapping = ConsumerMapping,
                                        queue_consumers  = QCons,
                                        queue_names      = QNames}) ->
    ConsumerTags = case dict:find(QPid, QCons) of
                       error       -> gb_sets:new();
                       {ok, CTags} -> CTags
                   end,
    ConsumerMapping1 =
        gb_sets:fold(fun (CTag, CMap) ->
                             ok = send(#'basic.cancel'{consumer_tag = CTag,
                                                       nowait       = true},
                                       State),
                             rabbit_event:notify(
                               consumer_deleted,
                               [{consumer_tag, CTag},
                                {channel,      self()},
                                {queue,        dict:fetch(QPid, QNames)}]),
                             dict:erase(CTag, CMap)
                     end, ConsumerMapping, ConsumerTags),
    State#ch{consumer_mapping = ConsumerMapping1,
             queue_consumers  = dict:erase(QPid, QCons)}.

handle_delivering_queue_down(QPid, State = #ch{delivering_queues = DQ}) ->
    State#ch{delivering_queues = sets:del_element(QPid, DQ)}.

parse_credit_args(Arguments) ->
    case rabbit_misc:table_lookup(Arguments, <<"x-credit">>) of
        {table, T} -> {case {rabbit_misc:table_lookup(T, <<"credit">>),
                             rabbit_misc:table_lookup(T, <<"drain">>)} of
                           {{long, Credit}, {bool, Drain}} -> {Credit, Drain};
                           _                               -> none
                       end, lists:keydelete(<<"x-credit">>, 1, Arguments)};
        undefined  -> {none, Arguments}
    end.

binding_action(Fun, ExchangeNameBin, DestinationType, DestinationNameBin,
               RoutingKey, Arguments, ReturnMethod, NoWait,
               State = #ch{virtual_host = VHostPath,
                           conn_pid     = ConnPid }) ->
    {DestinationName, ActualRoutingKey} =
        expand_binding(DestinationType, DestinationNameBin, RoutingKey, State),
    check_write_permitted(DestinationName, State),
    ExchangeName = rabbit_misc:r(VHostPath, exchange, ExchangeNameBin),
    [check_not_default_exchange(N) || N <- [DestinationName, ExchangeName]],
    check_read_permitted(ExchangeName, State),
    case Fun(#binding{source      = ExchangeName,
                      destination = DestinationName,
                      key         = ActualRoutingKey,
                      args        = Arguments},
             fun (_X, Q = #amqqueue{}) ->
                     try rabbit_amqqueue:check_exclusive_access(Q, ConnPid)
                     catch exit:Reason -> {error, Reason}
                     end;
                 (_X, #exchange{}) ->
                     ok
             end) of
        {error, {resources_missing, [{not_found, Name} | _]}} ->
            rabbit_misc:not_found(Name);
        {error, {resources_missing, [{absent, Q} | _]}} ->
            rabbit_misc:absent(Q);
        {error, binding_not_found} ->
            rabbit_misc:protocol_error(
              not_found, "no binding ~s between ~s and ~s",
              [RoutingKey, rabbit_misc:rs(ExchangeName),
               rabbit_misc:rs(DestinationName)]);
        {error, {binding_invalid, Fmt, Args}} ->
            rabbit_misc:protocol_error(precondition_failed, Fmt, Args);
        {error, #amqp_error{} = Error} ->
            rabbit_misc:protocol_error(Error);
        ok -> return_ok(State, NoWait, ReturnMethod)
    end.

basic_return(#basic_message{exchange_name = ExchangeName,
                            routing_keys  = [RoutingKey | _CcRoutes],
                            content       = Content},
             #ch{protocol = Protocol, writer_pid = WriterPid}, Reason) ->
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
              rabbit_amqqueue:reject(QPid, MsgIds, Requeue, self())
      end, Acked),
    ok = notify_limiter(Limiter, Acked).

record_sent(ConsumerTag, AckRequired,
            Msg = {QName, QPid, MsgId, Redelivered, _Message},
            State = #ch{unacked_message_q = UAMQ,
                        next_tag          = DeliveryTag,
                        trace_state       = TraceState}) ->
    ?INCR_STATS([{queue_stats, QName, 1}], case {ConsumerTag, AckRequired} of
                                               {none,  true} -> get;
                                               {none, false} -> get_no_ack;
                                               {_   ,  true} -> deliver;
                                               {_   , false} -> deliver_no_ack
                                           end, State),
    case Redelivered of
        true  -> ?INCR_STATS([{queue_stats, QName, 1}], redeliver, State);
        false -> ok
    end,
    rabbit_trace:tap_out(Msg, TraceState),
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
              ?INCR_STATS(case dict:find(QPid, QNames) of
                              {ok, QName} -> Count = length(MsgIds),
                                             [{queue_stats, QName, Count}];
                              error       -> []
                          end, ack, State)
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
    {rabbit_amqqueue:notify_down_all(QPids, self()), State#ch{state = closing}}.

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

maybe_limit_queues(OldLimiter, NewLimiter, State) ->
    case ((not rabbit_limiter:is_active(OldLimiter)) andalso
          rabbit_limiter:is_active(NewLimiter)) of
        true  -> Queues = consumer_queues(State#ch.consumer_mapping),
                 rabbit_amqqueue:activate_limit_all(Queues, self());
        false -> ok
    end,
    State.

consumer_queues(Consumers) ->
    lists:usort([QPid ||
                    {_Key, #amqqueue{pid = QPid}} <- dict:to_list(Consumers)]).

%% tell the limiter about the number of acks that have been received
%% for messages delivered to subscribed consumers, but not acks for
%% messages sent in a response to a basic.get (identified by their
%% 'none' consumer tag)
notify_limiter(Limiter, Acked) ->
    %% optimisation: avoid the potentially expensive 'foldl' in the
    %% common case.
     case rabbit_limiter:is_prefetch_limited(Limiter) of
        false -> ok;
        true  -> case lists:foldl(fun ({_, none, _}, Acc) -> Acc;
                                      ({_,    _, _}, Acc) -> Acc + 1
                                  end, 0, Acked) of
                     0     -> ok;
                     Count -> rabbit_limiter:ack(Limiter, Count)
                 end
    end.

deliver_to_queues({#delivery{message    = #basic_message{exchange_name = XName},
                             msg_seq_no = undefined,
                             mandatory  = false},
                   []}, State) -> %% optimisation
    ?INCR_STATS([{exchange_stats, XName, 1}], publish, State),
    State;
deliver_to_queues({Delivery = #delivery{message    = Message = #basic_message{
                                                       exchange_name = XName},
                                        msg_seq_no = MsgSeqNo},
                   DelQNames}, State = #ch{queue_names    = QNames,
                                           queue_monitors = QMons}) ->
    Qs = rabbit_amqqueue:lookup(DelQNames),
    {RoutingRes, DeliveredQPids} = rabbit_amqqueue:deliver_flow(Qs, Delivery),
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
                            {case dict:is_key(QPid, QNames0) of
                                 true  -> QNames0;
                                 false -> dict:store(QPid, QName, QNames0)
                             end, pmon:monitor(QPid, QMons0)}
                    end, {QNames, pmon:monitor_all(DeliveredQPids, QMons)}, Qs),
    State1 = process_routing_result(RoutingRes, DeliveredQPids,
                                    XName, MsgSeqNo, Message,
                                    State#ch{queue_names    = QNames1,
                                             queue_monitors = QMons1}),
    ?INCR_STATS([{exchange_stats, XName, 1} |
                 [{queue_exchange_stats, {QName, XName}, 1} ||
                     QPid        <- DeliveredQPids,
                     {ok, QName} <- [dict:find(QPid, QNames1)]]],
                publish, State1),
    State1.

process_routing_result(routed,     _,     _, undefined,   _, State) ->
    State;
process_routing_result(routed,    [], XName,  MsgSeqNo,   _, State) ->
    record_confirms([{MsgSeqNo, XName}], State);
process_routing_result(routed, QPids, XName,  MsgSeqNo,   _, State) ->
    State#ch{unconfirmed = dtree:insert(MsgSeqNo, QPids, XName,
                                        State#ch.unconfirmed)};
process_routing_result(unroutable, _, XName,  MsgSeqNo, Msg, State) ->
    ok = basic_return(Msg, State, no_route),
    ?INCR_STATS([{exchange_stats, XName, 1}], return_unroutable, State),
    case MsgSeqNo of
        undefined -> State;
        _         -> record_confirms([{MsgSeqNo, XName}], State)
    end.

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

send_confirms(State = #ch{tx = none, confirmed = []}) ->
    State;
send_confirms(State = #ch{tx = none, confirmed = C}) ->
    MsgSeqNos =
        lists:foldl(
          fun ({MsgSeqNo, XName}, MSNs) ->
                  ?INCR_STATS([{exchange_stats, XName, 1}], confirm, State),
                  [MsgSeqNo | MSNs]
          end, [], lists:append(C)),
    send_confirms(MsgSeqNos, State#ch{confirmed = []});
send_confirms(State) ->
    maybe_complete_tx(State).

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
i(vhost,          #ch{virtual_host     = VHost})   -> VHost;
i(transactional,  #ch{tx               = Tx})      -> Tx =/= none;
i(confirm,        #ch{confirm_enabled  = CE})      -> CE;
i(name,           State)                           -> name(State);
i(consumer_count,          #ch{consumer_mapping = CM})    -> dict:size(CM);
i(messages_unconfirmed,    #ch{unconfirmed = UC})         -> dtree:size(UC);
i(messages_unacknowledged, #ch{unacked_message_q = UAMQ}) -> queue:len(UAMQ);
i(messages_uncommitted,    #ch{tx = {Msgs, _Acks}})       -> queue:len(Msgs);
i(messages_uncommitted,    #ch{})                         -> 0;
i(acks_uncommitted,        #ch{tx = {_Msgs, Acks}})       -> ack_len(Acks);
i(acks_uncommitted,        #ch{})                         -> 0;
i(prefetch_count, #ch{limiter = Limiter}) ->
    rabbit_limiter:get_prefetch_limit(Limiter);
i(client_flow_blocked, #ch{limiter = Limiter}) ->
    rabbit_limiter:is_blocked(Limiter);
i(Item, _) ->
    throw({bad_argument, Item}).

name(#ch{conn_name = ConnName, channel = Channel}) ->
    list_to_binary(rabbit_misc:format("~s (~p)", [ConnName, Channel])).

incr_stats(Incs, Measure) ->
    [update_measures(Type, Key, Inc, Measure) || {Type, Key, Inc} <- Incs].

update_measures(Type, Key, Inc, Measure) ->
    Measures = case get({Type, Key}) of
                   undefined -> [];
                   D         -> D
               end,
    Cur = case orddict:find(Measure, Measures) of
              error   -> 0;
              {ok, C} -> C
          end,
    put({Type, Key}, orddict:store(Measure, Cur + Inc, Measures)).

emit_stats(State) ->
    emit_stats(State, []).

emit_stats(State, Extra) ->
    Coarse = infos(?STATISTICS_KEYS, State),
    case rabbit_event:stats_level(State, #ch.stats_timer) of
        coarse -> rabbit_event:notify(channel_stats, Extra ++ Coarse);
        fine   -> Fine = [{channel_queue_stats,
                           [{QName, Stats} ||
                               {{queue_stats,       QName}, Stats} <- get()]},
                          {channel_exchange_stats,
                           [{XName, Stats} ||
                               {{exchange_stats,    XName}, Stats} <- get()]},
                          {channel_queue_exchange_stats,
                           [{QX, Stats} ||
                               {{queue_exchange_stats, QX}, Stats} <- get()]}],
                  rabbit_event:notify(channel_stats, Extra ++ Coarse ++ Fine)
    end.

erase_queue_stats(QName) ->
    erase({queue_stats, QName}),
    [erase({queue_exchange_stats, QX}) ||
        {{queue_exchange_stats, QX = {QName0, _}}, _} <- get(),
        QName0 =:= QName].
