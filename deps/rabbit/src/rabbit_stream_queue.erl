%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_stream_queue).
-feature(maybe_expr, enable).

-include("mc.hrl").

-behaviour(rabbit_queue_type).

-export([is_enabled/0,
         is_compatible/3,
         declare/2,
         delete/4,
         purge/1,
         policy_changed/1,
         recover/2,
         is_recoverable/1,
         consume/3,
         cancel/3,
         handle_event/3,
         deliver/3,
         settle/5,
         credit_v1/5,
         credit/6,
         dequeue/5,
         info/2,
         queue_length/1,
         get_replicas/1,
         transfer_leadership/2,
         init/1,
         close/1,
         update/2,
         state_info/1,
         stat/1,
         format/2,
         capabilities/0,
         notify_decorators/1,
         is_stateful/0]).

-export([list_with_minimum_quorum/0]).

-export([restart_stream/3,
         add_replica/3,
         delete_replica/3,
         delete_all_replicas/1]).
-export([format_osiris_event/2]).
-export([update_stream_conf/2]).
-export([readers/1]).

-export([parse_offset_arg/1,
         filter_spec/1]).

-export([status/2,
         tracking_status/2,
         get_overview/1]).

-export([check_max_segment_size_bytes/1]).

-export([policy_apply_to_name/0,
         stop/1,
         drain/1,
         revive/0,
         queue_vm_stats_sups/0,
         queue_vm_ets/0]).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("amqqueue.hrl").

-define(INFO_KEYS, [name, durable, auto_delete, arguments, leader, members, online, state,
                    messages, messages_ready, messages_unacknowledged, committed_offset,
                    policy, operator_policy, effective_policy_definition, type, memory,
                    consumers, segments]).

-type appender_seq() :: non_neg_integer().

-type msg() :: term(). %% TODO: refine

-record(stream, {mode :: rabbit_queue_type:consume_mode(),
                 delivery_count :: none | rabbit_queue_type:delivery_count(),
                 credit :: rabbit_queue_type:credit(),
                 ack :: boolean(),
                 start_offset = 0 :: non_neg_integer(),
                 listening_offset = 0 :: non_neg_integer(),
                 last_consumed_offset :: non_neg_integer(),
                 log :: undefined | osiris_log:state(),
                 chunk_iterator :: undefined | osiris_log:chunk_iterator(),
                 %% These messages were already read ahead from the Osiris log,
                 %% were part of an uncompressed sub batch, and are buffered in
                 %% reversed order until the consumer has more credits to consume them.
                 buffer_msgs_rev = [] :: [rabbit_amqqueue:qmsg()],
                 filter :: rabbit_amqp_filter:expression(),
                 reader_options :: map()}).

-record(stream_client, {stream_id :: string(),
                        name :: rabbit_amqqueue:name(),
                        leader :: pid(),
                        local_pid :: undefined | pid(),
                        next_seq = 1 :: non_neg_integer(),
                        correlation = #{} :: #{appender_seq() =>
                                               {rabbit_queue_type:correlation(), msg()}},
                        soft_limit :: non_neg_integer(),
                        slow = false :: boolean(),
                        readers = #{} :: #{rabbit_types:ctag() => #stream{}},
                        writer_id :: binary()
                       }).

-import(rabbit_queue_type_util, [args_policy_lookup/3]).
-import(rabbit_misc, [queue_resource/2]).

-rabbit_boot_step(
   {?MODULE,
    [{description, "Stream queue: queue type"},
     {mfa,      {rabbit_registry, register,
                    [queue, <<"stream">>, ?MODULE]}},
     {cleanup,  {rabbit_registry, unregister,
                 [queue, <<"stream">>]}},
     {requires, rabbit_registry}
    ]}).

-type client() :: #stream_client{}.

-spec is_enabled() -> boolean().
is_enabled() -> true.

-spec is_compatible(boolean(), boolean(), boolean()) -> boolean().
is_compatible(_Durable = true,
              _Exclusive = false,
              _AutoDelete = false) ->
    true;
is_compatible(_, _, _) ->
    false.


-spec declare(amqqueue:amqqueue(), node()) ->
    {'new' | 'existing', amqqueue:amqqueue()} |
    {protocol_error, Type :: atom(), Reason :: string(), Args :: term()}.
declare(Q, _Node) when ?amqqueue_is_stream(Q) ->
    maybe
        ok ?= rabbit_queue_type_util:check_auto_delete(Q),
        ok ?= rabbit_queue_type_util:check_exclusive(Q),
        ok ?= rabbit_queue_type_util:check_non_durable(Q),
        ok ?= check_max_segment_size_bytes(Q),
        ok ?= check_filter_size(Q),
        create_stream(Q)
    end.

check_max_segment_size_bytes(Q) ->
    Args = amqqueue:get_arguments(Q),
    case rabbit_misc:table_lookup(Args, <<"x-stream-max-segment-size-bytes">>) of
        undefined ->
            ok;
        {_Type, Val} when Val > ?MAX_STREAM_MAX_SEGMENT_SIZE ->
            {protocol_error, precondition_failed, "Exceeded max value for x-stream-max-segment-size-bytes",
             []};
        _ ->
            ok
    end.

check_filter_size(Q) ->
    Args = amqqueue:get_arguments(Q),
    case rabbit_misc:table_lookup(Args, <<"x-stream-filter-size-bytes">>) of
        undefined ->
            ok;
        {_Type, Val} when Val > 255 orelse Val < 16 ->
            {protocol_error, precondition_failed,
             "Invalid value for  x-stream-filter-size-bytes", []};
        _ ->
            ok
    end.

create_stream(Q0) ->
    Arguments = amqqueue:get_arguments(Q0),
    QName = amqqueue:get_name(Q0),
    Opts = amqqueue:get_options(Q0),
    ActingUser = maps:get(user, Opts, ?UNKNOWN_USER),
    Conf0 = make_stream_conf(Q0),
    InitialClusterSize = initial_cluster_size(
                           args_policy_lookup(<<"initial-cluster-size">>,
                                              fun policy_precedence/2, Q0)),
    {Leader, Followers} = rabbit_queue_location:select_leader_and_followers(Q0, InitialClusterSize),
    Conf = maps:merge(Conf0, #{nodes => [Leader | Followers],
                               leader_node => Leader,
                               replica_nodes => Followers}),
    Q1 = amqqueue:set_type_state(Q0, Conf),
    case rabbit_amqqueue:internal_declare(Q1, false) of
        {created, Q} ->
            case rabbit_stream_coordinator:new_stream(Q, Leader) of
                {ok, {ok, LeaderPid}, _} ->
                    %% update record with leader pid
                    case set_leader_pid(LeaderPid, amqqueue:get_name(Q)) of
                        ok ->
                            rabbit_event:notify(queue_created,
                                                [{name, QName},
                                                 {durable, true},
                                                 {auto_delete, false},
                                                 {arguments, Arguments},
                                                 {type, amqqueue:get_type(Q1)},
                                                 {user_who_performed_action,
                                                  ActingUser}]),
                            {new, Q};
                        {error, timeout} ->
                            {protocol_error, internal_error,
                             "Could not set leader PID for ~ts on node '~ts' "
                             "because the metadata store operation timed out",
                             [rabbit_misc:rs(QName), node()]}
                    end;
                Error ->
                    _ = rabbit_amqqueue:internal_delete(Q, ActingUser),
                    {protocol_error, internal_error, "Cannot declare ~ts on node '~ts': ~255p",
                     [rabbit_misc:rs(QName), node(), Error]}
            end;
        {existing, Q} ->
            {existing, Q};
        {absent, Q, Reason} ->
            {absent, Q, Reason};
        {error, timeout} ->
            {protocol_error, internal_error,
             "Could not declare ~ts on node '~ts' because the metadata store "
             "operation timed out",
             [rabbit_misc:rs(QName), node()]}
    end.

-spec delete(amqqueue:amqqueue(), boolean(),
             boolean(), rabbit_types:username()) ->
    rabbit_types:ok(non_neg_integer()) |
    rabbit_types:error(timeout) |
    {protocol_error, Type :: atom(), Reason :: string(), Args :: term()}.
delete(Q, _IfUnused, _IfEmpty, ActingUser) ->
    case rabbit_stream_coordinator:delete_stream(Q, ActingUser) of
        {ok, Reply} ->
            Reply;
        {error, timeout} = Err ->
            Err;
        Error ->
            {protocol_error, internal_error, "Cannot delete ~ts on node '~ts': ~255p ",
             [rabbit_misc:rs(amqqueue:get_name(Q)), node(), Error]}
    end.

-spec purge(amqqueue:amqqueue()) ->
    {ok, non_neg_integer()} | {error, term()}.
purge(_) ->
    {error, not_supported}.

-spec policy_changed(amqqueue:amqqueue()) -> 'ok'.
policy_changed(Q) ->
    _ = rabbit_stream_coordinator:policy_changed(Q),
    ok.

stat(Q) ->
    Conf = amqqueue:get_type_state(Q),
    case maps:get(leader_node, Conf) of
        Node when Node =/= node() ->
            case rpc:call(Node, ?MODULE, info, [Q, [messages]]) of
                {badrpc, _} ->
                    {ok, 0, 0};
                [{messages, Messages}] ->
                    {ok, Messages, 0}
            end;
        _ ->
            {ok, i(messages, Q), 0}
    end.

format(Q, Ctx) ->
    case amqqueue:get_pid(Q) of
        Pid when is_pid(Pid) ->
            LeaderNode = node(Pid),
            Nodes = lists:sort(get_nodes(Q)),
            Running = case Ctx of
                          #{running_nodes := Running0} ->
                              Running0;
                          _ ->
                              %% WARN: slow
                              rabbit_nodes:list_running()
                      end,
            Online = [N || N <- Nodes, lists:member(N, Running)],
            State = case is_minority(Nodes, Online) of
                        true when length(Online) == 0 ->
                            down;
                        true ->
                            minority;
                        false ->
                            case lists:member(LeaderNode, Online) of
                                true ->
                                    running;
                                false ->
                                    down
                            end
                    end,
            [{type, rabbit_queue_type:short_alias_of(?MODULE)},
             {state, State},
             {leader, LeaderNode},
             {online, Online},
             {members, Nodes},
             {node, node(Pid)}];
        _ ->
            [{type, rabbit_queue_type:short_alias_of(?MODULE)},
             {state, down}]
    end.

consume(Q, #{mode := {simple_prefetch, 0}}, _)
  when ?amqqueue_is_stream(Q) ->
    {error, precondition_failed,
     "consumer prefetch count is not set for stream ~ts",
     [rabbit_misc:rs(amqqueue:get_name(Q))]};
consume(Q, #{no_ack := true,
             mode := {simple_prefetch, _}}, _)
  when ?amqqueue_is_stream(Q) ->
    {error, not_implemented,
     "automatic acknowledgement not supported by stream ~ts",
     [rabbit_misc:rs(amqqueue:get_name(Q))]};
consume(Q, #{limiter_active := true}, _State)
  when ?amqqueue_is_stream(Q) ->
    {error, not_implemented,
     "~ts does not support global qos",
     [rabbit_misc:rs(amqqueue:get_name(Q))]};
consume(Q, Spec, #stream_client{} = QState0)
  when ?amqqueue_is_stream(Q) ->
    QName = amqqueue:get_name(Q),
    %% Messages should include the offset as a custom header.
    case get_local_pid(QState0) of
        {LocalPid, QState} when is_pid(LocalPid) ->
            #{no_ack := NoAck,
              channel_pid := ChPid,
              mode := Mode,
              consumer_tag := ConsumerTag,
              exclusive_consume := ExclusiveConsume,
              args := Args,
              ok_msg := OkMsg,
              acting_user := ActingUser} = Spec,
            rabbit_log:debug("~s:~s Local pid resolved ~0p",
                             [?MODULE, ?FUNCTION_NAME, LocalPid]),
            case parse_offset_arg(
                   rabbit_misc:table_lookup(Args, <<"x-stream-offset">>)) of
                {ok, OffsetSpec} ->
                    ConsumerPrefetchCount = case Mode of
                                                {simple_prefetch, C} -> C;
                                                _ -> 0
                                            end,
                    AckRequired = not NoAck,
                    rabbit_core_metrics:consumer_created(
                      ChPid, ConsumerTag, ExclusiveConsume, AckRequired,
                      QName, ConsumerPrefetchCount, true, up, Args),
                    rabbit_event:notify(consumer_created,
                                        [{consumer_tag,   ConsumerTag},
                                         {exclusive,      ExclusiveConsume},
                                         {ack_required,   AckRequired},
                                         {channel,        ChPid},
                                         {queue,          QName},
                                         {prefetch_count, ConsumerPrefetchCount},
                                         {arguments,      Args},
                                         {user_who_performed_action, ActingUser}]),
                    %% reply needs to be sent before the stream
                    %% begins sending
                    maybe_send_reply(ChPid, OkMsg),
                    _ = rabbit_stream_coordinator:register_local_member_listener(Q),
                    Filter = maps:get(filter, Spec, undefined),
                    begin_stream(QState, ConsumerTag, OffsetSpec, Mode,
                                 AckRequired, Filter, filter_spec(Args));
                {error, Reason} ->
                    {error, precondition_failed,
                     "failed consuming from stream ~ts: ~tp",
                     [rabbit_misc:rs(QName), Reason]}
            end;
        {undefined, _} ->
            {error, precondition_failed,
             "stream ~ts does not have a running replica on the local node",
             [rabbit_misc:rs(QName)]}
    end.

-spec parse_offset_arg(undefined |
                       osiris:offset() |
                       {longstr, binary()} |
                       {timestamp, non_neg_integer()} |
                       {term(), non_neg_integer()}) ->
    {ok, osiris:offset_spec()} | {error, term()}.
parse_offset_arg(undefined) ->
    {ok, next};
parse_offset_arg({_, <<"first">>}) ->
    {ok, first};
parse_offset_arg({_, <<"last">>}) ->
    {ok, last};
parse_offset_arg({_, <<"next">>}) ->
    {ok, next};
parse_offset_arg({timestamp, V}) ->
    {ok, {timestamp, V * 1000}};
parse_offset_arg({longstr, V}) ->
    case rabbit_amqqueue:check_max_age(V) of
        {error, _} = Err ->
            Err;
        Ms ->
            {ok, {timestamp, erlang:system_time(millisecond) - Ms}}
    end;
parse_offset_arg({_, V}) ->
    {ok, V};
parse_offset_arg(V) ->
    {error, {invalid_offset_arg, V}}.

filter_spec(Args) ->
    Filters = case lists:keysearch(<<"x-stream-filter">>, 1, Args) of
                  {value, {_, array, FilterValues}} ->
                      lists:foldl(fun({longstr, V}, Acc) ->
                                          [V] ++ Acc;
                                     (_, Acc) ->
                                          Acc
                                  end, [], FilterValues);
                  {value, {_, longstr, FilterValue}} ->
                      [FilterValue];
                  _ ->
                      undefined
              end,
    MatchUnfiltered = case lists:keysearch(<<"x-stream-match-unfiltered">>, 1, Args) of
                          {value, {_, bool, Match}} when is_list(Filters) ->
                              Match;
                          _ when is_list(Filters) ->
                              false;
                          _ ->
                              undefined
                      end,
    case MatchUnfiltered of
        undefined ->
            #{};
        MU ->
            #{filter_spec =>
              #{filters => Filters, match_unfiltered => MU}}
    end.

get_local_pid(#stream_client{local_pid = Pid} = State)
  when is_pid(Pid) ->
    case erlang:is_process_alive(Pid) of
        true ->
            {Pid, State};
        false ->
            query_local_pid(State)
    end;
get_local_pid(#stream_client{leader = Pid} = State)
  when is_pid(Pid) andalso node(Pid) == node() ->
    get_local_pid(State#stream_client{local_pid = Pid});
get_local_pid(#stream_client{} = State) ->
    %% query local coordinator to get pid
    query_local_pid(State).

query_local_pid(#stream_client{stream_id = StreamId} = State) ->
    case rabbit_stream_coordinator:local_pid(StreamId) of
        {ok, Pid} ->
            {Pid, State#stream_client{local_pid = Pid}};
        {error, not_found} ->
            {undefined, State}
    end.

begin_stream(#stream_client{name = QName,
                            readers = Readers0,
                            local_pid = LocalPid} = State,
             Tag, Offset, Mode, AckRequired, Filter, Options)
  when is_pid(LocalPid) ->
    CounterSpec = {{?MODULE, QName, Tag, self()}, []},
    {ok, Seg0} = osiris:init_reader(LocalPid, Offset, CounterSpec, Options),
    NextOffset = osiris_log:next_offset(Seg0) - 1,
    osiris:register_offset_listener(LocalPid, NextOffset),
    StartOffset = case Offset of
                      first -> NextOffset;
                      last -> NextOffset;
                      next -> NextOffset;
                      {timestamp, _} -> NextOffset;
                      _ -> Offset
                  end,
    {DeliveryCount, Credit} = case Mode of
                                  {simple_prefetch, N} ->
                                      {none, N};
                                  {credited, InitialDC} ->
                                      {InitialDC, 0}
                              end,
    Str0 = #stream{mode = Mode,
                   delivery_count = DeliveryCount,
                   credit = Credit,
                   ack = AckRequired,
                   start_offset = StartOffset,
                   listening_offset = NextOffset,
                   last_consumed_offset = StartOffset,
                   log = Seg0,
                   filter = Filter,
                   reader_options = Options},
    {ok, State#stream_client{readers = Readers0#{Tag => Str0}}}.

cancel(_Q, #{consumer_tag := ConsumerTag,
             user := ActingUser} = Spec,
       #stream_client{readers = Readers0,
                      name = QName} = State) ->
    case maps:take(ConsumerTag, Readers0) of
        {#stream{log = Log}, Readers} ->
            ok = close_log(Log),
            rabbit_core_metrics:consumer_deleted(self(), ConsumerTag, QName),
            rabbit_event:notify(consumer_deleted,
                                [{consumer_tag, ConsumerTag},
                                 {channel, self()},
                                 {queue, QName},
                                 {user_who_performed_action, ActingUser}]),
            maybe_send_reply(self(), maps:get(ok_msg, Spec, undefined)),
            {ok, State#stream_client{readers = Readers}};
        error ->
            {ok, State}
    end.

-dialyzer({nowarn_function, credit_v1/5}).
credit_v1(_, _, _, _, _) ->
    erlang:error(credit_v1_unsupported).

credit(QName, CTag, DeliveryCountRcv, LinkCreditRcv, Drain,
       #stream_client{readers = Readers,
                      name = Name,
                      local_pid = LocalPid} = State0) ->
    case Readers of
        #{CTag := Str0 = #stream{delivery_count = DeliveryCountSnd}} ->
            LinkCreditSnd = amqp10_util:link_credit_snd(
                              DeliveryCountRcv, LinkCreditRcv, DeliveryCountSnd),
            Str1 = Str0#stream{credit = LinkCreditSnd},
            {Str2 = #stream{delivery_count = DeliveryCount,
                            credit = Credit,
                            ack = Ack}, Msgs} = stream_entries(QName, Name, LocalPid, Str1),
            Str = case Drain andalso Credit > 0 of
                      true ->
                          Str2#stream{delivery_count = serial_number:add(DeliveryCount, Credit),
                                      credit = 0};
                      false ->
                          Str2
                  end,
            State = State0#stream_client{readers = maps:update(CTag, Str, Readers)},
            Actions = deliver_actions(CTag, Ack, Msgs) ++ [{credit_reply,
                                                            CTag,
                                                            Str#stream.delivery_count,
                                                            Str#stream.credit,
                                                            available_messages(Str),
                                                            Drain}],
            {State, Actions};
        _ ->
            {State0, []}
    end.

%% Returns only an approximation.
available_messages(#stream{log = Log,
                           last_consumed_offset = LastConsumedOffset}) ->
    max(0, osiris_log:committed_offset(Log) - LastConsumedOffset).

deliver(QSs, Msg, Options) ->
    lists:foldl(
      fun({Q, stateless}, {Qs, Actions}) ->
              LeaderPid = amqqueue:get_pid(Q),
              ok = osiris:write(LeaderPid, stream_message(Msg)),
              {Qs, Actions};
         ({Q, S0}, {Qs, Actions0}) ->
              {S, Actions} = deliver0(maps:get(correlation, Options, undefined),
                                      Msg, S0, Actions0),
              {[{Q, S} | Qs], Actions}
      end, {[], []}, QSs).

deliver0(Corr, Msg,
         #stream_client{name = Name,
                        leader = LeaderPid,
                        writer_id = WriterId,
                        next_seq = Seq,
                        correlation = Correlation0,
                        soft_limit = SftLmt,
                        slow = Slow0} = State,
         Actions0) ->
    ok = osiris:write(LeaderPid, WriterId, Seq, stream_message(Msg)),
    Correlation = case Corr of
                      undefined ->
                          Correlation0;
                      _ ->
                          Correlation0#{Seq => {Corr, Msg}}
                  end,
    {Slow, Actions} = case maps:size(Correlation) >= SftLmt of
                          true when not Slow0 ->
                              {true, [{block, Name} | Actions0]};
                          Bool ->
                              {Bool, Actions0}
                      end,
    {State#stream_client{next_seq = Seq + 1,
                         correlation = Correlation,
                         slow = Slow}, Actions}.

stream_message(Msg) ->
    McAmqp = mc:convert(mc_amqp, Msg),
    MsgData = mc:protocol_state(McAmqp),
    case mc:x_header(<<"x-stream-filter-value">>, McAmqp) of
        undefined ->
            MsgData;
        {utf8, Value} ->
            {Value, MsgData}
    end.

-spec dequeue(_, _, _, _, client()) -> no_return().
dequeue(_, _, _, _, #stream_client{name = Name}) ->
    {protocol_error, not_implemented, "basic.get not supported by stream queues ~ts",
     [rabbit_misc:rs(Name)]}.

handle_event(_QName, {osiris_written, From, _WriterId, Corrs},
             State0 = #stream_client{correlation = Correlation0,
                                     soft_limit = SftLmt,
                                     slow = Slow0,
                                     name = Name}) ->
    MsgIds = lists:sort(maps:fold(
                          fun (_Seq, {I, _M}, Acc) ->
                                  [I | Acc]
                          end, [], maps:with(Corrs, Correlation0))),

    Correlation = maps:without(Corrs, Correlation0),
    {Slow, Actions0} = case maps:size(Correlation) < SftLmt of
                           true when Slow0 ->
                               {false, [{unblock, Name}]};
                           _ ->
                               {Slow0, []}
                       end,
    Actions = case MsgIds of
                  [] -> Actions0;
                  [_|_] -> [{settled, From, MsgIds} | Actions0]
              end,
    State = State0#stream_client{correlation = Correlation,
                                 slow = Slow},
    {ok, State, Actions};
handle_event(QName, {osiris_offset, _From, _Offs},
             State = #stream_client{local_pid = LocalPid,
                                    readers = Readers0,
                                    name = Name}) ->
    %% offset isn't actually needed as we use the atomic to read the
    %% current committed
    {Readers, Actions} = maps:fold(
                           fun (Tag, Str0, {Rds, As}) ->
                                   {Str, Msgs} = stream_entries(QName, Name, LocalPid, Str0),
                                   {Rds#{Tag => Str}, deliver_actions(Tag, Str#stream.ack, Msgs) ++ As}
                           end, {#{}, []}, Readers0),
    {ok, State#stream_client{readers = Readers}, Actions};
handle_event(_QName, {stream_leader_change, Pid}, State) ->
    {ok, update_leader_pid(Pid, State), []};
handle_event(_QName, {stream_local_member_change, Pid},
             #stream_client{local_pid = P} = State)
  when P == Pid ->
    {ok, State, []};
handle_event(_QName, {stream_local_member_change, Pid},
             #stream_client{name = QName,
                            readers = Readers0} = State) ->
    rabbit_log:debug("Local member change event for ~tp", [QName]),
    Readers1 = maps:fold(fun(T, #stream{log = Log0, reader_options = Options} = S0, Acc) ->
                                 Offset = osiris_log:next_offset(Log0),
                                 osiris_log:close(Log0),
                                 CounterSpec = {{?MODULE, QName, self()}, []},
                                 rabbit_log:debug("Re-creating Osiris reader for consumer ~tp at offset ~tp "
                                                  " with options ~tp",
                                                  [T, Offset, Options]),
                                 {ok, Log1} = osiris:init_reader(Pid, Offset, CounterSpec, Options),
                                 NextOffset = osiris_log:next_offset(Log1) - 1,
                                 rabbit_log:debug("Registering offset listener at offset ~tp", [NextOffset]),
                                 osiris:register_offset_listener(Pid, NextOffset),
                                 S1 = S0#stream{listening_offset = NextOffset,
                                                log = Log1},
                                 Acc#{T => S1}

                         end, #{}, Readers0),
    {ok, State#stream_client{local_pid = Pid, readers = Readers1}, []};
handle_event(_QName, eol, #stream_client{name = Name}) ->
    {eol, [{unblock, Name}]};
handle_event(QName, deleted_replica, State) ->
    {ok, State, [{queue_down, QName}]}.

is_recoverable(Q) ->
    Node = node(),
    #{replica_nodes := Nodes,
      leader_node := Leader} = amqqueue:get_type_state(Q),
    lists:member(Node, Nodes ++ [Leader]).

recover(_VHost, Queues) ->
    lists:foldl(
      fun (Q0, {R0, F0}) ->
              {ok, Q} = recover(Q0),
              {[Q | R0], F0}
      end, {[], []}, Queues).

settle(QName, _, CTag, MsgIds, #stream_client{readers = Readers0,
                                              local_pid = LocalPid,
                                              name = Name} = State) ->
    case Readers0 of
        #{CTag := #stream{mode = {simple_prefetch, _MaxCredit},
                          ack = Ack,
                          credit = Credit0} = Str0} ->
            %% all settle reasons will "give credit" to the stream queue
            Credit = length(MsgIds),
            Str1 = Str0#stream{credit = Credit0 + Credit},
            {Str, Msgs} = stream_entries(QName, Name, LocalPid, Str1),
            Readers = maps:update(CTag, Str, Readers0),
            {State#stream_client{readers = Readers},
             deliver_actions(CTag, Ack, Msgs)};
        _ ->
            {State, []}
    end.

info(Q, all_keys) ->
    info(Q, ?INFO_KEYS);
info(Q, Items) ->
    lists:foldr(fun(Item, Acc) ->
                        [{Item, i(Item, Q)} | Acc]
                end, [], Items).

i(name,        Q) when ?is_amqqueue(Q) -> amqqueue:get_name(Q);
i(durable,     Q) when ?is_amqqueue(Q) -> amqqueue:is_durable(Q);
i(auto_delete, Q) when ?is_amqqueue(Q) -> amqqueue:is_auto_delete(Q);
i(arguments,   Q) when ?is_amqqueue(Q) -> amqqueue:get_arguments(Q);
i(leader, Q) when ?is_amqqueue(Q) ->
    case amqqueue:get_pid(Q) of
        none ->
            undefined;
        Pid -> node(Pid)
    end;
i(members, Q) when ?is_amqqueue(Q) ->
    #{nodes := Nodes} = amqqueue:get_type_state(Q),
    Nodes;
i(consumers, Q) when ?is_amqqueue(Q) ->
    QName = amqqueue:get_name(Q),
    #{nodes := Nodes} = amqqueue:get_type_state(Q),
    Spec = [{{{'$1', '_', '_'}, '_', '_', '_', '_', '_', '_'}, [{'==', {QName}, '$1'}], [true]}],
    lists:foldl(fun(N, Acc) ->
                        case rabbit_misc:rpc_call(N,
                                                  ets,
                                                  select_count,
                                                  [consumer_created, Spec],
                                                  10000) of
                            Count when is_integer(Count) ->
                                Acc + Count;
                            _ ->
                                Acc
                        end
                end, 0, Nodes);
i(memory, Q) when ?is_amqqueue(Q) ->
    %% Return writer memory. It's not the full memory usage (we also have replica readers on
    %% the writer node), but might be good enough
    case amqqueue:get_pid(Q) of
        none ->
            0;
        Pid ->
            try
                {memory, M} = process_info(Pid, memory),
                M
            catch
                error:badarg ->
                    0
            end
    end;
i(online, Q) ->
    #{name := StreamId} = amqqueue:get_type_state(Q),
    case rabbit_stream_coordinator:members(StreamId) of
        {ok, Members} ->
            maps:fold(fun(_, {undefined, _}, Acc) ->
                              Acc;
                         (Key, _, Acc) ->
                              [Key | Acc]
                      end, [], Members);
        {error, not_found} ->
            []
    end;
i(state, Q) when ?is_amqqueue(Q) ->
    %% TODO the coordinator should answer this, I guess??
    running;
i(messages, Q) when ?is_amqqueue(Q) ->
    QName = amqqueue:get_name(Q),
    case ets:lookup(queue_coarse_metrics, QName) of
        [{_, _, _, M, _}] ->
            M;
        [] ->
            0
    end;
i(messages_ready, Q) when ?is_amqqueue(Q) ->
    QName = amqqueue:get_name(Q),
    case ets:lookup(queue_coarse_metrics, QName) of
        [{_, MR, _, _, _}] ->
            MR;
        [] ->
            0
    end;
i(messages_unacknowledged, Q) when ?is_amqqueue(Q) ->
    QName = amqqueue:get_name(Q),
    case ets:lookup(queue_coarse_metrics, QName) of
        [{_, _, MU, _, _}] ->
            MU;
        [] ->
            0
    end;
i(committed_offset, Q) ->
    %% TODO should it be on a metrics table?
    %% The queue could be removed between the list() and this call
    %% to retrieve the overview. Let's default to '' if it's gone.
    Key = {osiris_writer, amqqueue:get_name(Q)},
    case osiris_counters:overview(Key) of
        undefined ->
            '';
        Data ->
            maps:get(committed_offset, Data, '')
    end;
i(segments, Q) ->
    Key = {osiris_writer, amqqueue:get_name(Q)},
    case osiris_counters:overview(Key) of
        undefined ->
            '';
        Data ->
            maps:get(segments, Data, '')
    end;
i(policy, Q) ->
    case rabbit_policy:name(Q) of
        none   -> '';
        Policy -> Policy
    end;
i(operator_policy, Q) ->
    case rabbit_policy:name_op(Q) of
        none   -> '';
        Policy -> Policy
    end;
i(effective_policy_definition, Q) ->
    case rabbit_policy:effective_definition(Q) of
        undefined -> [];
        Def       -> Def
    end;
i(readers, Q) ->
    QName = amqqueue:get_name(Q),
    Conf = amqqueue:get_type_state(Q),
    Nodes = [maps:get(leader_node, Conf) | maps:get(replica_nodes, Conf)],
    {Data, _} = rpc:multicall(Nodes, ?MODULE, readers, [QName]),
    lists:flatten(Data);
i(type, _) ->
    stream;
i(_, _) ->
    ''.

queue_length(Q) ->
    i(messages, Q).

get_replicas(Q) ->
    i(members, Q).

-spec transfer_leadership(amqqueue:amqqueue(), node()) -> {migrated, node()} | {not_migrated, atom()}.
transfer_leadership(Q, Destination) ->
    case rabbit_stream_coordinator:restart_stream(Q, #{preferred_leader_node => Destination}) of
        {ok, NewNode} -> {migrated, NewNode};
        {error, coordinator_unavailable} -> {not_migrated, timeout};
        {error, Reason} -> {not_migrated, Reason}
    end.

-spec status(rabbit_types:vhost(), Name :: rabbit_misc:resource_name()) ->
    [[{binary(), term()}]] | {error, term()}.
status(Vhost, QueueName) ->
    %% Handle not found queues
    QName = #resource{virtual_host = Vhost, name = QueueName, kind = queue},
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} when ?amqqueue_is_classic(Q) ->
            {error, classic_queue_not_supported};
        {ok, Q} when ?amqqueue_is_quorum(Q) ->
            {error, quorum_queue_not_supported};
        {ok, Q} when ?amqqueue_is_stream(Q) ->
            [begin
                 [get_key(role, C),
                  get_key(node, C),
                  get_key(epoch, C),
                  get_key(offset, C),
                  get_key(committed_offset, C),
                  get_key(first_offset, C),
                  get_key(readers, C),
                  get_key(segments, C)]
             end || C <- get_counters(Q)];
        {ok, _Q} ->
            {error, not_supported};
        {error, not_found} = E ->
            E
    end.

get_key(Key, Cnt) ->
    {Key, maps:get(Key, Cnt, undefined)}.

-spec get_role({pid() | undefined, writer | replica}) -> writer | replica.
get_role({_, Role}) -> Role.

get_counters(Q) ->
    #{name := StreamId} = amqqueue:get_type_state(Q),
    {ok, Members} = rabbit_stream_coordinator:members(StreamId),
    %% split members to query the writer last
    %% this minimizes the risk of confusing output where replicas are ahead of the writer
    NodeRoles = [{Node, get_role(M)} || {Node, M} <- maps:to_list(Members)],
    {Writer, Replicas} = lists:partition(fun({_, Role}) -> Role =:= writer end, NodeRoles),
    QName = amqqueue:get_name(Q),
    Counters = [safe_get_overview(Node, QName, Role)
                || {Node, Role} <- lists:append(Replicas, Writer)],
    %% sort again in the original order (by node)
    lists:sort(fun (M1, M2) -> maps:get(node, M1) < maps:get(node, M2) end, Counters).

-spec safe_get_overview(node(), rabbit_amqqueue:name(), writer | reader) ->
          map().
safe_get_overview(Node, QName, Role) ->
    case rpc:call(Node, ?MODULE, get_overview, [QName]) of
        {badrpc, _} ->
            #{role => Role,
              node => Node};
        undefined ->
            #{role => Role,
              node => Node};
        {Role, C} ->
            C#{role => Role}
    end.

-spec get_overview(rabbit_amqqueue:name()) ->
          {writer | reader, map()} | undefined.
get_overview(QName) ->
    case osiris_counters:overview({osiris_writer, QName}) of
        undefined ->
            case osiris_counters:overview({osiris_replica, QName}) of
                undefined ->
                    undefined;
                M ->
                    {replica, M#{node => node()}}
            end;
        M ->
            {writer, M#{node => node()}}
    end.

-spec tracking_status(rabbit_types:vhost(), Name :: rabbit_misc:resource_name()) ->
    [[{atom(), term()}]] | {error, term()}.
tracking_status(Vhost, QueueName) ->
    %% Handle not found queues
    QName = #resource{virtual_host = Vhost, name = QueueName, kind = queue},
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} when ?amqqueue_is_classic(Q) ->
            {error, classic_queue_not_supported};
        {ok, Q} when ?amqqueue_is_quorum(Q) ->
            {error, quorum_queue_not_supported};
        {ok, Q} when ?amqqueue_is_stream(Q) ->
            Leader = amqqueue:get_pid(Q),
            Map = osiris:read_tracking(Leader),
            maps:fold(fun(Type, Trackings, Acc) ->
                              %% Convert for example 'offsets' to 'offset' or 'sequences' to 'sequence'
                              T = list_to_atom(lists:droplast(atom_to_list(Type))),
                              maps:fold(fun(TrkId, TrkData, Acc0) ->
                                                [[{type, T},
                                                  {reference, TrkId},
                                                  {value, TrkData}] | Acc0]
                                        end, [], Trackings) ++ Acc
                      end, [], Map);
        {ok, Q} ->
            {error, {queue_not_supported, amqqueue:get_type(Q)}};
        {error, not_found} = E->
            E
    end.

readers(QName) ->
    try
        {_Role, Counters} = get_overview(QName),
        Readers = maps:get(readers, Counters, 0),
        {node(), Readers}
    catch
        _:_ ->
            {node(), 0}
    end.

get_writer_pid(Q) ->
    case amqqueue:get_pid(Q) of
        none ->
            %% the stream is still starting; wait up to 5 seconds
            %% and ask the coordinator as it has the Pid sooner
            #{name := StreamId} = amqqueue:get_type_state(Q),
            get_writer_pid(StreamId, 50);
        Pid ->
            Pid
    end.

get_writer_pid(_StreamId, 0) ->
    stream_not_found;
get_writer_pid(StreamId, N) ->
    case rabbit_stream_coordinator:writer_pid(StreamId) of
        {ok, Pid} ->
            Pid;
        _ ->
            timer:sleep(100),
            get_writer_pid(StreamId, N - 1)
    end.


init(Q) when ?is_amqqueue(Q) ->
    Leader = get_writer_pid(Q),
    QName = amqqueue:get_name(Q),
    #{name := StreamId} = amqqueue:get_type_state(Q),
    %% tell us about leader changes so we can fail over
    case rabbit_stream_coordinator:register_listener(Q) of
        {ok, ok, _} ->
            Prefix = erlang:pid_to_list(self()) ++ "_",
            WriterId = rabbit_guid:binary(rabbit_guid:gen(), Prefix),
            {ok, SoftLimit} = application:get_env(rabbit, stream_messages_soft_limit),
            {ok, #stream_client{stream_id = StreamId,
                                name = amqqueue:get_name(Q),
                                leader = Leader,
                                writer_id = WriterId,
                                soft_limit = SoftLimit}};
        {ok, stream_not_found, _} ->
            {error, stream_not_found};
        {error, coordinator_unavailable} = E ->
            rabbit_log:warning("Failed to start stream client ~tp: coordinator unavailable",
                               [rabbit_misc:rs(QName)]),
            E
    end.

close(#stream_client{readers = Readers,
                     name = QName}) ->
    maps:foreach(fun (CTag, #stream{log = Log}) ->
                         close_log(Log),
                         rabbit_core_metrics:consumer_deleted(self(), CTag, QName)
                 end, Readers).

update(Q, State)
  when ?is_amqqueue(Q) ->
    State.

update_leader_pid(Pid, #stream_client{leader = Pid} =  State) ->
    State;
update_leader_pid(Pid, #stream_client{} =  State) ->
    rabbit_log:debug("stream client: new leader detected ~w", [Pid]),
    resend_all(State#stream_client{leader = Pid}).

state_info(_) ->
    #{}.

-spec restart_stream(VHost :: binary(), Queue :: binary(),
                     #{preferred_leader_node => node()}) ->
    {ok, node()} |
    {error, term()}.
restart_stream(VHost, Queue, Options)
  when is_binary(VHost) andalso
       is_binary(Queue) ->
    case rabbit_amqqueue:lookup(Queue, VHost) of
        {ok, Q} when ?amqqueue_type_is(Q, ?MODULE) ->
            rabbit_stream_coordinator:restart_stream(Q, Options);
        {ok, Q} ->
            {error, {not_supported_for_type, amqqueue:get_type(Q)}};
        E ->
            E
    end.


add_replica(VHost, Name, Node) ->
    QName = queue_resource(VHost, Name),
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} when ?amqqueue_is_classic(Q) ->
            {error, classic_queue_not_supported};
        {ok, Q} when ?amqqueue_is_quorum(Q) ->
            {error, quorum_queue_not_supported};
        {ok, Q} when ?amqqueue_is_stream(Q) ->
            case lists:member(Node, rabbit_nodes:list_running()) of
                false ->
                    {error, node_not_running};
                true ->
                    rabbit_stream_coordinator:add_replica(Q, Node)
            end;
        {ok, Q} ->
            {error, {queue_not_supported, amqqueue:get_type(Q)}};
        E ->
            E
    end.

delete_replica(VHost, Name, Node) ->
    QName = queue_resource(VHost, Name),
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} when ?amqqueue_is_classic(Q) ->
            {error, classic_queue_not_supported};
        {ok, Q} when ?amqqueue_is_quorum(Q) ->
            {error, quorum_queue_not_supported};
        {ok, Q} when ?amqqueue_is_stream(Q) ->
            #{name := StreamId} = amqqueue:get_type_state(Q),
            {ok, Reply, _} = rabbit_stream_coordinator:delete_replica(StreamId, Node),
            Reply;
        {ok, Q} ->
            {error, {queue_not_supported, amqqueue:get_type(Q)}};
        E ->
            E
    end.

delete_all_replicas(Node) ->
    rabbit_log:info("Asked to remove all stream replicas from node ~ts", [Node]),
    Streams = rabbit_amqqueue:list_stream_queues_on(Node),
    lists:map(fun(Q) ->
                      QName = amqqueue:get_name(Q),
                      rabbit_log:info("~ts: removing replica on node ~w",
                                      [rabbit_misc:rs(QName), Node]),
                      #{name := StreamId} = amqqueue:get_type_state(Q),
                      {ok, Reply, _} = rabbit_stream_coordinator:delete_replica(StreamId, Node),
                      case Reply of
                          ok ->
                              {QName, ok};
                          Err ->
                              rabbit_log:warning("~ts: failed to remove replica on node ~w, error: ~w",
                                                 [rabbit_misc:rs(QName), Node, Err]),
                              {QName, {error, Err}}
                      end
              end, Streams).

make_stream_conf(Q) ->
    QName = amqqueue:get_name(Q),
    Name = stream_name(QName),
    Formatter = {?MODULE, format_osiris_event, [QName]},
    update_stream_conf(Q, #{reference => QName,
                            name => Name,
                            event_formatter => Formatter,
                            epoch => 1}).

update_stream_conf(undefined, #{} = Conf) ->
    Conf;
update_stream_conf(Q, #{} = Conf) when ?is_amqqueue(Q) ->
    MaxBytes = args_policy_lookup(<<"max-length-bytes">>, fun policy_precedence/2, Q),
    MaxAge = max_age(args_policy_lookup(<<"max-age">>, fun policy_precedence/2, Q)),
    MaxSegmentSizeBytes = args_policy_lookup(<<"stream-max-segment-size-bytes">>,
                                             fun policy_precedence/2, Q),
    FilterSizeBytes = args_policy_lookup(<<"stream-filter-size-bytes">>,
                                         fun policy_precedence/2, Q),
    Retention = lists:filter(fun({_, R}) ->
                                     R =/= undefined
                             end, [{max_bytes, MaxBytes},
                                   {max_age, MaxAge}]),
    add_if_defined(
      filter_size, FilterSizeBytes,
      add_if_defined(max_segment_size_bytes, MaxSegmentSizeBytes,
                     Conf#{retention => Retention})).

add_if_defined(_, undefined, Map) ->
    Map;
add_if_defined(Key, Value, Map) ->
    maps:put(Key, Value, Map).

format_osiris_event(Evt, QRef) ->
    {'$gen_cast', {queue_event, QRef, Evt}}.

max_age(undefined) ->
    undefined;
max_age(Bin) when is_binary(Bin) ->
    rabbit_amqqueue:check_max_age(Bin);
max_age(Age) ->
    Age.

initial_cluster_size(undefined) ->
    length(rabbit_nodes:list_members());
initial_cluster_size(Val) ->
    Val.

policy_precedence(PolVal, _ArgVal) ->
    PolVal.

stream_name(#resource{virtual_host = VHost, name = Name}) ->
    Timestamp = erlang:integer_to_binary(erlang:system_time()),
    osiris_util:to_base64uri(<<VHost/binary, "_", Name/binary, "_", Timestamp/binary>>).

recover(Q) ->
    {ok, Q}.

maybe_send_reply(_ChPid, undefined) -> ok;
maybe_send_reply(ChPid, Msg) -> ok = rabbit_channel:send_command(ChPid, Msg).

stream_entries(QName, Name, LocalPid,
               #stream{chunk_iterator = undefined,
                       credit = Credit} = Str0) ->
    case Credit > 0 of
        true ->
            case chunk_iterator(Str0, LocalPid) of
                {ok, Str} ->
                    stream_entries(QName, Name, LocalPid, Str);
                {end_of_stream, Str} ->
                    {Str, []}
            end;
        false ->
            {Str0, []}
    end;
stream_entries(QName, Name, LocalPid,
               #stream{delivery_count = DC,
                       credit = Credit,
                       buffer_msgs_rev = Buf0,
                       last_consumed_offset = LastOff} = Str0)
  when Credit > 0 andalso Buf0 =/= [] ->
    BufLen = length(Buf0),
    case Credit =< BufLen of
        true ->
            %% Entire credit worth of messages can be served from the buffer.
            {Buf, BufMsgsRev} = lists:split(BufLen - Credit, Buf0),
            {Str0#stream{delivery_count = delivery_count_add(DC, Credit),
                         credit = 0,
                         buffer_msgs_rev = Buf,
                         last_consumed_offset = LastOff + Credit},
             lists:reverse(BufMsgsRev)};
        false ->
            Str = Str0#stream{delivery_count = delivery_count_add(DC, BufLen),
                              credit = Credit - BufLen,
                              buffer_msgs_rev = [],
                              last_consumed_offset = LastOff + BufLen},
            stream_entries(QName, Name, LocalPid, Str, Buf0)
    end;
stream_entries(QName, Name, LocalPid, Str) ->
    stream_entries(QName, Name, LocalPid, Str, []).

stream_entries(_, _, _, #stream{credit = Credit} = Str, Acc)
  when Credit < 1 ->
    {Str, lists:reverse(Acc)};
stream_entries(QName, Name, LocalPid,
               #stream{chunk_iterator = Iter0,
                       delivery_count = DC,
                       credit = Credit,
                       start_offset = StartOffset,
                       filter = Filter} = Str0, Acc0) ->
    case osiris_log:iterator_next(Iter0) of
        end_of_chunk ->
            case chunk_iterator(Str0, LocalPid) of
                {ok, Str} ->
                    stream_entries(QName, Name, LocalPid, Str, Acc0);
                {end_of_stream, Str} ->
                    {Str, lists:reverse(Acc0)}
            end;
        {{Offset, Entry}, Iter} ->
            {Str, Acc} = case Entry of
                             {batch, _NumRecords, 0, _Len, BatchedEntries} ->
                                 {MsgsRev, NumMsgs} = parse_uncompressed_subbatch(
                                                        BatchedEntries, Offset, StartOffset,
                                                        QName, Name, LocalPid, Filter, {[], 0}),
                                 case Credit >= NumMsgs of
                                     true ->
                                         {Str0#stream{chunk_iterator = Iter,
                                                      delivery_count = delivery_count_add(DC, NumMsgs),
                                                      credit = Credit - NumMsgs,
                                                      last_consumed_offset = Offset + NumMsgs - 1},
                                          MsgsRev ++ Acc0};
                                     false ->
                                         %% Consumer doesn't have sufficient credit.
                                         %% Buffer the remaining messages.
                                         [] = Str0#stream.buffer_msgs_rev, % assertion
                                         {Buf, MsgsRev1} = lists:split(NumMsgs - Credit, MsgsRev),
                                         {Str0#stream{chunk_iterator = Iter,
                                                      delivery_count = delivery_count_add(DC, Credit),
                                                      credit = 0,
                                                      buffer_msgs_rev = Buf,
                                                      last_consumed_offset = Offset + Credit - 1},
                                          MsgsRev1 ++ Acc0}
                                 end;
                             {batch, _, _CompressionType, _, _} ->
                                 %% Skip compressed sub batch.
                                 %% It can only be consumed by Stream protocol clients.
                                 {Str0#stream{chunk_iterator = Iter}, Acc0};
                             _SimpleEntry ->
                                 case Offset >= StartOffset of
                                     true ->
                                         case entry_to_msg(Entry, Offset, QName,
                                                           Name, LocalPid, Filter) of
                                             none ->
                                                 {Str0#stream{chunk_iterator = Iter,
                                                              last_consumed_offset = Offset},
                                                  Acc0};
                                             Msg ->
                                                 {Str0#stream{chunk_iterator = Iter,
                                                              delivery_count = delivery_count_add(DC, 1),
                                                              credit = Credit - 1,
                                                              last_consumed_offset = Offset},
                                                  [Msg | Acc0]}
                                         end;
                                     false ->
                                         {Str0#stream{chunk_iterator = Iter}, Acc0}
                                 end
                         end,
            stream_entries(QName, Name, LocalPid, Str, Acc)
    end.

chunk_iterator(#stream{credit = Credit,
                       listening_offset = LOffs,
                       log = Log0} = Str0, LocalPid) ->
    case osiris_log:chunk_iterator(Log0, Credit) of
        {ok, _ChunkHeader, Iter, Log} ->
            {ok, Str0#stream{chunk_iterator = Iter,
                             log = Log}};
        {end_of_stream, Log} ->
            NextOffset = osiris_log:next_offset(Log),
            Str = case NextOffset > LOffs of
                      true ->
                          osiris:register_offset_listener(LocalPid, NextOffset),
                          Str0#stream{log = Log,
                                      listening_offset = NextOffset};
                      false ->
                          Str0#stream{log = Log}
                  end,
            {end_of_stream, Str};
        {error, Err} ->
            rabbit_log:info("stream client: failed to create chunk iterator ~p", [Err]),
            exit(Err)
    end.

%% Deliver each record of an uncompressed sub batch individually.
parse_uncompressed_subbatch(
  <<>>, _Offset, _StartOffset, _QName, _Name, _LocalPid, _Filter, Acc) ->
    Acc;
parse_uncompressed_subbatch(
  <<0:1, %% simple entry
    Len:31/unsigned,
    Entry:Len/binary,
    Rem/binary>>,
  Offset, StartOffset, QName, Name, LocalPid, Filter, Acc0 = {AccList, AccCount}) ->
    Acc = case Offset >= StartOffset of
              true ->
                  case entry_to_msg(Entry, Offset, QName, Name, LocalPid, Filter) of
                      none ->
                          Acc0;
                      Msg ->
                          {[Msg | AccList], AccCount + 1}
                  end;
              false ->
                  Acc0
          end,
    parse_uncompressed_subbatch(Rem, Offset + 1, StartOffset, QName,
                                Name, LocalPid, Filter, Acc).

entry_to_msg(Entry, Offset, #resource{kind = queue, name = QName},
             Name, LocalPid, Filter) ->
    Mc = mc_amqp:init_from_stream(Entry, #{?ANN_EXCHANGE => <<>>,
                                           ?ANN_ROUTING_KEYS => [QName],
                                           <<"x-stream-offset">> => Offset}),
    case rabbit_amqp_filter:eval(Filter, Mc) of
        true ->
            {Name, LocalPid, Offset, false, Mc};
        false ->
            none
    end.

capabilities() ->
    #{unsupported_policies => [%% Classic policies
                               <<"expires">>, <<"message-ttl">>, <<"dead-letter-exchange">>,
                               <<"dead-letter-routing-key">>, <<"max-length">>,
                               <<"max-in-memory-length">>, <<"max-in-memory-bytes">>,
                               <<"max-priority">>, <<"overflow">>, <<"queue-mode">>,
                               <<"delivery-limit">>,
                               <<"ha-mode">>, <<"ha-params">>, <<"ha-sync-mode">>,
                               <<"ha-promote-on-shutdown">>, <<"ha-promote-on-failure">>,
                               <<"queue-master-locator">>,
                               %% Quorum policies
                               <<"dead-letter-strategy">>, <<"target-group-size">>],
      queue_arguments => [<<"x-max-length-bytes">>, <<"x-queue-type">>,
                          <<"x-max-age">>, <<"x-stream-max-segment-size-bytes">>,
                          <<"x-initial-cluster-size">>, <<"x-queue-leader-locator">>],
      consumer_arguments => [<<"x-stream-offset">>,
                             <<"x-stream-filter">>,
                             <<"x-stream-match-unfiltered">>],
      %% AMQP property filter expressions
      %% https://groups.oasis-open.org/higherlogic/ws/public/document?document_id=66227
      amqp_capabilities => [<<"AMQP_FILTEX_PROP_V1_0">>],
      server_named => false,
      rebalance_module => ?MODULE,
      can_redeliver => true,
      is_replicable => true
     }.

notify_decorators(Q) when ?is_amqqueue(Q) ->
    %% Not supported
    ok.

resend_all(#stream_client{leader = LeaderPid,
                          writer_id = WriterId,
                          correlation = Corrs} = State) ->
    Msgs = lists:sort(maps:to_list(Corrs)),
    case Msgs of
        [] -> ok;
        [{Seq, _} | _] ->
            rabbit_log:debug("stream client: resending from seq ~w num ~b",
                             [Seq, maps:size(Corrs)])
    end,
    [begin
         ok = osiris:write(LeaderPid, WriterId, Seq, stream_message(Msg))
     end || {Seq, {_Corr, Msg}} <- Msgs],
    State.

-spec set_leader_pid(Pid, QName) -> Ret when
      Pid :: pid(),
      QName :: rabbit_amqqueue:name(),
      Ret :: ok | {error, timeout}.

set_leader_pid(Pid, QName) ->
    %% TODO this should probably be a single khepri transaction for better performance.
    Fun = fun (Q) ->
                  amqqueue:set_pid(Q, Pid)
          end,
    case rabbit_amqqueue:update(QName, Fun) of
        not_found ->
            %% This can happen during recovery
            {ok, Q} = rabbit_amqqueue:lookup_durable_queue(QName),
            rabbit_amqqueue:ensure_rabbit_queue_record_is_initialized(Fun(Q));
        _ ->
            ok
    end.

close_log(undefined) -> ok;
close_log(Log) ->
    osiris_log:close(Log).

%% this is best effort only; the state of replicas is slightly stale and things can happen
%% between the time this function is called and the time decision based on its result is made
-spec list_with_minimum_quorum() -> [amqqueue:amqqueue()].
list_with_minimum_quorum() ->
    lists:filter(fun (Q) ->
                         StreamId = maps:get(name, amqqueue:get_type_state(Q)),
                         {ok, Members} = rabbit_stream_coordinator:members(StreamId),
                         RunningMembers = maps:filter(fun(_, {State, _}) -> State =/= undefined end, Members),
                         map_size(RunningMembers) =< map_size(Members) div 2 + 1
                 end, rabbit_amqqueue:list_local_stream_queues()).

is_stateful() -> true.

get_nodes(Q) when ?is_amqqueue(Q) ->
    #{nodes := Nodes} = amqqueue:get_type_state(Q),
    Nodes.

is_minority(All, Up) ->
    MinQuorum = length(All) div 2 + 1,
    length(Up) < MinQuorum.

deliver_actions(_, _, []) ->
    [];
deliver_actions(CTag, Ack, Msgs) ->
    [{deliver, CTag, Ack, Msgs}].

delivery_count_add(none, _) ->
    none;
delivery_count_add(Count, N) ->
    serial_number:add(Count, N).

policy_apply_to_name() ->
    <<"streams">>.

stop(_VHost) ->
    ok.

drain(TransferCandidates) ->
    case whereis(rabbit_stream_coordinator) of
        undefined -> ok;
        _Pid -> transfer_leadership_of_stream_coordinator(TransferCandidates)
    end.

revive() ->
    ok.

-spec transfer_leadership_of_stream_coordinator([node()]) -> ok.
transfer_leadership_of_stream_coordinator([]) ->
    rabbit_log:warning("Skipping leadership transfer of stream coordinator: no candidate "
                       "(online, not under maintenance) nodes to transfer to!");
transfer_leadership_of_stream_coordinator(TransferCandidates) ->
    % try to transfer to the node with the lowest uptime; the assumption is that
    % nodes are usually restarted in a rolling fashion, in a consistent order;
    % therefore, the youngest node has already been restarted  or (if we are draining the first node)
    % that it will be restarted last. either way, this way we limit the number of transfers
    Uptimes = rabbit_misc:append_rpc_all_nodes(TransferCandidates, erlang, statistics, [wall_clock]),
    Candidates = lists:zipwith(fun(N, {U, _}) -> {N, U}  end, TransferCandidates, Uptimes),
    BestCandidate = element(1, hd(lists:keysort(2, Candidates))),
    case rabbit_stream_coordinator:transfer_leadership([BestCandidate]) of
        {ok, Node} ->
            rabbit_log:info("Leadership transfer for stream coordinator completed. The new leader is ~p", [Node]);
        Error ->
            rabbit_log:warning("Skipping leadership transfer of stream coordinator: ~p", [Error])
    end.

queue_vm_stats_sups() ->
    {[stream_queue_procs,
      stream_queue_replica_reader_procs,
      stream_queue_coordinator_procs],
     [[osiris_server_sup],
      [osiris_replica_reader_sup],
      [rabbit_stream_coordinator]]}.

queue_vm_ets() ->
    {[],
     []}.
