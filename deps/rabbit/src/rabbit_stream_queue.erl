%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stream_queue).

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
         cancel/5,
         handle_event/3,
         deliver/2,
         settle/5,
         credit/5,
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
         capabilities/0,
         notify_decorators/1,
         is_stateful/0]).

-export([list_with_minimum_quorum/0]).

-export([set_retention_policy/3]).
-export([restart_stream/3,
         add_replica/3,
         delete_replica/3]).
-export([format_osiris_event/2]).
-export([update_stream_conf/2]).
-export([readers/1]).

-export([parse_offset_arg/1]).

-export([status/2,
         tracking_status/2,
         get_overview/1]).

-export([check_max_segment_size_bytes/1]).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("amqqueue.hrl").

-define(INFO_KEYS, [name, durable, auto_delete, arguments, leader, members, online, state,
                    messages, messages_ready, messages_unacknowledged, committed_offset,
                    policy, operator_policy, effective_policy_definition, type, memory,
                    consumers]).

-type appender_seq() :: non_neg_integer().

-type msg_id() :: non_neg_integer().
-type msg() :: term(). %% TODO: refine

-record(stream, {credit :: integer(),
                 max :: non_neg_integer(),
                 start_offset = 0 :: non_neg_integer(),
                 listening_offset = 0 :: non_neg_integer(),
                 log :: undefined | osiris_log:state()}).

-record(stream_client, {stream_id :: string(),
                        name :: term(),
                        leader :: pid(),
                        local_pid :: undefined | pid(),
                        next_seq = 1 :: non_neg_integer(),
                        correlation = #{} :: #{appender_seq() => {msg_id(), msg()}},
                        soft_limit :: non_neg_integer(),
                        slow = false :: boolean(),
                        readers = #{} :: #{term() => #stream{}},
                        writer_id :: binary()
                       }).

-import(rabbit_queue_type_util, [args_policy_lookup/3]).

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
declare(Q0, _Node) when ?amqqueue_is_stream(Q0) ->
    case rabbit_queue_type_util:run_checks(
           [fun rabbit_queue_type_util:check_auto_delete/1,
            fun rabbit_queue_type_util:check_exclusive/1,
            fun rabbit_queue_type_util:check_non_durable/1,
            fun rabbit_stream_queue:check_max_segment_size_bytes/1],
           Q0) of
        ok ->
            create_stream(Q0);
        Err ->
            Err
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
                    set_leader_pid(LeaderPid, amqqueue:get_name(Q)),
                    rabbit_event:notify(queue_created,
                                        [{name, QName},
                                         {durable, true},
                                         {auto_delete, false},
                                         {arguments, Arguments},
                                         {type, amqqueue:get_type(Q1)},
                                         {user_who_performed_action,
                                          ActingUser}]),
                    {new, Q};
                Error ->
                    _ = rabbit_amqqueue:internal_delete(Q, ActingUser),
                    {protocol_error, internal_error, "Cannot declare a queue '~ts' on node '~ts': ~255p",
                     [rabbit_misc:rs(QName), node(), Error]}
            end;
        {existing, Q} ->
            {existing, Q};
        {absent, Q, Reason} ->
            {absent, Q, Reason}
    end.

-spec delete(amqqueue:amqqueue(), boolean(),
             boolean(), rabbit_types:username()) ->
    rabbit_types:ok(non_neg_integer()) |
    rabbit_types:error(in_use | not_empty).
delete(Q, _IfUnused, _IfEmpty, ActingUser) ->
    case rabbit_stream_coordinator:delete_stream(Q, ActingUser) of
        {ok, Reply} ->
            Reply;
        Error ->
            {protocol_error, internal_error, "Cannot delete queue '~ts' on node '~ts': ~255p ",
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

consume(Q, #{prefetch_count := 0}, _)
  when ?amqqueue_is_stream(Q) ->
    {protocol_error, precondition_failed, "consumer prefetch count is not set for '~ts'",
     [rabbit_misc:rs(amqqueue:get_name(Q))]};
consume(Q, #{no_ack := true}, _)
  when ?amqqueue_is_stream(Q) ->
    {protocol_error, not_implemented,
     "automatic acknowledgement not supported by stream queues ~ts",
     [rabbit_misc:rs(amqqueue:get_name(Q))]};
consume(Q, #{limiter_active := true}, _State)
  when ?amqqueue_is_stream(Q) ->
    {error, global_qos_not_supported_for_queue_type};
consume(Q, Spec, QState0) when ?amqqueue_is_stream(Q) ->
    %% Messages should include the offset as a custom header.
    case check_queue_exists_in_local_node(Q) of
        ok ->
            #{no_ack := NoAck,
              channel_pid := ChPid,
              prefetch_count := ConsumerPrefetchCount,
              consumer_tag := ConsumerTag,
              exclusive_consume := ExclusiveConsume,
              args := Args,
              ok_msg := OkMsg} = Spec,
            QName = amqqueue:get_name(Q),
            case parse_offset_arg(rabbit_misc:table_lookup(Args, <<"x-stream-offset">>)) of
                {error, _} = Err ->
                    Err;
                {ok, OffsetSpec} ->
                    _ = rabbit_stream_coordinator:register_local_member_listener(Q),
                    rabbit_core_metrics:consumer_created(ChPid, ConsumerTag, ExclusiveConsume,
                                                         not NoAck, QName,
                                                         ConsumerPrefetchCount, false,
                                                         up, Args),
                    %% FIXME: reply needs to be sent before the stream begins sending
                    %% really it should be sent by the stream queue process like classic queues
                    %% do
                    maybe_send_reply(ChPid, OkMsg),
                    begin_stream(QState0, ConsumerTag, OffsetSpec, ConsumerPrefetchCount)
            end;
        Err ->
            Err
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

get_local_pid(#stream_client{local_pid = Pid} = State)
  when is_pid(Pid) ->
    {Pid, State};
get_local_pid(#stream_client{leader = Pid} = State)
  when is_pid(Pid) andalso node(Pid) == node() ->
    {Pid, State#stream_client{local_pid = Pid}};
get_local_pid(#stream_client{stream_id = StreamId,
                             local_pid = undefined} = State) ->
    %% query local coordinator to get pid
    case rabbit_stream_coordinator:local_pid(StreamId) of
        {ok, Pid} ->
            {Pid, State#stream_client{local_pid = Pid}};
        {error, not_found} ->
            {undefined, State}
    end.

begin_stream(#stream_client{name = QName, readers = Readers0} = State0,
             Tag, Offset, Max) ->
    {LocalPid, State} = get_local_pid(State0),
    case LocalPid of
        undefined ->
            {error, no_local_stream_replica_available};
        _ ->
            CounterSpec = {{?MODULE, QName, Tag, self()}, []},
            {ok, Seg0} = osiris:init_reader(LocalPid, Offset, CounterSpec),
            NextOffset = osiris_log:next_offset(Seg0) - 1,
            osiris:register_offset_listener(LocalPid, NextOffset),
            %% TODO: avoid double calls to the same process
            StartOffset = case Offset of
                              first -> NextOffset;
                              last -> NextOffset;
                              next -> NextOffset;
                              {timestamp, _} -> NextOffset;
                              _ -> Offset
                          end,
            Str0 = #stream{credit = Max,
                           start_offset = StartOffset,
                           listening_offset = NextOffset,
                           log = Seg0,
                           max = Max},
            {ok, State#stream_client{local_pid = LocalPid,
                                     readers = Readers0#{Tag => Str0}}}
    end.

cancel(_Q, ConsumerTag, OkMsg, ActingUser, #stream_client{readers = Readers0,
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
            maybe_send_reply(self(), OkMsg),
            {ok, State#stream_client{readers = Readers}};
        error ->
            {ok, State}
    end.

credit(QName, CTag, Credit, Drain, #stream_client{readers = Readers0,
                                           name = Name,
                                           local_pid = LocalPid} = State) ->
    {Readers1, Msgs} = case Readers0 of
                          #{CTag := #stream{credit = Credit0} = Str0} ->
                              Str1 = Str0#stream{credit = Credit0 + Credit},
                              {Str, Msgs0} = stream_entries(QName, Name, LocalPid, Str1),
                              {Readers0#{CTag => Str}, Msgs0};
                          _ ->
                              {Readers0, []}
                      end,
    {Readers, Actions} =
        case Drain of
            true ->
                case Readers1 of
                    #{CTag := #stream{credit = Credit1} = Str2} ->
                        {Readers0#{CTag => Str2#stream{credit = 0}}, [{send_drained, {CTag, Credit1}}]};
                    _ ->
                        {Readers1, []}
                end;
            false ->
                {Readers1, []}
        end,
    {State#stream_client{readers = Readers}, [{send_credit_reply, length(Msgs)},
                                              {deliver, CTag, true, Msgs}] ++ Actions}.

deliver(QSs, #delivery{message = Msg, confirm = Confirm} = Delivery) ->
    lists:foldl(
      fun({Q, stateless}, {Qs, Actions}) ->
              LeaderPid = amqqueue:get_pid(Q),
              ok = osiris:write(LeaderPid, msg_to_iodata(Msg)),
              {Qs, Actions};
         ({Q, S0}, {Qs, Actions}) ->
              {S, As} = deliver(Confirm, Delivery, S0),
              {[{Q, S} | Qs], As ++ Actions}
      end, {[], []}, QSs).

deliver(_Confirm, #delivery{message = Msg, msg_seq_no = MsgId},
        #stream_client{name = Name,
                       leader = LeaderPid,
                       writer_id = WriterId,
                       next_seq = Seq,
                       correlation = Correlation0,
                       soft_limit = SftLmt,
                       slow = Slow0} = State) ->
    ok = osiris:write(LeaderPid, WriterId, Seq, msg_to_iodata(Msg)),
    Correlation = case MsgId of
                      undefined ->
                          Correlation0;
                      _ when is_number(MsgId) ->
                          Correlation0#{Seq => {MsgId, Msg}}
                  end,
    {Slow, Actions} = case maps:size(Correlation) >= SftLmt of
                          true when not Slow0 ->
                              {true, [{block, Name}]};
                          Bool ->
                              {Bool, []}
                      end,
    {State#stream_client{next_seq = Seq + 1,
                         correlation = Correlation,
                         slow = Slow}, Actions}.

-spec dequeue(_, _, _, _, client()) -> no_return().
dequeue(_, _, _, _, #stream_client{name = Name}) ->
    {protocol_error, not_implemented, "basic.get not supported by stream queues ~ts",
     [rabbit_misc:rs(Name)]}.

handle_event(_QName, {osiris_written, From, _WriterId, Corrs},
             State = #stream_client{correlation = Correlation0,
                                    soft_limit = SftLmt,
                                    slow = Slow0,
                                    name = Name}) ->
    MsgIds = lists:sort(maps:fold(
                          fun (_Seq, {I, _M}, Acc) ->
                                  [I | Acc]
                          end, [], maps:with(Corrs, Correlation0))),

    Correlation = maps:without(Corrs, Correlation0),
    {Slow, Actions} = case maps:size(Correlation) < SftLmt of
                          true when Slow0 ->
                              {false, [{unblock, Name}]};
                          _ ->
                              {Slow0, []}
                      end,
    {ok, State#stream_client{correlation = Correlation,
                             slow = Slow}, [{settled, From, MsgIds} | Actions]};
handle_event(QName, {osiris_offset, _From, _Offs},
             State = #stream_client{local_pid = LocalPid,
                                    readers = Readers0,
                                    name = Name}) ->
    %% offset isn't actually needed as we use the atomic to read the
    %% current committed
    {Readers, TagMsgs} = maps:fold(
                           fun (Tag, Str0, {Acc, TM}) ->
                                   {Str, Msgs} = stream_entries(QName, Name, LocalPid, Str0),
                                   {Acc#{Tag => Str}, [{Tag, LocalPid, Msgs} | TM]}
                           end, {#{}, []}, Readers0),
    Ack = true,
    Deliveries = [{deliver, Tag, Ack, OffsetMsg}
                  || {Tag, _LeaderPid, OffsetMsg} <- TagMsgs],
    {ok, State#stream_client{readers = Readers}, Deliveries};
handle_event(_QName, {stream_leader_change, Pid}, State) ->
    {ok, update_leader_pid(Pid, State), []};
handle_event(_QName, {stream_local_member_change, Pid}, #stream_client{local_pid = P} = State)
  when P == Pid ->
    {ok, State, []};
handle_event(_QName, {stream_local_member_change, Pid}, State = #stream_client{name = QName,
                                                                       readers = Readers0}) ->
    rabbit_log:debug("Local member change event for ~tp", [QName]),
    Readers1 = maps:fold(fun(T, #stream{log = Log0} = S0, Acc) ->
                                 Offset = osiris_log:next_offset(Log0),
                                 osiris_log:close(Log0),
                                 CounterSpec = {{?MODULE, QName, self()}, []},
                                 rabbit_log:debug("Re-creating Osiris reader for consumer ~tp at offset ~tp", [T, Offset]),
                                 {ok, Log1} = osiris:init_reader(Pid, Offset, CounterSpec),
                                 NextOffset = osiris_log:next_offset(Log1) - 1,
                                 rabbit_log:debug("Registering offset listener at offset ~tp", [NextOffset]),
                                 osiris:register_offset_listener(Pid, NextOffset),
                                 S1 = S0#stream{listening_offset = NextOffset,
                                                log = Log1},
                                 Acc#{T => S1}

                         end, #{}, Readers0),
    {ok, State#stream_client{local_pid = Pid, readers = Readers1}, []};
handle_event(_QName, eol, #stream_client{name = Name}) ->
    {eol, [{unblock, Name}]}.

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

settle(QName, complete, CTag, MsgIds, #stream_client{readers = Readers0,
                                              local_pid = LocalPid,
                                              name = Name} = State) ->
    Credit = length(MsgIds),
    {Readers, Msgs} = case Readers0 of
                          #{CTag := #stream{credit = Credit0} = Str0} ->
                              Str1 = Str0#stream{credit = Credit0 + Credit},
                              {Str, Msgs0} = stream_entries(QName, Name, LocalPid, Str1),
                              {Readers0#{CTag => Str}, Msgs0};
                          _ ->
                              {Readers0, []}
                      end,
    {State#stream_client{readers = Readers}, [{deliver, CTag, true, Msgs}]};
settle(_, _, _, _, #stream_client{name = Name}) ->
    {protocol_error, not_implemented,
     "basic.nack and basic.reject not supported by stream queues ~ts",
     [rabbit_misc:rs(Name)]}.

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
                 [{role, Role},
                  get_key(node, C),
                  get_key(epoch, C),
                  get_key(offset, C),
                  get_key(committed_offset, C),
                  get_key(first_offset, C),
                  get_key(readers, C),
                  get_key(segments, C)]
             end || {Role, C} <- get_counters(Q)];
        {error, not_found} = E ->
            E
    end.

get_key(Key, Cnt) ->
    {Key, maps:get(Key, Cnt, undefined)}.

-spec is_writer({pid() | undefined, writer | replica}) -> boolean().
is_writer({_, writer}) -> true;
is_writer(_Member) -> false.

get_counters(Q) ->
    #{name := StreamId} = amqqueue:get_type_state(Q),
    {ok, #{members := Members}} = rabbit_stream_coordinator:stream_overview(StreamId),
    %% split members to query the writer last
    %% this minimizes the risk of confusing output where replicas are ahead of the writer
    Writer = maps:keys(maps:filter(fun (_, M) -> is_writer(M) end, Members)),
    Replicas = maps:keys(maps:filter(fun (_, M) -> not is_writer(M) end, Members)),
    QName = amqqueue:get_name(Q),
    Counters0 = [begin
                    safe_get_overview(Node, QName)
                 end || Node <- lists:append(Replicas, Writer)],
    Counters1 = lists:filter(fun (X) -> X =/= undefined end, Counters0),
    %% sort again in the original order (by node)
    lists:sort(fun ({_, M1}, {_, M2}) -> maps:get(node, M1) < maps:get(node, M2) end, Counters1).

safe_get_overview(Node, QName) ->
    case rpc:call(Node, ?MODULE, get_overview, [QName]) of
        {badrpc, _} ->
            #{node => Node};
        Result ->
            Result
    end.

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

init(Q) when ?is_amqqueue(Q) ->
    Leader = amqqueue:get_pid(Q),
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

close(#stream_client{readers = Readers}) ->
    maps:foreach(fun (_, #stream{log = Log}) ->
                         osiris_log:close(Log)
                 end, Readers).

update(Q, State)
  when ?is_amqqueue(Q) ->
    Pid = amqqueue:get_pid(Q),
    update_leader_pid(Pid, State).

update_leader_pid(Pid, #stream_client{leader = Pid} =  State) ->
    State;
update_leader_pid(Pid, #stream_client{} =  State) ->
    rabbit_log:debug("stream client: new leader detected ~w", [Pid]),
    resend_all(State#stream_client{leader = Pid}).

state_info(_) ->
    #{}.

set_retention_policy(Name, VHost, Policy) ->
    case rabbit_amqqueue:check_max_age(Policy) of
        {error, _} = E ->
            E;
        MaxAge ->
            QName = rabbit_misc:r(VHost, queue, Name),
            Fun = fun(Q) ->
                          Conf = amqqueue:get_type_state(Q),
                          amqqueue:set_type_state(Q, Conf#{max_age => MaxAge})
                  end,
            case rabbit_amqqueue:update(QName, Fun) of
                not_found ->
                    {error, not_found};
                _ ->
                    ok
            end
    end.

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
    QName = rabbit_misc:r(VHost, queue, Name),
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
        E ->
            E
    end.

delete_replica(VHost, Name, Node) ->
    QName = rabbit_misc:r(VHost, queue, Name),
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
                    #{name := StreamId} = amqqueue:get_type_state(Q),
                    {ok, Reply, _} = rabbit_stream_coordinator:delete_replica(StreamId, Node),
                    Reply
            end;
        E ->
            E
    end.

make_stream_conf(Q) ->
    QName = amqqueue:get_name(Q),
    Name = stream_name(QName),
    %% MaxLength = args_policy_lookup(<<"max-length">>, policy_precedence/2, Q),
    MaxBytes = args_policy_lookup(<<"max-length-bytes">>, fun policy_precedence/2, Q),
    MaxAge = max_age(args_policy_lookup(<<"max-age">>, fun policy_precedence/2, Q)),
    MaxSegmentSizeBytes = args_policy_lookup(<<"stream-max-segment-size-bytes">>, fun policy_precedence/2, Q),
    Formatter = {?MODULE, format_osiris_event, [QName]},
    Retention = lists:filter(fun({_, R}) ->
                                     R =/= undefined
                             end, [{max_bytes, MaxBytes},
                                   {max_age, MaxAge}]),
    add_if_defined(max_segment_size_bytes, MaxSegmentSizeBytes,
                   #{reference => QName,
                     name => Name,
                     retention => Retention,
                     event_formatter => Formatter,
                     epoch => 1}).

update_stream_conf(undefined, #{} = Conf) ->
    Conf;
update_stream_conf(Q, #{} = Conf) when ?is_amqqueue(Q) ->
    MaxBytes = args_policy_lookup(<<"max-length-bytes">>, fun policy_precedence/2, Q),
    MaxAge = max_age(args_policy_lookup(<<"max-age">>, fun policy_precedence/2, Q)),
    MaxSegmentSizeBytes = args_policy_lookup(<<"stream-max-segment-size-bytes">>, fun policy_precedence/2, Q),
    Retention = lists:filter(fun({_, R}) ->
                                     R =/= undefined
                             end, [{max_bytes, MaxBytes},
                                   {max_age, MaxAge}]),
    add_if_defined(max_segment_size_bytes, MaxSegmentSizeBytes,
                   Conf#{retention => Retention}).

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

check_queue_exists_in_local_node(Q) ->
    #{name := StreamId} = amqqueue:get_type_state(Q),
    case rabbit_stream_coordinator:local_pid(StreamId) of
        {ok, Pid} when is_pid(Pid) ->
            ok;
        _ ->
            {protocol_error, precondition_failed,
             "queue '~ts' does not have a replica on the local node",
             [rabbit_misc:rs(amqqueue:get_name(Q))]}
    end.

maybe_send_reply(_ChPid, undefined) -> ok;
maybe_send_reply(ChPid, Msg) -> ok = rabbit_channel:send_command(ChPid, Msg).

stream_entries(QName, Name, LocalPid, Str) ->
    stream_entries(QName, Name, LocalPid, Str, []).

stream_entries(QName, Name, LocalPid,
               #stream{credit = Credit,
                       start_offset = StartOffs,
                       listening_offset = LOffs,
                       log = Seg0} = Str0, MsgIn)
  when Credit > 0 ->
    case osiris_log:read_chunk_parsed(Seg0) of
        {end_of_stream, Seg} ->
            NextOffset = osiris_log:next_offset(Seg),
            case NextOffset > LOffs of
                true ->
                    osiris:register_offset_listener(LocalPid, NextOffset),
                    {Str0#stream{log = Seg,
                                 listening_offset = NextOffset}, MsgIn};
                false ->
                    {Str0#stream{log = Seg}, MsgIn}
            end;
        {error, Err} ->
            rabbit_log:debug("stream client: error reading chunk ~w", [Err]),
            exit(Err);
        {Records, Seg} ->
            Msgs = [begin
                        Msg0 = binary_to_msg(QName, B),
                        Msg = rabbit_basic:add_header(<<"x-stream-offset">>,
                                                      long, O, Msg0),
                        {Name, LocalPid, O, false, Msg}
                    end || {O, B} <- Records,
                           O >= StartOffs],

            NumMsgs = length(Msgs),

            Str = Str0#stream{credit = Credit - NumMsgs,
                              log = Seg},
            case Str#stream.credit < 1 of
                true ->
                    %% we are done here
                    {Str, MsgIn ++ Msgs};
                false ->
                    %% if there are fewer Msgs than Entries0 it means there were non-events
                    %% in the log and we should recurse and try again
                    stream_entries(QName, Name, LocalPid, Str, MsgIn ++ Msgs)
            end
    end;
stream_entries(_QName, _Name, _LocalPid, Str, Msgs) ->
    {Str, Msgs}.

binary_to_msg(#resource{virtual_host = VHost,
                        kind = queue,
                        name = QName}, Data) ->
    R0 = rabbit_msg_record:init(Data),
    %% if the message annotation isn't present the data most likely came from
    %% the rabbitmq-stream plugin so we'll choose defaults that simulate use
    %% of the direct exchange
    {utf8, Exchange} = rabbit_msg_record:message_annotation(<<"x-exchange">>,
                                                            R0, {utf8, <<>>}),
    {utf8, RoutingKey} = rabbit_msg_record:message_annotation(<<"x-routing-key">>,
                                                              R0, {utf8, QName}),
    {Props, Payload} = rabbit_msg_record:to_amqp091(R0),
    XName = #resource{kind = exchange,
                      virtual_host = VHost,
                      name = Exchange},
    Content = #content{class_id = 60,
                       properties = Props,
                       properties_bin = none,
                       payload_fragments_rev = [Payload]},
    {ok, Msg} = rabbit_basic:message(XName, RoutingKey, Content),
    Msg.


msg_to_iodata(#basic_message{exchange_name = #resource{name = Exchange},
                             routing_keys = [RKey | _],
                             content = Content}) ->
    #content{properties = Props,
             payload_fragments_rev = Payload} =
        rabbit_binary_parser:ensure_content_decoded(Content),
    R0 = rabbit_msg_record:from_amqp091(Props, lists:reverse(Payload)),
    %% TODO durable?
    R = rabbit_msg_record:add_message_annotations(
          #{<<"x-exchange">> => {utf8, Exchange},
            <<"x-routing-key">> => {utf8, RKey}}, R0),
    rabbit_msg_record:to_iodata(R).

capabilities() ->
    #{unsupported_policies => [%% Classic policies
                               <<"expires">>, <<"message-ttl">>, <<"dead-letter-exchange">>,
                               <<"dead-letter-routing-key">>, <<"max-length">>,
                               <<"max-in-memory-length">>, <<"max-in-memory-bytes">>,
                               <<"max-priority">>, <<"overflow">>, <<"queue-mode">>,
                               <<"single-active-consumer">>, <<"delivery-limit">>,
                               <<"ha-mode">>, <<"ha-params">>, <<"ha-sync-mode">>,
                               <<"ha-promote-on-shutdown">>, <<"ha-promote-on-failure">>,
                               <<"queue-master-locator">>,
                               %% Quorum policies
                               <<"dead-letter-strategy">>],
      queue_arguments => [<<"x-max-length-bytes">>, <<"x-queue-type">>,
                          <<"x-max-age">>, <<"x-stream-max-segment-size-bytes">>,
                          <<"x-initial-cluster-size">>, <<"x-queue-leader-locator">>],
      consumer_arguments => [<<"x-stream-offset">>, <<"x-credit">>],
      server_named => false}.

notify_decorators(Q) when ?is_amqqueue(Q) ->
    %% Not supported
    ok.

resend_all(#stream_client{leader = LeaderPid,
                          writer_id = WriterId,
                          correlation = Corrs} = State) ->
    Msgs = lists:sort(maps:values(Corrs)),
    case Msgs of
        [] -> ok;
        [{Seq, _} | _] ->
            rabbit_log:debug("stream client: resending from seq ~w num ~b",
                             [Seq, maps:size(Corrs)])
    end,
    [begin
         ok = osiris:write(LeaderPid, WriterId, Seq, msg_to_iodata(Msg))
     end || {Seq, Msg} <- Msgs],
    State.

set_leader_pid(Pid, QName) ->
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
