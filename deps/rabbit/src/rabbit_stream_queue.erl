%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stream_queue).

-behaviour(rabbit_queue_type).

-export([is_enabled/0,
         declare/2,
         delete/4,
         purge/1,
         policy_changed/1,
         recover/2,
         is_recoverable/1,
         consume/3,
         cancel/5,
         handle_event/2,
         deliver/2,
         settle/4,
         credit/4,
         dequeue/4,
         info/2,
         init/1,
         close/1,
         update/2,
         state_info/1,
         stat/1,
         capabilities/0,
         notify_decorators/1]).

-export([set_retention_policy/3]).
-export([add_replica/3,
         delete_replica/3]).
-export([format_osiris_event/2]).
-export([update_stream_conf/2]).
-export([readers/1]).

-export([parse_offset_arg/1]).

-export([status/2,
         tracking_status/2]).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("amqqueue.hrl").

-define(INFO_KEYS, [name, durable, auto_delete, arguments, leader, members, online, state,
                    messages, messages_ready, messages_unacknowledged, committed_offset,
                    policy, operator_policy, effective_policy_definition, type, memory]).

-type appender_seq() :: non_neg_integer().

-type msg_id() :: non_neg_integer().
-type msg() :: term(). %% TODO: refine

-record(stream, {name :: rabbit_types:r('queue'),
                 credit :: integer(),
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
is_enabled() ->
    rabbit_feature_flags:is_enabled(stream_queue).

-spec declare(amqqueue:amqqueue(), node()) ->
    {'new' | 'existing', amqqueue:amqqueue()} |
    {protocol_error, Type :: atom(), Reason :: string(), Args :: term()}.
declare(Q0, Node) when ?amqqueue_is_stream(Q0) ->
    case rabbit_queue_type_util:run_checks(
           [fun rabbit_queue_type_util:check_auto_delete/1,
            fun rabbit_queue_type_util:check_exclusive/1,
            fun rabbit_queue_type_util:check_non_durable/1],
           Q0) of
        ok ->
            create_stream(Q0, Node);
        Err ->
            Err
    end.

create_stream(Q0, Node) ->
    Arguments = amqqueue:get_arguments(Q0),
    QName = amqqueue:get_name(Q0),
    Opts = amqqueue:get_options(Q0),
    ActingUser = maps:get(user, Opts, ?UNKNOWN_USER),
    Conf0 = make_stream_conf(Node, Q0),
    Conf = apply_leader_locator_strategy(Conf0),
    #{leader_node := LeaderNode} = Conf,
    Q1 = amqqueue:set_type_state(Q0, Conf),
    case rabbit_amqqueue:internal_declare(Q1, false) of
        {created, Q} ->
            case rabbit_stream_coordinator:new_stream(Q, LeaderNode) of
                {ok, {ok, LeaderPid}, _} ->
                    %% update record with leader pid
                    set_leader_pid(LeaderPid, amqqueue:get_name(Q)),
                    rabbit_event:notify(queue_created,
                                        [{name, QName},
                                         {durable, true},
                                         {auto_delete, false},
                                         {arguments, Arguments},
                                         {user_who_performed_action,
                                          ActingUser}]),
                    {new, Q};
                Error ->

                    _ = rabbit_amqqueue:internal_delete(QName, ActingUser),
                    {protocol_error, internal_error, "Cannot declare a queue '~s' on node '~s': ~255p",
                     [rabbit_misc:rs(QName), node(), Error]}
            end;
        {existing, Q} ->
            {existing, Q}
    end.

-spec delete(amqqueue:amqqueue(), boolean(),
             boolean(), rabbit_types:username()) ->
    rabbit_types:ok(non_neg_integer()) |
    rabbit_types:error(in_use | not_empty).
delete(Q, _IfUnused, _IfEmpty, ActingUser) ->
    {ok, Reply} = rabbit_stream_coordinator:delete_stream(Q, ActingUser),
    Reply.

-spec purge(amqqueue:amqqueue()) ->
    {ok, non_neg_integer()} | {error, term()}.
purge(_) ->
    {error, not_supported}.

-spec policy_changed(amqqueue:amqqueue()) -> 'ok'.
policy_changed(Q) ->
    _ = rabbit_stream_coordinator:policy_changed(Q),
    ok.

stat(Q) ->
    {ok, i(messages, Q), 0}.

consume(Q, #{prefetch_count := 0}, _)
  when ?amqqueue_is_stream(Q) ->
    {protocol_error, precondition_failed, "consumer prefetch count is not set for '~s'",
     [rabbit_misc:rs(amqqueue:get_name(Q))]};
consume(Q, #{no_ack := true}, _)
  when ?amqqueue_is_stream(Q) ->
    {protocol_error, not_implemented,
     "automatic acknowledgement not supported by stream queues ~s",
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
                    rabbit_core_metrics:consumer_created(ChPid, ConsumerTag, ExclusiveConsume,
                                                         not NoAck, QName,
                                                         ConsumerPrefetchCount, false,
                                                         up, Args),
                    %% FIXME: reply needs to be sent before the stream begins sending
                    %% really it should be sent by the stream queue process like classic queues
                    %% do
                    maybe_send_reply(ChPid, OkMsg),
                    begin_stream(QState0, Q, ConsumerTag, OffsetSpec, ConsumerPrefetchCount)
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
             Q, Tag, Offset, Max) ->
    {LocalPid, State} = get_local_pid(State0),
    case LocalPid of
        undefined ->
            {error, no_local_stream_replica_available};
        _ ->
            CounterSpec = {{?MODULE, QName, self()}, []},
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
            Str0 = #stream{name = amqqueue:get_name(Q),
                           credit = Max,
                           start_offset = StartOffset,
                           listening_offset = NextOffset,
                           log = Seg0,
                           max = Max},
            Actions = [],
            %% TODO: we need to monitor the local pid in case the stream is
            %% restarted
            {ok, State#stream_client{readers = Readers0#{Tag => Str0}}, Actions}
    end.

cancel(_Q, ConsumerTag, OkMsg, ActingUser, #stream_client{readers = Readers0,
                                                          name = QName} = State) ->
    Readers = maps:remove(ConsumerTag, Readers0),
    rabbit_core_metrics:consumer_deleted(self(), ConsumerTag, QName),
    rabbit_event:notify(consumer_deleted, [{consumer_tag, ConsumerTag},
                                           {channel, self()},
                                           {queue, QName},
                                           {user_who_performed_action, ActingUser}]),
    maybe_send_reply(self(), OkMsg),
    {ok, State#stream_client{readers = Readers}}.

credit(CTag, Credit, Drain, #stream_client{readers = Readers0,
                                           name = Name,
                                           leader = Leader} = State) ->
    {Readers1, Msgs} = case Readers0 of
                          #{CTag := #stream{credit = Credit0} = Str0} ->
                              Str1 = Str0#stream{credit = Credit0 + Credit},
                              {Str, Msgs0} = stream_entries(Name, Leader, Str1),
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

deliver(QSs, #delivery{confirm = Confirm} = Delivery) ->
    lists:foldl(
      fun({_Q, stateless}, {Qs, Actions}) ->
              %% TODO what do we do with stateless?
              %% QRef = amqqueue:get_pid(Q),
              %% ok = rabbit_fifo_client:untracked_enqueue(
              %%        [QRef], Delivery#delivery.message),
              {Qs, Actions};
         ({Q, S0}, {Qs, Actions}) ->
              S = deliver(Confirm, Delivery, S0),
              {[{Q, S} | Qs], Actions}
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
    Slow = case maps:size(Correlation) >= SftLmt of
               true when not Slow0 ->
                   credit_flow:block(Name),
                   true;
               Bool ->
                   Bool
           end,
    State#stream_client{next_seq = Seq + 1,
                        correlation = Correlation,
                        slow = Slow}.

-spec dequeue(_, _, _, client()) -> no_return().
dequeue(_, _, _, #stream_client{name = Name}) ->
    {protocol_error, not_implemented, "basic.get not supported by stream queues ~s",
     [rabbit_misc:rs(Name)]}.

handle_event({osiris_written, From, _WriterId, Corrs},
             State = #stream_client{correlation = Correlation0,
                                    soft_limit = SftLmt,
                                    slow = Slow0,
                                    name = Name}) ->
    MsgIds = lists:sort(maps:fold(
                          fun (_Seq, {I, _M}, Acc) ->
                                  [I | Acc]
                          end, [], maps:with(Corrs, Correlation0))),

    Correlation = maps:without(Corrs, Correlation0),
    Slow = case maps:size(Correlation) < SftLmt of
               true when Slow0 ->
                   credit_flow:unblock(Name),
                   false;
               _ ->
                   Slow0
           end,
    {ok, State#stream_client{correlation = Correlation,
                             slow = Slow}, [{settled, From, MsgIds}]};
handle_event({osiris_offset, _From, _Offs},
             State = #stream_client{leader = Leader,
                                    readers = Readers0,
                                    name = Name}) ->
    %% offset isn't actually needed as we use the atomic to read the
    %% current committed
    {Readers, TagMsgs} = maps:fold(
                           fun (Tag, Str0, {Acc, TM}) ->
                                   {Str, Msgs} = stream_entries(Name, Leader, Str0),
                                   %% HACK for now, better to just return but
                                   %% tricky with acks credits
                                   %% that also evaluate the stream
                                   % gen_server:cast(self(), {stream_delivery, Tag, Msgs}),
                                   {Acc#{Tag => Str}, [{Tag, Leader, Msgs} | TM]}
                           end, {#{}, []}, Readers0),
    Ack = true,
    Deliveries = [{deliver, Tag, Ack, OffsetMsg}
                  || {Tag, _LeaderPid, OffsetMsg} <- TagMsgs],
    {ok, State#stream_client{readers = Readers}, Deliveries};
handle_event({stream_leader_change, Pid}, State) ->
    {ok, update_leader_pid(Pid, State), []}.

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

settle(complete, CTag, MsgIds, #stream_client{readers = Readers0,
                                              name = Name,
                                              leader = Leader} = State) ->
    Credit = length(MsgIds),
    {Readers, Msgs} = case Readers0 of
                          #{CTag := #stream{credit = Credit0} = Str0} ->
                              Str1 = Str0#stream{credit = Credit0 + Credit},
                              {Str, Msgs0} = stream_entries(Name, Leader, Str1),
                              {Readers0#{CTag => Str}, Msgs0};
                          _ ->
                              {Readers0, []}
                      end,
    {State#stream_client{readers = Readers}, [{deliver, CTag, true, Msgs}]};
settle(_, _, _, #stream_client{name = Name}) ->
    {protocol_error, not_implemented,
     "basic.nack and basic.reject not supported by stream queues ~s",
     [rabbit_misc:rs(Name)]}.

info(Q, all_items) ->
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
    Data = osiris_counters:overview(),
    maps:get(committed_offset,
             maps:get({osiris_writer, amqqueue:get_name(Q)}, Data, #{}), '');
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
            _Pid = amqqueue:get_pid(Q),
            % Max = maps:get(max_segment_size_bytes, Conf, osiris_log:get_default_max_segment_size_bytes()),
            [begin
                 [{role, Role},
                  get_key(node, C),
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

get_counters(Q) ->
    #{name := StreamId} = amqqueue:get_type_state(Q),
    {ok, Members} = rabbit_stream_coordinator:members(StreamId),
    QName = amqqueue:get_name(Q),
    Counters = [begin
                    Data = safe_get_overview(Node),
                    get_counter(QName, Data, #{node => Node})
                end || Node <- maps:keys(Members)],
    lists:filter(fun (X) -> X =/= undefined end, Counters).

safe_get_overview(Node) ->
    case rpc:call(Node, osiris_counters, overview, []) of
        {badrpc, _} ->
            #{node => Node};
        Data ->
            Data
    end.

get_counter(QName, Data, Add) ->
    case maps:get({osiris_writer, QName}, Data, undefined) of
        undefined ->
            case maps:get({osiris_replica, QName}, Data, undefined) of
                undefined ->
                    {undefined, Add};
                M ->
                    {replica, maps:merge(Add, M)}
            end;
        M ->
            {writer, maps:merge(Add, M)}
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
            maps:fold(fun(K, {Type, Value}, Acc) ->
                              [[{reference, K},
                                {type, Type},
                                {value, Value}] | Acc]
                      end, [], Map);
        {error, not_found} = E->
            E
    end.

readers(QName) ->
    try
        Data = osiris_counters:overview(),
        Readers = case maps:get({osiris_writer, QName}, Data, not_found) of
                      not_found ->
                          maps:get(readers, maps:get({osiris_replica, QName}, Data, #{}), 0);
                      Map ->
                          maps:get(readers, Map, 0)
                  end,
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
        {error, coordinator_unavailable} = E ->
            rabbit_log:warning("Failed to start stream queue ~p: coordinator unavailable",
                               [rabbit_misc:rs(QName)]),
            E
    end.

close(#stream_client{readers = Readers}) ->
    _ = maps:map(fun (_, #stream{log = Log}) ->
                         osiris_log:close(Log)
                 end, Readers),
    ok.

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
            case rabbit_misc:execute_mnesia_transaction(
                   fun() -> rabbit_amqqueue:update(QName, Fun) end) of
                not_found ->
                    {error, not_found};
                _ ->
                    ok
            end
    end.

add_replica(VHost, Name, Node) ->
    QName = rabbit_misc:r(VHost, queue, Name),
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} when ?amqqueue_is_classic(Q) ->
            {error, classic_queue_not_supported};
        {ok, Q} when ?amqqueue_is_quorum(Q) ->
            {error, quorum_queue_not_supported};
        {ok, Q} when ?amqqueue_is_stream(Q) ->
            case lists:member(Node, rabbit_mnesia:cluster_nodes(running)) of
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
            case lists:member(Node, rabbit_mnesia:cluster_nodes(running)) of
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

make_stream_conf(Node, Q) ->
    QName = amqqueue:get_name(Q),
    Name = stream_name(QName),
    %% MaxLength = args_policy_lookup(<<"max-length">>, policy_precedence/2, Q),
    MaxBytes = args_policy_lookup(<<"max-length-bytes">>, fun policy_precedence/2, Q),
    MaxAge = max_age(args_policy_lookup(<<"max-age">>, fun policy_precedence/2, Q)),
    MaxSegmentSizeBytes = args_policy_lookup(<<"stream-max-segment-size-bytes">>, fun policy_precedence/2, Q),
    LeaderLocator = queue_leader_locator(args_policy_lookup(<<"queue-leader-locator">>,
                                                            fun policy_precedence/2, Q)),
    InitialClusterSize = initial_cluster_size(
                           args_policy_lookup(<<"initial-cluster-size">>,
                                              fun policy_precedence/2, Q)),
    Replicas0 = rabbit_mnesia:cluster_nodes(all) -- [Node],
    %% TODO: try to avoid nodes that are not connected
    Replicas = select_stream_nodes(InitialClusterSize - 1, Replicas0),
    Formatter = {?MODULE, format_osiris_event, [QName]},
    Retention = lists:filter(fun({_, R}) ->
                                     R =/= undefined
                             end, [{max_bytes, MaxBytes},
                                   {max_age, MaxAge}]),
    add_if_defined(max_segment_size_bytes, MaxSegmentSizeBytes, #{reference => QName,
                                                       name => Name,
                                                       retention => Retention,
                                                       nodes => [Node | Replicas],
                                                       leader_locator_strategy => LeaderLocator,
                                                       leader_node => Node,
                                                       replica_nodes => Replicas,
                                                       event_formatter => Formatter,
                                                       epoch => 1}).

select_stream_nodes(Size, All) when length(All) =< Size ->
    All;
select_stream_nodes(Size, All) ->
    Node = node(),
    case lists:member(Node, All) of
        true ->
            select_stream_nodes(Size - 1, lists:delete(Node, All), [Node]);
        false ->
            select_stream_nodes(Size, All, [])
    end.

select_stream_nodes(0, _, Selected) ->
    Selected;
select_stream_nodes(Size, Rest, Selected) ->
    S = lists:nth(rand:uniform(length(Rest)), Rest),
    select_stream_nodes(Size - 1, lists:delete(S, Rest), [S | Selected]).

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

queue_leader_locator(undefined) -> <<"client-local">>;
queue_leader_locator(Val) -> Val.

initial_cluster_size(undefined) ->
    length(rabbit_mnesia:cluster_nodes(running));
initial_cluster_size(Val) ->
    Val.

policy_precedence(PolVal, _ArgVal) ->
    PolVal.

stream_name(#resource{virtual_host = VHost, name = Name}) ->
    Timestamp = erlang:integer_to_binary(erlang:system_time()),
    osiris_util:to_base64uri(erlang:binary_to_list(<<VHost/binary, "_", Name/binary, "_",
                                                     Timestamp/binary>>)).

recover(Q) ->
    rabbit_stream_coordinator:recover(),
    {ok, Q}.

check_queue_exists_in_local_node(Q) ->
    Conf = amqqueue:get_type_state(Q),
    AllNodes = [maps:get(leader_node, Conf) |
                maps:get(replica_nodes, Conf)],
    case lists:member(node(), AllNodes) of
        true ->
            ok;
        false ->
            {protocol_error, precondition_failed,
             "queue '~s' does not a have a replica on the local node",
             [rabbit_misc:rs(amqqueue:get_name(Q))]}
    end.

maybe_send_reply(_ChPid, undefined) -> ok;
maybe_send_reply(ChPid, Msg) -> ok = rabbit_channel:send_command(ChPid, Msg).

stream_entries(Name, Id, Str) ->
    stream_entries(Name, Id, Str, []).

stream_entries(Name, LeaderPid,
               #stream{name = QName,
                       credit = Credit,
                       start_offset = StartOffs,
                       listening_offset = LOffs,
                       log = Seg0} = Str0, MsgIn)
  when Credit > 0 ->
    case osiris_log:read_chunk_parsed(Seg0) of
        {end_of_stream, Seg} ->
            NextOffset = osiris_log:next_offset(Seg),
            case NextOffset > LOffs of
                true ->
                    osiris:register_offset_listener(LeaderPid, NextOffset),
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
                        {Name, LeaderPid, O, false, Msg}
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
                    stream_entries(Name, LeaderPid, Str, MsgIn ++ Msgs)
            end
    end;
stream_entries(_Name, _Id, Str, Msgs) ->
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
    #{unsupported_policies =>
          [ %% Classic policies
            <<"expires">>, <<"message-ttl">>, <<"dead-letter-exchange">>,
            <<"dead-letter-routing-key">>, <<"max-length">>,
            <<"max-in-memory-length">>, <<"max-in-memory-bytes">>,
            <<"max-priority">>, <<"overflow">>, <<"queue-mode">>,
            <<"single-active-consumer">>, <<"delivery-limit">>,
            <<"ha-mode">>, <<"ha-params">>, <<"ha-sync-mode">>,
            <<"ha-promote-on-shutdown">>, <<"ha-promote-on-failure">>,
            <<"queue-master-locator">>],
      queue_arguments => [<<"x-dead-letter-exchange">>, <<"x-dead-letter-routing-key">>,
                          <<"x-max-length">>, <<"x-max-length-bytes">>,
                          <<"x-single-active-consumer">>, <<"x-queue-type">>,
                          <<"x-max-age">>, <<"x-stream-max-segment-size-bytes">>,
                          <<"x-initial-cluster-size">>, <<"x-queue-leader-locator">>],
      consumer_arguments => [<<"x-stream-offset">>],
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
    case rabbit_misc:execute_mnesia_transaction(
           fun() ->
                   rabbit_amqqueue:update(QName, Fun)
           end) of
        not_found ->
            %% This can happen during recovery
            [Q] = mnesia:dirty_read(rabbit_durable_queue, QName),
            rabbit_amqqueue:ensure_rabbit_queue_record_is_initialized(Fun(Q));
        _ ->
            ok
    end.

apply_leader_locator_strategy(#{leader_locator_strategy := <<"client-local">>} = Conf) ->
    Conf;
apply_leader_locator_strategy(#{leader_node := Leader,
                                replica_nodes := Replicas0,
                                leader_locator_strategy := <<"random">>,
                                name := StreamId} = Conf) ->
    Replicas = [Leader | Replicas0],
    ClusterSize = length(Replicas),
    Hash = erlang:phash2(StreamId),
    Pos = (Hash rem ClusterSize) + 1,
    NewLeader = lists:nth(Pos, Replicas),
    NewReplicas = lists:delete(NewLeader, Replicas),
    Conf#{leader_node => NewLeader,
          replica_nodes => NewReplicas};
apply_leader_locator_strategy(#{leader_node := Leader,
                                replica_nodes := Replicas0,
                                leader_locator_strategy := <<"least-leaders">>} = Conf) ->
    Replicas = [Leader | Replicas0],
    Counters0 = maps:from_list([{R, 0} || R <- Replicas]),
    Counters = maps:to_list(
                 lists:foldl(fun(Q, Acc) ->
                                     P = amqqueue:get_pid(Q),
                                     case amqqueue:get_type(Q) of
                                         ?MODULE when is_pid(P) ->
                                             maps:update_with(node(P), fun(V) -> V + 1 end, 1, Acc);
                                         _ ->
                                             Acc
                                     end
                             end, Counters0, rabbit_amqqueue:list())),
    Ordered = lists:sort(fun({_, V1}, {_, V2}) ->
                                 V1 =< V2
                         end, Counters),
    %% We could have potentially introduced nodes that are not in the list of replicas if
    %% initial cluster size is smaller than the cluster size. Let's select the first one
    %% that is on the list of replicas
    NewLeader = select_first_matching_node(Ordered, Replicas),
    NewReplicas = lists:delete(NewLeader, Replicas),
    Conf#{leader_node => NewLeader,
          replica_nodes => NewReplicas}.

select_first_matching_node([{N, _} | Rest], Replicas) ->
    case lists:member(N, Replicas) of
        true -> N;
        false -> select_first_matching_node(Rest, Replicas)
    end.
