%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% Copyright (c) 2012-2020 VMware, Inc. or its affiliates.  All rights reserved.
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
         capabilities/0]).

-export([set_retention_policy/3]).
-export([add_replica/3,
         delete_replica/3]).
-export([format_osiris_event/2]).
-export([update_stream_conf/1]).

-include("rabbit.hrl").
-include("amqqueue.hrl").

-define(INFO_KEYS, [name, durable, auto_delete, arguments, leader, members, online, state,
                    messages, messages_ready, messages_unacknowledged, committed_offset,
                    policy, operator_policy, effective_policy_definition, type]).

-type appender_seq() :: non_neg_integer().

-record(stream, {name :: rabbit_types:r('queue'),
                 credit :: integer(),
                 max :: non_neg_integer(),
                 start_offset = 0 :: non_neg_integer(),
                 listening_offset = 0 :: non_neg_integer(),
                 log :: undefined | orisis_log:state()}).

-record(stream_client, {name :: term(),
                        leader :: pid(),
                        next_seq = 1 :: non_neg_integer(),
                        correlation = #{} :: #{appender_seq() => term()},
                        soft_limit :: non_neg_integer(),
                        slow = false :: boolean(),
                        readers = #{} :: #{term() => #stream{}}
                       }).

-import(rabbit_queue_type_util, [args_policy_lookup/3]).

-type client() :: #stream_client{}.

-spec is_enabled() -> boolean().
is_enabled() ->
    rabbit_feature_flags:is_enabled(stream_queue).

-spec declare(amqqueue:amqqueue(), node()) ->
    {'new' | 'existing', amqqueue:amqqueue()} |
    rabbit_types:channel_exit().
declare(Q0, Node) when ?amqqueue_is_stream(Q0) ->
    Arguments = amqqueue:get_arguments(Q0),
    QName = amqqueue:get_name(Q0),
    rabbit_queue_type_util:check_auto_delete(Q0),
    rabbit_queue_type_util:check_exclusive(Q0),
    rabbit_queue_type_util:check_non_durable(Q0),
    Opts = amqqueue:get_options(Q0),
    ActingUser = maps:get(user, Opts, ?UNKNOWN_USER),
    Conf0 = make_stream_conf(Node, Q0),
    case rabbit_stream_coordinator:start_cluster(
           amqqueue:set_type_state(Q0, Conf0)) of
        {ok, {error, already_started}, _} ->
            rabbit_misc:protocol_error(precondition_failed,
                                       "safe queue name already in use '~s'",
                                       [Node]);
        {ok, {created, Q}, _} ->
            rabbit_event:notify(queue_created,
                                [{name, QName},
                                 {durable, true},
                                 {auto_delete, false},
                                 {arguments, Arguments},
                                 {user_who_performed_action,
                                  ActingUser}]),
            {new, Q};
        {ok, {error, Error}, _} ->
            _ = rabbit_amqqueue:internal_delete(QName, ActingUser),
            rabbit_misc:protocol_error(
              internal_error,
              "Cannot declare a queue '~s' on node '~s': ~255p",
              [rabbit_misc:rs(QName), node(), Error]);
        {ok, {existing, Q}, _} ->
            {existing, Q};
        {error, coordinator_unavailable} ->
            _ = rabbit_amqqueue:internal_delete(QName, ActingUser),
            rabbit_misc:protocol_error(
              internal_error,
              "Cannot declare a queue '~s' on node '~s': coordinator unavailable",
              [rabbit_misc:rs(QName), node()])
    end.

-spec delete(amqqueue:amqqueue(), boolean(),
             boolean(), rabbit_types:username()) ->
    rabbit_types:ok(non_neg_integer()) |
    rabbit_types:error(in_use | not_empty).
delete(Q, _IfUnused, _IfEmpty, ActingUser) ->
    Name = maps:get(name, amqqueue:get_type_state(Q)),
    {ok, Reply, _} = rabbit_stream_coordinator:delete_cluster(Name, ActingUser),
    Reply.

-spec purge(amqqueue:amqqueue()) ->
    {'ok', non_neg_integer()}.
purge(_Q) ->
    {ok, 0}.

-spec policy_changed(amqqueue:amqqueue()) -> 'ok'.
policy_changed(Q) ->
    Name = maps:get(name, amqqueue:get_type_state(Q)),
    _ = rabbit_stream_coordinator:policy_changed(Name),
    ok.

stat(_) ->
    {ok, 0, 0}.

consume(Q, #{prefetch_count := 0}, _)
  when ?amqqueue_is_stream(Q) ->
    rabbit_misc:protocol_error(precondition_failed,
                               "consumer prefetch count is not set for '~s'",
                               [rabbit_misc:rs(amqqueue:get_name(Q))]);
consume(Q, #{no_ack := true}, _)
  when ?amqqueue_is_stream(Q) ->
    rabbit_misc:protocol_error(
      not_implemented,
      "automatic acknowledgement not supported by stream queues ~s",
      [rabbit_misc:rs(amqqueue:get_name(Q))]);
consume(Q, #{limiter_active := true}, _State)
  when ?amqqueue_is_stream(Q) ->
    {error, global_qos_not_supported_for_queue_type};
consume(Q, Spec, QState0) when ?amqqueue_is_stream(Q) ->
    %% Messages should include the offset as a custom header.
    check_queue_exists_in_local_node(Q),
    #{no_ack := NoAck,
      channel_pid := ChPid,
      prefetch_count := ConsumerPrefetchCount,
      consumer_tag := ConsumerTag,
      exclusive_consume := ExclusiveConsume,
      args := Args,
      ok_msg := OkMsg} = Spec,
    QName = amqqueue:get_name(Q),
    Offset = case rabbit_misc:table_lookup(Args, <<"x-stream-offset">>) of
                 undefined ->
                     next;
                 {_, <<"first">>} ->
                     first;
                 {_, <<"last">>} ->
                     last;
                 {_, <<"next">>} ->
                     next;
                 {_, V} ->
                     V
             end,
    rabbit_core_metrics:consumer_created(ChPid, ConsumerTag, ExclusiveConsume,
                                         not NoAck, QName,
                                         ConsumerPrefetchCount, false,
                                         up, Args),
    %% FIXME: reply needs to be sent before the stream begins sending
    %% really it should be sent by the stream queue process like classic queues
    %% do
    maybe_send_reply(ChPid, OkMsg),
    QState = begin_stream(QState0, Q, ConsumerTag, Offset,
                          ConsumerPrefetchCount),
    {ok, QState, []}.

get_local_pid(#{leader_pid := Pid}) when node(Pid) == node() ->
    Pid;
get_local_pid(#{replica_pids := ReplicaPids}) ->
    [Local | _] = lists:filter(fun(Pid) ->
                                       node(Pid) == node()
                               end, ReplicaPids),
    Local.

begin_stream(#stream_client{readers = Readers0} = State,
             Q, Tag, Offset, Max) ->
    LocalPid = get_local_pid(amqqueue:get_type_state(Q)),
    {ok, Seg0} = osiris:init_reader(LocalPid, Offset),
    NextOffset = osiris_log:next_offset(Seg0) - 1,
    osiris:register_offset_listener(LocalPid, NextOffset),
    %% TODO: avoid double calls to the same process
    StartOffset = case Offset of
                      first -> NextOffset;
                      last -> NextOffset;
                      next -> NextOffset;
                      _ -> Offset
                  end,
    Str0 = #stream{name = amqqueue:get_name(Q),
                   credit = Max,
                   start_offset = StartOffset,
                   listening_offset = NextOffset,
                   log = Seg0,
                   max = Max},
    State#stream_client{readers = Readers0#{Tag => Str0}}.

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

credit(_, _, _, _) ->
    ok.

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
                      next_seq = Seq,
                      correlation = Correlation0,
                      soft_limit = SftLmt,
                      slow = Slow0} = State) ->
    ok = osiris:write(LeaderPid, Seq, msg_to_iodata(Msg)),
    Correlation = case MsgId of
                      undefined ->
                          Correlation0;
                      _ when is_number(MsgId) ->
                          Correlation0#{Seq => MsgId}
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
    rabbit_misc:protocol_error(
      not_implemented,
      "basic.get not supported by stream queues ~s",
      [rabbit_misc:rs(Name)]).

handle_event({osiris_written, From, Corrs}, State = #stream_client{correlation = Correlation0,
                                                   soft_limit = SftLmt,
                                                   slow = Slow0,
                                                   name = Name}) ->
    MsgIds = maps:values(maps:with(Corrs, Correlation0)),
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
handle_event({osiris_offset, _From, _Offs}, State = #stream_client{leader = Leader,
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
    {ok, State#stream_client{readers = Readers}, Deliveries}.

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
    rabbit_misc:protocol_error(
      not_implemented,
      "basic.nack and basic.reject not supported by stream queues ~s",
      [rabbit_misc:rs(Name)]).

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
    #{leader_node := Leader} = amqqueue:get_type_state(Q),
    Leader;
i(members, Q) when ?is_amqqueue(Q) ->
    #{replica_nodes := Nodes} = amqqueue:get_type_state(Q),
    Nodes;
i(online, Q) ->
    #{replica_pids := ReplicaPids,
      leader_pid := LeaderPid} = amqqueue:get_type_state(Q),
    [node(P) || P <- ReplicaPids ++ [LeaderPid], rabbit_misc:is_process_alive(P)];
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
    Data = osiris_counters:overview(),
    maps:get(committed_offset,
             maps:get({osiris_writer, amqqueue:get_name(Q)}, Data));
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
i(type, _) ->
    stream;
i(_, _) ->
    ''.

init(Q) when ?is_amqqueue(Q) ->
    Leader = amqqueue:get_pid(Q),
    {ok, SoftLimit} = application:get_env(rabbit, stream_messages_soft_limit),
    #stream_client{name = amqqueue:get_name(Q),
                   leader = Leader,
                   soft_limit = SoftLimit}.

close(#stream_client{readers = Readers}) ->
    _ = maps:map(fun (_, #stream{log = Log}) ->
                         osiris_log:close(Log)
                 end, Readers),
    ok.

update(_, State) ->
    State.

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
                    #{name := StreamId} = amqqueue:get_type_state(Q),
                    {ok, Reply, _} = rabbit_stream_coordinator:add_replica(StreamId, Node),
                    Reply
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
    Name = queue_name(QName),
    %% MaxLength = args_policy_lookup(<<"max-length">>, fun min/2, Q),
    MaxBytes = args_policy_lookup(<<"max-length-bytes">>, fun min/2, Q),
    MaxAge = max_age(args_policy_lookup(<<"max-age">>, fun max_age/2, Q)),
    MaxSegmentSize = args_policy_lookup(<<"max-segment-size">>, fun min/2, Q),
    LeaderLocator = queue_leader_locator(args_policy_lookup(<<"queue-leader-locator">>,
                                                            fun res_arg/2, Q)),
    Replicas0 = rabbit_mnesia:cluster_nodes(all) -- [Node],
    Arguments = amqqueue:get_arguments(Q),
    Replicas = select_stream_nodes(get_initial_cluster_size(Arguments) - 1, Replicas0),
    Formatter = {?MODULE, format_osiris_event, [QName]},
    Retention = lists:filter(fun({_, R}) ->
                                     R =/= undefined
                             end, [{max_bytes, MaxBytes},
                                   {max_age, MaxAge}]),
    add_if_defined(max_segment_size, MaxSegmentSize, #{reference => QName,
                                                       name => Name,
                                                       retention => Retention,
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

update_stream_conf(#{reference := QName} = Conf) ->
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} ->
            MaxBytes = args_policy_lookup(<<"max-length-bytes">>, fun min/2, Q),
            MaxAge = max_age(args_policy_lookup(<<"max-age">>, fun max_age/2, Q)),
            MaxSegmentSize = args_policy_lookup(<<"max-segment-size">>, fun min/2, Q),
            Retention = lists:filter(fun({_, R}) ->
                                             R =/= undefined
                                     end, [{max_bytes, MaxBytes},
                                           {max_age, MaxAge}]),
            add_if_defined(max_segment_size, MaxSegmentSize, Conf#{retention => Retention});
        _ ->
            Conf
    end.

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

max_age(Age1, Age2) ->
    min(rabbit_amqqueue:check_max_age(Age1), rabbit_amqqueue:check_max_age(Age2)).

queue_leader_locator(undefined) -> <<"client-local">>;
queue_leader_locator(Val) -> Val.

res_arg(PolVal, undefined) -> PolVal;
res_arg(_, ArgVal) ->  ArgVal.
    
queue_name(#resource{virtual_host = VHost, name = Name}) ->
    Timestamp = erlang:integer_to_binary(erlang:system_time()),
    osiris_util:to_base64uri(erlang:binary_to_list(<<VHost/binary, "_", Name/binary, "_",
                                                     Timestamp/binary>>)).

recover(Q) ->
    rabbit_stream_coordinator:recover(),
    {ok, Q}.

check_queue_exists_in_local_node(Q) ->
    Conf = amqqueue:get_type_state(Q),
    AllNodes = [maps:get(leader_node, Conf) | maps:get(replica_nodes, Conf)],
    case lists:member(node(), AllNodes) of
        true ->
            ok;
        false ->
            rabbit_misc:protocol_error(precondition_failed,
                                       "queue '~s' does not a have a replica on the local node",
                                       [rabbit_misc:rs(amqqueue:get_name(Q))])
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
    #{policies => [<<"max-length-bytes">>, <<"max-age">>, <<"max-segment-size">>,
                   <<"queue-leader-locator">>],
      queue_arguments => [<<"x-dead-letter-exchange">>, <<"x-dead-letter-routing-key">>,
                          <<"x-max-length">>, <<"x-max-length-bytes">>,
                          <<"x-single-active-consumer">>, <<"x-queue-type">>,
                          <<"x-max-age">>, <<"x-max-segment-size">>,
                          <<"x-initial-cluster-size">>, <<"x-queue-leader-locator">>],
      consumer_arguments => [<<"x-stream-offset">>],
      server_named => false}.

get_initial_cluster_size(Arguments) ->
    case rabbit_misc:table_lookup(Arguments, <<"x-initial-cluster-size">>) of
        undefined -> length(rabbit_mnesia:cluster_nodes(running));
        {_Type, Val} -> Val
    end.
