%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2016-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_data).

-include("rabbit_mgmt_records.hrl").
-include("rabbit_mgmt_metrics.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_core_metrics.hrl").

-export([empty/2, pick_range/2]).

% delegate api
-export([overview_data/4,
         consumer_data/2,
         all_list_queue_data/3,
         all_detail_queue_data/3,
         all_exchange_data/3,
         all_connection_data/3,
         all_list_channel_data/3,
         all_detail_channel_data/3,
         all_vhost_data/3,
         all_node_data/3,
         augmented_created_stats/2,
         augmented_created_stats/3,
         augment_channel_pids/2,
         augment_details/2,
         lookup_element/2,
         lookup_element/3
        ]).

-import(rabbit_misc, [pget/2]).

-type maybe_slide() :: exometer_slide:slide() | not_found.
-type ranges() :: {maybe_range(), maybe_range(), maybe_range(), maybe_range()}.
-type maybe_range() :: no_range | #range{}.

%%----------------------------------------------------------------------------
%% Internal, query-time - node-local operations
%%----------------------------------------------------------------------------

created_stats(Name, Type) ->
    case ets:select(Type, [{{'_', '$2', '$3'}, [{'==', Name, '$2'}], ['$3']}]) of
        [] -> not_found;
        [Elem] -> Elem
    end.

created_stats(Type) ->
    %% TODO better tab2list?
    ets:select(Type, [{{'_', '_', '$3'}, [], ['$3']}]).

-spec all_detail_queue_data(pid(), [any()], ranges())  -> #{atom() => any()}.
all_detail_queue_data(_Pid, Ids, Ranges) ->
    lists:foldl(fun (Id, Acc) ->
                        Data = detail_queue_data(Ranges, Id),
                        maps:put(Id, Data, Acc)
                end, #{}, Ids).

all_list_queue_data(_Pid, Ids, Ranges) ->
    lists:foldl(fun (Id, Acc) ->
                        Data = list_queue_data(Ranges, Id),
                        maps:put(Id, Data, Acc)
                end, #{}, Ids).

all_detail_channel_data(_Pid, Ids, Ranges) ->
    lists:foldl(fun (Id, Acc) ->
                        Data = detail_channel_data(Ranges, Id),
                        maps:put(Id, Data, Acc)
                end, #{}, Ids).

all_list_channel_data(_Pid, Ids, Ranges) ->
    lists:foldl(fun (Id, Acc) ->
                        Data = list_channel_data(Ranges, Id),
                        maps:put(Id, Data, Acc)
                end, #{}, Ids).

connection_data(Ranges, Id) ->
    maps:from_list([raw_message_data(connection_stats_coarse_conn_stats,
                                     pick_range(coarse_conn_stats, Ranges), Id),
                    {connection_stats, lookup_element(connection_stats, Id)}]).

exchange_data(Ranges, Id) ->
    maps:from_list(
      exchange_raw_detail_stats_data(Ranges, Id) ++
      [raw_message_data(exchange_stats_publish_out,
                        pick_range(fine_stats, Ranges), Id),
       raw_message_data(exchange_stats_publish_in,
                        pick_range(fine_stats, Ranges), Id)]).

vhost_data(Ranges, Id) ->
    maps:from_list([raw_message_data(vhost_stats_coarse_conn_stats,
                                     pick_range(coarse_conn_stats, Ranges), Id),
                    raw_message_data(vhost_msg_stats,
                                     pick_range(queue_msg_rates, Ranges), Id),
                    raw_message_data(vhost_stats_fine_stats,
                                     pick_range(fine_stats, Ranges), Id),
                    raw_message_data(vhost_stats_deliver_stats,
                                     pick_range(deliver_get, Ranges), Id)]).

node_data(Ranges, Id) ->
    maps:from_list(
      [{mgmt_stats, mgmt_queue_length_stats(Id)}] ++
      [{node_node_metrics, node_node_metrics()}] ++
      node_raw_detail_stats_data(Ranges, Id) ++
      [raw_message_data(node_coarse_stats,
                        pick_range(coarse_node_stats, Ranges), Id),
       raw_message_data(node_persister_stats,
                        pick_range(coarse_node_stats, Ranges), Id),
       {node_stats, lookup_element(node_stats, Id)}] ++
      node_connection_churn_rates_data(Ranges, Id)).

overview_data(_Pid, User, Ranges, VHosts) ->
    Raw = [raw_all_message_data(vhost_msg_stats, pick_range(queue_msg_counts, Ranges), VHosts),
           raw_all_message_data(vhost_stats_fine_stats, pick_range(fine_stats, Ranges), VHosts),
           raw_all_message_data(vhost_msg_rates, pick_range(queue_msg_rates, Ranges), VHosts),
           raw_all_message_data(vhost_stats_deliver_stats, pick_range(deliver_get, Ranges), VHosts),
           raw_message_data(connection_churn_rates, pick_range(queue_msg_rates, Ranges), node())],
    maps:from_list(Raw ++
                   [{connections_count, count_created_stats(connection_created_stats, User)},
                    {channels_count, count_created_stats(channel_created_stats, User)},
                    {consumers_count, ets:info(consumer_stats, size)}]).

consumer_data(_Pid, VHost) ->
    maps:from_list(
    [{C, augment_msg_stats(augment_consumer(C))}
     || C <- consumers_by_vhost(VHost)]).

all_connection_data(_Pid, Ids, Ranges) ->
    maps:from_list([{Id, connection_data(Ranges, Id)} || Id <- Ids]).

all_exchange_data(_Pid, Ids, Ranges) ->
    maps:from_list([{Id, exchange_data(Ranges, Id)} || Id <- Ids]).

all_vhost_data(_Pid, Ids, Ranges) ->
    maps:from_list([{Id, vhost_data(Ranges, Id)} || Id <- Ids]).

all_node_data(_Pid, Ids, Ranges) ->
    maps:from_list([{Id, node_data(Ranges, Id)} || Id <- Ids]).

channel_raw_message_data(Ranges, Id) ->
    [raw_message_data(channel_stats_fine_stats, pick_range(fine_stats, Ranges), Id),
     raw_message_data(channel_stats_deliver_stats, pick_range(deliver_get, Ranges), Id),
     raw_message_data(channel_process_stats, pick_range(process_stats, Ranges), Id)].

queue_raw_message_data(Ranges, Id) ->
    [raw_message_data(queue_stats_publish, pick_range(fine_stats, Ranges), Id),
     raw_message_data(queue_stats_deliver_stats, pick_range(deliver_get, Ranges), Id),
     raw_message_data(queue_process_stats, pick_range(process_stats, Ranges), Id),
     raw_message_data(queue_msg_stats, pick_range(queue_msg_counts, Ranges), Id)].

queue_raw_deliver_stats_data(Ranges, Id) ->
     [raw_message_data2(channel_queue_stats_deliver_stats,
                        pick_range(deliver_get, Ranges), Key)
      || Key <- get_table_keys(channel_queue_stats_deliver_stats, second(Id))] ++
     [raw_message_data2(queue_exchange_stats_publish,
                        pick_range(fine_stats, Ranges), Key)
      || Key <- get_table_keys(queue_exchange_stats_publish, first(Id))].

node_raw_detail_stats_data(Ranges, Id) ->
     [raw_message_data2(node_node_coarse_stats,
                        pick_range(coarse_node_node_stats, Ranges), Key)
      || Key <- get_table_keys(node_node_coarse_stats, first(Id))].

node_connection_churn_rates_data(Ranges, Id) ->
    [raw_message_data(connection_churn_rates,
                      pick_range(churn_rates, Ranges), Id)].

exchange_raw_detail_stats_data(Ranges, Id) ->
     [raw_message_data2(channel_exchange_stats_fine_stats,
                        pick_range(fine_stats, Ranges), Key)
      || Key <- get_table_keys(channel_exchange_stats_fine_stats, second(Id))] ++
     [raw_message_data2(queue_exchange_stats_publish,
                        pick_range(fine_stats, Ranges), Key)
      || Key <- get_table_keys(queue_exchange_stats_publish, second(Id))].

channel_raw_detail_stats_data(Ranges, Id) ->
     [raw_message_data2(channel_exchange_stats_fine_stats,
                        pick_range(fine_stats, Ranges), Key)
      || Key <- get_table_keys(channel_exchange_stats_fine_stats, first(Id))] ++
     [raw_message_data2(channel_queue_stats_deliver_stats,
                        pick_range(fine_stats, Ranges), Key)
      || Key <- get_table_keys(channel_queue_stats_deliver_stats, first(Id))].

raw_message_data2(Table, no_range, Id) ->
    SmallSample = lookup_smaller_sample(Table, Id),
    {{Table, Id}, {SmallSample, not_found}};
raw_message_data2(Table, Range, Id) ->
    SmallSample = lookup_smaller_sample(Table, Id),
    Samples = lookup_samples(Table, Id, Range),
    {{Table, Id}, {SmallSample, Samples}}.

detail_queue_data(Ranges, Id) ->
    maps:from_list(queue_raw_message_data(Ranges, Id) ++
                   queue_raw_deliver_stats_data(Ranges, Id) ++
                   [{queue_stats, lookup_element(queue_stats, Id)},
                    {consumer_stats, get_queue_consumer_stats(Id)}]).

list_queue_data(Ranges, Id) ->
    maps:from_list(queue_raw_message_data(Ranges, Id) ++
                   queue_raw_deliver_stats_data(Ranges, Id) ++
                   [{queue_stats, lookup_element(queue_stats, Id)}]).

detail_channel_data(Ranges, Id) ->
    maps:from_list(channel_raw_message_data(Ranges, Id) ++
                   channel_raw_detail_stats_data(Ranges, Id) ++
                   [{channel_stats, lookup_element(channel_stats, Id)},
                    {consumer_stats, get_consumer_stats(Id)}]).

list_channel_data(Ranges, Id) ->
    maps:from_list(channel_raw_message_data(Ranges, Id) ++
                   channel_raw_detail_stats_data(Ranges, Id) ++
                   [{channel_stats, lookup_element(channel_stats, Id)}]).

-spec raw_message_data(atom(), maybe_range(), any()) ->
    {atom(), {maybe_slide(), maybe_slide()}}.
raw_message_data(Table, no_range, Id) ->
    SmallSample = lookup_smaller_sample(Table, Id),
    {Table, {SmallSample, not_found}};
raw_message_data(Table, Range, Id) ->
    SmallSample = lookup_smaller_sample(Table, Id),
    Samples = lookup_samples(Table, Id, Range),
    {Table, {SmallSample, Samples}}.

raw_all_message_data(Table, Range, VHosts) ->
    SmallSample = lookup_all(Table, VHosts, select_smaller_sample(Table)),
    RangeSample = case Range of
                      no_range -> not_found;
                      _ ->
                          lookup_all(Table, VHosts, select_range_sample(Table,
                                                                        Range))
                  end,
    {Table, {SmallSample, RangeSample}}.

get_queue_consumer_stats(Id) ->
    Consumers = ets:select(consumer_stats, match_queue_consumer_spec(Id)),
    [augment_consumer(C) || C <- Consumers].

get_consumer_stats(Id) ->
    Consumers = ets:select(consumer_stats, match_consumer_spec(Id)),
    [augment_consumer(C) || C <- Consumers].

count_created_stats(Type, all) ->
    ets:info(Type, size);
count_created_stats(Type, User) ->
    length(filter_user(created_stats(Type), User)).

augment_consumer({{Q, Ch, CTag}, Props}) ->
    [{queue, format_resource(Q)},
     {channel_details, augment_channel_pid(Ch)},
     {channel_pid, Ch},
     {consumer_tag, CTag} | Props].

consumers_by_vhost(VHost) ->
    ets:select(consumer_stats,
               [{{{#resource{virtual_host = '$1', _ = '_'}, '_', '_'}, '_'},
                 [{'orelse', {'==', 'all', VHost}, {'==', VHost, '$1'}}],
                 ['$_']}]).

augment_msg_stats(Props) ->
    augment_details(Props, []) ++ Props.

augment_details([{_, none} | T], Acc) ->
    augment_details(T, Acc);
augment_details([{_, unknown} | T], Acc) ->
    augment_details(T, Acc);
augment_details([{connection, Value} | T], Acc) ->
    augment_details(T, [{connection_details, augment_connection_pid(Value)} | Acc]);
augment_details([{channel, Value} | T], Acc) ->
    augment_details(T, [{channel_details, augment_channel_pid(Value)} | Acc]);
augment_details([{owner_pid, Value} | T], Acc) ->
    augment_details(T, [{owner_pid_details, augment_connection_pid(Value)} | Acc]);
augment_details([_ | T], Acc) ->
    augment_details(T, Acc);
augment_details([], Acc) ->
    Acc.

augment_channel_pids(_Pid, ChPids) ->
    lists:map(fun (ChPid) -> augment_channel_pid(ChPid) end, ChPids).

augment_channel_pid(Pid) ->
    Ch = lookup_channel_with_fallback_to_connection(Pid),
    Conn = lookup_element(connection_created_stats, pget(connection, Ch), 3),
    case Conn of
    [] -> %% If the connection has just been opened, we might not yet have the data
        [];
    _ ->
        [{name,            pget(name,   Ch)},
         {pid,             pget(pid,    Ch)},
         {number,          pget(number, Ch)},
         {user,            pget(user,   Ch)},
         {connection_name, pget(name,         Conn)},
         {peer_port,       pget(peer_port,    Conn)},
         {peer_host,       pget(peer_host,    Conn)}]
    end.

lookup_channel_with_fallback_to_connection(ChannelOrConnectionPid) ->
    % stream consumers report a stream connection PID for their channel PID,
    % so we adapt to this here
    case lookup_element(channel_created_stats, ChannelOrConnectionPid, 3) of
        [] ->
            case lookup_element(connection_created_stats, ChannelOrConnectionPid, 3) of
                [] ->
                    % not a channel and not a connection, not much we can do here
                    [{pid, ChannelOrConnectionPid}];
                Conn ->
                    [{name, <<"">>},
                     {pid, ChannelOrConnectionPid},
                     {number, 0},
                     {user, pget(user, Conn)},
                     {connection, ChannelOrConnectionPid}]
            end;
        Ch ->
            Ch
    end.

augment_connection_pid(Pid) ->
    Conn = lookup_element(connection_created_stats, Pid, 3),
    case Conn of
    [] -> %% If the connection has just been opened, we might not yet have the data
        [];
    _ ->
        [{name,         pget(name,         Conn)},
         {peer_port,    pget(peer_port,    Conn)},
         {peer_host,    pget(peer_host,    Conn)}]
    end.

augmented_created_stats(_Pid, Key, Type) ->
    case created_stats(Key, Type) of
        not_found -> not_found;
        S -> augment_msg_stats(S)
    end.

augmented_created_stats(_Pid, Type) ->
    [ augment_msg_stats(S) || S <- created_stats(Type) ].

match_consumer_spec(Id) ->
    [{{{'_', '$1', '_'}, '_'}, [{'==', Id, '$1'}], ['$_']}].

match_queue_consumer_spec(Id) ->
    [{{{'$1', '_', '_'}, '_'}, [{'==', {Id}, '$1'}], ['$_']}].

lookup_element(Table, Key) -> lookup_element(Table, Key, 2).

lookup_element(Table, Key, Pos) ->
    try ets:lookup_element(Table, Key, Pos)
    catch error:badarg -> []
    end.

-spec lookup_smaller_sample(atom(), any()) -> maybe_slide().
lookup_smaller_sample(Table, Id) ->
    case ets:lookup(Table, {Id, select_smaller_sample(Table)}) of
        [] ->
            not_found;
        [{_, Slide}] ->
            Slide1 = exometer_slide:optimize(Slide),
            maybe_convert_for_compatibility(Table, Slide1)
    end.

-spec lookup_samples(atom(), any(), #range{}) -> maybe_slide().
lookup_samples(Table, Id, Range) ->
    case ets:lookup(Table, {Id, select_range_sample(Table, Range)}) of
        [] ->
            not_found;
        [{_, Slide}] ->
            Slide1 = exometer_slide:optimize(Slide),
            maybe_convert_for_compatibility(Table, Slide1)
    end.

lookup_all(Table, Ids, SecondKey) ->
    Slides = lists:foldl(fun(Id, Acc) ->
                                 case ets:lookup(Table, {Id, SecondKey}) of
                                     [] ->
                                         Acc;
                                     [{_, Slide}] ->
                                         [Slide | Acc]
                                 end
                         end, [], Ids),
    case Slides of
        [] ->
            not_found;
        _ ->
            Slide = exometer_slide:sum(Slides, empty(Table, 0)),
            maybe_convert_for_compatibility(Table, Slide)
    end.

maybe_convert_for_compatibility(Table, Slide)
  when Table =:= channel_stats_fine_stats orelse
       Table =:= channel_exchange_stats_fine_stats orelse
       Table =:= vhost_stats_fine_stats ->
     ConversionNeeded = rabbit_feature_flags:is_disabled(
                          drop_unroutable_metric),
     case ConversionNeeded of
         false ->
             Slide;
         true ->
             %% drop_drop because the metric is named "drop_unroutable"
             rabbit_mgmt_data_compat:drop_drop_unroutable_metric(Slide)
     end;
maybe_convert_for_compatibility(Table, Slide)
  when Table =:= channel_queue_stats_deliver_stats orelse
       Table =:= channel_stats_deliver_stats orelse
       Table =:= queue_stats_deliver_stats orelse
       Table =:= vhost_stats_deliver_stats ->
    ConversionNeeded = rabbit_feature_flags:is_disabled(
                         empty_basic_get_metric),
    case ConversionNeeded of
        false ->
            Slide;
        true ->
            rabbit_mgmt_data_compat:drop_get_empty_queue_metric(Slide)
    end;
maybe_convert_for_compatibility(_, Slide) ->
    Slide.

get_table_keys(Table, Id0) ->
    ets:select(Table, match_spec_keys(Id0)).

match_spec_keys(Id) ->
    MatchCondition = to_match_condition(Id),
    MatchHead = {{{'$1', '$2'}, '_'}, '_'},
    [{MatchHead, [MatchCondition], [{{'$1', '$2'}}]}].

to_match_condition({'_', Id1}) when is_tuple(Id1) ->
    {'==', {Id1}, '$2'};
to_match_condition({'_', Id1}) ->
    {'==', Id1, '$2'};
to_match_condition({Id0, '_'}) when is_tuple(Id0) ->
    {'==', {Id0}, '$1'};
to_match_condition({Id0, '_'}) ->
    {'==', Id0, '$1'}.

mgmt_queue_length_stats(Id) when Id =:= node() ->
    GCsQueueLengths = lists:map(fun (T) ->
        case whereis(rabbit_mgmt_metrics_gc:name(T)) of
            P when is_pid(P) ->
                {message_queue_len, Len} =
                    erlang:process_info(P, message_queue_len),
                    {T, Len};
            _ -> {T, 0}
        end
    end,
    ?GC_EVENTS),
    [{metrics_gc_queue_length, GCsQueueLengths}];
mgmt_queue_length_stats(_Id) ->
    % if it isn't for the current node just return an empty list
    [].

node_node_metrics() ->
    maps:from_list(ets:tab2list(node_node_metrics)).

select_range_sample(Table, #range{first = First, last = Last}) ->
    Range = Last - First,
    Policies = rabbit_mgmt_agent_config:get_env(sample_retention_policies),
    Policy = retention_policy(Table),
    [T | _] = TablePolicies = lists:sort(proplists:get_value(Policy, Policies)),
    {_, Sample} = select_smallest_above(T, TablePolicies, Range),
    Sample.

select_smaller_sample(Table) ->
    Policies = rabbit_mgmt_agent_config:get_env(sample_retention_policies),
    Policy = retention_policy(Table),
    TablePolicies = proplists:get_value(Policy, Policies),
    [V | _] = lists:sort([I || {_, I} <- TablePolicies]),
    V.

select_smallest_above(V, [], _) ->
    V;
select_smallest_above(_, [{H, _} = S | _T], Interval) when (H * 1000) > Interval ->
    S;
select_smallest_above(_, [H | T], Interval) ->
    select_smallest_above(H, T, Interval).

pick_range(queue_msg_counts, {RangeL, _RangeM, _RangeD, _RangeN}) ->
    RangeL;
pick_range(K, {_RangeL, RangeM, _RangeD, _RangeN}) when K == fine_stats;
                                                        K == deliver_get;
                                                        K == queue_msg_rates ->
    RangeM;
pick_range(K, {_RangeL, _RangeM, RangeD, _RangeN}) when K == coarse_conn_stats;
                                                        K == process_stats ->
    RangeD;
pick_range(K, {_RangeL, _RangeM, _RangeD, RangeN})
  when K == coarse_node_stats;
       K == coarse_node_node_stats;
       K == churn_rates ->
    RangeN.

first(Id)  ->
    {Id, '_'}.

second(Id) ->
    {'_', Id}.

empty(Type, V) when Type =:= connection_stats_coarse_conn_stats;
            Type =:= queue_msg_stats;
            Type =:= vhost_msg_stats ->
    {V, V, V};
empty(Type, V) when Type =:= channel_stats_fine_stats;
            Type =:= channel_exchange_stats_fine_stats;
            Type =:= vhost_stats_fine_stats ->
    {V, V, V, V};
empty(Type, V) when Type =:= channel_queue_stats_deliver_stats;
            Type =:= queue_stats_deliver_stats;
            Type =:= vhost_stats_deliver_stats;
            Type =:= channel_stats_deliver_stats ->
    {V, V, V, V, V, V, V, V};
empty(Type, V) when Type =:= channel_process_stats;
            Type =:= queue_process_stats;
            Type =:= queue_stats_publish;
            Type =:= queue_exchange_stats_publish;
            Type =:= exchange_stats_publish_out;
            Type =:= exchange_stats_publish_in ->
    {V};
empty(node_coarse_stats, V) ->
    {V, V, V, V, V, V, V, V};
empty(node_persister_stats, V) ->
    {V, V, V, V, V, V, V, V, V, V, V, V, V, V, V, V, V, V, V, V};
empty(Type, V) when Type =:= node_node_coarse_stats;
            Type =:= vhost_stats_coarse_conn_stats;
            Type =:= queue_msg_rates;
            Type =:= vhost_msg_rates ->
    {V, V};
empty(connection_churn_rates, V) ->
    {V, V, V, V, V, V, V}.

retention_policy(connection_stats_coarse_conn_stats) ->
    basic;
retention_policy(channel_stats_fine_stats) ->
    basic;
retention_policy(channel_queue_stats_deliver_stats) ->
    detailed;
retention_policy(channel_exchange_stats_fine_stats) ->
    detailed;
retention_policy(channel_process_stats) ->
    basic;
retention_policy(vhost_stats_fine_stats) ->
    global;
retention_policy(vhost_stats_deliver_stats) ->
    global;
retention_policy(vhost_stats_coarse_conn_stats) ->
    global;
retention_policy(vhost_msg_rates) ->
    global;
retention_policy(channel_stats_deliver_stats) ->
    basic;
retention_policy(queue_stats_deliver_stats) ->
    basic;
retention_policy(queue_stats_publish) ->
    basic;
retention_policy(queue_exchange_stats_publish) ->
    basic;
retention_policy(exchange_stats_publish_out) ->
    basic;
retention_policy(exchange_stats_publish_in) ->
    basic;
retention_policy(queue_process_stats) ->
    basic;
retention_policy(queue_msg_stats) ->
    basic;
retention_policy(queue_msg_rates) ->
    basic;
retention_policy(vhost_msg_stats) ->
    global;
retention_policy(node_coarse_stats) ->
    global;
retention_policy(node_persister_stats) ->
    global;
retention_policy(node_node_coarse_stats) ->
    global;
retention_policy(connection_churn_rates) ->
    global.

format_resource(unknown) -> unknown;
format_resource(Res)     -> format_resource(name, Res).

format_resource(_, unknown) ->
    unknown;
format_resource(NameAs, #resource{name = Name, virtual_host = VHost}) ->
    [{NameAs, Name}, {vhost, VHost}].

filter_user(List, #user{username = Username, tags = Tags}) ->
    case is_monitor(Tags) of
        true  -> List;
        false -> [I || I <- List, pget(user, I) == Username]
    end.

is_monitor(T)     -> intersects(T, [administrator, monitoring]).
intersects(A, B) -> lists:any(fun(I) -> lists:member(I, B) end, A).
