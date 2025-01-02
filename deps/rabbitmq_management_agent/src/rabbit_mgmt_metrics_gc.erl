%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(rabbit_mgmt_metrics_gc).

-record(state, {basic_i,
                detailed_i,
                global_i}).

-include_lib("rabbit_common/include/rabbit.hrl").

-spec start_link(atom()) -> rabbit_types:ok_pid_or_error().

-export([name/1]).
-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(LARGE_CONSUMER_COUNT, 1000).

name(EventType) ->
    list_to_atom((atom_to_list(EventType) ++ "_metrics_gc")).

start_link(EventType) ->
    gen_server:start_link({local, name(EventType)}, ?MODULE, [], []).

init(_) ->
    Policies = rabbit_mgmt_agent_config:get_env(sample_retention_policies),
    {ok, #state{basic_i = intervals(basic, Policies),
                global_i = intervals(global, Policies),
                detailed_i = intervals(detailed, Policies)}}.

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast({event, #event{type  = connection_closed, props = Props}},
            State = #state{basic_i = BIntervals}) ->
    Pid = pget(pid, Props),
    remove_connection(Pid, BIntervals),
    {noreply, State};
handle_cast({event, #event{type  = channel_closed, props = Props}},
            State = #state{basic_i = BIntervals}) ->
    Pid = pget(pid, Props),
    ConsumerCount = pget(consumer_count, Props),
    remove_channel(Pid, ConsumerCount, BIntervals),
    {noreply, State};
handle_cast({event, #event{type  = consumer_deleted, props = Props}}, State) ->
    remove_consumer(Props),
    {noreply, State};
handle_cast({event, #event{type  = exchange_deleted, props = Props}},
            State = #state{basic_i = BIntervals}) ->
    Name = pget(name, Props),
    remove_exchange(Name, BIntervals),
    {noreply, State};
handle_cast({event, #event{type  = queue_deleted, props = Props}},
            State = #state{basic_i = BIntervals}) ->
    Name = pget(name, Props),
    remove_queue(Name, BIntervals),
    {noreply, State};
handle_cast({event, #event{type  = vhost_deleted, props = Props}},
            State = #state{global_i = GIntervals}) ->
    Name = pget(name, Props),
    remove_vhost(Name, GIntervals),
    {noreply, State};
handle_cast({event, #event{type  = node_node_deleted, props = Props}}, State) ->
    Name = pget(route, Props),
    remove_node_node(Name),
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

remove_connection(Id, BIntervals) ->
    ets:delete(connection_created_stats, Id),
    ets:delete(connection_stats, Id),
    delete_samples(connection_stats_coarse_conn_stats, Id, BIntervals),
    ok.

remove_channel(Id, ConsumerCount, BIntervals) ->
    ets:delete(channel_created_stats, Id),
    ets:delete(channel_stats, Id),
    delete_samples(channel_process_stats, Id, BIntervals),
    delete_samples(channel_stats_fine_stats, Id, BIntervals),
    delete_samples(channel_stats_deliver_stats, Id, BIntervals),
    index_delete(consumer_stats, {channel, ConsumerCount}, Id),
    index_delete(channel_exchange_stats_fine_stats, channel, Id),
    index_delete(channel_queue_stats_deliver_stats, channel, Id),
    ok.

remove_consumer(Props) ->
    Id = {pget(queue, Props), pget(channel, Props), pget(consumer_tag, Props)},
    ets:delete(consumer_stats, Id),
    cleanup_index(consumer_stats, Id),
    ok.

remove_exchange(Name, BIntervals) ->
    delete_samples(exchange_stats_publish_out, Name, BIntervals),
    delete_samples(exchange_stats_publish_in, Name, BIntervals),
    index_delete(queue_exchange_stats_publish, exchange, Name),
    index_delete(channel_exchange_stats_fine_stats, exchange, Name),
    ok.

remove_queue(Name, BIntervals) ->
    ets:delete(queue_stats, Name),
    ets:delete(queue_basic_stats, Name),
    delete_samples(queue_stats_publish, Name, BIntervals),
    delete_samples(queue_stats_deliver_stats, Name, BIntervals),
    delete_samples(queue_process_stats, Name, BIntervals),
    delete_samples(queue_msg_stats, Name, BIntervals),
    delete_samples(queue_msg_rates, Name, BIntervals),
    index_delete(channel_queue_stats_deliver_stats, queue, Name),
    index_delete(queue_exchange_stats_publish, queue, Name),
    index_delete(consumer_stats, queue, Name),

    ok.

remove_vhost(Name, GIntervals) ->
    delete_samples(vhost_stats_coarse_conn_stats, Name, GIntervals),
    delete_samples(vhost_stats_fine_stats, Name, GIntervals),
    delete_samples(vhost_stats_deliver_stats, Name, GIntervals),
    ok.

remove_node_node(Name) ->
    index_delete(node_node_coarse_stats, node, Name),
    ok.

intervals(Type, Policies) ->
    [I || {_, I} <- proplists:get_value(Type, Policies)].

delete_samples(Table, Id, Intervals) ->
    [ets:delete(Table, {Id, I}) || I <- Intervals],
    ok.

index_delete(consumer_stats = Table, {channel = Type, ConsumerCount}, Id) ->
    %% This uses two different deletion strategies depending on how many
    %% consumers a channel had. Most of the time there are many channels
    %% with a few (or even just one) consumers. For this common case, `ets:delete/2` is optimal
    %% since it avoids table scans.
    %%
    %% In the rather extreme scenario where only a handful of channels have a very large
    %% (e.g. tens of thousands) of consumers, `ets:match_delete/2` becomes a more efficient option.
    %%
    %% See rabbitmq-server/rabbitmq#10451, rabbitmq-server/rabbitmq#9356.
    case ConsumerCount > ?LARGE_CONSUMER_COUNT of
        true ->
            IndexTable = rabbit_mgmt_metrics_collector:index_table(Table, Type),
            MatchPattern = {'_', Id, '_'},
            %% Delete consumer_stats_queue_index
            ets:match_delete(consumer_stats_queue_index,
                             {'_', MatchPattern}),
            %% Delete consumer_stats
            ets:match_delete(consumer_stats,
                             {MatchPattern,'_'}),
            %% Delete consumer_stats_channel_index
            ets:delete(IndexTable, Id),
            ok;
        false ->
            index_delete(Table, Type, Id)
    end;
index_delete(Table, Type, Id) ->
    IndexTable = rabbit_mgmt_metrics_collector:index_table(Table, Type),
    Keys = ets:lookup(IndexTable, Id),
    [ begin
          ets:delete(Table, Key),
          cleanup_index(Table, Key)
      end
      || {_Index, Key} <- Keys ],
    ets:delete(IndexTable, Id),
    ok.

cleanup_index(consumer_stats, {Q, Ch, _} = Key) ->
    delete_index(consumer_stats, queue, {Q, Key}),
    delete_index(consumer_stats, channel, {Ch, Key}),
    ok;
cleanup_index(channel_exchange_stats_fine_stats, {{Ch, Ex}, _} = Key) ->
    delete_index(channel_exchange_stats_fine_stats, exchange, {Ex, Key}),
    delete_index(channel_exchange_stats_fine_stats, channel, {Ch, Key}),
    ok;
cleanup_index(channel_queue_stats_deliver_stats, {{Ch, Q}, _} = Key) ->
    delete_index(channel_queue_stats_deliver_stats, queue, {Q, Key}),
    delete_index(channel_queue_stats_deliver_stats, channel, {Ch, Key}),
    ok;
cleanup_index(queue_exchange_stats_publish, {{Q, Ex}, _} = Key) ->
    delete_index(queue_exchange_stats_publish, queue, {Q, Key}),
    delete_index(queue_exchange_stats_publish, exchange, {Ex, Key}),
    ok;
cleanup_index(node_node_coarse_stats, {{_, Node}, _} = Key) ->
    delete_index(node_node_coarse_stats, node, {Node, Key}),
    ok;
cleanup_index(_, _) -> ok.

delete_index(Table, Index, Obj) ->
    ets:delete_object(rabbit_mgmt_metrics_collector:index_table(Table, Index),
                      Obj).

pget(Key, List) -> rabbit_misc:pget(Key, List, unknown).
