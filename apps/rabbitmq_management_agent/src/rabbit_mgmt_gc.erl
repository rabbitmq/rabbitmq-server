%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(rabbit_mgmt_gc).

-include_lib("rabbit_common/include/rabbit.hrl").

-record(state, {timer,
                interval
               }).

-spec start_link() -> rabbit_types:ok_pid_or_error().

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_) ->
    Interval = rabbit_misc:get_env(rabbitmq_management_agent, metrics_gc_interval, 120000),
    {ok, start_timer(#state{interval = Interval})}.

handle_call(test, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(start_gc, State) ->
    gc_connections(),
    gc_vhosts(),
    gc_channels(),
    gc_queues(),
    gc_exchanges(),
    gc_nodes(),
    {noreply, start_timer(State)}.

terminate(_Reason, #state{timer = TRef}) ->
    _ = erlang:cancel_timer(TRef),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

start_timer(#state{interval = Interval} = St) ->
    TRef = erlang:send_after(Interval, self(), start_gc),
    St#state{timer = TRef}.

gc_connections() ->
    gc_process(connection_stats_coarse_conn_stats),
    gc_process(connection_created_stats),
    gc_process(connection_stats).

gc_vhosts() ->
    VHosts = rabbit_vhost:list(),
    GbSet = gb_sets:from_list(VHosts),
    gc_entity(vhost_stats_coarse_conn_stats, GbSet),
    gc_entity(vhost_stats_fine_stats, GbSet),
    gc_entity(vhost_msg_stats, GbSet),
    gc_entity(vhost_msg_rates, GbSet),
    gc_entity(vhost_stats_deliver_stats, GbSet).

gc_channels() ->
    gc_process(channel_created_stats),
    gc_process(channel_stats),
    gc_process(channel_stats_fine_stats),
    gc_process(channel_process_stats),
    gc_process(channel_stats_deliver_stats),
    ok.

gc_queues() ->
    Queues = rabbit_amqqueue:list_names(),
    GbSet = gb_sets:from_list(Queues),
    LocalQueues = rabbit_amqqueue:list_local_names(),
    LocalGbSet = gb_sets:from_list(LocalQueues),
    gc_entity(queue_stats_publish, GbSet),
    gc_entity(queue_stats, LocalGbSet),
    gc_entity(queue_msg_stats, LocalGbSet),
    gc_entity(queue_process_stats, LocalGbSet),
    gc_entity(queue_msg_rates, LocalGbSet),
    gc_entity(queue_stats_deliver_stats, GbSet),
    gc_process_and_entity(channel_queue_stats_deliver_stats_queue_index, GbSet),
    gc_process_and_entity(consumer_stats_queue_index, GbSet),
    gc_process_and_entity(consumer_stats_channel_index, GbSet),
    gc_process_and_entity(consumer_stats, GbSet),
    gc_process_and_entity(channel_exchange_stats_fine_stats_channel_index, GbSet),
    gc_process_and_entity(channel_queue_stats_deliver_stats, GbSet),
    gc_process_and_entity(channel_queue_stats_deliver_stats_channel_index, GbSet),
    ExchangeGbSet = gb_sets:from_list(rabbit_exchange:list_names()),
    gc_entities(queue_exchange_stats_publish, GbSet, ExchangeGbSet),
    gc_entities(queue_exchange_stats_publish_queue_index, GbSet, ExchangeGbSet),
    gc_entities(queue_exchange_stats_publish_exchange_index, GbSet, ExchangeGbSet).

gc_exchanges() ->
    Exchanges = rabbit_exchange:list_names(),
    GbSet = gb_sets:from_list(Exchanges),
    gc_entity(exchange_stats_publish_in, GbSet),
    gc_entity(exchange_stats_publish_out, GbSet),
    gc_entity(channel_exchange_stats_fine_stats_exchange_index, GbSet),
    gc_process_and_entity(channel_exchange_stats_fine_stats, GbSet).

gc_nodes() ->
    Nodes = rabbit_mnesia:cluster_nodes(all),
    GbSet = gb_sets:from_list(Nodes),
    gc_entity(node_stats, GbSet),
    gc_entity(node_coarse_stats, GbSet),
    gc_entity(node_persister_stats, GbSet),
    gc_entity(node_node_coarse_stats_node_index, GbSet),
    gc_entity(node_node_stats, GbSet),
    gc_entity(node_node_coarse_stats, GbSet).

gc_process(Table) ->
    ets:foldl(fun({{Pid, _} = Key, _}, none) ->
                      gc_process(Pid, Table, Key);
                 ({Pid = Key, _}, none) ->
                      gc_process(Pid, Table, Key);
                 ({Pid = Key, _, _}, none) ->
                      gc_process(Pid, Table, Key);
                 ({{Pid, _} = Key, _, _, _, _}, none) ->
                      gc_process(Pid, Table, Key)
              end, none, Table).

gc_process(Pid, Table, Key) ->
    case rabbit_misc:is_process_alive(Pid) of
        true ->
            none;
        false ->
            ets:delete(Table, Key),
            none
    end.

gc_entity(Table, GbSet) ->
    ets:foldl(fun({{_, Id} = Key, _}, none) when Table == node_node_stats ->
                      gc_entity(Id, Table, Key, GbSet);
                 ({{{_, Id}, _} = Key, _}, none) when Table == node_node_coarse_stats ->
                      gc_entity(Id, Table, Key, GbSet);
                  ({{Id, _} = Key, _}, none) ->
                      gc_entity(Id, Table, Key, GbSet);
                 ({Id = Key, _}, none) ->
                      gc_entity(Id, Table, Key, GbSet);
                 ({{Id, _} = Key, _}, none) ->
                      gc_entity(Id, Table, Key, GbSet)
              end, none, Table).

gc_entity(Id, Table, Key, GbSet) ->
    case gb_sets:is_member(Id, GbSet) of
        true ->
            none;
        false ->
            ets:delete(Table, Key),
            none
    end.

gc_process_and_entity(Table, GbSet) ->
    ets:foldl(fun({{Id, Pid, _} = Key, _}, none) when Table == consumer_stats ->
                      gc_process_and_entity(Id, Pid, Table, Key, GbSet);
                 ({Id = Key, {_, Pid, _}} = Object, none)
                    when Table == consumer_stats_queue_index ->
                      gc_object(Pid, Table, Object),
                      gc_entity(Id, Table, Key, GbSet);
                 ({Pid = Key, {Id, _, _}} = Object, none)
                    when Table == consumer_stats_channel_index ->
                      gc_object(Id, Table, Object, GbSet),
                      gc_process(Pid, Table, Key);
                 ({Id = Key, {{Pid, _}, _}} = Object, none)
                    when Table == channel_exchange_stats_fine_stats_exchange_index;
                         Table == channel_queue_stats_deliver_stats_queue_index ->
                      gc_object(Pid, Table, Object),
                      gc_entity(Id, Table, Key, GbSet);
                 ({Pid = Key, {{_, Id}, _}} = Object, none)
                    when Table == channel_exchange_stats_fine_stats_channel_index;
                         Table == channel_queue_stats_deliver_stats_channel_index ->
                      gc_object(Id, Table, Object, GbSet),
                      gc_process(Pid, Table, Key);
                 ({{{Pid, Id}, _} = Key, _}, none)
                    when Table == channel_queue_stats_deliver_stats;
                         Table == channel_exchange_stats_fine_stats ->
                      gc_process_and_entity(Id, Pid, Table, Key, GbSet);
                 ({{{Pid, Id}, _} = Key, _, _, _, _, _, _, _, _}, none) ->
                      gc_process_and_entity(Id, Pid, Table, Key, GbSet);
                 ({{{Pid, Id}, _} = Key, _, _, _, _}, none) ->
                      gc_process_and_entity(Id, Pid, Table, Key, GbSet)
              end, none, Table).

gc_process_and_entity(Id, Pid, Table, Key, GbSet) ->
    case rabbit_misc:is_process_alive(Pid) andalso gb_sets:is_member(Id, GbSet) of
        true ->
            none;
        false ->
            ets:delete(Table, Key),
            none
    end.

gc_object(Pid, Table, Object) ->
    case rabbit_misc:is_process_alive(Pid) of
        true ->
            none;
        false ->
            ets:delete_object(Table, Object),
            none
    end.

gc_object(Id, Table, Object, GbSet) ->
    case gb_sets:is_member(Id, GbSet) of
        true ->
            none;
        false ->
            ets:delete_object(Table, Object),
            none
    end.

gc_entities(Table, QueueGbSet, ExchangeGbSet) ->
    ets:foldl(fun({{{Q, X}, _} = Key, _}, none)
                    when Table == queue_exchange_stats_publish ->
                      gc_entity(Q, Table, Key, QueueGbSet),
                      gc_entity(X, Table, Key, ExchangeGbSet);
                 ({Q, {{_, X}, _}} = Object, none)
                    when Table == queue_exchange_stats_publish_queue_index ->
                      gc_object(X, Table, Object, ExchangeGbSet),
                      gc_entity(Q, Table, Q, QueueGbSet);
                 ({X, {{Q, _}, _}} = Object, none)
                    when Table == queue_exchange_stats_publish_exchange_index ->
                      gc_object(Q, Table, Object, QueueGbSet),
                      gc_entity(X, Table, X, ExchangeGbSet)
              end, none, Table).
