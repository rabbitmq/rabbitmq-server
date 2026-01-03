%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(rabbit_mgmt_gc).

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
    {noreply, start_timer(State), hibernate}.

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
    Set = sets:from_list(VHosts, [{version, 2}]),
    gc_entity(vhost_stats_coarse_conn_stats, Set),
    gc_entity(vhost_stats_fine_stats, Set),
    gc_entity(vhost_msg_stats, Set),
    gc_entity(vhost_msg_rates, Set),
    gc_entity(vhost_stats_deliver_stats, Set).

gc_channels() ->
    gc_process(channel_created_stats),
    gc_process(channel_stats),
    gc_process(channel_stats_fine_stats),
    gc_process(channel_process_stats),
    gc_process(channel_stats_deliver_stats),
    ok.

gc_queues() ->
    Queues = rabbit_amqqueue:list_names(),
    Set = sets:from_list(Queues, [{version, 2}]),
    LocalQueues = rabbit_amqqueue:list_local_names(),
    LocalSet = sets:from_list(LocalQueues, [{version, 2}]),
    gc_entity(queue_stats_publish, Set),
    gc_entity(queue_stats, LocalSet),
    gc_entity(queue_basic_stats, LocalSet),
    gc_entity(queue_msg_stats, LocalSet),
    gc_entity(queue_process_stats, LocalSet),
    gc_entity(queue_msg_rates, LocalSet),
    gc_entity(queue_stats_deliver_stats, Set),
    gc_process_and_entity(channel_queue_stats_deliver_stats_queue_index, Set),
    gc_process_and_entity(consumer_stats_queue_index, Set),
    gc_process_and_entity(consumer_stats_channel_index, Set),
    gc_process_and_entity(consumer_stats, Set),
    gc_process_and_entity(channel_exchange_stats_fine_stats_channel_index, Set),
    gc_process_and_entity(channel_queue_stats_deliver_stats, Set),
    gc_process_and_entity(channel_queue_stats_deliver_stats_channel_index, Set),
    ExchangeSet = sets:from_list(rabbit_exchange:list_names(), [{version, 2}]),
    gc_entities(queue_exchange_stats_publish, Set, ExchangeSet),
    gc_entities(queue_exchange_stats_publish_queue_index, Set, ExchangeSet),
    gc_entities(queue_exchange_stats_publish_exchange_index, Set, ExchangeSet).

gc_exchanges() ->
    Exchanges = rabbit_exchange:list_names(),
    Set = sets:from_list(Exchanges, [{version, 2}]),
    gc_entity(exchange_stats_publish_in, Set),
    gc_entity(exchange_stats_publish_out, Set),
    gc_entity(channel_exchange_stats_fine_stats_exchange_index, Set),
    gc_process_and_entity(channel_exchange_stats_fine_stats, Set).

gc_nodes() ->
    Nodes = rabbit_nodes:list_members(),
    Set = sets:from_list(Nodes, [{version, 2}]),
    gc_entity(node_stats, Set),
    gc_entity(node_coarse_stats, Set),
    gc_entity(node_persister_stats, Set),
    gc_entity(node_node_coarse_stats_node_index, Set),
    gc_entity(node_node_stats, Set),
    gc_entity(node_node_coarse_stats, Set).

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

gc_entity(Table, Set) ->
    ets:foldl(fun({{_, Id} = Key, _}, none) when Table == node_node_stats ->
                      gc_entity(Id, Table, Key, Set);
                 ({{{_, Id}, _} = Key, _}, none) when Table == node_node_coarse_stats ->
                      gc_entity(Id, Table, Key, Set);
                  ({{Id, _} = Key, _}, none) ->
                      gc_entity(Id, Table, Key, Set);
                 ({Id = Key, _}, none) ->
                      gc_entity(Id, Table, Key, Set);
                 ({{Id, _} = Key, _}, none) ->
                      gc_entity(Id, Table, Key, Set)
              end, none, Table).

gc_entity(Id, Table, Key, Set) ->
    case sets:is_element(Id, Set) of
        true ->
            none;
        false ->
            ets:delete(Table, Key),
            none
    end.

gc_process_and_entity(Table, Set) ->
    ets:foldl(fun({{Id, Pid, _} = Key, _}, none) when Table == consumer_stats ->
                      gc_process_and_entity(Id, Pid, Table, Key, Set);
                 ({Id = Key, {_, Pid, _}} = Object, none)
                    when Table == consumer_stats_queue_index ->
                      gc_object(Pid, Table, Object),
                      gc_entity(Id, Table, Key, Set);
                 ({Pid = Key, {Id, _, _}} = Object, none)
                    when Table == consumer_stats_channel_index ->
                      gc_object(Id, Table, Object, Set),
                      gc_process(Pid, Table, Key);
                 ({Id = Key, {{Pid, _}, _}} = Object, none)
                    when Table == channel_exchange_stats_fine_stats_exchange_index;
                         Table == channel_queue_stats_deliver_stats_queue_index ->
                      gc_object(Pid, Table, Object),
                      gc_entity(Id, Table, Key, Set);
                 ({Pid = Key, {{_, Id}, _}} = Object, none)
                    when Table == channel_exchange_stats_fine_stats_channel_index;
                         Table == channel_queue_stats_deliver_stats_channel_index ->
                      gc_object(Id, Table, Object, Set),
                      gc_process(Pid, Table, Key);
                 ({{{Pid, Id}, _} = Key, _}, none)
                    when Table == channel_queue_stats_deliver_stats;
                         Table == channel_exchange_stats_fine_stats ->
                      gc_process_and_entity(Id, Pid, Table, Key, Set);
                 ({{{Pid, Id}, _} = Key, _, _, _, _, _, _, _, _}, none) ->
                      gc_process_and_entity(Id, Pid, Table, Key, Set);
                 ({{{Pid, Id}, _} = Key, _, _, _, _}, none) ->
                      gc_process_and_entity(Id, Pid, Table, Key, Set)
              end, none, Table).

gc_process_and_entity(Id, Pid, Table, Key, Set) ->
    case rabbit_misc:is_process_alive(Pid) andalso sets:is_element(Id, Set) of
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

gc_object(Id, Table, Object, Set) ->
    case sets:is_element(Id, Set) of
        true ->
            none;
        false ->
            ets:delete_object(Table, Object),
            none
    end.

gc_entities(Table, QueueSet, ExchangeSet) ->
    ets:foldl(fun({{{Q, X}, _} = Key, _}, none)
                    when Table == queue_exchange_stats_publish ->
                      gc_entity(Q, Table, Key, QueueSet),
                      gc_entity(X, Table, Key, ExchangeSet);
                 ({Q, {{_, X}, _}} = Object, none)
                    when Table == queue_exchange_stats_publish_queue_index ->
                      gc_object(X, Table, Object, ExchangeSet),
                      gc_entity(Q, Table, Q, QueueSet);
                 ({X, {{Q, _}, _}} = Object, none)
                    when Table == queue_exchange_stats_publish_exchange_index ->
                      gc_object(Q, Table, Object, QueueSet),
                      gc_entity(X, Table, X, ExchangeSet)
              end, none, Table).
