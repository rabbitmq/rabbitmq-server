%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(rabbit_core_metrics_gc).

-record(state, {timer,
                interval
               }).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-spec start_link() -> rabbit_types:ok_pid_or_error().

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_) ->
    Interval = rabbit_misc:get_env(rabbit, core_metrics_gc_interval, 120000),
    {ok, start_timer(#state{interval = Interval})}.

handle_call(test, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(start_gc, State) ->
    gc_connections(),
    gc_channels(),
    gc_queues(),
    gc_exchanges(),
    gc_nodes(),
    gc_gen_server2(),
    gc_auth_attempts(),
    {noreply, start_timer(State)}.

terminate(_Reason, #state{timer = TRef}) ->
    erlang:cancel_timer(TRef),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

start_timer(#state{interval = Interval} = St) ->
    TRef = erlang:send_after(Interval, self(), start_gc),
    St#state{timer = TRef}.

gc_connections() ->
    gc_process(connection_created),
    gc_process(connection_metrics),
    gc_process(connection_coarse_metrics).

gc_channels() ->
    gc_process(channel_created),
    gc_process(channel_metrics),
    gc_process(channel_process_metrics),
    ok.

gc_queues() ->
    gc_local_queues(),
    gc_global_queues().

gc_local_queues() ->
    Queues = rabbit_amqqueue:list_local_names(),
    QueuesDown = rabbit_amqqueue:list_local_names_down(),
    GbSet = gb_sets:from_list(Queues),
    GbSetDown = gb_sets:from_list(QueuesDown),
    gc_queue_metrics(GbSet, GbSetDown),
    gc_entity(queue_coarse_metrics, GbSet),
    Followers = gb_sets:from_list([amqqueue:get_name(Q) || Q <- rabbit_amqqueue:list_local_followers() ]),
    gc_leader_data(Followers).

gc_leader_data(Followers) ->
    ets:foldl(fun({Id, _, _, _, _}, none) ->
                      gc_leader_data(Id, queue_coarse_metrics, Followers)
              end, none, queue_coarse_metrics).

gc_leader_data(Id, Table, GbSet) ->
    case gb_sets:is_member(Id, GbSet) of
        true ->
            ets:delete(Table, Id),
            none;
        false ->
            none
    end.

gc_global_queues() ->
    GbSet = gb_sets:from_list(rabbit_amqqueue:list_names()),
    gc_process_and_entity(channel_queue_metrics, GbSet),
    gc_process_and_entity(consumer_created, GbSet),
    ExchangeGbSet = gb_sets:from_list(rabbit_exchange:list_names()),
    gc_process_and_entities(channel_queue_exchange_metrics, GbSet, ExchangeGbSet).

gc_exchanges() ->
    Exchanges = rabbit_exchange:list_names(),
    GbSet = gb_sets:from_list(Exchanges),
    gc_process_and_entity(channel_exchange_metrics, GbSet).

gc_nodes() ->
    Nodes = rabbit_mnesia:cluster_nodes(all),
    GbSet = gb_sets:from_list(Nodes),
    gc_entity(node_node_metrics, GbSet).

gc_gen_server2() ->
    gc_process(gen_server2_metrics).

gc_process(Table) ->
    ets:foldl(fun({Pid = Key, _}, none) ->
                      gc_process(Pid, Table, Key);
                 ({Pid = Key, _, _, _, _}, none) ->
                      gc_process(Pid, Table, Key);
                 ({Pid = Key, _, _, _}, none) ->
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

gc_queue_metrics(GbSet, GbSetDown) ->
    Table = queue_metrics,
    ets:foldl(fun({Key, Props, Marker}, none) ->
                      case gb_sets:is_member(Key, GbSet) of
                          true ->
                              case gb_sets:is_member(Key, GbSetDown) of
                                  true ->
                                      ets:insert(Table, {Key, [{state, down} | lists:keydelete(state, 1, Props)], Marker}),
                                      none;
                                  false ->
                                      none
                              end;
                          false ->
                              ets:delete(Table, Key),
                              none
                      end
              end, none, Table).

gc_entity(Table, GbSet) ->
    ets:foldl(fun({{_, Id} = Key, _}, none) ->
                      gc_entity(Id, Table, Key, GbSet);
                 ({Id = Key, _}, none) ->
                      gc_entity(Id, Table, Key, GbSet);
                 ({Id = Key, _, _}, none) ->
                      gc_entity(Id, Table, Key, GbSet);
                 ({Id = Key, _, _, _, _}, none) ->
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
    ets:foldl(fun({{Pid, Id} = Key, _, _, _, _, _, _, _, _}, none)
                  when Table == channel_queue_metrics ->
                      gc_process_and_entity(Id, Pid, Table, Key, GbSet);
                 ({{Pid, Id} = Key, _, _, _, _, _}, none)
                    when Table == channel_exchange_metrics ->
                      gc_process_and_entity(Id, Pid, Table, Key, GbSet);
                 ({{Id, Pid, _} = Key, _, _, _, _, _, _}, none)
                    when Table == consumer_created ->
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

gc_process_and_entities(Table, QueueGbSet, ExchangeGbSet) ->
    ets:foldl(fun({{Pid, {Q, X}} = Key, _, _}, none) ->
                      gc_process(Pid, Table, Key),
                      gc_entity(Q, Table, Key, QueueGbSet),
                      gc_entity(X, Table, Key, ExchangeGbSet)
              end, none, Table).

gc_auth_attempts() ->
    ets:delete_all_objects(auth_attempt_detailed_metrics).
