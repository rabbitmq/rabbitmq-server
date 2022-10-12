-module(util).

-export([all_connection_pids/1]).

all_connection_pids(Config) ->
    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Result = erpc:multicall(Nodes, rabbit_mqtt, local_connection_pids, [], 5000),
    lists:foldl(fun({ok, Pids}, Acc) ->
                        Pids ++ Acc;
                   (_, Acc) ->
                        Acc
                end, [], Result).
