-module(util).

-export([all_connection_pids/1,
         publish_qos1_timeout/4,
         sync_publish_result/3]).

all_connection_pids(Config) ->
    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Result = erpc:multicall(Nodes, rabbit_mqtt, local_connection_pids, [], 5000),
    lists:foldl(fun({ok, Pids}, Acc) ->
                        Pids ++ Acc;
                   (_, Acc) ->
                        Acc
                end, [], Result).

publish_qos1_timeout(Client, Topic, Payload, Timeout) ->
    Mref = erlang:monitor(process, Client),
    ok = emqtt:publish_async(Client, Topic, #{}, Payload, [{qos, 1}], infinity,
                             {fun ?MODULE:sync_publish_result/3, [self(), Mref]}),
    receive
        {Mref, Reply} ->
            erlang:demonitor(Mref, [flush]),
            Reply;
        {'DOWN', Mref, process, Client, Reason} ->
            ct:fail("client is down: ~tp", [Reason])
    after
        Timeout ->
            erlang:demonitor(Mref, [flush]),
            puback_timeout
    end.

sync_publish_result(Caller, Mref, Result) ->
    erlang:send(Caller, {Mref, Result}).
