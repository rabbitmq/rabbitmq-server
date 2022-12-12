-module(util).

-include("rabbit_mqtt.hrl").

-export([all_connection_pids/1,
         publish_qos1_timeout/4,
         sync_publish_result/3,
         get_global_counters/2,
         get_global_counters/3,
         get_global_counters/4,
         expect_publishes/2,
         connect/2,
         connect/3,
         connect/4,
         connect_to_node/3]).

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

expect_publishes(_Topic, []) ->
    ok;
expect_publishes(Topic, [Payload|Rest]) ->
    receive
        {publish, #{topic := Topic,
                    payload := Payload}} ->
            expect_publishes(Topic, Rest)
    after 5000 ->
              {publish_not_received, Payload}
    end.

sync_publish_result(Caller, Mref, Result) ->
    erlang:send(Caller, {Mref, Result}).

get_global_counters(Config, ProtoVer) ->
    get_global_counters(Config, ProtoVer, 0, []).

get_global_counters(Config, ProtoVer, Node) ->
    get_global_counters(Config, ProtoVer, Node, []).

get_global_counters(Config, v3, Node, QType) ->
    get_global_counters(Config, ?MQTT_PROTO_V3, Node, QType);
get_global_counters(Config, v4, Node, QType) ->
    get_global_counters(Config, ?MQTT_PROTO_V4, Node, QType);
get_global_counters(Config, Proto, Node, QType) ->
    maps:get([{protocol, Proto}] ++ QType,
             rabbit_ct_broker_helpers:rpc(Config, Node, rabbit_global_counters, overview, [])).

connect(ClientId, Config) ->
    connect(ClientId, Config, []).

connect(ClientId, Config, AdditionalOpts) ->
    connect(ClientId, Config, 0, AdditionalOpts).

connect(ClientId, Config, Node, AdditionalOpts) ->
    P = rabbit_ct_broker_helpers:get_node_config(Config, Node, tcp_port_mqtt),
    Options = [{host, "localhost"},
               {port, P},
               {clientid, rabbit_data_coercion:to_binary(ClientId)},
               {proto_ver, v4}
              ] ++ AdditionalOpts,
    {ok, C} = emqtt:start_link(Options),
    {ok, _Properties} = emqtt:connect(C),
    C.

connect_to_node(Config, Node, ClientID) ->
  C = connect(ClientID, Config, Node, [{connect_timeout, 1}, {ack_timeout, 1}]),
  unlink(C),
  MRef = erlang:monitor(process, C),
  {ok, MRef, C}.
