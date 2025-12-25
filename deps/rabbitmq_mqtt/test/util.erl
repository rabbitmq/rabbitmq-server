-module(util).

-include("rabbit_mqtt.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all_connection_pids/1,
         publish_qos1_timeout/4,
         sync_publish_result/3,
         get_global_counters/1,
         get_global_counters/2,
         get_global_counters/3,
         get_global_counters/4,
         expect_publishes/3,
         connect/2,
         connect/3,
         connect/4,
         start_client/4,
         get_events/1,
         get_events/2,
         assert_event_type/2,
         assert_event_prop/2,
         assert_message_expiry_interval/2,
         await_exit/1,
         await_exit/2,
         non_clean_sess_opts/0,
         enable_plugin/2
        ]).

all_connection_pids(Config) ->
    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Result = erpc:multicall(Nodes, rabbit_mqtt, local_connection_pids, [], 5000),
    lists:append([Pids || {ok, Pids} <- Result]).

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

expect_publishes(_, _, []) ->
    ok;
expect_publishes(Client, Topic, [Payload|Rest])
  when is_pid(Client) ->
    receive
        {publish, #{client_pid := Client,
                    topic := Topic,
                    payload := Payload}} ->
            expect_publishes(Client, Topic, Rest);
        {publish, #{client_pid := Client,
                    topic := Topic,
                    payload := Other}} ->
            ct:fail("Received unexpected PUBLISH payload. Expected: ~p Got: ~p",
                    [Payload, Other])
    after 5000 ->
              {publish_not_received, Payload}
    end.

get_global_counters(Config) ->
    get_global_counters(Config, rabbit_ct_helpers:get_config(Config, mqtt_version, v4)).

get_global_counters(Config, ProtoVer) ->
    get_global_counters(Config, ProtoVer, 0).

get_global_counters(Config, ProtoVer, Node) ->
    get_global_counters(Config, ProtoVer, Node, []).

get_global_counters(Config, v3, Node, QType) ->
    get_global_counters(Config, ?MQTT_PROTO_V3, Node, QType);
get_global_counters(Config, v4, Node, QType) ->
    get_global_counters(Config, ?MQTT_PROTO_V4, Node, QType);
get_global_counters(Config, v5, Node, QType) ->
    get_global_counters(Config, ?MQTT_PROTO_V5, Node, QType);
get_global_counters(Config, Proto, Node, QType) ->
    maps:get([{protocol, Proto}] ++ QType,
             rabbit_ct_broker_helpers:rpc(Config, Node, rabbit_global_counters, overview, [])).

get_events(Node) ->
    timer:sleep(500), %% events are sent and processed asynchronously
    Result = gen_event:call({rabbit_event, Node}, event_recorder, take_state),
    ?assert(is_list(Result)),
    Result.

get_events(Node, Type) ->
    lists:filter(fun(#event{type = T}) ->
                         T == Type
                 end, get_events(Node)).

assert_event_type(ExpectedType, #event{type = ActualType}) ->
    ?assertEqual(ExpectedType, ActualType).

assert_event_prop(ExpectedProp = {Key, _Value}, #event{props = Props}) ->
    ?assertEqual(ExpectedProp, lists:keyfind(Key, 1, Props));
assert_event_prop(ExpectedProps, Event)
  when is_list(ExpectedProps) ->
    lists:foreach(fun(P) ->
                          assert_event_prop(P, Event)
                  end, ExpectedProps).

assert_message_expiry_interval(Expected, Actual)
  when is_integer(Expected),
       is_integer(Actual) ->
    ?assert(Expected >= Actual - 1),
    ?assert(Expected =< Actual + 1).

await_exit(Pid) ->
    receive
        {'EXIT', Pid, _} -> ok
    after
        30_000 -> ct:fail({missing_exit, Pid})
    end.

await_exit(Pid, Reason) ->
    receive
        {'EXIT', Pid, Reason} -> ok
    after
        30_000 -> ct:fail({missing_exit, Pid})
    end.

%% "CleanStart=0 and SessionExpiry=0xFFFFFFFF (UINT_MAX) for
%% MQTT 5.0 would provide the same as CleanSession=0 for 3.1.1."
%% https://issues.oasis-open.org/projects/MQTT/issues/MQTT-538
non_clean_sess_opts() ->
    [{clean_start, false},
     {properties, #{'Session-Expiry-Interval' => 16#FFFFFFFF}}].

connect(ClientId, Config) ->
    connect(ClientId, Config, []).

connect(ClientId, Config, AdditionalOpts) ->
    connect(ClientId, Config, 0, AdditionalOpts).

connect(ClientId, Config, Node, AdditionalOpts) ->
    {C, Connect} = start_client(ClientId, Config, Node, AdditionalOpts),
    {ok, _Properties} = Connect(C),
    C.

start_client(ClientId, Config, Node, AdditionalOpts) ->
    {Port, WsOpts, Connect} =
    case rabbit_ct_helpers:get_config(Config, websocket, false) of
        false ->
            {rabbit_ct_broker_helpers:get_node_config(Config, Node, tcp_port_mqtt),
             [],
             fun emqtt:connect/1};
        true ->
            {rabbit_ct_broker_helpers:get_node_config(Config, Node, tcp_port_web_mqtt),
             [{ws_path, "/ws"}],
             fun emqtt:ws_connect/1}
    end,
    ProtoVer = proplists:get_value(
                 proto_ver,
                 AdditionalOpts,
                 rabbit_ct_helpers:get_config(Config, mqtt_version, v4)),
    Options = [{host, "localhost"},
               {port, Port},
               {proto_ver, ProtoVer},
               {clientid, rabbit_data_coercion:to_binary(ClientId)}
              ] ++ WsOpts ++ AdditionalOpts,
    {ok, C} = emqtt:start_link(Options),
    {C, Connect}.

enable_plugin(Config, Plugin) ->
    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    [rabbit_ct_broker_helpers:enable_plugin(Config, Node, Plugin)
     || Node <- Nodes].
