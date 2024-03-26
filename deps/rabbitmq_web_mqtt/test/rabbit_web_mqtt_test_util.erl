-module(rabbit_web_mqtt_test_util).

-include_lib("eunit/include/eunit.hrl").

-export([connect/3,
         connect/4
        ]).

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
