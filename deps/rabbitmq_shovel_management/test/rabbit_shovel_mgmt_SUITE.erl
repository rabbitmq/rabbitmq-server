-module(rabbit_shovel_mgmt_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").

-compile(export_all).

-define(MOCK_SHOVELS,
    [[
        {node,node()},
        {name,<<"shovel1">>},
        {vhost,<<"/">>},
        {type,dynamic},
        {state,running},
        {src_uri,<<"amqp://">>},
        {src_protocol,<<"amqp091">>},
        {dest_protocol,<<"amqp091">>},
        {dest_uri,<<"amqp://">>},
        {src_queue,<<"q1">>},
        {dest_queue,<<"q2">>}
    ],
    [
        {node,'node2'},
        {name,<<"shovel2">>},
        {vhost,<<"otherVhost">>},
        {type,dynamic},
        {state,running},
        {src_uri,<<"amqp://">>},
        {src_protocol,<<"amqp091">>},
        {dest_protocol,<<"amqp091">>},
        {dest_uri,<<"amqp://">>},
        {src_queue,<<"q1">>},
        {dest_queue,<<"q2">>}
    ]]).

all() ->
    [
        get_shovel_node_shovel_different_name,
        get_shovel_node_shovel_different_vhost_name,
        get_shovel_node_shovel_found,
        delete_resource_badrpc
    ].

init_per_testcase(delete_resource_badrpc, _Config) ->
    meck:expect(rabbit_shovel_mgmt_util, status, fun(_,_) -> ?MOCK_SHOVELS end),
    meck:expect(rabbit_shovel_status, lookup,
                fun({_, Name}) ->
                        case [S || S <- ?MOCK_SHOVELS, proplists:get_value(name, S) =:= Name] of
                            [Obj] -> Obj;
                            [] -> not_found
                        end
                end),
    _Config;
init_per_testcase(_, _Config) ->
    meck:new(rabbit_shovel_mgmt_util),
    meck:expect(rabbit_shovel_mgmt_util, status, fun(_,_) -> ?MOCK_SHOVELS end),
    _Config.

end_per_testcase(delete_resource_badrpc, _Config) ->
    meck:unload(rabbit_shovel_mgmt_util),
    meck:unload(rabbit_shovel_status),
    _Config;
end_per_testcase(_, _Config) ->
    meck:unload(rabbit_shovel_mgmt_util),
    _Config.

get_shovel_node_shovel_different_name(_Config) ->
    VHost = <<"otherVhost">>,
    Name= <<"shovelThatDoesntExist">>,
    User = #user{username="admin",tags = [administrator]},
    Node = rabbit_shovel_mgmt:get_shovel_node(VHost, Name, {}, #context{user = User}),
    ?assertEqual(undefined, Node).

get_shovel_node_shovel_different_vhost_name(_Config) ->
    VHost = <<"VHostThatDoesntExist">>,
    Name= <<"shovel1">>,
    User = #user{username="admin",tags = [administrator]},
    Node = rabbit_shovel_mgmt:get_shovel_node(VHost, Name, {}, #context{user = User}),
    ?assertEqual(undefined, Node).

get_shovel_node_shovel_found(_Config) ->
    VHost = <<"otherVhost">>,
    Name= <<"shovel2">>,
    User = #user{username="admin",tags = [administrator]},
    Node = rabbit_shovel_mgmt:get_shovel_node(VHost, Name, {}, #context{user = User}),
    ?assertEqual('node2', Node).

delete_resource_badrpc(_Config) ->
    VHost = <<"/">>,
    Name= <<"shovel1">>,
    User = #user{username="admin",tags = [administrator]},
    Context = #context{user = User},
    ReqData = #{path => <<"/shovels/vhost/././restart">>,
                bindings => #{vhost => VHost, name => Name}},
    {Reply, ReqData, Context} = rabbit_shovel_mgmt:delete_resource(ReqData, Context),
    ?assertEqual(false, Reply),

    ReqData2 = #{path => <<"/shovels/vhost/./.">>,
                 bindings => #{vhost => VHost, name => Name}},
    {Reply, ReqData2, Context} = rabbit_shovel_mgmt:delete_resource(ReqData2, Context),
    ?assertEqual(false, Reply).
