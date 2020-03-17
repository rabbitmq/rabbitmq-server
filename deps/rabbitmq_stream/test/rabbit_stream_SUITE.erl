%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is Pivotal Software, Inc.
%% Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stream_SUITE).

-include_lib("common_test/include/ct.hrl").

-compile(export_all).

all() ->
    [
        {group, single_node},
        {group, cluster}
    ].

groups() ->
    [
        {single_node, [], [test_stream]},
        {cluster, [], [test_stream]}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(single_node, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [{rmq_nodes_clustered, false}]),
    rabbit_ct_helpers:run_setup_steps(Config1,
        rabbit_ct_broker_helpers:setup_steps());
init_per_group(cluster = Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [{rmq_nodes_clustered, true}]),
    Config2 = rabbit_ct_helpers:set_config(Config1,
        [{rmq_nodes_count, 3},
            {rmq_nodename_suffix, Group},
            {tcp_ports_base}]),
    rabbit_ct_helpers:run_setup_steps(Config2,
        rabbit_ct_broker_helpers:setup_steps()).

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_steps(Config,
        rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_Test, _Config) ->
    ok.

test_stream(Config) ->
    Port = get_stream_port(Config),
    test_server(Port),
    ok.

get_stream_port(Config) ->
    rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_stream).

test_server(Port) ->
    {ok, S} = gen_tcp:connect("localhost", Port, [{active, false},
        {mode, binary}]),
    Target = <<"target1">>,
    test_create_target(S, Target),
    Body = <<"hello">>,
    test_publish_confirm(S, Target, Body),
    SubscriptionId = 42,
    test_subscribe(S, SubscriptionId, Target),
    test_deliver(S, SubscriptionId, Body),
    test_delete_target(S, Target),
    test_metadata_update_target_deleted(S, Target),
    ok.

test_create_target(S, Target) ->
    TargetSize = byte_size(Target),
    CreateTargetFrame = <<998:16, 0:16, 1:32, TargetSize:16, Target:TargetSize/binary>>,
    FrameSize = byte_size(CreateTargetFrame),
    gen_tcp:send(S, <<FrameSize:32, CreateTargetFrame/binary>>),
    {ok, <<_Size:32, 998:16, 0:16, 1:32, 0:16>>} = gen_tcp:recv(S, 0, 5000).

test_delete_target(S, Target) ->
    TargetSize = byte_size(Target),
    DeleteTargetFrame = <<999:16, 0:16, 1:32, TargetSize:16, Target:TargetSize/binary>>,
    FrameSize = byte_size(DeleteTargetFrame),
    gen_tcp:send(S, <<FrameSize:32, DeleteTargetFrame/binary>>),
    ResponseFrameSize = 10,
    {ok, <<ResponseFrameSize:32, 999:16, 0:16, 1:32, 0:16>>} = gen_tcp:recv(S, 4 + 10, 5000).

test_publish_confirm(S, Target, Body) ->
    BodySize = byte_size(Body),
    TargetSize = byte_size(Target),
    PublishFrame = <<0:16, 0:16, TargetSize:16, Target:TargetSize/binary, 1:32, 1:64, BodySize:32, Body:BodySize/binary>>,
    FrameSize = byte_size(PublishFrame),
    gen_tcp:send(S, <<FrameSize:32, PublishFrame/binary>>),
    {ok, <<_Size:32, 1:16, 0:16, 1:32, 1:64>>} = gen_tcp:recv(S, 0, 5000).

test_subscribe(S, SubscriptionId, Target) ->
    TargetSize = byte_size(Target),
    SubscribeFrame = <<2:16, 0:16, 1:32, SubscriptionId:32, TargetSize:16, Target:TargetSize/binary, 0:64, 10:16>>,
    FrameSize = byte_size(SubscribeFrame),
    gen_tcp:send(S, <<FrameSize:32, SubscribeFrame/binary>>),
    Res = gen_tcp:recv(S, 0, 5000),
    {ok, <<_Size:32, 2:16, 0:16, 1:32, 0:16>>} = Res.

test_deliver(S, SubscriptionId, Body) ->
    BodySize = byte_size(Body),
    Frame = read_frame(S, <<>>),
    <<48:32, 3:16, 0:16, SubscriptionId:32, 5:4/unsigned, 0:4/unsigned, 1:16, 1:32, _Epoch:64, 0:64, _Crc:32, _DataLength:32,
        0:1, BodySize:31/unsigned, Body/binary>> = Frame.

test_metadata_update_target_deleted(S, Target) ->
    TargetSize = byte_size(Target),
    {ok, <<15:32, 7:16, 0:16, 5:16, TargetSize:16, Target/binary>>} = gen_tcp:recv(S, 0, 5000).

read_frame(S, Buffer) ->
    inet:setopts(S, [{active, once}]),
    receive
        {tcp, S, Received} ->
            Data = <<Buffer/binary, Received/binary>>,
            case Data of
                <<Size:32, _Body:Size/binary>> ->
                    Data;
                _ ->
                    read_frame(S, Data)
            end
    after
        1000 ->
            inet:setopts(S, [{active, false}])
    end.