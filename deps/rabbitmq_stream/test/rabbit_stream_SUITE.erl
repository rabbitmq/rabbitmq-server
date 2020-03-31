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
-include("rabbit_stream.hrl").

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
    test_authenticate(S),
    Target = <<"target1">>,
    test_create_target(S, Target),
    Body = <<"hello">>,
    test_publish_confirm(S, Target, Body),
    SubscriptionId = 42,
    test_subscribe(S, SubscriptionId, Target),
    test_deliver(S, SubscriptionId, Body),
    test_delete_target(S, Target),
    test_metadata_update_target_deleted(S, Target),
    test_close(S),
    closed = wait_for_socket_close(S, 10),
    ok.

test_authenticate(S) ->
    SaslHandshakeFrame = <<?COMMAND_SASL_HANDSHAKE:16, ?VERSION_0:16, 1:32>>,
    SaslHandshakeFrameSize = byte_size(SaslHandshakeFrame),
    gen_tcp:send(S, <<SaslHandshakeFrameSize:32, SaslHandshakeFrame/binary>>),
    Plain = <<"PLAIN">>,
    AmqPlain = <<"AMQPLAIN">>,
    {ok, SaslAvailable} = gen_tcp:recv(S, 0, 5000),
    %% mechanisms order is not deterministic, so checking both orders
    ok = case SaslAvailable of
             <<31:32, ?COMMAND_SASL_HANDSHAKE:16, ?VERSION_0:16, 1:32, ?RESPONSE_CODE_OK:16, 2:32,
                 5:16, Plain:5/binary, 8:16, AmqPlain:8/binary>> ->
                 ok;
             <<31:32, ?COMMAND_SASL_HANDSHAKE:16, ?VERSION_0:16, 1:32, ?RESPONSE_CODE_OK:16, 2:32,
                 8:16, AmqPlain:8/binary, 5:16, Plain:5/binary>> ->
                 ok;
             _ ->
                 failed
         end,

    Username = <<"guest">>,
    Password = <<"guest">>,
    Null = 0,
    PlainSasl = <<Null:8, Username/binary, Null:8, Password/binary>>,
    PlainSaslSize = byte_size(PlainSasl),

    SaslAuthenticateFrame = <<?COMMAND_SASL_AUTHENTICATE:16, ?VERSION_0:16, 2:32,
        5:16, Plain/binary, PlainSaslSize:32, PlainSasl/binary>>,

    SaslAuthenticateFrameSize = byte_size(SaslAuthenticateFrame),

    gen_tcp:send(S, <<SaslAuthenticateFrameSize:32, SaslAuthenticateFrame/binary>>),

    {ok, <<10:32, ?COMMAND_SASL_AUTHENTICATE:16, ?VERSION_0:16, 2:32, ?RESPONSE_CODE_OK:16, RestTune/binary>>} = gen_tcp:recv(S, 0, 5000),

    TuneExpected = <<12:32, ?COMMAND_TUNE:16, ?VERSION_0:16, ?DEFAULT_FRAME_MAX:32, ?DEFAULT_HEARTBEAT:32>>,
    case RestTune of
        <<>> ->
            {ok, TuneExpected} = gen_tcp:recv(S, 0, 5000);
        TuneReceived ->
            TuneExpected = TuneReceived
    end,

    TuneFrame = <<?COMMAND_TUNE:16, ?VERSION_0:16, ?DEFAULT_FRAME_MAX:32, ?DEFAULT_HEARTBEAT:32>>,
    TuneFrameSize = byte_size(TuneFrame),
    gen_tcp:send(S, <<TuneFrameSize:32, TuneFrame/binary>>),

    VirtualHost = <<"/">>,
    VirtualHostLength = byte_size(VirtualHost),
    OpenFrame = <<?COMMAND_OPEN:16, ?VERSION_0:16, 3:32, VirtualHostLength:16, VirtualHost/binary>>,
    OpenFrameSize = byte_size(OpenFrame),
    gen_tcp:send(S, <<OpenFrameSize:32, OpenFrame/binary>>),
    {ok, <<10:32, ?COMMAND_OPEN:16, ?VERSION_0:16, 3:32, ?RESPONSE_CODE_OK:16>>} = gen_tcp:recv(S, 0, 5000).


test_create_target(S, Target) ->
    TargetSize = byte_size(Target),
    CreateTargetFrame = <<?COMMAND_CREATE_TARGET:16, ?VERSION_0:16, 1:32, TargetSize:16, Target:TargetSize/binary>>,
    FrameSize = byte_size(CreateTargetFrame),
    gen_tcp:send(S, <<FrameSize:32, CreateTargetFrame/binary>>),
    {ok, <<_Size:32, ?COMMAND_CREATE_TARGET:16, ?VERSION_0:16, 1:32, ?RESPONSE_CODE_OK:16>>} = gen_tcp:recv(S, 0, 5000).

test_delete_target(S, Target) ->
    TargetSize = byte_size(Target),
    DeleteTargetFrame = <<?COMMAND_DELETE_TARGET:16, ?VERSION_0:16, 1:32, TargetSize:16, Target:TargetSize/binary>>,
    FrameSize = byte_size(DeleteTargetFrame),
    gen_tcp:send(S, <<FrameSize:32, DeleteTargetFrame/binary>>),
    ResponseFrameSize = 10,
    {ok, <<ResponseFrameSize:32, ?COMMAND_DELETE_TARGET:16, ?VERSION_0:16, 1:32, ?RESPONSE_CODE_OK:16>>} = gen_tcp:recv(S, 4 + 10, 5000).

test_publish_confirm(S, Target, Body) ->
    BodySize = byte_size(Body),
    TargetSize = byte_size(Target),
    PublishFrame = <<?COMMAND_PUBLISH:16, ?VERSION_0:16, TargetSize:16, Target:TargetSize/binary, 1:32, 1:64, BodySize:32, Body:BodySize/binary>>,
    FrameSize = byte_size(PublishFrame),
    gen_tcp:send(S, <<FrameSize:32, PublishFrame/binary>>),
    {ok, <<_Size:32, ?COMMAND_PUBLISH_CONFIRM:16, ?VERSION_0:16, 1:32, 1:64>>} = gen_tcp:recv(S, 0, 5000).

test_subscribe(S, SubscriptionId, Target) ->
    TargetSize = byte_size(Target),
    SubscribeFrame = <<?COMMAND_SUBSCRIBE:16, ?VERSION_0:16, 1:32, SubscriptionId:32, TargetSize:16, Target:TargetSize/binary, 0:64, 10:16>>,
    FrameSize = byte_size(SubscribeFrame),
    gen_tcp:send(S, <<FrameSize:32, SubscribeFrame/binary>>),
    Res = gen_tcp:recv(S, 0, 5000),
    {ok, <<_Size:32, ?COMMAND_SUBSCRIBE:16, ?VERSION_0:16, 1:32, ?RESPONSE_CODE_OK:16>>} = Res.

test_deliver(S, SubscriptionId, Body) ->
    BodySize = byte_size(Body),
    Frame = read_frame(S, <<>>),
    <<48:32, ?COMMAND_DELIVER:16, ?VERSION_0:16, SubscriptionId:32, 5:4/unsigned, 0:4/unsigned, 1:16, 1:32, _Epoch:64, 0:64, _Crc:32, _DataLength:32,
        0:1, BodySize:31/unsigned, Body/binary>> = Frame.

test_metadata_update_target_deleted(S, Target) ->
    TargetSize = byte_size(Target),
    {ok, <<15:32, ?COMMAND_METADATA_UPDATE:16, ?VERSION_0:16, ?RESPONSE_CODE_TARGET_DELETED:16, TargetSize:16, Target/binary>>} = gen_tcp:recv(S, 0, 5000).

test_close(S) ->
    CloseReason = <<"OK">>,
    CloseReasonSize = byte_size(CloseReason),
    CloseFrame = <<?COMMAND_CLOSE:16, ?VERSION_0:16, 1:32, ?RESPONSE_CODE_OK:16, CloseReasonSize:16, CloseReason/binary>>,
    CloseFrameSize = byte_size(CloseFrame),
    gen_tcp:send(S, <<CloseFrameSize:32, CloseFrame/binary>>),
    {ok, <<10:32, ?COMMAND_CLOSE:16, ?VERSION_0:16, 1:32, ?RESPONSE_CODE_OK:16>>} = gen_tcp:recv(S, 0, 5000).

wait_for_socket_close(_S, 0) ->
    not_closed;
wait_for_socket_close(S, Attempt) ->
    case gen_tcp:recv(S, 0, 1000) of
        {error, timeout} ->
            wait_for_socket_close(S, Attempt - 1);
        {error, closed} ->
            closed
    end.

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