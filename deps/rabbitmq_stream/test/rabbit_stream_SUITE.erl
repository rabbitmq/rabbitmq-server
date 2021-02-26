%% The contents of this file are subject to the Mozilla Public License
%% Version 2.0 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/en-US/MPL/2.0/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is Pivotal Software, Inc.
%% Copyright (c) 2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stream_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-include("rabbit_stream.hrl").
-include("rabbit_stream_metrics.hrl").

-compile(export_all).

all() ->
    [{group, single_node}, {group, cluster}].

groups() ->
    [{single_node, [],
      [test_stream, test_gc_consumers, test_gc_publishers]},
     {cluster, [], [test_stream, java]}].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(single_node, Config) ->
    Config1 =
        rabbit_ct_helpers:set_config(Config, [{rmq_nodes_clustered, false}]),
    rabbit_ct_helpers:run_setup_steps(Config1,
                                      [fun(StepConfig) ->
                                          rabbit_ct_helpers:merge_app_env(StepConfig,
                                                                          {rabbit,
                                                                           [{core_metrics_gc_interval,
                                                                             1000}]})
                                       end]
                                      ++ rabbit_ct_broker_helpers:setup_steps());
init_per_group(cluster = Group, Config) ->
    Config1 =
        rabbit_ct_helpers:set_config(Config, [{rmq_nodes_clustered, true}]),
    Config2 =
        rabbit_ct_helpers:set_config(Config1,
                                     [{rmq_nodes_count, 3},
                                      {rmq_nodename_suffix, Group},
                                      {tcp_ports_base}]),
    rabbit_ct_helpers:run_setup_steps(Config2,
                                      [fun(StepConfig) ->
                                          rabbit_ct_helpers:merge_app_env(StepConfig,
                                                                          {aten,
                                                                           [{poll_interval,
                                                                             1000}]})
                                       end]
                                      ++ rabbit_ct_broker_helpers:setup_steps());
init_per_group(_, Config) ->
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_group(java, Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config);
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

test_gc_consumers(Config) ->
    Pid = spawn(fun() -> ok end),
    rabbit_ct_broker_helpers:rpc(Config,
                                 0,
                                 rabbit_stream_metrics,
                                 consumer_created,
                                 [Pid,
                                  #resource{name = <<"test">>,
                                            kind = queue,
                                            virtual_host = <<"/">>},
                                  0,
                                  10,
                                  0,
                                  0]),
    ok = test_utils:wait_until(fun() -> consumer_count(Config) == 0 end),
    ok.

test_gc_publishers(Config) ->
    Pid = spawn(fun() -> ok end),
    rabbit_ct_broker_helpers:rpc(Config,
                                 0,
                                 rabbit_stream_metrics,
                                 publisher_created,
                                 [Pid,
                                  #resource{name = <<"test">>,
                                            kind = queue,
                                            virtual_host = <<"/">>},
                                  0,
                                  <<"ref">>]),
    ok = test_utils:wait_until(fun() -> publisher_count(Config) == 0 end),
    ok.

consumer_count(Config) ->
    ets_count(Config, ?TABLE_CONSUMER).

publisher_count(Config) ->
    ets_count(Config, ?TABLE_PUBLISHER).

ets_count(Config, Table) ->
    Info = rabbit_ct_broker_helpers:rpc(Config, 0, ets, info, [Table]),
    rabbit_misc:pget(size, Info).

java(Config) ->
    StreamPortNode1 = get_stream_port(Config, 0),
    StreamPortNode2 = get_stream_port(Config, 1),
    Node1Name = get_node_name(Config, 0),
    Node2Name = get_node_name(Config, 1),
    RabbitMqCtl = get_rabbitmqctl(Config),
    DataDir = rabbit_ct_helpers:get_config(Config, data_dir),
    MakeResult =
        rabbit_ct_helpers:make(Config, DataDir,
                               ["tests",
                                {"NODE1_STREAM_PORT=~b", [StreamPortNode1]},
                                {"NODE1_NAME=~p", [Node1Name]},
                                {"NODE2_NAME=~p", [Node2Name]},
                                {"NODE2_STREAM_PORT=~b", [StreamPortNode2]},
                                {"RABBITMQCTL=~p", [RabbitMqCtl]}]),
    {ok, _} = MakeResult.

get_rabbitmqctl(Config) ->
    rabbit_ct_helpers:get_config(Config, rabbitmqctl_cmd).

get_stream_port(Config) ->
    get_stream_port(Config, 0).

get_stream_port(Config, Node) ->
    rabbit_ct_broker_helpers:get_node_config(Config, Node,
                                             tcp_port_stream).

get_node_name(Config) ->
    get_node_name(Config, 0).

get_node_name(Config, Node) ->
    rabbit_ct_broker_helpers:get_node_config(Config, Node, nodename).

test_server(Port) ->
    {ok, S} =
        gen_tcp:connect("localhost", Port, [{active, false}, {mode, binary}]),
    test_peer_properties(S),
    test_authenticate(S),
    Stream = <<"stream1">>,
    test_create_stream(S, Stream),
    PublisherId = 42,
    test_declare_publisher(S, PublisherId, Stream),
    Body = <<"hello">>,
    test_publish_confirm(S, PublisherId, Body),
    SubscriptionId = 42,
    Rest = test_subscribe(S, SubscriptionId, Stream),
    test_deliver(S, Rest, SubscriptionId, Body),
    test_delete_stream(S, Stream),
    test_metadata_update_stream_deleted(S, Stream),
    test_close(S),
    closed = wait_for_socket_close(S, 10),
    ok.

test_peer_properties(S) ->
    PeerPropertiesFrame =
        <<?REQUEST:1,
          ?COMMAND_PEER_PROPERTIES:15,
          ?VERSION_1:16,
          1:32,
          0:32>>,
    PeerPropertiesFrameSize = byte_size(PeerPropertiesFrame),
    gen_tcp:send(S,
                 <<PeerPropertiesFrameSize:32, PeerPropertiesFrame/binary>>),
    {ok,
     <<_Size:32,
       ?RESPONSE:1,
       ?COMMAND_PEER_PROPERTIES:15,
       ?VERSION_1:16,
       1:32,
       ?RESPONSE_CODE_OK:16,
       _Rest/binary>>} =
        gen_tcp:recv(S, 0, 5000).

test_authenticate(S) ->
    SaslHandshakeFrame =
        <<?REQUEST:1, ?COMMAND_SASL_HANDSHAKE:15, ?VERSION_1:16, 1:32>>,
    SaslHandshakeFrameSize = byte_size(SaslHandshakeFrame),
    gen_tcp:send(S,
                 <<SaslHandshakeFrameSize:32, SaslHandshakeFrame/binary>>),
    Plain = <<"PLAIN">>,
    AmqPlain = <<"AMQPLAIN">>,
    {ok, SaslAvailable} = gen_tcp:recv(S, 0, 5000),
    %% mechanisms order is not deterministic, so checking both orders
    ok =
        case SaslAvailable of
            <<31:32,
              ?RESPONSE:1,
              ?COMMAND_SASL_HANDSHAKE:15,
              ?VERSION_1:16,
              1:32,
              ?RESPONSE_CODE_OK:16,
              2:32,
              5:16,
              Plain:5/binary,
              8:16,
              AmqPlain:8/binary>> ->
                ok;
            <<31:32,
              ?RESPONSE:1,
              ?COMMAND_SASL_HANDSHAKE:15,
              ?VERSION_1:16,
              1:32,
              ?RESPONSE_CODE_OK:16,
              2:32,
              8:16,
              AmqPlain:8/binary,
              5:16,
              Plain:5/binary>> ->
                ok;
            _ ->
                failed
        end,

    Username = <<"guest">>,
    Password = <<"guest">>,
    Null = 0,
    PlainSasl = <<Null:8, Username/binary, Null:8, Password/binary>>,
    PlainSaslSize = byte_size(PlainSasl),

    SaslAuthenticateFrame =
        <<?REQUEST:1,
          ?COMMAND_SASL_AUTHENTICATE:15,
          ?VERSION_1:16,
          2:32,
          5:16,
          Plain/binary,
          PlainSaslSize:32,
          PlainSasl/binary>>,

    SaslAuthenticateFrameSize = byte_size(SaslAuthenticateFrame),

    gen_tcp:send(S,
                 <<SaslAuthenticateFrameSize:32,
                   SaslAuthenticateFrame/binary>>),
    {ok,
     <<10:32,
       ?RESPONSE:1,
       ?COMMAND_SASL_AUTHENTICATE:15,
       ?VERSION_1:16,
       2:32,
       ?RESPONSE_CODE_OK:16,
       RestTune/binary>>} =
        gen_tcp:recv(S, 0, 5000),

    TuneExpected =
        <<12:32,
          ?REQUEST:1,
          ?COMMAND_TUNE:15,
          ?VERSION_1:16,
          ?DEFAULT_FRAME_MAX:32,
          ?DEFAULT_HEARTBEAT:32>>,
    case RestTune of
        <<>> ->
            {ok, TuneExpected} = gen_tcp:recv(S, 0, 5000);
        TuneReceived ->
            TuneExpected = TuneReceived
    end,

    TuneFrame =
        <<?RESPONSE:1,
          ?COMMAND_TUNE:15,
          ?VERSION_1:16,
          ?DEFAULT_FRAME_MAX:32,
          0:32>>,
    TuneFrameSize = byte_size(TuneFrame),
    gen_tcp:send(S, <<TuneFrameSize:32, TuneFrame/binary>>),

    VirtualHost = <<"/">>,
    VirtualHostLength = byte_size(VirtualHost),
    OpenFrame =
        <<?REQUEST:1,
          ?COMMAND_OPEN:15,
          ?VERSION_1:16,
          3:32,
          VirtualHostLength:16,
          VirtualHost/binary>>,
    OpenFrameSize = byte_size(OpenFrame),
    gen_tcp:send(S, <<OpenFrameSize:32, OpenFrame/binary>>),
    {ok,
     <<10:32,
       ?RESPONSE:1,
       ?COMMAND_OPEN:15,
       ?VERSION_1:16,
       3:32,
       ?RESPONSE_CODE_OK:16>>} =
        gen_tcp:recv(S, 0, 5000).

test_create_stream(S, Stream) ->
    StreamSize = byte_size(Stream),
    CreateStreamFrame =
        <<?REQUEST:1,
          ?COMMAND_CREATE_STREAM:15,
          ?VERSION_1:16,
          1:32,
          StreamSize:16,
          Stream:StreamSize/binary,
          0:32>>,
    FrameSize = byte_size(CreateStreamFrame),
    gen_tcp:send(S, <<FrameSize:32, CreateStreamFrame/binary>>),
    {ok,
     <<_Size:32,
       ?RESPONSE:1,
       ?COMMAND_CREATE_STREAM:15,
       ?VERSION_1:16,
       1:32,
       ?RESPONSE_CODE_OK:16>>} =
        gen_tcp:recv(S, 0, 5000).

test_delete_stream(S, Stream) ->
    StreamSize = byte_size(Stream),
    DeleteStreamFrame =
        <<?REQUEST:1,
          ?COMMAND_DELETE_STREAM:15,
          ?VERSION_1:16,
          1:32,
          StreamSize:16,
          Stream:StreamSize/binary>>,
    FrameSize = byte_size(DeleteStreamFrame),
    gen_tcp:send(S, <<FrameSize:32, DeleteStreamFrame/binary>>),
    ResponseFrameSize = 10,
    {ok,
     <<ResponseFrameSize:32,
       ?RESPONSE:1,
       ?COMMAND_DELETE_STREAM:15,
       ?VERSION_1:16,
       1:32,
       ?RESPONSE_CODE_OK:16>>} =
        gen_tcp:recv(S, 4 + 10, 5000).

test_declare_publisher(S, PublisherId, Stream) ->
    StreamSize = byte_size(Stream),
    DeclarePublisherFrame =
        <<?REQUEST:1,
          ?COMMAND_DECLARE_PUBLISHER:15,
          ?VERSION_1:16,
          1:32,
          PublisherId:8,
          0:16, %% empty publisher reference
          StreamSize:16,
          Stream:StreamSize/binary>>,
    FrameSize = byte_size(DeclarePublisherFrame),
    gen_tcp:send(S, <<FrameSize:32, DeclarePublisherFrame/binary>>),
    Res = gen_tcp:recv(S, 0, 5000),
    {ok,
     <<_Size:32,
       ?RESPONSE:1,
       ?COMMAND_DECLARE_PUBLISHER:15,
       ?VERSION_1:16,
       1:32,
       ?RESPONSE_CODE_OK:16,
       Rest/binary>>} =
        Res,
    Rest.

test_publish_confirm(S, PublisherId, Body) ->
    BodySize = byte_size(Body),
    PublishFrame =
        <<?REQUEST:1,
          ?COMMAND_PUBLISH:15,
          ?VERSION_1:16,
          PublisherId:8,
          1:32,
          1:64,
          BodySize:32,
          Body:BodySize/binary>>,
    FrameSize = byte_size(PublishFrame),
    gen_tcp:send(S, <<FrameSize:32, PublishFrame/binary>>),
    {ok,
     <<_Size:32,
       ?REQUEST:1,
       ?COMMAND_PUBLISH_CONFIRM:15,
       ?VERSION_1:16,
       PublisherId:8,
       1:32,
       1:64>>} =
        gen_tcp:recv(S, 0, 5000).

test_subscribe(S, SubscriptionId, Stream) ->
    StreamSize = byte_size(Stream),
    SubscribeFrame =
        <<?REQUEST:1,
          ?COMMAND_SUBSCRIBE:15,
          ?VERSION_1:16,
          1:32,
          SubscriptionId:8,
          StreamSize:16,
          Stream:StreamSize/binary,
          ?OFFSET_TYPE_OFFSET:16,
          0:64,
          10:16>>,
    FrameSize = byte_size(SubscribeFrame),
    gen_tcp:send(S, <<FrameSize:32, SubscribeFrame/binary>>),
    Res = gen_tcp:recv(S, 0, 5000),
    {ok,
     <<_Size:32,
       ?RESPONSE:1,
       ?COMMAND_SUBSCRIBE:15,
       ?VERSION_1:16,
       1:32,
       ?RESPONSE_CODE_OK:16,
       Rest/binary>>} =
        Res,
    Rest.

test_deliver(S, Rest, SubscriptionId, Body) ->
    BodySize = byte_size(Body),
    Frame = read_frame(S, Rest),
    <<58:32,
      ?REQUEST:1,
      ?COMMAND_DELIVER:15,
      ?VERSION_1:16,
      SubscriptionId:8,
      5:4/unsigned,
      0:4/unsigned,
      0:8,
      1:16,
      1:32,
      _Timestamp:64,
      _Epoch:64,
      0:64,
      _Crc:32,
      _DataLength:32,
      _TrailerLength:32,
      0:1,
      BodySize:31/unsigned,
      Body/binary>> =
        Frame.

test_metadata_update_stream_deleted(S, Stream) ->
    StreamSize = byte_size(Stream),
    FrameSize = 2 + 2 + 2 + 2 + StreamSize,
    {ok,
     <<FrameSize:32,
       ?REQUEST:1,
       ?COMMAND_METADATA_UPDATE:15,
       ?VERSION_1:16,
       ?RESPONSE_CODE_STREAM_NOT_AVAILABLE:16,
       StreamSize:16,
       Stream/binary>>} =
        gen_tcp:recv(S, 0, 5000).

test_close(S) ->
    CloseReason = <<"OK">>,
    CloseReasonSize = byte_size(CloseReason),
    CloseFrame =
        <<?REQUEST:1,
          ?COMMAND_CLOSE:15,
          ?VERSION_1:16,
          1:32,
          ?RESPONSE_CODE_OK:16,
          CloseReasonSize:16,
          CloseReason/binary>>,
    CloseFrameSize = byte_size(CloseFrame),
    gen_tcp:send(S, <<CloseFrameSize:32, CloseFrame/binary>>),
    {ok,
     <<10:32,
       ?RESPONSE:1,
       ?COMMAND_CLOSE:15,
       ?VERSION_1:16,
       1:32,
       ?RESPONSE_CODE_OK:16>>} =
        gen_tcp:recv(S, 0, 5000).

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
    after 1000 ->
        inet:setopts(S, [{active, false}]),
        Buffer
    end.
