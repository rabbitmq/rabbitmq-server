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
%% Copyright (c) 2020-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stream_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").
-include_lib("rabbitmq_stream_common/include/rabbit_stream.hrl").

-include("rabbit_stream_metrics.hrl").

-compile(nowarn_export_all).
-compile(export_all).

-define(WAIT, 5000).

all() ->
    [{group, single_node}, {group, single_node_1}, {group, cluster}].

groups() ->
    [{single_node, [],
      [test_stream,
       test_stream_tls,
       test_gc_consumers,
       test_gc_publishers,
       unauthenticated_client_rejected_tcp_connected,
       timeout_tcp_connected,
       unauthenticated_client_rejected_peer_properties_exchanged,
       timeout_peer_properties_exchanged,
       unauthenticated_client_rejected_authenticating,
       timeout_authenticating,
       timeout_close_sent,
       max_segment_size_bytes_validation,
       close_connection_on_consumer_update_timeout]},
     %% Run `test_global_counters` on its own so the global metrics are
     %% initialised to 0 for each testcase
     {single_node_1, [], [test_global_counters]},
     {cluster, [], [test_stream, test_stream_tls, test_metadata, java]}].

init_per_suite(Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            {skip, "mixed version clusters are not supported"};
        _ ->
            rabbit_ct_helpers:log_environment(),
            Config
    end.

end_per_suite(Config) ->
    Config.

init_per_group(Group, Config)
  when Group == single_node orelse Group == single_node_1 ->
    Config1 = rabbit_ct_helpers:set_config(
                Config, [{rmq_nodes_clustered, false},
                         {rabbitmq_ct_tls_verify, verify_none},
                         {rabbitmq_stream, verify_none}
                        ]),
    rabbit_ct_helpers:run_setup_steps(
      Config1,
      [fun(StepConfig) ->
               rabbit_ct_helpers:merge_app_env(StepConfig,
                                               {rabbit,
                                                [{core_metrics_gc_interval,
                                                  1000}]})
       end,
       fun(StepConfig) ->
               rabbit_ct_helpers:merge_app_env(StepConfig,
                                               {rabbitmq_stream,
                                                [{connection_negotiation_step_timeout,
                                                  500}]})
       end]
      ++ rabbit_ct_broker_helpers:setup_steps());
init_per_group(cluster = Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(
                Config, [{rmq_nodes_clustered, true},
                         {rmq_nodes_count, 3},
                         {rmq_nodename_suffix, Group},
                         {tcp_ports_base},
                         {rabbitmq_ct_tls_verify, verify_none}
                        ]),
    rabbit_ct_helpers:run_setup_steps(
      Config1,
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

init_per_testcase(close_connection_on_consumer_update_timeout = TestCase, Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config,
                                      0,
                                      application,
                                      set_env,
                                      [rabbitmq_stream, request_timeout, 2000]),
    rabbit_ct_helpers:testcase_started(Config, TestCase);
init_per_testcase(TestCase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, TestCase).

end_per_testcase(close_connection_on_consumer_update_timeout = TestCase, Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config,
                                      0,
                                      application,
                                      set_env,
                                      [rabbitmq_stream, request_timeout, 60000]),
    rabbit_ct_helpers:testcase_finished(Config, TestCase);
end_per_testcase(TestCase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, TestCase).

test_global_counters(Config) ->
    Stream = atom_to_binary(?FUNCTION_NAME, utf8),
    test_server(gen_tcp, Stream, Config),
    ?assertEqual(#{publishers => 0,
                   consumers => 0,
                   messages_confirmed_total => 2,
                   messages_received_confirm_total => 2,
                   messages_received_total => 2,
                   messages_routed_total => 0,
                   messages_unroutable_dropped_total => 0,
                   messages_unroutable_returned_total => 0,
                   stream_error_access_refused_total => 0,
                   stream_error_authentication_failure_total => 0,
                   stream_error_frame_too_large_total => 0,
                   stream_error_internal_error_total => 0,
                   stream_error_precondition_failed_total => 0,
                   stream_error_publisher_does_not_exist_total => 0,
                   stream_error_sasl_authentication_failure_loopback_total => 0,
                   stream_error_sasl_challenge_total => 0,
                   stream_error_sasl_error_total => 0,
                   stream_error_sasl_mechanism_not_supported_total => 0,
                   stream_error_stream_already_exists_total => 0,
                   stream_error_stream_does_not_exist_total => 0,
                   stream_error_stream_not_available_total => 1,
                   stream_error_subscription_id_already_exists_total => 0,
                   stream_error_subscription_id_does_not_exist_total => 0,
                   stream_error_unknown_frame_total => 0,
                   stream_error_vhost_access_failure_total => 0},
                 get_global_counters(Config)),
    ok.

test_stream(Config) ->
    Stream = atom_to_binary(?FUNCTION_NAME, utf8),
    test_server(gen_tcp, Stream, Config),
    ok.

test_stream_tls(Config) ->
    Stream = atom_to_binary(?FUNCTION_NAME, utf8),
    test_server(ssl, Stream, Config),
    ok.

test_metadata(Config) ->
    Stream = atom_to_binary(?FUNCTION_NAME, utf8),
    Transport = gen_tcp,
    Port = get_stream_port(Config),
    FirstNode = get_node_name(Config, 0),
    NodeInMaintenance = get_node_name(Config, 1),
    {ok, S} =
        Transport:connect("localhost", Port,
                          [{active, false}, {mode, binary}]),
    C0 = rabbit_stream_core:init(0),
    C1 = test_peer_properties(Transport, S, C0),
    C2 = test_authenticate(Transport, S, C1),
    C3 = test_create_stream(Transport, S, Stream, C2),
    GetStreamNodes =
        fun() ->
           MetadataFrame =
               rabbit_stream_core:frame({request, 1, {metadata, [Stream]}}),
           ok = Transport:send(S, MetadataFrame),
           {CmdMetadata, _} = receive_commands(Transport, S, C3),
           {response, 1,
            {metadata, _Nodes, #{Stream := {Leader = {_H, _P}, Replicas}}}} =
               CmdMetadata,
           [Leader | Replicas]
        end,
    rabbit_ct_helpers:await_condition(fun() ->
                                         length(GetStreamNodes()) == 3
                                      end),
    rabbit_ct_broker_helpers:rpc(Config,
                                 NodeInMaintenance,
                                 rabbit_maintenance,
                                 drain,
                                 []),

    IsBeingDrained =
        fun() ->
           rabbit_ct_broker_helpers:rpc(Config,
                                        FirstNode,
                                        rabbit_maintenance,
                                        is_being_drained_consistent_read,
                                        [NodeInMaintenance])
        end,
    rabbit_ct_helpers:await_condition(fun() -> IsBeingDrained() end),

    rabbit_ct_helpers:await_condition(fun() ->
                                         length(GetStreamNodes()) == 2
                                      end),

    rabbit_ct_broker_helpers:rpc(Config,
                                 NodeInMaintenance,
                                 rabbit_maintenance,
                                 revive,
                                 []),

    rabbit_ct_helpers:await_condition(fun() -> IsBeingDrained() =:= false
                                      end),

    rabbit_ct_helpers:await_condition(fun() ->
                                         length(GetStreamNodes()) == 3
                                      end),

    DeleteStreamFrame =
        rabbit_stream_core:frame({request, 1, {delete_stream, Stream}}),
    ok = Transport:send(S, DeleteStreamFrame),
    {CmdDelete, C4} = receive_commands(Transport, S, C3),
    ?assertMatch({response, 1, {delete_stream, ?RESPONSE_CODE_OK}},
                 CmdDelete),
    _C5 = test_close(Transport, S, C4),
    closed = wait_for_socket_close(Transport, S, 10),
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
                                  0,
                                  0,
                                  true,
                                  #{}]),
    ?awaitMatch(0, consumer_count(Config), ?WAIT),
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
    ?awaitMatch(0, publisher_count(Config), ?WAIT),
    ok.

unauthenticated_client_rejected_tcp_connected(Config) ->
    Port = get_stream_port(Config),
    {ok, S} =
        gen_tcp:connect("localhost", Port, [{active, false}, {mode, binary}]),
    ?assertEqual(ok, gen_tcp:send(S, <<"invalid data">>)),
    ?assertEqual(closed, wait_for_socket_close(gen_tcp, S, 1)).

timeout_tcp_connected(Config) ->
    Port = get_stream_port(Config),
    {ok, S} =
        gen_tcp:connect("localhost", Port, [{active, false}, {mode, binary}]),
    ?assertEqual(closed, wait_for_socket_close(gen_tcp, S, 1)).

unauthenticated_client_rejected_peer_properties_exchanged(Config) ->
    Port = get_stream_port(Config),
    {ok, S} =
        gen_tcp:connect("localhost", Port, [{active, false}, {mode, binary}]),
    C0 = rabbit_stream_core:init(0),
    test_peer_properties(gen_tcp, S, C0),
    ?assertEqual(ok, gen_tcp:send(S, <<"invalid data">>)),
    ?assertEqual(closed, wait_for_socket_close(gen_tcp, S, 1)).

timeout_peer_properties_exchanged(Config) ->
    Port = get_stream_port(Config),
    {ok, S} =
        gen_tcp:connect("localhost", Port, [{active, false}, {mode, binary}]),
    C0 = rabbit_stream_core:init(0),
    test_peer_properties(gen_tcp, S, C0),
    ?assertEqual(closed, wait_for_socket_close(gen_tcp, S, 1)).

unauthenticated_client_rejected_authenticating(Config) ->
    Port = get_stream_port(Config),
    {ok, S} =
        gen_tcp:connect("localhost", Port, [{active, false}, {mode, binary}]),
    C0 = rabbit_stream_core:init(0),
    test_peer_properties(gen_tcp, S, C0),
    SaslHandshakeFrame =
        rabbit_stream_core:frame({request, 1, sasl_handshake}),
    ?assertEqual(ok, gen_tcp:send(S, SaslHandshakeFrame)),
    ?awaitMatch({error, closed}, gen_tcp:send(S, <<"invalid data">>),
                ?WAIT).

timeout_authenticating(Config) ->
    Port = get_stream_port(Config),
    {ok, S} =
        gen_tcp:connect("localhost", Port, [{active, false}, {mode, binary}]),
    C0 = rabbit_stream_core:init(0),
    test_peer_properties(gen_tcp, S, C0),
    _Frame = rabbit_stream_core:frame({request, 1, sasl_handshake}),
    ?assertEqual(closed, wait_for_socket_close(gen_tcp, S, 1)).

timeout_close_sent(Config) ->
    Port = get_stream_port(Config),
    {ok, S} =
        gen_tcp:connect("localhost", Port, [{active, false}, {mode, binary}]),
    C0 = rabbit_stream_core:init(0),
    C1 = test_peer_properties(gen_tcp, S, C0),
    C2 = test_authenticate(gen_tcp, S, C1),
    % Trigger rabbit_stream_reader to transition to state close_sent
    NonExistentCommand = 999,
    IOData = <<?REQUEST:1, NonExistentCommand:15, ?VERSION_1:16>>,
    Size = iolist_size(IOData),
    Frame = [<<Size:32>> | IOData],
    ok = gen_tcp:send(S, Frame),
    {{request, _CorrelationID,
      {close, ?RESPONSE_CODE_UNKNOWN_FRAME, <<"unknown frame">>}},
     _Config} =
        receive_commands(gen_tcp, S, C2),
    % Now, rabbit_stream_reader is in state close_sent.
    ?assertEqual(closed, wait_for_socket_close(gen_tcp, S, 1)).

max_segment_size_bytes_validation(Config) ->
    Transport = gen_tcp,
    Port = get_stream_port(Config),
    {ok, S} =
        Transport:connect("localhost", Port,
                          [{active, false}, {mode, binary}]),
    C0 = rabbit_stream_core:init(0),
    C1 = test_peer_properties(Transport, S, C0),
    C2 = test_authenticate(Transport, S, C1),
    Stream = <<"stream-max-segment-size">>,
    CreateStreamFrame =
        rabbit_stream_core:frame({request, 1,
                                  {create_stream, Stream,
                                   #{<<"stream-max-segment-size-bytes">> =>
                                         <<"3000000001">>}}}),
    ok = Transport:send(S, CreateStreamFrame),
    {Cmd, C3} = receive_commands(Transport, S, C2),
    ?assertMatch({response, 1,
                  {create_stream, ?RESPONSE_CODE_PRECONDITION_FAILED}},
                 Cmd),
    test_close(Transport, S, C3),
    ok.

close_connection_on_consumer_update_timeout(Config) ->
    Transport = gen_tcp,
    Port = get_stream_port(Config),
    {ok, S} =
    Transport:connect("localhost", Port,
                      [{active, false}, {mode, binary}]),
    C0 = rabbit_stream_core:init(0),
    C1 = test_peer_properties(Transport, S, C0),
    C2 = test_authenticate(Transport, S, C1),
    Stream = atom_to_binary(?FUNCTION_NAME, utf8),
    C3 = test_create_stream(Transport, S, Stream, C2),

    SubId = 42,
    C4 = test_subscribe(Transport, S, SubId, Stream,
                        #{<<"single-active-consumer">> => <<"true">>,
                          <<"name">> => <<"foo">>},
                        C3),
    {Cmd, _C5} = receive_commands(Transport, S, C4),
    ?assertMatch({request, _, {consumer_update, SubId, true}}, Cmd),
    closed = wait_for_socket_close(Transport, S, 10),
    {ok, Sb} =
    Transport:connect("localhost", Port,
                      [{active, false}, {mode, binary}]),
    Cb0 = rabbit_stream_core:init(0),
    Cb1 = test_peer_properties(Transport, Sb, Cb0),
    Cb2 = test_authenticate(Transport, Sb, Cb1),
    Cb3 = test_delete_stream(Transport, Sb, Stream, Cb2, false),
    _Cb4 = test_close(Transport, Sb, Cb3),
    closed = wait_for_socket_close(Transport, Sb, 10),
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
    StreamPortTlsNode1 = get_stream_port_tls(Config, 0),
    StreamPortTlsNode2 = get_stream_port_tls(Config, 1),
    Node1Name = get_node_name(Config, 0),
    Node2Name = get_node_name(Config, 1),
    RabbitMqCtl = get_rabbitmqctl(Config),
    DataDir = rabbit_ct_helpers:get_config(Config, data_dir),
    MakeResult =
        rabbit_ct_helpers:make(Config, DataDir,
                               ["tests",
                                {"NODE1_STREAM_PORT=~b", [StreamPortNode1]},
                                {"NODE1_STREAM_PORT_TLS=~b",
                                 [StreamPortTlsNode1]},
                                {"NODE1_NAME=~tp", [Node1Name]},
                                {"NODE2_NAME=~tp", [Node2Name]},
                                {"NODE2_STREAM_PORT=~b", [StreamPortNode2]},
                                {"NODE2_STREAM_PORT_TLS=~b",
                                 [StreamPortTlsNode2]},
                                {"RABBITMQCTL=~tp", [RabbitMqCtl]}]),
    {ok, _} = MakeResult.

get_rabbitmqctl(Config) ->
    rabbit_ct_helpers:get_config(Config, rabbitmqctl_cmd).

get_stream_port(Config) ->
    get_stream_port(Config, 0).

get_stream_port(Config, Node) ->
    rabbit_ct_broker_helpers:get_node_config(Config, Node,
                                             tcp_port_stream).

get_stream_port_tls(Config) ->
    get_stream_port_tls(Config, 0).

get_stream_port_tls(Config, Node) ->
    rabbit_ct_broker_helpers:get_node_config(Config, Node,
                                             tcp_port_stream_tls).

get_node_name(Config) ->
    get_node_name(Config, 0).

get_node_name(Config, Node) ->
    rabbit_ct_broker_helpers:get_node_config(Config, Node, nodename).

test_server(Transport, Stream, Config) ->
    QName = rabbit_misc:r(<<"/">>, queue, Stream),
    Port =
        case Transport of
            gen_tcp ->
                get_stream_port(Config);
            ssl ->
                application:ensure_all_started(ssl),
                get_stream_port_tls(Config)
        end,
    {ok, S} =
        Transport:connect("localhost", Port,
                          [{active, false}, {mode, binary}]),
    C0 = rabbit_stream_core:init(0),
    C1 = test_peer_properties(Transport, S, C0),
    C2 = test_authenticate(Transport, S, C1),
    C3 = test_create_stream(Transport, S, Stream, C2),
    PublisherId = 42,
    ?assertMatch(#{publishers := 0}, get_global_counters(Config)),
    C4 = test_declare_publisher(Transport, S, PublisherId, Stream, C3),
    ?awaitMatch(#{publishers := 1}, get_global_counters(Config), ?WAIT),
    Body = <<"hello">>,
    C5 = test_publish_confirm(Transport, S, PublisherId, Body, C4),
    C6 = test_publish_confirm(Transport, S, PublisherId, Body, C5),
    SubscriptionId = 42,
    ?assertMatch(#{consumers := 0}, get_global_counters(Config)),
    C7 = test_subscribe(Transport, S, SubscriptionId, Stream, C6),
    ?awaitMatch(#{consumers := 1}, get_global_counters(Config), ?WAIT),
    CounterKeys = maps:keys(get_osiris_counters(Config)),
    %% find the counter key for the subscriber
    {value, SubKey} =
        lists:search(fun ({rabbit_stream_reader, Q, Id, _}) ->
                             Q == QName andalso Id == SubscriptionId;
                         (_) ->
                             false
                     end,
                     CounterKeys),
    C8 = test_deliver(Transport, S, SubscriptionId, 0, Body, C7),
    C8b = test_deliver(Transport, S, SubscriptionId, 1, Body, C8),

    C9 = test_unsubscribe(Transport, S, SubscriptionId, C8b),

    %% assert the counter key got removed after unsubscribe
    ?assertNot(maps:is_key(SubKey, get_osiris_counters(Config))),
    %% exchange capabilities, which says we support deliver v2
    %% the connection should adapt its deliver frame accordingly
    C10 = test_exchange_command_versions(Transport, S, C9),
    SubscriptionId2 = 43,
    C11 = test_subscribe(Transport, S, SubscriptionId2, Stream, C10),
    C12 = test_deliver_v2(Transport, S, SubscriptionId2, 0, Body, C11),
    C13 = test_deliver_v2(Transport, S, SubscriptionId2, 1, Body, C12),

    C14 = test_stream_stats(Transport, S, Stream, C13),

    C15 = test_delete_stream(Transport, S, Stream, C14),
    _C16 = test_close(Transport, S, C15),
    closed = wait_for_socket_close(Transport, S, 10),
    ok.

test_peer_properties(Transport, S, C0) ->
    PeerPropertiesFrame =
        rabbit_stream_core:frame({request, 1, {peer_properties, #{}}}),
    ok = Transport:send(S, PeerPropertiesFrame),
    {Cmd, C} = receive_commands(Transport, S, C0),
    ?assertMatch({response, 1, {peer_properties, ?RESPONSE_CODE_OK, _}},
                 Cmd),
    C.

test_authenticate(Transport, S, C0) ->
    SaslHandshakeFrame =
        rabbit_stream_core:frame({request, 1, sasl_handshake}),
    ok = Transport:send(S, SaslHandshakeFrame),
    Plain = <<"PLAIN">>,
    AmqPlain = <<"AMQPLAIN">>,
    {Cmd, C1} = receive_commands(Transport, S, C0),
    case Cmd of
        {response, _, {sasl_handshake, ?RESPONSE_CODE_OK, Mechanisms}} ->
            ?assertEqual([AmqPlain, Plain], lists:sort(Mechanisms));
        _ ->
            ct:fail("invalid cmd ~tp", [Cmd])
    end,

    Username = <<"guest">>,
    Password = <<"guest">>,
    Null = 0,
    PlainSasl = <<Null:8, Username/binary, Null:8, Password/binary>>,

    SaslAuthenticateFrame =
        rabbit_stream_core:frame({request, 2,
                                  {sasl_authenticate, Plain, PlainSasl}}),
    ok = Transport:send(S, SaslAuthenticateFrame),
    {SaslAuth, C2} = receive_commands(Transport, S, C1),
    {response, 2, {sasl_authenticate, ?RESPONSE_CODE_OK}} = SaslAuth,
    {Tune, C3} = receive_commands(Transport, S, C2),
    {tune, ?DEFAULT_FRAME_MAX, ?DEFAULT_HEARTBEAT} = Tune,

    TuneFrame =
        rabbit_stream_core:frame({response, 0,
                                  {tune, ?DEFAULT_FRAME_MAX, 0}}),
    ok = Transport:send(S, TuneFrame),

    VirtualHost = <<"/">>,
    OpenFrame =
        rabbit_stream_core:frame({request, 3, {open, VirtualHost}}),
    ok = Transport:send(S, OpenFrame),
    {{response, 3, {open, ?RESPONSE_CODE_OK, _ConnectionProperties}},
     C4} =
        receive_commands(Transport, S, C3),
    C4.

test_create_stream(Transport, S, Stream, C0) ->
    CreateStreamFrame =
        rabbit_stream_core:frame({request, 1, {create_stream, Stream, #{}}}),
    ok = Transport:send(S, CreateStreamFrame),
    {Cmd, C} = receive_commands(Transport, S, C0),
    ?assertMatch({response, 1, {create_stream, ?RESPONSE_CODE_OK}}, Cmd),
    C.

test_delete_stream(Transport, S, Stream, C0) ->
    test_delete_stream(Transport, S, Stream, C0, true).

test_delete_stream(Transport, S, Stream, C0, false) ->
    do_test_delete_stream(Transport, S, Stream, C0);
test_delete_stream(Transport, S, Stream, C0, true) ->
    C1 = do_test_delete_stream(Transport, S, Stream, C0),
    test_metadata_update_stream_deleted(Transport, S, Stream, C1).

do_test_delete_stream(Transport, S, Stream, C0) ->
    DeleteStreamFrame =
        rabbit_stream_core:frame({request, 1, {delete_stream, Stream}}),
    ok = Transport:send(S, DeleteStreamFrame),
    {Cmd, C1} = receive_commands(Transport, S, C0),
    ?assertMatch({response, 1, {delete_stream, ?RESPONSE_CODE_OK}}, Cmd),
    C1.

test_metadata_update_stream_deleted(Transport, S, Stream, C0) ->
    {Meta, C1} = receive_commands(Transport, S, C0),
    {metadata_update, Stream, ?RESPONSE_CODE_STREAM_NOT_AVAILABLE} = Meta,
    C1.

test_declare_publisher(Transport, S, PublisherId, Stream, C0) ->
    DeclarePublisherFrame =
        rabbit_stream_core:frame({request, 1,
                                  {declare_publisher,
                                   PublisherId,
                                   <<>>,
                                   Stream}}),
    ok = Transport:send(S, DeclarePublisherFrame),
    {Cmd, C} = receive_commands(Transport, S, C0),
    ?assertMatch({response, 1, {declare_publisher, ?RESPONSE_CODE_OK}},
                 Cmd),
    C.

test_publish_confirm(Transport, S, PublisherId, Body, C0) ->
    BodySize = byte_size(Body),
    Messages = [<<1:64, 0:1, BodySize:31, Body:BodySize/binary>>],
    PublishFrame =
        rabbit_stream_core:frame({publish, PublisherId, 1, Messages}),
    ok = Transport:send(S, PublishFrame),
    {Cmd, C} = receive_commands(Transport, S, C0),
    ?assertMatch({publish_confirm, PublisherId, [1]}, Cmd),
    C.

test_subscribe(Transport, S, SubscriptionId, Stream, C0) ->
    test_subscribe(Transport,
                   S,
                   SubscriptionId,
                   Stream,
                   #{<<"random">> => <<"thing">>},
                   C0).

test_subscribe(Transport,
               S,
               SubscriptionId,
               Stream,
               SubscriptionProperties,
               C0) ->
    SubCmd =
        {request, 1,
         {subscribe, SubscriptionId, Stream, 0, 10, SubscriptionProperties}},
    SubscribeFrame = rabbit_stream_core:frame(SubCmd),
    ok = Transport:send(S, SubscribeFrame),
    {Cmd, C} = receive_commands(Transport, S, C0),
    ?assertMatch({response, 1, {subscribe, ?RESPONSE_CODE_OK}}, Cmd),
    C.

test_unsubscribe(Transport, Socket, SubscriptionId, C0) ->
    UnsubCmd = {request, 1, {unsubscribe, SubscriptionId}},
    UnsubscribeFrame = rabbit_stream_core:frame(UnsubCmd),
    ok = Transport:send(Socket, UnsubscribeFrame),
    {Cmd, C} = receive_commands(Transport, Socket, C0),
    ?assertMatch({response, 1, {unsubscribe, ?RESPONSE_CODE_OK}}, Cmd),
    C.

test_deliver(Transport, S, SubscriptionId, COffset, Body, C0) ->
    ct:pal("test_deliver ", []),
    {{deliver, SubscriptionId, Chunk}, C} =
        receive_commands(Transport, S, C0),
    <<5:4/unsigned,
      0:4/unsigned,
      0:8,
      1:16,
      1:32,
      _Timestamp:64,
      _Epoch:64,
      COffset:64,
      _Crc:32,
      _DataLength:32,
      _TrailerLength:32,
      _ReservedBytes:32,
      0:1,
      BodySize:31/unsigned,
      Body:BodySize/binary>> =
        Chunk,
    C.

test_deliver_v2(Transport, S, SubscriptionId, COffset, Body, C0) ->
    ct:pal("test_deliver ", []),
    {{deliver_v2, SubscriptionId, _CommittedOffset, Chunk}, C} =
        receive_commands(Transport, S, C0),
    <<5:4/unsigned,
      0:4/unsigned,
      0:8,
      1:16,
      1:32,
      _Timestamp:64,
      _Epoch:64,
      COffset:64,
      _Crc:32,
      _DataLength:32,
      _TrailerLength:32,
      _ReservedBytes:32,
      0:1,
      BodySize:31/unsigned,
      Body:BodySize/binary>> =
        Chunk,
    C.

test_exchange_command_versions(Transport, S, C0) ->
    ExCmd =
        {request, 1,
         {exchange_command_versions, [{deliver, ?VERSION_1, ?VERSION_2}]}},
    ExFrame = rabbit_stream_core:frame(ExCmd),
    ok = Transport:send(S, ExFrame),
    {Cmd, C} = receive_commands(Transport, S, C0),
    ?assertMatch({response, 1,
                  {exchange_command_versions, ?RESPONSE_CODE_OK,
                   [{declare_publisher, _, _} | _]}},
                 Cmd),
    C.

test_stream_stats(Transport, S, Stream, C0) ->
    SICmd = {request, 1, {stream_stats, Stream}},
    SIFrame = rabbit_stream_core:frame(SICmd),
    ok = Transport:send(S, SIFrame),
    {Cmd, C} = receive_commands(Transport, S, C0),
    ?assertMatch({response, 1,
                  {stream_stats, ?RESPONSE_CODE_OK,
                   #{<<"first_chunk_id">> := 0,
                     <<"committed_chunk_id">> := 1}}},
                 Cmd),
    C.

test_close(Transport, S, C0) ->
    CloseReason = <<"OK">>,
    CloseFrame =
        rabbit_stream_core:frame({request, 1,
                                  {close, ?RESPONSE_CODE_OK, CloseReason}}),
    ok = Transport:send(S, CloseFrame),
    {{response, 1, {close, ?RESPONSE_CODE_OK}}, C} =
        receive_commands(Transport, S, C0),
    C.

wait_for_socket_close(_Transport, _S, 0) ->
    not_closed;
wait_for_socket_close(Transport, S, Attempt) ->
    case Transport:recv(S, 0, 1000) of
        {error, timeout} ->
            wait_for_socket_close(Transport, S, Attempt - 1);
        {error, closed} ->
            closed
    end.

receive_commands(Transport, S, C0) ->
    case rabbit_stream_core:next_command(C0) of
        empty ->
            case Transport:recv(S, 0, 5000) of
                {ok, Data} ->
                    C1 = rabbit_stream_core:incoming_data(Data, C0),
                    case rabbit_stream_core:next_command(C1) of
                        empty ->
                            {ok, Data2} = Transport:recv(S, 0, 5000),
                            rabbit_stream_core:next_command(
                                rabbit_stream_core:incoming_data(Data2, C1));
                        Res ->
                            Res
                    end;
                {error, Err} ->
                    ct:fail("error receiving data ~w", [Err])
            end;
        Res ->
            Res
    end.

get_osiris_counters(Config) ->
    rabbit_ct_broker_helpers:rpc(Config,
                                 0,
                                 osiris_counters,
                                 overview,
                                 []).

get_global_counters(Config) ->
    maps:get([{protocol, stream}],
             rabbit_ct_broker_helpers:rpc(Config,
                                          0,
                                          rabbit_global_counters,
                                          overview,
                                          [])).
