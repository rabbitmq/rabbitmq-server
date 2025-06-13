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
%% Copyright (c) 2020-2025 Broadcom. All Rights Reserved.
%% The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_stream_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").
-include_lib("rabbitmq_stream_common/include/rabbit_stream.hrl").

-include("rabbit_stream_metrics.hrl").

-compile(nowarn_export_all).
-compile(export_all).

-import(rabbit_stream_core, [frame/1]).
-import(rabbit_ct_broker_helpers, [rpc/5]).

-define(WAIT, 5000).

all() ->
    [{group, single_node}, {group, single_node_1}, {group, cluster}].

groups() ->
    [{single_node, [],
      [test_stream,
       test_stream_tls,
       test_publish_v2,
       test_super_stream_creation_deletion,
       test_gc_consumers,
       test_gc_publishers,
       test_update_secret,
       cannot_update_username_after_authenticated,
       cannot_use_another_authmechanism_when_updating_secret,
       unauthenticated_client_rejected_tcp_connected,
       timeout_tcp_connected,
       unauthenticated_client_rejected_peer_properties_exchanged,
       timeout_peer_properties_exchanged,
       unauthenticated_client_rejected_authenticating,
       timeout_authenticating,
       timeout_close_sent,
       max_segment_size_bytes_validation,
       close_connection_on_consumer_update_timeout,
       set_filter_size,
       vhost_queue_limit,
       connection_should_be_closed_on_token_expiry,
       should_receive_metadata_update_after_update_secret,
       store_offset_requires_read_access,
       offset_lag_calculation,
       test_super_stream_duplicate_partitions,
       authentication_error_should_close_with_delay,
       unauthorized_vhost_access_should_close_with_delay,
       sasl_anonymous,
       test_publisher_with_too_long_reference_errors,
       test_consumer_with_too_long_reference_errors,
       subscribe_unsubscribe_should_create_events,
       test_stream_test_utils,
       sac_subscription_with_partition_index_conflict_should_return_error
      ]},
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
    %% filtering feature flag disabled for the first test,
    %% then enabled in the end_per_testcase function
    ExtraSetupSteps =
        case Group of
            single_node ->
                [fun(StepConfig) ->
                    rabbit_ct_helpers:merge_app_env(StepConfig,
                                                    {rabbit,
                                                     [{forced_feature_flags_on_init,
                                                       [stream_queue,
                                                        stream_sac_coordinator_unblock_group,
                                                        stream_single_active_consumer]}]})
                 end];
            _ ->
                []
        end,
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
      ++ ExtraSetupSteps
      ++ rabbit_ct_broker_helpers:setup_steps());
init_per_group(cluster = Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(
                Config, [{rmq_nodes_clustered, true},
                         {rmq_nodes_count, 3},
                         {rmq_nodename_suffix, Group},
                         {tcp_ports_base},
                         {rabbitmq_ct_tls_verify, verify_none},
                         {find_crashes, false} %% we kill stream members in some tests
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

init_per_testcase(test_update_secret = TestCase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, TestCase);

init_per_testcase(cannot_update_username_after_authenticated = TestCase, Config) ->
  ok = rabbit_ct_broker_helpers:add_user(Config, <<"other">>),
  rabbit_ct_helpers:testcase_started(Config, TestCase);

init_per_testcase(close_connection_on_consumer_update_timeout = TestCase, Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config,
                                      0,
                                      application,
                                      set_env,
                                      [rabbitmq_stream, request_timeout, 2000]),
    rabbit_ct_helpers:testcase_started(Config, TestCase);
init_per_testcase(vhost_queue_limit = TestCase, Config) ->
    QueueCount = rabbit_ct_broker_helpers:rpc(Config,
                                              0,
                                              rabbit_amqqueue,
                                              count,
                                              [<<"/">>]),
    ok = rabbit_ct_broker_helpers:set_vhost_limit(Config, 0, <<"/">>, max_queues, QueueCount + 5),
    rabbit_ct_helpers:testcase_started(Config, TestCase);

init_per_testcase(store_offset_requires_read_access = TestCase, Config) ->
  ok = rabbit_ct_broker_helpers:add_user(Config, <<"test">>),
  rabbit_ct_helpers:testcase_started(Config, TestCase);

init_per_testcase(unauthorized_vhost_access_should_close_with_delay = TestCase, Config) ->
  ok = rabbit_ct_broker_helpers:add_user(Config, <<"other">>),
  rabbit_ct_helpers:testcase_started(Config, TestCase);

init_per_testcase(TestCase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, TestCase).

end_per_testcase(test_update_secret = TestCase, Config) ->
    ok = rabbit_ct_broker_helpers:change_password(Config, <<"guest">>, <<"guest">>),
    rabbit_ct_helpers:testcase_finished(Config, TestCase);

end_per_testcase(cannot_update_username_after_authenticated = TestCase, Config) ->
    ok = rabbit_ct_broker_helpers:delete_user(Config, <<"other">>),
    rabbit_ct_helpers:testcase_finished(Config, TestCase);

end_per_testcase(close_connection_on_consumer_update_timeout = TestCase, Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config,
                                      0,
                                      application,
                                      set_env,
                                      [rabbitmq_stream, request_timeout, 60000]),
    rabbit_ct_helpers:testcase_finished(Config, TestCase);
end_per_testcase(vhost_queue_limit = TestCase, Config) ->
    _ = rabbit_ct_broker_helpers:rpc(Config,
                                     0,
                                     rabbit_vhost_limit,
                                     clear,
                                     [<<"/">>, <<"guest">>]),
    rabbit_ct_helpers:testcase_finished(Config, TestCase);
end_per_testcase(store_offset_requires_read_access = TestCase, Config) ->
    ok = rabbit_ct_broker_helpers:delete_user(Config, <<"test">>),
    rabbit_ct_helpers:testcase_finished(Config, TestCase);
end_per_testcase(unauthorized_vhost_access_should_close_with_delay = TestCase, Config) ->
    ok = rabbit_ct_broker_helpers:delete_user(Config, <<"other">>),
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

sasl_anonymous(Config) ->
    Port = get_port(gen_tcp, Config),
    Opts = get_opts(gen_tcp),
    {ok, S} = gen_tcp:connect("localhost", Port, Opts),
    C0 = rabbit_stream_core:init(0),
    C1 = test_peer_properties(gen_tcp, S, C0),
    C2 = sasl_handshake(gen_tcp, S, C1),
    C3 = test_anonymous_sasl_authenticate(gen_tcp, S, C2),
    _C = tune(gen_tcp, S, C3).

test_update_secret(Config) ->
    Transport = gen_tcp,
    {S, C0} = connect_and_authenticate(Transport, Config),
    rabbit_ct_broker_helpers:change_password(Config, <<"guest">>, <<"password">>),
    C1 = expect_successful_authentication(
      try_authenticate(Transport, S, C0, <<"PLAIN">>, <<"guest">>, <<"password">>)),
    _C2 = test_close(Transport, S, C1),
    closed = wait_for_socket_close(Transport, S, 10),
    ok.

cannot_update_username_after_authenticated(Config) ->
    {S, C0} = connect_and_authenticate(gen_tcp, Config),
    C1 = expect_unsuccessful_authentication(
      try_authenticate(gen_tcp, S, C0, <<"PLAIN">>, <<"other">>, <<"other">>),
        ?RESPONSE_SASL_CANNOT_CHANGE_USERNAME),
    _C2 = test_close(gen_tcp, S, C1),
    closed = wait_for_socket_close(gen_tcp, S, 10),
    ok.

cannot_use_another_authmechanism_when_updating_secret(Config) ->
    {S, C0} = connect_and_authenticate(gen_tcp, Config),
    C1 = expect_unsuccessful_authentication(
      try_authenticate(gen_tcp, S, C0, <<"EXTERNAL">>, <<"guest">>, <<"new_password">>),
        ?RESPONSE_SASL_CANNOT_CHANGE_MECHANISM),
    _C2 = test_close(gen_tcp, S, C1),
    closed = wait_for_socket_close(gen_tcp, S, 10),
    ok.

test_stream_tls(Config) ->
    Stream = atom_to_binary(?FUNCTION_NAME, utf8),
    test_server(ssl, Stream, Config),
    ok.

test_publish_v2(Config) ->
    Stream = atom_to_binary(?FUNCTION_NAME, utf8),
    Transport = gen_tcp,
    Port = get_stream_port(Config),
    Opts = [{active, false}, {mode, binary}],
    {ok, S} =
        Transport:connect("localhost", Port, Opts),
    C0 = rabbit_stream_core:init(0),
    C1 = test_peer_properties(Transport, S, C0),
    C2 = test_authenticate(Transport, S, C1),
    C3 = test_create_stream(Transport, S, Stream, C2),
    PublisherId = 42,
    C4 = test_declare_publisher(Transport, S, PublisherId, Stream, C3),
    Body = <<"hello">>,
    C5 = test_publish_confirm(Transport, S, publish_v2, PublisherId, Body,
                              publish_confirm, C4),
    C6 = test_publish_confirm(Transport, S, publish_v2, PublisherId, Body,
                              publish_confirm, C5),
    SubscriptionId = 42,
    C7 = test_subscribe(Transport, S, SubscriptionId, Stream,
                        #{<<"filter.0">> => <<"foo">>},
                        ?RESPONSE_CODE_OK,
                        C6),
    C8 = test_deliver(Transport, S, SubscriptionId, 0, Body, C7),
    C8b = test_deliver(Transport, S, SubscriptionId, 1, Body, C8),

    C9 = test_unsubscribe(Transport, S, SubscriptionId, C8b),

    C10 = test_delete_stream(Transport, S, Stream, C9),
    _C11 = test_close(Transport, S, C10),
    closed = wait_for_socket_close(Transport, S, 10),
    ok.


test_super_stream_creation_deletion(Config) ->
    T = gen_tcp,
    Port = get_port(T, Config),
    Opts = get_opts(T),
    {ok, S} = T:connect("localhost", Port, Opts),
    C = rabbit_stream_core:init(0),
    test_peer_properties(T, S, C),
    test_authenticate(T, S, C),

    Ss = atom_to_binary(?FUNCTION_NAME, utf8),
    Partitions = [unicode:characters_to_binary([Ss, <<"-">>, integer_to_binary(N)]) || N <- lists:seq(0, 2)],
    Bks = [integer_to_binary(N) || N <- lists:seq(0, 2)],
    SsCreationFrame = request({create_super_stream, Ss, Partitions, Bks, #{}}),
    ok = T:send(S, SsCreationFrame),
    {Cmd1, _} = receive_commands(T, S, C),
    ?assertMatch({response, 1, {create_super_stream, ?RESPONSE_CODE_OK}},
                 Cmd1),

    PartitionsFrame = request({partitions, Ss}),
    ok = T:send(S, PartitionsFrame),
    {Cmd2, _} = receive_commands(T, S, C),
    ?assertMatch({response, 1, {partitions, ?RESPONSE_CODE_OK, Partitions}},
                 Cmd2),
    [begin
         RouteFrame = request({route, Rk, Ss}),
         ok = T:send(S, RouteFrame),
         {Command, _} = receive_commands(T, S, C),
         ?assertMatch({response, 1, {route, ?RESPONSE_CODE_OK, _}}, Command),
         {response, 1, {route, ?RESPONSE_CODE_OK, [P]}} = Command,
         ?assertEqual(unicode:characters_to_binary([Ss, <<"-">>, Rk]), P)
     end || Rk <- Bks],

    SsDeletionFrame = request({delete_super_stream, Ss}),
    ok = T:send(S, SsDeletionFrame),
    {Cmd3, _} = receive_commands(T, S, C),
    ?assertMatch({response, 1, {delete_super_stream, ?RESPONSE_CODE_OK}},
                 Cmd3),

    ok = T:send(S, PartitionsFrame),
    {Cmd4, _} = receive_commands(T, S, C),
    ?assertMatch({response, 1, {partitions, ?RESPONSE_CODE_STREAM_DOES_NOT_EXIST, []}},
                 Cmd4),

    %% not the same number of partitions and binding keys
    SsCreationBadFrame = request({create_super_stream, Ss,
                                  [<<"s1">>, <<"s2">>], [<<"bk1">>], #{}}),
    ok = T:send(S, SsCreationBadFrame),
    {Cmd5, _} = receive_commands(T, S, C),
    ?assertMatch({response, 1, {create_super_stream, ?RESPONSE_CODE_PRECONDITION_FAILED}},
                 Cmd5),

    test_close(T, S, C),
    closed = wait_for_socket_close(T, S, 10),
    ok.

test_super_stream_duplicate_partitions(Config) ->
    T = gen_tcp,
    Port = get_port(T, Config),
    Opts = get_opts(T),
    {ok, S} = T:connect("localhost", Port, Opts),
    C = rabbit_stream_core:init(0),
    test_peer_properties(T, S, C),
    test_authenticate(T, S, C),

    Ss = atom_to_binary(?FUNCTION_NAME, utf8),
    Partitions = [<<"same-name">>, <<"same-name">>],
    SsCreationFrame = request({create_super_stream, Ss, Partitions, [<<"1">>, <<"2">>], #{}}),
    ok = T:send(S, SsCreationFrame),
    {Cmd1, _} = receive_commands(T, S, C),
    ?assertMatch({response, 1, {create_super_stream, ?RESPONSE_CODE_PRECONDITION_FAILED}},
                 Cmd1),

    test_close(T, S, C),
    closed = wait_for_socket_close(T, S, 10),
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
           MetadataFrame = request({metadata, [Stream]}),
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

    DeleteStreamFrame = request({delete_stream, Stream}),
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
                                  #{},
                                  <<"guest">>]),
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
    {ok, S} = gen_tcp:connect("localhost", Port, [{active, false}, {mode, binary}]),
    ?assertEqual(ok, gen_tcp:send(S, <<"invalid data">>)),
    ?assertEqual(closed, wait_for_socket_close(gen_tcp, S, 1)).

timeout_tcp_connected(Config) ->
    Port = get_stream_port(Config),
    {ok, S} = gen_tcp:connect("localhost", Port, [{active, false}, {mode, binary}]),
    ?assertEqual(closed, wait_for_socket_close(gen_tcp, S, 1)).

unauthenticated_client_rejected_peer_properties_exchanged(Config) ->
    Port = get_stream_port(Config),
    {ok, S} = gen_tcp:connect("localhost", Port, [{active, false}, {mode, binary}]),
    C0 = rabbit_stream_core:init(0),
    test_peer_properties(gen_tcp, S, C0),
    ?assertEqual(ok, gen_tcp:send(S, <<"invalid data">>)),
    ?assertEqual(closed, wait_for_socket_close(gen_tcp, S, 1)).

timeout_peer_properties_exchanged(Config) ->
    Port = get_stream_port(Config),
    {ok, S} = gen_tcp:connect("localhost", Port, [{active, false}, {mode, binary}]),
    C0 = rabbit_stream_core:init(0),
    test_peer_properties(gen_tcp, S, C0),
    ?assertEqual(closed, wait_for_socket_close(gen_tcp, S, 1)).

unauthenticated_client_rejected_authenticating(Config) ->
    Port = get_stream_port(Config),
    {ok, S} = gen_tcp:connect("localhost", Port, [{active, false}, {mode, binary}]),
    C0 = rabbit_stream_core:init(0),
    test_peer_properties(gen_tcp, S, C0),
    SaslHandshakeFrame = request(sasl_handshake),
    ?assertEqual(ok, gen_tcp:send(S, SaslHandshakeFrame)),
    ?awaitMatch({error, closed}, gen_tcp:send(S, <<"invalid data">>),
                ?WAIT).

timeout_authenticating(Config) ->
    Port = get_stream_port(Config),
    {ok, S} = gen_tcp:connect("localhost", Port, [{active, false}, {mode, binary}]),
    C0 = rabbit_stream_core:init(0),
    test_peer_properties(gen_tcp, S, C0),
    _Frame = request(sasl_handshake),
    ?assertEqual(closed, wait_for_socket_close(gen_tcp, S, 1)).

timeout_close_sent(Config) ->
    Port = get_stream_port(Config),
    {ok, S} = gen_tcp:connect("localhost", Port, [{active, false}, {mode, binary}]),
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
    {ok, S} = Transport:connect("localhost", Port,
                                [{active, false}, {mode, binary}]),
    C0 = rabbit_stream_core:init(0),
    C1 = test_peer_properties(Transport, S, C0),
    C2 = test_authenticate(Transport, S, C1),
    Stream = <<"stream-max-segment-size">>,
    CreateStreamFrame = request({create_stream, Stream,
                                 #{<<"stream-max-segment-size-bytes">> =>
                                   <<"3000000001">>}}),
    ok = Transport:send(S, CreateStreamFrame),
    {Cmd, C3} = receive_commands(Transport, S, C2),
    ?assertMatch({response, 1,
                  {create_stream, ?RESPONSE_CODE_PRECONDITION_FAILED}},
                 Cmd),
    test_close(Transport, S, C3),
    ok.

close_connection_on_consumer_update_timeout(Config) ->
    Stream = atom_to_binary(?FUNCTION_NAME, utf8),
    {ok, S, C0} = stream_test_utils:connect(Config, 0),
    {ok, C1} = stream_test_utils:create_stream(S, C0, Stream),

    SubId = 42,
    Props = #{<<"single-active-consumer">> => <<"true">>,
              <<"name">> => <<"foo">>},
    {ok, C2} = stream_test_utils:subscribe(S, C1, Stream, SubId, 10, Props),

    {Cmd, _C3} = receive_commands(S, C2),
    ?assertMatch({request, _, {consumer_update, SubId, true}}, Cmd),
    closed = wait_for_socket_close(S, 10),

    {ok, Sb, Cb0} = stream_test_utils:connect(Config, 0),
    {ok, Cb1} = stream_test_utils:delete_stream(Sb, Cb0, Stream),
    stream_test_utils:close(Sb, Cb1),
    closed = wait_for_socket_close(Sb, 10),
    ok.

set_filter_size(Config) ->
    Stream = atom_to_binary(?FUNCTION_NAME, utf8),
    Transport = gen_tcp,
    Port = get_stream_port(Config),
    Opts = [{active, false}, {mode, binary}],
    {ok, S} = Transport:connect("localhost", Port, Opts),
    C0 = rabbit_stream_core:init(0),
    C1 = test_peer_properties(Transport, S, C0),
    C2 = test_authenticate(Transport, S, C1),

    Tests = [
             {128, ?RESPONSE_CODE_OK},
             {15, ?RESPONSE_CODE_PRECONDITION_FAILED},
             {256, ?RESPONSE_CODE_PRECONDITION_FAILED}
            ],

    C3 = lists:foldl(fun({Size, ExpectedResponseCode}, Conn0) ->
                             Frame = request({create_stream, Stream,
                                              #{<<"stream-filter-size-bytes">> => integer_to_binary(Size)}}),
                             ok = Transport:send(S, Frame),
                             {Cmd, Conn1} = receive_commands(Transport, S, Conn0),
                             ?assertMatch({response, 1, {create_stream, ExpectedResponseCode}}, Cmd),
                             Conn1
                     end, C2, Tests),

    _ = test_close(Transport, S, C3),
    closed = wait_for_socket_close(Transport, S, 10),
    ok.

vhost_queue_limit(Config) ->
    T = gen_tcp,
    Port = get_port(T, Config),
    Opts = get_opts(T),
    {ok, S} = T:connect("localhost", Port, Opts),
    C = rabbit_stream_core:init(0),
    test_peer_properties(T, S, C),
    test_authenticate(T, S, C),
    QueueCount = rabbit_ct_broker_helpers:rpc(Config,
                                              0,
                                              rabbit_amqqueue,
                                              count,
                                              [<<"/">>]),
    {ok, QueueLimit} = rabbit_ct_broker_helpers:rpc(Config,
                                                    0,
                                                    rabbit_vhost_limit,
                                                    queue_limit,
                                                    [<<"/">>]),

    PartitionCount = QueueLimit - 1 - QueueCount,
    Name = atom_to_binary(?FUNCTION_NAME, utf8),
    Partitions = [unicode:characters_to_binary([Name, <<"-">>, integer_to_binary(N)]) || N <- lists:seq(0, PartitionCount)],
    Bks = [integer_to_binary(N) || N <- lists:seq(0, PartitionCount)],
    SsCreationFrame = request({create_super_stream, Name, Partitions, Bks, #{}}),
    ok = T:send(S, SsCreationFrame),
    {Cmd1, _} = receive_commands(T, S, C),
    ?assertMatch({response, 1, {create_super_stream, ?RESPONSE_CODE_OK}},
                 Cmd1),

    SsCreationFrameKo = request({create_super_stream,
                                 <<"exceed-queue-limit">>,
                                 [<<"s1">>, <<"s2">>, <<"s3">>],
                                 [<<"1">>, <<"2">>, <<"3">>], #{}}),

    ok = T:send(S, SsCreationFrameKo),
    {Cmd2, _} = receive_commands(T, S, C),
    ?assertMatch({response, 1, {create_super_stream, ?RESPONSE_CODE_PRECONDITION_FAILED}},
                 Cmd2),

    CreateStreamFrame = request({create_stream, <<"exceed-queue-limit">>, #{}}),
    ok = T:send(S, CreateStreamFrame),
    {Cmd3, C} = receive_commands(T, S, C),
    ?assertMatch({response, 1, {create_stream, ?RESPONSE_CODE_PRECONDITION_FAILED}}, Cmd3),

    SsDeletionFrame = request({delete_super_stream, Name}),
    ok = T:send(S, SsDeletionFrame),
    {Cmd4, _} = receive_commands(T, S, C),
    ?assertMatch({response, 1, {delete_super_stream, ?RESPONSE_CODE_OK}},
                 Cmd4),

    ok = T:send(S, request({create_stream, Name, #{}})),
    {Cmd5, C} = receive_commands(T, S, C),
    ?assertMatch({response, 1, {create_stream, ?RESPONSE_CODE_OK}}, Cmd5),

    ok = T:send(S, request({delete_stream, Name})),
    {Cmd6, C} = receive_commands(T, S, C),
    ?assertMatch({response, 1, {delete_stream, ?RESPONSE_CODE_OK}}, Cmd6),

    ok.

connection_should_be_closed_on_token_expiry(Config) ->
    rabbit_ct_broker_helpers:setup_meck(Config),
    Mod = rabbit_access_control,
    ok = rpc(Config, 0, meck, new, [Mod, [no_link, passthrough]]),
    ok = rpc(Config, 0, meck, expect, [Mod, check_user_loopback, 2, ok]),
    ok = rpc(Config, 0, meck, expect, [Mod, check_vhost_access, 4, ok]),
    ok = rpc(Config, 0, meck, expect, [Mod, permission_cache_can_expire, 1, true]),
    Expiry = os:system_time(seconds) + 2,
    ok = rpc(Config, 0, meck, expect, [Mod, expiry_timestamp, 1, Expiry]),

    T = gen_tcp,
    Port = get_port(T, Config),
    Opts = get_opts(T),
    {ok, S} = T:connect("localhost", Port, Opts),
    C = rabbit_stream_core:init(0),
    test_peer_properties(T, S, C),
    test_authenticate(T, S, C),
    closed = wait_for_socket_close(T, S, 10),
    ok = rpc(Config, 0, meck, unload, [Mod]).

should_receive_metadata_update_after_update_secret(Config) ->
    T = gen_tcp,
    Port = get_port(T, Config),
    Opts = get_opts(T),
    {ok, S} = T:connect("localhost", Port, Opts),
    C = rabbit_stream_core:init(0),
    test_peer_properties(T, S, C),
    test_authenticate(T, S, C),

    Prefix = atom_to_binary(?FUNCTION_NAME, utf8),
    PublishStream = <<Prefix/binary, <<"-publish">>/binary>>,
    test_create_stream(T, S, PublishStream, C),
    ConsumeStream = <<Prefix/binary, <<"-consume">>/binary>>,
    test_create_stream(T, S, ConsumeStream, C),

    test_declare_publisher(T, S, 1, PublishStream, C),
    test_subscribe(T, S, 1, ConsumeStream, C),

    rabbit_ct_broker_helpers:setup_meck(Config),
    Mod = rabbit_stream_utils,
    ok = rpc(Config, 0, meck, new, [Mod, [no_link, passthrough]]),
    ok = rpc(Config, 0, meck, expect, [Mod, check_write_permitted, 2, error]),
    ok = rpc(Config, 0, meck, expect, [Mod, check_read_permitted, 3, error]),

    C01 = expect_successful_authentication(try_authenticate(T, S, C, <<"PLAIN">>, <<"guest">>, <<"guest">>)),

    {Meta1, C02} = receive_commands(T, S, C01),
    {metadata_update, Stream1, ?RESPONSE_CODE_STREAM_NOT_AVAILABLE} = Meta1,
    {Meta2, C03} = receive_commands(T, S, C02),
    {metadata_update, Stream2, ?RESPONSE_CODE_STREAM_NOT_AVAILABLE} = Meta2,
    ImpactedStreams = #{Stream1 => ok, Stream2 => ok},
    ?assert(maps:is_key(PublishStream, ImpactedStreams)),
    ?assert(maps:is_key(ConsumeStream, ImpactedStreams)),

    test_close(T, S, C03),
    closed = wait_for_socket_close(T, S, 10),

    ok = rpc(Config, 0, meck, unload, [Mod]),

    {ok, S2} = T:connect("localhost", Port, Opts),
    C2 = rabbit_stream_core:init(0),
    test_peer_properties(T, S2, C2),
    test_authenticate(T, S2, C2),
    test_delete_stream(T, S2, PublishStream, C2, false),
    test_delete_stream(T, S2, ConsumeStream, C2, false),
    test_close(T, S2, C2),
    closed = wait_for_socket_close(T, S2, 10),
    ok.

store_offset_requires_read_access(Config) ->
    Username = <<"test">>,
    rabbit_ct_broker_helpers:set_full_permissions(Config, Username, <<"/">>),

    T = gen_tcp,
    Port = get_port(T, Config),
    Opts = get_opts(T),
    {ok, S} = T:connect("localhost", Port, Opts),
    C0 = rabbit_stream_core:init(0),
    C1 = test_peer_properties(T, S, C0),
    C2 = test_authenticate(T, S, C1, Username),
    Stream = atom_to_binary(?FUNCTION_NAME, utf8),
    C3 = test_create_stream(T, S, Stream, C2),

    C4 = test_subscribe(T, S, 1, Stream, C3),
    %% store_offset should work because the subscription is still active
    Reference = <<"foo">>,
    ok = store_offset(T, S, Reference, Stream, 42),
    {O42, C5} = query_expected_offset(T, S, C4, Reference, Stream, 42),
    ?assertEqual(42, O42),

    C6 = test_unsubscribe(T, S, 1, C5),
    %% store_offset should still work because the user has read access to the stream
    ok = store_offset(T, S, Reference, Stream, 43),
    {O43, C7} = query_expected_offset(T, S, C6, Reference, Stream, 43),
    ?assertEqual(43, O43),

    %% no read access anymore
    rabbit_ct_broker_helpers:set_permissions(Config, Username, <<"/">>,
                                             <<".*">>, <<".*">>, <<"foobar">>),
    %% this store_offset request will not work because no read access
    ok = store_offset(T, S, Reference, Stream, 44),

    %% providing read access back to be able to query_offset
    rabbit_ct_broker_helpers:set_full_permissions(Config, Username, <<"/">>),
    %% we never get the offset from the last query_offset attempt
    {Timeout, C8} = query_expected_offset(T, S, C7, Reference, Stream, 44),
    ?assertMatch(timeout, Timeout),

    C9 = test_delete_stream(T, S, Stream, C8, true),
    test_close(T, S, C9),
    closed = wait_for_socket_close(T, S, 10),
    ok.

offset_lag_calculation(Config) ->
    FunctionName = atom_to_binary(?FUNCTION_NAME, utf8),
    T = gen_tcp,
    Port = get_port(T, Config),
    Opts = get_opts(T),
    {ok, S} = T:connect("localhost", Port, Opts),
    C = rabbit_stream_core:init(0),
    ConnectionName = FunctionName,
    test_peer_properties(T, S, #{<<"connection_name">> => ConnectionName}, C),
    test_authenticate(T, S, C),

    Stream = FunctionName,
    test_create_stream(T, S, Stream, C),

    SubId = 1,
    TheFuture = os:system_time(millisecond) + 60 * 60 * 1_000,
    lists:foreach(fun(OffsetSpec) ->
                          test_subscribe(T, S, SubId, Stream,
                                         OffsetSpec, 10, #{},
                                         ?RESPONSE_CODE_OK, C),
                          ConsumerInfo = consumer_offset_info(Config, ConnectionName),
                          ?assertEqual({0, 0}, ConsumerInfo),
                          test_unsubscribe(T, S, SubId, C)
                  end, [first, last, next, 0, 1_000, {timestamp, TheFuture}]),


    PublisherId = 1,
    test_declare_publisher(T, S, PublisherId, Stream, C),
    MessageCount = 10,
    Body = <<"hello">>,
    lists:foreach(fun(_) ->
                          test_publish_confirm(T, S, PublisherId, Body, C)
                  end, lists:seq(1, MessageCount - 1)),
    %% to make sure to have 2 chunks
    timer:sleep(200),
    test_publish_confirm(T, S, PublisherId, Body, C),
    test_delete_publisher(T, S, PublisherId, C),

    NextOffset = MessageCount,
    lists:foreach(fun({OffsetSpec, ReceiveDeliver, CheckFun}) ->
                          test_subscribe(T, S, SubId, Stream,
                                         OffsetSpec, 1, #{},
                                         ?RESPONSE_CODE_OK, C),
                          case ReceiveDeliver of
                              true ->
                                  {{deliver, SubId, _}, _} = receive_commands(T, S, C);
                              _ ->
                                  ok
                          end,
                          {Offset, Lag} = consumer_offset_info(Config, ConnectionName),
                          CheckFun(Offset, Lag),
                          test_unsubscribe(T, S, SubId, C)
                  end, [{first, true,
                         fun(Offset, Lag) ->
                                 ?assert(Offset >= 0, "first, at least one chunk consumed"),
                                 ?assert(Lag > 0, "first, not all messages consumed")
                         end},
                        {last, true,
                         fun(Offset, _Lag) ->
                                 ?assert(Offset > 0, "offset expected for last")
                         end},
                        {next, false,
                         fun(Offset, Lag) ->
                                 ?assertEqual(NextOffset, Offset, "next, offset should be at the end of the stream"),
                                 ?assert(Lag =:= 0, "next, offset lag should be 0")
                         end},
                        {0, true,
                         fun(Offset, Lag) ->
                                 ?assert(Offset >= 0, "offset spec = 0, at least one chunk consumed"),
                                 ?assert(Lag > 0, "offset spec = 0, not all messages consumed")
                         end},
                        {1_000, false,
                         fun(Offset, Lag) ->
                                 ?assertEqual(NextOffset, Offset, "offset spec = 1000, offset should be at the end of the stream"),
                                 ?assert(Lag =:= 0, "offset spec = 1000, offset lag should be 0")
                         end},
                        {{timestamp, TheFuture}, false,
                         fun(Offset, Lag) ->
                                 ?assertEqual(NextOffset, Offset, "offset spec in future, offset should be at the end of the stream"),
                                 ?assert(Lag =:= 0, "offset spec in future , offset lag should be 0")
                         end}]),

    test_delete_stream(T, S, Stream, C, false),
    test_close(T, S, C),

    ok.

authentication_error_should_close_with_delay(Config) ->
    T = gen_tcp,
    Port = get_port(T, Config),
    Opts = get_opts(T),
    {ok, S} = T:connect("localhost", Port, Opts),
    C0 = rabbit_stream_core:init(0),
    C1 = test_peer_properties(T, S, C0),
    Start = erlang:monotonic_time(millisecond),
    _ = expect_unsuccessful_authentication(
      try_authenticate(T, S, C1, <<"PLAIN">>, <<"guest">>, <<"wrong password">>),
        ?RESPONSE_AUTHENTICATION_FAILURE),
    End = erlang:monotonic_time(millisecond),
    %% the stream reader module defines the delay (3 seconds)
    ?assert(End - Start > 2_000),
    closed = wait_for_socket_close(T, S, 10),
    ok.

unauthorized_vhost_access_should_close_with_delay(Config) ->
    T = gen_tcp,
    Port = get_port(T, Config),
    Opts = get_opts(T),
    {ok, S} = T:connect("localhost", Port, Opts),
    C0 = rabbit_stream_core:init(0),
    C1 = test_peer_properties(T, S, C0),
    User = <<"other">>,
    C2 = test_plain_sasl_authenticate(T, S, sasl_handshake(T, S, C1), User),
    Start = erlang:monotonic_time(millisecond),
    R = do_tune(T, S, C2),
    ?assertMatch({{response,_,{open,12}}, _}, R),
    End = erlang:monotonic_time(millisecond),
    %% the stream reader module defines the delay (3 seconds)
    ?assert(End - Start > 2_000),
    closed = wait_for_socket_close(T, S, 10),
    ok.

test_publisher_with_too_long_reference_errors(Config) ->
  FunctionName = atom_to_binary(?FUNCTION_NAME, utf8),
  T = gen_tcp,
  Port = get_port(T, Config),
  Opts = get_opts(T),
  {ok, S} = T:connect("localhost", Port, Opts),
  C = rabbit_stream_core:init(0),
  ConnectionName = FunctionName,
  test_peer_properties(T, S, #{<<"connection_name">> => ConnectionName}, C),
  test_authenticate(T, S, C),

  Stream = FunctionName,
  test_create_stream(T, S, Stream, C),

  MaxSize = 255,
  ReferenceOK = iolist_to_binary(lists:duplicate(MaxSize, <<"a">>)),
  ReferenceKO = iolist_to_binary(lists:duplicate(MaxSize + 1, <<"a">>)),

  Tests = [{1, ReferenceOK, ?RESPONSE_CODE_OK},
           {2, ReferenceKO, ?RESPONSE_CODE_PRECONDITION_FAILED}],

  [begin
     F = request({declare_publisher, PubId, Ref, Stream}),
     ok = T:send(S, F),
     {Cmd, C} = receive_commands(T, S, C),
     ?assertMatch({response, 1, {declare_publisher, ExpectedResponseCode}}, Cmd)
   end || {PubId, Ref, ExpectedResponseCode} <- Tests],

  test_delete_stream(T, S, Stream, C),
  test_close(T, S, C),
  ok.

test_consumer_with_too_long_reference_errors(Config) ->
  FunctionName = atom_to_binary(?FUNCTION_NAME, utf8),
  T = gen_tcp,
  Port = get_port(T, Config),
  Opts = get_opts(T),
  {ok, S} = T:connect("localhost", Port, Opts),
  C = rabbit_stream_core:init(0),
  ConnectionName = FunctionName,
  test_peer_properties(T, S, #{<<"connection_name">> => ConnectionName}, C),
  test_authenticate(T, S, C),

  Stream = FunctionName,
  test_create_stream(T, S, Stream, C),

  MaxSize = 255,
  ReferenceOK = iolist_to_binary(lists:duplicate(MaxSize, <<"a">>)),
  ReferenceKO = iolist_to_binary(lists:duplicate(MaxSize + 1, <<"a">>)),

  Tests = [{1, ReferenceOK, ?RESPONSE_CODE_OK},
           {2, ReferenceKO, ?RESPONSE_CODE_PRECONDITION_FAILED}],

  [begin
     F = request({subscribe, SubId, Stream, first, 1, #{<<"name">> => Ref}}),
     ok = T:send(S, F),
     {Cmd, C} = receive_commands(T, S, C),
     ?assertMatch({response, 1, {subscribe, ExpectedResponseCode}}, Cmd)
   end || {SubId, Ref, ExpectedResponseCode} <- Tests],

  test_delete_stream(T, S, Stream, C),
  test_close(T, S, C),
  ok.

subscribe_unsubscribe_should_create_events(Config) ->
    HandlerMod = rabbit_list_test_event_handler,
    rabbit_ct_broker_helpers:add_code_path_to_all_nodes(Config, HandlerMod),
    rabbit_ct_broker_helpers:rpc(Config, 0,
                                 gen_event,
                                 add_handler,
                                 [rabbit_event, HandlerMod, []]),
    Stream = atom_to_binary(?FUNCTION_NAME, utf8),
    Transport = gen_tcp,
    Port = get_stream_port(Config),
    Opts = get_opts(Transport),
    {ok, S} = Transport:connect("localhost", Port, Opts),
    C0 = rabbit_stream_core:init(0),
    C1 = test_peer_properties(Transport, S, C0),
    C2 = test_authenticate(Transport, S, C1),
    C3 = test_create_stream(Transport, S, Stream, C2),

    ?assertEqual([], filtered_events(Config, consumer_created)),
    ?assertEqual([], filtered_events(Config, consumer_deleted)),

    SubscriptionId = 42,
    C4 = test_subscribe(Transport, S, SubscriptionId, Stream, C3),

    ?awaitMatch([{event, consumer_created, _, _, _}], filtered_events(Config, consumer_created), ?WAIT),
    ?assertEqual([], filtered_events(Config, consumer_deleted)),

    C5 = test_unsubscribe(Transport, S, SubscriptionId, C4),

    ?awaitMatch([{event, consumer_deleted, _, _, _}], filtered_events(Config, consumer_deleted), ?WAIT),

    rabbit_ct_broker_helpers:rpc(Config, 0,
                                 gen_event,
                                 delete_handler,
                                 [rabbit_event, HandlerMod, []]),

    C6 = test_delete_stream(Transport, S, Stream, C5, false),
    _C7 = test_close(Transport, S, C6),
    closed = wait_for_socket_close(Transport, S, 10),
    ok.

test_stream_test_utils(Config) ->
    Stream = atom_to_binary(?FUNCTION_NAME, utf8),
    {ok, S, C0} = stream_test_utils:connect(Config, 0),
    {ok, C1} = stream_test_utils:create_stream(S, C0, Stream),
    PublisherId = 42,
    {ok, C2} = stream_test_utils:declare_publisher(S, C1, Stream, PublisherId),
    MsgPerBatch = 100,
    Payloads = lists:duplicate(MsgPerBatch, <<"m1">>),
    SequenceFrom1 = 1,
    {ok, C3} = stream_test_utils:publish(S, C2, PublisherId, SequenceFrom1, Payloads),
    {ok, C4} = stream_test_utils:delete_publisher(S, C3, PublisherId),
    {ok, C5} = stream_test_utils:delete_stream(S, C4, Stream),
    {ok, _} = stream_test_utils:close(S, C5),
    ok.

sac_subscription_with_partition_index_conflict_should_return_error(Config) ->
    T = gen_tcp,
    App = <<"app-1">>,
    {ok, S, C0} = stream_test_utils:connect(Config, 0),
    Ss = atom_to_binary(?FUNCTION_NAME, utf8),
    Partition = unicode:characters_to_binary([Ss, <<"-0">>]),
    SsCreationFrame = request({create_super_stream, Ss, [Partition], [<<"0">>], #{}}),
    ok = T:send(S, SsCreationFrame),
    {Cmd1, C1} = receive_commands(T, S, C0),
    ?assertMatch({response, 1, {create_super_stream, ?RESPONSE_CODE_OK}},
                 Cmd1),

    SacSubscribeFrame = request({subscribe, 0, Partition,
                                 first, 1,
                                 #{<<"single-active-consumer">> => <<"true">>,
                                   <<"name">> => App}}),
    ok = T:send(S, SacSubscribeFrame),
    {Cmd2, C2} = receive_commands(T, S, C1),
    ?assertMatch({response, 1, {subscribe, ?RESPONSE_CODE_OK}},
                 Cmd2),
    {Cmd3, C3} = receive_commands(T, S, C2),
    ?assertMatch({request,0,{consumer_update,0,true}},
                 Cmd3),

    SsSubscribeFrame = request({subscribe, 1, Partition,
                                 first, 1,
                                 #{<<"super-stream">> => Ss,
                                   <<"single-active-consumer">> => <<"true">>,
                                   <<"name">> => App}}),
    ok = T:send(S, SsSubscribeFrame),
    {Cmd4, C4} = receive_commands(T, S, C3),
    ?assertMatch({response, 1, {subscribe, ?RESPONSE_CODE_PRECONDITION_FAILED}},
                 Cmd4),

    {ok, C5} = stream_test_utils:unsubscribe(S, C4, 0),

    SsDeletionFrame = request({delete_super_stream, Ss}),
    ok = T:send(S, SsDeletionFrame),
    {Cmd5, C5} = receive_commands(T, S, C5),
    ?assertMatch({response, 1, {delete_super_stream, ?RESPONSE_CODE_OK}},
                 Cmd5),

    {ok, _} = stream_test_utils:close(S, C5),
    ok.


filtered_events(Config, EventType) ->
    Events = rabbit_ct_broker_helpers:rpc(Config, 0,
                                          gen_event,
                                          call,
                                          [rabbit_event, rabbit_list_test_event_handler, get_events]),
    lists:filter(fun({event, Type, _, _, _}) when Type =:= EventType ->
                         true;
                    (_) ->
                         false
                 end, Events).

consumer_offset_info(Config, ConnectionName) ->
    [[{offset, Offset},
      {offset_lag, Lag}]] = rpc(Config, 0, ?MODULE,
                                list_consumer_info, [ConnectionName, [offset, offset_lag]]),
    {Offset, Lag}.

list_consumer_info(ConnectionName, Infos) ->
    Pids = rabbit_stream:list(<<"/">>),
    [ConnPid] = lists:filter(fun(ConnectionPid) ->
                                     ConnectionPid ! {infos, self()},
                                     receive
                                         {ConnectionPid,
                                          #{<<"connection_name">> := ConnectionName}} ->
                                             true;
                                         {ConnectionPid, _ClientProperties} ->
                                             false
                                     after 1000 ->
                                               false
                                     end
                             end,
                             Pids),
    rabbit_stream_reader:consumers_info(ConnPid, Infos).

store_offset(Transport, S, Reference, Stream, Offset) ->
    StoreFrame = rabbit_stream_core:frame({store_offset, Reference, Stream, Offset}),
    ok = Transport:send(S, StoreFrame).

query_expected_offset(T, S, C, Reference, Stream, Expected) ->
    query_expected_offset(T, S, C, Reference, Stream, Expected, 10).

query_expected_offset(_, _, C, _, _, _, 0) ->
    {timeout, C};
query_expected_offset(T, S, C0, Reference, Stream, Expected, Count) ->
    case query_offset(T, S, C0, Reference, Stream) of
        {Expected, _} = R ->
            R;
        {_, C1} ->
            timer:sleep(100),
            query_expected_offset(T, S, C1, Reference, Stream, Expected, Count - 1)
    end.

query_offset(T, S, C0, Reference, Stream) ->
    QueryFrame = request({query_offset, Reference, Stream}),
    ok = T:send(S, QueryFrame),

    {Cmd, C1} = receive_commands(T, S, C0),
    {response, 1, {query_offset, _, Offset}} = Cmd,

    {Offset, C1}.

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

get_port(Transport, Config) ->
  case Transport of
      gen_tcp ->
          get_stream_port(Config);
      ssl ->
          application:ensure_all_started(ssl),
          get_stream_port_tls(Config)
  end.
get_opts(Transport) ->
  case Transport of
      gen_tcp ->
          [{active, false}, {mode, binary}];
      ssl ->
          [{active, false}, {mode, binary}, {verify, verify_none}]
  end.

connect_and_authenticate(Transport, Config) ->
  Port = get_port(Transport, Config),
  Opts = get_opts(Transport),
  {ok, S} = Transport:connect("localhost", Port, Opts),
  C0 = rabbit_stream_core:init(0),
  C1 = test_peer_properties(Transport, S, C0),
  {S, test_authenticate(Transport, S, C1)}.

try_authenticate(Transport, S, C, AuthMethod, Username, Password) ->
  case AuthMethod of
    <<"PLAIN">> ->
      plain_sasl_authenticate(Transport, S, C, Username, Password);
    _ ->
      Null = 0,
      sasl_authenticate(Transport, S, C, AuthMethod, <<Null:8, Username/binary, Null:8, Password/binary>>)
  end.

test_server(Transport, Stream, Config) ->
    QName = rabbit_misc:r(<<"/">>, queue, Stream),
    Port = get_port(Transport, Config),
    Opts = get_opts(Transport),
    {ok, S} =
        Transport:connect("localhost", Port, Opts),
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
    test_peer_properties(Transport, S, #{}, C0).

test_peer_properties(Transport, S, Properties, C0) ->
    PeerPropertiesFrame = request({peer_properties, Properties}),
    ok = Transport:send(S, PeerPropertiesFrame),
    {Cmd, C} = receive_commands(Transport, S, C0),
    ?assertMatch({response, 1, {peer_properties, ?RESPONSE_CODE_OK, _}},
                 Cmd),
    C.

test_authenticate(Transport, S, C0) ->
    tune(Transport, S,
         test_plain_sasl_authenticate(Transport, S, sasl_handshake(Transport, S, C0), <<"guest">>)).

test_authenticate(Transport, S, C0, Username) ->
    test_authenticate(Transport, S, C0, Username, Username).

test_authenticate(Transport, S, C0, Username, Password) ->
    tune(Transport, S,
         test_plain_sasl_authenticate(Transport, S, sasl_handshake(Transport, S, C0), Username, Password)).

sasl_handshake(Transport, S, C0) ->
    SaslHandshakeFrame = request(sasl_handshake),
    ok = Transport:send(S, SaslHandshakeFrame),
    {Cmd, C1} = receive_commands(Transport, S, C0),
    case Cmd of
        {response, _, {sasl_handshake, ?RESPONSE_CODE_OK, Mechanisms}} ->
            ?assertEqual([<<"AMQPLAIN">>, <<"ANONYMOUS">>, <<"PLAIN">>],
                         lists:sort(Mechanisms));
        _ ->
            ct:fail("invalid cmd ~tp", [Cmd])
    end,
    C1.

test_anonymous_sasl_authenticate(Transport, S, C) ->
    Res = sasl_authenticate(Transport, S, C, <<"ANONYMOUS">>, <<>>),
    expect_successful_authentication(Res).

test_plain_sasl_authenticate(Transport, S, C1, Username) ->
    test_plain_sasl_authenticate(Transport, S, C1, Username, Username).

test_plain_sasl_authenticate(Transport, S, C1, Username, Password) ->
    expect_successful_authentication(plain_sasl_authenticate(Transport, S, C1, Username, Password)).

plain_sasl_authenticate(Transport, S, C1, Username, Password) ->
    Null = 0,
    sasl_authenticate(Transport, S, C1, <<"PLAIN">>, <<Null:8, Username/binary, Null:8, Password/binary>>).

expect_successful_authentication({SaslAuth, C2} = _SaslReponse) ->
    ?assertEqual({response, 2, {sasl_authenticate, ?RESPONSE_CODE_OK}},
                 SaslAuth),
    C2.

expect_unsuccessful_authentication({SaslAuth, C2} = _SaslReponse, ExpectedError) ->
    ?assertEqual({response, 2, {sasl_authenticate, ExpectedError}},
                 SaslAuth),
    C2.

sasl_authenticate(Transport, S, C1, AuthMethod, AuthBody) ->
    SaslAuthenticateFrame = request(2, {sasl_authenticate, AuthMethod, AuthBody}),
    ok = Transport:send(S, SaslAuthenticateFrame),
    receive_commands(Transport, S, C1).

tune(Transport, S, C2) ->
    {{response, _, {open, ?RESPONSE_CODE_OK, _}}, C3} = do_tune(Transport, S, C2),
    C3.

do_tune(Transport, S, C2) ->
    {Tune, C3} = receive_commands(Transport, S, C2),
    {tune, ?DEFAULT_FRAME_MAX, ?DEFAULT_HEARTBEAT} = Tune,

    TuneFrame =
        rabbit_stream_core:frame({response, 0,
                                  {tune, ?DEFAULT_FRAME_MAX, 0}}),
    ok = Transport:send(S, TuneFrame),

    VirtualHost = <<"/">>,
    OpenFrame = request(3, {open, VirtualHost}),
    ok = Transport:send(S, OpenFrame),
    receive_commands(Transport, S, C3).

test_create_stream(Transport, S, Stream, C0) ->
    CreateStreamFrame = request({create_stream, Stream, #{}}),
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
    DeleteStreamFrame = request({delete_stream, Stream}),
    ok = Transport:send(S, DeleteStreamFrame),
    {Cmd, C1} = receive_commands(Transport, S, C0),
    ?assertMatch({response, 1, {delete_stream, ?RESPONSE_CODE_OK}}, Cmd),
    C1.

test_metadata_update_stream_deleted(Transport, S, Stream, C0) ->
    {Meta, C1} = receive_commands(Transport, S, C0),
    {metadata_update, Stream, ?RESPONSE_CODE_STREAM_NOT_AVAILABLE} = Meta,
    C1.

test_declare_publisher(Transport, S, PublisherId, Stream, C0) ->
    test_declare_publisher(Transport, S, PublisherId, <<>>, Stream, C0).

test_declare_publisher(Transport, S, PublisherId, Reference, Stream, C0) ->
    DeclarePublisherFrame = request({declare_publisher,
                                     PublisherId,
                                     Reference,
                                     Stream}),
    ok = Transport:send(S, DeclarePublisherFrame),
    {Cmd, C} = receive_commands(Transport, S, C0),
    ?assertMatch({response, 1, {declare_publisher, ?RESPONSE_CODE_OK}},
                 Cmd),
    C.

test_publish_confirm(Transport, S, PublisherId, Body, C0) ->
    test_publish_confirm(Transport, S, PublisherId, 1, Body, C0).

test_publish_confirm(Transport, S, PublisherId, Sequence, Body, C0) ->
    test_publish_confirm(Transport, S, publish, PublisherId, Sequence, Body,
                         publish_confirm, C0).

test_publish_confirm(Transport, S, PublishCmd, PublisherId, Body,
                     ExpectedConfirmCommand, C0) ->
    test_publish_confirm(Transport, S, PublishCmd, PublisherId, 1, Body,
                         ExpectedConfirmCommand, C0).

test_publish_confirm(Transport, S, publish = PublishCmd, PublisherId,
                     Sequence, Body,
                     ExpectedConfirmCommand, C0) ->
    BodySize = byte_size(Body),
    Messages = [<<Sequence:64, 0:1, BodySize:31, Body:BodySize/binary>>],
    PublishFrame = frame({PublishCmd, PublisherId, 1, Messages}),
    ok = Transport:send(S, PublishFrame),
    {Cmd, C} = receive_commands(Transport, S, C0),
    ?assertMatch({ExpectedConfirmCommand, PublisherId, [Sequence]}, Cmd),
    C;
test_publish_confirm(Transport, S, publish_v2 = PublishCmd, PublisherId,
                     Sequence, Body,
                     ExpectedConfirmCommand, C0) ->
    BodySize = byte_size(Body),
    FilterValue = <<"foo">>,
    FilterValueSize = byte_size(FilterValue),
    Messages = [<<Sequence:64, FilterValueSize:16, FilterValue:FilterValueSize/binary,
                  0:1, BodySize:31, Body:BodySize/binary>>],
    PublishFrame = frame({PublishCmd, PublisherId, 1, Messages}),
    ok = Transport:send(S, PublishFrame),
    {Cmd, C} = receive_commands(Transport, S, C0),
    case ExpectedConfirmCommand of
        publish_confirm ->
            ?assertMatch({ExpectedConfirmCommand, PublisherId, [Sequence]}, Cmd);
        publish_error ->
            ?assertMatch({ExpectedConfirmCommand, PublisherId, _, [Sequence]}, Cmd)
    end,
    C.

test_delete_publisher(Transport, Socket, PublisherId, C0) ->
    Frame = request({delete_publisher, PublisherId}),
    ok = Transport:send(Socket, Frame),
    {Cmd, C} = receive_commands(Transport, Socket, C0),
    ?assertMatch({response, 1, {delete_publisher, ?RESPONSE_CODE_OK}}, Cmd),
    C.

test_subscribe(Transport, S, SubscriptionId, Stream, C0) ->
    test_subscribe(Transport,
                   S,
                   SubscriptionId,
                   Stream,
                   #{<<"random">> => <<"thing">>},
                   ?RESPONSE_CODE_OK,
                   C0).

test_subscribe(Transport,
               S,
               SubscriptionId,
               Stream,
               SubscriptionProperties,
               ExpectedResponseCode,
               C0) ->
    test_subscribe(Transport, S, SubscriptionId, Stream, 0, 10,
                   SubscriptionProperties,
                   ExpectedResponseCode, C0).

test_subscribe(Transport,
               S,
               SubscriptionId,
               Stream,
               OffsetSpec,
               Credit,
               SubscriptionProperties,
               ExpectedResponseCode,
               C0) ->
    SubscribeFrame = request({subscribe, SubscriptionId, Stream,
                              OffsetSpec, Credit, SubscriptionProperties}),
    ok = Transport:send(S, SubscribeFrame),
    {Cmd, C} = receive_commands(Transport, S, C0),
    ?assertMatch({response, 1, {subscribe, ExpectedResponseCode}}, Cmd),
    C.

test_unsubscribe(Transport, Socket, SubscriptionId, C0) ->
    UnsubscribeFrame = request({unsubscribe, SubscriptionId}),
    ok = Transport:send(Socket, UnsubscribeFrame),
    {Cmd, C} = receive_commands(Transport, Socket, C0),
    ?assertMatch({response, 1, {unsubscribe, ?RESPONSE_CODE_OK}}, Cmd),
    C.

test_deliver(Transport, S, SubscriptionId, COffset, Body, C0) ->
    {{deliver, SubscriptionId, Chunk}, C} =
        receive_commands(Transport, S, C0),
    ct:pal("test_deliver ~p", [Chunk]),
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
    {{deliver_v2, SubscriptionId, _CommittedOffset, Chunk}, C} =
        receive_commands(Transport, S, C0),
    ct:pal("test_deliver_v2 ~p", [Chunk]),
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
    ExFrame = request({exchange_command_versions, [{deliver, ?VERSION_1, ?VERSION_2}]}),
    ok = Transport:send(S, ExFrame),
    {Cmd, C} = receive_commands(Transport, S, C0),
    ?assertMatch({response, 1,
                  {exchange_command_versions, ?RESPONSE_CODE_OK,
                   [{declare_publisher, _, _} | _]}},
                 Cmd),
    C.

test_stream_stats(Transport, S, Stream, C0) ->
    SIFrame = request({stream_stats, Stream}),
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
    CloseFrame = request({close, ?RESPONSE_CODE_OK, CloseReason}),
    ok = Transport:send(S, CloseFrame),
    {{response, 1, {close, ?RESPONSE_CODE_OK}}, C} =
        receive_commands(Transport, S, C0),
    C.

wait_for_socket_close(S, Attempt) ->
    wait_for_socket_close(gen_tcp, S, Attempt).

wait_for_socket_close(_Transport, _S, 0) ->
    not_closed;
wait_for_socket_close(Transport, S, Attempt) ->
    case Transport:recv(S, 0, 1000) of
        {error, timeout} ->
            wait_for_socket_close(Transport, S, Attempt - 1);
        {error, closed} ->
            closed
    end.


receive_commands(S, C) ->
    receive_commands(gen_tcp, S, C).

receive_commands(Transport, S, C) ->
   stream_test_utils:receive_stream_commands(Transport, S, C).

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

request(Cmd) ->
    request(1, Cmd).

request(CorrId, Cmd) ->
    rabbit_stream_core:frame({request, CorrId, Cmd}).
