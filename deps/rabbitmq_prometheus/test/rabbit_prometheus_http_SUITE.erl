%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_prometheus_http_SUITE).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_mgmt_test.hrl").

-compile(export_all).

all() ->
    [
        {group, default_config},
        {group, config_path},
        {group, aggregated_metrics},
        {group, per_object_metrics},
        {group, per_object_endpoint_metrics},
        {group, commercial},
        {group, detailed_metrics}
    ].

groups() ->
    [
        {default_config, [], generic_tests()},
        {config_path, [], generic_tests()},
        {aggregated_metrics, [], [
            aggregated_metrics_test,
            specific_erlang_metrics_present_test,
            global_metrics_present_test,
            global_metrics_single_metric_family_test
        ]},
        {per_object_metrics, [], [
            globally_configure_per_object_metrics_test,
            specific_erlang_metrics_present_test,
            global_metrics_present_test,
            global_metrics_single_metric_family_test
        ]},
        {per_object_endpoint_metrics, [], [
            endpoint_per_object_metrics,
            specific_erlang_metrics_present_test
        ]},
        {commercial, [], [
            build_info_product_test
        ]},
        {detailed_metrics, [], [
                                     detailed_metrics_no_families_enabled_by_default,
                                     queue_consumer_count_single_vhost_per_object_test,
                                     queue_consumer_count_all_vhosts_per_object_test,
                                     queue_coarse_metrics_per_object_test,
                                     queue_metrics_per_object_test,
                                     queue_consumer_count_and_queue_metrics_mutually_exclusive_test,
                                     vhost_status_metric,
                                     exchange_bindings_metric,
                                     exchange_names_metric
        ]}
    ].

generic_tests() ->
    [
        get_test,
        content_type_test,
        encoding_test,
        gzip_encoding_test,
        build_info_test,
        identity_info_test
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------
init_per_group(default_config, Config) ->
    init_per_group(default_config, Config, []);
init_per_group(config_path, Config0) ->
    PathConfig = {rabbitmq_prometheus, [{path, "/bunnieshop"}]},
    Config1 = rabbit_ct_helpers:merge_app_env(Config0, PathConfig),
    init_per_group(config_path, Config1, [{prometheus_path, "/bunnieshop"}]);
init_per_group(per_object_metrics, Config0) ->
    PathConfig = {rabbitmq_prometheus, [{return_per_object_metrics, true}]},
    Config1 = rabbit_ct_helpers:merge_app_env(Config0, PathConfig),
    init_per_group(aggregated_metrics, Config1);
init_per_group(per_object_endpoint_metrics, Config0) ->
    PathConfig = {rabbitmq_prometheus, [
        {return_per_object_metrics, false}
    ]},
    Config1 = rabbit_ct_helpers:merge_app_env(Config0, PathConfig),
    init_per_group(aggregated_metrics, Config1);
init_per_group(detailed_metrics, Config0) ->
    StatsEnv = {rabbit, [{collect_statistics, coarse}, {collect_statistics_interval, 100}]},

    Config1 = init_per_group(detailed_metrics, rabbit_ct_helpers:merge_app_env(Config0, StatsEnv), []),

    rabbit_ct_broker_helpers:add_vhost(Config1, 0, <<"vhost-1">>, <<"guest">>),
    rabbit_ct_broker_helpers:set_full_permissions(Config1, <<"vhost-1">>),
    VHost1Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config1, 0, <<"vhost-1">>),
    {ok, VHost1Ch} = amqp_connection:open_channel(VHost1Conn),

    rabbit_ct_broker_helpers:add_vhost(Config1, 0, <<"vhost-2">>, <<"guest">>),
    rabbit_ct_broker_helpers:set_full_permissions(Config1, <<"vhost-2">>),
    VHost2Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config1, 0, <<"vhost-2">>),
    {ok, VHost2Ch} = amqp_connection:open_channel(VHost2Conn),

    DefaultCh = rabbit_ct_client_helpers:open_channel(Config1),

    [
     (fun () ->
              QPart = case VHost of
                          <<"/">> -> <<"default">>;
                          _ -> VHost
                      end,
              QName = << QPart/binary, "-", Q/binary>>,
              #'queue.declare_ok'{} = amqp_channel:call(Ch,
                                                        #'queue.declare'{queue = QName,
                                                                         durable = true

                                                                        }),
              lists:foreach( fun (_) ->
                                     amqp_channel:cast(Ch,
                                                       #'basic.publish'{routing_key = QName},
                                                       #amqp_msg{payload = <<"msg">>})
                             end, lists:seq(1, MsgNum) ),
              ExDirect = <<QName/binary, "-direct-exchange">>,
              #'exchange.declare_ok'{} = amqp_channel:call(Ch, #'exchange.declare'{exchange = ExDirect}),
              ExTopic = <<QName/binary, "-topic-exchange">>,
              #'exchange.declare_ok'{} = amqp_channel:call(Ch, #'exchange.declare'{exchange = ExTopic, type = <<"topic">>}),
              #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{queue = QName, exchange = ExDirect, routing_key = QName}),
              lists:foreach( fun (Idx) ->
                                     #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{queue = QName, exchange = ExTopic, routing_key = integer_to_binary(Idx)})
                             end, lists:seq(1, MsgNum) )
      end)()
     || {VHost, Ch, MsgNum} <- [{<<"/">>, DefaultCh, 3}, {<<"vhost-1">>, VHost1Ch, 7}, {<<"vhost-2">>, VHost2Ch, 11}],
        Q <- [ <<"queue-with-messages">>, <<"queue-with-consumer">> ]
    ],

    DefaultConsumer = sleeping_consumer(),
    #'basic.consume_ok'{consumer_tag = DefaultCTag} =
        amqp_channel:subscribe(DefaultCh, #'basic.consume'{queue = <<"default-queue-with-consumer">>}, DefaultConsumer),

    VHost1Consumer = sleeping_consumer(),
    #'basic.consume_ok'{consumer_tag = VHost1CTag} =
        amqp_channel:subscribe(VHost1Ch, #'basic.consume'{queue = <<"vhost-1-queue-with-consumer">>}, VHost1Consumer),

    VHost2Consumer = sleeping_consumer(),
    #'basic.consume_ok'{consumer_tag = VHost2CTag} =
        amqp_channel:subscribe(VHost2Ch, #'basic.consume'{queue = <<"vhost-2-queue-with-consumer">>}, VHost2Consumer),

    timer:sleep(1000),

    Config1 ++ [ {default_consumer_pid, DefaultConsumer}
               , {default_consumer_ctag, DefaultCTag}
               , {default_channel, DefaultCh}
               , {vhost1_consumer_pid, VHost1Consumer}
               , {vhost1_consumer_ctag, VHost1CTag}
               , {vhost1_channel, VHost1Ch}
               , {vhost1_conn, VHost1Conn}
               , {vhost2_consumer_pid, VHost2Consumer}
               , {vhost2_consumer_ctag, VHost2CTag}
               , {vhost2_channel, VHost2Ch}
               , {vhost2_conn, VHost2Conn}
               ];
init_per_group(aggregated_metrics, Config0) ->
    Config1 = rabbit_ct_helpers:merge_app_env(
        Config0,
        [{rabbit, [{collect_statistics, coarse}, {collect_statistics_interval, 100}]}]
    ),
    Config2 = init_per_group(aggregated_metrics, Config1, []),
    ok = rabbit_ct_broker_helpers:enable_feature_flag(Config2, quorum_queue),

    A = rabbit_ct_broker_helpers:get_node_config(Config2, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config2, A),

    Q = <<"prometheus_test_queue">>,
    amqp_channel:call(Ch,
                      #'queue.declare'{queue = Q,
                                       durable = true,
                                       arguments = [{<<"x-queue-type">>, longstr, <<"quorum">>}]
                                      }),
    amqp_channel:cast(Ch,
                      #'basic.publish'{routing_key = Q},
                      #amqp_msg{payload = <<"msg">>}),
    timer:sleep(150),
    {#'basic.get_ok'{}, #amqp_msg{}} = amqp_channel:call(Ch, #'basic.get'{queue = Q}),
    %% We want to check consumer metrics, so we need at least 1 consumer bound
    %% but we don't care what it does if anything as long as the runner process does
    %% not have to handle the consumer's messages.
    ConsumerPid = sleeping_consumer(),
    #'basic.consume_ok'{consumer_tag = CTag} =
      amqp_channel:subscribe(Ch, #'basic.consume'{queue = Q}, ConsumerPid),
    timer:sleep(10000),

    Config2 ++ [{channel_pid, Ch}, {queue_name, Q}, {consumer_tag, CTag}, {consumer_pid, ConsumerPid}];
init_per_group(commercial, Config0) ->
    ProductConfig = {rabbit, [{product_name, "WolfMQ"}, {product_version, "2020"}]},
    Config1 = rabbit_ct_helpers:merge_app_env(Config0, ProductConfig),
    init_per_group(commercial, Config1, []).

init_per_group(Group, Config0, Extra) ->
    rabbit_ct_helpers:log_environment(),
    inets:start(),
    NodeConf = [{rmq_nodename_suffix, Group}] ++ Extra,
    Config1 = rabbit_ct_helpers:set_config(Config0, NodeConf),
    rabbit_ct_helpers:run_setup_steps(Config1, rabbit_ct_broker_helpers:setup_steps()
                                      ++ rabbit_ct_client_helpers:setup_steps()).

end_per_group(aggregated_metrics, Config) ->
    Ch = ?config(channel_pid, Config),
    CTag = ?config(consumer_tag, Config),
    amqp_channel:call(Ch, #'basic.cancel'{consumer_tag = CTag}),
    ConsumerPid = ?config(consumer_pid, Config),
    ConsumerPid ! stop,
    amqp_channel:call(Ch, #'queue.delete'{queue = ?config(queue_name, Config)}),
    rabbit_ct_client_helpers:close_channel(Ch),
    end_per_group_(Config);

end_per_group(detailed_metrics, Config) ->
    DefaultCh = ?config(default_channel, Config),
    amqp_channel:call(DefaultCh, #'basic.cancel'{consumer_tag = ?config(default_consumer_ctag, Config)}),
    ?config(default_consumer_pid, Config) ! stop,
    rabbit_ct_client_helpers:close_channel(DefaultCh),

    VHost1Ch = ?config(vhost1_channel, Config),
    amqp_channel:call(VHost1Ch, #'basic.cancel'{consumer_tag = ?config(vhost1_consumer_ctag, Config)}),
    ?config(vhost1_consumer_pid, Config) ! stop,
    amqp_channel:close(VHost1Ch),
    amqp_connection:close(?config(vhost1_conn, Config)),

    VHost2Ch = ?config(vhost2_channel, Config),
    amqp_channel:call(VHost2Ch, #'basic.cancel'{consumer_tag = ?config(vhost2_consumer_ctag, Config)}),
    ?config(vhost2_consumer_pid, Config) ! stop,
    amqp_channel:close(VHost2Ch),
    amqp_connection:close(?config(vhost2_conn, Config)),

    %% Delete queues?
    end_per_group_(Config);

end_per_group(_, Config) ->
    end_per_group_(Config).

end_per_group_(Config) ->
    inets:stop(),
    rabbit_ct_helpers:run_teardown_steps(Config, rabbit_ct_client_helpers:teardown_steps()
                                         ++ rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% a no-op consumer
sleeping_consumer_loop() ->
  receive
    stop -> ok;
    #'basic.consume_ok'{}          -> sleeping_consumer_loop();
    #'basic.cancel'{}              -> sleeping_consumer_loop();
    {#'basic.deliver'{}, _Payload} -> sleeping_consumer_loop()
  end.

sleeping_consumer() ->
  spawn(fun() ->
    sleeping_consumer_loop()
  end).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

get_test(Config) ->
    {_Headers, Body} = http_get_with_pal(Config, [], 200),
    %% Check that the body looks like a valid response
    ?assertEqual(match, re:run(Body, "TYPE", [{capture, none}])),
    Port = rabbit_mgmt_test_util:config_port(Config, tcp_port_prometheus),
    URI = lists:flatten(io_lib:format("http://localhost:~p/metricsooops", [Port])),
    {ok, {{_, CodeAct, _}, _, _}} = httpc:request(get, {URI, []}, ?HTTPC_OPTS, []),
    ?assertMatch(404, CodeAct).

content_type_test(Config) ->
    {Headers, Body} = http_get_with_pal(Config, [{"accept", "text/plain"}], 200),
    ?assertEqual(match, re:run(proplists:get_value("content-type", Headers),
                               "text/plain", [{capture, none}])),
    %% Check that the body looks like a valid response
    ?assertEqual(match, re:run(Body, "^# TYPE", [{capture, none}, multiline])),

    http_get_with_pal(Config, [{"accept", "text/plain, text/html"}], 200),
    http_get_with_pal(Config, [{"accept", "*/*"}], 200),
    http_get_with_pal(Config, [{"accept", "text/xdvi"}], 406),
    http_get_with_pal(Config, [{"accept", "application/vnd.google.protobuf"}], 406).

encoding_test(Config) ->
    {Headers, Body} = http_get(Config, [{"accept-encoding", "deflate"}], 200),
    ?assertMatch("identity", proplists:get_value("content-encoding", Headers)),
    ?assertEqual(match, re:run(Body, "^# TYPE", [{capture, none}, multiline])).

gzip_encoding_test(Config) ->
    {Headers, Body} = http_get(Config, [{"accept-encoding", "gzip"}], 200),
    ?assertMatch("gzip", proplists:get_value("content-encoding", Headers)),
    %% If the body is not gzip, zlib:gunzip will crash
    ?assertEqual(match, re:run(zlib:gunzip(Body), "^# TYPE", [{capture, none}, multiline])).

aggregated_metrics_test(Config) ->
    {_Headers, Body} = http_get_with_pal(Config, [], 200),
    ?assertEqual(match, re:run(Body, "^# TYPE", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^# HELP", [{capture, none}, multiline])),
    ?assertEqual(nomatch, re:run(Body, ?config(queue_name, Config), [{capture, none}])),
    %% Check the first metric value from each ETS table owned by rabbitmq_metrics
    ?assertEqual(match, re:run(Body, "^rabbitmq_channel_consumers ", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_channel_messages_published_total ", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_channel_process_reductions_total ", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_channel_get_ack_total ", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_connections_opened_total ", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_connection_incoming_bytes_total ", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_connection_incoming_packets_total ", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_queue_messages_published_total ", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_process_open_fds ", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_process_max_fds ", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_io_read_ops_total ", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_raft_term_total ", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_queue_messages_ready ", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_queue_consumers ", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "TYPE rabbitmq_auth_attempts_total", [{capture, none}, multiline])),
    ?assertEqual(nomatch, re:run(Body, "TYPE rabbitmq_auth_attempts_detailed_total", [{capture, none}, multiline])),
    %% Check the first metric value in each ETS table that requires converting
    ?assertEqual(match, re:run(Body, "^rabbitmq_erlang_uptime_seconds ", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_io_read_time_seconds_total ", [{capture, none}, multiline])),
    %% Check the first TOTALS metric value
    ?assertEqual(match, re:run(Body, "^rabbitmq_connections ", [{capture, none}, multiline])),
    %% Check raft_entry_commit_latency_seconds because we are aggregating it
    ?assertEqual(match, re:run(Body, "^rabbitmq_raft_entry_commit_latency_seconds ", [{capture, none}, multiline])).

endpoint_per_object_metrics(Config) ->
    per_object_metrics_test(Config, "/metrics/per-object").

globally_configure_per_object_metrics_test(Config) ->
    per_object_metrics_test(Config, "/metrics").

per_object_metrics_test(Config, Path) ->
    {_Headers, Body} = http_get_with_pal(Config, Path, [], 200),
    ?assertEqual(match, re:run(Body, "^# TYPE", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^# HELP", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, ?config(queue_name, Config), [{capture, none}])),
    %% Check the first metric value from each ETS table owned by rabbitmq_metrics
    ?assertEqual(match, re:run(Body, "^rabbitmq_channel_consumers{", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_channel_messages_published_total{", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_channel_process_reductions_total{", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_channel_get_ack_total{", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_connections_opened_total ", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_connection_incoming_bytes_total{", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_connection_incoming_packets_total{", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_queue_messages_published_total{", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_process_open_fds ", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_process_max_fds ", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_io_read_ops_total ", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_raft_term_total{", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_queue_messages_ready{", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_queue_consumers{", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "TYPE rabbitmq_auth_attempts_total", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "TYPE rabbitmq_auth_attempts_detailed_total", [{capture, none}, multiline])),
    %% Check the first metric value in each ETS table that requires converting
    ?assertEqual(match, re:run(Body, "^rabbitmq_erlang_uptime_seconds ", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_io_read_time_seconds_total ", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_raft_entry_commit_latency_seconds{", [{capture, none}, multiline])),
    %% Check the first TOTALS metric value
    ?assertEqual(match, re:run(Body, "^rabbitmq_connections ", [{capture, none}, multiline])).

build_info_test(Config) ->
    {_Headers, Body} = http_get_with_pal(Config, [], 200),
    ?assertEqual(match, re:run(Body, "^rabbitmq_build_info{", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "rabbitmq_version=\"", [{capture, none}])),
    ?assertEqual(match, re:run(Body, "prometheus_plugin_version=\"", [{capture, none}])),
    ?assertEqual(match, re:run(Body, "prometheus_client_version=\"", [{capture, none}])),
    ?assertEqual(match, re:run(Body, "erlang_version=\"", [{capture, none}])).

build_info_product_test(Config) ->
    {_Headers, Body} = http_get_with_pal(Config, [], 200),
    ?assertEqual(match, re:run(Body, "product_name=\"WolfMQ\"", [{capture, none}])),
    ?assertEqual(match, re:run(Body, "product_version=\"2020\"", [{capture, none}])),
    %% Check that RabbitMQ version is still displayed
    ?assertEqual(match, re:run(Body, "rabbitmq_version=\"", [{capture, none}])).

identity_info_test(Config) ->
    {_Headers, Body} = http_get_with_pal(Config, [], 200),
    ?assertEqual(match, re:run(Body, "^rabbitmq_identity_info{", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "rabbitmq_node=", [{capture, none}])),
    ?assertEqual(match, re:run(Body, "rabbitmq_cluster=", [{capture, none}])),
    ?assertEqual(match, re:run(Body, "rabbitmq_cluster_permanent_id=", [{capture, none}])).

specific_erlang_metrics_present_test(Config) ->
    {_Headers, Body} = http_get_with_pal(Config, [], 200),
    ?assertEqual(match, re:run(Body, "^erlang_vm_dist_node_queue_size_bytes{", [{capture, none}, multiline])).

global_metrics_present_test(Config) ->
    {_Headers, Body} = http_get_with_pal(Config, [], 200),
    ?assertEqual(match, re:run(Body, "^rabbitmq_global_messages_received_total{", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_global_messages_received_confirm_total{", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_global_messages_routed_total{", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_global_messages_unroutable_dropped_total{", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_global_messages_unroutable_returned_total{", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_global_messages_confirmed_total{", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_global_messages_delivered_total{", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_global_messages_delivered_consume_manual_ack_total{", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_global_messages_delivered_consume_auto_ack_total{", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_global_messages_delivered_get_manual_ack_total{", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_global_messages_delivered_get_auto_ack_total{", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_global_messages_get_empty_total{", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_global_messages_redelivered_total{", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_global_messages_acknowledged_total{", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_global_publishers{", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_global_consumers{", [{capture, none}, multiline])).

global_metrics_single_metric_family_test(Config) ->
    {_Headers, Body} = http_get_with_pal(Config, [], 200),
    {match, MetricFamilyMatches} = re:run(Body, "TYPE rabbitmq_global_messages_acknowledged_total", [global]),
    ?assertEqual(1, length(MetricFamilyMatches)).

queue_consumer_count_single_vhost_per_object_test(Config) ->
    {_, Body} = http_get_with_pal(Config, "/metrics/detailed?vhost=vhost-1&family=queue_consumer_count&per-object=1", [], 200),

    %% There should be exactly 2 metrics returned (2 queues in that vhost, `queue_consumer_count` has only single metric)
    ?assertEqual(#{rabbitmq_detailed_queue_consumers =>
                       #{#{queue => "vhost-1-queue-with-consumer",vhost => "vhost-1"} => [1],
                         #{queue => "vhost-1-queue-with-messages",vhost => "vhost-1"} => [0]}},
                 parse_response(Body)),
    ok.

queue_consumer_count_all_vhosts_per_object_test(Config) ->
    Expected = #{rabbitmq_detailed_queue_consumers =>
                     #{#{queue => "vhost-1-queue-with-consumer",vhost => "vhost-1"} => [1],
                       #{queue => "vhost-1-queue-with-messages",vhost => "vhost-1"} => [0],
                       #{queue => "vhost-2-queue-with-consumer",vhost => "vhost-2"} => [1],
                       #{queue => "vhost-2-queue-with-messages",vhost => "vhost-2"} => [0],
                       #{queue => "default-queue-with-consumer",vhost => "/"} => [1],
                       #{queue => "default-queue-with-messages",vhost => "/"} => [0]}},

    %% No vhost given, all should be returned
    {_, Body1} = http_get_with_pal(Config, "/metrics/detailed?family=queue_consumer_count&per-object=1", [], 200),
    ?assertEqual(Expected, parse_response(Body1)),

    %% Both vhosts are listed explicitly
    {_, Body2} = http_get_with_pal(Config, "/metrics/detailed?vhost=vhost-1&vhost=vhost-2&vhost=%2f&family=queue_consumer_count&per-object=1", [], 200),
    ?assertEqual(Expected, parse_response(Body2)),
    ok.

queue_coarse_metrics_per_object_test(Config) ->
    Expected1 =  #{#{queue => "vhost-1-queue-with-consumer", vhost => "vhost-1"} => [7],
                   #{queue => "vhost-1-queue-with-messages", vhost => "vhost-1"} => [7]},
    Expected2 =  #{#{queue => "vhost-2-queue-with-consumer", vhost => "vhost-2"} => [11],
                   #{queue => "vhost-2-queue-with-messages", vhost => "vhost-2"} => [11]},
    ExpectedD =  #{#{queue => "default-queue-with-consumer", vhost => "/"} => [3],
                   #{queue => "default-queue-with-messages", vhost => "/"} => [3]},

    {_, Body1} = http_get_with_pal(Config, "/metrics/detailed?vhost=vhost-1&family=queue_coarse_metrics", [], 200),
    ?assertEqual(Expected1,
                 map_get(rabbitmq_detailed_queue_messages, parse_response(Body1))),

    {_, Body2} = http_get_with_pal(Config, "/metrics/detailed?family=queue_coarse_metrics", [], 200),
    ?assertEqual(lists:foldl(fun maps:merge/2, #{}, [Expected1, Expected2, ExpectedD]),
                 map_get(rabbitmq_detailed_queue_messages, parse_response(Body2))),

    {_, Body3} = http_get_with_pal(Config, "/metrics/detailed?vhost=vhost-1&vhost=vhost-2&family=queue_coarse_metrics", [], 200),
    ?assertEqual(lists:foldl(fun maps:merge/2, #{}, [Expected1, Expected2]),
                 map_get(rabbitmq_detailed_queue_messages, parse_response(Body3))),
    ok.

queue_metrics_per_object_test(Config) ->
    Expected1 =  #{#{queue => "vhost-1-queue-with-consumer", vhost => "vhost-1"} => [7],
                   #{queue => "vhost-1-queue-with-messages", vhost => "vhost-1"} => [7]},
    Expected2 =  #{#{queue => "vhost-2-queue-with-consumer", vhost => "vhost-2"} => [11],
                   #{queue => "vhost-2-queue-with-messages", vhost => "vhost-2"} => [11]},
    ExpectedD =  #{#{queue => "default-queue-with-consumer", vhost => "/"} => [3],
                   #{queue => "default-queue-with-messages", vhost => "/"} => [3]},
    {_, Body1} = http_get_with_pal(Config, "/metrics/detailed?vhost=vhost-1&family=queue_metrics", [], 200),
    ?assertEqual(Expected1,
                 map_get(rabbitmq_detailed_queue_messages_ram, parse_response(Body1))),

    {_, Body2} = http_get_with_pal(Config, "/metrics/detailed?family=queue_metrics", [], 200),
    ?assertEqual(lists:foldl(fun maps:merge/2, #{}, [Expected1, Expected2, ExpectedD]),
                 map_get(rabbitmq_detailed_queue_messages_ram, parse_response(Body2))),

    {_, Body3} = http_get_with_pal(Config, "/metrics/detailed?vhost=vhost-1&vhost=vhost-2&family=queue_metrics", [], 200),
    ?assertEqual(lists:foldl(fun maps:merge/2, #{}, [Expected1, Expected2]),
                 map_get(rabbitmq_detailed_queue_messages_ram, parse_response(Body3))),
    ok.

queue_consumer_count_and_queue_metrics_mutually_exclusive_test(Config) ->
    {_, Body1} = http_get_with_pal(Config, "/metrics/detailed?vhost=vhost-1&family=queue_consumer_count&family=queue_metrics", [], 200),
    ?assertEqual(#{#{queue => "vhost-1-queue-with-consumer", vhost => "vhost-1"} => [1],
                   #{queue => "vhost-1-queue-with-messages", vhost => "vhost-1"} => [0]},
                 map_get(rabbitmq_detailed_queue_consumers, parse_response(Body1))),

    ok.

detailed_metrics_no_families_enabled_by_default(Config) ->
    {_, Body} = http_get_with_pal(Config, "/metrics/detailed", [], 200),
    ?assertEqual(#{}, parse_response(Body)),
    ok.

vhost_status_metric(Config) ->
    {_, Body1} = http_get_with_pal(Config, "/metrics/detailed?family=vhost_status", [], 200),
    Expected = #{rabbitmq_cluster_vhost_status =>
                     #{#{vhost => "vhost-1"} => [1],
                       #{vhost => "vhost-2"} => [1],
                       #{vhost => "/"} => [1]}},
    ?assertEqual(Expected, parse_response(Body1)),
    ok.

exchange_bindings_metric(Config) ->
    {_, Body1} = http_get_with_pal(Config, "/metrics/detailed?family=exchange_bindings", [], 200),

    Bindings = map_get(rabbitmq_cluster_exchange_bindings, parse_response(Body1)),
    ?assertEqual([11], map_get(#{vhost=>"vhost-2",exchange=>"vhost-2-queue-with-messages-topic-exchange",type=>"topic"}, Bindings)),
    ?assertEqual([1],  map_get(#{vhost=>"vhost-2",exchange=>"vhost-2-queue-with-messages-direct-exchange",type=>"direct"}, Bindings)),
    ok.

exchange_names_metric(Config) ->
    {_, Body1} = http_get_with_pal(Config, "/metrics/detailed?family=exchange_names", [], 200),

    Names = maps:filter(
              fun
                  (#{exchange := [$a, $m, $q|_]}, _) ->
                      false;
                  (_, _) ->
                      true
              end,
              map_get(rabbitmq_cluster_exchange_name, parse_response(Body1))),

    ?assertEqual(#{ #{vhost=>"vhost-2",exchange=>"vhost-2-queue-with-messages-topic-exchange",type=>"topic"} => [1],
                    #{vhost=>"vhost-2",exchange=>"vhost-2-queue-with-messages-direct-exchange",type=>"direct"} => [1],
                    #{vhost=>"vhost-1",exchange=>"vhost-1-queue-with-messages-topic-exchange",type=>"topic"} => [1],
                    #{vhost=>"vhost-1",exchange=>"vhost-1-queue-with-messages-direct-exchange",type=>"direct"} => [1],
                    #{vhost=>"/",exchange=>"default-queue-with-messages-topic-exchange",type=>"topic"} => [1],
                    #{vhost=>"/",exchange=>"default-queue-with-messages-direct-exchange",type=>"direct"} => [1],
                    #{vhost=>"vhost-2",exchange=>"vhost-2-queue-with-consumer-topic-exchange",type=>"topic"} => [1],
                    #{vhost=>"vhost-2",exchange=>"vhost-2-queue-with-consumer-direct-exchange",type=>"direct"} => [1],
                    #{vhost=>"vhost-1",exchange=>"vhost-1-queue-with-consumer-topic-exchange",type=>"topic"} => [1],
                    #{vhost=>"vhost-1",exchange=>"vhost-1-queue-with-consumer-direct-exchange",type=>"direct"} => [1],
                    #{vhost=>"/",exchange=>"default-queue-with-consumer-topic-exchange",type=>"topic"} => [1],
                    #{vhost=>"/",exchange=>"default-queue-with-consumer-direct-exchange",type=>"direct"} => [1]
                  }, Names),
    ok.


http_get(Config, ReqHeaders, CodeExp) ->
    Path = proplists:get_value(prometheus_path, Config, "/metrics"),
    http_get(Config, Path, ReqHeaders, CodeExp).

http_get(Config, Path, ReqHeaders, CodeExp) ->
    Port = rabbit_mgmt_test_util:config_port(Config, tcp_port_prometheus),
    URI = lists:flatten(io_lib:format("http://localhost:~p~s", [Port, Path])),
    {ok, {{_HTTP, CodeAct, _}, Headers, Body}} =
        httpc:request(get, {URI, ReqHeaders}, ?HTTPC_OPTS, []),
    ?assertMatch(CodeExp, CodeAct),
    {Headers, Body}.

http_get_with_pal(Config, ReqHeaders, CodeExp) ->
    Path = proplists:get_value(prometheus_path, Config, "/metrics"),
    http_get_with_pal(Config, Path, ReqHeaders, CodeExp).

http_get_with_pal(Config, Path, ReqHeaders, CodeExp) ->
    {Headers, Body} = http_get(Config, Path, ReqHeaders, CodeExp),
    %% Print and log response body - it makes is easier to find why a match failed
    ct:pal(Body),
    {Headers, Body}.

parse_response(Body) ->
    Lines = string:split(Body, "\n", all),
    Metrics = [ parse_metric(L)
                || L = [C|_] <- Lines, C /= $#
              ],
    lists:foldl(fun ({Metric, Label, Value}, MetricMap) ->
                        case re:run(atom_to_list(Metric), "^(telemetry|rabbitmq_identity_info|rabbitmq_build_info)", [{capture, none}]) of
                            match ->
                                MetricMap;
                            _ ->
                                OldLabelMap = maps:get(Metric, MetricMap, #{}),
                                OldValues = maps:get(Label, OldLabelMap, []),
                                NewValues = [Value|OldValues],
                                NewLabelMap = maps:put(Label, NewValues, OldLabelMap),
                                maps:put(Metric, NewLabelMap, MetricMap)
                        end
                end, #{}, Metrics).

parse_metric(M) ->
    case string:lexemes(M, "{}" ) of
        [Metric, Label, Value] ->
            {list_to_atom(Metric), parse_label(Label), parse_value(string:trim(Value))};
        _ ->
            [Metric, Value] = string:split(M, " "),
            {list_to_atom(Metric), undefined, parse_value(string:trim(Value))}
    end.

parse_label(L) ->
    Parts = string:split(L, ",", all),
    maps:from_list([ parse_kv(P) || P <- Parts ]).

parse_kv(KV) ->
    [K, V] = string:split(KV, "="),
    {list_to_atom(K), string:trim(V, both, [$"])}.

parse_value(V) ->
    case lists:all(fun (C) -> C >= $0 andalso C =< $9 end, V) of
        true -> list_to_integer(V);
        _ -> V
    end.
