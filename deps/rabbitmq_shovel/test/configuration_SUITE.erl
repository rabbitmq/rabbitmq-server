%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(configuration_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

-define(EXCHANGE,    <<"test_exchange">>).
-define(TO_SHOVEL,   <<"to_the_shovel">>).
-define(FROM_SHOVEL, <<"from_the_shovel">>).
-define(UNSHOVELLED, <<"unshovelled">>).
-define(SHOVELLED,   <<"shovelled">>).
-define(TIMEOUT,     1000).

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
          zero_shovels,
          invalid_legacy_configuration,
          valid_legacy_configuration,
          valid_configuration
        ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE}
      ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps() ++
      [fun stop_shovel_plugin/1]).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

stop_shovel_plugin(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
      application, stop, [rabbitmq_shovel]),
    Config.

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

zero_shovels(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, zero_shovels1, [Config]).

zero_shovels1(_Config) ->
    %% shovel can be started with zero shovels configured
    ok = application:start(rabbitmq_shovel),
    ok = application:stop(rabbitmq_shovel),
    passed.

invalid_legacy_configuration(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, invalid_legacy_configuration1, [Config]).

invalid_legacy_configuration1(_Config) ->
    %% various ways of breaking the config
    require_list_of_shovel_configurations =
        test_broken_shovel_configs(invalid_config),

    require_list_of_shovel_configurations =
        test_broken_shovel_configs([{test_shovel, invalid_shovel_config}]),

    Config = [{sources, [{broker, "amqp://"}]},
              {destinations, [{broker, "amqp://"}]},
              {queue, <<"">>}],

    {duplicate_shovel_definition, test_shovel} =
        test_broken_shovel_configs(
          [{test_shovel, Config}, {test_shovel, Config}]),

    {invalid_parameters, [{invalid, invalid, invalid}]} =
        test_broken_shovel_config([{invalid, invalid, invalid} | Config]),

    {duplicate_parameters, [queue]} =
        test_broken_shovel_config([{queue, <<"">>} | Config]),

    {missing_parameter, _} =
        test_broken_shovel_config([]),

    {require_list, invalid} =
        test_broken_shovel_sources(invalid),

    {missing_parameter, broker} =
        test_broken_shovel_sources([]),

    {require_list, brokers, invalid} =
        test_broken_shovel_sources([{brokers, invalid}]),

    {expected_string_uri, 42} =
        test_broken_shovel_sources([{brokers, [42]}]),

    {{unexpected_uri_scheme, "invalid"}, "invalid://"} =
        test_broken_shovel_sources([{broker, "invalid://"}]),

    {{unable_to_parse_uri, no_scheme}, "invalid"} =
        test_broken_shovel_sources([{broker, "invalid"}]),

    {require_list, invalid} =
        test_broken_shovel_sources([{broker, "amqp://"},
                                    {declarations, invalid}]),
    {unknown_method_name, 42} =
        test_broken_shovel_sources([{broker, "amqp://"},
                                    {declarations, [42]}]),

    {expected_method_field_list, 'queue.declare', 42} =
        test_broken_shovel_sources([{broker, "amqp://"},
                                    {declarations, [{'queue.declare', 42}]}]),

    {unknown_fields, 'queue.declare', [invalid]} =
        test_broken_shovel_sources(
          [{broker, "amqp://"},
           {declarations, [{'queue.declare', [invalid]}]}]),

    {{invalid_amqp_params_parameter, heartbeat, "text",
      [{"heartbeat", "text"}], {not_an_integer, "text"}}, _} =
        test_broken_shovel_sources(
          [{broker, "amqp://localhost/?heartbeat=text"}]),

    {{invalid_amqp_params_parameter, username, "text",
      [{"username", "text"}],
      {parameter_unconfigurable_in_query, username, "text"}}, _} =
        test_broken_shovel_sources([{broker, "amqp://?username=text"}]),

    {invalid_parameter_value, prefetch_count,
     {require_non_negative_integer, invalid}} =
        test_broken_shovel_config([{prefetch_count, invalid} | Config]),

    {invalid_parameter_value, ack_mode,
     {ack_mode_value_requires_one_of,
      {no_ack, on_publish, on_confirm}, invalid}} =
        test_broken_shovel_config([{ack_mode, invalid} | Config]),

    {invalid_parameter_value, queue,
     {require_binary, invalid}} =
        test_broken_shovel_config([{sources, [{broker, "amqp://"}]},
                                   {destinations, [{broker, "amqp://"}]},
                                   {queue, invalid}]),

    {invalid_parameter_value, publish_properties,
     {require_list, invalid}} =
        test_broken_shovel_config([{publish_properties, invalid} | Config]),

    {invalid_parameter_value, publish_properties,
     {unexpected_fields, [invalid], _}} =
        test_broken_shovel_config([{publish_properties, [invalid]} | Config]),

    {{invalid_ssl_parameter, fail_if_no_peer_cert, "42", _,
      {require_boolean, '42'}}, _} =
        test_broken_shovel_sources([{broker, "amqps://username:password@host:5673/vhost?cacertfile=/path/to/cacert.pem&certfile=/path/to/certfile.pem&keyfile=/path/to/keyfile.pem&verify=verify_peer&fail_if_no_peer_cert=42"}]),

    passed.

test_broken_shovel_configs(Configs) ->
    application:set_env(rabbitmq_shovel, shovels, Configs),
    {error, {Error, _}} = application:start(rabbitmq_shovel),
    Error.

test_broken_shovel_config(Config) ->
    {invalid_shovel_configuration, test_shovel, Error} =
        test_broken_shovel_configs([{test_shovel, Config}]),
    Error.

test_broken_shovel_sources(Sources) ->
    test_broken_shovel_config([{sources, Sources},
                               {destinations, [{broker, "amqp://"}]},
                               {queue, <<"">>}]).

valid_legacy_configuration(Config) ->
    ok = setup_legacy_shovels(Config),
    run_valid_test(Config).

valid_configuration(Config) ->
    ok = setup_shovels(Config),
    run_valid_test(Config).

run_valid_test(Config) ->
    Chan = rabbit_ct_client_helpers:open_channel(Config, 0),

    #'queue.declare_ok'{ queue = Q } =
        amqp_channel:call(Chan, #'queue.declare' { exclusive = true }),
    #'queue.bind_ok'{} =
        amqp_channel:call(Chan, #'queue.bind' { queue = Q, exchange = ?EXCHANGE,
                                                routing_key = ?FROM_SHOVEL }),
    #'queue.bind_ok'{} =
        amqp_channel:call(Chan, #'queue.bind' { queue = Q, exchange = ?EXCHANGE,
                                                routing_key = ?TO_SHOVEL }),

    #'basic.consume_ok'{ consumer_tag = CTag } =
        amqp_channel:subscribe(Chan,
                               #'basic.consume' { queue = Q, exclusive = true },
                               self()),
    receive
        #'basic.consume_ok'{ consumer_tag = CTag } -> ok
    after ?TIMEOUT -> throw(timeout_waiting_for_consume_ok)
    end,

    ok = amqp_channel:call(Chan,
                           #'basic.publish' { exchange    = ?EXCHANGE,
                                              routing_key = ?TO_SHOVEL },
                           #amqp_msg { payload = <<42>>,
                                       props   = #'P_basic' {
                                         delivery_mode = 2,
                                         content_type  = ?UNSHOVELLED }
                                     }),

    receive
        {#'basic.deliver' { consumer_tag = CTag, delivery_tag = AckTag,
                            routing_key = ?FROM_SHOVEL },
         #amqp_msg { payload = <<42>>,
                     props   = #'P_basic' {
                        delivery_mode = 2,
                        content_type  = ?SHOVELLED,
                        headers       = [{<<"x-shovelled">>, _, _},
                                         {<<"x-shovelled-timestamp">>,
                                          long, _}]}
                   }} ->
            ok = amqp_channel:call(Chan, #'basic.ack'{ delivery_tag = AckTag })
    after ?TIMEOUT -> throw(timeout_waiting_for_deliver1)
    end,

    [{test_shovel, static, {running, _Info}, _Time}] =
        rabbit_ct_broker_helpers:rpc(Config, 0,
          rabbit_shovel_status, status, []),

    receive
        {#'basic.deliver' { consumer_tag = CTag, delivery_tag = AckTag1,
                            routing_key = ?TO_SHOVEL },
         #amqp_msg { payload = <<42>>,
                     props   = #'P_basic' { delivery_mode = 2,
                                            content_type  = ?UNSHOVELLED }
                   }} ->
            ok = amqp_channel:call(Chan, #'basic.ack'{ delivery_tag = AckTag1 })
    after ?TIMEOUT -> throw(timeout_waiting_for_deliver2)
    end,

    rabbit_ct_client_helpers:close_channel(Chan).

setup_legacy_shovels(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, setup_legacy_shovels1, [Config]).

setup_shovels(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, setup_shovels1, [Config]).

setup_legacy_shovels1(Config) ->
    _ = application:stop(rabbitmq_shovel),
    Hostname = ?config(rmq_hostname, Config),
    TcpPort = rabbit_ct_broker_helpers:get_node_config(Config, 0,
      tcp_port_amqp),
    %% a working config
    application:set_env(
      rabbitmq_shovel,
      shovels,
      [{test_shovel,
        [{sources,
          [{broker, rabbit_misc:format("amqp://~s:~b/%2f?heartbeat=5",
                                       [Hostname, TcpPort])},
           {declarations,
            [{'queue.declare',    [exclusive, auto_delete]},
             {'exchange.declare', [{exchange, ?EXCHANGE}, auto_delete]},
             {'queue.bind',       [{queue, <<>>}, {exchange, ?EXCHANGE},
                                   {routing_key, ?TO_SHOVEL}]}
            ]}]},
         {destinations,
          [{broker, rabbit_misc:format("amqp://~s:~b/%2f",
                                       [Hostname, TcpPort])}]},
         {queue, <<>>},
         {ack_mode, on_confirm},
         {publish_fields, [{exchange, ?EXCHANGE}, {routing_key, ?FROM_SHOVEL}]},
         {publish_properties, [{delivery_mode, 2},
                               {cluster_id,    <<"my-cluster">>},
                               {content_type,  ?SHOVELLED}]},
         {add_forward_headers, true},
         {add_timestamp_header, true}
        ]}],
      infinity),

    ok = application:start(rabbitmq_shovel),
    await_running_shovel(test_shovel).

setup_shovels1(Config) ->
    _ = application:stop(rabbitmq_shovel),
    Hostname = ?config(rmq_hostname, Config),
    TcpPort = rabbit_ct_broker_helpers:get_node_config(Config, 0,
      tcp_port_amqp),
    %% a working config
    application:set_env(
      rabbitmq_shovel,
      shovels,
      [{test_shovel,
        [{source,
          [{uris, [rabbit_misc:format("amqp://~s:~b/%2f?heartbeat=5",
                                      [Hostname, TcpPort])]},
           {declarations,
            [{'queue.declare',    [exclusive, auto_delete]},
             {'exchange.declare', [{exchange, ?EXCHANGE}, auto_delete]},
             {'queue.bind',       [{queue, <<>>}, {exchange, ?EXCHANGE},
                                   {routing_key, ?TO_SHOVEL}]}]},
           {queue, <<>>}]},
         {destination,
          [{uris, [rabbit_misc:format("amqp://~s:~b/%2f",
                                      [Hostname, TcpPort])]},
           {publish_fields, [{exchange, ?EXCHANGE}, {routing_key, ?FROM_SHOVEL}]},
           {publish_properties, [{delivery_mode, 2},
                                 {cluster_id,    <<"my-cluster">>},
                                 {content_type,  ?SHOVELLED}]},
           {add_forward_headers, true},
           {add_timestamp_header, true}]},
         {ack_mode, on_confirm}]}],
      infinity),

    ok = application:start(rabbitmq_shovel),
    await_running_shovel(test_shovel).

await_running_shovel(Name) ->
    case [N || {N, _, {running, _}, _}
                      <- rabbit_shovel_status:status(),
                         N =:= Name] of
        [_] -> ok;
        _   -> timer:sleep(100),
               await_running_shovel(Name)
    end.
