%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(amqp091_dynamic_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-import(rabbit_ct_helpers, [eventually/1]).
-import(shovel_test_utils, [await_autodelete/2,
                            invalid_param/2, invalid_param/3,
                            valid_param/2, valid_param/3]).

-compile(export_all).

-export([spawn_suspender_proc/1]).

all() ->
    [
      {group, core_tests},
      {group, core_tests_with_preclared_topology},
      {group, quorum_queue_tests},
      {group, stream_queue_tests}
    ].

groups() ->
    [
      {core_tests, [], [
          set_properties_using_proplist,
          set_properties_using_map,
          set_empty_properties_using_proplist,
          set_empty_properties_using_map,
          headers,
          exchange,
          missing_dest_exchange,
          restart,
          change_definition,
          autodelete,
          validation,
          security_validation,
          get_connection_name,
          credit_flow,
          missing_src_queue_with_src_predeclared,
          missing_dest_queue_with_dest_predeclared
        ]},
    {core_tests_with_preclared_topology, [], [
          missing_src_queue_without_src_predeclared,
          missing_dest_queue_without_dest_predeclared,
          missing_src_and_dest_queue_with_false_src_and_dest_predeclared
    ]},
    {quorum_queue_tests, [], [
          quorum_queues
    ]},

    {stream_queue_tests, [], [
          stream_queues
    ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE},
        {ignored_crashes, [
            "server_initiated_close,404",
            "writer,send_failed,closed"
        ]}
      ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(quorum_queue_tests, Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        false -> Config;
        _     -> {skip, "quorum queue tests are skipped in mixed mode"}
    end;
init_per_group(stream_queue_tests, Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        false -> Config;
        _     -> {skip, "stream queue tests are skipped in mixed mode"}
    end;
init_per_group(core_tests_with_preclared_topology, Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
        [rabbitmq_shovel, topology, [{predeclared, true}]]),
    Config;

init_per_group(_, Config) ->
    Config.

end_per_group(core_tests_with_preclared_topology, Config) ->
     ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, unset_env,
        [rabbitmq_shovel, topology]),
    Config;
end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------
quorum_queues(Config) ->
    with_ch(Config,
      fun (Ch) ->
              shovel_test_utils:set_param(
                Config,
                <<"test">>, [
                             {<<"src-queue">>,       <<"src">>},
                             {<<"dest-queue">>,      <<"dest">>},
                             {<<"src-queue-args">>,  #{<<"x-queue-type">> => <<"quorum">>}},
                             {<<"dest-queue-args">>, #{<<"x-queue-type">> => <<"quorum">>}}
                            ]),
              publish_expect(Ch, <<>>, <<"src">>, <<"dest">>, <<"hello">>)
      end).

stream_queues(Config) ->
    with_ch(Config,
      fun (Ch) ->
              shovel_test_utils:set_param(
                Config,
                <<"test">>, [
                             {<<"src-queue">>,       <<"src">>},
                             {<<"dest-queue">>,      <<"dest">>},
                             {<<"src-queue-args">>,  #{<<"x-queue-type">> => <<"stream">>}},
                             {<<"src-consumer-args">>,  #{<<"x-stream-offset">> => <<"first">>}}
                            ]),
              publish_expect(Ch, <<>>, <<"src">>, <<"dest">>, <<"hello">>)
      end).

set_properties_using_map(Config) ->
    with_ch(Config,
      fun (Ch) ->
              Ps = [{<<"src-queue">>,      <<"src">>},
                    {<<"dest-queue">>,     <<"dest">>},
                    {<<"publish-properties">>, #{<<"cluster_id">> => <<"x">>}}],
              shovel_test_utils:set_param(Config, <<"test">>, Ps),
              #amqp_msg{props = #'P_basic'{cluster_id = Cluster}} =
                  publish_expect(Ch, <<>>, <<"src">>, <<"dest">>, <<"hi">>),
              <<"x">> = Cluster
      end).

set_properties_using_proplist(Config) ->
    with_ch(Config,
      fun (Ch) ->
              Ps = [{<<"src-queue">>,      <<"src">>},
                    {<<"dest-queue">>,     <<"dest">>},
                    {<<"publish-properties">>, [{<<"cluster_id">>, <<"x">>}]}],
              shovel_test_utils:set_param(Config, <<"test">>, Ps),
              #amqp_msg{props = #'P_basic'{cluster_id = Cluster}} =
                  publish_expect(Ch, <<>>, <<"src">>, <<"dest">>, <<"hi">>),
              <<"x">> = Cluster
      end).

set_empty_properties_using_map(Config) ->
    with_ch(Config,
      fun (Ch) ->
              Ps = [{<<"src-queue">>,      <<"src">>},
                    {<<"dest-queue">>,     <<"dest">>},
                    {<<"publish-properties">>, #{}}],
              shovel_test_utils:set_param(Config, <<"test">>, Ps),
              #amqp_msg{props = #'P_basic'{}} =
                  publish_expect(Ch, <<>>, <<"src">>, <<"dest">>, <<"hi">>)
      end).

set_empty_properties_using_proplist(Config) ->
    with_ch(Config,
      fun (Ch) ->
              Ps = [{<<"src-queue">>,      <<"src">>},
                    {<<"dest-queue">>,     <<"dest">>},
                    {<<"publish-properties">>, []}],
              shovel_test_utils:set_param(Config, <<"test">>, Ps),
              #amqp_msg{props = #'P_basic'{}} =
                  publish_expect(Ch, <<>>, <<"src">>, <<"dest">>, <<"hi">>)
      end).

headers(Config) ->
    with_ch(Config,
        fun(Ch) ->
            %% No headers by default
            shovel_test_utils:set_param(Config,
                <<"test">>,
                [{<<"src-queue">>,            <<"src">>},
                 {<<"dest-queue">>,           <<"dest">>}]),
            ?assertMatch(#amqp_msg{props = #'P_basic'{headers = H0}}
                           when H0 == undefined orelse H0 == [],
                                publish_expect(Ch, <<>>, <<"src">>, <<"dest">>, <<"hi1">>)),

            shovel_test_utils:set_param(Config,
                <<"test">>,
                [{<<"src-queue">>,            <<"src">>},
                 {<<"dest-queue">>,           <<"dest">>},
                 {<<"add-forward-headers">>,  true},
                 {<<"add-timestamp-header">>, true}]),
            Timestmp = os:system_time(seconds),
            #amqp_msg{props = #'P_basic'{headers = Headers}} =
                  publish_expect(Ch, <<>>, <<"src">>, <<"dest">>, <<"hi2">>),
            [{<<"x-shovelled">>, _, [{table, ShovelledHeader}]},
             {<<"x-shovelled-timestamp">>, long, TS}] = Headers,
            %% We assume that the message was shovelled within a 2 second
            %% window.
            true = TS >= Timestmp andalso TS =< Timestmp + 2,
            {<<"shovel-type">>, _, <<"dynamic">>} =
                lists:keyfind(<<"shovel-type">>, 1, ShovelledHeader),
            {<<"shovel-vhost">>, _, <<"/">>} =
                lists:keyfind(<<"shovel-vhost">>, 1, ShovelledHeader),
            {<<"shovel-name">>, _, <<"test">>} =
                lists:keyfind(<<"shovel-name">>, 1, ShovelledHeader),

            shovel_test_utils:set_param(Config,
                <<"test">>,
                [{<<"src-queue">>,            <<"src">>},
                 {<<"dest-queue">>,           <<"dest">>},
                 {<<"add-timestamp-header">>, true}]),
            #amqp_msg{props = #'P_basic'{headers = [{<<"x-shovelled-timestamp">>,
                                                    long, _}]}} =
                  publish_expect(Ch, <<>>, <<"src">>, <<"dest">>, <<"hi3">>),

            shovel_test_utils:set_param(Config,
                <<"test">>,
                [{<<"src-queue">>,            <<"src">>},
                 {<<"dest-queue">>,           <<"dest">>},
                 {<<"add-forward-headers">>,  true}]),
            #amqp_msg{props = #'P_basic'{headers = [{<<"x-shovelled">>,
                                                     _, _}]}} =
                  publish_expect(Ch, <<>>, <<"src">>, <<"dest">>, <<"hi4">>)

        end).

exchange(Config) ->
    with_ch(Config,
      fun (Ch) ->
              amqp_channel:call(Ch, #'queue.declare'{queue   = <<"queue">>,
                                                     durable = true}),
              amqp_channel:call(
                Ch, #'queue.bind'{queue       = <<"queue">>,
                                  exchange    = <<"amq.topic">>,
                                  routing_key = <<"test-key">>}),
              shovel_test_utils:set_param(Config,
                        <<"test">>, [{<<"src-exchange">>,    <<"amq.direct">>},
                                     {<<"src-exchange-key">>,<<"test-key">>},
                                     {<<"dest-exchange">>,   <<"amq.topic">>}]),
              publish_expect(Ch, <<"amq.direct">>, <<"test-key">>,
                             <<"queue">>, <<"hello">>),
              shovel_test_utils:set_param(Config,
                        <<"test">>, [{<<"src-exchange">>,     <<"amq.direct">>},
                                     {<<"src-exchange-key">>, <<"test-key">>},
                                     {<<"dest-exchange">>,    <<"amq.topic">>},
                                     {<<"dest-exchange-key">>,<<"new-key">>}]),
              publish(Ch, <<"amq.direct">>, <<"test-key">>, <<"hello">>),
              expect_empty(Ch, <<"queue">>),
              amqp_channel:call(
                Ch, #'queue.bind'{queue       = <<"queue">>,
                                  exchange    = <<"amq.topic">>,
                                  routing_key = <<"new-key">>}),
              publish_expect(Ch, <<"amq.direct">>, <<"test-key">>,
                             <<"queue">>, <<"hello">>)
      end).

missing_src_queue_with_src_predeclared(Config) ->
    with_ch(Config,
        fun (Ch) ->
            amqp_channel:call(
              Ch, #'queue.declare'{queue = <<"dest">>,
                                   durable = true}),
            amqp_channel:call(
              Ch, #'exchange.declare'{exchange = <<"dest-ex">>}),
            amqp_channel:call(
              Ch, #'queue.bind'{queue = <<"dest">>,
                                exchange = <<"dest-ex">>,
                                routing_key = <<"dest-key">>}),

            shovel_test_utils:set_param_nowait(Config,
                        <<"test">>, [{<<"src-queue">>, <<"src">>},
                                        {<<"src-predeclared">>, true},
                                        {<<"dest-exchange">>, <<"dest-ex">>},
                                        {<<"dest-exchange-key">>, <<"dest-key">>},
                                        {<<"src-prefetch-count">>, 1}]),
            shovel_test_utils:await_shovel(Config, 0, <<"test">>, terminated),
            expect_missing_queue(Ch, <<"src">>),

            with_newch(Config,
                fun(Ch2) ->
                    amqp_channel:call(
                        Ch2, #'queue.declare'{queue = <<"src">>,
                                        durable = true}),
                    amqp_channel:call(
                        Ch2, #'queue.bind'{queue = <<"src">>,
                                        exchange = <<"amq.direct">>,
                                        routing_key = <<"src-key">>}),
                    shovel_test_utils:await_shovel(Config, 0, <<"test">>, running),

                    publish_expect(Ch2, <<"amq.direct">>, <<"src-key">>, <<"dest">>, <<"hello!">>)
                end)
    end).


missing_src_and_dest_queue_with_false_src_and_dest_predeclared(Config) ->
    with_ch(Config,
        fun (Ch) ->

            shovel_test_utils:set_param(
                Config,
                <<"test">>, [{<<"src-queue">>,  <<"src">>},
                             {<<"src-predeclared">>, false},
                             {<<"dest-predeclared">>, false},
                             {<<"dest-queue">>, <<"dest">>}]),
              publish_expect(Ch, <<>>, <<"src">>, <<"dest">>, <<"hello">>)

    end).

missing_dest_queue_with_dest_predeclared(Config) ->
    with_ch(Config,
        fun (Ch) ->
            amqp_channel:call(
                Ch, #'queue.declare'{queue = <<"src">>,
                                 durable = true}),
            amqp_channel:call(
                Ch, #'queue.bind'{queue = <<"src">>,
                                exchange = <<"amq.direct">>,
                                routing_key = <<"src-key">>}),

            shovel_test_utils:set_param_nowait(Config,
                        <<"test">>, [{<<"src-queue">>, <<"src">>},
                                        {<<"dest-predeclared">>, true},
                                        {<<"dest-queue">>, <<"dest">>},
                                        {<<"src-prefetch-count">>, 1}]),
            shovel_test_utils:await_shovel(Config, 0, <<"test">>, terminated),
            expect_missing_queue(Ch, <<"dest">>),

            with_newch(Config,
                fun(Ch2) ->
                    amqp_channel:call(
                        Ch2, #'queue.declare'{queue = <<"dest">>,
                                            durable = true}),

                    shovel_test_utils:await_shovel(Config, 0, <<"test">>, running),

                    publish_expect(Ch2, <<"amq.direct">>, <<"src-key">>, <<"dest">>, <<"hello!">>)
                end)
    end).

missing_src_queue_without_src_predeclared(Config) ->
    with_ch(Config,
        fun (Ch) ->
            amqp_channel:call(
              Ch, #'queue.declare'{queue = <<"dest">>,
                                   durable = true}),
            amqp_channel:call(
              Ch, #'exchange.declare'{exchange = <<"dest-ex">>}),
            amqp_channel:call(
              Ch, #'queue.bind'{queue = <<"dest">>,
                                exchange = <<"dest-ex">>,
                                routing_key = <<"dest-key">>}),

            shovel_test_utils:set_param_nowait(Config,
                        <<"test">>, [{<<"src-queue">>, <<"src">>},
                                        {<<"dest-exchange">>, <<"dest-ex">>},
                                        {<<"dest-exchange-key">>, <<"dest-key">>},
                                        {<<"src-prefetch-count">>, 1}]),
            shovel_test_utils:await_shovel(Config, 0, <<"test">>, terminated),
            expect_missing_queue(Ch, <<"src">>),

            with_newch(Config,
                fun(Ch2) ->
                    amqp_channel:call(
                        Ch2, #'queue.declare'{queue = <<"src">>,
                                        durable = true}),
                    amqp_channel:call(
                        Ch2, #'queue.bind'{queue = <<"src">>,
                                        exchange = <<"amq.direct">>,
                                        routing_key = <<"src-key">>}),
                    shovel_test_utils:await_shovel(Config, 0, <<"test">>, running),

                    publish_expect(Ch2, <<"amq.direct">>, <<"src-key">>, <<"dest">>, <<"hello!">>)
                end)
    end).


missing_dest_queue_without_dest_predeclared(Config) ->
    with_ch(Config,
        fun (Ch) ->
            amqp_channel:call(
                Ch, #'queue.declare'{queue = <<"src">>,
                                 durable = true}),
            amqp_channel:call(
                Ch, #'queue.bind'{queue = <<"src">>,
                                exchange = <<"amq.direct">>,
                                routing_key = <<"src-key">>}),

            shovel_test_utils:set_param_nowait(Config,
                        <<"test">>, [{<<"src-queue">>, <<"src">>},
                                        {<<"dest-queue">>, <<"dest">>},
                                        {<<"src-prefetch-count">>, 1}]),
            shovel_test_utils:await_shovel(Config, 0, <<"test">>, terminated),
            expect_missing_queue(Ch, <<"dest">>),

            with_newch(Config,
                fun(Ch2) ->
                    amqp_channel:call(
                        Ch2, #'queue.declare'{queue = <<"dest">>,
                                            durable = true}),

                    shovel_test_utils:await_shovel(Config, 0, <<"test">>, running),

                    publish_expect(Ch2, <<"amq.direct">>, <<"src-key">>, <<"dest">>, <<"hello!">>)
                end)
    end).

missing_dest_exchange(Config) ->
    with_ch(Config,
        fun (Ch) ->
            amqp_channel:call(
            Ch, #'queue.declare'{queue = <<"src">>,
                                 durable = true}),
            amqp_channel:call(
              Ch, #'queue.declare'{queue = <<"dest">>,
                                   durable = true}),
            amqp_channel:call(
              Ch, #'queue.bind'{queue = <<"src">>,
                                exchange = <<"amq.direct">>,
                                routing_key = <<"src-key">>}),
            shovel_test_utils:set_param(Config,
                        <<"test">>, [{<<"src-queue">>, <<"src">>},
                                        {<<"dest-exchange">>, <<"dest-ex">>},
                                        {<<"dest-exchange-key">>, <<"dest-key">>},
                                        {<<"src-prefetch-count">>, 1}]),
            publish(Ch, <<"amq.direct">>, <<"src-key">>, <<"hello">>),
            expect_empty(Ch, <<"src">>),
            amqp_channel:call(
              Ch, #'exchange.declare'{exchange = <<"dest-ex">>}),
            amqp_channel:call(
              Ch, #'queue.bind'{queue = <<"dest">>,
                                exchange = <<"dest-ex">>,
                                routing_key = <<"dest-key">>}),
            publish_expect(Ch, <<"amq.direct">>, <<"src-key">>, <<"dest">>, <<"hello!">>)
end).

restart(Config) ->
    with_ch(Config,
      fun (Ch) ->
              shovel_test_utils:set_param(Config,
                        <<"test">>, [{<<"src-queue">>,  <<"src">>},
                                     {<<"dest-queue">>, <<"dest">>}]),
              %% The catch is because connections link to the shovel,
              %% so one connection will die, kill the shovel, kill
              %% the other connection, then we can't close it
              Conns = rabbit_ct_broker_helpers:rpc(Config, 0,
                rabbit_direct, list, []),
              [catch amqp_connection:close(C) || C <- Conns],
              publish_expect(Ch, <<>>, <<"src">>, <<"dest">>, <<"hello">>)
      end).

change_definition(Config) ->
    with_ch(Config,
      fun (Ch) ->
              shovel_test_utils:set_param(
                Config,
                <<"test">>, [{<<"src-queue">>,  <<"src">>},
                             {<<"dest-queue">>, <<"dest">>}]),
              publish_expect(Ch, <<>>, <<"src">>, <<"dest">>, <<"hello">>),
              shovel_test_utils:set_param(
                Config,
                <<"test">>, [{<<"src-queue">>,  <<"src">>},
                             {<<"dest-queue">>, <<"dest2">>}]),
              publish_expect(Ch, <<>>, <<"src">>, <<"dest2">>, <<"hello">>),
              expect_empty(Ch, <<"dest">>),
              shovel_test_utils:clear_param(Config, <<"test">>),
              publish_expect(Ch, <<>>, <<"src">>, <<"src">>, <<"hello">>),
              expect_empty(Ch, <<"dest">>),
              expect_empty(Ch, <<"dest2">>)
      end).

autodelete(Config) ->
    autodelete_case(Config, {<<"on-confirm">>, 0, 100, 0}),
    autodelete_case(Config, {<<"on-confirm">>, 50, 50, 50}),
    autodelete_case(Config, {<<"on-confirm">>, <<"queue-length">>,  0, 100}),
    autodelete_case(Config, {<<"on-publish">>, <<"queue-length">>,  0, 100}),
    autodelete_case(Config, {<<"on-publish">>, 50,                 50,  50}),
    %% no-ack is not compatible with explicit count
    autodelete_case(Config, {<<"no-ack">>,     <<"queue-length">>,  0, 100}),
    ok.

autodelete_case(Config, Args) ->
    with_ch(Config, autodelete_do(Config, Args)).

autodelete_do(Config, {AckMode, After, ExpSrc, ExpDest}) ->
    fun (Ch) ->
            amqp_channel:call(Ch, #'confirm.select'{}),
            amqp_channel:call(Ch, #'queue.declare'{queue = <<"src">>}),
            publish_count(Ch, <<>>, <<"src">>, <<"hello">>, 100),
            amqp_channel:wait_for_confirms(Ch),
            shovel_test_utils:set_param_nowait(
              Config,
              <<"test">>, [{<<"src-queue">>,    <<"src">>},
                           {<<"dest-queue">>,   <<"dest">>},
                           {<<"src-prefetch-count">>, 50},
                           {<<"ack-mode">>,     AckMode},
                           {<<"src-delete-after">>, After}]),
            await_autodelete(Config, <<"test">>),
            expect_count(Ch, <<"dest">>, <<"hello">>, ExpDest),
            expect_count(Ch, <<"src">>, <<"hello">>, ExpSrc)
    end.

validation(Config) ->
    URIs = [{<<"src-uri">>,  <<"amqp://">>},
            {<<"dest-uri">>, <<"amqp://">>}],

    %% Need valid src and dest URIs
    invalid_param(Config, []),
    invalid_param(Config,
                  [{<<"src-queue">>, <<"test">>},
                   {<<"src-uri">>,   <<"derp">>},
                   {<<"dest-uri">>,  <<"amqp://">>}]),
    invalid_param(Config,
                  [{<<"src-queue">>, <<"test">>},
                   {<<"src-uri">>,   [<<"derp">>]},
                   {<<"dest-uri">>,  <<"amqp://">>}]),
    invalid_param(Config,
                  [{<<"src-queue">>, <<"test">>},
                   {<<"dest-uri">>,  <<"amqp://">>}]),

    %% Also need src exchange or queue
    invalid_param(Config,
                  URIs),
    valid_param(Config,
                [{<<"src-exchange">>, <<"test">>} | URIs]),
    QURIs =     [{<<"src-queue">>,    <<"test">>} | URIs],
    valid_param(Config, QURIs),

    %% But not both
    invalid_param(Config,
                  [{<<"src-exchange">>, <<"test">>} | QURIs]),

    %% Check these are of right type
    invalid_param(Config,
                  [{<<"prefetch-count">>,  <<"three">>} | QURIs]),
    invalid_param(Config,
                  [{<<"reconnect-delay">>, <<"three">>} | QURIs]),
    invalid_param(Config,
                  [{<<"ack-mode">>,        <<"whenever">>} | QURIs]),
    invalid_param(Config,
                  [{<<"src-delete-after">>,    <<"whenever">>} | QURIs]),

    %% Check properties have to look property-ish
    valid_param(Config,
                [{<<"src-exchange">>, <<"test">>},
                 {<<"publish-properties">>, [{<<"cluster_id">>, <<"rabbit@localhost">>},
                                             {<<"delivery_mode">>, 2}]}
                 | URIs]),
    valid_param(Config,
                #{<<"publish-properties">> => #{<<"cluster_id">>    => <<"rabbit@localhost">>,
                                                <<"delivery_mode">> => 2},
                  <<"src-exchange">> => <<"test">>,
                  <<"src-uri">>      => <<"amqp://">>,
                  <<"dest-uri">>     => <<"amqp://">>}),
    invalid_param(Config,
                  [{<<"publish-properties">>, [{<<"nonexistent">>, <<>>}]}]),
    invalid_param(Config,
                  #{<<"publish-properties">> => #{<<"nonexistent">> => <<>>}}),
    invalid_param(Config,
                  [{<<"publish-properties">>, [{<<"cluster_id">>, 2}]}]),
    invalid_param(Config,
                  [{<<"publish-properties">>, <<"something">>}]),

    %% Can't use explicit message count and no-ack together
    invalid_param(Config,
                  [{<<"src-delete-after">>,    1},
                   {<<"ack-mode">>,        <<"no-ack">>} | QURIs]),
    %% superseded by src-delete-after
    invalid_param(Config,
                  [{<<"delete-after">>,    1},
                   {<<"ack-mode">>,        <<"no-ack">>} | QURIs]),
    ok.

security_validation(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, security_validation_add_user, []),

    Qs = [{<<"src-queue">>, <<"test">>},
          {<<"dest-queue">>, <<"test2">>}],

    A = lookup_user(Config, <<"a">>),
    valid_param(Config, [{<<"src-uri">>,  <<"amqp://localhost:5672/a">>},
                         {<<"dest-uri">>, <<"amqp://localhost:5672/b">>} | Qs], A),
    %% src-uri and dest-uri are not valid URIs
    invalid_param(Config,
                  [{<<"src-uri">>,  <<"an arbitrary string">>},
                   {<<"dest-uri">>, <<"\o/ \o/ \o/">>} | Qs], A),
    %% missing src-queue and dest-queue
    invalid_param(Config,
                  [{<<"src-uri">>,  <<"amqp://localhost/a">>},
                   {<<"dest-uri">>, <<"amqp://localhost/b">>}], A),

    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, security_validation_remove_user, []),
    ok.

security_validation_add_user() ->
    [begin
         rabbit_vhost:add(U, <<"acting-user">>),
         rabbit_auth_backend_internal:add_user(U, <<>>, <<"acting-user">>),
         rabbit_auth_backend_internal:set_permissions(
           U, U, <<".*">>, <<".*">>, <<".*">>, <<"acting-user">>)
     end || U <- [<<"a">>, <<"b">>]],
    ok.

security_validation_remove_user() ->
    [begin
         rabbit_vhost:delete(U, <<"acting-user">>),
         rabbit_auth_backend_internal:delete_user(U, <<"acting-user">>)
     end || U <- [<<"a">>, <<"b">>]],
    ok.

get_connection_name(_Config) ->
    <<"Shovel static_shovel_name_as_atom">> = rabbit_shovel_worker:get_connection_name(static_shovel_name_as_atom),
    <<"Shovel dynamic_shovel_name_as_binary">> = rabbit_shovel_worker:get_connection_name({<<"/">>, <<"dynamic_shovel_name_as_binary">>}),
    <<"Shovel">> = rabbit_shovel_worker:get_connection_name({<<"/">>, {unexpected, tuple}}),
    <<"Shovel">> = rabbit_shovel_worker:get_connection_name({one, two, three}),
    <<"Shovel">> = rabbit_shovel_worker:get_connection_name(<<"anything else">>).

credit_flow(Config) ->
    OrigCredit = set_default_credit(Config, {20, 10}),

    with_ch(Config,
      fun (Ch) ->
              try
                  shovel_test_utils:set_param_nowait(
                    Config,
                    <<"test">>, [{<<"src-queue">>,    <<"src">>},
                                 {<<"dest-queue">>,   <<"dest">>},
                                 {<<"src-prefetch-count">>, 50},
                                 {<<"ack-mode">>,     <<"on-publish">>},
                                 {<<"src-delete-after">>, <<"never">>}]),
                  shovel_test_utils:await_shovel(Config, <<"test">>),
                  running = shovel_test_utils:get_shovel_status(Config, <<"test">>),

                  ShovelPid = find_shovel_pid(Config),
                  #{dest :=
                        #{current :=
                              {_DestConn, DestChan, _DestUri}}} =
                      get_shovel_state(ShovelPid),
                  WriterPid = find_writer_pid_for_channel(Config, DestChan),

                  %% When the broker-side channel is blocked by flow
                  %% control, it stops reading from the tcp
                  %% socket. After all the OS, BEAM and process buffers
                  %% are full, gen_tcp:send/2 will block the writer
                  %% process. Simulate this by suspending the writer process.
                  true = suspend_process(Config, WriterPid),

                  %% Publish 1000 messages to the src queue
                  amqp_channel:call(Ch, #'confirm.select'{}),
                  publish_count(Ch, <<>>, <<"src">>, <<"hello">>, 1000),
                  amqp_channel:wait_for_confirms(Ch),

                  %% Wait until the shovel is blocked
                  shovel_test_utils:await(
                    fun() ->
                            case shovel_test_utils:get_shovel_status(Config, <<"test">>) of
                                flow -> true;
                                Status -> Status
                            end
                    end,
                    5000),

                  %% There should be only one process with a message buildup
                  Top = [{WriterPid, MQLen, _}, {_, P, _} | _] =
                      rabbit_ct_broker_helpers:rpc(
                        Config, 0, recon, proc_count, [message_queue_len, 10]),
                  ct:pal("Top processes by message queue length: ~p", [Top]),
                  ?assert(P < 3),

                  %% The writer process should have only a limited
                  %% message queue. The shovel stops sending messages
                  %% when the channel and shovel process used up all
                  %% their initial credit (that is 20 + 20).
                  2 * 20 = MQLen = proc_info(WriterPid, message_queue_len),

                  %% Most messages should still be in the queue either ready or unacked
                  ExpDestCnt = 0,
                  #{messages := ExpDestCnt} = message_count(Config, <<"dest">>),
                  ExpSrcCnt = 1000 - MQLen,
                  #{messages := ExpSrcCnt,
                    messages_unacknowledged := 50} = message_count(Config, <<"src">>),

                  %% After the writer process is resumed all messages
                  %% should be shoveled to the dest queue, and process
                  %% message queues should be empty
                  resume_process(Config),

                  shovel_test_utils:await(
                    fun() ->
                            #{messages := Cnt} = message_count(Config, <<"src">>),
                            Cnt =:= 0
                    end,
                    5000),
                  #{messages := 1000} = message_count(Config, <<"dest">>),
                  [{_, P, _}] =
                      rabbit_ct_broker_helpers:rpc(
                        Config, 0, recon, proc_count, [message_queue_len, 1]),
                  ?assert(P < 3),

                  %% Status only transitions from flow to running
                  %% after a 1 second state-change-interval
                  timer:sleep(1000),
                  running = shovel_test_utils:get_shovel_status(Config, <<"test">>)
              after
                  resume_process(Config),
                  set_default_credit(Config, OrigCredit)
              end
      end).

%%----------------------------------------------------------------------------

with_ch(Config, Fun) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Fun(Ch),
    rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch),
    cleanup(Config),
    ok.

with_newch(Config, Fun) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Fun(Ch),
    rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch),
    ok.

publish(Ch, X, Key, Payload) when is_binary(Payload) ->
    publish(Ch, X, Key, #amqp_msg{payload = Payload});

publish(Ch, X, Key, Msg = #amqp_msg{}) ->
    amqp_channel:cast(Ch, #'basic.publish'{exchange    = X,
                                           routing_key = Key}, Msg).

publish_expect(Ch, X, Key, Q, Payload) ->
    publish(Ch, X, Key, Payload),
    expect(Ch, Q, Payload).

expect(Ch, Q, Payload) ->
    amqp_channel:subscribe(Ch, #'basic.consume'{queue  = Q,
                                                no_ack = true}, self()),
    CTag = receive
        #'basic.consume_ok'{consumer_tag = CT} -> CT
    end,
    Msg = receive
              {#'basic.deliver'{}, #amqp_msg{payload = Payload} = M} ->
                  M
          after 4000 ->
                  exit({not_received, Payload})
          end,
    amqp_channel:call(Ch, #'basic.cancel'{consumer_tag = CTag}),
    Msg.

expect_empty(Ch, Q) ->
    #'basic.get_empty'{} = amqp_channel:call(Ch, #'basic.get'{ queue = Q }).

expect_missing_queue(Ch, Q) ->
    try
        amqp_channel:call(Ch, #'queue.declare'{queue   = Q,
                                               passive = true}),
        ct:fail(queue_still_exists)
    catch exit:{{shutdown, {server_initiated_close, ?NOT_FOUND, _Text}}, _} ->
        ok
    end.
expect_missing_exchange(Ch, X) ->
    try
        amqp_channel:call(Ch, #'exchange.declare'{exchange   = X,
                                                 passive = true}),
        ct:fail(exchange_still_exists)
    catch exit:{{shutdown, {server_initiated_close, ?NOT_FOUND, _Text}}, _} ->
        ok
    end.

publish_count(Ch, X, Key, M, Count) ->
    [begin

         publish(Ch, X, Key, M)
     end || _ <- lists:seq(1, Count)].

expect_count(Ch, Q, M, Count) ->
    [begin
         expect(Ch, Q, M)
     end || _ <- lists:seq(1, Count)],
    expect_empty(Ch, Q).

lookup_user(Config, Name) ->
    {ok, User} = rabbit_ct_broker_helpers:rpc(Config, 0,
      rabbit_access_control, check_user_login, [Name, []]),
    User.

cleanup(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, cleanup1, [Config]).

cleanup1(_Config) ->
    [rabbit_runtime_parameters:clear(rabbit_misc:pget(vhost, P),
                                     rabbit_misc:pget(component, P),
                                     rabbit_misc:pget(name, P),
                                     <<"acting-user">>) ||
        P <- rabbit_runtime_parameters:list()],
    [rabbit_amqqueue:delete(Q, false, false, <<"acting-user">>)
     || Q <- rabbit_amqqueue:list()].

set_default_credit(Config, Value) ->
    Key = credit_flow_default_credit,
    OrigValue = rabbit_ct_broker_helpers:rpc(Config, persistent_term, get, [Key]),
    ok = rabbit_ct_broker_helpers:rpc(Config, persistent_term, put, [Key, Value]),
    OrigValue.

message_count(Config, QueueName) ->
    Resource = rabbit_misc:r(<<"/">>, queue, QueueName),
    {ok, Q} = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, lookup, [Resource]),
    maps:from_list(
      rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, info,
                                   [Q, [messages, messages_unacknowledged]])).

%% A process can be only suspended by another process on the same node
suspend_process(Config, Pid) ->
    true = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, spawn_suspender_proc, [Pid]),
    suspended = proc_info(Pid, status),
    true.

%% When the suspender process terminates, the suspended process is also resumed
resume_process(Config) ->
    case rabbit_ct_broker_helpers:rpc(Config, 0, erlang, whereis, [suspender]) of
        undefined ->
            false;
        SusPid ->
            exit(SusPid, kill)
    end.

spawn_suspender_proc(Pid) ->
    undefined = whereis(suspender),
    ReqPid = self(),
    SusPid =
        spawn(
          fun() ->
                  register(suspender, self()),
                  Res = catch (true = erlang:suspend_process(Pid)),
                  ReqPid ! {suspend_res, self(), Res},
                  %% wait indefinitely
                  receive stop -> ok end
          end),
    receive
        {suspend_res, SusPid, Res} -> Res
    after
        5000 -> timeout
    end.

find_shovel_pid(Config) ->
    [ShovelPid] = [P || P <- rabbit_ct_broker_helpers:rpc(
                               Config, 0, erlang, processes, []),
                        rabbit_shovel_worker ==
                            (catch element(1, erpc:call(node(P), proc_lib, initial_call, [P])))],
    ShovelPid.

get_shovel_state(ShovelPid) ->
    gen_server2:with_state(ShovelPid, fun rabbit_shovel_worker:get_internal_config/1).

find_writer_pid_for_channel(Config, ChanPid) ->
    {amqp_channel, ChanName} = process_name(ChanPid),
    [WriterPid] = [P || P <- rabbit_ct_broker_helpers:rpc(
                               Config, 0, erlang, processes, []),
                        {rabbit_writer, ChanName} == process_name(P)],
    WriterPid.

process_name(Pid) ->
    try proc_info(Pid, dictionary) of
        Dict ->
            proplists:get_value(process_name, Dict)
    catch _:_ ->
            undefined
    end.

proc_info(Pid, Item) ->
    {Item, Value} = erpc:call(node(Pid), erlang, process_info, [Pid, Item]),
    Value.
