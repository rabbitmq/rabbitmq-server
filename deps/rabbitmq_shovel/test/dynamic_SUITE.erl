%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(dynamic_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

all() ->
    [
      {group, core_tests},
      {group, quorum_queue_tests},
      {group, stream_queue_tests}
    ].

groups() ->
    [
      {core_tests, [], [
          simple,
          set_properties_using_proplist,
          set_properties_using_map,
          set_empty_properties_using_proplist,
          set_empty_properties_using_map,
          headers,
          exchange,
          restart,
          change_definition,
          autodelete,
          validation,
          security_validation,
          get_connection_name
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
        {rmq_nodename_suffix, ?MODULE}
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
init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

simple(Config) ->
    with_ch(Config,
      fun (Ch) ->
              shovel_test_utils:set_param(
                Config,
                <<"test">>, [{<<"src-queue">>,  <<"src">>},
                             {<<"dest-queue">>, <<"dest">>}]),
              publish_expect(Ch, <<>>, <<"src">>, <<"dest">>, <<"hello">>)
      end).

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
            #amqp_msg{props = #'P_basic'{headers = undefined}} =
                  publish_expect(Ch, <<>>, <<"src">>, <<"dest">>, <<"hi1">>),

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


%%----------------------------------------------------------------------------

with_ch(Config, Fun) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    Fun(Ch),
    rabbit_ct_client_helpers:close_channel(Ch),
    cleanup(Config),
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

publish_count(Ch, X, Key, M, Count) ->
    [begin

         publish(Ch, X, Key, M)
     end || _ <- lists:seq(1, Count)].

expect_count(Ch, Q, M, Count) ->
    [begin
         expect(Ch, Q, M)
     end || _ <- lists:seq(1, Count)],
    expect_empty(Ch, Q).

invalid_param(Config, Value, User) ->
    {error_string, _} = rabbit_ct_broker_helpers:rpc(Config, 0,
      rabbit_runtime_parameters, set,
      [<<"/">>, <<"shovel">>, <<"invalid">>, Value, User]).

valid_param(Config, Value, User) ->
    rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, valid_param1, [Config, Value, User]).

valid_param1(_Config, Value, User) ->
    ok = rabbit_runtime_parameters:set(
           <<"/">>, <<"shovel">>, <<"name">>, Value, User),
    ok = rabbit_runtime_parameters:clear(<<"/">>, <<"shovel">>, <<"name">>, <<"acting-user">>).

invalid_param(Config, Value) -> invalid_param(Config, Value, none).
valid_param(Config, Value) -> valid_param(Config, Value, none).

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

await_autodelete(Config, Name) ->
    rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, await_autodelete1, [Config, Name]).

await_autodelete1(_Config, Name) ->
    shovel_test_utils:await(
      fun () -> not lists:member(Name, shovels_from_parameters()) end),
    shovel_test_utils:await(
      fun () ->
              not lists:member(Name,
                               shovel_test_utils:shovels_from_status())
      end).

shovels_from_parameters() ->
    L = rabbit_runtime_parameters:list(<<"/">>, <<"shovel">>),
    [rabbit_misc:pget(name, Shovel) || Shovel <- L].
