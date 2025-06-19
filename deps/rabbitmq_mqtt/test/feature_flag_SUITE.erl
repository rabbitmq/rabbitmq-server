%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

%% This suite should be deleted when feature flag 'rabbitmq_4.1.0' becomes required.
-module(feature_flag_SUITE).
-compile([export_all,
          nowarn_export_all]).

-include_lib("eunit/include/eunit.hrl").

-import(util,
        [connect/2,
         connect/3,
         non_clean_sess_opts/0
        ]).

-define(RC_SESSION_TAKEN_OVER, 16#8E).

all() ->
    [migrate_binding_args].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(
                Config,
                [{mqtt_version, v5},
                 {rmq_nodename_suffix, ?MODULE},
                 {start_rmq_with_plugins_disabled, true}
                ]),
    Config2 = rabbit_ct_helpers:merge_app_env(
                Config1,
                {rabbit, [{forced_feature_flags_on_init, []}]}),
    Config3 = rabbit_ct_helpers:run_setup_steps(
                Config2,
                rabbit_ct_broker_helpers:setup_steps() ++
                    rabbit_ct_client_helpers:setup_steps()),
    util:enable_plugin(Config3, rabbitmq_mqtt),
    Config3.

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

migrate_binding_args(Config) ->
    %% Feature flag rabbitmq_4.1.0 enables binding arguments v2.
    FeatureFlag = 'rabbitmq_4.1.0',
    ?assertNot(rabbit_ct_broker_helpers:is_feature_flag_enabled(Config, FeatureFlag)),

    Sub1a = connect(<<"sub 1">>, Config, non_clean_sess_opts()),
    {ok, _, [0]} = emqtt:subscribe(Sub1a, <<"x/+">>, qos0),
    ok = emqtt:disconnect(Sub1a),

    Sub2a = connect(<<"sub 2">>, Config,non_clean_sess_opts()),
    {ok, _, [0, 1]} = emqtt:subscribe(
                        Sub2a,
                        #{'Subscription-Identifier' => 9},
                        [{<<"x/y">>, [{nl, false}, {rap, false}, {qos, qos0}]},
                         {<<"z">>, [{nl, true}, {rap, true}, {qos, qos1}]}]),

    Pub = connect(<<"pub">>, Config),
    {ok, _} = emqtt:publish(Pub, <<"x/y">>, <<"m1">>, [{retain, true}, {qos, 1}]),
    receive {publish, #{client_pid := Sub2a,
                        qos := 0,
                        topic := <<"x/y">>,
                        payload := <<"m1">>,
                        retain := false}} -> ok
    after 10_000 -> ct:fail({missing_publish, ?LINE})
    end,

    ?assertEqual(ok, rabbit_ct_broker_helpers:enable_feature_flag(Config, FeatureFlag)),

    %% Connecting causes binding args to be migrated from v1 to v2.
    Sub1b = connect(<<"sub 1">>, Config, [{clean_start, false}]),
    receive {publish, #{client_pid := Sub1b,
                        qos := 0,
                        topic := <<"x/y">>,
                        payload := <<"m1">>}} -> ok
    after 10_000 -> ct:fail({missing_publish, ?LINE})
    end,

    unlink(Sub2a),
    %% Connecting causes binding args to be migrated from v1 to v2.
    Sub2b = connect(<<"sub 2">>, Config, [{clean_start, false}]),
    receive {disconnected, ?RC_SESSION_TAKEN_OVER, #{}} -> ok
    after 10_000 -> ct:fail({missing_disconnected, ?LINE})
    end,

    {ok, _} = emqtt:publish(Sub2b, <<"z">>, <<"m2">>, qos1),
    %% We should not receive m2 since it's a local publish.
    {ok, _} = emqtt:publish(Pub, <<"z">>, <<"m3">>, [{retain, true}, {qos, qos1}]),
    receive {publish, Publish} ->
                ?assertMatch(#{client_pid := Sub2b,
                               qos := 1,
                               topic := <<"z">>,
                               payload := <<"m3">>,
                               properties := #{'Subscription-Identifier' := 9},
                               retain := true},
                             Publish)
    after 10_000 -> ct:fail({missing_publish, ?LINE})
    end,

    ok = emqtt:disconnect(Sub1b),
    ok = emqtt:disconnect(Sub2b),
    ok = emqtt:disconnect(Pub).
