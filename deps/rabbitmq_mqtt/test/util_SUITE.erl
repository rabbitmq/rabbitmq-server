%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

-module(util_SUITE).
-compile([export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
      {group, util_tests}
    ].

groups() ->
    [
      {util_tests, [parallel], [
                                coerce_exchange,
                                coerce_vhost,
                                coerce_default_user,
                                coerce_default_pass,
                                mqtt_amqp_topic_translation
                               ]
      }
    ].

suite() ->
    [{timetrap, {seconds, 60}}].

init_per_suite(Config) ->
    ok = application:load(rabbitmq_mqtt),
    Config.
end_per_suite(Config) ->
    ok = application:unload(rabbitmq_mqtt),
    Config.
init_per_group(_, Config) -> Config.
end_per_group(_, Config) -> Config.
init_per_testcase(_, Config) -> Config.
end_per_testcase(_, Config) -> Config.

coerce_exchange(_) ->
    ?assertEqual(<<"amq.topic">>, rabbit_mqtt_util:env(exchange)).

coerce_vhost(_) ->
    ?assertEqual(<<"/">>, rabbit_mqtt_util:env(vhost)).

coerce_default_user(_) ->
    ?assertEqual(<<"guest_user">>, rabbit_mqtt_util:env(default_user)).

coerce_default_pass(_) ->
    ?assertEqual(<<"guest_pass">>, rabbit_mqtt_util:env(default_pass)).

mqtt_amqp_topic_translation(_) ->
    ok = application:set_env(rabbitmq_mqtt, sparkplug, true),
    {ok, {mqtt2amqp_fun, Mqtt2AmqpFun}, {amqp2mqtt_fun, Amqp2MqttFun}} =
        rabbit_mqtt_util:get_topic_translation_funs(),

    T0 = "/foo/bar/+/baz",
    T0_As_Amqp = <<".foo.bar.*.baz">>,
    T0_As_Mqtt = <<"/foo/bar/+/baz">>,
    ?assertEqual(T0_As_Amqp, Mqtt2AmqpFun(T0)),
    ?assertEqual(T0_As_Mqtt, Amqp2MqttFun(T0_As_Amqp)),

    T1 = "spAv1.0/foo/bar/+/baz",
    T1_As_Amqp = <<"spAv1___0.foo.bar.*.baz">>,
    T1_As_Mqtt = <<"spAv1.0/foo/bar/+/baz">>,
    ?assertEqual(T1_As_Amqp, Mqtt2AmqpFun(T1)),
    ?assertEqual(T1_As_Mqtt, Amqp2MqttFun(T1_As_Amqp)),

    T2 = "spBv2.90/foo/bar/+/baz",
    T2_As_Amqp = <<"spBv2___90.foo.bar.*.baz">>,
    T2_As_Mqtt = <<"spBv2.90/foo/bar/+/baz">>,
    ?assertEqual(T2_As_Amqp, Mqtt2AmqpFun(T2)),
    ?assertEqual(T2_As_Mqtt, Amqp2MqttFun(T2_As_Amqp)),

    ok = application:unset_env(rabbitmq_mqtt, sparkplug),
    ok.
