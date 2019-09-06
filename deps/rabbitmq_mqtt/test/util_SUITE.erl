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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.

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
