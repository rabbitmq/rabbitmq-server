%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(util_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("eunit/include/eunit.hrl").

all() ->
    [
      {group, tests}
    ].

groups() ->
    [
     {tests, [parallel], [
                          coerce_vhost,
                          mqtt_amqp_topic_translation,
                          multi_level_wildcard_expansions
                         ]
     }
    ].

init_per_suite(Config) ->
    ok = application:load(rabbitmq_mqtt),
    Config.

end_per_suite(Config) ->
    ok = application:unload(rabbitmq_mqtt),
    Config.

coerce_vhost(_) ->
    ?assertEqual(<<"/">>, rabbit_mqtt_util:env(vhost)).

mqtt_amqp_topic_translation(_) ->
    ok = application:set_env(rabbitmq_mqtt, sparkplug, true),
    ok = rabbit_mqtt_util:init_sparkplug(),

    T0 = <<"/foo/bar/+/baz">>,
    T0_As_Amqp = <<".foo.bar.*.baz">>,
    T0_As_Mqtt = <<"/foo/bar/+/baz">>,
    ?assertEqual(T0_As_Amqp, rabbit_mqtt_util:mqtt_to_amqp(T0)),
    ?assertEqual(T0_As_Mqtt, rabbit_mqtt_util:amqp_to_mqtt(T0_As_Amqp)),

    T1 = <<"spAv1.0/foo/bar/+/baz">>,
    T1_As_Amqp = <<"spAv1___0.foo.bar.*.baz">>,
    T1_As_Mqtt = <<"spAv1.0/foo/bar/+/baz">>,
    ?assertEqual(T1_As_Amqp, rabbit_mqtt_util:mqtt_to_amqp(T1)),
    ?assertEqual(T1_As_Mqtt, rabbit_mqtt_util:amqp_to_mqtt(T1_As_Amqp)),

    T2 = <<"spBv2.90/foo/bar/+/baz">>,
    T2_As_Amqp = <<"spBv2___90.foo.bar.*.baz">>,
    T2_As_Mqtt = <<"spBv2.90/foo/bar/+/baz">>,
    ?assertEqual(T2_As_Amqp, rabbit_mqtt_util:mqtt_to_amqp(T2)),
    ?assertEqual(T2_As_Mqtt, rabbit_mqtt_util:amqp_to_mqtt(T2_As_Amqp)),

    ok = application:unset_env(rabbitmq_mqtt, sparkplug).

multi_level_wildcard_expansions(_) ->
    ?assertEqual(none, rabbit_mqtt_util:multi_level_wildcard_expansions(<<"allowed.x">>)),
    ?assertEqual(none, rabbit_mqtt_util:multi_level_wildcard_expansions(<<"allowed.*">>)),
    ?assertEqual(none, rabbit_mqtt_util:multi_level_wildcard_expansions(<<"allowed.a#">>)),
    ?assertEqual({<<"x">>, <<"x.x.x.x.x.x.x.x">>},
                 rabbit_mqtt_util:multi_level_wildcard_expansions(<<"#">>)),
    ?assertEqual({<<"allowed.x">>, <<"allowed.x.x.x.x.x.x.x.x">>},
                 rabbit_mqtt_util:multi_level_wildcard_expansions(<<"allowed.#">>)),
    ?assertEqual({<<"allowed.x.y">>, <<"allowed.x.x.x.x.x.x.x.x.y">>},
                 rabbit_mqtt_util:multi_level_wildcard_expansions(<<"allowed.#.y">>)),
    ?assertEqual({<<"a.x.b.*.x">>, <<"a.x.x.x.x.x.x.x.x.b.*.x.x.x.x.x.x.x.x">>},
                 rabbit_mqtt_util:multi_level_wildcard_expansions(<<"a.#.b.*.#">>)).
