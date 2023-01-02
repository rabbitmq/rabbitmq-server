%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(retainer_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-import(util, [expect_publishes/3,
               connect/3]).

all() ->
    [
     {group, dets},
     {group, ets},
     {group, noop}
    ].

groups() ->
    [
     {dets, [], tests()},
     {ets, [], tests()},
     {noop, [], [does_not_retain]}
    ].

tests() ->
    [
     coerce_configuration_data,
     should_translate_amqp2mqtt_on_publish,
     should_translate_amqp2mqtt_on_retention,
     should_translate_amqp2mqtt_on_retention_search
    ].

suite() ->
    [{timetrap, {minutes, 2}}].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(Group, Config0) ->
    Config = rabbit_ct_helpers:set_config(
               Config0,
               [
                {rmq_nodename_suffix, Group},
                {rmq_extra_tcp_ports, [tcp_port_mqtt_extra,
                                       tcp_port_mqtt_tls_extra]}
               ]),
    Mod = list_to_atom("rabbit_mqtt_retained_msg_store_" ++ atom_to_list(Group)),
    Env = [{rabbitmq_mqtt, [{retained_message_store, Mod}]},
           {rabbit, [
                     {default_user, "guest"},
                     {default_pass, "guest"},
                     {default_vhost, "/"},
                     {default_permissions, [".*", ".*", ".*"]}
                    ]}],
    rabbit_ct_helpers:run_setup_steps(
      Config,
      [fun(Conf) -> rabbit_ct_helpers:merge_app_env(Conf, Env) end] ++
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).


%% -------------------------------------------------------------------
%% Testsuite cases
%% -------------------------------------------------------------------

coerce_configuration_data(Config) ->
    C = connect(<<"simpleClientRetainer">>, Config, [{ack_timeout, 1}]),

    {ok, _, _} = emqtt:subscribe(C, <<"TopicA">>, qos0),
    ok = emqtt:publish(C, <<"TopicA">>, <<"Payload">>),
    ok = expect_publishes(C, <<"TopicA">>, [<<"Payload">>]),

    ok = emqtt:disconnect(C).

%% -------------------------------------------------------------------
%% When a client is subscribed to TopicA/Device.Field and another
%% client publishes to TopicA/Device.Field the client should be
%% sent messages for the translated topic (TopicA/Device/Field)
%% -------------------------------------------------------------------
should_translate_amqp2mqtt_on_publish(Config) ->
    C = connect(<<"simpleClientRetainer">>, Config, [{ack_timeout, 1}]),
    %% there's an active consumer
    {ok, _, _} = emqtt:subscribe(C, <<"TopicA/Device.Field">>, qos1),
    ok = emqtt:publish(C, <<"TopicA/Device.Field">>, #{},  <<"Payload">>, [{retain, true}]),
    ok = expect_publishes(C, <<"TopicA/Device/Field">>, [<<"Payload">>]),
    ok = emqtt:disconnect(C).

%% -------------------------------------------------------------------
%% If a client publishes a retained message to TopicA/Device.Field and another
%% client subscribes to TopicA/Device.Field the client should be
%% sent the retained message for the translated topic (TopicA/Device/Field)
%% -------------------------------------------------------------------
should_translate_amqp2mqtt_on_retention(Config) ->
    C = connect(<<"simpleClientRetainer">>, Config, [{ack_timeout, 1}]),
    %% publish with retain = true before a consumer comes around
    ok = emqtt:publish(C, <<"TopicA/Device.Field">>, #{},  <<"Payload">>, [{retain, true}]),
    {ok, _, _} = emqtt:subscribe(C, <<"TopicA/Device.Field">>, qos1),
    ok = expect_publishes(C, <<"TopicA/Device/Field">>, [<<"Payload">>]),
    ok = emqtt:disconnect(C).

%% -------------------------------------------------------------------
%% If a client publishes a retained message to TopicA/Device.Field and another
%% client subscribes to TopicA/Device/Field the client should be
%% sent retained message for the translated topic (TopicA/Device/Field)
%% -------------------------------------------------------------------
should_translate_amqp2mqtt_on_retention_search(Config) ->
    C = connect(<<"simpleClientRetainer">>, Config, [{ack_timeout, 1}]),
    ok = emqtt:publish(C, <<"TopicA/Device.Field">>, #{},  <<"Payload">>, [{retain, true}]),
    {ok, _, _} = emqtt:subscribe(C, <<"TopicA/Device/Field">>, qos1),
    ok = expect_publishes(C, <<"TopicA/Device/Field">>, [<<"Payload">>]),
    ok = emqtt:disconnect(C).

does_not_retain(Config) ->
    C = connect(<<"simpleClientRetainer">>, Config, [{ack_timeout, 1}]),
    ok = emqtt:publish(C, <<"TopicA/Device.Field">>, #{},  <<"Payload">>, [{retain, true}]),
    {ok, _, _} = emqtt:subscribe(C, <<"TopicA/Device.Field">>, qos1),
    receive
        Unexpected ->
            ct:fail("Unexpected message: ~p", [Unexpected])
    after 1000 ->
              ok
    end,
    ok = emqtt:disconnect(C).
