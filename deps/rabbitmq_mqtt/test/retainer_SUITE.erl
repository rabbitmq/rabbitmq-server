%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(retainer_SUITE).
-compile([export_all]).

-include_lib("common_test/include/ct.hrl").

all() ->
    [
        {group, non_parallel_tests}
    ].

groups() ->
    [
        {non_parallel_tests, [], [
            coerce_configuration_data,
            should_translate_amqp2mqtt_on_publish,
            should_translate_amqp2mqtt_on_retention,
            should_translate_amqp2mqtt_on_retention_search
        ]}
    ].

suite() ->
    [{timetrap, {seconds, 600}}].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE},
        {rmq_extra_tcp_ports, [tcp_port_mqtt_extra,
            tcp_port_mqtt_tls_extra]}
    ]),
    % see https://github.com/rabbitmq/rabbitmq-mqtt/issues/86
    RabbitConfig = {rabbit, [
        {default_user, "guest"},
        {default_pass, "guest"},
        {default_vhost, "/"},
        {default_permissions, [".*", ".*", ".*"]}
    ]},
    rabbit_ct_helpers:run_setup_steps(Config1,
        [ fun(Conf) -> merge_app_env(RabbitConfig, Conf) end ] ++
            rabbit_ct_broker_helpers:setup_steps() ++
            rabbit_ct_client_helpers:setup_steps()).

merge_app_env(MqttConfig, Config) ->
    rabbit_ct_helpers:merge_app_env(Config, MqttConfig).

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


%% -------------------------------------------------------------------
%% Testsuite cases
%% -------------------------------------------------------------------

coerce_configuration_data(Config) ->
    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt),
    C = connect(P),

    {ok, _, _} = emqtt:subscribe(C, <<"TopicA">>, qos0),
    ok = emqtt:publish(C, <<"TopicA">>, <<"Payload">>),
    expect_publishes(<<"TopicA">>, [<<"Payload">>]),

    ok = emqtt:disconnect(C).

%% -------------------------------------------------------------------
%% When a client is subscribed to TopicA/Device.Field and another
%% client publishes to TopicA/Device.Field the client should be
%% sent messages for the translated topic (TopicA/Device/Field)
%% -------------------------------------------------------------------
should_translate_amqp2mqtt_on_publish(Config) ->
    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt),
    C = connect(P),
    %% there's an active consumer
    {ok, _, _} = emqtt:subscribe(C, <<"TopicA/Device.Field">>, qos1),
    ok = emqtt:publish(C, <<"TopicA/Device.Field">>, #{},  <<"Payload">>, [{retain, true}]),
    expect_publishes(<<"TopicA/Device/Field">>, [<<"Payload">>]),
    ok = emqtt:disconnect(C).

%% -------------------------------------------------------------------
%% If a client publishes a retained message to TopicA/Device.Field and another
%% client subscribes to TopicA/Device.Field the client should be
%% sent the retained message for the translated topic (TopicA/Device/Field)
%% -------------------------------------------------------------------
should_translate_amqp2mqtt_on_retention(Config) ->
    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt),
    C = connect(P),
    %% publish with retain = true before a consumer comes around
    ok = emqtt:publish(C, <<"TopicA/Device.Field">>, #{},  <<"Payload">>, [{retain, true}]),
    {ok, _, _} = emqtt:subscribe(C, <<"TopicA/Device.Field">>, qos1),
    expect_publishes(<<"TopicA/Device/Field">>, [<<"Payload">>]),
    ok = emqtt:disconnect(C).

%% -------------------------------------------------------------------
%% If a client publishes a retained message to TopicA/Device.Field and another
%% client subscribes to TopicA/Device/Field the client should be
%% sent retained message for the translated topic (TopicA/Device/Field)
%% -------------------------------------------------------------------
should_translate_amqp2mqtt_on_retention_search(Config) ->
    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt),
    C = connect(P),
    ok = emqtt:publish(C, <<"TopicA/Device.Field">>, #{},  <<"Payload">>, [{retain, true}]),
    {ok, _, _} = emqtt:subscribe(C, <<"TopicA/Device/Field">>, qos1),
    expect_publishes(<<"TopicA/Device/Field">>, [<<"Payload">>]),
    ok = emqtt:disconnect(C).

connect(Port) ->
    {ok, C} = emqtt:start_link(
                [{host, "localhost"},
                 {port, Port},
                 {clientid, <<"simpleClientRetainer">>},
                 {proto_ver, v4},
                 {ack_timeout, 1}]),
    {ok, _Properties} = emqtt:connect(C),
    C.

expect_publishes(_Topic, []) -> ok;
expect_publishes(Topic, [Payload|Rest]) ->
    receive
        {publish, #{topic := Topic,
                    payload := Payload}} ->
            expect_publishes(Topic, Rest)
    after 1500 ->
              throw({publish_not_delivered, Payload})
    end.
