%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.


-module(disconnect_on_unauthorized_SUITE).

-compile([export_all,
    nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("rabbit_mqtt.hrl").

-define(RC_Failure, 16#80).
-define(RC_NOT_AUTHORIZED, 16#87).
-define(RC_GRANTED_QOS_0, 16#0).

all() ->
    [
        {group, v4},
        {group, v5}
    ].

groups() ->
    [
        {v4, [], test_cases()},
        {v5, [], test_cases()}
    ].

test_cases() ->
    [
        publish_unauthorized_no_disconnect,
        subscribe_unauthorized_no_disconnect,
        unsubscribe_unauthorized_no_disconnect
    ].

init_per_suite(Config0) ->
    rabbit_ct_helpers:log_environment(),

    User = <<"mqtt-user">>,
    Password = <<"mqtt-password">>,

    Env = [{rabbitmq_mqtt,
        [{disconnect_on_unauthorized, false}]}
    ],
    Config = rabbit_ct_helpers:merge_app_env(Config0, Env),

    Config1 = rabbit_ct_helpers:run_setup_steps(
        Config,
        rabbit_ct_broker_helpers:setup_steps() ++
        rabbit_ct_client_helpers:setup_steps()),
    util:enable_plugin(Config1, rabbitmq_mqtt),

    rabbit_ct_broker_helpers:add_user(Config1, User, Password),

    Config2 = rabbit_ct_helpers:set_config(Config1, [{mqtt_user, User}, {mqtt_pass, Password}]),

    rabbit_ct_broker_helpers:set_permissions(Config2, User, <<"/">>, <<".*">>, <<".*">>, <<".*">>),
    ok = rabbit_ct_broker_helpers:rpc(Config2, 0,
        rabbit_auth_backend_internal, set_topic_permissions,
        [?config(mqtt_user, Config2), <<"/">>,
            <<"amq.topic">>, <<"^topic1$">>, <<"^topic1$">>, <<"acting-user">>]),

    Config2.

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(
        Config,
        rabbit_ct_client_helpers:teardown_steps() ++
        rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, {mqtt_version, Group}),
    Config1.

end_per_group(_Group, Config) ->
    Config.

%%====================================================================
%% Test cases
%%====================================================================

publish_unauthorized_no_disconnect(Config) ->
    C = util:connect(
        <<"pub_client">>,
        Config,
        [{username, ?config(mqtt_user, Config)},
            {password, ?config(mqtt_pass, Config)}]),

    case ?config(mqtt_version, Config) of
        v5 ->
            {ok, #{reason_code := ?RC_NOT_AUTHORIZED}} =
                emqtt:publish(
                    C,
                    <<"topic2">>,
                    <<"payload">>,
                    [{qos, 1}]
                );
        v4 ->
            {ok, _} =
                emqtt:publish(
                    C,
                    <<"topic2">>,
                    <<"payload">>,
                    [{qos, 1}]
                )
    end,

    timer:sleep(300),
    %% Client still connected
    ?assert(is_process_alive(C)),

    ok = emqtt:disconnect(C).

subscribe_unauthorized_no_disconnect(Config) ->
    C = util:connect(
        <<"sub_client">>,
        Config,
        [{username, ?config(mqtt_user, Config)},
            {password, ?config(mqtt_pass, Config)}]),

    {ok, _, [ReasonCode]} =
        emqtt:subscribe(
            C,
            {<<"topic2">>, qos0}
        ),

    case ?config(mqtt_version, Config) of
        v5 ->
            ?assertEqual(?RC_NOT_AUTHORIZED, ReasonCode);
        v4 ->
            ?assertEqual(?RC_Failure, ReasonCode)
    end,

    timer:sleep(300),
    %% Client still connected
    ?assert(is_process_alive(C)),

    ok = emqtt:disconnect(C).

unsubscribe_unauthorized_no_disconnect(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
        rabbit_auth_backend_internal, set_topic_permissions,
        [?config(mqtt_user, Config), <<"/">>,
            <<"amq.topic">>, <<"^(topic1|topic3)$">>, <<"^(topic1|topic3)$">>, <<"acting-user">>]),

    C = util:connect(
        <<"sub_client">>,
        Config,
        [{username, ?config(mqtt_user, Config)},
            {password, ?config(mqtt_pass, Config)}]),

    {ok, _, _} =
        emqtt:subscribe(
            C,
            {<<"topic3">>, qos0}
        ),

    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
        rabbit_auth_backend_internal, set_topic_permissions,
        [?config(mqtt_user, Config), <<"/">>,
            <<"amq.topic">>, <<"^topic1$">>, <<"^topic1$">>, <<"acting-user">>]),

    timer:sleep(1000),

    case ?config(mqtt_version, Config) of
        v5 ->
            {ok, _, [?RC_NOT_AUTHORIZED]} =
                emqtt:unsubscribe(
                    C,
                    <<"topic3">>
                );
        v4 ->
            {ok, _, _} =
                emqtt:unsubscribe(
                    C,
                    <<"topic3">>
                )
    end,

    timer:sleep(300),
    %% Client still connected
    ?assert(is_process_alive(C)),

    ok = emqtt:disconnect(C).
