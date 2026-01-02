%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.

-module(disconnect_on_unauthorized_SUITE).

-compile([export_all,
          nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(RC_SUCCESS, 16#00).
-define(RC_FAILURE, 16#80).
-define(RC_NOT_AUTHORIZED, 16#87).

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

    Env = [{rabbitmq_mqtt, [{disconnect_on_unauthorized, false}]}],
    Config1 = rabbit_ct_helpers:merge_app_env(Config0, Env),
    Config2 = rabbit_ct_helpers:run_setup_steps(
                Config1,
                rabbit_ct_broker_helpers:setup_steps() ++
                rabbit_ct_client_helpers:setup_steps()),
    util:enable_plugin(Config2, rabbitmq_mqtt),
    User = <<"mqtt-user">>,
    Password = <<"mqtt-password">>,
    ok = rabbit_ct_broker_helpers:add_user(Config2, User, Password),
    Config = rabbit_ct_helpers:set_config(Config2, [{mqtt_user, User}, {mqtt_pass, Password}]),
    rabbit_ct_broker_helpers:set_permissions(Config, User, <<"/">>, <<".*">>, <<".*">>, <<".*">>),
    ok = rabbit_ct_broker_helpers:rpc(
           Config, rabbit_auth_backend_internal, set_topic_permissions,
           [?config(mqtt_user, Config), <<"/">>,
            <<"amq.topic">>, <<"^topic1$">>, <<"^topic1$">>, <<"acting-user">>]),
    Config.

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(Group, Config) ->
    rabbit_ct_helpers:set_config(Config, {mqtt_version, Group}).

end_per_group(_Group, Config) ->
    Config.

%%====================================================================
%% Test cases
%%====================================================================

publish_unauthorized_no_disconnect(Config) ->
    C = util:connect(<<"pub_client">>,
                     Config,
                     [{username, ?config(mqtt_user, Config)},
                      {password, ?config(mqtt_pass, Config)}]),

    {ok, PublishReply} = emqtt:publish(C, <<"topic2">>, <<"payload">>, [{qos, 1}]), 
    case ?config(mqtt_version, Config) of
        v5 ->
            ?assertMatch(#{reason_code := ?RC_NOT_AUTHORIZED}, PublishReply);
        v4 ->
            ok
    end,

    ok = emqtt:publish(C, <<"topic3">>, <<"payload">>, [{qos, 0}]),

    timer:sleep(100),
    %% Client should be still connected.
    ?assert(is_process_alive(C)),

    ok = emqtt:disconnect(C).

subscribe_unauthorized_no_disconnect(Config) ->
    C = util:connect(<<"sub_client 1">>,
                     Config,
                     [{username, ?config(mqtt_user, Config)},
                      {password, ?config(mqtt_pass, Config)}]),

    {ok, _, [ReasonCode]} = emqtt:subscribe(C, {<<"topic2">>, qos0}),

    case ?config(mqtt_version, Config) of
        v5 ->
            ?assertEqual(?RC_NOT_AUTHORIZED, ReasonCode);
        v4 ->
            ?assertEqual(?RC_FAILURE, ReasonCode)
    end,

    timer:sleep(100),
    %% Client should be still connected.
    ?assert(is_process_alive(C)),

    ok = emqtt:disconnect(C).

unsubscribe_unauthorized_no_disconnect(Config) ->
    C = util:connect(<<"sub_client 2">>,
                     Config,
                     [{username, ?config(mqtt_user, Config)},
                      {password, ?config(mqtt_pass, Config)}]),

    {ok, _, [0]} = emqtt:subscribe(C, {<<"topic1">>, qos0}),

    ok = rabbit_ct_broker_helpers:rpc(
           Config, rabbit_auth_backend_internal, set_topic_permissions,
           [?config(mqtt_user, Config), <<"/">>,
            <<"amq.topic">>, <<"^topic9$">>, <<"^topic9$">>, <<"acting-user">>]),

    %% Let the MQTT connection process on the server hibernate which will cause
    %% its topic permission cache to be erased.
    timer:sleep(7000),

    {ok, _, ReasonCodes0} = emqtt:unsubscribe(C, <<"topic1">>),

    timer:sleep(100),
    %% Client should be still connected.
    ?assert(is_process_alive(C)),

    ok = rabbit_ct_broker_helpers:rpc(
           Config, rabbit_auth_backend_internal, set_topic_permissions,
           [?config(mqtt_user, Config), <<"/">>,
            <<"amq.topic">>, <<"^topic1$">>, <<"^topic1$">>, <<"acting-user">>]),

    {ok, _, ReasonCodes1} = emqtt:unsubscribe(C, <<"topic1">>),
    case ?config(mqtt_version, Config) of
        v5 ->
            ?assertEqual([?RC_NOT_AUTHORIZED], ReasonCodes0),
            ?assertEqual([?RC_SUCCESS], ReasonCodes1);
        v4 ->
            ok
    end,
    ok = emqtt:disconnect(C).
