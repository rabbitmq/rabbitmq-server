%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(federation_SUITE).
-compile([export_all,
          nowarn_export_all]).

-include_lib("eunit/include/eunit.hrl").

-import(rabbit_ct_helpers,
        [eventually/3]).

all() ->
    [exchange_federation].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(
                Config,
                [{rmq_nodename_suffix, ?MODULE},
                 {rmq_nodes_count, 2},
                 {rmq_nodes_clustered, false},
                 {start_rmq_with_plugins_disabled, true}
                ]),
    Config2 = rabbit_ct_helpers:run_setup_steps(
                Config1,
                rabbit_ct_broker_helpers:setup_steps() ++
                    rabbit_ct_client_helpers:setup_steps()),
    util:enable_plugin(Config2, rabbitmq_mqtt),
    util:enable_plugin(Config2, rabbitmq_federation),
    Config2.

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    case rabbit_ct_broker_helpers:enable_feature_flag(Config, 'rabbitmq_4.1.0') of
        ok ->
            rabbit_ct_helpers:testcase_started(Config, Testcase);
        Skip = {skip, _Reason} ->
            Skip
    end.

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% Test that exchange federation works for MQTT clients.
%% https://github.com/rabbitmq/rabbitmq-server/issues/13040
exchange_federation(Config) ->
    Upstream = 0,
    Downstream = 1,
    ok = rabbit_ct_broker_helpers:set_parameter(
           Config, Downstream, <<"federation-upstream">>, <<"origin">>,
           [
            {<<"uri">>, rabbit_ct_broker_helpers:node_uri(Config, Upstream)}
           ]),
    ok = rabbit_ct_broker_helpers:set_policy(
           Config, Downstream, <<"my policy">>, <<"^amq\.topic$">>, <<"exchanges">>,
           [
            {<<"federation-upstream-set">>, <<"all">>}
           ]),

    %% Subscribe on the downstream.
    SubV4 = util:connect(<<"v4 client">>, Config, Downstream, [{proto_ver, v4}]),
    SubV5 = util:connect(<<"v5 client">>, Config, Downstream, [{proto_ver, v5}]),
    {ok, _, [1]} = emqtt:subscribe(SubV4, <<"vsn/4">>, qos1),
    {ok, _, [1]} = emqtt:subscribe(SubV5, #{'Subscription-Identifier' => 500}, <<"vsn/5">>, qos1),

    %% "The bindings are replicated with the upstream asynchronously so the effect of
    %% adding or removing a binding is only guaranteed to be seen eventually."
    %% https://www.rabbitmq.com/docs/federated-exchanges#details
    eventually(
      ?_assertMatch(
         [_V4, _V5],
         rabbit_ct_broker_helpers:rpc(
           Config, Upstream, rabbit_binding, list_for_source,
           [rabbit_misc:r(<<"/">>, exchange, <<"amq.topic">>)])),
      1000, 10),

    %% Publish on the upstream.
    Pub = util:connect(<<"v3 client">>, Config, Upstream, [{proto_ver, v3}]),
    {ok, #{reason_code_name := success}} = emqtt:publish(Pub, <<"vsn/4">>, <<"m1">>, qos1),
    {ok, #{reason_code_name := success}} = emqtt:publish(Pub, <<"vsn/5">>, <<"m2">>, qos1),

    receive {publish, #{client_pid := SubV4,
                        qos := 1,
                        topic := <<"vsn/4">>,
                        payload := <<"m1">>}} -> ok
    after 10_000 -> ct:fail({missing_publish, ?LINE})
    end,
    receive {publish, #{client_pid := SubV5,
                        qos := 1,
                        topic := <<"vsn/5">>,
                        payload := <<"m2">>,
                        properties := #{'Subscription-Identifier' := 500}}} -> ok
    after 10_000 -> ct:fail({missing_publish, ?LINE})
    end,

    ok = emqtt:disconnect(SubV4),
    ok = emqtt:disconnect(SubV5),
    ok = emqtt:disconnect(Pub).
