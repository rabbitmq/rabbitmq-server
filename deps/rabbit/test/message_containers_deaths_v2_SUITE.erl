%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

%% This SUITE should be deleted when feature flag message_containers_deaths_v2 becomes required.
-module(message_containers_deaths_v2_SUITE).

-define(FEATURE_FLAG, message_containers_deaths_v2).

-compile([export_all, nowarn_export_all]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

all() ->
    [
     {group, cluster_size_1}
    ].

groups() ->
    [
     {cluster_size_1, [], [enable_feature_flag]}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config, []).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(_Group, Config0) ->
    Config = rabbit_ct_helpers:merge_app_env(
               Config0, {rabbit, [{forced_feature_flags_on_init, []}]}),
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_broker_helpers:setup_steps() ++
                                rabbit_ct_client_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_client_helpers:teardown_steps() ++
                                rabbit_ct_broker_helpers:teardown_steps()).

enable_feature_flag(Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    Q1 = <<"q1">>,
    Q2 = <<"q2">>,
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{queue = Q1,
                                                   arguments = [{<<"x-dead-letter-exchange">>, longstr, <<>>},
                                                                {<<"x-dead-letter-routing-key">>, longstr, Q2},
                                                                {<<"x-message-ttl">>, long, 3}]}),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{queue = Q2,
                                                   arguments = [{<<"x-dead-letter-exchange">>, longstr, <<>>},
                                                                {<<"x-dead-letter-routing-key">>, longstr, Q1}]}),
    P1 = <<"payload 1">>,
    P2 = <<"payload 2">>,
    amqp_channel:call(Ch,
                      #'basic.publish'{routing_key = Q1},
                      #amqp_msg{payload = P1}),
    ?assertNot(rabbit_ct_broker_helpers:is_feature_flag_enabled(Config, ?FEATURE_FLAG)),
    ?assertEqual(ok, rabbit_ct_broker_helpers:enable_feature_flag(Config, ?FEATURE_FLAG)),
    amqp_channel:call(Ch,
                      #'basic.publish'{routing_key = Q1},
                      #amqp_msg{payload = P2}),

    %% We now have 2 messages in Q2 with different mc annotations:
    %% * deaths for v1 in the 1st msg
    %% * deaths_v2 for v2 in the 2nd msg

    reject(Ch, Q2, P1),
    reject(Ch, Q2, P2),
    reject(Ch, Q2, P1),
    reject(Ch, Q2, P2),

    {#'basic.get_ok'{}, #amqp_msg{props = #'P_basic'{headers = H1}}} =
    ?awaitMatch({#'basic.get_ok'{},
                 #amqp_msg{payload = P1}},
                amqp_channel:call(Ch, #'basic.get'{queue = Q2}),
                5000),

    {#'basic.get_ok'{}, #amqp_msg{props = #'P_basic'{headers = H2}}} =
    ?awaitMatch({#'basic.get_ok'{},
                 #amqp_msg{payload = P2}},
                amqp_channel:call(Ch, #'basic.get'{queue = Q2}),
                5000),

    lists:foreach(
      fun(Headers) ->
              ?assertEqual({longstr, <<"expired">>}, rabbit_misc:table_lookup(Headers, <<"x-first-death-reason">>)),
              ?assertEqual({longstr, Q1}, rabbit_misc:table_lookup(Headers, <<"x-first-death-queue">>)),
              ?assertEqual({longstr, <<>>}, rabbit_misc:table_lookup(Headers, <<"x-first-death-exchange">>)),
              ?assertEqual({longstr, <<"expired">>}, rabbit_misc:table_lookup(Headers, <<"x-last-death-reason">>)),
              ?assertEqual({longstr, Q1}, rabbit_misc:table_lookup(Headers, <<"x-last-death-queue">>)),
              ?assertEqual({longstr, <<>>}, rabbit_misc:table_lookup(Headers, <<"x-last-death-exchange">>)),

              {array, [{table, Death1},
                       {table, Death2}]} = rabbit_misc:table_lookup(H1, <<"x-death">>),

              ?assertEqual({longstr, Q1}, rabbit_misc:table_lookup(Death1, <<"queue">>)),
              ?assertEqual({longstr, <<"expired">>}, rabbit_misc:table_lookup(Death1, <<"reason">>)),
              ?assertMatch({timestamp, _}, rabbit_misc:table_lookup(Death1, <<"time">>)),
              ?assertEqual({longstr, <<>>}, rabbit_misc:table_lookup(Death1, <<"exchange">>)),
              ?assertEqual({long, 3}, rabbit_misc:table_lookup(Death1, <<"count">>)),
              ?assertEqual({array, [{longstr, Q1}]}, rabbit_misc:table_lookup(Death1, <<"routing-keys">>)),

              ?assertEqual({longstr, Q2}, rabbit_misc:table_lookup(Death2, <<"queue">>)),
              ?assertEqual({longstr, <<"rejected">>}, rabbit_misc:table_lookup(Death2, <<"reason">>)),
              ?assertMatch({timestamp, _}, rabbit_misc:table_lookup(Death2, <<"time">>)),
              ?assertEqual({longstr, <<>>}, rabbit_misc:table_lookup(Death2, <<"exchange">>)),
              ?assertEqual({long, 2}, rabbit_misc:table_lookup(Death2, <<"count">>)),
              ?assertEqual({array, [{longstr, Q2}]}, rabbit_misc:table_lookup(Death2, <<"routing-keys">>))
      end, [H1, H2]),
    ok.

reject(Ch, Queue, Payload) ->
    {#'basic.get_ok'{delivery_tag = DTag}, #amqp_msg{}} =
    ?awaitMatch({#'basic.get_ok'{},
                 #amqp_msg{payload = Payload}},
                amqp_channel:call(Ch, #'basic.get'{queue = Queue}),
                5000),
    amqp_channel:cast(Ch, #'basic.reject'{delivery_tag = DTag,
                                          requeue = false}).
