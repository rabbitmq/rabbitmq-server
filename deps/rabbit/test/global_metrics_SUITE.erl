%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(global_metrics_SUITE).

-compile([export_all, nowarn_export_all]).
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
     {group, tests}
    ].

groups() ->
    [
     {tests, [], [
                  message_size,
                  over_max_message_size
                 ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
                {rmq_nodename_suffix, Group},
                {rmq_nodes_count, 1}
              ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
              rabbit_ct_client_helpers:teardown_steps() ++
              rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Test cases
%% -------------------------------------------------------------------

message_size(Config) ->
    Binary2B = <<"12">>,
    Binary2M  = binary:copy(<<"x">>, 2 * 1024 * 1024),

    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),

    Before = get_msg_size_metrics(Config),

    [amqp_channel:call(Ch, #'basic.publish'{routing_key = <<"none">>}, #amqp_msg{payload = Payload})
     || Payload <- [Binary2B, Binary2M, Binary2M]],

    After = get_msg_size_metrics(Config),

    ?assertEqual(#{message_size_64B => 1, message_size_4MiB => 2},
                 rabbit_msg_size_metrics:changed_buckets(After, Before)).

over_max_message_size(Config) ->
    Binary4M  = binary:copy(<<"x">>, 4 * 1024 * 1024),

    ok = rabbit_ct_broker_helpers:rpc(Config, persistent_term, put, [max_message_size, 4 * 1024 * 1024]),

    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),

    Before = get_msg_size_metrics(Config),

    amqp_channel:call(Ch, #'basic.publish'{routing_key = <<"none">>}, #amqp_msg{payload = Binary4M}),

    After = get_msg_size_metrics(Config),

    %% No metrics are bumped if over max message size
    ?assertEqual(Before, After).

%% -------------------------------------------------------------------
%% Implementation
%% -------------------------------------------------------------------

get_msg_size_metrics(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, rabbit_msg_size_metrics, overview, [[{protocol, amqp091}]]).
