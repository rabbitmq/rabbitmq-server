%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(message_size_limit_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(TIMEOUT_LIST_OPS_PASS, 5000).
-define(TIMEOUT, 30000).
-define(TIMEOUT_CHANNEL_EXCEPTION, 5000).

-define(CLEANUP_QUEUE_NAME, <<"cleanup-queue">>).

all() ->
    [
      {group, parallel_tests}
    ].

groups() ->
    [
      {parallel_tests, [parallel], [
          max_message_size
       ]}
    ].

suite() ->
    [
      {timetrap, {minutes, 3}}
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

max_message_size(Config) ->
    Binary2M  = gen_binary_mb(2),
    Binary4M  = gen_binary_mb(4),
    Binary6M  = gen_binary_mb(6),
    Binary10M = gen_binary_mb(10),

    Size2Mb = 1024 * 1024 * 2,
    Size2Mb = byte_size(Binary2M),

    rabbit_ct_broker_helpers:rpc(Config, 0,
                                 application, set_env, [rabbit, max_message_size, 1024 * 1024 * 3]),

    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),

    %% Binary is within the max size limit
    amqp_channel:call(Ch, #'basic.publish'{routing_key = <<"none">>}, #amqp_msg{payload = Binary2M}),
    %% The channel process is alive
    assert_channel_alive(Ch),

    Monitor = monitor(process, Ch),
    amqp_channel:call(Ch, #'basic.publish'{routing_key = <<"none">>}, #amqp_msg{payload = Binary4M}),
    assert_channel_fail_max_size(Ch, Monitor),

    %% increase the limit
    rabbit_ct_broker_helpers:rpc(Config, 0,
                                 application, set_env, [rabbit, max_message_size, 1024 * 1024 * 8]),

    {_, Ch1} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),

    amqp_channel:call(Ch1, #'basic.publish'{routing_key = <<"nope">>}, #amqp_msg{payload = Binary2M}),
    assert_channel_alive(Ch1),

    amqp_channel:call(Ch1, #'basic.publish'{routing_key = <<"nope">>}, #amqp_msg{payload = Binary4M}),
    assert_channel_alive(Ch1),

    amqp_channel:call(Ch1, #'basic.publish'{routing_key = <<"nope">>}, #amqp_msg{payload = Binary6M}),
    assert_channel_alive(Ch1),

    Monitor1 = monitor(process, Ch1),
    amqp_channel:call(Ch1, #'basic.publish'{routing_key = <<"none">>}, #amqp_msg{payload = Binary10M}),
    assert_channel_fail_max_size(Ch1, Monitor1),

    %% increase beyond the hard limit
    rabbit_ct_broker_helpers:rpc(Config, 0,
                                 application, set_env, [rabbit, max_message_size, 1024 * 1024 * 600]),
    Val = rabbit_ct_broker_helpers:rpc(Config, 0,
                                       rabbit_channel, get_max_message_size, []),

    ?assertEqual(?MAX_MSG_SIZE, Val).

%% -------------------------------------------------------------------
%% Implementation
%% -------------------------------------------------------------------

gen_binary_mb(N) ->
    B1M = << <<"_">> || _ <- lists:seq(1, 1024 * 1024) >>,
    << B1M || _ <- lists:seq(1, N) >>.

assert_channel_alive(Ch) ->
    amqp_channel:call(Ch, #'basic.publish'{routing_key = <<"nope">>},
                          #amqp_msg{payload = <<"HI">>}).

assert_channel_fail_max_size(Ch, Monitor) ->
    receive
        {'DOWN', Monitor, process, Ch,
            {shutdown,
                {server_initiated_close, 406, _Error}}} ->
            ok
    after ?TIMEOUT_CHANNEL_EXCEPTION ->
        error({channel_exception_expected, max_message_size})
    end.
