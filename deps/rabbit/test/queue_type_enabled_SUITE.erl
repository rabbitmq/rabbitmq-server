%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(queue_type_enabled_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

all() ->
    [{group, cluster_size_1}].

groups() ->
    [{cluster_size_1, [], [
                           declare_allowed_when_type_enabled,
                           declare_refused_when_type_disabled
                          ]}].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(
                Config, [{rmq_nodes_count, 1},
                         {rmq_nodename_suffix, Group}]),
    rabbit_ct_helpers:run_steps(
      Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
          rabbit_ct_client_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
          rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    ok = set_stream_queues_enabled(Config, true),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

declare_allowed_when_type_enabled(Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    Q = <<"queue_type_enabled_SUITE.allowed">>,
    ?assertMatch(#'queue.declare_ok'{},
                 amqp_channel:call(Ch, declare(Q))),
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = Q}),
    rabbit_ct_client_helpers:close_channel(Ch).

declare_refused_when_type_disabled(Config) ->
    ok = set_stream_queues_enabled(Config, false),
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    Q = <<"queue_type_enabled_SUITE.refused">>,
    ?assertExit(
       {{shutdown, {connection_closing,
                    {server_initiated_close, 541, _}}}, _},
       amqp_channel:call(Ch, declare(Q))),
    QName = rabbit_misc:r(<<"/">>, queue, Q),
    ?assertEqual({error, not_found},
                 rabbit_ct_broker_helpers:rpc(
                   Config, 0, rabbit_amqqueue, lookup, [QName])).

%%----------------------------------------------------------------------------

declare(Q) ->
    #'queue.declare'{queue = Q,
                     durable = true,
                     arguments = [{<<"x-queue-type">>, longstr, <<"stream">>}]}.

set_stream_queues_enabled(Config, Value) ->
    rabbit_ct_broker_helpers:rpc(
      Config, 0, application, set_env, [rabbit, stream_queues_enabled, Value]).
