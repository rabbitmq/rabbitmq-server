%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(amqp_connection_uniqueness_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-compile([nowarn_export_all,
          export_all]).

-import(amqp_utils,
        [connection_config/1,
         close_connection_sync/1]).

all() ->
    [
      {group, cluster_size_1}
    ].

groups() ->
    [
     {cluster_size_1, [shuffle],
      [
       sole_conn_no_conflict,
       sole_conn_conflict_should_refuse_second_connection
      ]}
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(amqp10_client),
    rabbit_ct_helpers:log_environment(),
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(cluster_size_1, Config) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(
                Config, [{rmq_nodes_count, 1},
                         {rmq_nodename_suffix, Suffix}]),
    rabbit_ct_helpers:run_setup_steps(
      Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

sole_conn_no_conflict(Config) ->
    OpnConf0 = connection_config(Config),
    OpnConf1 = OpnConf0#{
                 container_id => <<"my-container-1">>,
                 desired_capabilities => [<<"sole-connection-for-container">>]
                },
    {ok, Connection1} = amqp10_client:open_connection(OpnConf1),
    receive {amqp10_event, {connection, Connection1, opened}} -> ok
    after 30000 -> ct:fail(opened_timeout)
    end,
    OpnConf2 = OpnConf1#{
                 container_id => <<"my-container-2">>
                },
    {ok, Connection2} = amqp10_client:open_connection(OpnConf2),
    receive {amqp10_event, {connection, Connection2, opened}} -> ok
    after 30000 -> ct:fail(opened_timeout)
    end,

    ok = close_connection_sync(Connection2),
    ok = close_connection_sync(Connection1).

sole_conn_conflict_should_refuse_second_connection(Config) ->
    OpnConf0 = connection_config(Config),
    OpnConf1 = OpnConf0#{
                 container_id => <<"my-container-1">>,
                 desired_capabilities => [<<"sole-connection-for-container">>]
                },
    {ok, Connection1} = amqp10_client:open_connection(OpnConf1),
    receive {amqp10_event, {connection, Connection1, opened}} -> ok
    after 30000 -> ct:fail(opened_timeout)
    end,
    OpnConf2 = OpnConf1#{
                 container_id => <<"my-container-1">>
                },
    {ok, Connection2} = amqp10_client:open_connection(OpnConf2),
    receive {amqp10_event, {connection, Connection2, opened}} -> ok
    after 30000 -> ct:fail(opened_timeout)
    end,
    receive {amqp10_event, {connection, Connection2, {closed, Why}}} ->
                ?assertMatch({invalid_field, _}, Why)
    after 30000 -> ct:fail(closed_timeout)
    end,


    ok = close_connection_sync(Connection1).
