%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(amqp_connection_uniqueness_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").
-include_lib("amqp10_common/include/amqp10_sole_conn.hrl").

-compile([nowarn_export_all,
          export_all]).

-import(amqp_utils,
        [connection_config/1,
         close_connection_sync/1]).
-import(rabbit_amqp_util,
        [has_capability/2]).

all() ->
    [
      {group, cluster_size_1}
    ].

groups() ->
    [
     {cluster_size_1, [shuffle],
      [
       no_sole_conn_capability,
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

no_sole_conn_capability(Config) ->
    OpnConf0 = connection_config(Config),
    OpnConf1 = OpnConf0#{
                 container_id => <<"my-container-1">>,
                 notify_with_performative => true
                },
    {ok, Connection1} = amqp10_client:open_connection(OpnConf1),
    receive {amqp10_event, {connection, Connection1,
                            {opened, #'v1_0.open'{
                                        offered_capabilities = OffCaps1,
                                        properties = Props1}}}} ->
                ?assertNot(has_capability(?CAP_SOLE_CONN, OffCaps1)),
                ?assertNot(has_field(?SOLE_CONN_DETECTION_POLICY, Props1))
    after 30000 -> ct:fail(opened_timeout)
    end,
    {ok, Connection2} = amqp10_client:open_connection(OpnConf1),
    receive {amqp10_event, {connection, Connection2,
                            {opened, #'v1_0.open'{
                                        offered_capabilities = OffCaps2,
                                        properties = Props2}}}} ->
                ?assertNot(has_capability(?CAP_SOLE_CONN, OffCaps2)),
                ?assertNot(has_field(?SOLE_CONN_DETECTION_POLICY, Props2))
    after 30000 -> ct:fail(opened_timeout)
    end,

    ok = close_connection_sync(Connection2),
    ok = close_connection_sync(Connection1).

sole_conn_no_conflict(Config) ->
    OpnConf0 = connection_config(Config),
    OpnConf1 = OpnConf0#{
                 container_id => <<"my-container-1">>,
                 desired_capabilities => [?CAP_SOLE_CONN],
                 notify_with_performative => true
                },
    {ok, Connection1} = amqp10_client:open_connection(OpnConf1),
    receive {amqp10_event, {connection, Connection1,
                            {opened, #'v1_0.open'{
                                        offered_capabilities = OffCaps1,
                                        properties = Props1}}}} ->
                assert_has_sole_cap(OffCaps1),
                assert_has_weak_policy(Props1)
    after 30000 -> ct:fail(opened_timeout)
    end,
    OpnConf2 = OpnConf1#{
                 container_id => <<"my-container-2">>
                },
    {ok, Connection2} = amqp10_client:open_connection(OpnConf2),
    receive {amqp10_event, {connection, Connection2,
                            {opened, #'v1_0.open'{
                                        offered_capabilities = OffCaps2,
                                        properties = Props2}}}} ->
                assert_has_sole_cap(OffCaps2),
                assert_has_weak_policy(Props2)
    after 30000 -> ct:fail(opened_timeout)
    end,

    ok = close_connection_sync(Connection2),
    ok = close_connection_sync(Connection1).

sole_conn_conflict_should_refuse_second_connection(Config) ->
    OpnConf0 = connection_config(Config),
    OpnConf1 = OpnConf0#{
                 container_id => <<"my-container-1">>,
                 desired_capabilities => [?CAP_SOLE_CONN],
                 notify_with_performative => true
                },
    {ok, Connection1} = amqp10_client:open_connection(OpnConf1),
   receive {amqp10_event, {connection, Connection1,
                            {opened, #'v1_0.open'{
                                        offered_capabilities = OffCaps1,
                                        properties = Props1}}}} ->
                assert_has_sole_cap(OffCaps1),
                assert_has_weak_policy(Props1)
    after 30000 -> ct:fail(opened_timeout)
    end,
    OpnConf2 = OpnConf1#{
                 container_id => <<"my-container-1">>
                },
    {ok, Connection2} = amqp10_client:open_connection(OpnConf2),
    receive {amqp10_event, {connection, Connection2,
                            {opened, #'v1_0.open'{
                                        offered_capabilities = OffCaps2,
                                        properties = Props2}}}} ->
                assert_has_sole_cap(OffCaps2),
                assert_has_weak_policy(Props2)
    after 30000 -> ct:fail(opened_timeout)
    end,
    receive {amqp10_event, {connection, Connection2,
                            {closed, #'v1_0.close'{error = Error}}}} ->
                #'v1_0.error'{condition = Cond,
                              description = Desc,
                              info = {map, Info}
                             } = Error,
                ?assertEqual(?V_1_0_AMQP_ERROR_INVALID_FIELD, Cond),
                ?assertEqual({utf8,
                              <<"The container-id is already bound to an active exclusive connection.">>},
                             Desc),
                ?assert(lists:member({?V_1_0_AMQP_ERROR_INVALID_FIELD, {symbol, <<"container-id">>}},
                                     Info))
    after 30000 -> ct:fail(closed_timeout)
    end,

    ok = close_connection_sync(Connection1).

%% ------------------------------------------------------------------
%% Internal Helpers
%% ------------------------------------------------------------------

assert_has_sole_cap(Caps) ->
    ?assert(has_capability(?CAP_SOLE_CONN, Caps)).

assert_has_weak_policy({map, Props}) ->
    ExpectedPair = {?SOLE_CONN_DETECTION_POLICY, ?SOLE_CONN_DETECTION_POLICY_WEAK},
    ?assert(lists:member(ExpectedPair, Props)).

has_field(Field, {map, Props}) ->
    case lists:keyfind(Field, 1, Props) of
        false ->
            false;
        _ ->
            true
    end.
