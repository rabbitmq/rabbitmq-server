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
        [connection_config/2,
         close_connection_sync/1]).
-import(rabbit_amqp_util,
        [has_capability/2]).

-import(rabbit_ct_broker_helpers,
        [enable_feature_flag/2,
         rpc_all/4]).

all() ->
    [
      {group, cluster_size_1},
      {group, cluster_size_3}
    ].

groups() ->
    [
     {cluster_size_1, [shuffle], all_tests()},
     {cluster_size_3, [shuffle], all_tests()}
    ].

all_tests() ->
    [
     no_sole_conn_capability_offered_if_not_desired,
     no_sole_conn_enforcement_when_feature_flag_disabled,
     refuse_connection_no_conflict,
     refuse_connection_no_conflict_across_vhosts,
     refuse_connection_conflict_should_refuse_new_connection,
     refuse_connection_conflict_different_user_should_be_refused,
     refuse_connection_let_new_through_if_previous_closed,
     close_existing_conflict_should_close_existing_connection,
     close_existing_conflict_different_user_should_be_refused
    ].

init_per_group(Group, Config) ->
    {ok, _} = application:ensure_all_started(amqp10_client),
    rabbit_ct_helpers:log_environment(),
    Nodes = case Group of
                cluster_size_3 -> 3;
                _ -> 1
            end,
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(
                Config, [{rmq_nodes_count, Nodes},
                         {rmq_nodename_suffix, Suffix}]),
    Config2 = rabbit_ct_helpers:run_setup_steps(
                Config1,
                rabbit_ct_broker_helpers:setup_steps() ++
                rabbit_ct_client_helpers:setup_steps()),
    case Nodes > 1 of
        true ->
            case enable_feature_flag(Config2, 'rabbitmq_4.4.0') of
                ok ->
                    Config2;
                _ ->
                    tear_down_cluster(Config2),
                    {skip, "sole conn feature not available cluster-wide"}
            end;
        _ ->
            Config2
    end.

end_per_group(_, Config) ->
    tear_down_cluster(Config).

tear_down_cluster(Config) ->
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

no_sole_conn_capability_offered_if_not_desired(Config) ->
    ContainerId = atom_to_binary(?FUNCTION_NAME),
    OpnConf0 = conn_config(first, Config),
    OpnConf1 = OpnConf0#{
                 container_id => ContainerId,
                 notify_with_performative => true
                },
    {ok, Connection1} = amqp10_client:open_connection(OpnConf1),
    receive {amqp10_event, {connection, Connection1,
                            {opened, #'v1_0.open'{
                                        offered_capabilities = OffCaps1,
                                        properties = Props1}}}} ->
                ?assertNot(has_capability(?CAP_SOLE_CONN, OffCaps1)),
                ?assertNot(has_field(?SOLE_CONN_DETECTION_POLICY, Props1))
    after 10000 -> ct:fail(opened_timeout)
    end,
    %% trying to connect with the same container ID
    OpnConfConn2 = maps:merge(OpnConf1, conn_config(other, Config)),
    {ok, Connection2} = amqp10_client:open_connection(OpnConfConn2),
    receive {amqp10_event, {connection, Connection2,
                            {opened, #'v1_0.open'{
                                        offered_capabilities = OffCaps2,
                                        properties = Props2}}}} ->
                ?assertNot(has_capability(?CAP_SOLE_CONN, OffCaps2)),
                ?assertNot(has_field(?SOLE_CONN_DETECTION_POLICY, Props2))
    after 10000 -> ct:fail(opened_timeout)
    end,

    ok = close_connection_sync(Connection2),
    ok = close_connection_sync(Connection1).

%% While the cluster is mid-upgrade (not all nodes on rabbitmq_4.4.0 yet), the
%% server must behave as if the client hadn't requested the capability at
%% all: don't offer it, don't enforce exclusivity.
no_sole_conn_enforcement_when_feature_flag_disabled(Config) ->
    %% Mocked cluster-wide (not just on the node the client happens to
    %% connect to): the real feature flag is a cluster-wide property too.
    Mod = rabbit_amqp_sole_conn,
    rabbit_ct_broker_helpers:setup_meck(Config, [?MODULE]),
    _ = rabbit_ct_broker_helpers:rpc_all(Config, meck, new, [Mod, [no_link, passthrough]]),
    _ = rabbit_ct_broker_helpers:rpc_all(Config, meck, expect,
                                        [Mod, is_feature_enabled, fun() -> false end]),

    ContainerId = atom_to_binary(?FUNCTION_NAME),
    OpnConf0 = conn_config(first, Config),
    OpnConf1 = OpnConf0#{
                 container_id => ContainerId,
                 desired_capabilities => [?CAP_SOLE_CONN],
                 notify_with_performative => true
                },
    {ok, Connection1} = amqp10_client:open_connection(OpnConf1),
    receive {amqp10_event, {connection, Connection1,
                            {opened, #'v1_0.open'{
                                        offered_capabilities = OffCaps1,
                                        properties = Props1}}}} ->
                ?assertNot(has_capability(?CAP_SOLE_CONN, OffCaps1)),
                ?assertNot(has_field(?SOLE_CONN_DETECTION_POLICY, Props1))
    after 10000 -> ct:fail(opened_timeout)
    end,

    %% same container-id, same user: must NOT conflict, enforcement is off
    %% while the feature flag is disabled
    OpnConfConn2 = maps:merge(OpnConf1, conn_config(other, Config)),
    {ok, Connection2} = amqp10_client:open_connection(OpnConfConn2),
    receive {amqp10_event, {connection, Connection2,
                            {opened, #'v1_0.open'{
                                        offered_capabilities = OffCaps2,
                                        properties = Props2}}}} ->
                ?assertNot(has_capability(?CAP_SOLE_CONN, OffCaps2)),
                ?assertNot(has_field(?SOLE_CONN_DETECTION_POLICY, Props2))
    after 10000 -> ct:fail(opened_timeout)
    end,

    ok = close_connection_sync(Connection2),
    ok = close_connection_sync(Connection1),

    ?assert(lists:all(fun(V) -> V end,
                      rabbit_ct_broker_helpers:rpc_all(Config, meck, validate, [Mod]))),
    _ = rabbit_ct_broker_helpers:rpc_all(Config, meck, unload, [Mod]).

refuse_connection_no_conflict(Config) ->
    ContainerId = atom_to_binary(?FUNCTION_NAME),
    OpnConf0 = conn_config(first, Config),
    OpnConf1 = OpnConf0#{
                 container_id => ContainerId,
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
    after 10000 -> ct:fail(opened_timeout)
    end,
    OpnConf2 = OpnConf1#{
                 container_id => <<ContainerId/binary, "-other">>
                },
    OpnConfConn2 = maps:merge(OpnConf2, conn_config(other, Config)),
    {ok, Connection2} = amqp10_client:open_connection(OpnConfConn2),
    receive {amqp10_event, {connection, Connection2,
                            {opened, #'v1_0.open'{
                                        offered_capabilities = OffCaps2,
                                        properties = Props2}}}} ->
                assert_has_sole_cap(OffCaps2),
                assert_has_weak_policy(Props2)
    after 10000 -> ct:fail(opened_timeout)
    end,

    ok = close_connection_sync(Connection2),
    ok = close_connection_sync(Connection1).

refuse_connection_no_conflict_across_vhosts(Config) ->
    ContainerId = atom_to_binary(?FUNCTION_NAME),
    Vhost2 = <<ContainerId/binary, "-vhost2">>,
    ok = rabbit_ct_broker_helpers:add_vhost(Config, Vhost2),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, <<"guest">>, Vhost2),

    OpnConf0 = conn_config(first, Config),
    OpnConf1 = OpnConf0#{
                 container_id => ContainerId,
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
    after 10000 -> ct:fail(opened_timeout)
    end,

    %% same container-id, same user, but a different vhost: must not
    %% conflict, sole_conn paths are vhost-scoped
    OpnConf2 = OpnConf1#{hostname => <<"vhost:", Vhost2/binary>>},
    {ok, Connection2} = amqp10_client:open_connection(OpnConf2),
    receive {amqp10_event, {connection, Connection2,
                            {opened, #'v1_0.open'{
                                        offered_capabilities = OffCaps2,
                                        properties = Props2}}}} ->
                assert_has_sole_cap(OffCaps2),
                assert_has_weak_policy(Props2)
    after 10000 -> ct:fail(opened_timeout)
    end,

    ok = close_connection_sync(Connection2),
    ok = close_connection_sync(Connection1),
    ok = rabbit_ct_broker_helpers:delete_vhost(Config, Vhost2).

refuse_connection_conflict_should_refuse_new_connection(Config) ->
    ContainerId = atom_to_binary(?FUNCTION_NAME),
    %% TODO test also with explicit refuse-connection policy (it is the default)
    OpnConf0 = conn_config(first, Config),
    OpnConf1 = OpnConf0#{
                 container_id => ContainerId,
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
    after 10000 -> ct:fail(opened_timeout)
    end,
    OpnConfConn2 = maps:merge(OpnConf1, conn_config(other, Config)),
    {ok, Connection2} = amqp10_client:open_connection(OpnConfConn2),
    receive {amqp10_event, {connection, Connection2,
                            {opened, #'v1_0.open'{
                                        offered_capabilities = OffCaps2,
                                        properties = Props2}}}} ->
                assert_has_sole_cap(OffCaps2),
                assert_has_weak_policy(Props2)
    after 10000 -> ct:fail(opened_timeout)
    end,
    receive {amqp10_event, {connection, Connection2,
                            {closed, #'v1_0.close'{error = Error}}}} ->
                #'v1_0.error'{condition = Cond,
                              description = Desc,
                              info = {map, Info}
                             } = Error,
                ?assertEqual(?V_1_0_AMQP_ERROR_INVALID_FIELD, Cond),
                ?assertEqual({utf8,
                              <<"The container-id is already bound to "
                                "an active exclusive connection.">>},
                             Desc),
                ?assert(lists:member({?V_1_0_AMQP_ERROR_INVALID_FIELD,
                                      {symbol, <<"container-id">>}},
                                     Info))
    after 10000 -> ct:fail(closed_timeout)
    end,

    ok = close_connection_sync(Connection1).

refuse_connection_conflict_different_user_should_be_refused(Config) ->
    ContainerId = atom_to_binary(?FUNCTION_NAME),
    OtherUser = <<ContainerId/binary, "-user2">>,
    ok = rabbit_ct_broker_helpers:add_user(Config, OtherUser),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, OtherUser, <<"/">>),

    OpnConf0 = conn_config(first, Config),
    OpnConf1 = OpnConf0#{
                 container_id => ContainerId,
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
    after 10000 -> ct:fail(opened_timeout)
    end,

    %% same container-id, but a different (authenticated) user must be
    %% refused outright, regardless of Connection1's aliveness
    OpnConf2 = OpnConf1#{sasl => {plain, OtherUser, OtherUser}},
    {ok, Connection2} = amqp10_client:open_connection(OpnConf2),
    receive {amqp10_event, {connection, Connection2,
                            {opened, #'v1_0.open'{
                                        offered_capabilities = OffCaps2,
                                        properties = Props2}}}} ->
                assert_has_sole_cap(OffCaps2),
                assert_has_weak_policy(Props2)
    after 10000 -> ct:fail(opened_timeout)
    end,
    receive {amqp10_event, {connection, Connection2,
                            {closed, #'v1_0.close'{error = Error}}}} ->
                #'v1_0.error'{condition = Cond,
                              description = Desc,
                              info = {map, Info}
                             } = Error,
                ?assertEqual(?V_1_0_AMQP_ERROR_INVALID_FIELD, Cond),
                ?assertEqual({utf8,
                              <<"The container-id is already bound to "
                                "an active exclusive connection.">>},
                             Desc),
                ?assert(lists:member({?V_1_0_AMQP_ERROR_INVALID_FIELD,
                                      {symbol, <<"container-id">>}},
                                     Info))
    after 10000 -> ct:fail(closed_timeout)
    end,

    %% Connection1 must be left untouched
    ok = close_connection_sync(Connection1),
    ok = rabbit_ct_broker_helpers:delete_user(Config, OtherUser).

refuse_connection_let_new_through_if_previous_closed(Config) ->
    ContainerId = atom_to_binary(?FUNCTION_NAME),
    OpnConf0 = conn_config(first, Config),
    OpnConf1 = OpnConf0#{
                 container_id => ContainerId,
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
    after 10000 -> ct:fail(opened_timeout)
    end,

    ok = close_connection_sync(Connection1),

    %% same container-id, same user: must be let through once the previous
    %% connection has actually gone away broker-side. Releasing the lease
    %% can lag slightly behind the client observing the close handshake
    %% complete, so retry rather than assume it is immediate.
    OpnConfConn2 = maps:merge(OpnConf1, conn_config(other, Config)),
    Connection2 = open_connection_retry_until_accepted(OpnConfConn2, 20),

    ok = close_connection_sync(Connection2).

close_existing_conflict_should_close_existing_connection(Config) ->
    ContainerId = atom_to_binary(?FUNCTION_NAME),
    OpnConf0 = conn_config(first, Config),
    OpnConf1 = OpnConf0#{
                 container_id => ContainerId,
                 desired_capabilities => [?CAP_SOLE_CONN],
                 properties => #{?SOLE_CONN_ENFORCEMENT_POLICY_KEY =>
                                 ?SOLE_CONN_ENFORCEMENT_POLICY_CLOSE_EXISTING},
                 notify_with_performative => true
                },
    {ok, Connection1} = amqp10_client:open_connection(OpnConf1),
    receive {amqp10_event, {connection, Connection1,
                            {opened, #'v1_0.open'{
                                        offered_capabilities = OffCaps1,
                                        properties = Props1}}}} ->
                assert_has_sole_cap(OffCaps1),
                assert_has_weak_policy(Props1)
    after 10000 -> ct:fail(opened_timeout)
    end,
    OpnConfConn2 = maps:merge(OpnConf1, conn_config(other, Config)),
    {ok, Connection2} = amqp10_client:open_connection(OpnConfConn2),
    receive {amqp10_event, {connection, Connection1,
                            {closed, #'v1_0.close'{
                                        error = #'v1_0.error'{
                                                   condition = ?V_1_0_AMQP_ERROR_RESOURCE_LOCKED,
                                                   info = {map, Info}
                                                  }}}}} ->
                ?assert(lists:member({?SOLE_CONN_ENFORCEMENT, true}, Info))
    after 10000 -> ct:fail(closed_timeout)
    end,
    receive {amqp10_event, {connection, Connection1,
                            {closed, normal}}} ->
                ok
    after 10000 -> ct:fail(closed_timeout)
    end,
    receive {amqp10_event, {connection, Connection2,
                            {opened, #'v1_0.open'{
                                        offered_capabilities = OffCaps2,
                                        properties = Props2}}}} ->
                assert_has_sole_cap(OffCaps2),
                assert_has_weak_policy(Props2)
    after 10000 -> ct:fail(opened_timeout)
    end,
    ok = close_connection_sync(Connection2).

close_existing_conflict_different_user_should_be_refused(Config) ->
    ContainerId = atom_to_binary(?FUNCTION_NAME),
    OtherUser = <<ContainerId/binary, "-user2">>,
    ok = rabbit_ct_broker_helpers:add_user(Config, OtherUser),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, OtherUser, <<"/">>),

    OpnConf0 = conn_config(first, Config),
    OpnConf1 = OpnConf0#{
                 container_id => ContainerId,
                 desired_capabilities => [?CAP_SOLE_CONN],
                 properties => #{?SOLE_CONN_ENFORCEMENT_POLICY_KEY =>
                                 ?SOLE_CONN_ENFORCEMENT_POLICY_CLOSE_EXISTING},
                 notify_with_performative => true
                },
    {ok, Connection1} = amqp10_client:open_connection(OpnConf1),
    receive {amqp10_event, {connection, Connection1,
                            {opened, #'v1_0.open'{
                                        offered_capabilities = OffCaps1,
                                        properties = Props1}}}} ->
                assert_has_sole_cap(OffCaps1),
                assert_has_weak_policy(Props1)
    after 10000 -> ct:fail(opened_timeout)
    end,

    %% a different user must be refused outright, Connection1 must be left
    %% untouched: close_existing only takes over a lease held by the same user
    OpnConf2 = OpnConf1#{sasl => {plain, OtherUser, OtherUser}},
    {ok, Connection2} = amqp10_client:open_connection(OpnConf2),
    receive {amqp10_event, {connection, Connection2,
                            {opened, #'v1_0.open'{
                                        offered_capabilities = OffCaps2,
                                        properties = Props2}}}} ->
                assert_has_sole_cap(OffCaps2),
                assert_has_weak_policy(Props2)
    after 10000 -> ct:fail(opened_timeout)
    end,
    receive {amqp10_event, {connection, Connection2,
                            {closed, #'v1_0.close'{error = Error}}}} ->
                #'v1_0.error'{condition = Cond,
                              description = Desc,
                              info = {map, Info}
                             } = Error,
                ?assertEqual(?V_1_0_AMQP_ERROR_INVALID_FIELD, Cond),
                ?assertEqual({utf8,
                              <<"The container-id is already bound to "
                                "an active exclusive connection.">>},
                             Desc),
                ?assert(lists:member({?V_1_0_AMQP_ERROR_INVALID_FIELD,
                                      {symbol, <<"container-id">>}},
                                     Info))
    after 10000 -> ct:fail(closed_timeout)
    end,

    %% Connection1 must be left untouched
    ok = close_connection_sync(Connection1),
    ok = rabbit_ct_broker_helpers:delete_user(Config, OtherUser).

%% ------------------------------------------------------------------
%% Internal Helpers
%% ------------------------------------------------------------------

conn_config(first, Config) ->
    OpnCnf = connection_config(0, Config),
    maps:remove(container_id, OpnCnf);
conn_config(_, Config) ->
    NodeIndex = case proplists:get_value(rmq_nodes_count, Config, 1) of
        1 -> 0;
        Count -> rand:uniform(Count - 1)
    end,
    OpnCnf = connection_config(NodeIndex, Config),
    maps:remove(container_id, OpnCnf).

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

%% Opens a connection and retries (with a fresh connection each time) as
%% long as it gets refused, up to Retries attempts. Used where a previous
%% connection holding the same container-id has just been closed: the lease
%% release is not guaranteed to be visible yet by the time the client
%% observes the close handshake complete.
open_connection_retry_until_accepted(_OpnConf, 0) ->
    ct:fail(container_id_never_released);
open_connection_retry_until_accepted(OpnConf, Retries) ->
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    receive {amqp10_event, {connection, Connection,
                            {opened, #'v1_0.open'{
                                        offered_capabilities = OffCaps,
                                        properties = Props}}}} ->
                case has_field(?AMQP_ERROR_CONNECTION_ESTABLISHMENT_FAILED, Props) of
                    false ->
                        assert_has_sole_cap(OffCaps),
                        assert_has_weak_policy(Props),
                        Connection;
                    true ->
                        %% The server already closes a refused connection on
                        %% its own (with an error, not `normal'), so wait for
                        %% that instead of calling close_connection_sync/1,
                        %% which only ever expects a `normal' close.
                        receive {amqp10_event, {connection, Connection, {closed, _}}} -> ok
                        after 10000 -> ok
                        end,
                        timer:sleep(200),
                        open_connection_retry_until_accepted(OpnConf, Retries - 1)
                end
    after 10000 -> ct:fail(opened_timeout)
    end.
