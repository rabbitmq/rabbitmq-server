%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(rabbit_amqp_sole_conn_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("khepri/include/khepri.hrl").

-compile([nowarn_export_all,
          export_all]).

-import(rabbit_amqp_sole_conn,
        [acquire/5]).

-import(rabbit_ct_helpers,
        [eventually/1, eventually/3]).

-import(rabbit_amqp_sole_conn,
        [get_store_id/0,
         get_ra_system/0]).

-define(VH, <<"/">>).
-define(CID1, <<"id-1">>).
-define(USER1, <<"user-1">>).
-define(USER2, <<"user-2">>).

all() ->
    [
      {group, default_group}
    ].

groups() ->
    [
     {default_group, [shuffle],
      [
        refuse_connection_should_refuse_new_connection_if_conflict,
        refuse_connection_let_new_through_if_previous_died,
        refuse_connection_should_refuse_different_user_even_if_existing_dead,
        close_existing_should_close_existing_connection,
        close_existing_should_refuse_different_user_and_not_kill_existing,
        try_put,
        force_delete_should_remove_existing_lease,
        force_delete_should_return_not_found_for_missing_lease,
        khepri_put_should_override_keep_while_monitor,
        khepri_triggers,
        khepri_cas,
        acquire_with_none_policy_should_always_succeed,
        refuse_connection_should_return_refuse_connection_on_unexpected_khepri_error,
        is_feature_enabled_reflects_flag_state
      ]}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(_, Config) ->
    ok = meck:new(rabbit_nodes, [passthrough, no_link]),
    ok = meck:expect(rabbit_nodes, list_running, fun() -> [node()] end),
    %% assume the feature flag is enabled
    ok = meck:new(rabbit_feature_flags, [passthrough, no_link]),
    ok = meck:expect(rabbit_feature_flags, is_enabled, fun(_) -> true end),
    ok = meck:new(rabbit_sup, [passthrough, no_link]),
    ok = meck:expect(rabbit_sup, start_child,
                     fun(rabbit_amqp_sole_conn) ->
                             %% We use gen_server:start instead of start_link
                             %% so it survives the death of the init_per_group process
                             gen_server:start({local, rabbit_amqp_sole_conn},
                                              rabbit_amqp_sole_conn, [], []),
                             ok
                     end),
    PrivDir = ?config(priv_dir, Config),
    DataDir = filename:join(
                PrivDir,
                rabbit_misc:format("data-~ts", [node()])),
    case application:load(rabbit) of
        ok                           -> ok;
        {error, {already_loaded, _}} -> ok
    end,
    ok = application:set_env(rabbit, data_dir, DataDir),
    {ok, _} = application:ensure_all_started(khepri),
    ok = rabbit_ra_systems:ensure_ra_system_started(get_ra_system()),
    rabbit_amqp_sole_conn:ensure_running(),
    Config.

end_per_group(_, Config) ->
    case whereis(rabbit_amqp_sole_conn) of
        undefined ->
            ok;
        Pid ->
            gen_server:stop(Pid)
    end,
    ok = khepri:stop(get_store_id()),
    ok = application:stop(khepri),
    ok = ra_system:stop(get_ra_system()),
    ok = application:stop(ra),
    ok = meck:unload(rabbit_sup),
    ok = meck:unload(rabbit_nodes),
    ok = meck:unload(rabbit_feature_flags),
    Config.

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, Config) ->
    clean_store_from_connections(),
    %% Defensive cleanup in case a testcase-local mock was left behind by a
    %% failed assertion before it could unload it.
    try meck:unload(khepri_adv) catch _:_ -> ok end,
    %% Defensive: restore the group-wide default (flag enabled) expectation
    %% in case a failed assertion in is_feature_enabled_reflects_flag_state
    %% left it mocked to `false`.
    try meck:expect(rabbit_feature_flags, is_enabled, fun(_) -> true end)
    catch _:_ -> ok end,
    Config.

refuse_connection_should_refuse_new_connection_if_conflict(_) ->
    Pid1 = spawn_disposable(),
    ?assertEqual(ok, acquire(refuse_connection, ?VH, ?CID1, ?USER1, Pid1)),
    Pid2 = spawn_disposable(),
    ?assertEqual({error, refuse_connection},
                 acquire(refuse_connection, ?VH, ?CID1, ?USER1, Pid2)),
    Pid2 ! die,
    Pid1 ! die,
    ok.

refuse_connection_let_new_through_if_previous_died(_) ->
    Pid1 = spawn_disposable(),
    ?assertEqual(ok, acquire(refuse_connection, ?VH, ?CID1, ?USER1, Pid1)),
    ?assertEqual({error, refuse_connection},
                 acquire(refuse_connection, ?VH, ?CID1, ?USER1, self())),
    Pid1 ! die,
    eventually(?_assertNot(is_process_alive(Pid1))),

    Path = rabbit_amqp_sole_conn:conn_path(?VH, ?CID1),
    eventually(?_assertMatch({error, {khepri, node_not_found, _}},
                             khepri:get(get_store_id(), Path))),

    Pid2 = spawn_disposable(),
    ?assertEqual(ok, acquire(refuse_connection, ?VH, ?CID1, ?USER1, Pid2)),
    Pid2 ! die,
    ok.

refuse_connection_should_refuse_different_user_even_if_existing_dead(_) ->
    Path = rabbit_amqp_sole_conn:conn_path(?VH, ?CID1),
    Pid1 = spawn_disposable(),
    Pid1 ! die,
    eventually(?_assertNot(is_process_alive(Pid1))),
    %% insert the stale entry directly, without keep_while, so that it
    %% deterministically survives Pid1's death for this test
    Conn1 = rabbit_amqp_sole_conn:conn(Pid1, ?USER1),
    ?assertMatch({ok, _}, khepri_adv:create(get_store_id(), Path, Conn1)),

    %% a different user must be refused, even though the existing
    %% connection is dead: aliveness is only checked for the same user
    ?assertEqual({error, refuse_connection},
                 acquire(refuse_connection, ?VH, ?CID1, ?USER2, self())),
    ?assertEqual({ok, Conn1}, khepri:get(get_store_id(), Path)),

    %% the original user can still take over the dead connection's lease
    Pid2 = spawn_disposable(),
    ?assertEqual(ok, acquire(refuse_connection, ?VH, ?CID1, ?USER1, Pid2)),
    Pid2 ! die,
    ok.

close_existing_should_close_existing_connection(_) ->
    TestPid = self(),
    Path = rabbit_amqp_sole_conn:conn_path(?VH, ?CID1),
    Pid1 = spawn(fun() ->
                         process_flag(trap_exit, true),
                         receive
                             {'EXIT', _, sole_conn_enforcement} ->
                                 TestPid ! close_sole_conn_enforcement_received
                         after 5000 ->
                                   ok
                         end
                 end),
    Pid2 = spawn_disposable(),
    %% 1 takes the lease
    ?assertEqual(ok, acquire(close_existing, ?VH, ?CID1, ?USER1, Pid1)),
    ?assertEqual({ok, rabbit_amqp_sole_conn:conn(Pid1, ?USER1)},
                 khepri:get(get_store_id(), Path)),
    %% 2 takes the lease from 1
    ?assertEqual(ok, acquire(close_existing, ?VH, ?CID1, ?USER1, Pid2)),
    ?assertEqual({ok, rabbit_amqp_sole_conn:conn(Pid2, ?USER1)},
                 khepri:get(get_store_id(), Path)),
    %% 1 must have received the enforcement message
    ?assertEqual(ok, receive close_sole_conn_enforcement_received -> ok
                     after 5000 -> timeout end),
    %% 1 should have stopped
    eventually(?_assertNot(is_process_alive(Pid1))),

    Pid2 ! die,

    eventually(?_assertMatch({error, _}, khepri:get(get_store_id(), Path))),
    ok.

close_existing_should_refuse_different_user_and_not_kill_existing(_) ->
    Path = rabbit_amqp_sole_conn:conn_path(?VH, ?CID1),
    Pid1 = spawn_disposable(),
    Pid2 = spawn_disposable(),
    %% 1 takes the lease
    ?assertEqual(ok, acquire(close_existing, ?VH, ?CID1, ?USER1, Pid1)),
    %% a different user must be refused, 1 must be left untouched
    ?assertEqual({error, refuse_connection},
                 acquire(close_existing, ?VH, ?CID1, ?USER2, Pid2)),
    ?assert(is_process_alive(Pid1)),
    ?assertEqual({ok, rabbit_amqp_sole_conn:conn(Pid1, ?USER1)},
                 khepri:get(get_store_id(), Path)),
    %% the original user can still close and replace its own connection
    Pid3 = spawn_disposable(),
    ?assertEqual(ok, acquire(close_existing, ?VH, ?CID1, ?USER1, Pid3)),
    eventually(?_assertNot(is_process_alive(Pid1))),
    ?assertEqual({ok, rabbit_amqp_sole_conn:conn(Pid3, ?USER1)},
                 khepri:get(get_store_id(), Path)),

    Pid2 ! die,
    Pid3 ! die,
    ok.

try_put(_) ->
    Path = rabbit_amqp_sole_conn:conn_path(?VH, ?CID1),
    Pid1 = spawn_disposable(),
    Conn1 = rabbit_amqp_sole_conn:conn(Pid1, ?USER1),
    %% acquire lease
    ?assertEqual(ok, acquire(refuse_connection, ?VH, ?CID1, ?USER1, Pid1)),
    ?assertEqual({ok, Conn1}, khepri:get(get_store_id(), Path)),
    %% simulating new incoming connection
    Pid2 = spawn_disposable(),
    Conn2 = rabbit_amqp_sole_conn:conn(Pid2, ?USER1),
    %% new connection manages to replace old connection
    ?assertEqual(ok, rabbit_amqp_sole_conn:try_put(Path, Conn1, Conn2)),
    ?assertEqual({ok, Conn2}, khepri:get(get_store_id(), Path)),
    %% new connection arrives, but a bit slower than the second one,
    %% it still sees Conn1 in the datastore
    Pid3 = spawn_disposable(),
    Conn3 = rabbit_amqp_sole_conn:conn(Pid3, ?USER1),
    ?assertEqual(error, rabbit_amqp_sole_conn:try_put(Path, Conn1, Conn3)),
    ?assertEqual({ok, Conn2}, khepri:get(get_store_id(), Path)),

    %% can't take the lease, conn2 has it
    ?assertEqual({error, refuse_connection},
                 acquire(refuse_connection, ?VH, ?CID1, ?USER1, Pid1)),
    %% conn2 dies, it should release the lease
    Pid2 ! die,
    %% we try to take the lease with conn3 (the other 2 connections are dead)
    eventually(?_assertEqual(ok, acquire(refuse_connection, ?VH, ?CID1, ?USER1, Pid3))),

    Pid3 ! die,
    ok.

force_delete_should_remove_existing_lease(_) ->
    Path = rabbit_amqp_sole_conn:conn_path(?VH, ?CID1),
    Pid1 = spawn_disposable(),
    ?assertEqual(ok, acquire(refuse_connection, ?VH, ?CID1, ?USER1, Pid1)),
    ?assertMatch({ok, _}, khepri:get(get_store_id(), Path)),

    ?assertEqual(ok, rabbit_amqp_sole_conn:force_delete(?VH, ?CID1)),
    ?assertMatch({error, {khepri, node_not_found, _}},
                 khepri:get(get_store_id(), Path)),

    Pid1 ! die,
    ok.

force_delete_should_return_not_found_for_missing_lease(_) ->
    Path = rabbit_amqp_sole_conn:conn_path(?VH, ?CID1),
    ?assertMatch({error, {khepri, node_not_found, _}},
                 khepri:get(get_store_id(), Path)),

    ?assertEqual({error, not_found}, rabbit_amqp_sole_conn:force_delete(?VH, ?CID1)),
    ok.

khepri_put_should_override_keep_while_monitor(_) ->
    Pid1 = spawn_disposable(),
    Opts1 = #{keep_while => Pid1},
    Path1 = [rmq, vhosts, ?VH, sole_conn, <<"1">>],
    ?assertMatch({ok, _}, khepri_adv:create(get_store_id(), Path1, Pid1, Opts1)),
    ?assertEqual({ok, Pid1}, khepri:get(get_store_id(), Path1)),

    Pid2 = spawn_disposable(),
    Opts2 = #{keep_while => Pid2},
    ?assertMatch(ok, khepri:put(get_store_id(), Path1, Pid2, Opts2)),
    ?assertEqual({ok, Pid2}, khepri:get(get_store_id(), Path1)),

    %% making sure that the node does not monitor the first PID anymore
    Pid1 ! die,
    timer:sleep(500),
    ?assertEqual({ok, Pid2}, khepri:get(get_store_id(), Path1)),
    Pid2 ! die,
    eventually(?_assertMatch({error, {khepri, node_not_found, _}},
                             khepri:get(get_store_id(), Path1))),
    ok.

khepri_triggers(_) ->
    Key = ?FUNCTION_NAME,
    StoredProcPath = [rmq, sole_conn, proc],
    Pid = self(),
    Proc = fun(Props) ->
                   #khepri_trigger{type = tree,
                                   event = #{path := Path, change := Change}} = Props,
                   Pid ! {sproc, Key, {Change, Path}}
           end,

    khepri_adv:put(get_store_id(), StoredProcPath, Proc),

    EventFilter = khepri_evf:tree([rmq, vhosts,
                                   ?KHEPRI_WILDCARD_STAR_STAR,
                                   sole_conn,
                                   ?KHEPRI_WILDCARD_STAR_STAR],
                                  #{on_actions => [update, delete]}),

    %% we want to try closing the connection on all members
    %% this way a connection on an isolated node will get the message
    %% when the partition ends.
    Opts = #{where => all_members},
    ok = khepri:register_trigger(
           get_store_id(),
           sole_conn,
           EventFilter,
           StoredProcPath,
           Opts),

    Path1 = [rmq, vhosts, ?VH, sole_conn, <<"1">>],
    ?assertMatch({ok, _}, khepri_adv:create(get_store_id(), Path1, <<"1">>)),

    ?assertMatch({ok, _}, khepri_adv:put(get_store_id(), Path1, <<"2">>)),
    ?assertEqual(executed, receive_sproc_msg(Key, {update, Path1})),

    ?assertMatch({ok, _}, khepri_adv:delete(get_store_id(), Path1)),
    ?assertEqual(executed, receive_sproc_msg(Key, {delete, Path1})),
    %% the tree nodes created implictly are deleted automatically
    eventually(?_assertMatch({error, {khepri, node_not_found, _}},
                             khepri:get(get_store_id(), Path1))),
    ok.

khepri_cas(_) ->
    StoreId = get_store_id(),
    Path = [rmq, vhosts, ?VH, sole_conn, <<"1">>],
    Pid1 = spawn_disposable(),
    Pid2 = spawn_disposable(),
    Pid3 = spawn_disposable(),
    V1 = rabbit_amqp_sole_conn:conn(Pid1, ?USER1),
    V2 = rabbit_amqp_sole_conn:conn(Pid2, ?USER1),
    V3 = rabbit_amqp_sole_conn:conn(Pid3, ?USER1),

    ?assertMatch(ok, khepri:create(get_store_id(), Path, V1)),
    ?assertEqual({ok, V1}, khepri:get(StoreId, Path)),

    ?assertMatch(ok,
                 khepri:compare_and_swap(StoreId, Path, V1, V2)),
    ?assertEqual({ok, V2}, khepri:get(StoreId, Path)),
    ?assertMatch({error, _},
                 khepri:compare_and_swap(StoreId, Path, V1, V3)),
    ?assertEqual({ok, V2}, khepri:get(StoreId, Path)),

    ?assertMatch(ok, khepri:delete(StoreId, Path)),
    ?assertMatch({error, _}, khepri:get(StoreId, Path)),
    ?assertMatch({error, _},
                 khepri:compare_and_swap(StoreId, Path, V1, V2)),
    ok.

acquire_with_none_policy_should_always_succeed(_) ->
    Pid1 = spawn_disposable(),
    ?assertEqual(ok, acquire(none, ?VH, ?CID1, ?USER1, Pid1)),
    %% a conflicting acquire, even from a different user, must also succeed:
    %% the `none' policy short-circuits before any Khepri lookup
    Pid2 = spawn_disposable(),
    ?assertEqual(ok, acquire(none, ?VH, ?CID1, ?USER2, Pid2)),
    Pid1 ! die,
    Pid2 ! die,
    ok.

refuse_connection_should_return_refuse_connection_on_unexpected_khepri_error(_) ->
    ok = meck:new(khepri_adv, [passthrough, no_link]),
    ok = meck:expect(khepri_adv, create,
                     fun(_StoreId, _Path, _Payload, _Opts) ->
                             {error, some_unexpected_reason}
                     end),
    Pid1 = spawn_disposable(),
    ?assertEqual({error, refuse_connection},
                 acquire(refuse_connection, ?VH, ?CID1, ?USER1, Pid1)),
    ok = meck:unload(khepri_adv),
    Pid1 ! die,
    ok.

is_feature_enabled_reflects_flag_state(_) ->
    ?assert(rabbit_amqp_sole_conn:is_feature_enabled()),

    ok = meck:expect(rabbit_feature_flags, is_enabled,
                     fun('rabbitmq_4.4.0') -> false end),
    ?assertNot(rabbit_amqp_sole_conn:is_feature_enabled()),
    ?assertEqual({error, sole_conn_feature_flag_not_enabled},
                 rabbit_amqp_sole_conn:status()),
    ?assertEqual({error, sole_conn_feature_flag_not_enabled},
                 rabbit_amqp_sole_conn:force_delete(?VH, ?CID1)),

    ok = meck:expect(rabbit_feature_flags, is_enabled, fun(_) -> true end),
    ok.

%% --------------------------------------------------------------
%% Internal Helpers
%% --------------------------------------------------------------

clean_store_from_connections() ->
    ok = khepri:delete(get_store_id(), [rabbitmq, vhosts]).

spawn_disposable() ->
    spawn(fun() -> receive die -> ok end end).


receive_sproc_msg(Key, V) ->
    receive {sproc, Key, V} -> executed
    after 1000              -> timeout
    end.
