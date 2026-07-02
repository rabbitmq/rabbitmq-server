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
-include_lib("amqp10_common/include/amqp10_framing.hrl").
-include_lib("amqp10_common/include/amqp10_sole_conn.hrl").

-compile([nowarn_export_all,
          export_all]).

-import(rabbit_amqp_sole_conn,
        [acquire/4]).

-import(rabbit_ct_helpers,
        [eventually/1, eventually/3]).

-define(VH, <<"/">>).
-define(CID1, <<"id-1">>).

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
        close_existing_should_close_existing_connection,
        close_connection_sends_proper_error,
        close_connection_tolerates_timeout,
        try_put,
        khepri_put_should_override_keep_while_monitor,
        khepri_triggers,
        khepri_cas
      ]}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(_, Config) ->
    PrivDir = ?config(priv_dir, Config),
    DataDir = filename:join(
                PrivDir,
                rabbit_misc:format("data-~ts", [node()])),
    case application:load(rabbit) of
        ok                           -> ok;
        {error, {already_loaded, _}} -> ok
    end,
    ok = application:set_env(rabbit, data_dir, DataDir),
    rabbit_khepri:setup(),
    Config.

end_per_group(_, Config) ->
    ok = khepri:stop(rabbit_khepri:get_store_id()),
    ok = ra_system:stop(coordination),
    Config.

init_per_testcase(_, Config) ->
    rabbit_amqp_sole_conn:init(),
    Config.

end_per_testcase(close_existing_should_close_existing_connection, Config) ->
    ok = khepri:delete(rabbit_khepri:get_store_id(), [?KHEPRI_ROOT_NODE]),
    Config;
end_per_testcase(close_connection_tolerates_timeout, Config) ->
    ok = meck:unload(rabbit_networking),
    ok = khepri:delete(rabbit_khepri:get_store_id(), [?KHEPRI_ROOT_NODE]),
    Config;
end_per_testcase(_, Config) ->
    clean_store(),
    Config.

refuse_connection_should_refuse_new_connection_if_conflict(_) ->
    Pid1 = spawn_disposable(),
    ?assertEqual(ok, acquire(refuse_connection, ?VH, ?CID1, Pid1)),
    Pid2 = spawn_disposable(),
    ?assertEqual({error, refuse_connection},
                 acquire(refuse_connection, ?VH, ?CID1, Pid2)),
    Pid2 ! die,
    Pid1 ! die,
    ok.

refuse_connection_let_new_through_if_previous_died(_) ->
    Pid1 = spawn_disposable(),
    ?assertEqual(ok, acquire(refuse_connection, ?VH, ?CID1, Pid1)),
    ?assertEqual({error, refuse_connection},
                 acquire(refuse_connection, ?VH, ?CID1, self())),
    Pid1 ! die,
    eventually(?_assertNot(is_process_alive(Pid1))),
    Pid2 = spawn_disposable(),
    ?assertEqual(ok, acquire(refuse_connection, ?VH, ?CID1, Pid2)),
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
    ?assertEqual(ok, acquire(close_existing, ?VH, ?CID1, Pid1)),
    ?assertEqual({ok, rabbit_amqp_sole_conn:conn(Pid1)},
                 rabbit_khepri:get(Path)),
    %% 2 takes the lease from 1
    ?assertEqual(ok, acquire(close_existing, ?VH, ?CID1, Pid2)),
    ?assertEqual({ok, rabbit_amqp_sole_conn:conn(Pid2)},
                 rabbit_khepri:get(Path)),
    %% 1 must have received the enforcement message
    ?assertEqual(ok, receive close_sole_conn_enforcement_received -> ok
                     after 5000 -> timeout end),
    %% 1 should have stopped
    eventually(?_assertNot(is_process_alive(Pid1))),

    Pid2 ! die,

    eventually(?_assertMatch({error, _}, rabbit_khepri:get(Path))),
    ok.

close_connection_sends_proper_error(_) ->
    TestPid = self(),
    ConnPid = spawn(fun() ->
        receive
            {rabbit_call, From, {close, Error}} ->
                TestPid ! {received, Error},
                gen:reply(From, ok)
        after 5000 ->
            ok
        end
    end),
    ok = rabbit_amqp_sole_conn:close_connection(rabbit_amqp_sole_conn:conn(ConnPid)),
    receive
        {received, Error} ->
            ?assertMatch(
               #'v1_0.error'{
                   condition = ?V_1_0_AMQP_ERROR_RESOURCE_LOCKED,
                   info = {map, [{?SOLE_CONN_ENFORCEMENT, {boolean, true}}]}},
               Error)
    after 5000 ->
        error(timeout)
    end.

close_connection_tolerates_timeout(_) ->
    ok = meck:new(rabbit_networking, [passthrough]),
    ok = meck:expect(rabbit_networking, close_connection,
                     fun(Pid, Error, _Timeout) ->
                         try rabbit_reader:force_close(Pid, Error, 10)
                         catch exit:{_Reason, _Location} -> ok
                         end
                     end),
    ConnPid = spawn(fun() ->
        receive
            %% Receive the close request but do not reply, simulating a slow connection.
            {rabbit_call, _From, {close, _Error}} -> receive die -> ok end;
            die -> ok
        end
    end),
    %% Must return ok even when the target process does not reply within the timeout.
    ?assertEqual(ok, rabbit_amqp_sole_conn:close_connection(
                       rabbit_amqp_sole_conn:conn(ConnPid))),
    ConnPid ! die.

try_put(_) ->
    %% clean the store, so the stored proc does not interfer
    clean_store(),
    Path = rabbit_amqp_sole_conn:conn_path(?VH, ?CID1),
    Pid1 = spawn_disposable(),
    Conn1 = rabbit_amqp_sole_conn:conn(Pid1),
    %% acquire lease
    ?assertEqual(ok, acquire(refuse_connection, ?VH, ?CID1, Pid1)),
    ?assertEqual({ok, Conn1}, rabbit_khepri:get(Path)),
    %% simulation new incoming connection
    Pid2 = spawn_disposable(),
    Conn2 = rabbit_amqp_sole_conn:conn(Pid2),
    %% new connection manages to replace old connection
    ?assertEqual(ok, rabbit_amqp_sole_conn:try_put(Path, Conn1, Conn2)),
    ?assertEqual({ok, Conn2}, rabbit_khepri:get(Path)),
    %% new connection arrives, but a bit slower than the second one,
    %% it still sees Conn1 in the datastore
    Pid3 = spawn_disposable(),
    Conn3 = rabbit_amqp_sole_conn:conn(Pid3),
    ?assertEqual(error, rabbit_amqp_sole_conn:try_put(Path, Conn1, Conn3)),
    ?assertEqual({ok, Conn2}, rabbit_khepri:get(Path)),

    %% can't take the lease, conn2 has it
    ?assertEqual({error, refuse_connection},
                 acquire(refuse_connection, ?VH, ?CID1, Pid1)),
    %% conn2 dies, it should release the lease
    Pid2 ! die,
    eventually(?_assertEqual(ok, acquire(refuse_connection, ?VH, ?CID1, Pid1))),

    Pid3 ! die,
    Pid1 ! die,
    ok.

khepri_put_should_override_keep_while_monitor(_) ->
    Pid1 = spawn_disposable(),
    Opts1 = #{keep_while => Pid1},
    Path1 = [rmq, vhosts, ?VH, sole_conn, <<"1">>],
    ?assertMatch({ok, _}, rabbit_khepri:adv_create(Path1, Pid1, Opts1)),
    ?assertEqual({ok, Pid1}, rabbit_khepri:get(Path1)),

    Pid2 = spawn_disposable(),
    Opts2 = #{keep_while => Pid2},
    ?assertMatch(ok, rabbit_khepri:put(Path1, Pid2, Opts2)),
    ?assertEqual({ok, Pid2}, rabbit_khepri:get(Path1)),

    %% making sure that the node does not monitor the first PID anymore
    Pid1 ! die,
    timer:sleep(500),
    ?assertEqual({ok, Pid2}, rabbit_khepri:get(Path1)),
    Pid2 ! die,
    eventually(?_assertMatch({error, {khepri, node_not_found, _}},
                             rabbit_khepri:get(Path1))),
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

    rabbit_khepri:adv_put(StoredProcPath, Proc),

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
           rabbit_khepri:get_store_id(),
           sole_conn,
           EventFilter,
           StoredProcPath,
           Opts),

    Path1 = [rmq, vhosts, ?VH, sole_conn, <<"1">>],
    ?assertMatch({ok, _}, rabbit_khepri:adv_create(Path1, <<"1">>)),

    ?assertMatch({ok, _}, rabbit_khepri:adv_put(Path1, <<"2">>)),
    ?assertEqual(executed, receive_sproc_msg(Key, {update, Path1})),

    ?assertMatch({ok, _}, rabbit_khepri:adv_delete(Path1)),
    ?assertEqual(executed, receive_sproc_msg(Key, {delete, Path1})),
    %% the tree nodes created implictly are deleted automatically
    eventually(?_assertMatch({error, {khepri, node_not_found, _}},
                             rabbit_khepri:get(Path1))),
    ok.

khepri_cas(_) ->
    StoreId = rabbit_khepri:get_store_id(),
    Path = [rmq, vhosts, ?VH, sole_conn, <<"1">>],
    Pid1 = spawn_disposable(),
    Pid2 = spawn_disposable(),
    Pid3 = spawn_disposable(),
    V1 = rabbit_amqp_sole_conn:conn(Pid1),
    V2 = rabbit_amqp_sole_conn:conn(Pid2),
    V3 = rabbit_amqp_sole_conn:conn(Pid3),

    ?assertMatch(ok, rabbit_khepri:create(Path, V1)),
    ?assertEqual({ok, V1}, khepri:get(StoreId, Path)),

    ?assertMatch(ok,
                 khepri:compare_and_swap(StoreId, Path, V1, V2)),
    ?assertEqual({ok, V2}, khepri:get(StoreId, Path)),
    ?assertMatch({error, _},
                 khepri:compare_and_swap(StoreId, Path, V1, V3)),
    ?assertEqual({ok, V2}, khepri:get(StoreId, Path)),

    ?assertMatch(ok, rabbit_khepri:delete(Path)),
    ?assertMatch({error, _}, khepri:get(StoreId, Path)),
    ?assertMatch({error, _},
                 khepri:compare_and_swap(StoreId, Path, V1, V2)),
    ok.

%% --------------------------------------------------------------
%% Internal Helpers
%% --------------------------------------------------------------

clean_store() ->
    ok = khepri:delete(rabbit_khepri:get_store_id(), [?KHEPRI_ROOT_NODE]).

spawn_disposable() ->
    spawn(fun() -> receive die -> ok end end).


receive_sproc_msg(Key, V) ->
    receive {sproc, Key, V} -> executed
    after 1000              -> timeout
    end.
