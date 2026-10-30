%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(vhost_deletion_SUITE).

-behaviour(rabbit_vhost_deletion).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile([export_all, nowarn_export_all]).

-define(APP, vhost_deletion_test_handler).
-define(VHOST, <<"deleted">>).
-define(TIMEOUT, 10000).

all() ->
    [{group, single_node}, {group, cluster}].

groups() ->
    [{single_node, [], [ids_increase_on_recreation,
                        ids_are_not_exported,
                        records_without_an_id_get_one,
                        legacy_upgrade_preserves_storage,
                        recreation_uses_separate_storage,
                        deletion_calls_the_handler,
                        leftovers_are_listed_with_ids,
                        a_recreated_vhost_restarts_its_supervisor,
                        an_unidentified_supervisor_is_restarted,
                        stale_deletion_keeps_the_replacement,
                        metadata_failure_refuses_supervisor_lookup,
                        metadata_recovery_retries_vhost_start,
                        uncertain_deletion_keeps_the_supervisor,
                        cleanup_rechecks_candidates,
                        legacy_reset_keeps_foreign_data]},
     {cluster, [], [deletion_calls_the_handler_on_every_node,
                    deletion_does_not_wait_for_a_stopped_node,
                    minority_cannot_authorize_cleanup,
                    classic_recreation_after_node_restart]}].

suite() ->
    [{timetrap, {minutes, 5}}].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config,
      [fun rabbit_ct_broker_helpers:configure_dist_proxy/1]).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group, Config) ->
    Nodes = case Group of single_node -> 1; cluster -> 3 end,
    Config1 = rabbit_ct_helpers:set_config(
                Config, [{rmq_nodes_count, Nodes},
                         {rmq_nodename_suffix, Group},
                         {net_ticktime, 5}]),
    rabbit_ct_helpers:run_steps(Config1, rabbit_ct_broker_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config, rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    _ = rabbit_ct_broker_helpers:delete_vhost(Config, ?VHOST),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------

ids_increase_on_recreation(Config) ->
    ok = rabbit_ct_broker_helpers:add_vhost(Config, ?VHOST),
    {ok, First} = rpc(Config, 0, rabbit_vhost, id, [?VHOST]),
    CreatedAt = info(Config, created_at),
    ?assertEqual(vhost:id_time(First), calendar:rfc3339_to_system_time(binary_to_list(CreatedAt),
                                                       [{unit, microsecond}])),
    ok = rabbit_ct_broker_helpers:delete_vhost(Config, ?VHOST),
    ?assertEqual({error, not_found}, rpc(Config, 0, rabbit_vhost, id, [?VHOST])),
    ok = rabbit_ct_broker_helpers:add_vhost(Config, ?VHOST),
    {ok, Second} = rpc(Config, 0, rabbit_vhost, id, [?VHOST]),
    ?assert(Second > First).

ids_are_not_exported(Config) ->
    ok = rabbit_ct_broker_helpers:add_vhost(Config, ?VHOST),
    {ok, Id} = rpc(Config, 0, rabbit_vhost, id, [?VHOST]),
    #{vhosts := VHosts} = Definitions = rpc(Config, 0, rabbit_definitions, all_definitions, []),
    [Exported] = [V || V = #{<<"name">> := ?VHOST} <- VHosts],
    ?assertEqual([<<"limits">>, <<"metadata">>, <<"name">>], lists:sort(maps:keys(Exported))),
    ok = rabbit_ct_broker_helpers:delete_vhost(Config, ?VHOST),
    Json = rpc(Config, 0, rabbit_json, encode, [Definitions]),
    ok = rpc(Config, 0, rabbit_definitions, import_raw, [Json]),
    {ok, Imported} = rpc(Config, 0, rabbit_vhost, id, [?VHOST]),
    ?assert(Imported > Id).

records_without_an_id_get_one(Config) ->
    Names = [<<"without-id-1">>, <<"without-id-2">>],
    try
        [ok = rpc(Config, 0, rabbit_khepri, put,
                  [rpc(Config, 0, rabbit_db_vhost, khepri_vhost_path, [Name]),
                   vhost:new(Name, [], #{})]) || Name <- Names],
        [First, Second] = Names,
        {ok, Lazy} = rpc(Config, 0, rabbit_vhost, id, [First]),
        ?assertEqual(Lazy, vhost:get_id(rpc(Config, 0, rabbit_db_vhost, get, [First]))),
        ok = rpc(Config, 0, rabbit_db_vhost, assign_ids, [#{feature_name => 'rabbitmq_5.0.0'}]),
        {ok, Assigned} = rpc(Config, 0, rabbit_vhost, id, [Second]),
        ?assert(Assigned > Lazy),
        ?assertEqual({ok, Lazy}, rpc(Config, 0, rabbit_vhost, id, [First]))
    after
        [rpc(Config, 0, rabbit_db_vhost, delete, [Name]) || Name <- Names]
    end.

legacy_upgrade_preserves_storage(Config) ->
    ok = rpc(Config, 0, ?MODULE, disable_incarnation_flag, []),
    {Path, Marker} = try
        ok = rabbit_ct_broker_helpers:add_vhost(Config, ?VHOST),
        {ok, Sup} = rpc(Config, 0, rabbit_vhost_sup_sup, get_vhost_sup, [?VHOST]),
        ?assertEqual(undefined, rpc(Config, 0, rabbit_vhost_sup_sup, id, [?VHOST])),
        LegacyPath = rpc(Config, 0, rabbit_vhost, msg_store_dir_path, [?VHOST]),
        LegacyMarker = filename:join(LegacyPath, "kept"),
        ok = rpc(Config, 0, file, write_file, [LegacyMarker, <<"legacy">>]),
        ok = rpc(Config, 0, rabbit_db_vhost, assign_ids, [#{feature_name => 'rabbitmq_5.0.0'}]),
        {ok, Id} = rpc(Config, 0, rabbit_vhost, id, [?VHOST]),
        ?assertEqual({ok, Id, legacy}, rpc(Config, 0, rabbit_db_vhost, identity, [?VHOST])),
        ?assertEqual({ok, Sup}, rpc(Config, 0, rabbit_vhost_sup_sup, get_vhost_sup, [?VHOST])),
        ?assertEqual(LegacyPath, rpc(Config, 0, rabbit_vhost, msg_store_dir_path, [?VHOST])),
        ?assertEqual({ok, <<"legacy">>}, rpc(Config, 0, file, read_file, [LegacyMarker])),
        {LegacyPath, LegacyMarker}
    after
        ok = rpc(Config, 0, meck, unload, [rabbit_feature_flags])
    end,
    ok = rabbit_ct_broker_helpers:delete_vhost(Config, ?VHOST),
    ok = rabbit_ct_broker_helpers:add_vhost(Config, ?VHOST),
    ?assertNotEqual(Path, rpc(Config, 0, rabbit_vhost, msg_store_dir_path, [?VHOST])),
    ?assertEqual({ok, <<"legacy">>}, rpc(Config, 0, file, read_file, [Marker])).

disable_incarnation_flag() ->
    ok = meck:new(rabbit_feature_flags, [passthrough, no_link]),
    meck:expect(rabbit_feature_flags, is_enabled,
                fun('rabbitmq_5.0.0') -> false;
                   (Flag) -> meck:passthrough([Flag])
                end).

recreation_uses_separate_storage(Config) ->
    ok = rabbit_ct_broker_helpers:add_vhost(Config, ?VHOST),
    OldPath = rpc(Config, 0, rabbit_vhost, msg_store_dir_path, [?VHOST]),
    {ok, OldId} = rpc(Config, 0, rabbit_vhost, id, [?VHOST]),
    VHost = rpc(Config, 0, rabbit_db_vhost, get, [?VHOST]),
    Marker = filename:join(OldPath, "kept"),
    ok = rpc(Config, 0, file, write_file, [Marker, <<"old">>]),
    ok = rpc(Config, 0, rabbit_khepri, put,
             [rpc(Config, 0, rabbit_db_vhost, khepri_vhost_path, [?VHOST]),
              vhost:new(?VHOST, vhost:get_limits(VHost), vhost:get_metadata(VHost), OldId + 1)]),
    _ = rpc(Config, 0, rabbit_vhost_sup_sup, start_vhost, [?VHOST]),
    NewPath = rpc(Config, 0, rabbit_vhost, msg_store_dir_path, [?VHOST]),
    ?assertNotEqual(OldPath, NewPath),
    ?assertEqual({error, enoent}, rpc(Config, 0, file, read_file, [filename:join(NewPath, "kept")])),
    ?assertEqual({ok, <<"old">>}, rpc(Config, 0, file, read_file, [Marker])).

legacy_reset_keeps_foreign_data(Config) ->
    ok = rpc(Config, 0, ?MODULE, disable_incarnation_flag, []),
    OldPath = try
        ok = rabbit_ct_broker_helpers:add_vhost(Config, ?VHOST),
        rpc(Config, 0, rabbit_vhost, msg_store_dir_path, [?VHOST])
    after
        ok = rpc(Config, 0, meck, unload, [rabbit_feature_flags])
    end,
    Marker = filename:join(OldPath, "kept"),
    ok = rpc(Config, 0, file, write_file, [Marker, <<"original-cluster">>]),
    {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, ["stop_app"]),
    {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, ["reset"]),
    {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, ["start_app"]),
    ok = rpc(Config, 0, ?MODULE, disable_incarnation_flag, []),
    try
        ok = rabbit_ct_broker_helpers:add_vhost(Config, ?VHOST),
        NewPath = rpc(Config, 0, rabbit_vhost, msg_store_dir_path, [?VHOST]),
        ?assertNotEqual(OldPath, NewPath),
        ?assertEqual({error, enoent}, rpc(Config, 0, file, read_file, [filename:join(NewPath, "kept")])),
        ?assertEqual({ok, <<"original-cluster">>}, rpc(Config, 0, file, read_file, [Marker]))
    after
        ok = rpc(Config, 0, meck, unload, [rabbit_feature_flags])
    end.

classic_recreation_after_node_restart(Config) ->
    recreation_after_node_restart(Config, <<"classic">>).

recreation_after_node_restart(Config, Type) ->
    ok = rabbit_ct_broker_helpers:add_vhost(Config, ?VHOST),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, ?VHOST),
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 2, ?VHOST),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    Declare = #'queue.declare'{queue = Type, durable = true,
                               arguments = [{<<"x-queue-type">>, longstr, Type}]},
    #'queue.declare_ok'{} = amqp_channel:call(Ch, Declare),
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:cast(Ch, #'basic.publish'{routing_key = Type},
                     #amqp_msg{props = #'P_basic'{delivery_mode = 2}, payload = <<"old">>}),
    true = amqp_channel:wait_for_confirms(Ch),
    OldPath = rpc(Config, 2, rabbit_vhost, msg_store_dir_path, [?VHOST]),
    ok = amqp_channel:close(Ch),
    ok = amqp_connection:close(Conn),
    ok = rabbit_ct_broker_helpers:stop_node(Config, 2),
    try
        ok = rabbit_ct_broker_helpers:delete_vhost(Config, ?VHOST),
        ok = rabbit_ct_broker_helpers:add_vhost(Config, ?VHOST),
        ok = rabbit_ct_broker_helpers:set_full_permissions(Config, ?VHOST)
    after
        ok = rabbit_ct_broker_helpers:start_node(Config, 2)
    end,
    ?assertNotEqual(OldPath, rpc(Config, 2, rabbit_vhost, msg_store_dir_path, [?VHOST])),
    FreshConn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 2, ?VHOST),
    {ok, FreshCh} = amqp_connection:open_channel(FreshConn),
    ?assertMatch(#'queue.declare_ok'{message_count = 0}, amqp_channel:call(FreshCh, Declare)),
    {ok, Q} = rpc(Config, 2, rabbit_amqqueue, lookup, [rabbit_misc:r(?VHOST, queue, Type)]),
    ?assertEqual({ok, 0, 0}, rpc(Config, 2, rabbit_queue_type, stat, [Q])),
    ok = amqp_channel:close(FreshCh),
    ok = amqp_connection:close(FreshConn).

deletion_calls_the_handler(Config) ->
    ok = install_handler(Config, 0),
    ok = rabbit_ct_broker_helpers:add_vhost(Config, ?VHOST),
    {ok, Id} = rpc(Config, 0, rabbit_vhost, id, [?VHOST]),
    ok = rabbit_ct_broker_helpers:delete_vhost(Config, ?VHOST),
    Node = node_name(Config, 0),
    receive {vhost_deleted, Node, ?VHOST, Id} -> ok
    after ?TIMEOUT -> ct:fail(handler_not_called)
    end.

leftovers_are_listed_with_ids(Config) ->
    ok = install_handler(Config, 0),
    ok = rabbit_ct_broker_helpers:add_vhost(Config, ?VHOST),
    {ok, Id} = rpc(Config, 0, rabbit_vhost, id, [?VHOST]),
    ok = rpc(Config, 0, rabbit_vhost_deletion, remove_leftovers, [[?APP]]),
    Node = node_name(Config, 0),
    receive {remove_leftovers, Node, Live} ->
                ?assertEqual(Id, maps:get(?VHOST, Live)),
                ?assert(is_integer(maps:get(<<"/">>, Live)))
    after ?TIMEOUT -> ct:fail(handler_not_called)
    end.

a_recreated_vhost_restarts_its_supervisor(Config) ->
    ok = install_handler(Config, 0),
    ok = rabbit_ct_broker_helpers:add_vhost(Config, ?VHOST),
    {ok, Old} = rpc(Config, 0, rabbit_vhost, id, [?VHOST]),
    {ok, Sup} = rpc(Config, 0, rabbit_vhost_sup_sup, get_vhost_sup, [?VHOST]),
    VHost = rpc(Config, 0, rabbit_db_vhost, get, [?VHOST]),
    New = Old + 1,
    %% What a node that was partitioned away while the vhost was deleted and
    %% recreated sees once it is back.
    ok = rpc(Config, 0, rabbit_khepri, put,
             [rpc(Config, 0, rabbit_db_vhost, khepri_vhost_path, [?VHOST]),
              vhost:new(?VHOST, vhost:get_limits(VHost), vhost:get_metadata(VHost), New)]),
    ?assertNotEqual({ok, Sup}, rpc(Config, 0, rabbit_vhost_sup_sup, get_vhost_sup, [?VHOST])),
    case rpc(Config, 0, rabbit_vhost_sup_sup, start_vhost, [?VHOST]) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end,
    Node = node_name(Config, 0),
    receive {vhost_deleted, Node, ?VHOST, Old} -> ok
    after ?TIMEOUT -> ct:fail(handler_not_called)
    end,
    rabbit_ct_helpers:await_condition(
      fun() -> rpc(Config, 0, rabbit_vhost_sup_sup, id, [?VHOST]) =:= New end, ?TIMEOUT),
    ?assertNotEqual({ok, Sup}, rpc(Config, 0, rabbit_vhost_sup_sup, get_vhost_sup, [?VHOST])).

an_unidentified_supervisor_is_restarted(Config) ->
    ok = rabbit_ct_broker_helpers:add_vhost(Config, ?VHOST),
    {ok, Id} = rpc(Config, 0, rabbit_vhost, id, [?VHOST]),
    {ok, Sup} = rpc(Config, 0, rabbit_vhost_sup_sup, get_vhost_sup, [?VHOST]),
    Record = rpc(Config, 0, rabbit_vhost_sup_sup, lookup_vhost_sup_record, [?VHOST]),
    Process = element(5, Record),
    Path = filename:join(rpc(Config, 0, rabbit_vhost, msg_store_dir_path, [?VHOST]), "incarnation-test"),
    ok = rpc(Config, 0, file, write_file, [Path, <<"kept">>]),
    true = rpc(Config, 0, ets, insert, [rabbit_vhost_sup_sup, setelement(tuple_size(Record), Record, undefined)]),
    check_vhost = rpc(Config, 0, erlang, send, [Process, check_vhost]),
    rabbit_ct_helpers:await_condition(
      fun() -> rpc(Config, 0, rabbit_vhost_sup_sup, id, [?VHOST]) =:= Id end, ?TIMEOUT),
    {ok, NewSup} = rpc(Config, 0, rabbit_vhost_sup_sup, get_vhost_sup, [?VHOST]),
    ?assertNotEqual(Sup, NewSup),
    ?assertEqual({ok, <<"kept">>}, rpc(Config, 0, file, read_file, [Path])),
    ok = rpc(Config, 0, rabbit_vhost_sup_sup, restart_vhost, [?VHOST, Process]),
    ?assertEqual({ok, NewSup}, rpc(Config, 0, rabbit_vhost_sup_sup, get_vhost_sup, [?VHOST])).

stale_deletion_keeps_the_replacement(Config) ->
    ok = rabbit_ct_broker_helpers:add_vhost(Config, ?VHOST),
    {ok, Old} = rpc(Config, 0, rabbit_vhost, id, [?VHOST]),
    ok = rabbit_ct_broker_helpers:delete_vhost(Config, ?VHOST),
    ok = rabbit_ct_broker_helpers:add_vhost(Config, ?VHOST),
    {ok, Sup} = rpc(Config, 0, rabbit_vhost_sup_sup, get_vhost_sup, [?VHOST]),
    ok = rpc(Config, 0, rabbit_vhost_sup_sup, stop_and_delete_vhost, [?VHOST, Old]),
    ?assertEqual({ok, Sup}, rpc(Config, 0, rabbit_vhost_sup_sup, get_vhost_sup, [?VHOST])),
    {ok, Current} = rpc(Config, 0, rabbit_vhost, id, [?VHOST]),
    ok = rpc(Config, 0, rabbit_vhost_sup_sup, stop_and_delete_vhost, [?VHOST, Current]),
    ?assertEqual({ok, Sup}, rpc(Config, 0, rabbit_vhost_sup_sup, get_vhost_sup, [?VHOST])).

metadata_failure_refuses_supervisor_lookup(Config) ->
    ok = rabbit_ct_broker_helpers:add_vhost(Config, ?VHOST),
    {ok, Id} = rpc(Config, 0, rabbit_vhost, id, [?VHOST]),
    ok = rpc(Config, 0, meck, new, [rabbit_db_vhost, [passthrough, no_link]]),
    try
        ok = rpc(Config, 0, meck, expect, [rabbit_db_vhost, get, 1, undefined]),
        ok = rpc(Config, 0, meck, expect, [rabbit_db_vhost, identity, 1, {error, timeout}]),
        Error = {error, {no_such_vhost, ?VHOST}},
        ?assertEqual(Error, rpc(Config, 0, rabbit_vhost_sup_sup, get_vhost_sup, [?VHOST])),
        ?assertEqual(Error, rpc(Config, 0, rabbit_amqqueue_sup_sup, find_for_vhost, [?VHOST])),
        ?assertEqual(Error, rpc(Config, 0, rabbit_amqqueue_sup_sup, start_for_vhost, [?VHOST])),
        ?assertEqual(Error, rpc(Config, 0, rabbit_vhost_msg_store, start,
                               [?VHOST, msg_store_transient, undefined, undefined])),
        ?assertEqual({error, timeout}, rpc(Config, 0, rabbit_vhost_sup_wrapper, start_vhost_sup,
                                          [?VHOST, Id]))
    after
        ok = rpc(Config, 0, meck, unload, [rabbit_db_vhost])
    end,
    ?assertMatch({ok, _}, rpc(Config, 0, rabbit_vhost_sup_sup, get_vhost_sup, [?VHOST])).

metadata_recovery_retries_vhost_start(Config) ->
    {new, _} = rpc(Config, 0, rabbit_db_vhost, create_or_get, [?VHOST, [], #{}]),
    ok = rpc(Config, 0, meck, new, [rabbit_db_vhost, [passthrough, no_link]]),
    try
        ok = rpc(Config, 0, meck, expect, [rabbit_db_vhost, identity, 1, {error, timeout}]),
        ok = rpc(Config, 0, rabbit_vhost_sup_sup, init_vhost, [?VHOST]),
        ?assertMatch({error, _}, rpc(Config, 0, rabbit_vhost_sup_sup, get_vhost_sup, [?VHOST]))
    after
        ok = rpc(Config, 0, meck, unload, [rabbit_db_vhost])
    end,
    rabbit_ct_helpers:await_condition(fun() ->
        case rpc(Config, 0, rabbit_vhost_sup_sup, get_vhost_sup, [?VHOST]) of
            {ok, _} -> true;
            _ -> false
        end
    end, 15000).

uncertain_deletion_keeps_the_supervisor(Config) ->
    ok = rabbit_ct_broker_helpers:add_vhost(Config, ?VHOST),
    {ok, Id} = rpc(Config, 0, rabbit_vhost, id, [?VHOST]),
    Record = rpc(Config, 0, rabbit_vhost_sup_sup, lookup_vhost_sup_record, [?VHOST]),
    Path = filename:join(rpc(Config, 0, rabbit_vhost, msg_store_dir_path, [?VHOST]), "kept"),
    ok = rpc(Config, 0, file, write_file, [Path, <<"data">>]),
    ok = rpc(Config, 0, meck, new, [rabbit_db_vhost, [passthrough, no_link]]),
    try
        [begin
             ok = rpc(Config, 0, meck, expect, [rabbit_db_vhost, get_id_for_deletion, 1, Result]),
             _ = rpc(Config, 0, rabbit_vhost_sup_sup, stop_and_delete_vhost, [?VHOST, Id]),
             _ = rpc(Config, 0, rabbit_vhost_sup_sup, restart_vhost, [?VHOST, element(5, Record)]),
             ?assertEqual(Record, rpc(Config, 0, rabbit_vhost_sup_sup, lookup_vhost_sup_record, [?VHOST])),
             ?assertEqual(true, rpc(Config, 0, erlang, is_process_alive, [element(3, Record)])),
             ?assertEqual({ok, <<"data">>}, rpc(Config, 0, file, read_file, [Path]))
         end || Result <- [{error, timeout}, {error, noproc}, {ok, undefined}, {ok, Id - 1}]]
    after
        ok = rpc(Config, 0, meck, unload, [rabbit_db_vhost])
    end.

cleanup_rechecks_candidates(Config) ->
    ok = rabbit_ct_broker_helpers:add_vhost(Config, ?VHOST),
    {ok, Id} = rpc(Config, 0, rabbit_vhost, id, [?VHOST]),
    ?assertEqual({ok, []}, rpc(Config, 0, rabbit_vhost_deletion, deleted_vhosts, [[{?VHOST, Id}]])),
    ok = rabbit_ct_broker_helpers:delete_vhost(Config, ?VHOST),
    ?assertEqual({ok, [{?VHOST, Id}]}, rpc(Config, 0, rabbit_vhost_deletion, deleted_vhosts,
                                         [[{?VHOST, Id}, {<<"unknown">>, undefined}]])),
    ok = rabbit_ct_broker_helpers:add_vhost(Config, ?VHOST),
    {ok, New} = rpc(Config, 0, rabbit_vhost, id, [?VHOST]),
    ?assertEqual({ok, [{?VHOST, Id}]}, rpc(Config, 0, rabbit_vhost_deletion, deleted_vhosts,
                                         [[{?VHOST, Id}, {?VHOST, New}, {?VHOST, New + 1}]])).

minority_cannot_authorize_cleanup(Config) ->
    ok = rabbit_ct_broker_helpers:add_vhost(Config, ?VHOST),
    {ok, Id} = rpc(Config, 0, rabbit_vhost, id, [?VHOST]),
    {rabbitmq_metadata, Leader} = rpc(Config, 0, ra_leaderboard, lookup_leader, [rabbitmq_metadata]),
    Peers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename) -- [Leader],
    Record = rabbit_ct_broker_helpers:rpc(Config, Leader, rabbit_vhost_sup_sup,
                                         lookup_vhost_sup_record, [?VHOST]),
    try
        [rabbit_ct_broker_helpers:block_traffic_between(Leader, Peer) || Peer <- Peers],
        rabbit_ct_helpers:await_condition(fun() ->
            Connected = rabbit_ct_broker_helpers:rpc(Config, Leader, erlang, nodes, []),
            is_list(Connected) andalso not lists:any(fun(Peer) -> lists:member(Peer, Connected) end, Peers)
        end, ?TIMEOUT),
        ?assertMatch({error, _}, rabbit_ct_broker_helpers:rpc(Config, Leader,
                       rabbit_db_vhost, get_id_for_deletion, [?VHOST])),
        ?assertMatch({error, _}, rabbit_ct_broker_helpers:rpc(Config, Leader,
                       rabbit_vhost_deletion, deleted_vhosts, [[{?VHOST, Id}, {<<"absent">>, Id}]])),
        ?assertMatch({error, _}, rabbit_ct_broker_helpers:rpc(Config, Leader,
                       rabbit_vhost_sup_sup, stop_and_delete_vhost, [?VHOST, Id])),
        ?assertEqual(Record, rabbit_ct_broker_helpers:rpc(Config, Leader,
                               rabbit_vhost_sup_sup, lookup_vhost_sup_record, [?VHOST])),
        ?assertEqual(true, rabbit_ct_broker_helpers:rpc(Config, Leader,
                             erlang, is_process_alive, [element(3, Record)]))
    after
        [rabbit_ct_broker_helpers:allow_traffic_between(Leader, Peer) || Peer <- Peers]
    end,
    rabbit_ct_helpers:await_condition(fun() ->
        case rabbit_ct_broker_helpers:rpc(Config, Leader, rabbit_db_vhost, list_ids, []) of
            {ok, _} -> true;
            _ -> false
        end
    end, ?TIMEOUT).

deletion_calls_the_handler_on_every_node(Config) ->
    [ok = install_handler(Config, N) || N <- [0, 1, 2]],
    ok = rabbit_ct_broker_helpers:add_vhost(Config, ?VHOST),
    {ok, Id} = rpc(Config, 0, rabbit_vhost, id, [?VHOST]),
    ok = rabbit_ct_broker_helpers:delete_vhost(Config, ?VHOST),
    [receive {vhost_deleted, Node, ?VHOST, Id} -> ok
     after ?TIMEOUT -> ct:fail({handler_not_called, Node})
     end || Node <- [node_name(Config, N) || N <- [0, 1, 2]]].

deletion_does_not_wait_for_a_stopped_node(Config) ->
    ok = rabbit_ct_broker_helpers:add_vhost(Config, ?VHOST),
    {ok, Old} = rpc(Config, 0, rabbit_vhost, id, [?VHOST]),
    ok = rabbit_ct_broker_helpers:stop_node(Config, 2),
    New = try
              ok = rabbit_ct_broker_helpers:delete_vhost(Config, ?VHOST),
              ok = rabbit_ct_broker_helpers:add_vhost(Config, ?VHOST),
              {ok, Id} = rpc(Config, 0, rabbit_vhost, id, [?VHOST]),
              Id
          after
              ok = rabbit_ct_broker_helpers:start_node(Config, 2)
          end,
    ?assert(New > Old),
    rabbit_ct_helpers:await_condition(
      fun() -> rpc(Config, 2, rabbit_vhost_sup_sup, id, [?VHOST]) =:= New end, ?TIMEOUT),
    ok = install_handler(Config, 2),
    ok = rpc(Config, 2, rabbit_vhost_deletion, remove_leftovers, [[?APP]]),
    Node = node_name(Config, 2),
    receive {remove_leftovers, Node, #{?VHOST := New}} -> ok
    after ?TIMEOUT -> ct:fail(handler_not_called)
    end.

%% -------------------------------------------------------------------
%% The handler, loaded on the nodes as an application of its own.
%% -------------------------------------------------------------------

vhost_deleted(VHost, Id) ->
    notify({vhost_deleted, node(), VHost, Id}).

remove_leftovers(Live) ->
    notify({remove_leftovers, node(), Live}).

notify(Msg) ->
    case application:get_env(?APP, test_process) of
        {ok, Pid} -> Pid ! Msg, ok;
        undefined -> ok
    end.

install_handler(Config, Node) ->
    {Mod, Binary, File} = code:get_object_code(?MODULE),
    {module, Mod} = rpc(Config, Node, code, load_binary, [Mod, File, Binary]),
    Spec = {application, ?APP, [{description, "vhost deletion test handler"},
                                {vsn, "1"},
                                {modules, [?MODULE]},
                                {registered, []},
                                {applications, [kernel, stdlib]}]},
    case rpc(Config, Node, application, load, [Spec]) of
        ok -> ok;
        {error, {already_loaded, _}} -> ok
    end,
    ok = rpc(Config, Node, application, set_env, [?APP, test_process, self()]),
    case rpc(Config, Node, application, start, [?APP]) of
        ok -> ok;
        {error, {already_started, ?APP}} -> ok
    end.

%% -------------------------------------------------------------------
%% Helpers.
%% -------------------------------------------------------------------

info(Config, Key) ->
    [Info] = [I || I <- rpc(Config, 0, rabbit_vhost, info_all, []),
                   proplists:get_value(name, I) =:= ?VHOST],
    proplists:get_value(Key, Info).

node_name(Config, N) ->
    rabbit_ct_broker_helpers:get_node_config(Config, N, nodename).

rpc(Config, N, M, F, A) ->
    rabbit_ct_broker_helpers:rpc(Config, N, M, F, A).
