%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(quorum_queue_vhost_cleanup_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("khepri/include/khepri.hrl").

all() -> [{group, local}, {group, cluster}].
groups() ->
    [{local, [], [recreation_keeps_replacement, running_and_legacy_members_are_kept,
                  obsolete_running_member_is_stopped,
                  missing_member_is_kept,
                  member_starting_during_cleanup_is_kept, invalid_member_uid_is_kept,
                  metadata_failure_keeps_data, candidates_precede_metadata,
                  uncertain_config_is_kept, unidentified_config_is_kept]},
     {cluster, [], [deleted_member_removed_on_restart, recreated_member_removed_on_restart,
                    live_members_survive_restart, stopped_live_member_is_kept, live_members_survive_peer_reset,
                    standalone_reset_keeps_data, partitioned_member_cannot_join_recreated_vhost]}].
suite() -> [{timetrap, {minutes, 5}}].

init_per_suite(Config) ->
    rabbit_ct_helpers:run_setup_steps(Config, [fun rabbit_ct_broker_helpers:configure_dist_proxy/1]).
end_per_suite(Config) -> rabbit_ct_helpers:run_teardown_steps(Config).
init_per_group(local, Config) -> [{local, true} | Config];
init_per_group(cluster, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [{rmq_nodes_count, 3}, {net_ticktime, 5},
                                                   {rmq_nodename_suffix, ?MODULE}]),
    Config2 = rabbit_ct_helpers:merge_app_env(Config1,
                  [{rabbit, [{quorum_membership_reconciliation_enabled, false}]}]),
    rabbit_ct_helpers:run_steps(Config2, rabbit_ct_broker_helpers:setup_steps()).
end_per_group(local, Config) -> Config;
end_per_group(cluster, Config) ->
    rabbit_ct_helpers:run_steps(Config, rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Test, Config) ->
    case proplists:get_value(local, Config, false) of
        false -> Config;
        true ->
            Dir = filename:join(proplists:get_value(priv_dir, Config), atom_to_list(Test)),
            ok = filelib:ensure_path(Dir),
            [ok = meck:new(M, [non_strict]) || M <- [ra_system, ra_directory, rabbit_khepri]],
            ok = meck:expect(ra_system, fetch, 1, #{data_dir => Dir}),
            ok = meck:expect(ra_directory, where_is, 2, undefined),
            ok = meck:expect(ra_directory, where_is_parent, 2, undefined),
            [{data_dir, Dir} | Config]
    end.
end_per_testcase(_, Config) ->
    case proplists:get_value(local, Config, false) of
        false -> ok;
        true -> [meck:unload(M) || M <- [ra_system, ra_directory, rabbit_khepri]]
    end.

recreation_keeps_replacement(Config) ->
    Old = candidate(Config, <<"old">>),
    New = candidate(Config, <<"new">>),
    Q = queue(#{node() => <<"new">>}),
    ok = meck:expect(rabbit_khepri, transaction, 3, {ok, Q}),
    ok = rabbit_quorum_queue:vhost_deleted(<<"cleanup">>, 1),
    ?assertNot(filelib:is_dir(Old)),
    ?assert(filelib:is_dir(New)),
    ok = rabbit_quorum_queue:remove_leftovers(#{}),
    ?assert(filelib:is_dir(New)).

running_and_legacy_members_are_kept(Config) ->
    Dir = candidate(Config, <<"member">>),
    ok = meck:expect(rabbit_khepri, transaction, 3, {ok, queue(#{node() => <<"member">>})}),
    ok = meck:expect(ra_directory, where_is, 2, self()),
    ok = rabbit_quorum_queue:remove_leftovers(#{}),
    ?assert(filelib:is_dir(Dir)),
    {Dead, Ref} = spawn_monitor(fun() -> ok end),
    receive {'DOWN', Ref, process, Dead, normal} -> ok end,
    ok = meck:expect(ra_directory, where_is, 2, Dead),
    ok = rabbit_quorum_queue:remove_leftovers(#{}),
    ?assert(filelib:is_dir(Dir)),
    ok = meck:expect(ra_directory, where_is, 2, undefined),
    ok = meck:expect(ra_directory, where_is_parent, 2, self()),
    ok = rabbit_quorum_queue:remove_leftovers(#{}),
    ?assert(filelib:is_dir(Dir)),
    ok = meck:expect(ra_directory, where_is_parent, 2, undefined),
    ok = meck:expect(rabbit_khepri, transaction, 3, {ok, queue([node()])}),
    ok = rabbit_quorum_queue:remove_leftovers(#{}),
    ?assert(filelib:is_dir(Dir)).

obsolete_running_member_is_stopped(Config) ->
    Dir = candidate(Config, <<"old">>),
    {ok, Sup} = supervisor:start_link(?MODULE, []),
    {ok, Parent} = supervisor:start_child(Sup, []),
    ok = meck:expect(ra_system, fetch, 1,
                    #{data_dir => proplists:get_value(data_dir, Config), names => #{server_sup => Sup}}),
    ok = meck:expect(ra_directory, where_is_parent, 2, Parent),
    try
        ok = meck:expect(rabbit_khepri, transaction, 3, {error, timeout}),
        ok = rabbit_quorum_queue:remove_leftovers(#{}),
        ?assert(is_process_alive(Parent)),
        ok = meck:expect(rabbit_khepri, transaction, 3, {error, ?khepri_error(node_not_found, #{})}),
        ok = rabbit_quorum_queue:remove_leftovers(#{}),
        ?assertNot(is_process_alive(Parent)),
        ?assert(filelib:is_dir(Dir))
    after
        gen_server:stop(Sup)
    end.

init([]) ->
    {ok, {{simple_one_for_one, 0, 1},
          [{member, {?MODULE, start_test_member, []}, temporary, 5000, worker, [?MODULE]}]}}.

start_test_member() ->
    {ok, spawn_link(fun() -> receive stop -> ok end end)}.

missing_member_is_kept(Config) ->
    Dir = candidate(Config, <<"member">>),
    ok = meck:expect(rabbit_khepri, transaction, 3, {ok, queue(#{other@node => <<"other">>})}),
    ok = rabbit_quorum_queue:remove_leftovers(#{}),
    ?assert(filelib:is_dir(Dir)).

metadata_failure_keeps_data(Config) ->
    Dir = candidate(Config, <<"member">>),
    ok = meck:expect(rabbit_khepri, transaction, 3, {error, timeout}),
    ok = rabbit_quorum_queue:remove_leftovers(#{}),
    ?assert(filelib:is_dir(Dir)),
    ok = meck:expect(rabbit_khepri, transaction, fun(_, rw, #{timeout := 5000}) -> throw({error, timeout}) end),
    ok = rabbit_quorum_queue:remove_leftovers(#{}),
    ?assert(filelib:is_dir(Dir)).

member_starting_during_cleanup_is_kept(Config) ->
    Dir = candidate(Config, <<"member">>),
    ok = meck:expect(rabbit_khepri, transaction, fun(_, rw, #{timeout := 5000}) ->
        ok = meck:expect(ra_directory, where_is, 2, self()),
        {error, ?khepri_error(node_not_found, #{})}
    end),
    ok = rabbit_quorum_queue:remove_leftovers(#{}),
    ?assert(filelib:is_dir(Dir)).

invalid_member_uid_is_kept(Config) ->
    Dir = candidate(Config, <<"member">>),
    ok = meck:expect(rabbit_khepri, transaction, 3, {ok, queue(#{node() => undefined})}),
    ok = rabbit_quorum_queue:remove_leftovers(#{}),
    ?assert(filelib:is_dir(Dir)).

candidates_precede_metadata(Config) ->
    Old = candidate(Config, <<"old">>),
    ok = meck:expect(rabbit_khepri, transaction, fun(_, rw, #{timeout := 5000}) ->
        _ = candidate(Config, <<"new">>),
        {error, ?khepri_error(node_not_found, #{})}
    end),
    ok = rabbit_quorum_queue:remove_leftovers(#{}),
    ?assertNot(filelib:is_dir(Old)),
    ?assert(filelib:is_dir(filename:join(proplists:get_value(data_dir, Config), "new"))).

uncertain_config_is_kept(Config) ->
    Dir = candidate(Config, <<"unknown">>),
    File = filename:join(Dir, "config"),
    [begin
         ok = file:write_file(File, Data),
         ok = rabbit_quorum_queue:remove_leftovers(#{}),
         ?assert(filelib:is_file(File))
     end || Data <- [<<"incomplete #{">>,
                     io_lib:format("~p.~n", [#{uid => <<"different">>, id => {cleanup_queue, node()},
                                              machine => {module, rabbit_fifo, #{queue_resource => resource()}}}]),
                     io_lib:format("~p.~n", [#{uid => <<"unknown">>, id => {cleanup_queue, node()},
                                              machine => {module, other_machine, #{}}}])]].

unidentified_config_is_kept(Config) ->
    Dir = candidate(Config, <<"legacy">>),
    File = filename:join(Dir, "config"),
    {ok, [Conf]} = file:consult(File),
    ok = file:write_file(File, io_lib:format("~p.~n", [maps:remove(vhost_id, Conf)])),
    ok = rabbit_quorum_queue:remove_leftovers(#{}),
    ?assert(filelib:is_file(File)),
    ?assertNot(meck:called(rabbit_khepri, transaction, '_')).

candidate(Config, UId) ->
    Dir = filename:join(proplists:get_value(data_dir, Config), binary_to_list(UId)),
    ok = filelib:ensure_path(Dir),
    Data = #{uid => UId, id => {cleanup_queue, node()}, vhost_id => 1,
             machine => {module, rabbit_fifo, #{queue_resource => resource()}}},
    ok = file:write_file(filename:join(Dir, "config"), io_lib:format("~p.~n", [Data])),
    Dir.

resource() -> rabbit_misc:r(<<"cleanup">>, queue, <<"queue">>).
queue(Nodes) ->
    Q = amqqueue:new(resource(), {cleanup_queue, node()}, true, false, none,
                     [], <<"cleanup">>, #{}, rabbit_quorum_queue),
    amqqueue:set_type_state(Q, #{nodes => Nodes}).

deleted_member_removed_on_restart(Config) -> restart_cleanup(Config, false).
recreated_member_removed_on_restart(Config) -> restart_cleanup(Config, true).

live_members_survive_restart(Config) -> live_members_survive(Config, restart).
live_members_survive_peer_reset(Config) -> live_members_survive(Config, reset).
stopped_live_member_is_kept(Config) -> live_members_survive(Config, stopped).
standalone_reset_keeps_data(Config) -> live_members_survive(Config, standalone).

live_members_survive(Config, Action) ->
    VHost = atom_to_binary(Action),
    Name = <<"kept">>,
    Resource = rabbit_misc:r(VHost, queue, Name),
    ok = rabbit_ct_broker_helpers:add_vhost(Config, VHost),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, VHost),
    {Conn, Ch} = declare(Config, VHost, Name),
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    ok = amqp_channel:cast(Ch, #'basic.publish'{routing_key = Name},
                          #amqp_msg{payload = <<"kept">>}),
    true = amqp_channel:wait_for_confirms(Ch),
    {ok, Q} = rpc(Config, 0, rabbit_amqqueue, lookup, [Resource]),
    #{nodes := Nodes} = amqqueue:get_type_state(Q),
    Dirs = [{Node, rpc(Config, Node, ra_env, server_data_dir, [quorum_queues, UId])}
            || {Node, UId} <- maps:to_list(Nodes)],
    ok = amqp_channel:close(Ch),
    ok = amqp_connection:close(Conn),
    case Action of
        stopped ->
            Node = rabbit_ct_broker_helpers:get_node_config(Config, 2, nodename),
            {RaName, _} = amqqueue:get_pid(Q),
            UId = maps:get(Node, Nodes),
            {Node, Dir} = lists:keyfind(Node, 1, Dirs),
            ok = rpc(Config, 2, rabbit_quorum_queue, stop_server, [{RaName, Node}]),
            ?assertEqual(undefined, rpc(Config, 2, erlang, whereis, [RaName])),
            ok = rpc(Config, 2, ?MODULE, hide_stopped_registration, [UId]),
            try
                ok = rpc(Config, 2, rabbit_quorum_queue, remove_leftovers, [#{}]),
                ?assert(filelib:is_dir(Dir))
            after
                ok = rpc(Config, 2, meck, unload, [ra_directory]),
                ok = rpc(Config, 2, rabbit_quorum_queue, restart_server, [{RaName, Node}])
            end;
        restart ->
            ok = rabbit_ct_broker_helpers:stop_node(Config, 2),
            [ok = rpc(Config, N, rabbit_quorum_queue, remove_leftovers, [#{}]) || N <- [0, 1]],
            ok = rabbit_ct_broker_helpers:start_node(Config, 2);
        reset ->
            {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 2, ["stop_app"]),
            {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 2, ["reset"]),
            Seed = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
            {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 2, ["join_cluster", atom_to_list(Seed)]),
            {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 2, ["start_app"]);
        standalone ->
            Node = rabbit_ct_broker_helpers:get_node_config(Config, 2, nodename),
            {Node, Dir} = lists:keyfind(Node, 1, Dirs),
            {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 2, ["stop_app"]),
            {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 2, ["reset"]),
            try
                {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 2, ["start_app"]),
                ok = rpc(Config, 2, rabbit_quorum_queue, remove_leftovers, [#{}]),
                ?assert(filelib:is_dir(Dir)),
                {RaName, _} = amqqueue:get_pid(Q),
                ?assertEqual(undefined, rpc(Config, 2, erlang, whereis, [RaName])),
                ok = rpc(Config, 2, rabbit_vhost, add, [VHost, <<"guest">>]),
                ok = rpc(Config, 2, rabbit_auth_backend_internal, set_permissions,
                         [<<"guest">>, VHost, <<".*">>, <<".*">>, <<".*">>, <<"guest">>]),
                FreshConn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 2, VHost),
                {ok, FreshCh} = amqp_connection:open_channel(FreshConn),
                #'queue.declare_ok'{} = amqp_channel:call(FreshCh,
                    #'queue.declare'{queue = Name, durable = true,
                        arguments = [{<<"x-queue-type">>, longstr, <<"quorum">>}]}),
                ?assertMatch(#'basic.get_empty'{}, amqp_channel:call(FreshCh,
                    #'basic.get'{queue = Name, no_ack = true})),
                ?assert(filelib:is_dir(Dir)),
                ok = amqp_channel:close(FreshCh),
                ok = amqp_connection:close(FreshConn)
            after
                {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 2, ["stop_app"]),
                Seed = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
                {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 2, ["join_cluster", atom_to_list(Seed)]),
                {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 2, ["start_app"])
            end
    end,
    [ok = rpc(Config, N, rabbit_quorum_queue, remove_leftovers, [#{}]) || N <- [0, 1, 2]],
    ResetNode = rabbit_ct_broker_helpers:get_node_config(Config, 2, nodename),
    [?assert(filelib:is_dir(Dir)) || {Node, Dir} <- Dirs, Action =:= restart orelse Node =/= ResetNode],
    {LastConn, LastCh} = declare(Config, VHost, Name),
    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = <<"kept">>}},
                 amqp_channel:call(LastCh, #'basic.get'{queue = Name, no_ack = true})),
    ok = amqp_channel:close(LastCh),
    ok = amqp_connection:close(LastConn),
    ok = rabbit_ct_broker_helpers:delete_vhost(Config, VHost).

hide_stopped_registration(UId) ->
    ok = meck:new(ra_directory, [passthrough, no_link]),
    [ok = meck:expect(ra_directory, Fun,
        fun(quorum_queues, Candidate) when Candidate =:= UId -> undefined;
           (System, Candidate) -> meck:passthrough([System, Candidate])
        end) || Fun <- [where_is, where_is_parent]],
    ok.

restart_cleanup(Config, Recreate) ->
    VHost = <<"cleanup-integration">>,
    Name = <<"queue">>,
    Resource = rabbit_misc:r(VHost, queue, Name),
    ok = rabbit_ct_broker_helpers:add_vhost(Config, VHost),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, VHost),
    {Conn, Ch} = declare(Config, VHost, Name),
    {ok, OldId} = rpc(Config, 0, rabbit_vhost, id, [VHost]),
    {ok, Q} = rpc(Config, 0, rabbit_amqqueue, lookup, [Resource]),
    {RaName, _} = amqqueue:get_pid(Q),
    UId = rpc(Config, 2, ra_directory, uid_of, [quorum_queues, RaName]),
    ?assert(is_binary(UId)),
    Dir = rpc(Config, 2, ra_env, server_data_dir, [quorum_queues, UId]),
    ?assert(filelib:is_dir(Dir)),
    ok = amqp_channel:close(Ch),
    ok = amqp_connection:close(Conn),
    ok = rabbit_ct_broker_helpers:stop_node(Config, 2),
    try
        ok = rabbit_ct_broker_helpers:delete_vhost(Config, VHost),
        ?assert(filelib:is_dir(Dir)),
        case Recreate of
            false -> ok;
            true ->
                ok = rabbit_ct_broker_helpers:add_vhost(Config, VHost),
                ok = rabbit_ct_broker_helpers:set_full_permissions(Config, VHost),
                {NewConn, NewCh} = declare(Config, VHost, Name, 2),
                #'confirm.select_ok'{} = amqp_channel:call(NewCh, #'confirm.select'{}),
                ok = amqp_channel:cast(NewCh, #'basic.publish'{routing_key = Name},
                                      #amqp_msg{payload = <<"replacement">>}),
                true = amqp_channel:wait_for_confirms(NewCh),
                ok = amqp_channel:close(NewCh),
                ok = amqp_connection:close(NewConn)
        end
    after
        ok = rabbit_ct_broker_helpers:start_node(Config, 2)
    end,
    case Recreate of
        false -> rabbit_ct_helpers:await_condition(fun() -> not filelib:is_dir(Dir) end, 30000);
        true ->
            Node = rabbit_ct_broker_helpers:get_node_config(Config, 2, nodename),
            {ok, BeforeAdd} = rpc(Config, 0, rabbit_amqqueue, lookup, [Resource]),
            #{nodes := BeforeNodes} = amqqueue:get_type_state(BeforeAdd),
            ?assertNot(maps:is_key(Node, BeforeNodes)),
            ok = rpc(Config, 2, rabbit_quorum_queue, remove_leftovers, [#{}]),
            ?assertNot(filelib:is_dir(Dir)),
            ok = rpc(Config, 0, rabbit_quorum_queue, add_member, [VHost, Name, Node, voter, 10000]),
            {ok, AfterAdd} = rpc(Config, 0, rabbit_amqqueue, lookup, [Resource]),
            #{nodes := #{Node := NewUId}} = amqqueue:get_type_state(AfterAdd),
            ?assertNotEqual(UId, NewUId),
            NewDir = rpc(Config, 2, ra_env, server_data_dir, [quorum_queues, NewUId]),
            ?assert(filelib:is_dir(NewDir)),
            [ok = rpc(Config, N, rabbit_quorum_queue, vhost_deleted, [VHost, OldId]) || N <- [0, 1, 2]],
            ?assertNot(filelib:is_dir(Dir)),
            ?assert(filelib:is_dir(NewDir)),
            {NewRaName, _} = amqqueue:get_pid(AfterAdd),
            ?assertNotEqual(RaName, NewRaName),
            ?assertEqual(NewUId, rpc(Config, 2, ra_directory, uid_of, [quorum_queues, NewRaName])),
            {LastConn, LastCh} = declare(Config, VHost, Name, 2),
            ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = <<"replacement">>}},
                         amqp_channel:call(LastCh, #'basic.get'{queue = Name, no_ack = true})),
            ok = amqp_channel:close(LastCh),
            ok = amqp_connection:close(LastConn),
            ok = rabbit_ct_broker_helpers:delete_vhost(Config, VHost)
    end.

partitioned_member_cannot_join_recreated_vhost(Config) ->
    VHost = <<"partitioned-cleanup">>,
    Name = <<"queue">>,
    Resource = rabbit_misc:r(VHost, queue, Name),
    ok = rabbit_ct_broker_helpers:add_vhost(Config, VHost),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, VHost),
    {Conn, Ch} = declare(Config, VHost, Name),
    publish_confirmed(Ch, Name, <<"old">>),
    {ok, Q} = rpc(Config, 0, rabbit_amqqueue, lookup, [Resource]),
    {OldRaName, _} = amqqueue:get_pid(Q),
    OldPid = rpc(Config, 2, erlang, whereis, [OldRaName]),
    ?assert(is_pid(OldPid)),
    ok = amqp_channel:close(Ch),
    ok = amqp_connection:close(Conn),
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 2, nodename),
    Peers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename) -- [Node],
    [rabbit_ct_broker_helpers:block_traffic_between(Node, Peer) || Peer <- Peers],
    try
        rabbit_ct_helpers:await_condition(fun() ->
            Connected = rpc(Config, 2, erlang, nodes, []),
            not lists:any(fun(Peer) -> lists:member(Peer, Connected) end, Peers)
        end, 10000),
        ok = rabbit_ct_broker_helpers:delete_vhost(Config, VHost),
        ?assertEqual(true, rpc(Config, 2, erlang, is_process_alive, [OldPid])),
        ok = rabbit_ct_broker_helpers:add_vhost(Config, VHost),
        ok = rabbit_ct_broker_helpers:set_full_permissions(Config, VHost),
        {NewConn, NewCh} = declare(Config, VHost, Name, 2),
        publish_confirmed(NewCh, Name, <<"new">>),
        {ok, NewQ} = rpc(Config, 0, rabbit_amqqueue, lookup, [Resource]),
        {NewRaName, _} = amqqueue:get_pid(NewQ),
        ?assertNotEqual(OldRaName, NewRaName),
        ok = amqp_channel:close(NewCh),
        ok = amqp_connection:close(NewConn)
    after
        [rabbit_ct_broker_helpers:allow_traffic_between(Node, Peer) || Peer <- Peers]
    end,
    rabbit_ct_helpers:await_condition(fun() ->
        lists:member(Node, rpc(Config, 0, rabbit_nodes, list_running, []))
    end, 30000),
    ok = rpc(Config, 0, rabbit_quorum_queue, add_member, [VHost, Name, Node, voter, 10000]),
    rabbit_ct_helpers:await_condition(fun() ->
        rpc(Config, 2, erlang, is_process_alive, [OldPid]) =:= false
    end, 30000),
    {LastConn, LastCh} = declare(Config, VHost, Name, 2),
    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = <<"new">>}},
                 amqp_channel:call(LastCh, #'basic.get'{queue = Name, no_ack = true})),
    ?assertMatch(#'basic.get_empty'{}, amqp_channel:call(LastCh, #'basic.get'{queue = Name, no_ack = true})),
    ok = amqp_channel:close(LastCh),
    ok = amqp_connection:close(LastConn),
    ok = rabbit_ct_broker_helpers:delete_vhost(Config, VHost).

publish_confirmed(Ch, Name, Payload) ->
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:cast(Ch, #'basic.publish'{routing_key = Name}, #amqp_msg{payload = Payload}),
    true = amqp_channel:wait_for_confirms(Ch),
    ok.

declare(Config, VHost, Name) -> declare(Config, VHost, Name, 3).

declare(Config, VHost, Name, Size) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0, VHost),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = Name, durable = true,
        arguments = [{<<"x-queue-type">>, longstr, <<"quorum">>},
                     {<<"x-quorum-initial-group-size">>, long, Size}]}),
    {Conn, Ch}.

rpc(Config, Node, Mod, Fun, Args) ->
    rabbit_ct_broker_helpers:rpc(Config, Node, Mod, Fun, Args).
