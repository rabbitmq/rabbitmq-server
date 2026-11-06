%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(resource_alarm_cluster_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(resource_alarm_SUITE, [with_connection/2, with_connection/4,
                              await_state/3, block_publisher/4,
                              send/5, await_receipt/3]).

suite() ->
    [{timetrap, {minutes, 5}}].

all() ->
    [{group, cluster}, {group, rolling_upgrade}].

groups() ->
    [{cluster, [], [remote_memory_alarm, remote_disk_alarm,
                    remote_overlapping_alarms, remote_existing_alarms,
                    remote_same_resource_alarms,
                    remote_heartbeats_started_after_blocking]},
     {rolling_upgrade, [], [rolling_upgrade]}].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    {ok, _} = application:ensure_all_started(gun),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(rolling_upgrade, Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            start_cluster(rolling_upgrade,
                          rabbit_ct_helpers:set_config(Config, {force_secondary, true}));
        false ->
            {skip, "Rolling upgrade requires SECONDARY_DIST or SECONDARY_UMBRELLA"}
    end;
init_per_group(cluster, Config) ->
    start_cluster(cluster, Config).

start_cluster(Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(
                Config, [{rmq_nodes_count, 3},
                         {rmq_nodename_suffix, atom_to_list(?MODULE) ++ "_" ++
                                               atom_to_list(Group)}]),
    rabbit_ct_helpers:run_setup_steps(
      Config1, rabbit_ct_broker_helpers:setup_steps()).

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_teardown_steps(
      Config, rabbit_ct_broker_helpers:teardown_steps()).

end_per_testcase(_, Config) ->
    [catch clear_alarm(Config, I, Source)
     || I <- [0, 1, 2], Source <- [memory, disk]],
    Config.

remote_memory_alarm(Config) ->
    remote_alarm(Config, memory).

remote_disk_alarm(Config) ->
    remote_alarm(Config, disk).

remote_alarm(Config, Source) ->
    log_versions(Config),
    with_connection(Config, [{"heart-beat", "10000,0"}], <<>>,
                    fun(Conn, Ref, Pid) ->
        set_alarm(Config, 1, Source),
        await_state(Config, Pid, blocking),
        clear_alarm(Config, 1, Source),
        await_state(Config, Pid, running),
        ?assertEqual({error, timeout}, gun:await(Conn, Ref, 200)),
        set_alarm(Config, 1, Source),
        await_state(Config, Pid, blocking),
        block_publisher(Config, Conn, Ref, Pid),
        clear_alarm(Config, 1, Source),
        await_state(Config, Pid, running),
        await_receipt(Conn, Ref, <<"pending">>)
    end).

remote_overlapping_alarms(Config) ->
    with_connection(Config, fun(Conn, Ref, Pid) ->
        set_alarm(Config, 1, memory),
        set_alarm(Config, 2, disk),
        clear_overlapping_alarms(Config, Conn, Ref, Pid)
    end).

remote_existing_alarms(Config) ->
    set_alarm(Config, 1, memory),
    set_alarm(Config, 2, disk),
    with_connection(Config, fun(Conn, Ref, Pid) ->
        clear_overlapping_alarms(Config, Conn, Ref, Pid)
    end).

clear_overlapping_alarms(Config, Conn, Ref, Pid) ->
    await_state(Config, Pid, blocking),
    block_publisher(Config, Conn, Ref, Pid),
    clear_alarm(Config, 1, memory),
    ?assertEqual({error, timeout}, gun:await(Conn, Ref, 200)),
    await_state(Config, Pid, blocked),
    clear_alarm(Config, 2, disk),
    await_state(Config, Pid, running),
    await_receipt(Conn, Ref, <<"pending">>).

remote_same_resource_alarms(Config) ->
    with_connection(Config, fun(Conn, Ref, Pid) ->
        set_alarm(Config, 1, memory),
        set_alarm(Config, 2, memory),
        await_state(Config, Pid, blocking),
        block_publisher(Config, Conn, Ref, Pid),
        clear_alarm(Config, 1, memory),
        ?assertEqual({error, timeout}, gun:await(Conn, Ref, 200)),
        await_state(Config, Pid, blocked),
        clear_alarm(Config, 2, memory),
        await_state(Config, Pid, running),
        await_receipt(Conn, Ref, <<"pending">>)
    end).

remote_heartbeats_started_after_blocking(Config) ->
    set_alarm(Config, 1, memory),
    Send = stomp:marshal("SEND", [{"destination", "/topic/resource-alarm"}]),
    with_connection(Config, [{"heart-beat", "1000,0"}], Send,
                    fun(Conn, Ref, Pid) ->
        await_state(Config, Pid, blocked),
        ?assertEqual({error, timeout}, gun:await(Conn, Ref, 3500)),
        clear_alarm(Config, 1, memory),
        await_state(Config, Pid, running),
        send(Conn, Ref, "SUBSCRIBE",
             [{"destination", "/topic/resource-alarm"}, {"id", "s0"},
              {"receipt", "subscribed"}], <<>>),
        await_receipt(Conn, Ref, <<"subscribed">>),
        ?assertEqual({ws, {close, 1000, <<"STOMP died">>}},
                     gun:await(Conn, Ref, 4000))
    end).

rolling_upgrade(Config) ->
    OldVersion = version(Config, 0),
    ?assertEqual([OldVersion, OldVersion, OldVersion], log_versions(Config)),
    ok = rpc(Config, 0, rabbit_feature_flags, enable_all, []),
    Config1 = rabbit_ct_helpers:set_config(Config, {force_secondary, false}),
    ok = rabbit_ct_broker_helpers:stop_node(Config1, 0),
    ok = rabbit_ct_broker_helpers:start_node(Config1, 0),
    await_cluster(Config1),
    NewVersion = version(Config1, 0),
    ?assertNotEqual(OldVersion, NewVersion),
    with_connection(Config1, fun(Conn, Ref, Pid) ->
        [begin
             ?assertEqual(OldVersion, version(Config1, I)),
             set_alarm(Config1, I, memory),
             await_state(Config1, Pid, blocking),
             block_publisher(Config1, Conn, Ref, Pid),
             ok = rabbit_ct_broker_helpers:stop_node(Config1, I),
             await_alarm(Config1, I, memory, false),
             await_resumed(Config1, Conn, Ref, Pid),
             ok = rabbit_ct_broker_helpers:start_node(Config1, I),
             await_cluster(Config1),
             ?assertEqual(NewVersion, version(Config1, I)),
             set_alarm(Config1, I, disk),
             await_state(Config1, Pid, blocking),
             block_publisher(Config1, Conn, Ref, Pid),
             clear_alarm(Config1, I, disk),
             await_resumed(Config1, Conn, Ref, Pid)
         end || I <- [1, 2]],
        send(Conn, Ref, "SUBSCRIBE",
             [{"destination", "/topic/resource-alarm"}, {"id", "s0"},
              {"receipt", "subscribed"}], <<>>),
        await_receipt(Conn, Ref, <<"subscribed">>)
    end),
    ?assertEqual([NewVersion, NewVersion, NewVersion], log_versions(Config1)).

await_resumed(Config, Conn, Ref, Pid) ->
    await_state(Config, Pid, running),
    await_receipt(Conn, Ref, <<"pending">>),
    send(Conn, Ref, "UNSUBSCRIBE",
         [{"id", "pending"}, {"receipt", "unsubscribed"}], <<>>),
    await_receipt(Conn, Ref, <<"unsubscribed">>).

await_cluster(Config) ->
    Nodes = lists:sort(rabbit_ct_broker_helpers:get_node_configs(Config, nodename)),
    rabbit_ct_helpers:await_condition(fun() ->
        lists:all(fun(I) ->
            lists:sort(rabbit_ct_broker_helpers:cluster_members_online(Config, I)) =:= Nodes
        end, [0, 1, 2])
    end, 10_000).

version(Config, I) ->
    rpc(Config, I, rabbit, product_version, []).

log_versions(Config) ->
    Versions = [version(Config, I) || I <- [0, 1, 2]],
    ct:pal("Cluster versions: ~tp", [Versions]),
    Versions.

set_alarm(Config, I, Source) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, I, nodename),
    ok = rpc(Config, I, rabbit_alarm, set_alarm, [{{resource_limit, Source, Node}, []}]),
    await_alarm(Config, I, Source, true).

clear_alarm(Config, I, Source) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, I, nodename),
    ok = rpc(Config, I, rabbit_alarm, clear_alarm, [{resource_limit, Source, Node}]),
    await_alarm(Config, I, Source, false).

await_alarm(Config, I, Source, Expected) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, I, nodename),
    rabbit_ct_helpers:await_condition(fun() ->
        Alarms = rpc(Config, 0, rabbit_alarm, get_alarms, []),
        lists:keymember({resource_limit, Source, Node}, 1, Alarms) =:= Expected
    end, 5000).

rpc(Config, I, Module, Function, Args) ->
    rabbit_ct_broker_helpers:rpc(Config, I, Module, Function, Args).
