%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(retained_incarnation_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("eunit/include/eunit.hrl").
-include("rabbit_mqtt_packet.hrl").

-define(VHOST, <<"incarnation">>).

all() -> [{group, ets}, {group, dets}].

groups() ->
    [{Backend, [], [cleanup_stops_the_old_retainer,
                    lookup_replaces_the_old_retainer,
                    restart_keeps_the_child_incarnation,
                    legacy_upgrade_keeps_the_retainer,
                    metadata_failure_refuses_lookup,
                    expected_incarnation_cannot_follow_recreation,
                    local_files_are_isolated,
                    missed_deletion_stops_the_retainer,
                    bulk_deletion_keeps_the_supervisor,
                    bulk_legacy_deletion_keeps_the_supervisor,
                    foreign_legacy_storage_is_isolated,
                    steady_lookups_do_not_read_metadata,
                    foreign_metadata_keeps_files,
                    cleanup_rechecks_files,
                    cleanup_keeps_colliding_paths]} || Backend <- [ets, dets]].

init_per_group(Backend, Config) -> [{backend, Backend} | Config].
end_per_group(_, Config) -> Config.

init_per_testcase(Test, Config) ->
    Backend = proplists:get_value(backend, Config),
    Dir = filename:join(proplists:get_value(priv_dir, Config), atom_to_list(Backend) ++ atom_to_list(Test)),
    ok = filelib:ensure_path(Dir),
    ok = meck:new(rabbit, [non_strict]),
    ok = meck:expect(rabbit, data_dir, 0, Dir),
    ok = meck:new(rabbit_vhost, [passthrough]),
    ok = meck:expect(rabbit_vhost, list_names, 0, []),
    ok = meck:expect(rabbit_vhost, legacy_data_dir, 0, Dir),
    ok = meck:expect(rabbit_vhost, id, 1, {ok, 1}),
    ok = meck:new(rabbit_db_vhost, [non_strict]),
    ok = meck:expect(rabbit_db_vhost, identity, fun(VHost) ->
        case rabbit_vhost:id(VHost) of
            {ok, Id} -> {ok, Id, Id};
            Error -> Error
        end
    end),
    ok = meck:expect(rabbit_db_vhost, get, fun(VHost) ->
        case rabbit_vhost:id(VHost) of
            {ok, Id} -> vhost:new(VHost, [], #{}, Id);
            _ -> undefined
        end
    end),
    ok = meck:expect(rabbit_db_vhost, storage_id, fun(Id) -> {ok, Id} end),
    Mod = case Backend of ets -> rabbit_mqtt_retained_msg_store_ets; dets -> rabbit_mqtt_retained_msg_store_dets end,
    Previous = application:get_env(rabbitmq_mqtt, retained_message_store),
    ok = application:set_env(rabbitmq_mqtt, retained_message_store, Mod),
    {ok, Sup} = rabbit_mqtt_retainer_sup:start_link(),
    [{sup, Sup}, {store_module, Mod}, {store_dir, Dir}, {previous_backend, Previous} | Config].

end_per_testcase(_, Config) ->
    ok = gen_server:stop(proplists:get_value(sup, Config)),
    meck:unload(rabbit_db_vhost),
    meck:unload(rabbit_vhost),
    meck:unload(rabbit),
    case proplists:get_value(previous_backend, Config) of
        {ok, Mod} -> application:set_env(rabbitmq_mqtt, retained_message_store, Mod);
        undefined -> application:unset_env(rabbitmq_mqtt, retained_message_store)
    end.

cleanup_stops_the_old_retainer(_Config) ->
    Old = start(),
    retain(Old, <<"old">>),
    ok = meck:expect(rabbit_vhost, id, 1, {ok, 2}),
    ok = rabbit_mqtt_vhost:vhost_deleted(?VHOST, 1),
    ?assertNot(is_process_alive(Old)),
    New = start(),
    ?assertEqual(undefined, rabbit_mqtt_retainer:fetch(New, <<"t">>)),
    retain(New, <<"new">>),
    ok = rabbit_mqtt_vhost:vhost_deleted(?VHOST, 1),
    ?assertEqual(New, start()),
    assert_payload(New, <<"new">>).

lookup_replaces_the_old_retainer(_Config) ->
    Old = start(),
    retain(Old, <<"old">>),
    ok = meck:expect(rabbit_vhost, id, 1, {ok, 2}),
    New = start(),
    ?assertNot(is_process_alive(Old)),
    ?assertNotEqual(Old, New),
    ?assertEqual(undefined, rabbit_mqtt_retainer:fetch(New, <<"t">>)),
    retain(New, <<"new">>),
    ok = rabbit_mqtt_vhost:vhost_deleted(?VHOST, 1),
    ok = rabbit_mqtt_vhost:vhost_deleted(?VHOST, undefined),
    ?assertEqual(New, start()),
    assert_payload(New, <<"new">>).

restart_keeps_the_child_incarnation(_Config) ->
    Old = start(),
    retain(Old, <<"old">>),
    ok = meck:expect(rabbit_vhost, id, 1, {ok, 2}),
    exit(Old, kill),
    _ = supervisor:which_children(rabbit_mqtt_retainer_sup),
    New = start(),
    ?assertNotEqual(Old, New),
    ?assertEqual(undefined, rabbit_mqtt_retainer:fetch(New, <<"t">>)).

legacy_upgrade_keeps_the_retainer(_Config) ->
    ok = set_identity(undefined, legacy),
    Old = start(),
    retain(Old, <<"legacy">>),
    ok = set_identity(1, legacy),
    ?assertEqual(Old, start()),
    assert_payload(Old, <<"legacy">>),
    ok = gen_server:stop(Old),
    Recovered = start(),
    ?assertNotEqual(Old, Recovered),
    assert_payload(Recovered, <<"legacy">>),
    ok = set_identity(2, 2),
    New = start(),
    ?assertNot(is_process_alive(Recovered)),
    ?assertEqual(undefined, rabbit_mqtt_retainer:fetch(New, <<"t">>)).

missed_deletion_stops_the_retainer(_Config) ->
    Old = start(),
    Ref = monitor(process, Old),
    ok = meck:expect(rabbit_db_vhost, identity, 1, {error, not_found}),
    ok = meck:expect(rabbit_db_vhost, get, 1, undefined),
    Old ! check_vhost,
    receive {'DOWN', Ref, process, Old, normal} -> ok
    after 5000 -> ct:fail(retainer_not_stopped)
    end,
    ?assertEqual({error, not_found}, rabbit_mqtt_retainer_sup:start_child_for_vhost(?VHOST)).

set_identity(Id, StorageId) ->
    ok = meck:expect(rabbit_db_vhost, identity, 1, {ok, Id, StorageId}),
    V = case Id of
            undefined -> vhost:new(?VHOST, [], #{});
            _ -> vhost:new(?VHOST, [], #{}, Id)
        end,
    meck:expect(rabbit_db_vhost, get, 1, V).

bulk_deletion_keeps_the_supervisor(Config) ->
    Pids = [rabbit_mqtt_retainer_sup:start_child_for_vhost(integer_to_binary(N)) || N <- lists:seq(1, 10)],
    Refs = [monitor(process, Pid) || Pid <- Pids],
    ok = meck:expect(rabbit_db_vhost, get, 1, undefined),
    ok = meck:expect(rabbit_db_vhost, identity, 1, {error, not_found}),
    [Pid ! check_vhost || Pid <- Pids],
    [receive {'DOWN', Ref, process, _, normal} -> ok
     after 5000 -> ct:fail(retainer_not_stopped)
     end || Ref <- Refs],
    ?assertEqual(proplists:get_value(sup, Config), whereis(rabbit_mqtt_retainer_sup)),
    ok = set_identity(2, 2),
    ?assert(is_pid(start())).

bulk_legacy_deletion_keeps_the_supervisor(Config) ->
    ok = set_identity(undefined, legacy),
    bulk_deletion_keeps_the_supervisor(Config).

foreign_legacy_storage_is_isolated(Config) ->
    CacheKey = {rabbit_vhost, legacy_storage_epoch, proplists:get_value(store_dir, Config)},
    ok = meck:expect(rabbit_db_vhost, storage_epoch, 0, {ok, 1}),
    ok = meck:expect(rabbit_vhost, legacy_data_dir, fun() -> meck:passthrough([]) end),
    ok = set_identity(undefined, legacy),
    Original = start(),
    retain(Original, <<"original">>),
    ok = gen_server:stop(Original),
    persistent_term:erase(CacheKey),
    ok = meck:expect(rabbit_db_vhost, storage_epoch, 0, {ok, 2}),
    Different = start(),
    ?assertEqual(undefined, rabbit_mqtt_retainer:fetch(Different, <<"t">>)),
    ok = gen_server:stop(Different),
    persistent_term:erase(CacheKey),
    ok = meck:expect(rabbit_db_vhost, storage_epoch, 0, {ok, 1}),
    Recovered = start(),
    assert_payload(Recovered, <<"original">>).

steady_lookups_do_not_read_metadata(_Config) ->
    Pid = start(),
    ok = meck:reset(rabbit_db_vhost),
    ?assertEqual(Pid, start()),
    Pid ! check_vhost,
    ?assertEqual(undefined, rabbit_mqtt_retainer:fetch(Pid, <<"t">>)),
    ?assertNot(meck:called(rabbit_db_vhost, identity, '_')).

foreign_metadata_keeps_files(Config) ->
    Dir = proplists:get_value(store_dir, Config),
    Extension = "." ++ atom_to_list(proplists:get_value(backend, Config)),
    Path = rabbit_mqtt_retained_msg_store:incarnation_path(
        rabbit_mqtt_util:path_for(Dir, ?VHOST, Extension), 1),
    ok = file:write_file(Path, <<"kept">>),
    [begin
         ok = meck:expect(rabbit_db_vhost, cleanup_snapshot, 0, {ok, Epoch, #{}}),
         ok = rabbit_mqtt_retained_msg_store:remove_leftover_files(Dir, Extension, #{}),
         ?assertEqual({ok, <<"kept">>}, file:read_file(Path))
     end || Epoch <- [undefined, 1]].

metadata_failure_refuses_lookup(_Config) ->
    Old = start(),
    ok = meck:expect(rabbit_vhost, id, 1, {ok, 2}),
    ok = meck:expect(rabbit_db_vhost, identity, 1, {error, timeout}),
    ?assertEqual({error, timeout}, rabbit_mqtt_retainer_sup:start_child_for_vhost(?VHOST)),
    ?assert(is_process_alive(Old)).

expected_incarnation_cannot_follow_recreation(_Config) ->
    Old = start(),
    ?assertEqual(Old, rabbit_mqtt_retainer_sup:start_child_for_vhost(?VHOST, 1)),
    ok = meck:expect(rabbit_vhost, id, 1, {ok, 2}),
    ?assertEqual({error, stale_vhost_id}, rabbit_mqtt_retainer_sup:start_child_for_vhost(?VHOST, 1)),
    New = start(),
    ?assertEqual({error, stale_vhost_id}, rabbit_mqtt_retainer_sup:start_child_for_vhost(?VHOST, 1)),
    ?assertEqual(New, rabbit_mqtt_retainer_sup:start_child_for_vhost(?VHOST, 2)).

start() ->
    Pid = rabbit_mqtt_retainer_sup:start_child_for_vhost(?VHOST),
    ?assert(is_pid(Pid)),
    Pid.

retain(Pid, Payload) ->
    Msg = #mqtt_msg{topic = <<"t">>, payload = Payload, props = #{}, retain = true, qos = 0},
    ok = rabbit_mqtt_retainer:retain(Pid, <<"t">>, Msg),
    assert_payload(Pid, Payload).

assert_payload(Pid, Payload) ->
    ?assertMatch(#mqtt_msg{payload = Payload}, rabbit_mqtt_retainer:fetch(Pid, <<"t">>)).

local_files_are_isolated(Config) ->
    Mod = proplists:get_value(store_module, Config),
    Dir = proplists:get_value(store_dir, Config),
    Msg = #mqtt_msg{topic = <<"t">>, payload = <<"legacy">>, props = #{}},
    Legacy = Mod:new(Dir, ?VHOST),
    ok = Mod:insert(<<"t">>, Msg, Legacy),
    ok = Mod:terminate(Legacy),
    {ok, Old, _} = Mod:open(Dir, ?VHOST, 1),
    ?assertEqual(undefined, Mod:lookup(<<"t">>, Old)),
    {ok, New, _} = Mod:open(Dir, ?VHOST, 2),
    ok = Mod:insert(<<"t">>, Msg#mqtt_msg{payload = <<"new">>}, New),
    ok = Mod:terminate(Old),
    ok = Mod:terminate(New),
    ok = Mod:delete_vhost(?VHOST, 1),
    ok = Mod:delete_vhost(?VHOST, undefined),
    {ok, Kept, _} = Mod:open(Dir, ?VHOST, 2),
    ?assertMatch(#mqtt_msg{payload = <<"new">>}, Mod:lookup(<<"t">>, Kept)),
    ok = Mod:terminate(Kept),
    {ok, KeptLegacy, _} = Mod:recover(Dir, ?VHOST),
    ?assertEqual(Msg, Mod:lookup(<<"t">>, KeptLegacy)),
    ok = Mod:terminate(KeptLegacy).

cleanup_rechecks_files(Config) ->
    Dir = proplists:get_value(store_dir, Config),
    Extension = "." ++ atom_to_list(proplists:get_value(backend, Config)),
    Path = fun(Name, Id) -> rabbit_mqtt_retained_msg_store:incarnation_path(
                               rabbit_mqtt_util:path_for(Dir, Name, Extension), Id) end,
    [ok = file:write_file(Path(Name, Id), <<"data">>) || {Name, Id} <-
         [{?VHOST, 1}, {?VHOST, 2}, {?VHOST, 5}, {<<"unknown">>, 3}]],
    ok = meck:expect(rabbit_db_vhost, cleanup_snapshot, fun() ->
        ok = file:write_file(Path(<<"new">>, 4), <<"new">>),
        {ok, 0, #{?VHOST => 2, <<"unknown">> => undefined}}
    end),
    ok = rabbit_mqtt_retained_msg_store:remove_leftover_files(Dir, Extension, #{}),
    ?assertNot(filelib:is_file(Path(?VHOST, 1))),
    [?assert(filelib:is_file(Path(Name, Id))) || {Name, Id} <-
         [{?VHOST, 2}, {?VHOST, 5}, {<<"unknown">>, 3}, {<<"new">>, 4}]],
    ok = meck:expect(rabbit_db_vhost, cleanup_snapshot, 0, {error, timeout}),
    ?assertEqual({error, timeout}, rabbit_mqtt_retained_msg_store:remove_leftover_files(Dir, Extension, #{})),
    ?assert(filelib:is_file(Path(?VHOST, 2))).

cleanup_keeps_colliding_paths(Config) ->
    Dir = proplists:get_value(store_dir, Config),
    Extension = "." ++ atom_to_list(proplists:get_value(backend, Config)),
    Path = rabbit_mqtt_util:path_for(Dir, ?VHOST, Extension),
    [Old | Kept] = Files = [rabbit_mqtt_retained_msg_store:incarnation_path(Path, Id)
                           || Id <- lists:seq(1, 5)],
    ok = meck:new(rabbit_mqtt_util, [passthrough]),
    try
        ok = meck:expect(rabbit_mqtt_util, path_for, 3, Path),
        [begin
             [ok = file:write_file(File, <<"kept">>) || File <- Files],
             ok = meck:expect(rabbit_db_vhost, cleanup_snapshot, 0, {ok, 0, Live}),
             ok = rabbit_mqtt_retained_msg_store:remove_leftover_files(Dir, Extension, #{}),
             ?assertNot(filelib:is_file(Old)),
             [?assertEqual({ok, <<"kept">>}, file:read_file(File)) || File <- Kept]
         end || Live <- [#{?VHOST => 2, <<"collision">> => 4},
                         #{?VHOST => 4, <<"collision">> => 2}]]
    after
        meck:unload(rabbit_mqtt_util)
    end.
