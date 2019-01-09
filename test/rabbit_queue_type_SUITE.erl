-module(rabbit_queue_type_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

all() ->
    [
     {group, rabbit_quorum_queue}
    ].

all_tests() ->
    [
     basics
    ].

groups() ->
    [
     {rabbit_quorum_queue, [], all_tests()}
    ].

init_per_group(Group, Config) ->
    PrivDir = ?config(priv_dir, Config),
    _ = application:load(ra),
    ok = application:set_env(ra, data_dir, PrivDir),
    application:ensure_all_started(ra),
    application:ensure_all_started(lg),
    [{qt_mod, Group} | Config].

end_per_group(_, Config) ->
    _ = application:stop(ra),
    Config.

init_per_testcase(TestCase, Config) ->
    %% the requisite mocking of anything that calls into rabbit
    %% or requires external state
    meck:new(rabbit_quorum_queue, [passthrough]),
    meck:expect(rabbit_quorum_queue, update_metrics, fun (_, _) -> ok end),
    meck:expect(rabbit_quorum_queue, cancel_consumer_handler,
                fun (_, _) -> ok end),
    ra_server_sup_sup:remove_all(),
    ServerName2 = list_to_atom(atom_to_list(TestCase) ++ "2"),
    ServerName3 = list_to_atom(atom_to_list(TestCase) ++ "3"),
    ClusterName = rabbit_misc:r("/", queue, atom_to_binary(TestCase, utf8)),
    [
     {cluster_name, ClusterName},
     {uid, atom_to_binary(TestCase, utf8)},
     {node_id, {TestCase, node()}},
     {uid2, atom_to_binary(ServerName2, utf8)},
     {node_id2, {ServerName2, node()}},
     {uid3, atom_to_binary(ServerName3, utf8)},
     {node_id3, {ServerName3, node()}}
     | Config].

end_per_testcase(_, Config) ->
    meck:unload(),
    Config.

basics(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    UId = ?config(uid, Config),
    Mod = ?config(qt_mod, Config),
    ok = start_cluster(ClusterName, [ServerId]),
    ra:stop_server(ServerId),
    Qs0 = rabbit_queue_type:init(),
    {Qs1, _} = rabbit_queue_type:in([ClusterName],
                                    1, some_delivery, Qs0),

    % CustomerTag = UId,
    ok.

start_cluster(ClusterName, ServerIds, RaFifoConfig) ->
    {ok, Started, _} = ra:start_cluster(ClusterName#resource.name,
                                        {module, rabbit_fifo, RaFifoConfig},
                                        ServerIds),
    ?assertEqual(length(Started), length(ServerIds)),
    ok.

start_cluster(ClusterName, ServerIds) ->
    start_cluster(ClusterName, ServerIds, #{name => some_name,
                                            queue_resource => ClusterName}).

flush() ->
    receive
        Msg ->
            ct:pal("flushed: ~w~n", [Msg]),
            flush()
    after 10 ->
              ok
    end.
