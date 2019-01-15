-module(rabbit_queue_type_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

all() ->
    [
     {group, tests}
    ].

all_tests() ->
    [
     in_msg_is_confirmed
    ].

groups() ->
    [
     {tests, [], all_tests()}
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
     {queue_name, ClusterName},
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

in_msg_is_confirmed(Config) ->
    QName = ?config(queue_name, Config),
    meck:new(?FUNCTION_NAME, [non_strict]),
    meck:expect(?FUNCTION_NAME, init, fun (Q) -> {Q, []} end),
    meck:expect(?FUNCTION_NAME, in,
                fun (_QId, Qs, _Seq, _Msg) ->
                        {Qs, []}
                end),
    meck:expect(?FUNCTION_NAME, handle_queue_info,
                fun (_QId, {applied, Seqs}, Qs) when is_list(Seqs) ->
                        %% simply record as accepted
                        {Qs, [{msg_state_update, accepted, Seqs}]}
                end),

    LookupFun = fun(Name) ->
                        #{module => ?FUNCTION_NAME,
                          name => Name}
                end,
    Qs0 = rabbit_queue_type:init(#{queue_lookup_fun => LookupFun}),

    {Qs1, []} = rabbit_queue_type:in([QName], 1, some_delivery, Qs0),
    %% extract the assigned queue id
    #{queues := Queues,
      pending_in := P1} = rabbit_queue_type:to_map(Qs1),
    [QId] = maps:keys(Queues),
    ?assertEqual(1, maps:size(P1)),

    %% this is when the channel can send confirms for example
    {Qs2, [{msg_state_update, accepted, [1]}]} =
        rabbit_queue_type:handle_queue_info(QId, {applied, [1]}, Qs1),

    #{pending_in := P2} = rabbit_queue_type:to_map(Qs2),
    %% no pending should remain inside the state after the queue has accepted
    %% the message
    ?assertEqual(0, maps:size(P2)),
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
