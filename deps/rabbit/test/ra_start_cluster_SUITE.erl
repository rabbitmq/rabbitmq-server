%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(ra_start_cluster_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-compile([nowarn_export_all, export_all]).

all() ->
    [{group, cluster_size_3}].

groups() ->
    [{cluster_size_3, [],
      [stream_coordinator_start_with_existing_member,
       quorum_queue_declare_delete_churn,
       quorum_queue_declare_with_suspended_supervisor,
       quorum_queue_declare_with_colliding_ra_name]}].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_nodes_count, 3},
                                            {rmq_nodename_suffix, Group},
                                            {tcp_ports_base}]),
    rabbit_ct_helpers:run_steps(Config1,
                                rabbit_ct_broker_helpers:setup_steps() ++
                                rabbit_ct_client_helpers:setup_steps()).

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_client_helpers:teardown_steps() ++
                                rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_queues, []),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Test cases
%% -------------------------------------------------------------------

%% `ensure_coordinator_started/0` can reach `start_coordinator_cluster/0` while
%% a member is running if its 1s `whereis` probe of another node times out.
stream_coordinator_start_with_existing_member(Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    SQ = <<"ra_start_cluster_stream">>,
    ?assertMatch(#'queue.declare_ok'{},
                 declare(Ch, SQ, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    [?assert(is_pid(rabbit_ct_broker_helpers:rpc(Config, N, erlang, whereis,
                                                 [rabbit_stream_coordinator])))
     || N <- lists:seq(0, 2)],

    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE,
                                      expose_start_coordinator_cluster, []),
    Res = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_stream_coordinator,
                                       start_coordinator_cluster, []),
    ct:pal("start_coordinator_cluster/0 on ~w returned ~p", [Nodes, Res]),
    ?assertNot(is_crash(Res)),
    ?assert(is_pid(rabbit_ct_broker_helpers:rpc(Config, 1, erlang, whereis,
                                                [rabbit_stream_coordinator]))).

quorum_queue_declare_delete_churn(Config) ->
    Workers = 6,
    Iterations = 25,
    Parent = self(),
    Pids = [spawn_link(
              fun() ->
                      Node = W rem 3,
                      Failures = [begin
                                      Q = iolist_to_binary(
                                            ["ra_start_cluster_churn_", integer_to_list(I rem 4)]),
                                      churn_step(Config, Node, Q)
                                  end || I <- lists:seq(1, Iterations)],
                      Parent ! {self(), [F || F <- Failures, F =/= ok]}
              end) || W <- lists:seq(1, Workers)],
    Failures = lists:append([receive {P, F} -> F after 600_000 -> [timeout] end
                             || P <- Pids]),
    ct:pal("churn failures (~b): ~p", [length(Failures), lists:sublist(Failures, 20)]),
    ?assertEqual([], [F || F <- Failures, is_members_exist(F)]),
    ?assertEqual([], Failures),
    delete_queues(Config),
    [?assertEqual([], leftover_ra_servers(Config, N)) || N <- lists:seq(0, 2)].

quorum_queue_declare_with_suspended_supervisor(Config) ->
    Q = <<"ra_start_cluster_suspended">>,
    RaName = binary_to_atom(<<"%2F_", Q/binary>>),
    SupSup = rabbit_ct_broker_helpers:rpc(Config, 1, ?MODULE, server_sup_sup, []),
    ok = rabbit_ct_broker_helpers:rpc(Config, 1, sys, suspend, [SupSup]),
    {Conn, Ch} = open(Config, 0),
    Res = try
              declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"quorum">>}])
          catch exit:Reason ->
                    {exit, Reason}
          end,
    ct:pal("declare with suspended supervisor: ~p", [Res]),
    catch amqp_connection:close(Conn),
    ?assertMatch({exit, _}, Res),

    [?assertEqual(undefined,
                  rabbit_ct_broker_helpers:rpc(Config, N, erlang, whereis, [RaName]))
     || N <- [0, 2]],

    ok = rabbit_ct_broker_helpers:rpc(Config, 1, sys, resume, [SupSup]),
    timer:sleep(5000),
    Running = [{N, rabbit_ct_broker_helpers:rpc(Config, N, erlang, whereis, [RaName])}
               || N <- [0, 1, 2]],
    ct:pal("ra servers after resume: ~p", [Running]),
    ?assertNotEqual([], [R || {_, P} = R <- Running, P =/= undefined]),

    %% the first retry removes the orphan and fails, the next one succeeds
    {Conn2, Ch2} = open(Config, 0),
    ?assertExit(_, declare(Ch2, Q, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    catch amqp_connection:close(Conn2),
    {Conn3, Ch3} = open(Config, 0),
    ?assertMatch(#'queue.declare_ok'{},
                 declare(Ch3, Q, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    amqp_connection:close(Conn3),
    [?assert(is_pid(rabbit_ct_broker_helpers:rpc(Config, N, erlang, whereis, [RaName])))
     || N <- [0, 1, 2]].

quorum_queue_declare_with_colliding_ra_name(Config) ->
    VHost1 = <<"ra_coll_a">>,
    VHost2 = <<"ra_coll_a_b">>,
    Q1 = <<"b_c">>,
    Q2 = <<"c">>,
    RaName = binary_to_atom(<<"ra_coll_a_b_c">>),
    Args = [{<<"x-queue-type">>, longstr, <<"quorum">>}],
    [begin
         ok = rabbit_ct_broker_helpers:add_vhost(Config, V),
         ok = rabbit_ct_broker_helpers:set_full_permissions(Config, <<"guest">>, V)
     end || V <- [VHost1, VHost2]],
    try
        {Conn1, Ch1} = open(Config, 0, VHost1),
        ?assertMatch(#'queue.declare_ok'{}, declare(Ch1, Q1, Args)),
        amqp_channel:call(Ch1, #'confirm.select'{}),
        amqp_channel:cast(Ch1, #'basic.publish'{routing_key = Q1},
                          #amqp_msg{payload = <<"live">>}),
        true = amqp_channel:wait_for_confirms(Ch1, 30_000),

        [begin
             {Conn2, Ch2} = open(Config, 0, VHost2),
             ?assertExit(_, declare(Ch2, Q2, Args)),
             catch amqp_connection:close(Conn2)
         end || _ <- [1, 2]],

        [?assert(is_pid(rabbit_ct_broker_helpers:rpc(Config, N, erlang, whereis, [RaName])))
         || N <- [0, 1, 2]],
        ?assertMatch(#'queue.declare_ok'{message_count = 1},
                     amqp_channel:call(Ch1, #'queue.declare'{queue = Q1,
                                                             durable = true,
                                                             arguments = Args})),
        ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = <<"live">>}},
                     amqp_channel:call(Ch1, #'basic.get'{queue = Q1, no_ack = true})),
        amqp_connection:close(Conn1)
    after
        [rabbit_ct_broker_helpers:delete_vhost(Config, V) || V <- [VHost1, VHost2]]
    end.

%% -------------------------------------------------------------------
%% Helpers, some run on the broker nodes
%% -------------------------------------------------------------------

open(Config, Node) ->
    open(Config, Node, <<"/">>).

open(Config, Node, VHost) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, Node, VHost),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    {Conn, Ch}.

declare(Ch, Q, Args) ->
    amqp_channel:call(Ch, #'queue.declare'{queue = Q,
                                           durable = true,
                                           arguments = Args}).

churn_step(Config, Node, Q) ->
    {Conn, Ch} = open(Config, Node),
    try
        _ = declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"quorum">>}]),
        _ = amqp_channel:call(Ch, #'queue.delete'{queue = Q}),
        ok
    catch exit:{{shutdown, {_, _, Text}}, _} ->
              {Q, Node, Text};
          exit:Other ->
              {Q, Node, Other}
    after
        catch amqp_connection:close(Conn)
    end.

is_members_exist({_, _, Text}) when is_binary(Text) ->
    binary:match(Text, <<"members_exist">>) =/= nomatch;
is_members_exist(_) ->
    false.

is_crash({badrpc, _}) -> true;
is_crash({'EXIT', _}) -> true;
is_crash(_) -> false.

leftover_ra_servers(Config, N) ->
    rabbit_ct_broker_helpers:rpc(Config, N, ?MODULE, ra_servers_with_prefix, []).

ra_servers_with_prefix() ->
    [P || P <- registered(),
          lists:prefix("%2F_ra_start_cluster_", atom_to_list(P))].

server_sup_sup() ->
    #{names := #{server_sup := Name}} = ra_system:fetch(quorum_queues),
    Name.

delete_queues() ->
    [rabbit_amqqueue:delete(Q, false, false, <<"dummy">>)
     || Q <- rabbit_amqqueue:list()],
    ok.

delete_queues(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_queues, []).

%% Re-compiles `rabbit_stream_coordinator` with all functions exported so
%% that the test can call `start_coordinator_cluster/0` directly.
expose_start_coordinator_cluster() ->
    Mod = rabbit_stream_coordinator,
    {Mod, Bin, _} = code:get_object_code(Mod),
    {ok, {Mod, [{abstract_code, {raw_abstract_v1, Forms}}]}} =
        beam_lib:chunks(Bin, [abstract_code]),
    {ok, Mod, NewBin} = compile:forms(Forms, [export_all, nowarn_export_all,
                                              return_errors]),
    _ = code:purge(Mod),
    {module, Mod} = code:load_binary(Mod, "ra_start_cluster_SUITE", NewBin),
    ok.
