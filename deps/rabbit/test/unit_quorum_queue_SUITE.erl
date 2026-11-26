-module(unit_quorum_queue_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

all() ->
    [
     all_replica_states_includes_alive_nonvoters,
     filter_nonvoters,
     filter_quorum_critical_accounts_nonvoters,
     ra_machine_conf_delivery_limit,
     periodic_membership_reconciliation_target_group_size,
     leader_health_check_caps_time_per_queue,
     leader_health_check_queries_followers_in_parallel,
     leader_health_check_reports_outstanding_queues_on_timeout
    ].

ra_machine_conf_delivery_limit(_Config) ->
    Q0 = amqqueue:new(rabbit_misc:r(<<"/">>, queue, <<"q2">>),
                      {q2, test@leader},
                      false, false, none, [], undefined, #{}),
    %% ensure default is set
    ?assertMatch(#{delivery_limit := 20},
                 rabbit_quorum_queue:ra_machine_config(Q0)),

    Q = amqqueue:set_policy(Q0, [{name, <<"p1">>},
                                 {definition, [{<<"delivery-limit">>,-1}]}]),
    %% a policy of -1
    ?assertMatch(#{delivery_limit := -1},
                 rabbit_quorum_queue:ra_machine_config(Q)),

    %% if therre is a queue arg with a non neg value this takes precedence
    Q1 = amqqueue:set_arguments(Q, [{<<"x-delivery-limit">>, long, 5}]),
    ?assertMatch(#{delivery_limit := 5},
                 rabbit_quorum_queue:ra_machine_config(Q1)),

    Q2 = amqqueue:set_policy(Q1, [{name, <<"o1">>},
                                  {definition, [{<<"delivery-limit">>, 5}]}]),
    Q3 = amqqueue:set_arguments(Q2, [{<<"x-delivery-limit">>, long, -1}]),
    ?assertMatch(#{delivery_limit := 5},
                 rabbit_quorum_queue:ra_machine_config(Q3)),

    %% non neg takes precedence
    ?assertMatch(#{delivery_limit := 5},
                 make_ra_machine_conf(Q0, -1, -1, 5)),
    ?assertMatch(#{delivery_limit := 5},
                 make_ra_machine_conf(Q0, -1, 5, -1)),
    ?assertMatch(#{delivery_limit := 5},
                 make_ra_machine_conf(Q0, 5, -1, -1)),

    ?assertMatch(#{delivery_limit := 5},
                 make_ra_machine_conf(Q0, -1, 10, 5)),
    ?assertMatch(#{delivery_limit := 5},
                 make_ra_machine_conf(Q0, -1, 5, 10)),
    ?assertMatch(#{delivery_limit := 5},
                 make_ra_machine_conf(Q0, 5, 15, 10)),
    ?assertMatch(#{delivery_limit := 5},
                 make_ra_machine_conf(Q0, 15, 5, 10)),
    ?assertMatch(#{delivery_limit := 5},
                 make_ra_machine_conf(Q0, 15, 10, 5)),

    ok.


filter_quorum_critical_accounts_nonvoters(_Config) ->
    Nodes = [test@leader, test@follower1, test@follower2],
    Qs0 = [amqqueue:new(rabbit_misc:r(<<"/">>, queue, <<"q1">>),
                        {q1, test@leader},
                        false, false, none, [], undefined, #{}),
           amqqueue:new(rabbit_misc:r(<<"/">>, queue, <<"q2">>),
                        {q2, test@leader},
                        false, false, none, [], undefined, #{})],
    Qs = [Q1, Q2] = lists:map(fun (Q) ->
                                      amqqueue:set_type_state(Q, #{nodes => Nodes})
                              end, Qs0),
    Ss = #{test@leader    => #{q1 => leader,   q2 => leader},
           test@follower1 => #{q1 => promotable, q2 => follower},
           test@follower2 => #{q1 => follower, q2 => promotable}},
    Qs = rabbit_quorum_queue:filter_quorum_critical(Qs, Ss, test@leader),
    [Q2] = rabbit_quorum_queue:filter_quorum_critical(Qs, Ss, test@follower1),
    [Q1] = rabbit_quorum_queue:filter_quorum_critical(Qs, Ss, test@follower2),
    ok.

filter_nonvoters(_Config) ->
    Qs = [_, _, _, Q4] =
           [amqqueue:new(rabbit_misc:r(<<"/">>, queue, <<"q1">>),
                        {q1, test@leader},
                        false, false, none, [], undefined, #{}),
            amqqueue:new(rabbit_misc:r(<<"/">>, queue, <<"q2">>),
                        {q2, test@leader},
                        false, false, none, [], undefined, #{}),
            amqqueue:new(rabbit_misc:r(<<"/">>, queue, <<"q3">>),
                        {q3, test@leader},
                        false, false, none, [], undefined, #{}),
            amqqueue:new(rabbit_misc:r(<<"/">>, queue, <<"q4">>),
                        {q4, test@leader},
                        false, false, none, [], undefined, #{})],
    Ss = #{q1 => leader, q2 => follower, q3 => non_voter, q4 => promotable},
    [Q4] = rabbit_quorum_queue:filter_promotable(Qs, Ss),
    ok.

all_replica_states_includes_alive_nonvoters(_Config) ->
    ets:new(ra_state, [named_table, public, {write_concurrency, true}]),
    QPids = start_qprocs(_AliveQs = [q1, q2, q3, q4]),
    ets:insert(ra_state, [
                          {q1, leader, voter},
                          {q2, follower, voter},
                          {q3, follower, promotable},
                          {q4, init, unknown},
                          %% queues in ra_state but not alive
                          {q5, leader, voter},
                          {q6, follower, noproc}
                         ]),
    {_, #{
          q1 := leader,
          q2 := follower,
          q3 := promotable,
          q4 := init
         } = ReplicaStates} = rabbit_quorum_queue:all_replica_states(),
    ?assertNot(maps:is_key(q5, ReplicaStates)),
    ?assertNot(maps:is_key(q6, ReplicaStates)),

    true = ets:delete(ra_state),
    _ = stop_qprocs(QPids),
    ok.

periodic_membership_reconciliation_target_group_size(_Config) ->
    Q0 = amqqueue:new(rabbit_misc:r(<<"/">>, queue, <<"q1">>),
                      {q1, test@leader},
                      false, false, none, [], undefined, #{}),
    Q = amqqueue:set_arguments(
          Q0, [{<<"x-quorum-target-group-size">>, long, 3}]),
    ?assertEqual(
       3, rabbit_quorum_queue_periodic_membership_reconciliation:get_target_size(
            Q, undefined)),
    ok.

leader_health_check_caps_time_per_queue(_Config) ->
    Members = [{N, node()} || N <- [qq_hc_member_1, qq_hc_member_2, qq_hc_member_3]],
    TestPid = self(),
    with_mocked_ra_members(
      fun(_ServerId, _Timeout) -> hang(TestPid) end,
      fun() ->
              QName = rabbit_misc:r(<<"/">>, queue, <<"q1">>),
              Ref = make_ref(),
              ok = rabbit_quorum_queue:run_leader_health_check(
                     Members, QName, Ref, self(), 100, 200),
              ?assertEqual({error, Ref, QName}, receive_result(Ref)),
              Hung = receive_hung(3),
              [?awaitMatch(false, is_process_alive(P), 30_000) || P <- Hung]
      end).

leader_health_check_queries_followers_in_parallel(_Config) ->
    Members = [{N, node()} || N <- [qq_hc_down, qq_hc_hung_1, qq_hc_hung_2, qq_hc_ok]],
    TestPid = self(),
    with_mocked_ra_members(
      fun({qq_hc_down, _}, _Timeout) ->
              {error, noproc};
         ({qq_hc_ok, _} = ServerId, _Timeout) ->
              TestPid ! {ok_member, self()},
              receive go -> {ok, [ServerId], ServerId} end;
         (_ServerId, _Timeout) ->
              hang(TestPid)
      end,
      fun() ->
              QName = rabbit_misc:r(<<"/">>, queue, <<"q1">>),
              Ref = make_ref(),
              Self = self(),
              _ = spawn_link(
                    fun() ->
                            rabbit_quorum_queue:run_leader_health_check(
                              Members, QName, Ref, Self, 30_000, 60_000)
                    end),
              %% A sequential implementation never reaches `qq_hc_ok`
              Hung = receive_hung(2),
              OkMember = receive
                             {ok_member, P} -> P
                         after 30_000 ->
                                 ct:fail(healthy_member_not_queried)
                         end,
              OkMember ! go,
              ?assertEqual({ok, Ref, QName}, receive_result(Ref)),
              [?awaitMatch(false, is_process_alive(P), 30_000) || P <- Hung]
      end).

with_mocked_ra_members(MembersFun, TestFun) ->
    ok = meck:new(ra, [passthrough]),
    ok = meck:expect(ra, members, MembersFun),
    try
        TestFun()
    after
        meck:unload(ra)
    end.

hang(TestPid) ->
    TestPid ! {hung, self()},
    receive never -> ok end.

receive_hung(N) ->
    [receive
         {hung, Pid} -> Pid
     after 30_000 ->
             ct:fail(member_not_queried)
     end || _ <- lists:seq(1, N)].

receive_result(Ref) ->
    receive
        {_, Ref, _} = Msg -> Msg
    after 30_000 ->
            ct:fail(no_result)
    end.

leader_health_check_reports_outstanding_queues_on_timeout(_Config) ->
    ok = meck:new(rabbit_queue_type, [passthrough]),
    ok = meck:expect(rabbit_queue_type, discover,
                     fun(rabbit_quorum_queue) -> rabbit_quorum_queue end),
    try
        [Q1, Q2, Q3] = [rabbit_misc:r(<<"/">>, queue, N)
                        || N <- [<<"q1">>, <<"q2">>, <<"q3">>]],
        Ref = make_ref(),
        self() ! {ok, Ref, Q1},
        self() ! {ok, make_ref(), Q2},
        self() ! {error, Ref, Q3},
        Pending = maps:from_keys([Q1, Q2, Q3], true),
        Result = rabbit_quorum_queue:wait_for_leader_health_checks(Ref, Pending, 100),
        ?assertEqual([<<"queue 'q2' in vhost '/'">>, <<"queue 'q3' in vhost '/'">>],
                     lists:sort([maps:get(<<"readable_name">>, R) || R <- Result]))
    after
        meck:unload(rabbit_queue_type)
    end.

make_ra_machine_conf(Q0, Arg, Pol, OpPol) ->
    Q1 = amqqueue:set_arguments(Q0, [{<<"x-delivery-limit">>, long, Arg}]),
    Q2 = amqqueue:set_policy(Q1, [{name, <<"p1">>},
                                  {definition, [{<<"delivery-limit">>,Pol}]}]),
    Q = amqqueue:set_operator_policy(Q2, [{name, <<"p1">>},
                                          {definition, [{<<"delivery-limit">>,OpPol}]}]),
    rabbit_quorum_queue:ra_machine_config(Q).

start_qprocs(Qs) ->
    [begin
        QPid = spawn(fun() -> receive done -> ok end end),
        erlang:register(Q, QPid),
        QPid
     end || Q <- Qs].

stop_qprocs(Pids) ->
    [erlang:send(P, done)|| P <- Pids].

