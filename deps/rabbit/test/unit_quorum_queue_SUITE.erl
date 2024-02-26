-module(unit_quorum_queue_SUITE).

-compile(export_all).

all() ->
    [
     all_replica_states_includes_nonvoters,
     filter_nonvoters,
     filter_quorum_critical_accounts_nonvoters
    ].

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

all_replica_states_includes_nonvoters(_Config) ->
    ets:new(ra_state, [named_table, public, {write_concurrency, true}]),
    ets:insert(ra_state, [
                          {q1, leader, voter},
                          {q2, follower, voter},
                          {q3, follower, promotable},
                          %% pre ra-2.7.0
                          {q4, leader},
                          {q5, follower}
                         ]),
    {_, #{
          q1 := leader,
          q2 := follower,
          q3 := promotable,
          q4 := leader,
          q5 := follower
         }} = rabbit_quorum_queue:all_replica_states(),

    true = ets:delete(ra_state),
    ok.
