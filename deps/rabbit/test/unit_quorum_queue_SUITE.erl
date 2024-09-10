-module(unit_quorum_queue_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

all() ->
    [
     all_replica_states_includes_nonvoters,
     filter_nonvoters,
     filter_quorum_critical_accounts_nonvoters,
     ra_machine_conf_delivery_limit
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

all_replica_states_includes_nonvoters(_Config) ->
    ets:new(ra_state, [named_table, public, {write_concurrency, true}]),
    ets:insert(ra_state, [
                          {q1, leader, voter},
                          {q2, follower, voter},
                          {q3, follower, promotable},
                          {q4, init, unknown},
                          %% pre ra-2.7.0
                          {q5, leader},
                          {q6, follower}
                         ]),
    {_, #{
          q1 := leader,
          q2 := follower,
          q3 := promotable,
          q4 := init,
          q5 := leader,
          q6 := follower
         }} = rabbit_quorum_queue:all_replica_states(),

    true = ets:delete(ra_state),
    ok.

make_ra_machine_conf(Q0, Arg, Pol, OpPol) ->
    Q1 = amqqueue:set_arguments(Q0, [{<<"x-delivery-limit">>, long, Arg}]),
    Q2 = amqqueue:set_policy(Q1, [{name, <<"p1">>},
                                  {definition, [{<<"delivery-limit">>,Pol}]}]),
    Q = amqqueue:set_operator_policy(Q2, [{name, <<"p1">>},
                                          {definition, [{<<"delivery-limit">>,OpPol}]}]),
    rabbit_quorum_queue:ra_machine_config(Q).

