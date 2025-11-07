-module(rabbit_fifo_pq_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit/src/rabbit_fifo.hrl").

all() ->
    [
     {group, tests}
    ].


all_tests() ->
    [
     basics,
     property
    ].


groups() ->
    [
     {tests, [parallel], all_tests()}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

-define(MSG(L), ?MSG(L, L)).

basics(_Config) ->
    Q0 = rabbit_fifo_pq:new(),
    Q1 = lists:foldl(
           fun ({P, I}, Q) ->
                   rabbit_fifo_pq:in(P, I, Q)
           end, Q0, [
                     {1, ?MSG(1)},
                     {2, ?MSG(2)},
                     {3, ?MSG(3)},
                     {4, ?MSG(4)},
                     {5, ?MSG(5)}
                    ]),

    ?assertEqual([1,2,3,4,5], lists:sort(rabbit_fifo_pq:indexes(Q1))),
    ?assertMatch(#{len := 5,
                   num_active_priorities := 5,
                   lowest_index := 1}, rabbit_fifo_pq:overview(Q1)),
    {?MSG(5), Q2} = rabbit_fifo_pq:out(Q1),
    {?MSG(4), Q3} = rabbit_fifo_pq:out(Q2),
    {?MSG(3), Q4} = rabbit_fifo_pq:out(Q3),
    {?MSG(2), Q5} = rabbit_fifo_pq:out(Q4),
    {?MSG(1), Q6} = rabbit_fifo_pq:out(Q5),
    empty = rabbit_fifo_pq:out(Q6),
    ok.

hi_is_prioritised(_Config) ->
    Q0 = rabbit_fifo_q:new(),
    %% when `hi' has a lower index than the next 'no' then it is still
    %% prioritied (as this is safe to do).
    Q1 = lists:foldl(
           fun ({P, I}, Q) ->
                   rabbit_fifo_q:in(P, I, Q)
           end, Q0, [
                     {hi, ?MSG(1)},
                     {hi, ?MSG(2)},
                     {hi, ?MSG(3)},
                     {hi, ?MSG(4)},
                     {no, ?MSG(5)}
                    ]),
    {?MSG(1), Q2} = rabbit_fifo_q:out(Q1),
    {?MSG(2), Q3} = rabbit_fifo_q:out(Q2),
    {?MSG(3), Q4} = rabbit_fifo_q:out(Q3),
    {?MSG(4), Q5} = rabbit_fifo_q:out(Q4),
    {?MSG(5), Q6} = rabbit_fifo_q:out(Q5),
    empty = rabbit_fifo_q:out(Q6),
    ok.

get_lowest_index(_Config) ->
    Q0 = rabbit_fifo_q:new(),
    Q1 = rabbit_fifo_q:in(hi, ?MSG(1, ?LINE), Q0),
    Q2 = rabbit_fifo_q:in(no, ?MSG(2, ?LINE), Q1),
    Q3 = rabbit_fifo_q:in(no, ?MSG(3, ?LINE), Q2),
    {_, Q4} = rabbit_fifo_q:out(Q3),
    {_, Q5} = rabbit_fifo_q:out(Q4),
    {_, Q6} = rabbit_fifo_q:out(Q5),

    ?assertEqual(undefined, rabbit_fifo_q:get_lowest_index(Q0)),
    ?assertEqual(1, rabbit_fifo_q:get_lowest_index(Q1)),
    ?assertEqual(1, rabbit_fifo_q:get_lowest_index(Q2)),
    ?assertEqual(1, rabbit_fifo_q:get_lowest_index(Q3)),
    ?assertEqual(2, rabbit_fifo_q:get_lowest_index(Q4)),
    ?assertEqual(3, rabbit_fifo_q:get_lowest_index(Q5)),
    ?assertEqual(undefined, rabbit_fifo_q:get_lowest_index(Q6)).

property(_Config) ->
    run_proper(
      fun () ->
              ?FORALL(Ops, op_gen(256),
                      queue_prop(Ops))
      end, [], 25),
    ok.

queue_prop(Ops) ->
    %% create the expected output order
    SortedOps = lists:append([begin
                                  [I || {Pr, _} = I <- Ops, Pr == X]
                              end || X <- lists:seq(31, 0, -1)]),

    Sut0 = rabbit_fifo_pq:from_list(Ops),
    Out = rabbit_fifo_pq:to_list(Sut0),
    [element(2, O) || O <- SortedOps] == Out.

%%% helpers

-type item() :: {rabbit_fifo_pq:priority(), integer()}.
op_gen(Size) ->
    ?LET(Ops, resize(Size, list(item())), Ops).

run_proper(Fun, Args, NumTests) ->
    ?assert(
       proper:counterexample(
         erlang:apply(Fun, Args),
         [{numtests, NumTests},
          {on_output, fun(".", _) -> ok; % don't print the '.'s on new lines
                         (F, A) -> ct:pal(?LOW_IMPORTANCE, F, A)
                      end}])).
