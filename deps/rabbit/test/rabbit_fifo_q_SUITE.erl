-module(rabbit_fifo_q_SUITE).

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
     hi,
     basics,
     hi_is_prioritised,
     get_lowest_index,
     single_priority_behaves_like_queue
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

hi(_Config) ->
    Q0 = rabbit_fifo_q:new(),
    Q1 = lists:foldl(
           fun ({P, I}, Q) ->
                   rabbit_fifo_q:in(P, I, Q)
          end, Q0, [
                     {hi, ?MSG(1)}
                    ]),
    {?MSG(1), Q2} = rabbit_fifo_q:out(Q1),
    empty = rabbit_fifo_q:out(Q2),
    ok.

basics(_Config) ->
    Q0 = rabbit_fifo_q:new(),
    Q1 = lists:foldl(
           fun ({P, I}, Q) ->
                   rabbit_fifo_q:in(P, I, Q)
           end, Q0, [
                     {hi, ?MSG(1)},
                     {lo, ?MSG(2)},
                     {hi, ?MSG(3)},
                     {lo, ?MSG(4)},
                     {hi, ?MSG(5)}
                    ]),
    {?MSG(1), Q2} = rabbit_fifo_q:out(Q1),
    {?MSG(3), Q3} = rabbit_fifo_q:out(Q2),
    {?MSG(2), Q4} = rabbit_fifo_q:out(Q3),
    {?MSG(5), Q5} = rabbit_fifo_q:out(Q4),
    {?MSG(4), Q6} = rabbit_fifo_q:out(Q5),
    empty = rabbit_fifo_q:out(Q6),
    ok.

hi_is_prioritised(_Config) ->
    Q0 = rabbit_fifo_q:new(),
    %% when `hi' has a lower index than the next lo then it is still
    %% prioritied (as this is safe to do).
    Q1 = lists:foldl(
           fun ({P, I}, Q) ->
                   rabbit_fifo_q:in(P, I, Q)
           end, Q0, [
                     {hi, ?MSG(1)},
                     {hi, ?MSG(2)},
                     {hi, ?MSG(3)},
                     {hi, ?MSG(4)},
                     {lo, ?MSG(5)}
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
    Q2 = rabbit_fifo_q:in(lo, ?MSG(2, ?LINE), Q1),
    Q3 = rabbit_fifo_q:in(lo, ?MSG(3, ?LINE), Q2),
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

-type op() :: {in, integer()} | out.

single_priority_behaves_like_queue(_Config) ->
    run_proper(
      fun () ->
              ?FORALL({P, Ops}, {oneof([hi, lo]), op_gen(256)},
                      queue_prop(P, Ops))
      end, [], 25),
    ok.

queue_prop(P, Ops) ->
    % ct:pal("Running queue_prop for ~s", [Ops]),
    Que = queue:new(),
    Sut = rabbit_fifo_q:new(),
    {Queue, FifoQ} = lists:foldl(
                       fun ({in, V}, {Q0, S0}) ->
                               Q = queue:in(V, Q0),
                               S = rabbit_fifo_q:in(P, V, S0),
                               case queue:len(Q) == rabbit_fifo_q:len(S) of
                                   true ->
                                       {Q, S};
                                   false ->
                                       throw(false)
                               end;
                           (out, {Q0, S0}) ->
                               {V1, Q} = case queue:out(Q0) of
                                             {{value, V0}, Q1} ->
                                                 {V0, Q1};
                                             Res0 ->
                                                 Res0
                                         end,
                               {V2, S} = case rabbit_fifo_q:out(S0) of
                                             empty ->
                                                 {empty, S0};
                                             Res ->
                                                 Res
                                         end,
                               case V1 == V2 of
                                   true ->
                                       {Q, S};
                                   false ->
                                       ct:pal("V1 ~p, V2 ~p", [V1, V2]),
                                       throw(false)
                               end
                       end, {Que, Sut}, Ops),

    queue:len(Queue) == rabbit_fifo_q:len(FifoQ).




%%% helpers

op_gen(Size) ->
    ?LET(Ops,
         resize(Size,
                list(
                  frequency(
                    [
                     {20, {in, non_neg_integer()}},
                     {20, out}
                    ]
                   ))),
         begin
             {_, Ops1} = lists:foldl(
                           fun ({in, I}, {Idx, Os}) ->
                                   {Idx + 1, [{in, ?MSG(Idx, I)} | Os]};
                               (out, {Idx, Os}) ->
                                   {Idx + 1, [out | Os] }
                           end, {1, []}, Ops),
             lists:reverse(Ops1)
         end
        ).

run_proper(Fun, Args, NumTests) ->
    ?assert(
       proper:counterexample(
         erlang:apply(Fun, Args),
         [{numtests, NumTests},
          {on_output, fun(".", _) -> ok; % don't print the '.'s on new lines
                         (F, A) -> ct:pal(?LOW_IMPORTANCE, F, A)
                      end}])).
