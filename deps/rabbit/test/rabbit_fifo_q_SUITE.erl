-module(rabbit_fifo_q_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-export([
         ]).

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
     single_priority_behaves_like_queue
    ].


groups() ->
    [
     {tests, [], all_tests()}
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
                     {hi, ?MSG(?LINE)}
                    ]),
    {hi, _, Q2} = rabbit_fifo_q:out(Q1),
    {empty, _Q3} = rabbit_fifo_q:out(Q2),
    ok.

basics(_Config) ->
    Q0 = rabbit_fifo_q:new(),
    Q1 = lists:foldl(
           fun ({P, I}, Q) ->
                   rabbit_fifo_q:in(P, I, Q)
           end, Q0, [
                     {hi, ?MSG(?LINE)},
                     {lo, ?MSG(?LINE)},
                     {hi, ?MSG(?LINE)},
                     {lo, ?MSG(?LINE)},
                     {hi, ?MSG(?LINE)}
                    ]),
    {hi, _, Q2} = rabbit_fifo_q:out(Q1),
    {hi, _, Q3} = rabbit_fifo_q:out(Q2),
    {lo, _, Q4} = rabbit_fifo_q:out(Q3),
    {hi, _, Q5} = rabbit_fifo_q:out(Q4),
    {lo, _, Q6} = rabbit_fifo_q:out(Q5),
    {empty, _} = rabbit_fifo_q:out(Q6),
    ok.

hi_is_prioritised(_Config) ->
    Q0 = rabbit_fifo_q:new(),
    %% when `hi' has a lower index than the next lo then it is still
    %% prioritied (as this is safe to do).
    Q1 = lists:foldl(
           fun ({P, I}, Q) ->
                   rabbit_fifo_q:in(P, I, Q)
           end, Q0, [
                     {hi, ?MSG(1, ?LINE)},
                     {hi, ?MSG(2, ?LINE)},
                     {hi, ?MSG(3, ?LINE)},
                     {hi, ?MSG(4, ?LINE)},
                     {lo, ?MSG(5, ?LINE)}
                    ]),
    {hi, _, Q2} = rabbit_fifo_q:out(Q1),
    {hi, _, Q3} = rabbit_fifo_q:out(Q2),
    {hi, _, Q4} = rabbit_fifo_q:out(Q3),
    {hi, _, Q5} = rabbit_fifo_q:out(Q4),
    {lo, _, Q6} = rabbit_fifo_q:out(Q5),
    {empty, _} = rabbit_fifo_q:out(Q6),

    ok.

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
                                             {{value, V_}, Q1} ->
                                                 {V_, Q1};
                                             Res0 ->
                                                 Res0
                                         end,
                               {V2, S} = case rabbit_fifo_q:out(S0) of
                                             {_, V, S1} ->
                                                 {V, S1};
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
