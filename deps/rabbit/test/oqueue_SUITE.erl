-module(oqueue_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-export([
         ]).

-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, tests}
    ].


all_tests() ->
    [
     basics,
     delete,
     delete_front,
     order
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

basics(_Confg) ->
    Q0 = oqueue:new(),
    ?assertMatch({empty, _}, oqueue:out(Q0)),
    Q1 = oqueue:in(1, Q0),
    ?assertEqual(1, oqueue:len(Q1)),
    ?assertMatch({{value, 1}, _}, oqueue:out(Q1)),
    Q2 = oqueue:in(0, Q1),
    ?assertEqual(2, oqueue:len(Q2)),
    {V2, Q3}  = oqueue:out(Q2),
    ?assertMatch({value, 0}, V2),
    ?assertMatch({{value, 1}, _}, oqueue:out(Q3)),
    Q4 = oqueue:in(0, Q3),
    ?assertMatch({{value, 0}, _}, oqueue:out(Q4)),
    ok.


delete(_Config) ->
    Q0 = enq_list([1,2,3], oqueue:new()),
    Q1 = oqueue:delete(2, Q0),
    {error, not_found} = oqueue:delete(4, Q0),
    ?assertEqual(2, oqueue:len(Q1)),
    ?assertEqual([1,3], oqueue:to_list(Q1)),
    ok.

delete_front(_Config) ->
    Q0 = enq_list([1,2,3,4], oqueue:new()),
    %% this ensures there is a front
    {_, Q1}  = oqueue:out(Q0),
    {error, not_found} = oqueue:delete(1, Q1),
    Q2 = oqueue:delete(3, Q1),
    ?assertEqual(2, oqueue:len(Q2)),
    ?assertEqual([2,4], oqueue:to_list(Q2)),

    Q3 = oqueue:in(5, Q1),
    Q4 = oqueue:delete(3, Q3),
    ?assertEqual(3, oqueue:len(Q4)),
    ?assertEqual([2,4, 5], oqueue:to_list(Q4)),
    ok.

order(_Config) ->
    run_proper(
      fun () ->
              ?FORALL(Ops, list(
                             frequency([
                                        {5, non_neg_integer()},
                                        {1, deq},
                                        {2, {del, non_neg_integer()}}
                                       ])
                            ),
                      order_prop(Ops))
      end, [], 20000).

order_prop(Ops0) ->
    % ct:pal("Ops ~w", [Ops0]),
    OutQ = enq_list(Ops0, oqueue:new()),
    Expected = run_queue(Ops0, []),
    OQList = oqueue:to_list(OutQ),
    Expected == OQList andalso
    oqueue:len(OutQ) == length(Expected).

enq_list([], Q) ->
    Q;
enq_list([deq | T], Q0) ->
    {_, Q} = oqueue:out(Q0),
    enq_list(T, Q);
enq_list([{del, I} | T], Q0) ->
    case oqueue:delete(I, Q0) of
        {error, not_found} ->
            enq_list(T, Q0);
        Q ->
            enq_list(T, Q)
    end;
enq_list([H | T], Q) ->
    enq_list(T, oqueue:in(H, Q)).

run_proper(Fun, Args, NumTests) ->
    ?assertEqual(
       true,
       proper:counterexample(
         erlang:apply(Fun, Args),
         [{numtests, NumTests},
          {on_output, fun(".", _) -> ok; % don't print the '.'s on new lines
                         (F, A) -> ct:pal(?LOW_IMPORTANCE, F, A)
                      end}])).

run_queue([], Q) ->
    Q;
run_queue([deq | T], Q) ->
    run_queue(T, drop_head(Q));
run_queue([{del, I} | T], Q) ->
    run_queue(T, lists:delete(I, Q));
run_queue([I | T], Q) ->
    run_queue(T, insert(I, Q)).

drop_head([]) -> [];
drop_head([_ | T]) -> T.

insert(Item, [H | T]) when Item > H->
    [H | insert(Item, T)];
insert(Item, List) ->
    [Item | List].
