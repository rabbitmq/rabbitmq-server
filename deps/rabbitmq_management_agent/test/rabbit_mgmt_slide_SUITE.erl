%%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2016-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_slide_SUITE).

-include_lib("proper/include/proper.hrl").

-compile(export_all).

all() ->
    [
     {group, parallel_tests}
    ].

groups() ->
    [
     {parallel_tests, [parallel], [
                                   last_two_test,
                                   last_two_incremental_test,
                                   sum_test,
                                   sum_incremental_test
                                  ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------
init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_, Config) ->
    Config.

end_per_group(_, _Config) ->
    ok.

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, _Config) ->
    ok.

%% -------------------------------------------------------------------
%% Generators.
%% -------------------------------------------------------------------
elements_gen() ->
    ?LET(Length, oneof([1, 2, 3, 7, 8, 20]),
         ?LET(Elements, list(vector(Length, int())),
              [erlang:list_to_tuple(E) || E <- Elements])).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------
last_two_test(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_last_two/0, [], 100).

prop_last_two() ->
    ?FORALL(
       Elements, elements_gen(),
       begin
           Interval = 1,
           Incremental = false,
           {_LastTS, Slide} = new_slide(Interval, Incremental, Elements),
           Expected = last_two(Elements),
           ValuesOnly = [V || {_Timestamp, V} <- exometer_slide:last_two(Slide)],
           ?WHENFAIL(io:format("Last two values obtained: ~p~nExpected: ~p~n"
                               "Slide: ~p~n", [ValuesOnly, Expected, Slide]),
                     Expected == ValuesOnly)
       end).

last_two_incremental_test(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_last_two_incremental/0, [], 100).

prop_last_two_incremental() ->
    ?FORALL(
       Elements, non_empty(elements_gen()),
       begin
           Interval = 1,
           Incremental = true,
           {_LastTS, Slide} = new_slide(Interval, Incremental, Elements),
           [{_Timestamp, Values} | _] = exometer_slide:last_two(Slide),
           Expected = add_elements(Elements),
           ?WHENFAIL(io:format("Expected a total of: ~p~nGot: ~p~n"
                               "Slide: ~p~n", [Expected, Values, Slide]),
                     Values == Expected)
       end).

sum_incremental_test(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_sum/1, [true], 100).

sum_test(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_sum/1, [false], 100).

prop_sum(Inc) ->
    ?FORALL(
       {Elements, Number}, {non_empty(elements_gen()), ?SUCHTHAT(I, int(), I > 0)},
       begin
           Interval = 1,
           {LastTS, Slide} = new_slide(Interval, Inc, Elements),
           %% Add the same so the timestamp matches. As the timestamps are handled
           %% internally, we cannot guarantee on which interval they go otherwise
           %% (unless we manually manipulate the slide content).
           Sum = exometer_slide:sum([Slide || _ <- lists:seq(1, Number)]),
           Values = [V || {_TS, V} <- exometer_slide:to_list(LastTS + 1, Sum)],
           Expected = expected_sum(Slide, LastTS + 1, Number, Interval, Inc),
           ?WHENFAIL(io:format("Expected: ~p~nGot: ~p~nSlide:~p~n",
                               [Expected, Values, Slide]),
                     Values == Expected)
       end).

expected_sum(Slide, Now, Number, _Int, false) ->
    [sum_n_times(V, Number) || {_TS, V} <- exometer_slide:to_list(Now, Slide)];
expected_sum(Slide, Now, Number, Int, true) ->
    [{TSfirst, First} = F | Rest] = All = exometer_slide:to_list(Now, Slide),
    {TSlast, _Last} = case Rest of
                         [] ->
                             F;
                         _ ->
                             lists:last(Rest)
                     end,
    Seq = lists:seq(TSfirst, TSlast, Int),
    {Expected, _} = lists:foldl(fun(TS0, {Acc, Previous}) ->
                                        Actual = proplists:get_value(TS0, All, Previous),
                                        {[sum_n_times(Actual, Number) | Acc], Actual}
                                end, {[], First}, Seq),
    lists:reverse(Expected).
%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------
new_slide(Interval, Incremental, Elements) ->
    new_slide(Interval, Interval, Incremental, Elements).

new_slide(PublishInterval, Interval, Incremental, Elements) ->
    Now = 0,
    Slide = exometer_slide:new(Now, 60 * 1000, [{interval, Interval},
                                                {incremental, Incremental}]),
    lists:foldl(
      fun(E, {TS0, Acc}) ->
              TS1 = TS0 + PublishInterval,
              {TS1, exometer_slide:add_element(TS1, E, Acc)}
      end, {Now, Slide}, Elements).

last_two(Elements) when length(Elements) >= 2 ->
    [F, S | _] = lists:reverse(Elements),
    [F, S];
last_two(Elements) ->
    Elements.

add_elements([H | T]) ->
    add_elements(T, H).

add_elements([], Acc) ->
    Acc;
add_elements([Tuple | T], Acc) ->
    add_elements(T, sum(Tuple, Acc)).

sum(T1, T2) ->
    list_to_tuple(lists:zipwith(fun(A, B) -> A + B end, tuple_to_list(T1), tuple_to_list(T2))).

sum_n_times(V, N) ->
    sum_n_times(V, V, N - 1).

sum_n_times(_V, Acc, 0) ->
    Acc;
sum_n_times(V, Acc, N) ->
    sum_n_times(V, sum(V, Acc), N-1).
