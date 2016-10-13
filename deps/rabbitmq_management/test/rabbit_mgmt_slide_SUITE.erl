%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% Copyright (c) 2016 Pivotal Software, Inc.  All rights reserved.
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
     {parallel_tests, [], [
			   last_two_test,
			   last_two_incremental_test,
			   sum_test,
			   sum_incremental_test,
			   foldl_incremental_test
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
    ?FORALL(Elements, elements_gen(),
	    begin
		Slide = exometer_slide:new(60 * 1000, [{interval, 1},
                                               {incremental, false}]),
		Slide1 = lists:foldl(fun(E, Acc) ->
					     timer:sleep(1), %% ensure we are past interval
					     exometer_slide:add_element(E, Acc)
				     end, Slide, Elements),
		LastTwo = last_two(Elements),
		ValuesOnly = [V || {_Timestamp, V} <- exometer_slide:last_two(Slide1)],
		LastTwo == ValuesOnly
	    end).

last_two_incremental_test(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_last_two_incremental/0, [], 100).

prop_last_two_incremental() ->
    ?FORALL(Elements, non_empty(elements_gen()),
	    begin
		Slide = exometer_slide:new(60 * 1000, [{interval, 1},
                                               {incremental, true}]),
		Slide1 = lists:foldl(fun(E, Acc) ->
					     timer:sleep(1), %% ensure we are past interval
					     exometer_slide:add_element(E, Acc)
				     end, Slide, Elements),
		[{_Timestamp, Values} | _] = exometer_slide:last_two(Slide1),
		Values == add_elements(Elements)
	    end).

sum_incremental_test(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_sum/1, [true], 100).

sum_test(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_sum/1, [false], 100).

prop_sum(Inc) ->
    ?FORALL({Elements, Number}, {non_empty(elements_gen()), ?SUCHTHAT(I, int(), I > 0)},
	    begin
		Slide = exometer_slide:new(60 * 1000, [{interval, 1},
                                               {incremental, Inc}]),
		Slide1 = lists:foldl(fun(E, Acc) ->
                                 timer:sleep(1), %% ensure we are past interval
                                 exometer_slide:add_element(E, Acc)
                             end, Slide, Elements),
		%% Add the same so the timestamp matches. As the timestamps are handled
		%% internally, we cannot guarantee on which interval they go otherwise
		%% (unless we manually manipulate the slide content).
		Sum = exometer_slide:sum([Slide1 || _ <- lists:seq(1, Number)]),
		Values = [V || {_TS, V} <- exometer_slide:to_list(Sum)],
		Expected = [sum_n_times(V, Number) || {_TS, V} <- exometer_slide:to_list(Slide1)],
		Values == Expected
	    end).

foldl_incremental_test(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_foldl_incremental/0, [], 100).

prop_foldl_incremental() ->
    ?FORALL({Elements, Int}, {non_empty(elements_gen()), ?SUCHTHAT(N, nat(), (N > 0) and (N < 10))},
	    begin
		Slide = exometer_slide:new(1000, [{interval, Int},
						  {incremental, true}]),
		Slide1 = lists:foldl(fun(E, Acc) ->
					     %% sometimes the data will be within the intervals
					     %% and sometimes not
					     timer:sleep(1),
					     exometer_slide:add_element(E, Acc)
				     end, Slide, Elements),
		[{_Timestamp, Values} | _] = exometer_slide:foldl(
					       fun(V, Acc) -> [V | Acc] end,
					       [], Slide1),
		%% In an incremental, the last one is always reported as the total
		Values == add_elements(Elements)
	    end).

%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------
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
