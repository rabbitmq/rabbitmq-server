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

-module(exometer_slide_SUITE).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
     {group, tests}
    ].

groups() ->
    [
     {tests, [], [
                  incremental_add_element_basics,
                  incremental_last_two_returns_last_two_completed_samples,
                  incremental_sum,
                  incremental_sum_stale,
                  incremental_sum_with_drop,
                  incremental_sum_with_total,
                  foldl_realises_partial_sample,
                  optimize,
                  stale_to_list,
                  to_list_with_drop
                 ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------
init_per_suite(Config) ->
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

%% TODO: turn tests into properties

incremental_add_element_basics(_Config) ->
    Now = exometer_slide:timestamp(),
    S0 = exometer_slide:new(Now, 10, [{incremental, true},
                                      {interval, 100}]),

    [] = exometer_slide:to_list(S0),
    % add element before next interval
    S1 = exometer_slide:add_element(Now + 10, {1}, S0),
    %% to_list is empty
    [] = exometer_slide:to_list(S1),

    Then = Now + 101,
    % add element after interval
    S2 = exometer_slide:add_element(Then, {1}, S1),

    % contains single element with incremented value
    [{Then, {2}}] = exometer_slide:to_list(S2).

incremental_last_two_returns_last_two_completed_samples(_Config) ->
    Now = exometer_slide:timestamp(),
    S0 = exometer_slide:new(Now, 10, [{incremental, true},
                                      {interval, 100}]),

    % add two full elements then a partial
    Now100 = Now + 100,
    Now200 = Now + 200,
    S1 = exometer_slide:add_element(Now100, {1}, S0),
    S2 = exometer_slide:add_element(Now200, {1}, S1),
    S3 = exometer_slide:add_element(Now + 210, {1}, S2),
    %% to_list is empty
    [{Now200, {2}}, {Now100, {1}}] = exometer_slide:last_two(S3).

incremental_sum(_Config) ->
    Now = exometer_slide:timestamp(),
    S1 = lists:foldl(fun (Next, S) ->
                              exometer_slide:add_element(Now + Next, {1}, S)
                     end,
                     exometer_slide:new(Now, 1000, [{incremental, true}, {interval, 100}]),
                     lists:seq(100, 1000, 100)),
    Now50 = Now - 50,
    S2 = lists:foldl(fun (Next, S) ->
                              exometer_slide:add_element(Now50 + Next, {1}, S)
                     end,
                     exometer_slide:new(Now50, 1000, [{incremental, true}, {interval, 100}]),
                     lists:seq(100, 1000, 100)),
    S3 = exometer_slide:sum([S1, S2]),

    10 = length(exometer_slide:to_list(Now + 1000, S1)),
    10 = length(exometer_slide:to_list(Now + 1000, S2)),
    10 = length(exometer_slide:to_list(Now + 1000, S3)).

incremental_sum_stale(_Config) ->
    Now = 0,
    Slide = exometer_slide:new(Now, 25, [{incremental, true}, {interval, 5}]),

    S1 = lists:foldl(fun (Next, S) ->
                              exometer_slide:add_element(Now + Next, {1}, S)
                     end, Slide, [1, 8, 15, 21, 27]),

    S2 = lists:foldl(fun (Next, S) ->
                              exometer_slide:add_element(Now + Next, {1}, S)
                     end, Slide, [2, 7, 14, 20, 25]),
    S3 = exometer_slide:sum([S1, S2]),
    [27,22,17,12,7] = lists:reverse([T || {T, _} <- exometer_slide:to_list(27, S3)]),
    [10,8,6,4,2] = lists:reverse([V || {_, {V}} <- exometer_slide:to_list(27, S3)]).

incremental_sum_with_drop(_Config) ->
    Now = 0,
    Slide = exometer_slide:new(Now, 25, [{incremental, true}, {interval, 5}]),

    S1 = lists:foldl(fun ({Next, Incr}, S) ->
                              exometer_slide:add_element(Now + Next, {Incr}, S)
                     end, Slide, [{1, 1}, {8, 0}, {15, 0}, {21, 1}, {27, 0}]),

    S2 = lists:foldl(fun (Next, S) ->
                              exometer_slide:add_element(Now + Next, {1}, S)
                     end, Slide, [2, 7, 14, 20, 25]),
    S3 = exometer_slide:sum([S1, S2]),
    [27,22,17,12,7] = lists:reverse([T || {T, _} <- exometer_slide:to_list(27, S3)]),
    [7,6,4,3,3] = lists:reverse([V || {_, {V}} <- exometer_slide:to_list(27, S3)]).

incremental_sum_with_total(_Config) ->
    Now = 0,
    Slide = exometer_slide:new(Now, 50, [{incremental, true}, {interval, 5}]),

    S1 = lists:foldl(fun (Next, S) ->
                              exometer_slide:add_element(Now + Next, {1}, S)
                     end, Slide, [5, 10, 15, 20, 25]),

    S2 = lists:foldl(fun (Next, S) ->
                              exometer_slide:add_element(Now + Next, {1}, S)
                     end, Slide, [7, 12, 17, 22, 23]),
    S3 = exometer_slide:sum([S1, S2]),
    {10} = exometer_slide:last(S3),
    [25,20,15,10,5] = lists:reverse([T || {T, _} <- exometer_slide:to_list(26, S3)]),
    [ 9, 7, 5, 3,1] = lists:reverse([V || {_, {V}} <- exometer_slide:to_list(26, S3)]).

foldl_realises_partial_sample(_Config) ->
    Now = 0,
    Slide = exometer_slide:new(Now, 25, [{incremental, true}, {interval, 5}]),
    S = lists:foldl(fun (Next, S) ->
                              exometer_slide:add_element(Now + Next, {1}, S)
                    end, Slide, [5, 10, 15, 20, 23]),
    Fun = fun(last, Acc) -> Acc;
              ({TS, {X}}, Acc) -> [{TS, X} | Acc]
          end,
    [{25, 5}, {20, 4}, {15, 3}, {10, 2}, {5, 1}] =
        exometer_slide:foldl(25, 5, Fun, [], S),
    [{20, 4}, {15, 3}, {10, 2}, {5, 1}] =
        exometer_slide:foldl(20, 5, Fun, [], S).

optimize(_Config) ->
    Now = 0,
    Slide = exometer_slide:new(Now, 25, [{interval, 5}, {max_n, 5}]),
    S = lists:foldl(fun (Next, S) ->
                              exometer_slide:add_element(Now + Next, {Next}, S)
                    end, Slide, [5, 10, 15, 20, 25, 30, 35]),
    OS = exometer_slide:optimize(S),
    SRes = exometer_slide:to_list(35, S),
    OSRes = exometer_slide:to_list(35, OS),
    SRes = OSRes,
    ?assert(S =/= OS).

to_list_with_drop(_Config) ->
    Now = 0,
    Slide = exometer_slide:new(Now, 25, [{interval, 5},
                                         {incremental, true},
                                         {max_n, 5}]),
    S = exometer_slide:add_element(30, {1}, Slide),
    S2 = exometer_slide:add_element(35, {1}, S),
    S3 = exometer_slide:add_element(40, {0}, S2),
    S4 = exometer_slide:add_element(45, {0}, S3),
    [{30, {1}}, {35, {2}}, {40, {2}}, {45, {2}}] = exometer_slide:to_list(45, S4).

stale_to_list(_Config) ->
    Now = 0,
    Slide = exometer_slide:new(Now, 25, [{interval, 5}, {max_n, 5}]),
    S = exometer_slide:add_element(50, {1}, Slide),
    S2 = exometer_slide:add_element(55, {1}, S),
    [] = exometer_slide:to_list(100, S2).


%% -------------------------------------------------------------------
%% Util
%% -------------------------------------------------------------------

ele(TS, V) -> {TS, {V}}.
