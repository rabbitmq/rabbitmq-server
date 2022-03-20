%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2016-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(exometer_slide_SUITE).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
     {group, parallel_tests}
    ].

groups() ->
    [
     {parallel_tests, [parallel], [
                  incremental_add_element_basics,
                  last_two_normalises_old_sample_timestamp,
                  incremental_last_two_returns_last_two_completed_samples,
                  incremental_sum,
                  incremental_sum_stale,
                  incremental_sum_stale2,
                  incremental_sum_with_drop,
                  incremental_sum_with_total,
                  foldl_realises_partial_sample,
                  foldl_and_to_list,
                  foldl_and_to_list_incremental,
                  optimize,
                  stale_to_list,
                  to_list_single_after_drop,
                  to_list_drop_and_roll,
                  to_list_with_drop,
                  to_list_simple,
                  foldl_with_drop,
                  sum_single,
                  to_normalized_list,
                  to_normalized_list_no_padding,
                  to_list_in_the_past,
                  sum_mgmt_352,
                  sum_mgmt_352_extra,
                  sum_mgmt_352_peak
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

    [] = exometer_slide:to_list(Now, S0),
    % add element before next interval
    S1 = exometer_slide:add_element(Now + 10, {1}, S0),

    [] = exometer_slide:to_list(Now + 20, S1),

    %% to_list is not empty, as we take the 'real' total if a full interval has passed
    Now100 = Now + 100,
    [{Now100, {1}}] = exometer_slide:to_list(Now100, S1),

    Then = Now + 101,
    % add element after interval
    S2 = exometer_slide:add_element(Then, {1}, S1),

    % contains single element with incremented value
    [{Then, {2}}] = exometer_slide:to_list(Then, S2).

last_two_normalises_old_sample_timestamp(_Config) ->
    Now = 0,
    S0 = exometer_slide:new(Now, 10, [{incremental, true},
                                      {interval, 100}]),

    S1 = exometer_slide:add_element(100, {1}, S0),
    S2 = exometer_slide:add_element(500, {1}, S1),

    [{500, {2}}, {400, {1}}] = exometer_slide:last_two(S2).


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

incremental_sum_stale2(_Config) ->
    Now = 0,
    Slide = exometer_slide:new(Now, 25, [{incremental, true},
                                         {max_n, 5},
                                         {interval, 5}]),

    S1 = lists:foldl(fun (Next, S) ->
                              exometer_slide:add_element(Now + Next, {1}, S)
                     end, Slide, [5]),

    S2 = lists:foldl(fun (Next, S) ->
                              exometer_slide:add_element(Now + Next, {1}, S)
                     end, Slide, [500, 505, 510, 515, 520, 525, 527]),
    S3 = exometer_slide:sum([S1, S2], {0}),
    [500, 505, 510, 515, 520, 525] = [T || {T, _} <- exometer_slide:to_list(525, S3)],
    [7,6,5,4,3,2] = lists:reverse([V || {_, {V}} <- exometer_slide:to_list(525, S3)]).

incremental_sum_with_drop(_Config) ->
    Now = 0,
    Slide = exometer_slide:new(Now, 25, [{incremental, true},
                                         {max_n, 5},
                                         {interval, 5}]),

    S1 = lists:foldl(fun ({Next, Incr}, S) ->
                              exometer_slide:add_element(Now + Next, {Incr}, S)
                     end, Slide, [{1, 1}, {8, 0}, {15, 0}, {21, 1}, {27, 0}]),

    S2 = lists:foldl(fun (Next, S) ->
                              exometer_slide:add_element(Now + Next, {1}, S)
                     end, Slide, [2, 7, 14, 20, 25]),
    S3 = exometer_slide:sum([S1, S2]),
    [27,22,17,12,7] = lists:reverse([T || {T, _} <- exometer_slide:to_list(27, S3)]),
    [7,6,4,3,2] = lists:reverse([V || {_, {V}} <- exometer_slide:to_list(27, S3)]).

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
        exometer_slide:foldl(20, 5, Fun, [], S),
    % do not realise sample unless Now is at least an interval beyond the last
    % full sample
    [{20, 4}, {15, 3}, {10, 2}, {5, 1}] =
        exometer_slide:foldl(23, 5, Fun, [], S).

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

to_list_simple(_Config) ->
    Now = 0,
    Slide = exometer_slide:new(Now, 25, [{interval, 5},
                                         {incremental, true},
                                         {max_n, 5}]),
    S = exometer_slide:add_element(30, {0}, Slide),
    S2 = exometer_slide:add_element(35, {0}, S),
    [{30, {0}}, {35, {0}}] = exometer_slide:to_list(38, S2).

foldl_with_drop(_Config) ->
    Now = 0,
    Slide = exometer_slide:new(Now, 25, [{interval, 5},
                                         {incremental, true},
                                         {max_n, 5}]),
    S = exometer_slide:add_element(30, {1}, Slide),
    S2 = exometer_slide:add_element(35, {1}, S),
    S3 = exometer_slide:add_element(40, {0}, S2),
    S4 = exometer_slide:add_element(45, {0}, S3),
    Fun = fun(last, Acc) -> Acc;
              ({TS, {X}}, Acc) -> [{TS, X} | Acc]
          end,
    [{45, 2}, {40, 2}, {35, 2}, {30, 1}] =
        exometer_slide:foldl(45, 30, Fun, [], S4).

foldl_and_to_list(_Config) ->
    Now = 0,
    Tests = [ % {input, expected, query range}
             {[],
              [],
              {0, 10}},
             {[{5, 1}],
              [{5, {1}}],
              {0, 5}},
             {[{10, 1}],
              [{10, {1}}],
              {0, 10}},
             {[{5, 1}, {10, 2}],
              [{10, {2}}, {5, {1}}],
              {0, 10}},
             {[{5, 0}, {10, 0}], % drop 1
              [{10, {0}}, {5, {0}}],
              {0, 10}},
             {[{5, 2}, {10, 1}, {15, 1}], % drop 2
              [{15, {1}}, {10, {1}}, {5, {2}}],
              {0, 15}},
             {[{10, 0}, {15, 0}, {20, 0}], % drop
              [{20, {0}}, {15, {0}}, {10, {0}}],
              {0, 20}},
             {[{5, 1}, {10, 5}, {15, 5}, {20, 0}], % drop middle
              [{20, {0}}, {15, {5}}, {10, {5}}, {5, {1}}],
              {0, 20}},
             {[{5, 1}, {10, 5}, {15, 5}, {20, 1}], % drop middle filtered
              [{20, {1}}, {15, {5}}, {10, {5}}],
              {10, 20}},
             {[{5, 1}, {10, 2}, {15, 3}, {20, 4}, {25, 4}, {30, 5}], % buffer roll over
              [{30, {5}}, {25, {4}}, {20, {4}}, {15, {3}}, {10, {2}}, {5, {1}}],
              {5, 30}}
            ],
    Slide = exometer_slide:new(Now, 25, [{interval, 5},
                                         {max_n, 5}]),
    Fun = fun(last, Acc) -> Acc;
             (V, Acc) -> [V | Acc]
          end,
    [begin
        S = lists:foldl(fun ({T, V}, Acc) ->
                            exometer_slide:add_element(T, {V}, Acc)
                        end, Slide, Inputs),
        Expected = exometer_slide:foldl(To, From, Fun, [], S),
        ExpRev = lists:reverse(Expected),
        ExpRev = exometer_slide:to_list(To, From, S)
     end || {Inputs, Expected, {From, To}} <- Tests].

foldl_and_to_list_incremental(_Config) ->
    Now = 0,
    Tests = [ % {input, expected, query range}
             {[],
              [],
              {0, 10}},
             {[{5, 1}],
              [{5, {1}}],
              {0, 5}},
             {[{10, 1}],
              [{10, {1}}],
              {0, 10}},
             {[{5, 1}, {10, 1}],
              [{10, {2}}, {5, {1}}],
              {0, 10}},
             {[{5, 0}, {10, 0}], % drop 1
              [{10, {0}}, {5, {0}}],
              {0, 10}},
             {[{5, 1}, {10, 0}, {15, 0}], % drop 2
              [{15, {1}}, {10, {1}}, {5, {1}}],
              {0, 15}},
             {[{10, 0}, {15, 0}, {20, 0}], % drop
              [{20, {0}}, {15, {0}}, {10, {0}}],
              {0, 20}},
             {[{5, 1}, {10, 0}, {15, 0}, {20, 1}], % drop middle
              [{20, {2}}, {15, {1}}, {10, {1}}, {5, {1}}],
              {0, 20}},
             {[{5, 1}, {10, 0}, {15, 0}, {20, 1}], % drop middle filtered
              [{20, {2}}, {15, {1}}, {10, {1}}],
              {10, 20}},
             {[{5, 1}, {10, 1}, {15, 1}, {20, 1}, {25, 0}, {30, 1}], % buffer roll over
              [{30, {5}}, {25, {4}}, {20, {4}}, {15, {3}}, {10, {2}}, {5, {1}}],
              {5, 30}}
            ],
    Slide = exometer_slide:new(Now, 25, [{interval, 5},
                                         {incremental, true},
                                         {max_n, 5}]),
    Fun = fun(last, Acc) -> Acc;
             (V, Acc) -> [V | Acc]
          end,
    [begin
        S = lists:foldl(fun ({T, V}, Acc) ->
                            exometer_slide:add_element(T, {V}, Acc)
                        end, Slide, Inputs),
        Expected = exometer_slide:foldl(To, From, Fun, [], S),
        ExpRev = lists:reverse(Expected),
        ExpRev = exometer_slide:to_list(To, From, S)
     end || {Inputs, Expected, {From, To}} <- Tests].

stale_to_list(_Config) ->
    Now = 0,
    Slide = exometer_slide:new(Now, 25, [{interval, 5}, {max_n, 5}]),
    S = exometer_slide:add_element(50, {1}, Slide),
    S2 = exometer_slide:add_element(55, {1}, S),
    [] = exometer_slide:to_list(100, S2).

to_list_single_after_drop(_Config) ->
    Now = 0,
    Slide = exometer_slide:new(Now, 25, [{interval, 5},
                                         {incremental, true},
                                         {max_n, 5}]),
    S = exometer_slide:add_element(5, {0}, Slide),
    S2 = exometer_slide:add_element(10, {0}, S),
    S3 = exometer_slide:add_element(15, {1}, S2),
    Res = exometer_slide:to_list(17, S3),
    [{5,{0}},{10,{0}},{15,{1}}] = Res.


to_list_drop_and_roll(_Config) ->
    Now = 0,
    Slide = exometer_slide:new(Now, 10, [{interval, 5},
                                         {incremental, true},
                                         {max_n, 5}]),
    S = exometer_slide:add_element(5, {0}, Slide),
    S2 = exometer_slide:add_element(10, {0}, S),
    S3 = exometer_slide:add_element(15, {0}, S2),
    [{10, {0}}, {15, {0}}] = exometer_slide:to_list(17, S3).


sum_single(_Config) ->
    Now = 0,
    Slide = exometer_slide:new(Now, 25, [{interval, 5},
                                         {incremental, true},
                                         {max_n, 5}]),
    S = exometer_slide:add_element(Now + 5, {0}, Slide),
    S2 = exometer_slide:add_element(Now + 10, {0}, S),
    Summed = exometer_slide:sum([S2]),
    [_,_] = exometer_slide:to_list(15, Summed).


to_normalized_list(_Config) ->
    Interval = 5,
    Tests = [ % {input, expected, query range}
             {[], % zero pad when slide has never seen any samples
              [{10, {0}}, {5, {0}}, {0, {0}}],
              {0, 10}},
             {[{5, 1}], % zero pad before first known sample
              [{5, {1}}, {0, {0}}],
              {0, 5}},
             {[{10, 1}, {15, 1}], % zero pad before last know sample
              [{15, {2}}, {10, {1}}, {5, {0}}],
              {5, 15}},
             {[{5, 1}, {15, 1}], % insert missing sample using previous total
              [{15, {2}}, {10, {1}}, {5, {1}}],
              {5, 15}},
             % {[{6, 1}, {11, 1}, {16, 1}], % align timestamps with query
             %  [{15, {3}}, {10, {2}}, {5, {1}}, {0, {0}}],
             %  {0, 15}},
             {[{5, 1}, {10, 1}, {15, 1}, {20, 1}, {25, 1}, {30, 1}], % outside of max_n
              [{30, {6}}, {25, {5}}, {20, {4}}, {15, {3}}, {10, {2}}], % we cannot possibly be expected deduce what 10 should be
              {10, 30}},
             {[{5, 1}, {20, 1}, {25, 1}], % as long as the past TS 5 sample still exists we should use to for padding
              [{25, {3}}, {20, {2}}, {15, {1}}, {10, {1}}],
              {10, 25}},
             {[{5, 1}, {10, 1}], % pad based on total
              [{35, {2}}, {30, {2}}],
              {30, 35}},
             {[{5, 1}], %  make up future values to fill the window
              [{10, {1}}, {5, {1}}],
              {5, 10}},
             {[{5, 1}, {7, 1}], % realise last sample
              [{10, {2}}, {5, {1}}],
              {5, 10}}
            ],

    Slide = exometer_slide:new(0, 20, [{interval, 5},
                                       {incremental, true},
                                       {max_n, 4}]),
    [begin
        S0 = lists:foldl(fun ({T, V}, Acc) ->
                            exometer_slide:add_element(T, {V}, Acc)
                        end, Slide, Inputs),
        Expected = exometer_slide:to_normalized_list(To, From, Interval, S0, {0}),
        S = exometer_slide:sum([exometer_slide:optimize(S0)], {0}), % also test it post sum
        Expected = exometer_slide:to_normalized_list(To, From, Interval, S, {0})
     end || {Inputs, Expected, {From, To}} <- Tests].

to_normalized_list_no_padding(_Config) ->
    Interval = 5,
    Tests = [ % {input, expected, query range}
             {[],
              [],
              {0, 10}},
             {[{5, 1}],
              [{5, {1}}],
              {0, 5}},
             {[{5, 1}, {15, 1}],
              [{15, {2}}, {10, {1}}, {5, {1}}],
              {5, 15}},
             {[{10, 1}, {15, 1}],
              [{15, {2}}, {10, {1}}],
              {5, 15}},
             {[{5, 1}, {20, 1}], % NB as 5 is outside of the query we can't pick the value up
              [{20, {2}}],
              {10, 20}}
            ],

    Slide = exometer_slide:new(0, 20, [{interval, 5},
                                       {incremental, true},
                                       {max_n, 4}]),
    [begin
        S = lists:foldl(fun ({T, V}, Acc) ->
                            exometer_slide:add_element(T, {V}, Acc)
                        end, Slide, Inputs),
        Expected = exometer_slide:to_normalized_list(To, From, Interval, S, no_pad)
     end || {Inputs, Expected, {From, To}} <- Tests].

to_list_in_the_past(_Config) ->
    Slide = exometer_slide:new(0, 20, [{interval, 5},
                                       {incremental, true},
                                       {max_n, 4}]),
    % ensure firstTS is way in the past
    S0 = exometer_slide:add_element(5, {1}, Slide),
    S1 = exometer_slide:add_element(105, {0}, S0),
    S = exometer_slide:add_element(110, {0}, S1), % create drop
    % query into the past
    % this could happen if a node with and incorrect clock joins the cluster
    [] = exometer_slide:to_list(50, 10, S).

sum_mgmt_352(_Config) ->
    %% In bug mgmt#352 all the samples returned have the same vale
    Slide = sum_mgmt_352_slide(),
    Last = 1487689330000,
    First = 1487689270000,
    Incr = 5000,
    Empty = {0},
    Sum = exometer_slide:sum(Last, First, Incr, [Slide], Empty),
    Values = sets:to_list(sets:from_list(
                            [V || {_, V} <- exometer_slide:buffer(Sum)])),
    true = (length(Values) == 12),
    ok.

sum_mgmt_352_extra(_Config) ->
    %% Testing previous case clause to the one that fixes mgmt_352
    %% exometer_slide.erl#L463
    %% In the buggy version, all but the last sample are the same
    Slide = sum_mgmt_352_slide_extra(),
    Last = 1487689330000,
    First = 1487689260000,
    Incr = 5000,
    Empty = {0},
    Sum = exometer_slide:sum(Last, First, Incr, [Slide], Empty),
    Values = sets:to_list(sets:from_list(
                            [V || {_, V} <- exometer_slide:buffer(Sum)])),
    true = (length(Values) == 13),
    ok.

sum_mgmt_352_peak(_Config) ->
    %% When buf2 contains data, we were returning a too old sample that
    %% created a massive rate peak at the beginning of the graph.
    %% The sample used was captured during a debug session.
    Slide = sum_mgmt_352_slide_peak(),
    Last = 1487752040000,
    First = 1487751980000,
    Incr = 5000,
    Empty = {0},
    Sum = exometer_slide:sum(Last, First, Incr, [Slide], Empty),
    [{LastV}, {BLastV} | _] =
        lists:reverse([V || {_, V} <- exometer_slide:buffer(Sum)]),
    Rate = (BLastV - LastV) div 5,
    true = (Rate < 20000),
    ok.

%% -------------------------------------------------------------------
%% Util
%% -------------------------------------------------------------------

ele(TS, V) -> {TS, {V}}.

%% -------------------------------------------------------------------
%% Data
%% -------------------------------------------------------------------
sum_mgmt_352_slide() ->
    %% Provide slide as is, from a debug session triggering mgmt-352 bug
    {slide,610000,45,122,true,5000,1487689328468,1487689106834,
     [{1487689328468,{1574200}},
      {1487689323467,{1538800}},
      {1487689318466,{1500800}},
      {1487689313465,{1459138}},
      {1487689308463,{1419200}},
      {1487689303462,{1379600}},
      {1487689298461,{1340000}},
      {1487689293460,{1303400}},
      {1487689288460,{1265600}},
      {1487689283458,{1231400}},
      {1487689278457,{1215800}},
      {1487689273456,{1215200}},
      {1487689262487,drop},
      {1487689257486,{1205600}}],
     [],
     {1591000}}.

sum_mgmt_352_slide_extra() ->
    {slide,610000,45,122,true,5000,1487689328468,1487689106834,
     [{1487689328468,{1574200}},
      {1487689323467,{1538800}},
      {1487689318466,{1500800}},
      {1487689313465,{1459138}},
      {1487689308463,{1419200}},
      {1487689303462,{1379600}},
      {1487689298461,{1340000}},
      {1487689293460,{1303400}},
      {1487689288460,{1265600}},
      {1487689283458,{1231400}},
      {1487689278457,{1215800}},
      {1487689273456,{1215200}},
      {1487689272487,drop},
      {1487689269486,{1205600}}],
     [],
     {1591000}}.

sum_mgmt_352_slide_peak() ->
    {slide,610000,96,122,true,5000,1487752038481,1487750936863,
     [{1487752038481,{11994024}},
      {1487752033480,{11923200}},
      {1487752028476,{11855800}},
      {1487752023474,{11765800}},
      {1487752018473,{11702431}},
      {1487752013472,{11636200}},
      {1487752008360,{11579800}},
      {1487752003355,{11494800}},
      {1487751998188,{11441400}},
      {1487751993184,{11381000}},
      {1487751988180,{11320000}},
      {1487751983178,{11263000}},
      {1487751978177,{11187600}},
      {1487751973172,{11123375}},
      {1487751968167,{11071800}},
      {1487751963166,{11006200}},
      {1487751958162,{10939477}},
      {1487751953161,{10882400}},
      {1487751948140,{10819600}},
      {1487751943138,{10751200}},
      {1487751938134,{10744400}},
      {1487751933129,drop},
      {1487751927807,{10710200}},
      {1487751922803,{10670000}}],
     [{1487751553386,{6655800}},
      {1487751548385,{6580365}},
      {1487751543384,{6509358}}],
     {11994024}}.
