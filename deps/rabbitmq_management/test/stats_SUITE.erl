%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2016-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(stats_SUITE).

-include_lib("proper/include/proper.hrl").
-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").

-compile(export_all).

-import(rabbit_misc, [pget/2]).

all() ->
    [
     {group, parallel_tests}
    ].

groups() ->
    [
     {parallel_tests, [parallel], [
                                   format_range_empty_slide,
                                   format_range,
                                   format_range_missing_middle,
                                   format_range_missing_middle_drop,
                                   format_range_incremental_pad,
                                   format_range_incremental_pad2,
                                   format_range_constant
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

format_range_empty_slide(_Config) ->
    Slide = exometer_slide:new(0, 60000, [{incremental, true},
                                          {interval, 5000}]),
    Range = #range{first = 100, last = 200, incr = 10},
    Table = vhost_stats_fine_stats,
    SamplesFun = fun() -> [Slide] end,
    Got = rabbit_mgmt_stats:format_range(Range, 200, Table, 0, fun() ->
                                                                       ok
                                                               end,
                                         SamplesFun),
    PublishDetails = proplists:get_value(publish_details, Got),
    Samples = proplists:get_value(samples, PublishDetails),
    11 = length(Samples).

format_range(_Config) ->
    Slide = exometer_slide:new(0, 60000, [{incremental, true},
                                          {interval, 10}]),
    Slide1 = exometer_slide:add_element(197, {10}, Slide),
    Range = #range{first = 100, last = 200, incr = 10},
    Table = queue_stats_publish,
    SamplesFun = fun() -> [Slide1] end,
    Got = rabbit_mgmt_stats:format_range(Range, 200, Table, 0, fun() ->
                                                                       ok
                                                               end,
                                         SamplesFun),
    PublishDetails = proplists:get_value(publish_details, Got),
    [S1, S2 | _Rest] = Samples = proplists:get_value(samples, PublishDetails),
    0 = proplists:get_value(sample, S2),
    10 = proplists:get_value(sample, S1),
    11 = length(Samples).

format_range_missing_middle(_Config) ->
    Slide = exometer_slide:new(0, 60000, [{incremental, false},
                                          {interval, 10}]),
    Slide1 = exometer_slide:add_element(167, {10}, Slide),
    Slide2 = exometer_slide:add_element(197, {5}, Slide1),
    Range = #range{first = 100, last = 200, incr = 10},
    Table = queue_stats_publish,
    SamplesFun = fun() -> [Slide2] end,
    Got = rabbit_mgmt_stats:format_range(Range, 200, Table, 0, fun() ->
                                                                       ok
                                                               end,
                                         SamplesFun),
    PublishDetails = proplists:get_value(publish_details, Got),
    [S1, S2, S3, S4 | Rest] = Samples = proplists:get_value(samples, PublishDetails),
    10 = proplists:get_value(sample, S4),
    10 = proplists:get_value(sample, S3),
    10 = proplists:get_value(sample, S2),
    5 = proplists:get_value(sample, S1),
    true = lists:all(fun(P) ->
                             0 == proplists:get_value(sample, P)
                     end, Rest),
    11 = length(Samples).

format_range_missing_middle_drop(_Config) ->
    Slide = exometer_slide:new(0, 60000, [{incremental, false},
                                          {max_n, 12},
                                          {interval, 10}]),
    Slide1 = exometer_slide:add_element(167, {10}, Slide),
    Slide2 = exometer_slide:add_element(197, {10}, Slide1),
    Range = #range{first = 100, last = 200, incr = 10},
    Table = queue_stats_publish,
    SamplesFun = fun() -> [Slide2] end,
    Got = rabbit_mgmt_stats:format_range(Range, 200, Table, 0, fun() ->
                                                                       ok
                                                               end,
                                         SamplesFun),
    PublishDetails = proplists:get_value(publish_details, Got),
    [S1, S2, S3, S4 | Rest] = Samples = proplists:get_value(samples, PublishDetails),
    10 = proplists:get_value(sample, S4),
    10 = proplists:get_value(sample, S3),
    10 = proplists:get_value(sample, S2),
    10 = proplists:get_value(sample, S1),
    true = lists:all(fun(P) ->
                             0 == proplists:get_value(sample, P)
                     end, Rest),
    11 = length(Samples).

format_range_incremental_pad(_Config) ->
    Slide = exometer_slide:new(0, 10, [{incremental, true},
                                       {interval, 5}]),
    Slide1 = exometer_slide:add_element(15, {3}, Slide),
    Range = #range{first = 5, last = 15, incr = 5},
    Table = queue_stats_publish,
    SamplesFun = fun() -> [Slide1] end,
    Got = rabbit_mgmt_stats:format_range(Range, 0, Table, 0, fun() -> ok end,
                                         SamplesFun),
    PublishDetails = proplists:get_value(publish_details, Got),
    [{3, 15}, {0,10}, {0, 5}] = [{pget(sample, V), pget(timestamp, V)}
                                 || V <- pget(samples, PublishDetails)].

format_range_incremental_pad2(_Config) ->
    Slide = exometer_slide:new(0, 10, [{incremental, true},
                                       {interval, 5}]),
    {_, Slide1} = lists:foldl(fun (V,  {TS, S}) ->
                                {TS + 5, exometer_slide:add_element(TS, {V}, S)}
                              end, {5, Slide}, [1,1,0,0,0,1]),
    Range = #range{first = 10, last = 30, incr = 5},
    Table = queue_stats_publish,
    SamplesFun = fun() -> [Slide1] end,
    Got = rabbit_mgmt_stats:format_range(Range, 0, Table, 0, fun() -> ok end,
                                         SamplesFun),
    PublishDetails = pget(publish_details, Got),
    [{3, 30}, {2, 25}, {2, 20}, {2, 15}, {2, 10}] =
        [{pget(sample, V), pget(timestamp, V)}
         || V <- pget(samples, PublishDetails)].

format_range_constant(_Config) ->
    Now = 0,
    Slide = exometer_slide:new(0, 20, [{incremental, false},
                                       {max_n, 100},
                                       {interval, 5}]),
    Slide1 = lists:foldl(fun(N, Acc) ->
                                 exometer_slide:add_element(Now + N, {5}, Acc)
                         end, Slide, lists:seq(0, 100, 5)),
    Range = #range{first = 5, last = 50, incr = 5},
    Table = queue_stats_publish,
    SamplesFun = fun() -> [Slide1] end,
    Got = rabbit_mgmt_stats:format_range(Range, 0, Table, 0, fun() -> ok end,
                                         SamplesFun),
    5 = proplists:get_value(publish, Got),
    PD = proplists:get_value(publish_details, Got),
    0.0 = proplists:get_value(rate, PD).
