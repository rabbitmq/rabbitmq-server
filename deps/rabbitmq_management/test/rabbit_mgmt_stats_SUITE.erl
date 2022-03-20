%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2016-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_stats_SUITE).

-include_lib("proper/include/proper.hrl").
-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_metrics.hrl").
-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").

-compile(export_all).
-compile({no_auto_import, [ceil/1]}).

all() ->
    [
     {group, parallel_tests}
    ].

groups() ->
    [
     {parallel_tests, [parallel], [
                   format_rate_no_range_test,
                   format_zero_rate_no_range_test,
                   format_incremental_rate_no_range_test,
                   format_incremental_zero_rate_no_range_test,
                   format_total_no_range_test,
                   format_incremental_total_no_range_test,
                   format_rate_range_test,
                   format_incremental_rate_range_test,
                   format_incremental_zero_rate_range_test,
                   format_total_range_test,
                   format_incremental_total_range_test,
                   format_samples_range_test,
                   format_incremental_samples_range_test,
                   format_avg_rate_range_test,
                   format_incremental_avg_rate_range_test,
                   format_avg_range_test,
                   format_incremental_avg_range_test
                  ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------
init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config.

end_per_suite(Config) ->
    Config.

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

stats_tables() ->
    [connection_stats_coarse_conn_stats, vhost_stats_coarse_conn_stats,
     channel_stats_fine_stats, channel_exchange_stats_fine_stats,
     channel_queue_stats_deliver_stats, vhost_stats_fine_stats,
     queue_stats_deliver_stats, vhost_stats_deliver_stats,
     channel_stats_deliver_stats, channel_process_stats,
     queue_stats_publish, queue_exchange_stats_publish,
     exchange_stats_publish_out, exchange_stats_publish_in,
     queue_msg_stats, vhost_msg_stats, queue_process_stats,
     node_coarse_stats, node_persister_stats,
     node_node_coarse_stats, queue_msg_rates, vhost_msg_rates,
     connection_churn_rates
    ].

sample_size(large) ->
    choose(15, 200);
sample_size(small) ->
    choose(0, 1).

sample_gen(_Table, 0) ->
    [];
sample_gen(Table, 1) ->
    ?LET(Stats, stats_gen(Table), [Stats || _ <- lists:seq(1, 5)]);
sample_gen(Table, N) ->
    vector(N, stats_gen(Table)).

content_gen(Size) ->
    ?LET({Table, SampleSize}, {oneof(stats_tables()), sample_size(Size)},
     ?LET(Stats, sample_gen(Table, SampleSize),
          {Table, Stats})).

interval_gen() ->
    %% Keep it at most 150ms, so the test runs in a reasonable time
    choose(1, 150).

stats_gen(Table) ->
    ?LET(Vector, vector(length(?stats_per_table(Table)), choose(1, 100)),
     list_to_tuple(Vector)).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

%% Rates for 3 or more monotonically increasing samples will always be > 0
format_rate_no_range_test(_Config) ->
    Fun = fun() ->
                  prop_format(large, rate_check(fun(Rate) -> Rate > 0 end),
                              false, fun no_range/3)
          end,
    rabbit_ct_proper_helpers:run_proper(Fun, [], 100).

prop_format(SampleSize, Check, Incremental, RangeFun) ->
    ?FORALL(
       {{Table, Data}, Interval}, {content_gen(SampleSize), interval_gen()},
       begin
           {LastTS, Slide, Total, Samples}
               = create_slide(Data, Interval, Incremental, SampleSize),
           Range = RangeFun(Slide, LastTS, Interval),
           SamplesFun = fun() -> [Slide] end,
           InstantRateFun = fun() -> [Slide] end,
           Results = rabbit_mgmt_stats:format_range(Range, LastTS, Table, 5000,
                                                    InstantRateFun,
                                                    SamplesFun),
           ?WHENFAIL(io:format("Got: ~p~nSlide: ~p~nRange~p~n", [Results, Slide, Range]),
                     Check(Results, Total, Samples, Table))
       end).

%% Rates for 1 or no samples will always be 0.0 as there aren't
%% enough datapoints to calculate the instant rate
format_zero_rate_no_range_test(_Config) ->
    Fun = fun() ->
                  prop_format(small, rate_check(fun(Rate) -> Rate == 0.0 end),
                              false, fun no_range/3)
          end,
    rabbit_ct_proper_helpers:run_proper(Fun, [], 100).

%% Rates for 3 or more monotonically increasing incremental samples will always be > 0
format_incremental_rate_no_range_test(_Config) ->
    Fun = fun() ->
                  prop_format(large, rate_check(fun(Rate) -> Rate > 0 end),
                              true, fun no_range/3)
          end,
    rabbit_ct_proper_helpers:run_proper(Fun, [], 100).

%% Rates for 1 or no samples will always be 0.0 as there aren't
%% enough datapoints to calculate the instant rate
format_incremental_zero_rate_no_range_test(_Config) ->
    Fun = fun() ->
                  prop_format(small, rate_check(fun(Rate) -> Rate == 0.0 end),
                              true, fun no_range/3)
          end,
    rabbit_ct_proper_helpers:run_proper(Fun, [], 100).

%% Checking totals
format_total_no_range_test(_Config) ->
    Fun = fun() ->
                  prop_format(large, fun check_total/4, false, fun no_range/3)
          end,
    rabbit_ct_proper_helpers:run_proper(Fun, [], 100).

format_incremental_total_no_range_test(_Config) ->
    Fun = fun() ->
                  prop_format(large, fun check_total/4, true, fun no_range/3)
          end,
    rabbit_ct_proper_helpers:run_proper(Fun, [], 100).

%%---------------------
%% Requests using range
%%---------------------
format_rate_range_test(_Config) ->
    %% Request a range bang on the middle, so we ensure no padding is applied
    Fun = fun() ->
              prop_format(large, rate_check(fun(Rate) -> Rate > 0 end),
                          false, fun full_range/3)
          end,
    rabbit_ct_proper_helpers:run_proper(Fun, [], 100).

%% Rates for 3 or more monotonically increasing incremental samples will always be > 0
format_incremental_rate_range_test(_Config) ->
    %% Request a range bang on the middle, so we ensure no padding is applied
    Fun = fun() ->
                  prop_format(large, rate_check(fun(Rate) -> Rate > 0 end),
                              true, fun full_range/3)
          end,
    rabbit_ct_proper_helpers:run_proper(Fun, [], 100).

%% Rates for 1 or no samples will always be 0.0 as there aren't
%% enough datapoints to calculate the instant rate
format_incremental_zero_rate_range_test(_Config) ->
    Fun = fun() ->
              prop_format(small, rate_check(fun(Rate) -> Rate == 0.0 end),
                          true, fun full_range/3)
          end,
    rabbit_ct_proper_helpers:run_proper(Fun, [], 100).

%% Checking totals
format_total_range_test(_Config) ->
    Fun = fun() ->
              prop_format(large, fun check_total/4, false, fun full_range_plus_interval/3)
          end,
    rabbit_ct_proper_helpers:run_proper(Fun, [], 100).

format_incremental_total_range_test(_Config) ->
    Fun = fun() ->
                  prop_format(large, fun check_total/4, true, fun full_range_plus_interval/3)
          end,
    rabbit_ct_proper_helpers:run_proper(Fun, [], 100).

format_samples_range_test(_Config) ->
    Fun = fun() ->
          prop_format(large, fun check_samples/4, false, fun full_range/3)
      end,
    rabbit_ct_proper_helpers:run_proper(Fun, [], 100).

format_incremental_samples_range_test(_Config) ->
    Fun = fun() ->
          prop_format(large, fun check_samples/4, true, fun full_range/3)
      end,
    rabbit_ct_proper_helpers:run_proper(Fun, [], 100).

format_avg_rate_range_test(_Config) ->
    Fun = fun() ->
          prop_format(large, fun check_avg_rate/4, false, fun full_range/3)
      end,
    rabbit_ct_proper_helpers:run_proper(Fun, [], 100).

format_incremental_avg_rate_range_test(_Config) ->
    Fun = fun() ->
          prop_format(large, fun check_avg_rate/4, true, fun full_range/3)
      end,
    rabbit_ct_proper_helpers:run_proper(Fun, [], 100).

format_avg_range_test(_Config) ->
    Fun = fun() ->
          prop_format(large, fun check_avg/4, false, fun full_range/3)
      end,
    rabbit_ct_proper_helpers:run_proper(Fun, [], 100).

format_incremental_avg_range_test(_Config) ->
    Fun = fun() ->
          prop_format(large, fun check_avg/4, true, fun full_range/3)
      end,
    rabbit_ct_proper_helpers:run_proper(Fun, [], 100).
%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------
details(Table) ->
    [list_to_atom(atom_to_list(S) ++ "_details")  || S <- ?stats_per_table(Table)].

add(T1, undefined) ->
    T1;
add(T1, T2) ->
    list_to_tuple(lists:zipwith(fun(A, B) -> A + B end, tuple_to_list(T1), tuple_to_list(T2))).

create_slide(Data, Interval, Incremental, SampleSize) ->
    %% Use the samples as increments for data generation,
    %% so we have always increasing counters
    Now = 0,
    Slide = exometer_slide:new(Now, 60 * 1000,
                               [{interval, Interval}, {incremental, Incremental}]),
    Sleep = min_wait(Interval, Data),
    lists:foldl(
      fun(E, {TS0, Acc, Total, Samples}) ->
              TS1 = TS0 + Sleep,
              NewTotal = add(E, Total),
              %% We use small sample sizes to keep a constant rate
              Sample = create_sample(E, Incremental, SampleSize, NewTotal),
              {TS1, exometer_slide:add_element(TS1, Sample, Acc), NewTotal,
               [NewTotal | Samples]}
      end, {Now, Slide, undefined, []}, Data).

create_sample(E, Incremental, SampleSize, NewTotal) ->
    case {Incremental, SampleSize} of
        {false, small} -> E;
        {true, small} ->
            zero_tuple(E);
        {false, _} ->
            %% Guarantees a monotonically increasing counter
            NewTotal;
        {true, _} -> E
    end.

zero_tuple(E) ->
    Length = length(tuple_to_list(E)),
    list_to_tuple([0 || _ <- lists:seq(1, Length)]).

min_wait(_, []) ->
    0;
min_wait(Interval, Data) ->
    %% Send at constant intervals for Interval * 3 ms. This eventually ensures several samples
    %% on the same interval, max execution time of Interval * 5 and also enough samples to
    %% generate a rate.
    case round((Interval * 3) / length(Data)) of
        0 -> 1;
        Min -> Min
    end.

is_average_time(Atom) ->
    case re:run(atom_to_list(Atom), "_avg_time$") of
        nomatch ->
            false;
        _ ->
            true
    end.

rate_check(RateCheck) ->
    fun(Results, _, _, Table) ->
        Check =
        fun(Detail) ->
            Rate = proplists:get_value(rate, proplists:get_value(Detail, Results), 0),
            RateCheck(Rate)
        end,
        lists:all(Check, details(Table))
    end.

check_total(Results, Totals, _Samples, Table) ->
    Expected = lists:zip(?stats_per_table(Table), tuple_to_list(Totals)),
    lists:all(fun({K, _} = E) ->
                  case is_average_time(K) of
                      false -> lists:member(E, Results);
                      true -> lists:keymember(K, 1, Results)
                  end
              end, Expected).

is_avg_time_details(Detail) ->
    match == re:run(atom_to_list(Detail), "avg_time_details$", [{capture, none}]).

check_samples(Results, _Totals, Samples, Table) ->
    Details = details(Table),
    %% Lookup list for the position of the key in the stats tuple
    Pairs = lists:zip(Details, lists:seq(1, length(Details))),

    NonAvgTimeDetails = lists:filter(fun(D) ->
                                             not is_avg_time_details(D)
                                     end, Details),

    %% Check that all samples in the results match one of the samples in the inputs
    lists:all(fun(Detail) ->
              RSamples = get_from_detail(samples, Detail, Results),
              lists:all(fun(RS) ->
                    Value = proplists:get_value(sample, RS),
                    case Value of
                        0 ->
                        true;
                        _ ->
                        lists:keymember(Value,
                                proplists:get_value(Detail, Pairs),
                                Samples)
                    end
                end, RSamples)
          end, NonAvgTimeDetails)
    %% ensure that not all samples are 0
    andalso lists:all(fun(Detail) ->
                  RSamples = get_from_detail(samples, Detail, Results),
                  lists:any(fun(RS) ->
                            0 =/= proplists:get_value(sample, RS)
                        end, RSamples)
              end, Details).

check_avg_rate(Results, _Totals, _Samples, Table) ->
    Details = details(Table),

    NonAvgTimeDetails = lists:filter(fun(D) ->
                                             not is_avg_time_details(D)
                                     end, Details),

    AvgTimeDetails = lists:filter(fun(D) ->
                                          is_avg_time_details(D)
                                  end, Details),

    lists:all(fun(Detail) ->
              AvgRate = get_from_detail(avg_rate, Detail, Results),
              Samples = get_from_detail(samples, Detail, Results),
              S2 = proplists:get_value(sample, hd(Samples)),
              T2 = proplists:get_value(timestamp, hd(Samples)),
              S1 = proplists:get_value(sample, lists:last(Samples)),
              T1 = proplists:get_value(timestamp, lists:last(Samples)),
              AvgRate == ((S2 - S1) * 1000 / (T2 - T1))
          end, NonAvgTimeDetails) andalso
        lists:all(fun(Detail) ->
                          Avg = get_from_detail(avg_rate, Detail, Results),
                          Samples = get_from_detail(samples, Detail, Results),
                          First = proplists:get_value(sample, hd(Samples)),
                          Avg == First
                  end, AvgTimeDetails).

check_avg(Results, _Totals, _Samples, Table) ->
    Details = details(Table),

    NonAvgTimeDetails = lists:filter(fun(D) ->
                                             not is_avg_time_details(D)
                                     end, Details),

    AvgTimeDetails = lists:filter(fun(D) ->
                                          is_avg_time_details(D)
                                  end, Details),

    lists:all(fun(Detail) ->
              Avg = get_from_detail(avg, Detail, Results),
              Samples = get_from_detail(samples, Detail, Results),
              Sum = lists:sum([proplists:get_value(sample, S) || S <- Samples]),
              Avg == (Sum / length(Samples))
              end, NonAvgTimeDetails) andalso
        lists:all(fun(Detail) ->
                          Avg = get_from_detail(avg, Detail, Results),
                          Samples = get_from_detail(samples, Detail, Results),
                          First = proplists:get_value(sample, hd(Samples)),
                          Avg == First
                  end, AvgTimeDetails).

get_from_detail(Tag, Detail, Results) ->
    proplists:get_value(Tag, proplists:get_value(Detail, Results), []).

full_range(Slide, Last, Interval) ->
    LastTS = case exometer_slide:last_two(Slide) of
                 [] -> Last;
                 [{L, _} | _] -> L
             end,
    #range{first = 0, last = LastTS, incr = Interval}.

full_range_plus_interval(Slide, Last, Interval) ->
    LastTS = case exometer_slide:last_two(Slide) of
                 [] -> Last;
                 [{L, _} | _] -> L
             end,
    % were adding two intervals here due to rounding occasionally pushing the last
    % sample into the next time "bucket"
    #range{first = 0, last = LastTS + Interval + Interval, incr = Interval}.

no_range(_Slide, _LastTS, _Interval) ->
    no_range.

%% Generate a well-formed interval from Start using Interval steps
last_ts(First, Last, Interval) ->
    ceil(((Last - First) / Interval)) * Interval + First.

ceil(X) when X < 0 ->
    trunc(X);
ceil(X) ->
    T = trunc(X),
    case X - T == 0 of
        true -> T;
        false -> T + 1
    end.
