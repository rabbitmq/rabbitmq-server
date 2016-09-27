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

-module(rabbit_mgmt_stats_SUITE).

-include_lib("proper/include/proper.hrl").
-include("rabbit_mgmt_metrics.hrl").

-compile(export_all).

all() ->
    [
     {group, parallel_tests}
    ].

groups() ->
    [
     {parallel_tests, [], [
			   format_rate_no_range_test,
			   format_zero_rate_no_range_test,
			   format_incremental_rate_no_range_test,
			   format_incremental_zero_rate_no_range_test,
			   format_total_no_range_test,
			   format_incremental_total_no_range_test
			  ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------
init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    inets:start(),
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(_, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
                                                    {rmq_nodename_suffix, ?MODULE}
                                                   ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
				      rabbit_ct_broker_helpers:setup_steps() ++
					  rabbit_ct_client_helpers:setup_steps() ++
					  [fun rabbit_mgmt_test_util:reset_management_settings/1]).

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
                                         [fun rabbit_mgmt_test_util:reset_management_settings/1] ++
					     rabbit_ct_client_helpers:teardown_steps() ++
                                             rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_mgmt_test_util:reset_management_settings(Config),
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_mgmt_test_util:reset_management_settings(Config),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

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
     node_node_coarse_stats, queue_msg_rates, vhost_msg_rates
    ].

sample_size(large) ->
    choose(3, 200);
sample_size(small) ->
    choose(0, 1).

sample_gen(_Table, 0) ->
    [];
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
format_rate_no_range_test(Config) ->
    true == rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, format_rate_no_range, []).

format_rate_no_range() ->
    rabbit_ct_proper_helpers:run_proper(fun prop_rate_format_no_range/0, [], 100).

prop_rate_format_no_range() ->
    prop_format_no_range(large, rate_check(fun(Rate) -> Rate > 0 end), false).

rate_check(RateCheck) ->
    fun(Results, _, Table) ->
	    Check =
		fun(Detail) ->
			Rate = proplists:get_value(rate, proplists:get_value(Detail, Results), 0),
			RateCheck(Rate)
		end,
	    lists:all(Check, details(Table))
    end.

prop_format_no_range(SampleSize, Check, Incremental) ->
    ?FORALL(
       {{Table, Data}, Interval}, {content_gen(SampleSize), interval_gen()},
       begin
	   {Slide, Total} = create_slide(Data, Interval, Incremental),
	   Id = sample_one,
	   ets:insert(Table, {{Id, 5}, Slide}),
	   Results = rabbit_mgmt_stats:format(no_range, Table, Id, 5000),
	   Check(Results, Total, Table)
       end).

%% Rates for 1 or no samples will always be 0.0 as there aren't
%% enough datapoints to calculate the instant rate
format_zero_rate_no_range_test(Config) ->
    true == rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, format_zero_rate_no_range, []).

format_zero_rate_no_range() ->
    rabbit_ct_proper_helpers:run_proper(fun prop_zero_rate_format_no_range/0, [], 100).

prop_zero_rate_format_no_range() ->
    prop_format_no_range(small, rate_check(fun(Rate) -> Rate == 0.0 end), false).

%% Rates for 3 or more monotonically increasing incremental samples will always be > 0
format_incremental_rate_no_range_test(Config) ->
    true == rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, format_incremental_rate_no_range, []).

format_incremental_rate_no_range() ->
    rabbit_ct_proper_helpers:run_proper(fun prop_incremental_rate_format_no_range/0, [], 100).

prop_incremental_rate_format_no_range() ->
    prop_format_no_range(large, rate_check(fun(Rate) -> Rate > 0 end), true).

%% Rates for 1 or no samples will always be 0.0 as there aren't
%% enough datapoints to calculate the instant rate
format_incremental_zero_rate_no_range_test(Config) ->
    true == rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, format_incremental_zero_rate_no_range, []).

format_incremental_zero_rate_no_range() ->
    rabbit_ct_proper_helpers:run_proper(fun prop_incremental_zero_rate_format_no_range/0, [], 100).

prop_incremental_zero_rate_format_no_range() ->
    prop_format_no_range(small, rate_check(fun(Rate) -> Rate == 0.0 end), true).

%% Checking totals
format_total_no_range_test(Config) ->
    true == rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, format_total_no_range, []).

format_total_no_range() ->
    rabbit_ct_proper_helpers:run_proper(fun prop_total_format_no_range/0, [], 100).

prop_total_format_no_range() ->
    prop_format_no_range(large, fun check_total/3, false).

check_total(Results, Totals, Table) ->
    Expected = lists:zip(?stats_per_table(Table), tuple_to_list(Totals)),
    lists:all(fun({K, _} = E) ->
		      case is_average_time(K) of
			  false -> lists:member(E, Results);
			  true -> lists:keymember(K, 1, Results)
				    end
	      end, Expected).

format_incremental_total_no_range_test(Config) ->
    true == rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, format_incremental_total_no_range, []).

format_incremental_total_no_range() ->
    rabbit_ct_proper_helpers:run_proper(fun prop_incremental_total_format_no_range/0, [], 100).

prop_incremental_total_format_no_range() ->
    prop_format_no_range(large, fun check_total/3, true).

%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------
details(Table) ->
    [list_to_atom(atom_to_list(S) ++ "_details")  || S <- ?stats_per_table(Table)].

add(T1, undefined) ->
    T1;
add(T1, T2) ->
    list_to_tuple(lists:zipwith(fun(A, B) -> A + B end, tuple_to_list(T1), tuple_to_list(T2))).
    
create_slide(Data, Interval, Incremental) ->
    %% Use the samples as increments for data generation, so we have always increasing counters
    Slide = exometer_slide:new(60 * 1000, [{interval, Interval}, {incremental, Incremental}]),
    Sleep = min_wait(Interval, Data),
    lists:foldl(fun(E, {Acc, Total}) ->
			timer:sleep(Sleep),
			NewTotal = add(E, Total),
			Sample = case Incremental of
				     false ->
					 %% Guarantees a monotonically increasing counter
					 NewTotal;
				     true ->
					 E
				 end,
			{exometer_slide:add_element(Sample, Acc), NewTotal}
		end, {Slide, undefined}, Data).

min_wait(_, []) ->
    0;
min_wait(Interval, Data) ->
    %% Send at constant intervals for Interval * 3 ms. This eventually ensures several samples
    %% on the same interval, max execution time of Interval * 5 and also enough samples to
    %% generate a rate.
    case round((Interval * 3) / length(Data)) of
	0 ->
	    1;
	Min ->
	    Min
    end.

is_average_time(Atom) ->
    case re:run(atom_to_list(Atom), "_avg_time$") of
	nomatch ->
	    false;
	_ ->
	    true
    end.
