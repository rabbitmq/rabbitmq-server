%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(prometheus_rabbitmq_federation_collector_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("prometheus/include/prometheus_model.hrl").

-compile(export_all).

-define(ONE_RUNNING_METRIC, #'MetricFamily'{name = <<"rabbitmq_federation_links">>,
                                            help = "Current number of federation links.",
                                            type = 'GAUGE',
                                            metric = [#'Metric'{label = [#'LabelPair'{name = <<"status">>,
                                                                                      value = <<"running">>}],
                                                                gauge = #'Gauge'{value = 1}}]}).

-define(TWO_RUNNING_METRIC, #'MetricFamily'{name = <<"rabbitmq_federation_links">>,
                                            help = "Current number of federation links.",
                                            type = 'GAUGE',
                                            metric = [#'Metric'{label = [#'LabelPair'{name = <<"status">>,
                                                                                      value = <<"running">>}],
                                                                gauge = #'Gauge'{value = 2}}]}).

-define(ONE_RUNNING_ONE_STARTING_METRIC, #'MetricFamily'{name = <<"rabbitmq_federation_links">>,
                                                         help = "Current number of federation links.",
                                                         type = 'GAUGE',
                                                         metric = [#'Metric'{label = [#'LabelPair'{name = <<"status">>,
                                                                                                   value = <<"running">>}],
                                                                             gauge = #'Gauge'{value = 1}},
                                                                   #'Metric'{label = [#'LabelPair'{name = <<"status">>,
                                                                                                   value = <<"starting">>}],
                                                                             gauge = #'Gauge'{value = 1}}]}).

-import(rabbit_federation_test_util,
        [expect/3, expect_empty/2,
         set_upstream/4, clear_upstream/3, set_upstream_set/4,
         set_policy/5, clear_policy/3,
         set_policy_upstream/5, set_policy_upstreams/4,
         no_plugins/1, with_ch/3, q/2, maybe_declare_queue/3, delete_all/2]).

all() ->
    [
     {group, non_parallel_tests}
    ].

groups() ->
    [
     {non_parallel_tests, [], [
                               single_link_then_second_added,
                               two_links_from_the_start
                              ]}
    ].

suite() ->
    [{timetrap, {minutes, 5}}].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------
init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
                                                    {rmq_nodename_suffix, ?MODULE}
                                                   ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
                                      rabbit_ct_broker_helpers:setup_steps() ++
                                          rabbit_ct_client_helpers:setup_steps() ++
                                          [fun rabbit_federation_test_util:setup_federation/1]).
end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
                                         rabbit_ct_client_helpers:teardown_steps() ++
                                             rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Test cases
%% -------------------------------------------------------------------

single_link_then_second_added(Config) ->
    with_ch(
      Config,
      fun (Ch) ->
              timer:sleep(3000),
              [_L1] = rabbit_ct_broker_helpers:rpc(Config, 0,
                                                   rabbit_federation_status, status, []),
              MFs = get_metrics(Config),
              [?ONE_RUNNING_METRIC] = MFs,
              maybe_declare_queue(Config, Ch, q(<<"fed.downstream2">>, [{<<"x-queue-type">>, longstr, <<"classic">>}])),
              %% here we race against queue.declare... most of the times there is going to be
              %% new status=starting metric. In this case we wait a bit more for running=2.
              %% But running=2 is also possible first time if rpc for some reason is slow.
              %% And of course simple running=1 possible too if queue.declare is really slow
              MFs1 = get_metrics(Config),
              case MFs1 of
                  [?TWO_RUNNING_METRIC] -> ok;
                  [?ONE_RUNNING_METRIC] ->
                      rabbit_ct_helpers:eventually(?_assertEqual([?TWO_RUNNING_METRIC],
                                                                 get_metrics(Config)),
                                                   500,
                                                   5);
                  [?ONE_RUNNING_ONE_STARTING_METRIC] ->
                      rabbit_ct_helpers:eventually(?_assertEqual([?TWO_RUNNING_METRIC],
                                                                 get_metrics(Config)),
                                                   500,
                                                   5)
                      
              end,

              delete_all(Ch, [q(<<"fed.downstream2">>, [{<<"x-queue-type">>, longstr, <<"classic">>}])])
      end, upstream_downstream()).

two_links_from_the_start(Config) ->
    with_ch(
      Config,
      fun (_Ch) ->
              timer:sleep(3000),
              [_L1 | _L2] = rabbit_ct_broker_helpers:rpc(Config, 0,
                                                         rabbit_federation_status, status, []),
              MFs = get_metrics(Config),
              [?TWO_RUNNING_METRIC] = MFs

      end, upstream_downstream() ++ [q(<<"fed.downstream2">>, [{<<"x-queue-type">>, longstr, <<"classic">>}])]).

%% -------------------------------------------------------------------
%%
%% -------------------------------------------------------------------

upstream_downstream() ->
    [q(<<"upstream">>, undefined), q(<<"fed.downstream">>, undefined)].

get_metrics(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0,
                                 rabbitmq_prometheus_collector_test_proxy, collect_mf,
                                 [default, prometheus_rabbitmq_federation_collector]).
