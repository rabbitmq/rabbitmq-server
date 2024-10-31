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
                                            help = "Number of federation links",
                                            type = 'GAUGE',
                                            metric = [#'Metric'{label = [#'LabelPair'{name = <<"status">>,
                                                                                      value = <<"running">>}],
                                                                gauge = #'Gauge'{value = 1}}]}).

-define(TWO_RUNNING_METRIC, #'MetricFamily'{name = <<"rabbitmq_federation_links">>,
                                            help = "Number of federation links",
                                            type = 'GAUGE',
                                            metric = [#'Metric'{label = [#'LabelPair'{name = <<"status">>,
                                                                                      value = <<"running">>}],
                                                                gauge = #'Gauge'{value = 2}}]}).

-define(ONE_RUNNING_ONE_STARTING_METRIC, #'MetricFamily'{name = <<"rabbitmq_federation_links">>,
                                                         help = "Number of federation links",
                                                         type = 'GAUGE',
                                                         metric = [#'Metric'{label = [#'LabelPair'{name = <<"status">>,
                                                                                                   value = <<"running">>}],
                                                                             gauge = #'Gauge'{value = 1}},
                                                                   #'Metric'{label = [#'LabelPair'{name = <<"status">>,
                                                                                                   value = <<"starting">>}],
                                                                             gauge = #'Gauge'{value = 1}}]}).


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
                                          [fun setup_federation/1]).
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
              rabbit_ct_helpers:eventually(?_assertEqual([?ONE_RUNNING_METRIC],
                                                         get_metrics(Config)),
                                           500,
                                           5),
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
                                 ?MODULE, collect_mf,
                                 [default, rabbit_federation_prometheus_collector]).




setup_federation(Config) ->
    setup_federation_with_upstream_params(Config, []).

setup_federation_with_upstream_params(Config, ExtraParams) ->
    rabbit_ct_broker_helpers:set_parameter(Config, 0,
      <<"federation-upstream">>, <<"localhost">>, [
        {<<"uri">>, rabbit_ct_broker_helpers:node_uri(Config, 0)},
        {<<"consumer-tag">>, <<"fed.tag">>}
        ] ++ ExtraParams),

    rabbit_ct_broker_helpers:set_parameter(Config, 0,
      <<"federation-upstream">>, <<"local5673">>, [
        {<<"uri">>, <<"amqp://localhost:1">>}
        ] ++ ExtraParams),

    rabbit_ct_broker_helpers:set_parameter(Config, 0,
      <<"federation-upstream-set">>, <<"upstream">>, [
        [
          {<<"upstream">>, <<"localhost">>},
          {<<"exchange">>, <<"upstream">>},
          {<<"queue">>, <<"upstream">>}
        ]
      ]),

    rabbit_ct_broker_helpers:set_parameter(Config, 0,
      <<"federation-upstream-set">>, <<"upstream2">>, [
        [
          {<<"upstream">>, <<"localhost">>},
          {<<"exchange">>, <<"upstream2">>},
          {<<"queue">>, <<"upstream2">>}
        ]
      ]),

    rabbit_ct_broker_helpers:set_parameter(Config, 0,
      <<"federation-upstream-set">>, <<"localhost">>, [
        [{<<"upstream">>, <<"localhost">>}]
      ]),

    rabbit_ct_broker_helpers:set_parameter(Config, 0,
      <<"federation-upstream-set">>, <<"upstream12">>, [
        [
          {<<"upstream">>, <<"localhost">>},
          {<<"exchange">>, <<"upstream">>},
          {<<"queue">>, <<"upstream">>}
        ], [
          {<<"upstream">>, <<"localhost">>},
          {<<"exchange">>, <<"upstream2">>},
          {<<"queue">>, <<"upstream2">>}
        ]
      ]),

    rabbit_ct_broker_helpers:set_parameter(Config, 0,
      <<"federation-upstream-set">>, <<"one">>, [
        [
          {<<"upstream">>, <<"localhost">>},
          {<<"exchange">>, <<"one">>},
          {<<"queue">>, <<"one">>}
        ]
      ]),

    rabbit_ct_broker_helpers:set_parameter(Config, 0,
      <<"federation-upstream-set">>, <<"two">>, [
        [
          {<<"upstream">>, <<"localhost">>},
          {<<"exchange">>, <<"two">>},
          {<<"queue">>, <<"two">>}
        ]
      ]),

    rabbit_ct_broker_helpers:set_parameter(Config, 0,
      <<"federation-upstream-set">>, <<"upstream5673">>, [
        [
          {<<"upstream">>, <<"local5673">>},
          {<<"exchange">>, <<"upstream">>}
        ]
      ]),

    rabbit_ct_broker_helpers:rpc(
      Config, 0, rabbit_policy, set,
      [<<"/">>, <<"fed">>, <<"^fed\.">>, [{<<"federation-upstream-set">>, <<"upstream">>}],
       0, <<"all">>, <<"acting-user">>]),

    rabbit_ct_broker_helpers:rpc(
      Config, 0, rabbit_policy, set,
      [<<"/">>, <<"fed12">>, <<"^fed12\.">>, [{<<"federation-upstream-set">>, <<"upstream12">>}],
       2, <<"all">>, <<"acting-user">>]),

    rabbit_ct_broker_helpers:set_policy(Config, 0,
      <<"one">>, <<"^two$">>, <<"all">>, [
        {<<"federation-upstream-set">>, <<"one">>}]),

    rabbit_ct_broker_helpers:set_policy(Config, 0,
      <<"two">>, <<"^one$">>, <<"all">>, [
        {<<"federation-upstream-set">>, <<"two">>}]),

    rabbit_ct_broker_helpers:set_policy(Config, 0,
      <<"hare">>, <<"^hare\.">>, <<"all">>, [
        {<<"federation-upstream-set">>, <<"upstream5673">>}]),

    rabbit_ct_broker_helpers:set_policy(Config, 0,
      <<"all">>, <<"^all\.">>, <<"all">>, [
        {<<"federation-upstream-set">>, <<"all">>}]),

    rabbit_ct_broker_helpers:set_policy(Config, 0,
      <<"new">>, <<"^new\.">>, <<"all">>, [
        {<<"federation-upstream-set">>, <<"new-set">>}]),
    Config.

with_ch(Config, Fun, Methods) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    declare_all(Config, Ch, Methods),
    %% Clean up queues even after test failure.
    try
        Fun(Ch)
    after
        delete_all(Ch, Methods),
        rabbit_ct_client_helpers:close_channel(Ch)
    end,
    ok.

declare_all(Config, Ch, Methods) -> [maybe_declare_queue(Config, Ch, Op) || Op <- Methods].
delete_all(Ch, Methods) ->
    [delete_queue(Ch, Q) || #'queue.declare'{queue = Q} <- Methods].

maybe_declare_queue(Config, Ch, Method) ->
    OneOffCh = rabbit_ct_client_helpers:open_channel(Config),
    try
        amqp_channel:call(OneOffCh, Method#'queue.declare'{passive = true})
    catch exit:{{shutdown, {server_initiated_close, ?NOT_FOUND, _Message}}, _} ->
        amqp_channel:call(Ch, Method)
    after
        catch rabbit_ct_client_helpers:close_channel(OneOffCh)
    end.

delete_queue(Ch, Q) ->
    amqp_channel:call(Ch, #'queue.delete'{queue = Q}).

q(Name) ->
    q(Name, []).

q(Name, undefined) ->
    q(Name, []);
q(Name, Args) ->
    #'queue.declare'{queue   = Name,
                     durable = true,
                     arguments = Args}.

-define(PD_KEY, metric_families).
collect_mf(Registry, Collector) ->
    put(?PD_KEY, []),
    Collector:collect_mf(Registry, fun(MF) -> put(?PD_KEY, [MF | get(?PD_KEY)]) end),
    MFs = lists:reverse(get(?PD_KEY)),
    erase(?PD_KEY),
    MFs.
