%%% Collector for dynamic metrics that are calculated at collection time
-module(prometheus_rabbitmq_dynamic_collector).

-behaviour(prometheus_collector).
-include_lib("prometheus/include/prometheus.hrl").

-export([deregister_cleanup/1,
         collect_mf/2]).

-import(prometheus_model_helpers, [create_mf/5,
                                   gauge_metric/1,
                                   gauge_metric/2]).

-define(METRIC_NAME_PREFIX, "rabbitmq_").

-define(METRICS, [{partitioned_from, gauge,
                   "Indicates that the current node is partitioned "
                   "from the peer node during a network partition."}
                 ]).

%%====================================================================
%% Collector API
%%====================================================================

deregister_cleanup(_) -> ok.

collect_mf(_Registry, Callback) ->
    _ = lists:foreach(
          fun({Name, Type, Help}) ->
                  Callback(
                    prometheus_model_helpers:create_mf(
                      ?METRIC_NAME(Name),
                      Help,
                      Type,
                      values(Name))
                   )
          end,
          ?METRICS
         ),
    ok.

%%====================================================================
%% Private Parts
%%====================================================================

values(partitioned_from) ->
    [{[{peer, Node}], 1}
     || Node <- rabbit_node_monitor:partitions()].
