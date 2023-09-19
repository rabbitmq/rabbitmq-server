%%% Collector for dynamic metrics that are calculated at collection time
-module(prometheus_rabbitmq_dynamic_collector).

-behaviour(prometheus_collector).
-include_lib("prometheus/include/prometheus.hrl").

-export([deregister_cleanup/1,
         collect_mf/2]).

-define(METRIC_NAME_PREFIX, "rabbitmq_").

-define(METRICS, [{unreachable_cluster_peers_count, gauge,
                   "Number of peers in the cluster the current node cannot reach."}
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

values(unreachable_cluster_peers_count) ->
    [{[], length(rabbit_nodes:list_unreachable())}].
