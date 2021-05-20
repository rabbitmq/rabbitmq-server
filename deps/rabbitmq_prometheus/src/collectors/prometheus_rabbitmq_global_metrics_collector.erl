%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(prometheus_rabbitmq_global_metrics_collector).

-behaviour(prometheus_collector).
-include_lib("prometheus/include/prometheus.hrl").

-export([register/0,
         deregister_cleanup/1,
         collect_mf/2]).

-import(prometheus_model_helpers, [create_mf/4,
                                   counter_metric/2]).

%% This exposes the new global metrics which are instrumented directly, and bypass the Rabbit Core metrics entirely.
%% The short-term plan is to start building the new metrics sub-system alongside RabbitMQ Core metrics
%% The long-term plan is to replace RabbitMQ Core metrics with this new approach.
%%
-define(METRIC_NAME_PREFIX, "rabbitmq_global_").

%%====================================================================
%% Collector API
%%====================================================================

register() ->
    ok = prometheus_registry:register_collector(?MODULE).

deregister_cleanup(_) -> ok.

collect_mf(_Registry, Callback) ->
    _ = maps:fold(
          fun (Name, #{type := Type, help := Help, values := Values}, Acc) ->
                  Callback(
                    create_mf(?METRIC_NAME(Name),
                              Help,
                              Type,
                              maps:to_list(Values))),
                  Acc
          end,
          ok,
          rabbit_global_counters:prometheus_format()
         ).

%% ===================================================================
%% Private functions
%% ===================================================================
