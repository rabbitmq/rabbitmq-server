%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(prometheus_rabbitmq_message_size_metrics_collector).

-behaviour(prometheus_collector).
-include_lib("prometheus/include/prometheus.hrl").

-export([register/0,
         deregister_cleanup/1,
         collect_mf/2]).

-define(METRIC_NAME_PREFIX, "rabbitmq_").

register() ->
    ok = prometheus_registry:register_collector(?MODULE).

deregister_cleanup(_) ->
    ok.

collect_mf(_Registry, Callback) ->
    maps:foreach(
      fun(Name, #{type := Type,
                  help := Help,
                  values := Values}) ->
              MetricsFamily = prometheus_model_helpers:create_mf(
                                ?METRIC_NAME(Name), Help, Type, Values),
              Callback(MetricsFamily)
      end,
      rabbit_msg_size_metrics:prometheus_format()).
