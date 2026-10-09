%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_prometheus_util).

-export([normalize_disabled_metrics/1,
         normalize_metric_name/1]).

-spec normalize_disabled_metrics([atom()]) -> [atom()].
normalize_disabled_metrics(Metrics) ->
    [normalize_metric_name(M) || M <- Metrics].

-spec normalize_metric_name(atom()) -> atom().
normalize_metric_name(Metric) when is_atom(Metric) ->
    case atom_to_list(Metric) of
        "rabbitmq_detailed_" ++ Rest -> list_to_atom(Rest);
        "rabbitmq_cluster_" ++ Rest  -> list_to_atom(Rest);
        "rabbitmq_" ++ Rest          -> list_to_atom(Rest);
        _                            -> Metric
    end.
