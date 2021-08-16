%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_prometheus_dispatcher).

-export([build_dispatcher/0]).

-define(DEFAULT_PATH, "/metrics").

build_dispatcher() ->
    {ok, _} = application:ensure_all_started(prometheus),
    prometheus_registry:register_collectors([
        prometheus_rabbitmq_core_metrics_collector,
        prometheus_rabbitmq_alarm_metrics_collector]),
    prometheus_registry:register_collectors('per-object', [
        prometheus_vm_system_info_collector,
        prometheus_vm_dist_collector,
        prometheus_vm_memory_collector,
        prometheus_mnesia_collector,
        prometheus_vm_statistics_collector,
        prometheus_vm_msacc_collector,
        prometheus_rabbitmq_core_metrics_collector
        ]),
    rabbit_prometheus_handler:setup(),
    cowboy_router:compile([{'_', dispatcher()}]).

dispatcher() ->
    [{path() ++ "/[:registry]", rabbit_prometheus_handler, []}].

path() ->
    application:get_env(rabbitmq_prometheus, path, ?DEFAULT_PATH).
