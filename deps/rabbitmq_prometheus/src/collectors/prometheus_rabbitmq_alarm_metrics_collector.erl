%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(prometheus_rabbitmq_alarm_metrics_collector).

-export([register/0, deregister_cleanup/1, collect_mf/2]).

-import(prometheus_model_helpers, [create_mf/4, untyped_metric/1]).

-include_lib("prometheus/include/prometheus.hrl").
-include_lib("kernel/include/logger.hrl").

-behaviour(prometheus_collector).

-define(METRIC_NAME_PREFIX, "rabbitmq_alarms_").

%%====================================================================
%% Collector API
%%====================================================================

register() ->
    ok = prometheus_registry:register_collector(?MODULE).

deregister_cleanup(_) ->
    ok.

-spec collect_mf(_Registry, Callback) -> ok
              when _Registry :: prometheus_registry:registry(),
                   Callback :: prometheus_collector:collect_mf_callback().
collect_mf(_Registry, Callback) ->
    try
        Alarms = rabbit_alarm:get_local_alarms(500), %% TODO: figure out timeout
        ActiveAlarms =
            lists:foldl(fun ({{resource_limit, disk, _}, _}, Acc) ->
                                Acc#{disk_limit => 1};
                            ({{resource_limit, {disk, QT}, _}, _}, Acc) ->
                                Acc#{{disk, QT} => 1};
                            ({{resource_limit, memory, _}, _}, Acc) ->
                                Acc#{memory_limit => 1};
                            ({file_descriptor_limit, _}, Acc) ->
                                Acc#{file_descriptor_limit => 1}
                        end,
                        #{},
                        Alarms),

        Callback(create_mf(?METRIC_NAME(<<"file_descriptor_limit">>),
                           <<"is 1 if file descriptor limit alarm is in effect">>,
                           untyped,
                           [untyped_metric(maps:get(file_descriptor_limit,
                                                    ActiveAlarms,
                                                    0))])),
        Callback(create_mf(?METRIC_NAME(<<"free_disk_space_watermark">>),
                           <<"is 1 if free disk space watermark alarm is in effect">>,
                           untyped,
                           [untyped_metric(maps:get(disk_limit, ActiveAlarms, 0))])),
        Callback(create_mf(?METRIC_NAME(<<"memory_used_watermark">>),
                           <<"is 1 if VM memory watermark alarm is in effect">>,
                           untyped,
                           [untyped_metric(maps:get(memory_limit, ActiveAlarms, 0))])),

        Callback(create_mf(?METRIC_NAME(<<"queue_type_free_disk_space_watermark">>),
                           <<"is 1 if the queue type disk-space alarm is in effect">>,
                           untyped,
                           [prometheus_model_helpers:untyped_metric(
                              #{queue_type => QT},
                              maps:get({disk, QT}, ActiveAlarms, 0)) ||
                            {_, QT} <- rabbit_registry:lookup_all(queue)])),

        ok
    catch
        exit:{timeout, _} ->
            ?LOG_ERROR("alarm_metrics_collector failed to emit metrics: "
                             "rabbitm_alarm:get_local_alarms timed out"),
            %% We are not going to render any alarm metrics here.
            %% Breaks continuity but at least doesn't crash the
            %% whole scraping endpoint
            ok
    end.
