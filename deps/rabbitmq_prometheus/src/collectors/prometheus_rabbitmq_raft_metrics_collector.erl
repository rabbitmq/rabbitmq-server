%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(prometheus_rabbitmq_raft_metrics_collector).

-behaviour(prometheus_collector).

-export([register/0,
         deregister_cleanup/1,
         collect_mf/2]).

-import(prometheus_model_helpers, [create_mf/4,
                                   counter_metric/2]).

-define(METRIC_NAME_PREFIX, <<"rabbitmq_raft_">>).
-define(DETAILED_METRIC_NAME_PREFIX, <<"rabbitmq_detailed_raft_">>).

%%====================================================================
%% Collector API
%%====================================================================

register() ->
    ok = prometheus_registry:register_collector(?MODULE).

deregister_cleanup(_) ->
    ok.

collect_mf('per-object', Callback) ->
    collect_per_object_metrics(?METRIC_NAME_PREFIX, Callback);
collect_mf('detailed', Callback) ->
    case get(prometheus_mf_filter) of
        undefined ->
            ok;
        MFNames ->
            case lists:member(ra_metrics, MFNames) of
                true ->
                    collect_detailed_metrics(?DETAILED_METRIC_NAME_PREFIX, Callback);
                false ->
                    ok
            end
    end;
collect_mf(_Registry, Callback) ->
    case application:get_env(rabbitmq_prometheus, return_per_object_metrics, false) of
        false ->
            collect_aggregate_metrics(?METRIC_NAME_PREFIX, Callback);
        true ->
            collect_per_object_metrics(?METRIC_NAME_PREFIX, Callback)
    end.

%% INTERNAL

collect_aggregate_metrics(Prefix, Callback) ->
    collect_key_component_metrics(Prefix, Callback),
    collect_max_values(Prefix, Callback).

collect_per_object_metrics(Prefix, Callback) ->
    collect_key_component_metrics(Prefix, Callback),
    collect_key_per_object_metrics(Prefix, Callback).

collect_detailed_metrics(Prefix, Callback) ->
    VHostFilterFun = case get(prometheus_vhost_filter) of
                         undefined ->
                             fun(_) -> true end;
                         VHosts ->
                             fun(#{vhost := V}) ->
                                     lists:member(V, VHosts);
                                (_) ->
                                     false
                             end
                     end,
    collect_all_matching_metrics(Prefix, Callback, VHostFilterFun).

collect_key_per_object_metrics(Prefix, Callback) ->
    QQMetrics = [term,
                 snapshot_index,
                 last_applied,
                 commit_index,
                 last_written_index,
                 commit_latency,
                 num_segments],
    maps:foreach(
      fun(Name, #{type := Type, help := Help, values := Values}) ->
              Callback(
                create_mf(<<Prefix/binary, (prometheus_model_helpers:metric_name(Name))/binary>>,
                          Help,
                          Type,
                          Values))
      end,
      seshat:format(ra,
                    #{labels => as_binary,
                      metrics => QQMetrics,
                      filter_fun => fun onlyQueues/1})).

collect_all_matching_metrics(Prefix, Callback, VHostFilterFun) ->
    maps:foreach(
      fun(Name, #{type := Type, help := Help, values := Values0}) ->
              Values = maps:filter(fun(#{vhost := V}, _) ->
                                           VHostFilterFun(V);
                                      (_, _) -> true
                                   end, Values0),
              Callback(
                create_mf(<<Prefix/binary, (prometheus_model_helpers:metric_name(Name))/binary>>,
                          Help,
                          Type,
                          Values))
      end,
      seshat:format(ra,
                    #{labels => as_binary,
                      metrics => all,
                      filter_fun => VHostFilterFun})).

collect_max_values(Prefix, Callback) ->
    %% max values for QQ metrics
    %% eg.
    %% rabbitmq_raft_num_segments{queue="q1",vhost="/"} 5.0
    %% rabbitmq_raft_num_segments{queue="q2",vhost="/"} 10.0
    %% becomes
    %% rabbitmq_raft_max_num_segments 10.0
    QQMetrics = [num_segments, commit_latency],
    maps:foreach(
      fun(Name, #{type := Type, help := Help, values := Values}) ->
              Max = lists:max(maps:values(Values)),
              Callback(
                create_mf(<<Prefix/binary, "max_", (prometheus_model_helpers:metric_name(Name))/binary>>,
                          Help,
                          Type,
                          %% TODO: this should not be hardcoded, we should
                          %% something more like 'max() GROUP BY ra_system'
                          #{#{ra_system => quorum_queues} => Max}))

      end,
      seshat:format(ra,
                    #{labels => as_binary,
                      metrics => QQMetrics,
                      filter_fun => fun onlyQueues/1})).

collect_key_component_metrics(Prefix, Callback) ->
    %% quorum queue metrics
    WALMetrics = [wal_files, bytes_written, mem_tables],
    SegmentWriterMetrics = [entries, segments],
    maps:foreach(
      fun(Name, #{type := Type, help := Help, values := Values}) ->
              Callback(
                create_mf(<<Prefix/binary, (prometheus_model_helpers:metric_name(Name))/binary>>,
                          Help,
                          Type,
                          Values))
      end,
      seshat:format(ra,
                    #{labels => as_binary,
                      metrics => WALMetrics ++ SegmentWriterMetrics,
                      filter_fun => fun onlyQueues/1})),
    %% Khepri and other coordination metrics
    maps:foreach(
      fun(Name, #{type := Type, help := Help, values := Values}) ->
              Callback(
                create_mf(<<Prefix/binary, (prometheus_model_helpers:metric_name(Name))/binary>>,
                          Help,
                          Type,
                          Values))
      end,
      seshat:format(ra,
                    #{labels => as_binary,
                      filter_fun => fun onlyCoordinationSystem/1})).

onlyCoordinationSystem(#{ra_system := coordination}) -> true;
onlyCoordinationSystem(_) -> false.

onlyQueues(#{queue := _}) -> true;
onlyQueues(_) -> false.
