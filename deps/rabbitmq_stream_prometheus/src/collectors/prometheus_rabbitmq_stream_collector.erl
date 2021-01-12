%% The contents of this file are subject to the Mozilla Public License
%% Version 2.0 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/en-US/MPL/2.0/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is Pivotal Software, Inc.
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(prometheus_rabbitmq_stream_collector).

-export([deregister_cleanup/1,
         collect_mf/2,
         collect_metrics/2]).

-include_lib("prometheus/include/prometheus.hrl").
-include_lib("rabbitmq_stream/include/rabbit_stream_metrics.hrl").

-behaviour(prometheus_collector).

-define(METRIC_NAME_PREFIX, "rabbitmq_stream_").
-define(METRICS_RAW,
        [% { ETS table, [ {index, conversion, Prometheus metrics name, type, help, key} ] }
         {?TABLE_PUBLISHER,
          [{2,
            undefined,
            producers_messages_published_total,
            counter,
            "Total number of messages published to streams",
            published},
           {2,
            undefined,
            producers_messages_confirmed_total,
            counter,
            "Total number of messages confirmed",
            confirmed},
           {2,
            undefined,
            producers_messages_errored_total,
            counter,
            "Total number of messages errored",
            errored}]},
         {?TABLE_CONSUMER,
          [{2,
            undefined,
            consumers_messages_consumed_total,
            counter,
            "Total number of messages from streams",
            consumed}]}]).

%% Collector API

deregister_cleanup(_) ->
    ok.

collect_mf('per-object', Callback) ->
    collect(true, Callback);
collect_mf(_Registry, Callback) ->
    PerObjectMetrics =
        application:get_env(rabbitmq_prometheus, return_per_object_metrics,
                            false),
    collect(PerObjectMetrics, Callback).

collect(PerObjectMetrics, Callback) ->
    [begin
         Data = get_data(Table, PerObjectMetrics),
         mf(Callback, Contents, Data)
     end
     || {Table, Contents} <- ?METRICS_RAW],
    ok.

get_data(?TABLE_PUBLISHER = Table, false) ->
    {Table, A1, A2, A3} =
        ets:foldl(fun({_, Props}, {T, A1, A2, A3}) ->
                     {T,
                      sum(proplists:get_value(published, Props), A1),
                      sum(proplists:get_value(confirmed, Props), A2),
                      sum(proplists:get_value(errored, Props), A3)}
                  end,
                  empty(Table), Table),
    [{Table, [{published, A1}, {confirmed, A2}, {errored, A3}]}];
get_data(?TABLE_CONSUMER = Table, false) ->
    {Table, A1} =
        ets:foldl(fun({_, Props}, {T, A1}) ->
                     {T, sum(proplists:get_value(consumed, Props), A1)}
                  end,
                  empty(Table), Table),
    [{Table, [{consumed, A1}]}];
get_data(Table, _) ->
    ets:tab2list(Table).

mf(Callback, Contents, Data) ->
    [begin
         Fun = case Conversion of
                   undefined ->
                       fun(D) -> proplists:get_value(Key, element(Index, D))
                       end;
                   BaseUnitConversionFactor ->
                       fun(D) ->
                          proplists:get_value(Key, element(Index, D))
                          / BaseUnitConversionFactor
                       end
               end,
         Callback(prometheus_model_helpers:create_mf(?METRIC_NAME(Name),
                                                     Help,
                                                     catch_boolean(Type),
                                                     ?MODULE,
                                                     {Type, Fun, Data}))
     end
     || {Index, Conversion, Name, Type, Help, Key} <- Contents].

collect_metrics(_Name, {Type, Fun, Items}) ->
    [metric(Type, [], Fun(Item)) || Item <- Items].

metric(counter, Labels, Value) ->
    emit_counter_metric_if_defined(Labels, Value);
metric(gauge, Labels, Value) ->
    emit_gauge_metric_if_defined(Labels, Value).

emit_counter_metric_if_defined(Labels, Value) ->
    case Value of
        undefined ->
            undefined;
        '' ->
            prometheus_model_helpers:counter_metric(Labels, undefined);
        Value ->
            prometheus_model_helpers:counter_metric(Labels, Value)
    end.

emit_gauge_metric_if_defined(Labels, Value) ->
    case Value of
        undefined ->
            undefined;
        '' ->
            prometheus_model_helpers:gauge_metric(Labels, undefined);
        Value ->
            prometheus_model_helpers:gauge_metric(Labels, Value)
    end.

empty(T) when T == ?TABLE_CONSUMER ->
    {T, 0};
empty(T) when T == ?TABLE_PUBLISHER ->
    {T, 0, 0, 0}.

sum(undefined, B) ->
    B;
sum('', B) ->
    B;
sum(A, B) ->
    A + B.

catch_boolean(boolean) ->
    untyped;
catch_boolean(T) ->
    T.
