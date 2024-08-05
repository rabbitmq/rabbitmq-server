%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%
-module(prometheus_rabbitmq_shovel_collector).
-export([deregister_cleanup/1,
         collect_mf/2]).

-import(prometheus_model_helpers, [create_mf/4]).

-behaviour(prometheus_collector).

%% API exports
-export([]).

%%====================================================================
%% Collector API
%%====================================================================

deregister_cleanup(_) -> ok.

collect_mf(_Registry, Callback) ->
    Status = rabbit_shovel_status:status(500),
    {StaticStatusGroups, DynamicStatusGroups} = lists:foldl(fun({_,static,{S, _}, _}, {SMap, DMap}) ->
                                                                    {maps:update_with(S, fun(C) -> C + 1 end, 1, SMap), DMap};
                                                               ({_,dynamic,{S, _}, _}, {SMap, DMap}) ->
                                                                    {SMap, maps:update_with(S, fun(C) -> C + 1 end, 1, DMap)}
                                                            end, {#{}, #{}}, Status),

    Metrics = [{rabbitmq_shovel_dynamic, gauge, "Current number of dynamic shovels.",
                [{[{status, S}], C} || {S, C} <- maps:to_list(DynamicStatusGroups)]},
               {rabbitmq_shovel_static, gauge, "Current number of static shovels.",
                [{[{status, S}], C} || {S, C} <- maps:to_list(StaticStatusGroups)]}
              ],
    _ = [add_metric_family(Metric, Callback) || Metric <- Metrics],
    ok.

add_metric_family({Name, Type, Help, Metrics}, Callback) ->
    Callback(create_mf(Name, Help, Type, Metrics)).

%%====================================================================
%% Private Parts
%%====================================================================
