%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%
-module(rabbitmq_federation_prometheus).
-export([deregister_cleanup/1,
         collect_mf/2]).

-import(prometheus_model_helpers, [create_mf/4]).

-behaviour(prometheus_collector).

-rabbit_boot_step({?MODULE, [
  {description, "rabbitmq_federation prometheus collector plugin"},
  {mfa,         {?MODULE, start, []}},
  {cleanup,     {?MODULE, stop, []}}
]}).

%% API exports
-export([start/0, stop/0]).

start() ->
    {ok, _} = application:ensure_all_started(prometheus),
    prometheus_registry:register_collector(?MODULE).

stop() ->
    prometheus_registry:deregister_collector(?MODULE).

%%====================================================================
%% Collector API
%%====================================================================

deregister_cleanup(_) -> ok.

collect_mf(_Registry, Callback) ->
    Status = rabbit_federation_status:status(500),
    StatusGroups = lists:foldl(fun(S, Acc) ->
                                       %% note Init value set to 1 because if status seen first time
                                       %% update with will take Init and put into Acc, wuthout calling fun
                                       maps:update_with(proplists:get_value(status, S), fun(C) -> C + 1 end, 1, Acc)
                               end, #{}, Status),
    Metrics = [{rabbitmq_federation_links, gauge, "Current number of federation links",
               [{[{status, S}], C} || {S, C} <- maps:to_list(StatusGroups)]}],
    _ = [add_metric_family(Metric, Callback) || Metric <- Metrics],
    ok.

add_metric_family({Name, Type, Help, Metrics}, Callback) ->
    Callback(create_mf(Name, Help, Type, Metrics)).

%%====================================================================
%% Private Parts
%%====================================================================
