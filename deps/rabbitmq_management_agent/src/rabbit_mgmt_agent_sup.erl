%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_agent_sup).

%% pg2 is deprecated in OTP 23.
-compile(nowarn_deprecated_function).

-behaviour(supervisor).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_core_metrics.hrl").
-include("rabbit_mgmt_metrics.hrl").

-export([init/1]).
-export([start_link/0]).

init([]) ->
    MCs = maybe_enable_metrics_collector(),
    ExternalStats = {rabbit_mgmt_external_stats,
                     {rabbit_mgmt_external_stats, start_link, []},
                     permanent, 5000, worker, [rabbit_mgmt_external_stats]},
    {ok, {{one_for_one, 100, 10}, [ExternalStats] ++ MCs}}.

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


maybe_enable_metrics_collector() ->
    case application:get_env(rabbitmq_management_agent, disable_metrics_collector, false) of
        false ->
            pg2:create(management_db),
            ok = pg2:join(management_db, self()),
            ST = {rabbit_mgmt_storage, {rabbit_mgmt_storage, start_link, []},
                  permanent, ?WORKER_WAIT, worker, [rabbit_mgmt_storage]},
            MD = {delegate_management_sup, {delegate_sup, start_link, [5, ?DELEGATE_PREFIX]},
                  permanent, ?SUPERVISOR_WAIT, supervisor, [delegate_sup]},
            MC = [{rabbit_mgmt_metrics_collector:name(Table),
                   {rabbit_mgmt_metrics_collector, start_link, [Table]},
                   permanent, ?WORKER_WAIT, worker, [rabbit_mgmt_metrics_collector]}
                  || {Table, _} <- ?CORE_TABLES],
            MGC = [{rabbit_mgmt_metrics_gc:name(Event),
                    {rabbit_mgmt_metrics_gc, start_link, [Event]},
                    permanent, ?WORKER_WAIT, worker, [rabbit_mgmt_metrics_gc]}
                   || Event <- ?GC_EVENTS],
            GC = {rabbit_mgmt_gc, {rabbit_mgmt_gc, start_link, []},
          permanent, ?WORKER_WAIT, worker, [rabbit_mgmt_gc]},
            [ST, MD, GC | MC ++ MGC];
        true ->
            []
    end.
