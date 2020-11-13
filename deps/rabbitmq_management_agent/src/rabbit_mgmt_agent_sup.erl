%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ Management Console.
%%
%%   The Initial Developer of the Original Code is GoPivotal, Inc.
%%   Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
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
    ExternalStats = {rabbit_mgmt_external_stats,
                     {rabbit_mgmt_external_stats, start_link, []},
                     permanent, 5000, worker, [rabbit_mgmt_external_stats]},
    GC = {rabbit_mgmt_gc, {rabbit_mgmt_gc, start_link, []},
          permanent, ?WORKER_WAIT, worker, [rabbit_mgmt_gc]},
    {ok, {{one_for_one, 100, 10}, [ST, MD, ExternalStats, GC | MC ++ MGC]}}.

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).
