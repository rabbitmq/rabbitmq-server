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
%%   Copyright (c) 2011-2015 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_sup).

-behaviour(supervisor).

-export([init/1]).
-export([start_link/0]).
-export([setup_wm_logging/0]).

-include("rabbit_mgmt_metrics.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_core_metrics.hrl").

init([]) ->
    ST = {rabbit_mgmt_storage, {rabbit_mgmt_storage, start_link, []},
	  permanent, ?WORKER_WAIT, worker, [rabbit_mgmt_storage]},
    DB = {rabbit_mgmt_db, {rabbit_mgmt_db, start_link, []},
          permanent, ?WORKER_WAIT, worker, [rabbit_mgmt_db]},
    MC = [{rabbit_mgmt_metrics_collector:name(Table),
           {rabbit_mgmt_metrics_collector, start_link, [Table]},
           permanent, ?WORKER_WAIT, worker, [rabbit_mgmt_metrics_collector]}
          || {Table, _} <- ?CORE_TABLES],
    MGC = [{rabbit_mgmt_metrics_gc:name(Event),
            {rabbit_mgmt_metrics_gc, start_link, [Event]},
             permanent, ?WORKER_WAIT, worker, [rabbit_mgmt_metrics_gc]}
           || Event <- ?GC_EVENTS],
    MD = {delegate_management_sup, {delegate_sup, start_link, [5, ?DELEGATE_PREFIX]},
          permanent, ?SUPERVISOR_WAIT, supervisor, [delegate_sup]},
    WP = {management_worker_pool_sup, {worker_pool_sup, start_link, [3, management_worker_pool]},
          permanent, ?SUPERVISOR_WAIT, supervisor, [management_worker_pool_sup]},
    DBC = {rabbit_mgmt_db_cache_sup, {rabbit_mgmt_db_cache_sup, start_link, []},
          permanent, ?SUPERVISOR_WAIT, supervisor, [rabbit_mgmt_db_cache_sup]},
    %% Since we have a lot of collectors abd GCs, we should allow more restarts
    {ok, {{one_for_one, 100, 1}, [ST, DB, MD, WP, DBC] ++ MC ++ MGC}}.

start_link() ->
    Res = supervisor:start_link({local, ?MODULE}, ?MODULE, []),
    setup_wm_logging(),
    Res.

%% While the project has switched to Cowboy for HTTP handling, we still use
%% the logger from Webmachine; at least until RabbitMQ switches to Lager or
%% similar.
setup_wm_logging() ->
    {ok, LogDir} = application:get_env(rabbitmq_management, http_log_dir),
    case LogDir of
        none -> ok;
        _    -> webmachine_log:add_handler(webmachine_log_handler, [LogDir])
    end.
