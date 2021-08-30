%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_sup).

-behaviour(supervisor).

-export([init/1]).
-export([start_link/0]).
-export([setup_wm_logging/0]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_metrics.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_core_metrics.hrl").

init([]) ->
    DB = {rabbit_mgmt_db, {rabbit_mgmt_db, start_link, []},
          permanent, ?WORKER_WAIT, worker, [rabbit_mgmt_db]},
    WP = {management_worker_pool_sup, {worker_pool_sup, start_link, [3, management_worker_pool]},
          permanent, ?SUPERVISOR_WAIT, supervisor, [management_worker_pool_sup]},
    DBC = {rabbit_mgmt_db_cache_sup, {rabbit_mgmt_db_cache_sup, start_link, []},
          permanent, ?SUPERVISOR_WAIT, supervisor, [rabbit_mgmt_db_cache_sup]},
    {ok, {{one_for_one, 100, 1}, [DB, WP, DBC]}}.

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
