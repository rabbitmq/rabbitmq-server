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
          || Table <- ?CORE_TABLES],
    MGC = [{rabbit_mgmt_metrics_gc:name(Table),
	    {rabbit_mgmt_metrics_gc, start_link, [Table]},
	    permanent, ?WORKER_WAIT, worker, [rabbit_mgmt_metrics_gc]}
	   || Table <- ?GC_EVENTS],
    MD = {delegate_management_sup, {delegate_sup, start_link, [5, "delegate_management_"]},
          permanent, ?SUPERVISOR_WAIT, supervisor, [delegate_sup]},
    {ok, {{one_for_one, 10, 10}, [ST, DB, MD] ++ MC ++ MGC}}.

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).
