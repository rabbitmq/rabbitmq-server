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

-behaviour(mirrored_supervisor).

-export([init/1]).
-export([start_link/0]).

-include("rabbit_mgmt_metrics.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

init([]) ->
    COLLECTOR = {rabbit_mgmt_event_collector,
                 {rabbit_mgmt_event_collector, start_link, []},
                 permanent, ?WORKER_WAIT, worker, [rabbit_mgmt_event_collector]},
    CCOLLECTOR = {rabbit_mgmt_channel_stats_collector,
                  {rabbit_mgmt_channel_stats_collector, start_link, []},
                  permanent, ?WORKER_WAIT, worker, [rabbit_mgmt_channel_stats_collector]},
    QCOLLECTOR = {rabbit_mgmt_queue_stats_collector,
                  {rabbit_mgmt_queue_stats_collector, start_link, []},
                  permanent, ?WORKER_WAIT, worker, [rabbit_mgmt_queue_stats_collector]},
    GC = [{rabbit_mgmt_stats_gc:name(Table), {rabbit_mgmt_stats_gc, start_link, [Table]},
           permanent, ?WORKER_WAIT, worker, [rabbit_mgmt_stats_gc]}
          || Table <- ?AGGR_TABLES],
    ProcGC = [{rabbit_mgmt_stats_gc:name(Table), {rabbit_mgmt_stats_gc, start_link, [Table]},
           permanent, ?WORKER_WAIT, worker, [rabbit_mgmt_stats_gc]}
          || Table <- ?PROC_STATS_TABLES],
    DB = {rabbit_mgmt_db, {rabbit_mgmt_db, start_link, []},
          permanent, ?WORKER_WAIT, worker, [rabbit_mgmt_db]},
    {ok, {{one_for_one, 10, 10}, [COLLECTOR, CCOLLECTOR, QCOLLECTOR, DB] ++ GC ++ ProcGC}}.

start_link() ->
     mirrored_supervisor:start_link(
       {local, ?MODULE}, ?MODULE, fun rabbit_misc:execute_mnesia_transaction/1,
       ?MODULE, []).
