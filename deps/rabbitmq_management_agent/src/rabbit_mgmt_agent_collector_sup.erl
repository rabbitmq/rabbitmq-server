%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_agent_collector_sup).

-behaviour(supervisor2).

-export([start_link/0, start_child/3]).

-export([init/1]).

-include_lib("rabbit_common/include/rabbit.hrl").

-define(SERVER, ?MODULE).

%%----------------------------------------------------------------------------

-spec start_link() -> rabbit_types:ok_pid_or_error().
-spec start_child(pid(), atom(), integer()) -> rabbit_types:ok_pid_or_error().

%%----------------------------------------------------------------------------

start_link() ->
    supervisor2:start_link({local, ?SERVER}, ?MODULE, []).

start_child(Server, Table, Interval) ->
    supervisor2:start_child(?SERVER, [Server, Table, Interval]).

init([]) ->
    {ok, {{simple_one_for_one, 10, 10},
          [{rabbit_mgmt_agent_collector,
            {rabbit_mgmt_agent_collector, start_link, []},
            transient, ?WORKER_WAIT, worker, [rabbit_mgmt_agent_collector]}]}}.
