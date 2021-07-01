%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates. All rights reserved.
%%

-module(rabbit_mgmt_agent_sup_sup).

-behaviour(supervisor).

-export([init/1]).
-export([start_link/0, start_child/0]).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("rabbit_mgmt_agent.hrl").

start_child() ->
    supervisor:start_child(?MODULE, sup()).

sup() ->
    #{
        id      => rabbit_mgmt_agent_sup,
        start   => {rabbit_mgmt_agent_sup, start_link, []},
        restart => temporary,
        shutdown => ?SUPERVISOR_WAIT,
        type    => supervisor,
        modules => [rabbit_mgmt_agent_sup]
    }.

init([]) ->
    Flags = #{
        strategy  => one_for_one,
        intensity => 0,
        period    => 1
    },
    PgScope = #{
        id      => ?MANAGEMENT_PG_SCOPE,
        start   => {pg, start_link, [?MANAGEMENT_PG_SCOPE]},
        restart => temporary,
        shutdown => ?SUPERVISOR_WAIT,
        modules => []
    },
    Specs = [
        PgScope,
        sup()
    ],
    {ok, {Flags, Specs}}.

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).
