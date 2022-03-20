%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates. All rights reserved.
%%

-module(rabbit_mgmt_sup_sup).

-behaviour(supervisor).

-export([init/1]).
-export([start_link/0, start_child/0]).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("rabbit_mgmt.hrl").

start_child() ->
    supervisor:start_child(?MODULE, sup()).

sup() ->
    #{
        id      => rabbit_mgmt_sup,
        start   => {rabbit_mgmt_sup, start_link, []},
        restart => temporary,
        shutdown => ?SUPERVISOR_WAIT,
        type    => supervisor,
        modules => [rabbit_mgmt_sup]
    }.

init([]) ->
    %% This scope is used in the child process, so start it
    %% early. We don't attach it to the supervision tree because
    %%
    %% * rabbitmq_management and rabbitmq_management_agent share a scope
    %% * start an already running scope results in an "already started" error returned
    %% * such errors wreck supervision tree startup
    %%
    %% So we expect management agent to start the scope as part of its
    %% supervision tree and only start it here for environments
    %% such as tests that may be testing parts of this plugin in isolation.
    _ = pg:start_link(?MANAGEMENT_PG_SCOPE),

    Flags = #{
        strategy  => one_for_one,
        intensity => 0,
        period    => 1
    },
    Specs = [sup()],
    {ok, {Flags, Specs}}.

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).
