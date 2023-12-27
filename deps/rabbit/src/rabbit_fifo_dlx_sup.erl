%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2018-2023 VMware, Inc. or its affiliates.  All rights reserved.

-module(rabbit_fifo_dlx_sup).

-behaviour(supervisor).

-rabbit_boot_step({?MODULE,
                   [{description, "supervisor of quorum queue dead-letter workers"},
                    {mfa,         {rabbit_sup, start_supervisor_child, [?MODULE]}},
                    {requires,    kernel_ready},
                    {enables,     core_initialized}]}).

%% supervisor callback
-export([init/1]).
%% client API
-export([start_link/0]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => simple_one_for_one,
                 intensity => 100,
                 period => 1},
    Worker = rabbit_fifo_dlx_worker,
    ChildSpec = #{id => Worker,
                  start => {Worker, start_link, []},
                  type => worker,
                  restart => transient,
                  modules => [Worker]},
    {ok, {SupFlags, [ChildSpec]}}.
