%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_queue_federation_sup).

-behaviour(supervisor).

%% Supervises everything. There is just one of these.

-include_lib("rabbit_common/include/rabbit.hrl").
-include("rabbit_queue_federation.hrl").

-define(SUPERVISOR, ?MODULE).

-export([start_link/0, stop/0]).

-export([init/1]).

%% This supervisor needs to be part of the rabbit application since
%% a) it needs to be in place when exchange recovery takes place
%% b) it needs to go up and down with rabbit

-rabbit_boot_step({rabbit_queue_federation_supervisor,
                   [{description, "federation"},
                    {mfa,         {rabbit_sup, start_child, [?MODULE]}},
                    {requires,    [kernel_ready, rabbit_federation_supervisor]},
                    {cleanup,     {?MODULE, stop, []}},
                    {enables,     rabbit_federation_queue}]}).

%%----------------------------------------------------------------------------

start_link() ->
    supervisor:start_link({local, ?SUPERVISOR}, ?MODULE, []).

stop() ->
    ok = supervisor:terminate_child(rabbit_sup, ?MODULE),
    ok = supervisor:delete_child(rabbit_sup, ?MODULE).

%%----------------------------------------------------------------------------

init([]) ->
    QLinkSupSup = #{
        id       => q_links,
        start    => {rabbit_federation_queue_link_sup_sup, start_link, []},
        restart  => transient,
        shutdown => ?SUPERVISOR_WAIT,
        type     => supervisor,
        modules  => [rabbit_federation_queue_link_sup_sup]
    },
    %% with default reconnect-delay of 5 second, this supports up to
    %% 100 links constantly failing and being restarted a minute
    %% (or 200 links if reconnect-delay is 10 seconds, 600 with 30 seconds,
    %% etc: N * (60/reconnect-delay) <= 1200)
    Flags = #{
        strategy  => one_for_one,
        intensity => 1200,
        period    => 60
    },
    Specs = [QLinkSupSup],
    {ok, {Flags, Specs}}.
