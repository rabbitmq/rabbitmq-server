%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_federation_sup).

-behaviour(supervisor).

%% Supervises everything. There is just one of these.

-include_lib("rabbit_common/include/rabbit.hrl").

-define(SUPERVISOR, rabbit_federation_sup).

-export([start_link/0, stop/0]).

-export([init/1]).

%% This supervisor needs to be part of the rabbit application since
%% a) it needs to be in place when exchange recovery takes place
%% b) it needs to go up and down with rabbit

-rabbit_boot_step({rabbit_federation_supervisor,
                   [{description, "federation"},
                    {mfa,         {rabbit_sup, start_child, [?MODULE]}},
                    {requires,    kernel_ready},
                    {cleanup,     {?MODULE, stop, []}},
                    {enables,     rabbit_federation_exchange},
                    {enables,     rabbit_federation_queue}]}).

%%----------------------------------------------------------------------------

start_link() ->
    R = supervisor:start_link({local, ?SUPERVISOR}, ?MODULE, []),
    rabbit_federation_event:add_handler(),
    R.

stop() ->
    rabbit_federation_event:remove_handler(),
    ok = supervisor:terminate_child(rabbit_sup, ?MODULE),
    ok = supervisor:delete_child(rabbit_sup, ?MODULE).

%%----------------------------------------------------------------------------

init([]) ->
    Status = #{
        id       => status,
        start    => {rabbit_federation_status, start_link, []},
        restart  => transient,
        shutdown => ?WORKER_WAIT,
        type     => worker,
        modules  => [rabbit_federation_status]
    },
    XLinkSupSup = #{
        id       => x_links,
        start    => {rabbit_federation_exchange_link_sup_sup, start_link, []},
        restart  => transient,
        shutdown => ?SUPERVISOR_WAIT,
        type     => supervisor,
        modules  =>[rabbit_federation_exchange_link_sup_sup]
    },
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
    Specs = [Status, XLinkSupSup, QLinkSupSup],
    {ok, {Flags, Specs}}.
