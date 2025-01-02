%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_connection_helper_sup).

%% Supervises auxiliary processes of AMQP 0-9-1 connections:
%%
%%  * Channel supervisor
%%  * Heartbeat receiver
%%  * Heartbeat sender
%%  * Exclusive queue collector
%%
%% See also rabbit_heartbeat, rabbit_channel_sup_sup, rabbit_queue_collector.

-behaviour(supervisor).

-export([start_link/1]).
-export([
    start_channel_sup_sup/1,
    start_queue_collector/2
]).

-export([init/1]).

-include_lib("rabbit_common/include/rabbit.hrl").

%%----------------------------------------------------------------------------

-spec start_link(supervisor:sup_flags()) ->
    supervisor:startlink_ret().
start_link(SupFlags) ->
    supervisor:start_link(?MODULE, SupFlags).

-spec start_channel_sup_sup(pid()) -> rabbit_types:ok_pid_or_error().

start_channel_sup_sup(SupPid) ->
    ChildSpec = #{id => channel_sup_sup,
                  start => {rabbit_channel_sup_sup, start_link, []},
                  restart => transient,
                  significant => true,
                  shutdown => infinity,
                  type => supervisor,
                  modules => [rabbit_channel_sup_sup]},
    supervisor:start_child(SupPid, ChildSpec).

-spec start_queue_collector(pid(), rabbit_types:proc_name()) ->
    rabbit_types:ok_pid_or_error().

start_queue_collector(SupPid, Identity) ->
    ChildSpec = #{id => collector,
                  start => {rabbit_queue_collector, start_link, [Identity]},
                  restart => transient,
                  significant => true,
                  shutdown => ?WORKER_WAIT,
                  type => worker,
                  modules => [rabbit_queue_collector]},
    supervisor:start_child(SupPid, ChildSpec).

%%----------------------------------------------------------------------------

init(SupFlags) ->
    ?LG_PROCESS_TYPE(connection_helper_sup),
    {ok, {SupFlags, []}}.
