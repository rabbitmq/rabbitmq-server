%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
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

-behaviour(supervisor2).

-export([start_link/0]).
-export([start_channel_sup_sup/1,
         start_queue_collector/2]).

-export([init/1]).

-include_lib("rabbit_common/include/rabbit.hrl").

%%----------------------------------------------------------------------------

-spec start_link() -> rabbit_types:ok_pid_or_error().

start_link() ->
    supervisor2:start_link(?MODULE, []).

-spec start_channel_sup_sup(pid()) -> rabbit_types:ok_pid_or_error().

start_channel_sup_sup(SupPid) ->
    supervisor2:start_child(
          SupPid,
          {channel_sup_sup, {rabbit_channel_sup_sup, start_link, []},
           intrinsic, infinity, supervisor, [rabbit_channel_sup_sup]}).

-spec start_queue_collector(pid(), rabbit_types:proc_name()) ->
          rabbit_types:ok_pid_or_error().

start_queue_collector(SupPid, Identity) ->
    supervisor2:start_child(
      SupPid,
      {collector, {rabbit_queue_collector, start_link, [Identity]},
       intrinsic, ?WORKER_WAIT, worker, [rabbit_queue_collector]}).

%%----------------------------------------------------------------------------

init([]) ->
    ?LG_PROCESS_TYPE(connection_helper_sup),
    {ok, {{one_for_one, 10, 10}, []}}.
