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
%% Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
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

-include("rabbit.hrl").

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
