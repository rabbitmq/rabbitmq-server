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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_amqp1_0_client_sup).
-behaviour(supervisor2).

-define(MAX_WAIT, 16#ffffffff).
-export([start_link/0, init/1]).

start_link() ->
    {ok, SupPid} = supervisor2:start_link(?MODULE, []),
    {ok, Collector} =
        supervisor2:start_child(
          SupPid,
          {collector, {rabbit_queue_collector, start_link, []},
           intrinsic, ?MAX_WAIT, worker, [rabbit_queue_collector]}),
    {ok, ChannelSupSupPid} =
        supervisor2:start_child(
          SupPid,
          {channel_sup_sup, {rabbit_amqp1_0_session_sup_sup, start_link, []},
           intrinsic, infinity, supervisor, [rabbit_amqp1_0_session_sup_sup]}),
    {ok, ReaderPid} =
        supervisor2:start_child(
          SupPid,
          {reader, {rabbit_amqp1_0_reader, start_link,
                    [ChannelSupSupPid, Collector,
                     rabbit_heartbeat:start_heartbeat_fun(SupPid)]},
           intrinsic, ?MAX_WAIT, worker, [rabbit_amqp1_0_reader]}),
    {ok, SupPid, ReaderPid}.

%%--------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_all, 0, 1}, []}}.
