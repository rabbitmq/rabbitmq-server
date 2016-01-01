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

-module(rabbit_mqtt_connection_sup).

-behaviour(supervisor2).
-behaviour(ranch_protocol).

-define(MAX_WAIT, 16#ffffffff).

-export([start_link/4, start_keepalive_link/0]).

-export([init/1]).

%%----------------------------------------------------------------------------

start_link(Ref, Sock, _Transport, []) ->
    {ok, SupPid} = supervisor2:start_link(?MODULE, []),
    {ok, KeepaliveSup} = supervisor2:start_child(
                          SupPid,
                          {rabbit_mqtt_keepalive_sup,
                           {rabbit_mqtt_connection_sup, start_keepalive_link, []},
                           intrinsic, infinity, supervisor, [rabbit_keepalive_sup]}),
    {ok, ReaderPid} = supervisor2:start_child(
                        SupPid,
                        {rabbit_mqtt_reader,
                         {rabbit_mqtt_reader, start_link, [KeepaliveSup, Ref, Sock]},
                         intrinsic, ?MAX_WAIT, worker, [rabbit_mqtt_reader]}),
    {ok, SupPid, ReaderPid}.

start_keepalive_link() ->
    supervisor2:start_link(?MODULE, []).

%%----------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_all, 0, 1}, []}}.


