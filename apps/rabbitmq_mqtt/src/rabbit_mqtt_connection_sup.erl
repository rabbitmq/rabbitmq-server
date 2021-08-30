%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mqtt_connection_sup).

-behaviour(supervisor2).
-behaviour(ranch_protocol).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([start_link/3, start_keepalive_link/0]).

-export([init/1]).

%%----------------------------------------------------------------------------

start_link(Ref, _Transport, []) ->
    {ok, SupPid} = supervisor2:start_link(?MODULE, []),
    {ok, KeepaliveSup} = supervisor2:start_child(
                          SupPid,
                          {rabbit_mqtt_keepalive_sup,
                           {rabbit_mqtt_connection_sup, start_keepalive_link, []},
                           intrinsic, infinity, supervisor, [rabbit_keepalive_sup]}),
    {ok, ReaderPid} = supervisor2:start_child(
                        SupPid,
                        {rabbit_mqtt_reader,
                         {rabbit_mqtt_reader, start_link, [KeepaliveSup, Ref]},
                         intrinsic, ?WORKER_WAIT, worker, [rabbit_mqtt_reader]}),
    {ok, SupPid, ReaderPid}.

start_keepalive_link() ->
    supervisor2:start_link(?MODULE, []).

%%----------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_all, 0, 1}, []}}.


