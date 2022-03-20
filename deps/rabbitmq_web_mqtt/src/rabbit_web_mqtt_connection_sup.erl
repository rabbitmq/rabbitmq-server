%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_web_mqtt_connection_sup).

-behaviour(supervisor2).
-behaviour(ranch_protocol).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([start_link/3, start_keepalive_link/0]).

-export([init/1]).

%%----------------------------------------------------------------------------

start_link(Ref, Transport, CowboyOpts0) ->
    {ok, SupPid} = supervisor2:start_link(?MODULE, []),
    {ok, KeepaliveSup} = supervisor2:start_child(
                          SupPid,
                          {rabbit_web_mqtt_keepalive_sup,
                           {?MODULE, start_keepalive_link, []},
                           intrinsic, infinity, supervisor, [rabbit_keepalive_sup]}),

    %% In order for the Websocket handler to receive the KeepaliveSup
    %% variable, we need to pass it first through the environment and
    %% then have the middleware rabbit_web_mqtt_middleware place it
    %% in the initial handler state.
    Env = maps:get(env, CowboyOpts0),
    CowboyOpts = CowboyOpts0#{env => Env#{keepalive_sup => KeepaliveSup}},
    Protocol = case Transport of
        ranch_tcp -> cowboy_clear;
        ranch_ssl -> cowboy_tls
    end,
    {ok, ReaderPid} = supervisor2:start_child(
                        SupPid,
                        {Protocol,
                         {Protocol, start_link, [Ref, Transport, CowboyOpts]},
                         intrinsic, ?WORKER_WAIT, worker, [Protocol]}),
    {ok, SupPid, ReaderPid}.

start_keepalive_link() ->
    supervisor2:start_link(?MODULE, []).

%%----------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_all, 0, 1}, []}}.
