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
%% Copyright (c) 2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_ws_protocol).
-behaviour(ranch_protocol).

-export([start_link/4]).

start_link(Ref, Sock, Transport, CowboyOpts0) ->
    %% In order for the Websocket handler to receive the KeepaliveSup
    %% variable, we need to pass it first through the environment and
    %% then have the middleware rabbit_web_mqtt_middleware place it
    %% in the initial handler state.
    Env = maps:get(env, CowboyOpts0),
    CowboyOpts = CowboyOpts0#{env => Env#{socket => Sock}},
    Protocol = case Transport of
        ranch_tcp -> cowboy_clear;
        ranch_ssl -> cowboy_tls
    end,
    Protocol:start_link(Ref, Sock, Transport, CowboyOpts).
