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
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_ws_client_sup).
-behaviour(supervisor2).

-export([start_client/1]).
-export([init/1]).

-include_lib("amqp_client/include/amqp_client.hrl").


%% --------------------------------------------------------------------------

start_client({Conn, Heartbeat}) ->
    {ok, SupPid} = supervisor2:start_link(?MODULE, []),
    {ok, Client} = supervisor2:start_child(
                     SupPid, client_spec(SupPid, Conn, Heartbeat, Conn)),
    {ok, SupPid, Client}.


client_spec(SupPid, Conn, Heartbeat, Conn) ->
    {rabbit_ws_client, {rabbit_ws_client, start_link, [{SupPid, Conn, Heartbeat, Conn}]},
     intrinsic, ?WORKER_WAIT, worker, [rabbit_ws_client]}.

init(_Any) ->
    {ok, {{one_for_all, 0, 1}, []}}.
