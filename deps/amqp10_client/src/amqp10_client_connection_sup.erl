%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2020 Pivotal Software, Inc.  All rights reserved.
%%
-module(amqp10_client_connection_sup).

-behaviour(supervisor).

%% Private API.
-export([start_link/1]).

%% Supervisor callbacks.
-export([init/1]).

-define(CHILD(Id, Mod, Type, Args), {Id, {Mod, start_link, Args},
                                     transient, 5000, Type, [Mod]}).

%% -------------------------------------------------------------------
%% Private API.
%% -------------------------------------------------------------------

-spec start_link(amqp10_client_connection:connection_config()) ->
    {ok, pid()} | ignore | {error, any()}.
start_link(Config) ->
    supervisor:start_link(?MODULE, [Config]).

%% -------------------------------------------------------------------
%% Supervisor callbacks.
%% -------------------------------------------------------------------

init(Args) ->
    ReaderSpec = ?CHILD(reader, amqp10_client_frame_reader,
                        worker, [self() | Args]),
    ConnectionSpec = ?CHILD(connection, amqp10_client_connection,
                            worker, [self() | Args]),
    SessionsSupSpec = ?CHILD(sessions, amqp10_client_sessions_sup,
                             supervisor, []),
    {ok, {{one_for_all, 0, 1}, [ConnectionSpec,
                                ReaderSpec,
                                SessionsSupSpec]}}.
