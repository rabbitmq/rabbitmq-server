%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
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
