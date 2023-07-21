%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%
-module(amqp10_client_connection_sup).

-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

start_link(Config) ->
    supervisor:start_link(?MODULE, [Config]).

init(Args0) ->
    SupFlags = #{strategy => one_for_all,
                 intensity => 0,
                 period => 1},
    Fun = start_link,
    Args = [self() | Args0],
    ConnectionSpec = #{id => connection,
                       start => {amqp10_client_connection, Fun, Args},
                       restart => transient},
    ReaderSpec = #{id => reader,
                   start => {amqp10_client_frame_reader, Fun, Args},
                   restart => transient},
    SessionsSupSpec = #{id => sessions,
                        start => {amqp10_client_sessions_sup, Fun, []},
                        restart => transient,
                        type => supervisor},
    {ok, {SupFlags, [ConnectionSpec,
                     ReaderSpec,
                     SessionsSupSpec]}}.
