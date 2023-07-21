%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(rabbit_amqp1_0_session_sup).
-behaviour(supervisor).

-include_lib("rabbit_common/include/rabbit.hrl").

%% client API
-export([start_link/1,
         start_session/2]).

%% supervisor callback
-export([init/1]).

-spec start_link(Reader :: pid()) ->
    supervisor:startlink_ret().
start_link(ReaderPid) ->
    supervisor:start_link(?MODULE, ReaderPid).

init(ReaderPid) ->
    SupFlags = #{strategy => simple_one_for_one,
                 intensity => 0,
                 period => 1},
    ChildSpec = #{id => amqp1_0_session,
                  start => {rabbit_amqp1_0_session, start_link, [ReaderPid]},
                  restart => temporary,
                  shutdown => ?WORKER_WAIT,
                  type => worker,
                  modules => [rabbit_amqp1_0_session]},
    {ok, {SupFlags, [ChildSpec]}}.

-spec start_session(pid(), list()) ->
    supervisor:startchild_ret().
start_session(SessionSupPid, Args) ->
    supervisor:start_child(SessionSupPid, Args).
