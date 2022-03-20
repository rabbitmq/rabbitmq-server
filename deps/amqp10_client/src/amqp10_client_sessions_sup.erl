%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(amqp10_client_sessions_sup).

-behaviour(supervisor).

%% Private API.
-export([start_link/0]).

%% Supervisor callbacks.
-export([init/1]).

-define(CHILD(Id, Mod, Type, Args), {Id, {Mod, start_link, Args},
                                     transient, 5000, Type, [Mod]}).

%% -------------------------------------------------------------------
%% Private API.
%% -------------------------------------------------------------------

-spec start_link() ->
    {ok, pid()} | ignore | {error, any()}.

start_link() ->
    supervisor:start_link(?MODULE, []).

%% -------------------------------------------------------------------
%% Supervisor callbacks.
%% -------------------------------------------------------------------

init(Args) ->
    Template = ?CHILD(session, amqp10_client_session, worker, Args),
    {ok, {{simple_one_for_one, 0, 1}, [Template]}}.
