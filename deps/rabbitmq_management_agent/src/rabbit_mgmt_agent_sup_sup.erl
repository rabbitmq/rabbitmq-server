%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates. All rights reserved.
%%

-module(rabbit_mgmt_agent_sup_sup).

-behaviour(supervisor2).

-export([init/1]).
-export([start_link/0, start_child/0]).

-include_lib("rabbit_common/include/rabbit.hrl").

start_child() ->
    supervisor2:start_child(?MODULE, sup()).

sup() ->
    {rabbit_mgmt_agent_sup, {rabbit_mgmt_agent_sup, start_link, []},
     temporary, ?SUPERVISOR_WAIT, supervisor, [rabbit_mgmt_agent_sup]}.

init([]) ->
    {ok, {{one_for_one, 0, 1}, [sup()]}}.

start_link() ->
    supervisor2:start_link({local, ?MODULE}, ?MODULE, []).
