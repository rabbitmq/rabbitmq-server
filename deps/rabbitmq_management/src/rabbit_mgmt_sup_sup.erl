%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% Copyright (c) 2007-2018 Pivotal Software, Inc. All rights reserved.
%%

-module(rabbit_mgmt_sup_sup).

-behaviour(supervisor2).

-export([init/1]).
-export([start_link/0, start_child/0]).

-include_lib("rabbit_common/include/rabbit.hrl").

start_child() ->
    supervisor2:start_child(?MODULE, sup()).

sup() ->
    {rabbit_mgmt_sup, {rabbit_mgmt_sup, start_link, []},
     temporary, ?SUPERVISOR_WAIT, supervisor, [rabbit_mgmt_sup]}.

init([]) ->
    {ok, {{one_for_one, 0, 1}, [sup()]}}.

start_link() ->
    supervisor2:start_link({local, ?MODULE}, ?MODULE, []).
