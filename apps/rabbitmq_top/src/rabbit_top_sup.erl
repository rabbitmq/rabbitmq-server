%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_top_sup).

-behaviour(supervisor).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([start_link/0]).
-export([init/1]).

start_link() ->
     supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Top = {rabbit_top_worker,
           {rabbit_top_worker, start_link, []},
           permanent, ?WORKER_WAIT, worker, [rabbit_top_worker]},
    {ok, {{one_for_one, 10, 10}, [Top]}}.

