%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_web_stomp_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

-define(SUP_NAME, ?MODULE).

%%----------------------------------------------------------------------------

-spec start_link() -> ignore | {'ok', pid()} | {'error', any()}.
start_link() ->
    supervisor:start_link({local, ?SUP_NAME}, ?MODULE, []).

init([]) ->
    {ok, {{one_for_one, 1, 5}, []}}.
