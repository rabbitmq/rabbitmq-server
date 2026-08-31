%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

%% A supervisor for Ra systems which tie their lifetime to the `rabbit'
%% application.
-module(rabbit_ra_systems_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => one_for_one, intensity => 1, period => 5},
    Children = [ra_system_sup:child_spec(rabbit_ra_systems:get_config(RaSystem))
                || RaSystem <- rabbit_ra_systems:all_app_ra_systems()],
    {ok, {SupFlags, Children}}.
