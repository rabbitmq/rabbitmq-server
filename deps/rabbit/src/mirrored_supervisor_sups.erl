%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(mirrored_supervisor_sups).

-define(SUPERVISOR, supervisor2).
-define(GS_MODULE,  mirrored_supervisor).

-behaviour(?SUPERVISOR).

-export([init/1]).

%%----------------------------------------------------------------------------

init({overall, _Group, ignore}) -> ignore;
init({overall,  Group, {ok, {Restart, ChildSpecs}}}) ->
    %% Important: Delegate MUST start before Mirroring so that when we
    %% shut down from above it shuts down last, so Mirroring does not
    %% see it die.
    %%
    %% See comment in handle_info('DOWN', ...) in mirrored_supervisor
    {ok, {{one_for_all, 0, 1},
          [{delegate, {?SUPERVISOR, start_link, [?MODULE, {delegate, Restart}]},
            temporary, 16#ffffffff, supervisor, [?SUPERVISOR]},
           {mirroring, {?GS_MODULE, start_internal, [Group, ChildSpecs]},
            permanent, 16#ffffffff, worker, [?MODULE]}]}};


init({delegate, Restart}) ->
    {ok, {Restart, []}}.
