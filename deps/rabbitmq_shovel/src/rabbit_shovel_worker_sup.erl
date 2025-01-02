%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_shovel_worker_sup).
-behaviour(mirrored_supervisor).

-export([start_link/2, init/1]).
-export([id_to_khepri_path/1]).

-include_lib("rabbit_common/include/rabbit.hrl").

start_link(ShovelName, ShovelConfig) ->
    mirrored_supervisor:start_link({local, ShovelName}, ShovelName,
                                   ?MODULE, [ShovelName, ShovelConfig]).

init([Name, Config]) ->
    ChildSpecs = [{id(Name),
                   {rabbit_shovel_worker, start_link, [static, Name, Config]},
                   case Config of
                       #{reconnect_delay := N}
                         when is_integer(N) andalso N > 0 -> {permanent, N};
                       _ -> temporary
                   end,
                   16#ffffffff,
                   worker,
                   [rabbit_shovel_worker]}],
    {ok, {{one_for_one, 1, ?MAX_WAIT}, ChildSpecs}}.

id(Name) when is_atom(Name) ->
    Name.

id_to_khepri_path(Name) when is_atom(Name) ->
    [Name].
