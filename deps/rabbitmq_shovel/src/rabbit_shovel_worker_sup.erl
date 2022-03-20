%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_shovel_worker_sup).
-behaviour(mirrored_supervisor).

-export([start_link/2, init/1]).

-include("rabbit_shovel.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

start_link(ShovelName, ShovelConfig) ->
    mirrored_supervisor:start_link({local, ShovelName}, ShovelName,
                                   fun rabbit_misc:execute_mnesia_transaction/1,
                                   ?MODULE, [ShovelName, ShovelConfig]).

init([Name, Config]) ->
    ChildSpecs = [{Name,
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
