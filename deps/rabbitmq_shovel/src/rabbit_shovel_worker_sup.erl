%%  The contents of this file are subject to the Mozilla Public License
%%  Version 1.1 (the "License"); you may not use this file except in
%%  compliance with the License. You may obtain a copy of the License
%%  at http://www.mozilla.org/MPL/
%%
%%  Software distributed under the License is distributed on an "AS IS"
%%  basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%%  the License for the specific language governing rights and
%%  limitations under the License.
%%
%%  The Original Code is RabbitMQ.
%%
%%  The Initial Developer of the Original Code is GoPivotal, Inc.
%%  Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_shovel_worker_sup).
-behaviour(mirrored_supervisor).

-export([start_link/2, init/1]).

-include("rabbit_shovel.hrl").

start_link(ShovelName, ShovelConfig) ->
    mirrored_supervisor:start_link({local, ShovelName}, ShovelName,
                                   ?MODULE, [ShovelName, ShovelConfig]).

init([ShovelName, Config]) ->
    ChildSpecs = [{ShovelName,
                   {rabbit_shovel_worker, start_link, [ShovelName, Config]},
                   case proplists:get_value(reconnect_delay, Config, none) of
                       N when is_integer(N) andalso N > 0 -> {permanent, N};
                       _                                  -> temporary
                   end,
                   16#ffffffff,
                   worker,
                   [rabbit_shovel_worker]}],
    {ok, {{one_for_one, 3, 10}, ChildSpecs}}.
