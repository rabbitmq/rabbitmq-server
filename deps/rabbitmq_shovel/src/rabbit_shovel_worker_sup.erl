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
%%  Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
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
