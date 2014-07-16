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
%%  Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_shovel_dyn_worker_sup).
-behaviour(supervisor2).

-export([start_link/2, init/1]).

-import(rabbit_misc, [pget/3]).

-include_lib("rabbit_common/include/rabbit.hrl").
-define(SUPERVISOR, ?MODULE).

start_link(Name, Config) ->
    supervisor2:start_link(?MODULE, [Name, Config]).

%%----------------------------------------------------------------------------

init([Name, Config]) ->
    {ok, {{one_for_one, 1, ?MAX_WAIT},
          [{Name,
            {rabbit_shovel_worker, start_link, [dynamic, Name, Config]},
            case pget(<<"reconnect-delay">>, Config, 1) of
                N when is_integer(N) andalso N > 0 -> {transient, N};
                _                                  -> temporary
            end,
            16#ffffffff, worker, [rabbit_shovel_worker]}]}}.
