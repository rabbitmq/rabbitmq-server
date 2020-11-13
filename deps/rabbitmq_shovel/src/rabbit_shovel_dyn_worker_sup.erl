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

-module(rabbit_shovel_dyn_worker_sup).
-behaviour(supervisor2).

-export([start_link/2, init/1]).

-import(rabbit_misc, [pget/3]).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("rabbit_shovel.hrl").
-define(SUPERVISOR, ?MODULE).

start_link(Name, Config) ->
    supervisor2:start_link(?MODULE, [Name, Config]).

%%----------------------------------------------------------------------------

init([Name, Config0]) ->
    Config  = rabbit_data_coercion:to_proplist(Config0),
    Delay   = pget(<<"reconnect-delay">>, Config, ?DEFAULT_RECONNECT_DELAY),
    case Name of
      {VHost, ShovelName} -> rabbit_log:debug("Shovel '~s' in virtual host '~s' will use reconnection delay of ~p", [ShovelName, VHost, Delay]);
      ShovelName          -> rabbit_log:debug("Shovel '~s' will use reconnection delay of ~s", [ShovelName, Delay])
    end,
    Restart = case Delay of
        N when is_integer(N) andalso N > 0 ->
          case pget(<<"src-delete-after">>, Config, pget(<<"delete-after">>, Config, <<"never">>)) of
            %% always try to reconnect
            <<"never">>                        -> {permanent, N};
            %% this Shovel is an autodelete one
              M when is_integer(M) andalso M > 0 -> {transient, N};
              <<"queue-length">> -> {transient, N}
          end;
        %% reconnect-delay = 0 means "do not reconnect"
        _                                  -> temporary
    end,

    {ok, {{one_for_one, 1, ?MAX_WAIT},
          [{Name,
            {rabbit_shovel_worker, start_link, [dynamic, Name, Config]},
            Restart,
            16#ffffffff, worker, [rabbit_shovel_worker]}]}}.
