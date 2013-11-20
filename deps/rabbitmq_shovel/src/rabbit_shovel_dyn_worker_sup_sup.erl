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

-module(rabbit_shovel_dyn_worker_sup_sup).
-behaviour(mirrored_supervisor).

-export([start_link/0, init/1, adjust_or_start_child/2, stop_child/1]).

-import(rabbit_misc, [pget/2]).

-include_lib("rabbit_common/include/rabbit.hrl").
-define(SUPERVISOR, ?MODULE).

start_link() ->
    {ok, Pid} = mirrored_supervisor:start_link({local, ?SUPERVISOR},
                                               ?SUPERVISOR, ?MODULE, []),
    Shovels = rabbit_runtime_parameters:list_component(<<"shovel">>),
    [adjust_or_start_child(pget(name, Shovel),
                           pget(value, Shovel)) || Shovel <- Shovels],
    {ok, Pid}.

adjust_or_start_child(Name, Def) ->
    case child_exists(Name) of
        true  -> stop_child(Name);
        false -> ok
    end,
    {ok, _Pid} =
        mirrored_supervisor:start_child(
          ?SUPERVISOR,
          {Name, {rabbit_shovel_dyn_worker_sup, start_link, [Name, Def]},
           transient, ?MAX_WAIT, worker, [rabbit_shovel_dyn_worker_sup]}),
    ok.

child_exists(Name) ->
    lists:any(fun ({N, _, _, _}) -> N =:= Name end,
              mirrored_supervisor:which_children(?SUPERVISOR)).

stop_child(Name) ->
    ok = mirrored_supervisor:terminate_child(?SUPERVISOR, Name),
    ok = mirrored_supervisor:delete_child(?SUPERVISOR, Name),
    rabbit_shovel_status:remove(Name).

%%----------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_one, 3, 10}, []}}.
