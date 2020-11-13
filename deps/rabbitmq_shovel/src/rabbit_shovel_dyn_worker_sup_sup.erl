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

-module(rabbit_shovel_dyn_worker_sup_sup).
-behaviour(mirrored_supervisor).

-export([start_link/0, init/1, adjust/2, stop_child/1]).

-import(rabbit_misc, [pget/2]).

-include("rabbit_shovel.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-define(SUPERVISOR, ?MODULE).

start_link() ->
    Pid = case mirrored_supervisor:start_link(
                  {local, ?SUPERVISOR}, ?SUPERVISOR,
                  fun rabbit_misc:execute_mnesia_transaction/1, ?MODULE, []) of
            {ok, Pid0}                       -> Pid0;
            {error, {already_started, Pid0}} -> Pid0
          end,
    Shovels = rabbit_runtime_parameters:list_component(<<"shovel">>),
    [start_child({pget(vhost, Shovel), pget(name, Shovel)},
                 pget(value, Shovel)) || Shovel <- Shovels],
    {ok, Pid}.

adjust(Name, Def) ->
    case child_exists(Name) of
        true  -> stop_child(Name);
        false -> ok
    end,
    start_child(Name, Def).

start_child(Name, Def) ->
    case mirrored_supervisor:start_child(
           ?SUPERVISOR,
           {Name, {rabbit_shovel_dyn_worker_sup, start_link, [Name, Def]},
            transient, ?WORKER_WAIT, worker, [rabbit_shovel_dyn_worker_sup]}) of
        {ok,                      _Pid}  -> ok;
        {error, {already_started, _Pid}} -> ok
    end.

child_exists(Name) ->
    lists:any(fun ({N, _, _, _}) -> N =:= Name end,
              mirrored_supervisor:which_children(?SUPERVISOR)).

stop_child(Name) ->
    case get(shovel_worker_autodelete) of
        true -> ok; %% [1]
        _    ->
            ok = mirrored_supervisor:terminate_child(?SUPERVISOR, Name),
            ok = mirrored_supervisor:delete_child(?SUPERVISOR, Name),
            rabbit_shovel_status:remove(Name)
    end.

%% [1] An autodeleting worker removes its own parameter, and thus ends
%% up here via the parameter callback. It is a transient worker that
%% is just about to terminate normally - so we don't need to tell the
%% supervisor to stop us - and as usual if we call into our own
%% supervisor we risk deadlock.
%%
%% See rabbit_shovel_worker:maybe_autodelete/1

%%----------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_one, 3, 10}, []}}.
