%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2010-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_mochiweb_sup).

-behaviour(supervisor).

-define(SUP, ?MODULE).

%% External exports
-export([start_link/1, upgrade/1, ensure_listener/1, stop_listener/1]).

%% supervisor callbacks
-export([init/1]).

%% @spec start_link() -> ServerRet
%% @doc API for starting the supervisor.
start_link(ListenerSpecs) ->
    supervisor:start_link({local, ?SUP}, ?MODULE, [ListenerSpecs]).

%% @spec upgrade([instance()]) -> ok
%% @doc Add processes if necessary.
upgrade(ListenerSpecs) ->
    {ok, {_, Specs}} = init([ListenerSpecs]),

    Old = sets:from_list(
            [Name || {Name, _, _, _} <- supervisor:which_children(?MODULE)]),
    New = sets:from_list([Name || {Name, _, _, _, _, _} <- Specs]),
    Kill = sets:subtract(Old, New),

    sets:fold(fun (Id, ok) ->
                      supervisor:terminate_child(?SUP, Id),
                      supervisor:delete_child(?SUP, Id),
                      ok
              end, ok, Kill),

    [supervisor:start_child(?SUP, Spec) || Spec <- Specs],
    ok.

ensure_listener({Listener, Spec}) ->
    Child = {{rabbit_mochiweb_web, Listener},
             {rabbit_mochiweb_web, start, [{Listener, Spec}]},
             transient, 5000, worker, dynamic},
    case supervisor:start_child(?SUP, Child) of
        {ok,                      Pid}  -> {ok, Pid};
        {error, {already_started, Pid}} -> {ok, Pid}
    end.

stop_listener({Listener, _Spec}) ->
    ok = supervisor:terminate_child(?SUP, {rabbit_mochiweb_web, Listener}),
    ok = supervisor:delete_child(?SUP, {rabbit_mochiweb_web, Listener}).

%% @spec init([[instance()]]) -> SupervisorTree
%% @doc supervisor callback.
init([ListenerSpecs]) ->
    Registry = {rabbit_mochiweb_registry,
                {rabbit_mochiweb_registry, start_link, [ListenerSpecs]},
                transient, 5000, worker, dynamic},
    {ok, {{one_for_one, 10, 10}, [Registry]}}.
