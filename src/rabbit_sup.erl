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
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_sup).

-behaviour(supervisor).

-export([start_link/0, start_child/1, start_child/2, start_child/3,
         start_supervisor_child/1, start_supervisor_child/2,
         start_supervisor_child/3,
         start_restartable_child/1, start_restartable_child/2, stop_child/1]).

-export([init/1]).

-include("rabbit.hrl").

-define(SERVER, ?MODULE).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/0 :: () -> rabbit_types:ok_pid_or_error()).
-spec(start_child/1 :: (atom()) -> 'ok').
-spec(start_child/2 :: (atom(), [any()]) -> 'ok').
-spec(start_child/3 :: (atom(), atom(), [any()]) -> 'ok').
-spec(start_supervisor_child/1 :: (atom()) -> 'ok').
-spec(start_supervisor_child/2 :: (atom(), [any()]) -> 'ok').
-spec(start_supervisor_child/3 :: (atom(), atom(), [any()]) -> 'ok').
-spec(start_restartable_child/1 :: (atom()) -> 'ok').
-spec(start_restartable_child/2 :: (atom(), [any()]) -> 'ok').
-spec(stop_child/1 :: (atom()) -> rabbit_types:ok_or_error(any())).

-endif.

%%----------------------------------------------------------------------------

start_link() -> supervisor:start_link({local, ?SERVER}, ?MODULE, []).

start_child(Mod) -> start_child(Mod, []).

start_child(Mod, Args) -> start_child(Mod, Mod, Args).

start_child(ChildId, Mod, Args) ->
    child_reply(supervisor:start_child(
                  ?SERVER,
                  {ChildId, {Mod, start_link, Args},
                   transient, ?MAX_WAIT, worker, [Mod]})).

start_supervisor_child(Mod) -> start_supervisor_child(Mod, []).

start_supervisor_child(Mod, Args) -> start_supervisor_child(Mod, Mod, Args).

start_supervisor_child(ChildId, Mod, Args) ->
    child_reply(supervisor:start_child(
                  ?SERVER,
                  {ChildId, {Mod, start_link, Args},
                   transient, infinity, supervisor, [Mod]})).

start_restartable_child(Mod) -> start_restartable_child(Mod, []).

start_restartable_child(Mod, Args) ->
    Name = list_to_atom(atom_to_list(Mod) ++ "_sup"),
    child_reply(supervisor:start_child(
                  ?SERVER,
                  {Name, {rabbit_restartable_sup, start_link,
                          [Name, {Mod, start_link, Args}]},
                   transient, infinity, supervisor, [rabbit_restartable_sup]})).

stop_child(ChildId) ->
    case supervisor:terminate_child(?SERVER, ChildId) of
        ok -> supervisor:delete_child(?SERVER, ChildId);
        E  -> E
    end.

init([]) -> {ok, {{one_for_all, 0, 1}, []}}.


%%----------------------------------------------------------------------------

child_reply({ok, _}) -> ok;
child_reply(X)       -> X.
