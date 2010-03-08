%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2010 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2010 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_sup).

-behaviour(supervisor).

-export([start_link/0, start_child/1, start_child/2,
         start_restartable_child/1, start_restartable_child/2]).

-export([init/1]).

-include("rabbit.hrl").

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

start_child(Mod) ->
    start_child(Mod, []).

start_child(Mod, Args) ->
    {ok, _} = supervisor:start_child(?SERVER,
                                     {Mod, {Mod, start_link, Args},
                                      transient, ?MAX_WAIT, worker, [Mod]}),
    ok.

start_restartable_child(Mod) ->
    start_restartable_child(Mod, []).

start_restartable_child(Mod, Args) ->
    Name = list_to_atom(atom_to_list(Mod) ++ "_sup"),
    {ok, _} = supervisor:start_child(
                ?SERVER,
                {Name, {rabbit_restartable_sup, start_link,
                        [Name, {Mod, start_link, Args}]},
                 transient, infinity, supervisor, [rabbit_restartable_sup]}),
    ok.

init([]) ->
    {ok, {{one_for_all, 0, 1}, []}}.
