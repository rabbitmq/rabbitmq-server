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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(delegate_sup).

-behaviour(supervisor).

-export([start_link/1, start_link/2, count/1, count/2, sup_name/1]).

-export([init/1]).

-define(SERVER, "delegate_").

%%----------------------------------------------------------------------------

-spec start_link(integer()) -> rabbit_types:ok_pid_or_error().
-spec start_link(integer(), string()) -> rabbit_types:ok_pid_or_error().
-spec count([node()]) -> integer().

%%----------------------------------------------------------------------------

sup_name(Prefix) ->
    list_to_atom(Prefix ++ "sup").

start_link(Count, Prefix) ->
    supervisor:start_link({local, sup_name(Prefix)}, ?MODULE, [Count, Prefix]).
start_link(Count) ->
    start_link(Count, ?SERVER).

count(Nodes) ->
    count(Nodes, ?SERVER).

count([], _) ->
    1;
count([Node | Nodes], Prefix) ->
    try
        length(supervisor:which_children({sup_name(Prefix), Node}))
    catch exit:{{R, _}, _} when R =:= nodedown; R =:= shutdown ->
            count(Nodes, Prefix);
          exit:{R, _}      when R =:= noproc; R =:= normal; R =:= shutdown;
                                R =:= nodedown ->
            count(Nodes, Prefix)
    end.

%%----------------------------------------------------------------------------

init([Count, Name]) ->
    {ok, {{one_for_one, 10, 10},
          [{Num, {delegate, start_link, [Name, Num]},
            transient, 16#ffffffff, worker, [delegate]} ||
              Num <- lists:seq(0, Count - 1)]}}.
