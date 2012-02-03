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

-module(delegate_sup).

-behaviour(supervisor).

-export([start_link/1, count/1]).

-export([init/1]).

-define(SERVER, ?MODULE).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/1 :: (integer()) -> rabbit_types:ok_pid_or_error()).
-spec(count/1 :: ([node()]) -> integer()).

-endif.

%%----------------------------------------------------------------------------

start_link(Count) ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, [Count]).

count([]) ->
    1;
count([Node | Nodes]) ->
    try
        length(supervisor:which_children({?SERVER, Node}))
    catch exit:{{R, _}, _} when R =:= nodedown; R =:= shutdown ->
            count(Nodes);
          exit:{R, _}      when R =:= noproc; R =:= normal; R =:= shutdown;
                                R =:= nodedown ->
            count(Nodes)
    end.

%%----------------------------------------------------------------------------

init([Count]) ->
    {ok, {{one_for_one, 10, 10},
          [{Num, {delegate, start_link, [Num]},
            transient, 16#ffffffff, worker, [delegate]} ||
              Num <- lists:seq(0, Count - 1)]}}.
