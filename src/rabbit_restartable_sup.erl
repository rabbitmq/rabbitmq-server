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

-module(rabbit_restartable_sup).

-behaviour(supervisor).

-export([start_link/2]).

-export([init/1]).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/2 :: (atom(), rabbit_types:mfargs()) ->
                           rabbit_types:ok_pid_or_error()).

-endif.

%%----------------------------------------------------------------------------

start_link(Name, {_M, _F, _A} = Fun) ->
    supervisor:start_link({local, Name}, ?MODULE, [Fun]).

init([{Mod, _F, _A} = Fun]) ->
    {ok, {{one_for_one, 10, 10},
          [{Mod, Fun, transient, ?MAX_WAIT, worker, [Mod]}]}}.
