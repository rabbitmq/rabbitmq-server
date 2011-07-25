%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_limiter_sup).

-behaviour(supervisor2).

-export([start_link/0, start_limiter/3]).

-export([init/1]).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/0 :: () -> rabbit_types:ok(pid())).
-spec(start_limiter/3 :: (pid(), pid(), integer()) -> rabbit_types:ok(pid())).

-endif.

%%----------------------------------------------------------------------------

start_link() ->
    supervisor2:start_link(?MODULE, []).

start_limiter(Pid, Channel, UnackedCount) ->
    supervisor2:start_child(Pid, [Channel, UnackedCount]).

%%----------------------------------------------------------------------------

init([]) ->
    {ok, {{simple_one_for_one, 0, 1},
          [{limiter, {rabbit_limiter, start_link, []},
            transient, ?MAX_WAIT, worker, [rabbit_limiter]}]}}.
