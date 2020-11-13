%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
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

-module(worker_pool_sup).

-behaviour(supervisor).

-export([start_link/0, start_link/1, start_link/2]).

-export([init/1]).

-export([default_pool_size/0]).

%%----------------------------------------------------------------------------

-spec start_link() -> rabbit_types:ok_pid_or_error().
-spec start_link(non_neg_integer()) -> rabbit_types:ok_pid_or_error().
-spec start_link(non_neg_integer(), atom())
                   -> rabbit_types:ok_pid_or_error().

%%----------------------------------------------------------------------------

start_link() ->
    Size = default_pool_size(),
    start_link(Size).

start_link(PoolSize) ->
    rabbit_log:info("Will use ~p processes for default worker pool", [PoolSize]),
    start_link(PoolSize, worker_pool:default_pool()).

start_link(PoolSize, PoolName) ->
    rabbit_log:info("Starting worker pool '~p' with ~p processes in it", [PoolName, PoolSize]),
    SupName = list_to_atom(atom_to_list(PoolName) ++ "_sup"),
    supervisor:start_link({local, SupName}, ?MODULE, [PoolSize, PoolName]).

%%----------------------------------------------------------------------------

init([PoolSize, PoolName]) ->
    %% we want to survive up to 1K of worker restarts per second,
    %% e.g. when a large worker pool used for network connections
    %% encounters a network failure. This is the case in the LDAP authentication
    %% backend plugin.
    {ok, {{one_for_one, 1000, 1},
          [{worker_pool, {worker_pool, start_link, [PoolName]}, transient,
            16#ffffffff, worker, [worker_pool]} |
           [{N, {worker_pool_worker, start_link, [PoolName]}, transient,
             16#ffffffff, worker, [worker_pool_worker]}
            || N <- lists:seq(1, PoolSize)]]}}.

%%
%% Implementation
%%

-spec default_pool_size() -> integer().

default_pool_size() ->
  case rabbit_misc:get_env(rabbit, default_worker_pool_size, undefined) of
    N when is_integer(N) -> N;
    _                    -> guess_default_pool_size()
  end.

-spec guess_default_pool_size() -> integer().

guess_default_pool_size() ->
  erlang:system_info(schedulers).
