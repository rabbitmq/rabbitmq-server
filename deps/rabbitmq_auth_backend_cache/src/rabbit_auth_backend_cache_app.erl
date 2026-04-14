%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_auth_backend_cache_app).

-behaviour(application).
-export([start/2, stop/1]).

-behaviour(supervisor).
-export([init/1]).

-include_lib("kernel/include/logger.hrl").

start(_Type, _StartArgs) ->
    supervisor:start_link({local,?MODULE},?MODULE,[]).

stop(_State) ->
    ok.

%%----------------------------------------------------------------------------

init([]) ->
    {ok, AuthCache} = application:get_env(rabbitmq_auth_backend_cache,
                                          cache_module),

    {ok, AuthCacheArgs} = application:get_env(rabbitmq_auth_backend_cache, cache_module_args),
    % Load module to be able to check exported function.
    _ = code:load_file(AuthCache),
    ChildSpecs = case erlang:function_exported(AuthCache, start_link,
                                               length(AuthCacheArgs)) of
        true  -> [{auth_cache, {AuthCache, start_link, AuthCacheArgs},
                  permanent, 5000, worker, [AuthCache]}];
        false -> []
    end,
    validate_cached_backend(),
    {ok, {{one_for_one,3,10}, ChildSpecs}}.

validate_cached_backend() ->
    {ok, BackendConfig} = application:get_env(rabbitmq_auth_backend_cache,
                                              cached_backend),
    Backends = case BackendConfig of
                   Mod when is_atom(Mod) -> [Mod];
                   {N, Z}               -> [N, Z]
               end,
    case lists:member(rabbit_auth_backend_cache, Backends) of
        true ->
            ?LOG_ERROR(
                "Auth backend cache: cached_backend must not be set "
                "to rabbit_auth_backend_cache (the cache cannot wrap itself)");
        false ->
            ok
    end.
