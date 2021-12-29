%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_auth_backend_cache_app).

-behaviour(application).
-export([start/2, stop/1]).

-behaviour(supervisor).
-export([init/1]).

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
    code:load_file(AuthCache),
    ChildSpecs = case erlang:function_exported(AuthCache, start_link, 
                                               length(AuthCacheArgs)) of
        true  -> [{auth_cache, {AuthCache, start_link, AuthCacheArgs},
                  permanent, 5000, worker, [AuthCache]}];
        false -> []
    end,
    {ok, {{one_for_one,3,10}, ChildSpecs}}.
