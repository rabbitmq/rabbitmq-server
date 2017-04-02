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
%% The Original Code is RabbitMQ HTTP authentication.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
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
