%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_auth_backend_ldap_app).

-behaviour(application).
-export([start/2, stop/1]).

%% Dummy supervisor - see Ulf Wiger's comment at
%% http://erlang.org/pipermail/erlang-questions/2010-April/050508.html
-behaviour(supervisor).
-export([create_ldap_pool/0, init/1]).

-rabbit_boot_step({ldap_pool,
                   [{description, "LDAP pool"},
                    {mfa, {?MODULE, create_ldap_pool, []}},
                    {requires, kernel_ready}]}).

create_ldap_pool() ->
    {ok, PoolSize} = application:get_env(rabbitmq_auth_backend_ldap, pool_size),
    rabbit_sup:start_supervisor_child(ldap_pool_sup, worker_pool_sup, [PoolSize, ldap_pool]).

start(_Type, _StartArgs) ->
    {ok, Backends} = application:get_env(rabbit, auth_backends),
    case configured(rabbit_auth_backend_ldap, Backends) of
        true  -> ok;
        false -> rabbit_log_ldap:warning(
                   "LDAP plugin loaded, but rabbit_auth_backend_ldap is not "
                   "in the list of auth_backends. LDAP auth will not work.")
    end,
    {ok, SSL} = application:get_env(rabbitmq_auth_backend_ldap, use_ssl),
    {ok, TLS} = application:get_env(rabbitmq_auth_backend_ldap, use_starttls),
    case SSL orelse TLS of
        true  ->
            rabbit_networking:ensure_ssl(),
            ok;
        false -> ok
    end,
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

stop(_State) ->
    ok.

configured(_M, [])        -> false;
configured(M,  [M    |_]) -> true;
configured(M,  [{M,_}|_]) -> true;
configured(M,  [{_,M}|_]) -> true;
configured(M,  [_    |T]) -> configured(M, T).

%%----------------------------------------------------------------------------

init([]) -> {ok, {{one_for_one, 3, 10}, []}}.
