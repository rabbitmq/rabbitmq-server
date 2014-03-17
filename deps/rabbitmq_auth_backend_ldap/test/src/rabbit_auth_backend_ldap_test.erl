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
%% The Original Code is RabbitMQ
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_auth_backend_ldap_test).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-define(SIMON, #amqp_params_network{username     = <<"Simon MacMullen">>,
                                    password     = <<"password">>,
                                    virtual_host = <<"test">>}).

-define(MIKEB, #amqp_params_network{username     = <<"Mike Bridgen">>,
                                    password     = <<"password">>,
                                    virtual_host = <<"test">>}).

%%--------------------------------------------------------------------

login_test_() ->
    [test_login(Env, L, case {LGood, EnvGood} of
                            {good, good} -> fun succ/1;
                            _            -> fun fail/1
                        end) || {LGood, L}     <- logins(),
                                {EnvGood, Env} <- login_envs()].

logins() ->
    [{bad, #amqp_params_network{}},
     {bad, #amqp_params_network{username = <<"Simon MacMullen">>}},
     {bad, #amqp_params_network{username = <<"Simon MacMullen">>,
                                password = <<"password">>}},
     {good, ?SIMON},
     {good, ?MIKEB}].

login_envs() ->
    [{good, base_login_env()},
     {good, dn_lookup_pre_bind_env()},
     {good, other_bind_admin_env()},
     {good, other_bind_anon_env()},
     {bad, other_bind_broken_env()}].

base_login_env() ->
    [{user_dn_pattern,    "cn=${username},ou=People,dc=example,dc=com"},
     {dn_lookup_attribute, none},
     {dn_lookup_base,      none},
     {dn_lookup_bind,      as_user},
     {other_bind,          as_user}].

%% TODO configure OpenLDAP to allow a dn_lookup_post_bind_env()
dn_lookup_pre_bind_env() ->
    [{user_dn_pattern,    "${username}"},
     {dn_lookup_attribute, "cn"},
     {dn_lookup_base,      "OU=People,DC=example,DC=com"},
     {dn_lookup_bind,      {"cn=admin,dc=example,dc=com", "admin"}}].

other_bind_admin_env() ->
    [{other_bind, {"cn=admin,dc=example,dc=com", "admin"}}].

other_bind_anon_env() ->
    [{other_bind, anon}].

other_bind_broken_env() ->
    [{other_bind, {"cn=admin,dc=example,dc=com", "admi"}}].

test_login(Env, Login, ResultFun) ->
    ?_test(try
               set_env(Env),
               ResultFun(Login)
           after
               set_env(base_login_env())
           end).

set_env(Env) ->
    [application:set_env(rabbitmq_auth_backend_ldap, K, V) || {K, V} <- Env].

succ(Login) -> ?assertMatch({ok, _}, amqp_connection:start(Login)).
fail(Login) -> ?assertMatch({error, _}, amqp_connection:start(Login)).

%%--------------------------------------------------------------------

in_group_test_() ->
    X = [#'exchange.declare'{exchange = <<"test">>}],
    test_resource_funs([{?SIMON, X, ok},
                         {?MIKEB, X, fail}]).

const_test_() ->
    Q = [#'queue.declare'{queue = <<"test">>}],
    test_resource_funs([{?SIMON, Q, ok},
                        {?MIKEB, Q, fail}]).

string_match_test_() ->
    B = fun(N) ->
                [#'exchange.declare'{exchange = N},
                 #'queue.declare'{queue = <<"test">>},
                 #'queue.bind'{exchange = N, queue = <<"test">>}]
        end,
    test_resource_funs([{?SIMON, B(<<"xch-Simon MacMullen-abc123">>), ok},
                        {?SIMON, B(<<"abc123">>),                     fail},
                        {?SIMON, B(<<"xch-Someone Else-abc123">>),    fail}]).

boolean_logic_test_() ->
    Q1 = [#'queue.declare'{queue = <<"test1">>},
          #'basic.consume'{queue = <<"test1">>}],
    Q2 = [#'queue.declare'{queue = <<"test2">>},
          #'basic.consume'{queue = <<"test2">>}],
    [test_resource_fun(PTR) || PTR <- [{?SIMON, Q1, ok},
                                       {?SIMON, Q2, ok},
                                       {?MIKEB, Q1, fail},
                                       {?MIKEB, Q2, fail}]].

test_resource_funs(PTRs) -> [test_resource_fun(PTR) || PTR <- PTRs].

test_resource_fun({Person, Things, Result}) ->
    fun() ->
            {ok, Conn} = amqp_connection:start(Person),
            {ok, Ch} = amqp_connection:open_channel(Conn),
            ?assertEqual(Result,
                         try
                             [amqp_channel:call(Ch, T) || T <- Things],
                             amqp_connection:close(Conn),
                             ok
                         catch exit:_ -> fail
                         end)
    end.

%%--------------------------------------------------------------------
