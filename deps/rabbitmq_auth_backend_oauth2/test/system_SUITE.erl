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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2018 Pivotal Software, Inc.  All rights reserved.
%%

-module(system_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(rabbit_ct_client_helpers, [close_connection/1, close_channel/1,
                                   open_unmanaged_connection/4, open_unmanaged_connection/5,
                                   close_connection_and_channel/2]).
-import(rabbit_mgmt_test_util, [amqp_port/1]).

all() ->
    [
     {group, happy_path},
     {group, unhappy_path}
    ].

groups() ->
    [
     {happy_path, [], [
                       test_successful_connection_with_a_full_permission_token_and_all_defaults,
                       test_successful_connection_with_a_full_permission_token_and_explicitly_configured_vhost
                      ]},
     {unhappy_path, [], [
                       test_failed_connection_with_expired_token,
                       test_failed_connection_with_a_non_token,
                       test_failed_connection_with_a_token_with_insufficient_vhost_permission,
                       test_failed_connection_with_a_token_with_insufficient_resource_permission
                      ]}
    ].

%%
%% Setup and Teardown
%%

-define(UTIL_MOD, rabbit_auth_backend_oauth2_test_util).
-define(RESOURCE_SERVER_ID, <<"rabbitmq">>).

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config,
      rabbit_ct_broker_helpers:setup_steps() ++ [
        fun preconfigure_node/1,
        fun preconfigure_token/1
      ]).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config, rabbit_ct_broker_helpers:teardown_steps()).


init_per_group(_Group, Config) ->
    %% The broker is managed by {init,end}_per_testcase().
    lists:foreach(fun(Value) ->
                          rabbit_ct_broker_helpers:add_vhost(Config, Value)
                  end,
                  [<<"vhost1">>, <<"vhost2">>, <<"vhost3">>]),
    Config.

end_per_group(_Group, Config) ->
    %% The broker is managed by {init,end}_per_testcase().
    lists:foreach(fun(Value) ->
                          rabbit_ct_broker_helpers:delete_vhost(Config, Value)
                  end,
                  [<<"vhost1">>, <<"vhost2">>, <<"vhost3">>]),
    Config.


init_per_testcase(test_successful_connection_with_a_full_permission_token_and_explicitly_configured_vhost = Testcase, Config) ->
    rabbit_ct_broker_helpers:add_vhost(Config, <<"vhost1">>),
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    Config;

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    Config.

end_per_testcase(test_successful_connection_with_a_full_permission_token_and_explicitly_configured_vhost = Testcase, Config) ->
    rabbit_ct_broker_helpers:add_vhost(Config, <<"vhost1">>),
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    Config;

end_per_testcase(Testcase, Config) ->
    rabbit_ct_broker_helpers:delete_vhost(Config, <<"vhost1">>),
    rabbit_ct_helpers:testcase_finished(Config, Testcase),
    Config.

preconfigure_node(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                      [rabbit, auth_backends, [rabbit_auth_backend_oauth2]]),
    Jwk   = ?UTIL_MOD:fixture_jwk(),
    UaaEnv = [{signing_keys, #{<<"token-key">> => {map, Jwk}}}],
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                      [rabbitmq_auth_backend_oauth2, uaa_jwt, UaaEnv]),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                      [rabbitmq_auth_backend_oauth2, resource_server_id, ?RESOURCE_SERVER_ID]),

    rabbit_ct_helpers:set_config(Config, {fixture_jwk, Jwk}).

generate_valid_token(Config) ->
    generate_valid_token(Config, ?UTIL_MOD:full_permission_scopes()).

generate_valid_token(Config, Scopes) ->
    Jwk = case rabbit_ct_helpers:get_config(Config, fixture_jwk) of
              undefined -> ?UTIL_MOD:fixture_jwk();
              Value     -> Value
          end,
    ?UTIL_MOD:sign_token_hs(?UTIL_MOD:fixture_token_with_scopes(Scopes), Jwk).

generate_expired_token(Config) ->
    generate_expired_token(Config, ?UTIL_MOD:full_permission_scopes()).

generate_expired_token(Config, Scopes) ->
    Jwk = case rabbit_ct_helpers:get_config(Config, fixture_jwk) of
              undefined -> ?UTIL_MOD:fixture_jwk();
              Value     -> Value
          end,
    ?UTIL_MOD:sign_token_hs(?UTIL_MOD:expired_token_with_scopes(Scopes), Jwk).

preconfigure_token(Config) ->
    Token = generate_valid_token(Config),
    rabbit_ct_helpers:set_config(Config, {fixture_jwt, Token}).

%%
%% Test Cases
%%

test_successful_connection_with_a_full_permission_token_and_all_defaults(Config) ->
    {_Algo, Token} = rabbit_ct_helpers:get_config(Config, fixture_jwt),
    Conn     = open_unmanaged_connection(Config, 0, <<"username">>, Token),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    #'queue.declare_ok'{queue = _} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
    close_connection_and_channel(Conn, Ch).

test_successful_connection_with_a_full_permission_token_and_explicitly_configured_vhost(Config) ->
    {_Algo, Token} = generate_valid_token(Config, [<<"rabbitmq.configure:vhost1/*">>,
                                                   <<"rabbitmq.write:vhost1/*">>,
                                                   <<"rabbitmq.read:vhost1/*">>]),
    Conn     = open_unmanaged_connection(Config, 0, <<"vhost1">>, <<"username">>, Token),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    #'queue.declare_ok'{queue = _} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
    close_connection_and_channel(Conn, Ch).



test_failed_connection_with_expired_token(Config) ->
    {_Algo, Token} = generate_expired_token(Config, [<<"rabbitmq.configure:vhost1/*">>,
                                                     <<"rabbitmq.write:vhost1/*">>,
                                                     <<"rabbitmq.read:vhost1/*">>]),
    ?assertEqual({error, not_allowed},
                 open_unmanaged_connection(Config, 0, <<"vhost1">>, <<"username">>, Token)).

test_failed_connection_with_a_non_token(Config) ->
    ?assertMatch({error, {auth_failure, _}},
                 open_unmanaged_connection(Config, 0, <<"vhost1">>, <<"username">>, <<"a-non-token-value">>)).

test_failed_connection_with_a_token_with_insufficient_vhost_permission(Config) ->
    {_Algo, Token} = generate_valid_token(Config, [<<"rabbitmq.configure:alt-vhost/*">>,
                                                   <<"rabbitmq.write:alt-vhost/*">>,
                                                   <<"rabbitmq.read:alt-vhost/*">>]),
    ?assertEqual({error, not_allowed},
                 open_unmanaged_connection(Config, 0, <<"off-limits-vhost">>, <<"username">>, Token)).

test_failed_connection_with_a_token_with_insufficient_resource_permission(Config) ->
    {_Algo, Token} = generate_valid_token(Config, [<<"rabbitmq.configure:vhost2/jwt*">>,
                                                   <<"rabbitmq.write:vhost2/jwt*">>,
                                                   <<"rabbitmq.read:vhost2/jwt*">>]),
    Conn     = open_unmanaged_connection(Config, 0, <<"vhost2">>, <<"username">>, Token),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    ?assertExit({{shutdown, {server_initiated_close, 403, _}}, _},
       amqp_channel:call(Ch, #'queue.declare'{queue = <<"alt-prefix.eq.1">>, exclusive = true})),
    close_connection(Conn).
