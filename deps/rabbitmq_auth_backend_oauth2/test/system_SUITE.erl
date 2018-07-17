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

-import(rabbit_ct_client_helpers, [close_connection/1, close_channel/1,
                                   open_unmanaged_connection/4, open_unmanaged_connection/5,
                                   close_connection_and_channel/2]).
-import(rabbit_mgmt_test_util, [amqp_port/1]).

all() ->
    [
     {group, happy_path}
    ].

groups() ->
    [
     {happy_path, [], [
                       test_successful_connection_with_a_full_permission_token_and_all_defaults,
                       test_successful_connection_with_a_full_permission_token_and_explicitly_configured_vhost
                      ]}
    ].

%%
%% Setup and Teardown
%%

-define(UTIL_MOD, rabbit_auth_backend_uaa_test_util).
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

preconfigure_node(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                      [rabbit, auth_backends, [rabbit_auth_backend_uaa]]),
    Jwk   = ?UTIL_MOD:fixture_jwk(),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                      [uaa_jwt, signing_keys, #{<<"token-key">> => {map, Jwk}}]),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                      [rabbitmq_auth_backend_uaa, resource_server_id, ?RESOURCE_SERVER_ID]),

    rabbit_ct_helpers:set_config(Config, {fixture_jwk, Jwk}).

generate_valid_token(Config) ->
    generate_valid_token(Config, ?UTIL_MOD:full_permission_scopes()).

generate_valid_token(Config, Scopes) ->
    Jwk = case rabbit_ct_helpers:get_config(Config, fixture_jwk) of
              undefined -> ?UTIL_MOD:fixture_jwk();
              Value     -> Value
          end,
    ?UTIL_MOD:sign_token_hs(?UTIL_MOD:fixture_token_with_scopes(Scopes), Jwk).

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
    {_Algo, Token} = rabbit_ct_helpers:get_config(Config, fixture_jwt),
    Conn     = open_unmanaged_connection(Config, 0, <<"username">>, Token),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    #'queue.declare_ok'{queue = _} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
    close_connection_and_channel(Conn, Ch).
