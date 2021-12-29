%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
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
                       test_successful_connection_with_a_full_permission_token_and_explicitly_configured_vhost,
                       test_successful_connection_with_simple_strings_for_aud_and_scope,
                       test_successful_connection_with_complex_claim_as_a_map,
                       test_successful_connection_with_complex_claim_as_a_list,
                       test_successful_connection_with_complex_claim_as_a_binary,
                       test_successful_connection_with_keycloak_token,
                       test_successful_token_refresh
                      ]},
     {unhappy_path, [], [
                       test_failed_connection_with_expired_token,
                       test_failed_connection_with_a_non_token,
                       test_failed_connection_with_a_token_with_insufficient_vhost_permission,
                       test_failed_connection_with_a_token_with_insufficient_resource_permission,
                       test_failed_token_refresh_case1,
                       test_failed_token_refresh_case2
                      ]}
    ].

%%
%% Setup and Teardown
%%

-define(UTIL_MOD, rabbit_auth_backend_oauth2_test_util).
-define(RESOURCE_SERVER_ID, <<"rabbitmq">>).
-define(EXTRA_SCOPES_SOURCE, <<"additional_rabbitmq_scopes">>).

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
                  [<<"vhost1">>, <<"vhost2">>, <<"vhost3">>, <<"vhost4">>]),
    Config.

end_per_group(_Group, Config) ->
    %% The broker is managed by {init,end}_per_testcase().
    lists:foreach(fun(Value) ->
                          rabbit_ct_broker_helpers:delete_vhost(Config, Value)
                  end,
                  [<<"vhost1">>, <<"vhost2">>, <<"vhost3">>, <<"vhost4">>]),
    Config.


init_per_testcase(Testcase, Config) when Testcase =:= test_successful_connection_with_a_full_permission_token_and_explicitly_configured_vhost orelse
                                         Testcase =:= test_successful_token_refresh ->
    rabbit_ct_broker_helpers:add_vhost(Config, <<"vhost1">>),
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    Config;

init_per_testcase(Testcase, Config) when Testcase =:= test_failed_token_refresh_case1 orelse
                                         Testcase =:= test_failed_token_refresh_case2 ->
    rabbit_ct_broker_helpers:add_vhost(Config, <<"vhost4">>),
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    Config;

init_per_testcase(Testcase, Config) when Testcase =:= test_successful_connection_with_complex_claim_as_a_map orelse
                                         Testcase =:= test_successful_connection_with_complex_claim_as_a_list orelse
                                         Testcase =:= test_successful_connection_with_complex_claim_as_a_binary ->
  ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
        [rabbitmq_auth_backend_oauth2, extra_scopes_source, ?EXTRA_SCOPES_SOURCE]),
  rabbit_ct_helpers:testcase_started(Config, Testcase),
  Config;

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    Config.

end_per_testcase(Testcase, Config) when Testcase =:= test_failed_token_refresh_case1 orelse
                                        Testcase =:= test_failed_token_refresh_case2 ->
    rabbit_ct_broker_helpers:delete_vhost(Config, <<"vhost4">>),
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    Config;

end_per_testcase(Testcase, Config) when Testcase =:= test_successful_connection_with_complex_claim_as_a_map orelse
                                        Testcase =:= test_successful_connection_with_complex_claim_as_a_list orelse
                                        Testcase =:= test_successful_connection_with_complex_claim_as_a_binary ->
  rabbit_ct_broker_helpers:delete_vhost(Config, <<"vhost1">>),
  ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
    [rabbitmq_auth_backend_oauth2, extra_scopes_source, undefined]),
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
    KeyConfig = [{signing_keys, #{<<"token-key">> => {map, Jwk}}}],
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                      [rabbitmq_auth_backend_oauth2, key_config, KeyConfig]),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                      [rabbitmq_auth_backend_oauth2, resource_server_id, ?RESOURCE_SERVER_ID]),

    rabbit_ct_helpers:set_config(Config, {fixture_jwk, Jwk}).

generate_valid_token(Config) ->
    generate_valid_token(Config, ?UTIL_MOD:full_permission_scopes()).

generate_valid_token(Config, Scopes) ->
    generate_valid_token(Config, Scopes, undefined).

generate_valid_token(Config, Scopes, Audience) ->
    Jwk = case rabbit_ct_helpers:get_config(Config, fixture_jwk) of
              undefined -> ?UTIL_MOD:fixture_jwk();
              Value     -> Value
          end,
    Token = case Audience of
        undefined -> ?UTIL_MOD:fixture_token_with_scopes(Scopes);
        DefinedAudience -> maps:put(<<"aud">>, DefinedAudience, ?UTIL_MOD:fixture_token_with_scopes(Scopes))
    end,
    ?UTIL_MOD:sign_token_hs(Token, Jwk).

generate_valid_token_with_extra_fields(Config, ExtraFields) ->
    Jwk = case rabbit_ct_helpers:get_config(Config, fixture_jwk) of
              undefined -> ?UTIL_MOD:fixture_jwk();
              Value     -> Value
          end,
    Token = maps:merge(?UTIL_MOD:fixture_token_with_scopes([]), ExtraFields),
    ?UTIL_MOD:sign_token_hs(Token, Jwk).

generate_expired_token(Config) ->
    generate_expired_token(Config, ?UTIL_MOD:full_permission_scopes()).

generate_expired_token(Config, Scopes) ->
    Jwk = case rabbit_ct_helpers:get_config(Config, fixture_jwk) of
              undefined -> ?UTIL_MOD:fixture_jwk();
              Value     -> Value
          end,
    ?UTIL_MOD:sign_token_hs(?UTIL_MOD:expired_token_with_scopes(Scopes), Jwk).

generate_expirable_token(Config, Seconds) ->
    generate_expirable_token(Config, ?UTIL_MOD:full_permission_scopes(), Seconds).

generate_expirable_token(Config, Scopes, Seconds) ->
    Jwk = case rabbit_ct_helpers:get_config(Config, fixture_jwk) of
              undefined -> ?UTIL_MOD:fixture_jwk();
              Value     -> Value
          end,
    Expiration = os:system_time(seconds) + Seconds,
    ?UTIL_MOD:sign_token_hs(?UTIL_MOD:token_with_scopes_and_expiration(Scopes, Expiration), Jwk).

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

test_successful_connection_with_simple_strings_for_aud_and_scope(Config) ->
    {_Algo, Token} = generate_valid_token(
        Config,
        <<"rabbitmq.configure:*/* rabbitmq.write:*/* rabbitmq.read:*/*">>,
        <<"hare rabbitmq">>
    ),
    Conn     = open_unmanaged_connection(Config, 0, <<"username">>, Token),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    #'queue.declare_ok'{queue = _} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
    close_connection_and_channel(Conn, Ch).

test_successful_connection_with_complex_claim_as_a_map(Config) ->
    {_Algo, Token} = generate_valid_token_with_extra_fields(
        Config,
        #{<<"additional_rabbitmq_scopes">> => #{<<"rabbitmq">> => [<<"configure:*/*">>, <<"read:*/*">>, <<"write:*/*">>]}}
    ),
    Conn     = open_unmanaged_connection(Config, 0, <<"username">>, Token),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    #'queue.declare_ok'{queue = _} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
    close_connection_and_channel(Conn, Ch).

test_successful_connection_with_complex_claim_as_a_list(Config) ->
    {_Algo, Token} = generate_valid_token_with_extra_fields(
        Config,
        #{<<"additional_rabbitmq_scopes">> => [<<"rabbitmq.configure:*/*">>, <<"rabbitmq.read:*/*">>, <<"rabbitmq.write:*/*">>]}
    ),
    Conn     = open_unmanaged_connection(Config, 0, <<"username">>, Token),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    #'queue.declare_ok'{queue = _} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
    close_connection_and_channel(Conn, Ch).

test_successful_connection_with_complex_claim_as_a_binary(Config) ->
    {_Algo, Token} = generate_valid_token_with_extra_fields(
        Config,
        #{<<"additional_rabbitmq_scopes">> => <<"rabbitmq.configure:*/* rabbitmq.read:*/*" "rabbitmq.write:*/*">>}
    ),
    Conn     = open_unmanaged_connection(Config, 0, <<"username">>, Token),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    #'queue.declare_ok'{queue = _} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
    close_connection_and_channel(Conn, Ch).

test_successful_connection_with_keycloak_token(Config) ->
    {_Algo, Token} = generate_valid_token_with_extra_fields(
        Config,
        #{<<"authorization">> => #{<<"permissions">> =>
                [#{<<"rsid">> => <<"2c390fe4-02ad-41c7-98a2-cebb8c60ccf1">>,
                   <<"rsname">> => <<"allvhost">>,
                   <<"scopes">> => [<<"rabbitmq.configure:*/*">>]},
                 #{<<"rsid">> => <<"e7f12e94-4c34-43d8-b2b1-c516af644cee">>,
                   <<"rsname">> => <<"vhost1">>,
                   <<"scopes">> => [<<"rabbitmq.write:*/*">>]},
                 #{<<"rsid">> => <<"12ac3d1c-28c2-4521-8e33-0952eff10bd9">>,
                   <<"rsname">> => <<"Default Resource">>,
                   <<"scopes">> => [<<"rabbitmq.read:*/*">>]},
                   %% this one won't be used because of the resource id
                 #{<<"rsid">> => <<"bee8fac6-c3ec-11e9-aa8c-2a2ae2dbcce4">>,
                   <<"rsname">> => <<"Default Resource">>,
                   <<"scopes">> => [<<"rabbitmq-resource-read">>]}]}}
    ),
    Conn     = open_unmanaged_connection(Config, 0, <<"username">>, Token),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    #'queue.declare_ok'{queue = _} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
    close_connection_and_channel(Conn, Ch).

test_successful_token_refresh(Config) ->
    Duration = 5,
    {_, Token} = generate_expirable_token(Config, [<<"rabbitmq.configure:vhost1/*">>,
                                                   <<"rabbitmq.write:vhost1/*">>,
                                                   <<"rabbitmq.read:vhost1/*">>],
                                                   Duration),
    Conn     = open_unmanaged_connection(Config, 0, <<"vhost1">>, <<"username">>, Token),
    {ok, Ch} = amqp_connection:open_channel(Conn),

    {_, Token2} = generate_valid_token(Config, [<<"rabbitmq.configure:vhost1/*">>,
                                                <<"rabbitmq.write:vhost1/*">>,
                                                <<"rabbitmq.read:vhost1/*">>]),
    ?UTIL_MOD:wait_for_token_to_expire(timer:seconds(Duration)),
    ?assertEqual(ok, amqp_connection:update_secret(Conn, Token2, <<"token refresh">>)),

    {ok, Ch2} = amqp_connection:open_channel(Conn),

    #'queue.declare_ok'{queue = _} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
    #'queue.declare_ok'{queue = _} =
        amqp_channel:call(Ch2, #'queue.declare'{exclusive = true}),

    amqp_channel:close(Ch2),
    close_connection_and_channel(Conn, Ch).


test_failed_connection_with_expired_token(Config) ->
    {_Algo, Token} = generate_expired_token(Config, [<<"rabbitmq.configure:vhost1/*">>,
                                                     <<"rabbitmq.write:vhost1/*">>,
                                                     <<"rabbitmq.read:vhost1/*">>]),
    ?assertMatch({error, {auth_failure, _}},
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

test_failed_token_refresh_case1(Config) ->
    {_, Token} = generate_valid_token(Config, [<<"rabbitmq.configure:vhost4/*">>,
                                               <<"rabbitmq.write:vhost4/*">>,
                                               <<"rabbitmq.read:vhost4/*">>]),
    Conn     = open_unmanaged_connection(Config, 0, <<"vhost4">>, <<"username">>, Token),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    #'queue.declare_ok'{queue = _} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),

    {_, Token2} = generate_expired_token(Config, [<<"rabbitmq.configure:vhost4/*">>,
                                                  <<"rabbitmq.write:vhost4/*">>,
                                                  <<"rabbitmq.read:vhost4/*">>]),
    %% the error is communicated asynchronously via a connection-level error
    ?assertEqual(ok, amqp_connection:update_secret(Conn, Token2, <<"token refresh">>)),

    {ok, Ch2} = amqp_connection:open_channel(Conn),
    ?assertExit({{shutdown, {server_initiated_close, 403, _}}, _},
       amqp_channel:call(Ch2, #'queue.declare'{queue = <<"a.q">>, exclusive = true})),

    close_connection(Conn).

test_failed_token_refresh_case2(Config) ->
    {_Algo, Token} = generate_valid_token(Config, [<<"rabbitmq.configure:vhost4/*">>,
                                                   <<"rabbitmq.write:vhost4/*">>,
                                                   <<"rabbitmq.read:vhost4/*">>]),
    Conn     = open_unmanaged_connection(Config, 0, <<"vhost4">>, <<"username">>, Token),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    #'queue.declare_ok'{queue = _} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),

    %% the error is communicated asynchronously via a connection-level error
    ?assertEqual(ok, amqp_connection:update_secret(Conn, <<"not-a-token-^^^^5%">>, <<"token refresh">>)),

    ?assertExit({{shutdown, {connection_closing, {server_initiated_close, 530, _}}}, _},
       amqp_connection:open_channel(Conn)),

    close_connection(Conn).
