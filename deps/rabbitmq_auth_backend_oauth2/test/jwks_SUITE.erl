%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(jwks_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(rabbit_ct_client_helpers, [
    close_connection/1, 
    close_channel/1,
    open_unmanaged_connection/4, 
    open_unmanaged_connection/5,
    close_connection_and_channel/2
]).
-import(rabbit_ct_helpers, [
    set_config/2,
    get_config/2, get_config/3
]).
-import(rabbit_ct_broker_helpers, [    
    rpc/5
]).
-import(rabbit_mgmt_test_util, [
    amqp_port/1
]).

all() ->
    [
     {group, happy_path},
     {group, unhappy_path},
     {group, no_peer_verification},
     {group, verify_signing_keys}
    ].

groups() ->
    [{happy_path, [], [
        test_successful_connection_with_a_full_permission_token_and_all_defaults,
        test_successful_connection_with_a_full_permission_token_and_explicitly_configured_vhost,
        test_successful_connection_with_simple_strings_for_aud_and_scope,
        test_successful_connection_with_complex_claim_as_a_map,
        test_successful_connection_with_complex_claim_as_a_list,
        test_successful_connection_with_complex_claim_as_a_binary,
        test_successful_connection_with_keycloak_token,
        test_successful_connection_with_algorithm_restriction,
        test_successful_token_refresh
        ]},
     {unhappy_path, [], [
        test_failed_connection_with_expired_token,
        test_failed_connection_with_a_non_token,
        test_failed_connection_with_a_token_with_insufficient_vhost_permission,
        test_failed_connection_with_a_token_with_insufficient_resource_permission,
        test_failed_connection_with_algorithm_restriction,
        test_failed_token_refresh_case1,
        test_failed_token_refresh_case2
        ]},
    {no_peer_verification, [], [
        {group, happy_path},
        {group, unhappy_path}
    ]},
    {verify_signing_keys, [], [
        {with_oauth_providers_A_B_and_C, [], [
            {with_default_oauth_provider_B, [], [
                {with_oauth_provider_A_with_jwks_with_one_signing_key, [], [
                    {with_resource_servers_rabbitmq1_with_oauth_provider_A, [], [
                        test_successful_connection_for_rabbitmq1_audience_signed_by_provider_A,
                        {without_kid, [], [
                            test_unsuccessful_connection_for_rabbitmq1_signed_by_provider_A,
                            {with_oauth_providers_A_with_default_key, [], [
                                test_successful_connection_for_rabbitmq1_audience_signed_by_provider_A
                            ]}
                        ]}
                    ]}
                ]},
                {with_oauth_provider_B_with_one_static_key_and_jwks_with_two_signing_keys, [], [
                    {with_resource_servers_rabbitmq2, [], [
                        test_successful_connection_for_rabbitmq2_audience_signed_by_provider_B_with_static_key,
                        test_successful_connection_for_rabbitmq2_audience_signed_by_provider_B_with_jwks_key_1,
                        test_successful_connection_for_rabbitmq2_audience_signed_by_provider_B_with_jwks_key_2,
                        {without_kid, [], [
                            test_unsuccessful_connection_for_rabbitmq2_signed_by_provider_B_with_static_key,
                            {with_oauth_providers_B_with_default_key_static_key, [], [
                                test_successful_connection_for_rabbitmq2_audience_signed_by_provider_B_with_static_key
                            ]}
                        ]}
                    ]},
                    {with_oauth_provider_C_with_two_static_keys, [], [
                        {with_resource_servers_rabbitmq3_with_oauth_provider_C, [], [
                            test_successful_connection_for_rabbitmq3_audience_signed_by_provider_C_with_static_key_1,
                            test_successful_connection_for_rabbitmq3_audience_signed_by_provider_C_with_static_key_2,
                            {without_kid, [], [
                                test_unsuccessful_connection_for_rabbitmq3_audience_signed_by_provider_C_with_static_key_1,
                                {with_oauth_providers_C_with_default_key_static_key_1, [], [
                                    test_successful_connection_for_rabbitmq3_audience_signed_by_provider_C_with_static_key_1
                                ]}
                            ]}
                        ]}
                    ]}
                ]}
            ]}

        ]},
        {with_root_oauth_provider_with_two_static_keys_and_one_jwks_key, [], [
            {with_resource_server_rabbitmq, [], [
                test_successful_connection_for_rabbitmq_audience_signed_by_root_oauth_provider_with_static_key_1,
                test_successful_connection_for_rabbitmq_audience_signed_by_root_oauth_provider_with_static_key_2,
                test_successful_connection_for_rabbitmq_audience_signed_by_root_oauth_provider_with_jwks_key,
                {without_kid, [], [
                    test_unsuccessful_connection_for_rabbitmq_audience_signed_by_root_oauth_provider_with_static_key_1,
                    {with_root_oauth_provider_with_default_key_1, [], [
                        test_successful_connection_for_rabbitmq_audience_signed_by_root_oauth_provider_with_static_key_1
                    ]}
                ]},
                {with_resource_servers_rabbitmq2, [], [
                    test_successful_connection_for_rabbitmq2_audience_signed_by_root_oauth_provider_with_jwks_key,
                    {without_kid, [], [
                        test_unsuccessful_connection_for_rabbitmq2_audience_signed_by_root_oauth_provider_with_jwks_key,
                        {with_root_oauth_provider_with_default_jwks_key, [], [
                            test_successful_connection_for_rabbitmq2_audience_signed_by_root_oauth_provider_with_jwks_key
                        ]}
                    ]},
                    {with_oauth_providers_A_B_and_C, [], [
                        {with_oauth_provider_A_with_jwks_with_one_signing_key, [], [
                            {with_resource_servers_rabbitmq1_with_oauth_provider_A, [], [
                                test_successful_connection_for_rabbitmq1_audience_signed_by_provider_A,
                                test_successful_connection_for_rabbitmq2_audience_signed_by_root_oauth_provider_with_jwks_key,
                                test_successful_connection_for_rabbitmq_audience_signed_by_root_oauth_provider_with_static_key_1,
                                {without_kid, [], [
                                    test_unsuccessful_connection_for_rabbitmq1_signed_by_provider_A,
                                    {with_oauth_providers_A_with_default_key, [], [
                                        test_successful_connection_for_rabbitmq1_audience_signed_by_provider_A                                    
                                    ]}
                                ]}
                            ]}
                        ]}
                    ]}
                ]}
            ]}
        ]}
     ]}
    ].

%%
%% Setup and Teardown
%%

-define(UTIL_MOD, rabbit_auth_backend_oauth2_test_util).
-define(RESOURCE_SERVER_ID, <<"rabbitmq">>).
-define(RESOURCE_SERVER_TYPE, <<"rabbitmq">>).
-define(EXTRA_SCOPES_SOURCE, <<"additional_rabbitmq_scopes">>).

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config,
      rabbit_ct_broker_helpers:setup_steps() ++ [
        fun preconfigure_node/1,
        fun start_jwks_server/1,
        fun preconfigure_token/1
        %We fun add_vhosts/1
      ]).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      [
        fun stop_jwks_server/1
      ] ++ rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(no_peer_verification, Config) ->
    KeyConfig = set_config(?config(key_config, Config), [
            {jwks_url, ?config(non_strict_jwks_uri, Config)}, 
            {peer_verification, verify_none}
    ]),
    ok = rpc_set_env(Config, key_config, KeyConfig),
    set_config(Config, {key_config, KeyConfig});
init_per_group(without_kid, Config) ->
    set_config(Config, [{include_kid, false}]);
init_per_group(with_resource_servers_rabbitmq1_with_oauth_provider_A, Config) ->
    ResourceServersConfig0 = rpc_get_env(Config, resource_servers, #{}),
    Resource0 = maps:get(<<"rabbitmq1">>, ResourceServersConfig0, 
        [{id, <<"rabbitmq1">>}]),
    ResourceServersConfig1 = maps:put(<<"rabbitmq1">>, 
        [{oauth_provider_id, <<"A">>} | Resource0], ResourceServersConfig0),
    ok = rpc_set_env(Config, resource_servers, ResourceServersConfig1);
init_per_group(with_oauth_providers_A_B_and_C, Config) ->
    OAuthProviders = #{
        <<"A">> => [
            {id, <<"A">>},
            {https, [{verify, verify_none}]}
        ],
        <<"B">> => [
            {id, <<"B">>},
            {https, [{verify, verify_none}]}
        ],
        <<"C">> => [
            {id, <<"C">>},
            {https, [{verify, verify_none}]}
        ]
    },
    ok = rpc_set_env(Config, oauth_providers, OAuthProviders),
    Config;
init_per_group(with_default_oauth_provider_B, Config) ->
    ok = rpc_set_env(Config, default_oauth_provider, <<"B">>);
init_per_group(with_oauth_providers_A_with_default_key, Config) ->
    {ok, OAuthProviders0} = rpc_get_env(Config, oauth_providers),
    OAuthProvider = maps:get(<<"A">>, OAuthProviders0, []),
    OAuthProviders1 = maps:put(<<"A">>, [
        {default_key, ?UTIL_MOD:token_key(?config(fixture_jwksA, Config))} 
        | OAuthProvider], OAuthProviders0),
    ok = rpc_set_env(Config, oauth_providers, OAuthProviders1),
    Config;
init_per_group(with_oauth_provider_A_with_jwks_with_one_signing_key, Config) ->
    {ok, OAuthProviders0} = rpc_get_env(Config, oauth_providers),
    OAuthProvider = maps:get(<<"A">>, OAuthProviders0, []),
    OAuthProviders1 = maps:put(<<"A">>, [
        {jwks_uri, strict_jwks_uri(Config, "/jwksA")} | OAuthProvider],
        OAuthProviders0),
    ok = rpc_set_env(Config, oauth_providers, OAuthProviders1),
    Config;
init_per_group(with_resource_servers_rabbitmq2, Config) ->
    ResourceServersConfig0 = rpc_get_env(Config, resource_servers, #{}),
    Resource0 = maps:get(<<"rabbitmq2">>, ResourceServersConfig0, 
        [{id, <<"rabbitmq2">>}]),
    ResourceServersConfig1 = maps:put(<<"rabbitmq2">>, Resource0, 
        ResourceServersConfig0),
    ok = rpc_set_env(Config, resource_servers, ResourceServersConfig1),
    Config;
init_per_group(with_oauth_providers_B_with_default_key_static_key, Config) ->
    {ok, OAuthProviders0} = rpc_get_env(Config, oauth_providers),
    OAuthProvider = maps:get(<<"B">>, OAuthProviders0, []),
    OAuthProviders1 = maps:put(<<"B">>, [
        {default_key, ?UTIL_MOD:token_key(?config(fixture_staticB, Config))} |
        proplists:delete(default_key, OAuthProvider)],
        OAuthProviders0),
    ok = rpc_set_env(Config,oauth_providers, OAuthProviders1),
    Config;
init_per_group(with_oauth_provider_C_with_two_static_keys, Config) ->
    {ok, OAuthProviders0} = rpc_get_env(Config, oauth_providers),
    OAuthProvider = maps:get(<<"C">>, OAuthProviders0, []),
    Jwks1 = ?config(fixture_staticC_1, Config),
    Jwks2 = ?config(fixture_staticC_2, Config),
    SigningKeys = #{
        ?UTIL_MOD:token_key(Jwks1) => {json, Jwks1},
        ?UTIL_MOD:token_key(Jwks2) => {json, Jwks2}
    },
    OAuthProviders1 = maps:put(<<"C">>, [
        {signing_keys, SigningKeys} | OAuthProvider], OAuthProviders0),

    ok = rpc_set_env(Config, oauth_providers, OAuthProviders1),
    Config;
init_per_group(with_root_oauth_provider_with_two_static_keys_and_one_jwks_key, Config) ->
    KeyConfig = rpc_get_env(Config, key_config, []),
    Jwks1 = ?config(fixture_static_1, Config),
    Jwks2 = ?config(fixture_static_2, Config),
    SigningKeys = #{
        ?UTIL_MOD:token_key(Jwks1) => {json, Jwks1},
        ?UTIL_MOD:token_key(Jwks2) => {json, Jwks2}
    },
    KeyConfig1 = [{signing_keys, SigningKeys},
                  {jwks_url, strict_jwks_uri(Config, "/jwks")}| KeyConfig],
    ok = rpc_set_env(Config, key_config, KeyConfig1),
    Config;
init_per_group(with_root_oauth_provider_with_default_key_1, Config) ->
    KeyConfig = rpc_get_env(Config, key_config, []),
    KeyConfig1 = [
        {default_key, ?UTIL_MOD:token_key(?config(fixture_static_1, Config))} 
        | KeyConfig],
    ok = rpc_set_env(Config, key_config, KeyConfig1),
    Config;
init_per_group(with_root_oauth_provider_with_default_jwks_key, Config) ->
    KeyConfig = rpc_get_env(Config, key_config, []),
    KeyConfig1 = [
        {default_key, ?UTIL_MOD:token_key(?config(fixture_jwk, Config))} 
        | KeyConfig],
    ok = rpc_set_env(Config, key_config, KeyConfig1),
    Config;
init_per_group(with_oauth_provider_B_with_one_static_key_and_jwks_with_two_signing_keys, Config) ->
    {ok, OAuthProviders0} = rpc_get_env(Config, oauth_providers),
    OAuthProvider = maps:get(<<"B">>, OAuthProviders0, []),
    Jwks = ?config(fixture_staticB, Config),
    SigningKeys = #{
        ?UTIL_MOD:token_key(Jwks) => {json, Jwks}
    },
    OAuthProviders1 = maps:put(<<"B">>, [
        {signing_keys, SigningKeys},
        {jwks_uri, strict_jwks_uri(Config, "/jwksB")} | OAuthProvider],
        OAuthProviders0),

    ok = rpc_set_env(Config, oauth_providers, OAuthProviders1),
    Config;
init_per_group(with_resource_servers_rabbitmq3_with_oauth_provider_C, Config) ->
    ResourceServersConfig0 = rpc_get_env(Config, resource_servers, #{}),
    Resource0 = maps:get(<<"rabbitmq3">>, ResourceServersConfig0, [
        {id, <<"rabbitmq3">>},{oauth_provider_id, <<"C">>}]),
    ResourceServersConfig1 = maps:put(<<"rabbitmq3">>, Resource0, 
        ResourceServersConfig0),
    ok = rpc_set_env(Config, resource_servers, ResourceServersConfig1);
init_per_group(with_oauth_providers_C_with_default_key_static_key_1, Config) ->
    {ok, OAuthProviders0} = rpc_get_env(Config, oauth_providers),
    OAuthProvider = maps:get(<<"C">>, OAuthProviders0, []),
    Jwks = ?config(fixture_staticC_1, Config),
    OAuthProviders1 = maps:put(<<"C">>, [
        {default_key, ?UTIL_MOD:token_key(Jwks)} | OAuthProvider],
        OAuthProviders0),
    ok = rpc_set_env(Config, oauth_providers, OAuthProviders1),
    Config;
init_per_group(_Group, Config) ->
    ok = rpc_set_env(Config, resource_server_id, ?RESOURCE_SERVER_ID),
    Config.

end_per_group(without_kid, Config) ->
    rabbit_ct_helpers:delete_config(Config, include_kid);

end_per_group(no_peer_verification, Config) ->
    KeyConfig = set_config(?config(key_config, Config), [
        {jwks_uri, ?config(strict_jwks_uri, Config)}, 
        {peer_verification, verify_peer}]),
    ok = rpc_set_env(Config, key_config, KeyConfig),
    set_config(Config, {key_config, KeyConfig});

end_per_group(with_default_oauth_provider_B, Config) ->
    ok = rpc_unset_env(Config, default_oauth_provider);

end_per_group(with_root_oauth_provider_with_default_key_1, Config) ->
    KeyConfig = rpc_get_env(Config, key_config, []),
    KeyConfig1 = proplists:delete(default_key, KeyConfig),
    ok = rpc_set_env(Config, key_config, KeyConfig1),
    Config;
end_per_group(with_root_oauth_provider_with_default_jwks_key, Config) ->
    KeyConfig = rpc_get_env(Config, key_config, []),
    KeyConfig1 = proplists:delete(default_key, KeyConfig),
    ok = rpc_set_env(Config, key_config, KeyConfig1),
    Config;

end_per_group(_Group, Config) ->
    Config.

add_vhosts(Config) ->
    %% The broker is managed by {init,end}_per_testcase().
    lists:foreach(fun(Value) -> 
        rabbit_ct_broker_helpers:add_vhost(Config, Value) end,
        [<<"vhost1">>, <<"vhost2">>, <<"vhost3">>, <<"vhost4">>]).
    %rabbit_ct_helpers:set_config(Config, []).

delete_vhosts(Config) ->
    %% The broker is managed by {init,end}_per_testcase().
    lists:foreach(fun(Value) -> 
        rabbit_ct_broker_helpers:delete_vhost(Config, Value) end,
        [<<"vhost1">>, <<"vhost2">>, <<"vhost3">>, <<"vhost4">>]).

init_per_testcase(Testcase, Config) when 
        Testcase =:= test_successful_connection_with_a_full_permission_token_and_explicitly_configured_vhost orelse
        Testcase =:= test_successful_token_refresh ->
    rabbit_ct_broker_helpers:add_vhost(Config, <<"vhost1">>),
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    Config;

init_per_testcase(Testcase, Config) when 
        Testcase =:= test_failed_token_refresh_case1 orelse
        Testcase =:= test_failed_token_refresh_case2 ->
    rabbit_ct_broker_helpers:add_vhost(Config, <<"vhost4">>),
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    Config;

init_per_testcase(Testcase, Config) when 
        Testcase =:= test_successful_connection_with_complex_claim_as_a_map orelse
        Testcase =:= test_successful_connection_with_complex_claim_as_a_list orelse
        Testcase =:= test_successful_connection_with_complex_claim_as_a_binary ->
  ok = rpc_set_env(Config, extra_scopes_source, ?EXTRA_SCOPES_SOURCE),
  rabbit_ct_helpers:testcase_started(Config, Testcase),
  Config;

init_per_testcase(Testcase, Config) when 
        Testcase =:= test_successful_connection_with_algorithm_restriction ->
    KeyConfig = ?config(key_config, Config),
    ok = rpc_set_env(Config, key_config, [{algorithms, [<<"HS256">>]} | KeyConfig]),
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    Config;

init_per_testcase(Testcase, Config) when 
        Testcase =:= test_failed_connection_with_algorithm_restriction ->
    KeyConfig = ?config(key_config, Config),
    ok = rpc_set_env(Config, key_config, [{algorithms, [<<"RS256">>]} | KeyConfig]),
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    Config;

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    Config.

end_per_testcase(Testcase, Config) when 
        Testcase =:= test_failed_token_refresh_case1 orelse
        Testcase =:= test_failed_token_refresh_case2 ->
    rabbit_ct_broker_helpers:delete_vhost(Config, <<"vhost4">>),
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    Config;

end_per_testcase(Testcase, Config) when 
        Testcase =:= test_successful_connection_with_complex_claim_as_a_map orelse
        Testcase =:= test_successful_connection_with_complex_claim_as_a_list orelse
        Testcase =:= test_successful_connection_with_complex_claim_as_a_binary ->
  rabbit_ct_broker_helpers:delete_vhost(Config, <<"vhost1">>),
  ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, unset_env,
    [rabbitmq_auth_backend_oauth2, extra_scopes_source]),
  rabbit_ct_helpers:testcase_started(Config, Testcase),
  Config;

end_per_testcase(Testcase, Config) when 
        Testcase =:= test_successful_connection_with_algorithm_restriction orelse
        Testcase =:= test_failed_connection_with_algorithm_restriction ->
    rabbit_ct_broker_helpers:delete_vhost(Config, <<"vhost1">>),
    ok = rpc_set_env(Config, key_config, ?config(key_config, Config)),
    rabbit_ct_helpers:testcase_finished(Config, Testcase),
    Config;

end_per_testcase(Testcase, Config) ->
    rabbit_ct_broker_helpers:delete_vhost(Config, <<"vhost1">>),
    rabbit_ct_helpers:testcase_finished(Config, Testcase),
    Config.

preconfigure_node(Config) ->
    ok = rpc(Config, 0, application, set_env,
        [rabbit, auth_backends, [rabbit_auth_backend_oauth2]]),
    ok = rpc_set_env(Config, resource_server_id, ?RESOURCE_SERVER_ID),
    add_vhosts(Config),
    Config.

start_jwks_server(Config0) ->
    Jwk   = ?UTIL_MOD:fixture_jwk(),
    Jwk1  = ?UTIL_MOD:fixture_jwk(<<"token-key-1">>),
    Jwk2  = ?UTIL_MOD:fixture_jwk(<<"token-key-2">>),
    Jwk3  = ?UTIL_MOD:fixture_jwk(<<"token-key-3">>),
    Jwk4  = ?UTIL_MOD:fixture_jwk(<<"token-key-4">>),
    Jwk5  = ?UTIL_MOD:fixture_jwk(<<"token-key-5">>),
    Jwk6  = ?UTIL_MOD:fixture_jwk(<<"token-key-6">>),
    Jwk7  = ?UTIL_MOD:fixture_jwk(<<"token-key-7">>),
    Jwk8  = ?UTIL_MOD:fixture_jwk(<<"token-key-8">>),
    %% Assume we don't have more than 100 ports allocated for tests
    PortBase = rabbit_ct_broker_helpers:get_node_config(Config0, 0, tcp_ports_base),
    JwksServerPort = PortBase + 100,
    Config = set_config(Config0, [{jwksServerPort, JwksServerPort}]),

    %% Both URLs direct to the same JWKS server
    %% The NonStrictJwksUrl identity cannot be validated while StrictJwksUrl identity can be validated
    NonStrictJwksUri = non_strict_jwks_uri(Config),
    StrictJwksUri = strict_jwks_uri(Config),

    {ok, _} = application:ensure_all_started(ssl),
    {ok, _} = application:ensure_all_started(cowboy),
    CertsDir = ?config(rmq_certsdir, Config),
    ok = jwks_http_app:start(JwksServerPort, CertsDir,
      [ {"/jwksA", [Jwk]},
        {"/jwksB", [Jwk1, Jwk3]},
        {"/jwksRoot", [Jwk2]},
        {"/jwks", [Jwk]},
        {"/jwks1", [Jwk1, Jwk3]},
        {"/jwks2", [Jwk2]}
      ]),
    KeyConfig = [{jwks_url, StrictJwksUri},
                 {peer_verification, verify_peer},
                 {cacertfile, filename:join([CertsDir, "testca", "cacert.pem"])}],
    ok = rpc_set_env(Config, key_config, KeyConfig),
    set_config(Config, [
                        {non_strict_jwks_uri, NonStrictJwksUri},
                        {strict_jwks_uri, StrictJwksUri},
                        {key_config, KeyConfig},
                        {fixture_static_1, Jwk7},
                        {fixture_static_2, Jwk8},
                        {fixture_staticB, Jwk4},
                        {fixture_staticC_1, Jwk5},
                        {fixture_staticC_2, Jwk6},
                        {fixture_jwksB_1, Jwk1},
                        {fixture_jwksB_2, Jwk3},
                        {fixture_jwksA, Jwk},
                        {fixture_jwk, Jwk},
                        {fixture_jwks_1, [Jwk1, Jwk3]},
                        {fixture_jwks_2, [Jwk2]}
    ]).
strict_jwks_uri(Config) ->
  strict_jwks_uri(Config, "/jwks").
strict_jwks_uri(Config, Path) ->
  "https://localhost:" ++ integer_to_list(?config(jwksServerPort, Config)) ++ Path.
non_strict_jwks_uri(Config) ->
  non_strict_jwks_uri(Config, "/jwks").
non_strict_jwks_uri(Config, Path) ->
  "https://127.0.0.1:" ++ integer_to_list(?config(jwksServerPort, Config)) ++ Path.


stop_jwks_server(Config) ->
    ok = jwks_http_app:stop(),
    Config.

generate_valid_token(Config) ->
    generate_valid_token(Config, ?UTIL_MOD:full_permission_scopes()).

generate_valid_token(Config, Scopes) ->
    generate_valid_token(Config, Scopes, undefined).

generate_valid_token(Config, Scopes, Audience) ->
    Jwk = 
        case get_config(Config, fixture_jwk) of
            undefined -> ?UTIL_MOD:fixture_jwk();
            Value     -> Value
        end,
    generate_valid_token(Config, Jwk, Scopes, Audience).

generate_valid_token(Config, Jwk, Scopes, Audience) ->
    Token = 
        case Audience of
            undefined -> 
                ?UTIL_MOD:fixture_token_with_scopes(Scopes);
            DefinedAudience -> 
                maps:put(<<"aud">>, DefinedAudience, 
                    ?UTIL_MOD:fixture_token_with_scopes(Scopes))
    end,
    IncludeKid = rabbit_ct_helpers:get_config(Config, include_kid, true),
    ?UTIL_MOD:sign_token_hs(Token, Jwk, IncludeKid).

generate_valid_token_with_extra_fields(Config, ExtraFields) ->
    Jwk = 
        case rabbit_ct_helpers:get_config(Config, fixture_jwk) of
            undefined -> ?UTIL_MOD:fixture_jwk();
            Value     -> Value
        end,
    Token = maps:merge(?UTIL_MOD:fixture_token_with_scopes([]), ExtraFields),
    ?UTIL_MOD:sign_token_hs(Token, Jwk, 
        rabbit_ct_helpers:get_config(Config, include_kid, true)).

generate_expired_token(Config) ->
    generate_expired_token(Config, ?UTIL_MOD:full_permission_scopes()).

generate_expired_token(Config, Scopes) ->
    Jwk = 
        case get_config(Config, fixture_jwk) of
            undefined -> ?UTIL_MOD:fixture_jwk();
            Value     -> Value
        end,
    ?UTIL_MOD:sign_token_hs(?UTIL_MOD:expired_token_with_scopes(Scopes), Jwk,
        get_config(Config, include_kid, true)).

generate_expirable_token(Config, Seconds) ->
    generate_expirable_token(Config, ?UTIL_MOD:full_permission_scopes(), Seconds).

generate_expirable_token(Config, Scopes, Seconds) ->
    Jwk = 
        case get_config(Config, fixture_jwk) of
            undefined -> ?UTIL_MOD:fixture_jwk();
            Value     -> Value
        end,
    Expiration = os:system_time(seconds) + Seconds,
    ?UTIL_MOD:sign_token_hs(?UTIL_MOD:token_with_scopes_and_expiration(
        Scopes, Expiration), Jwk, get_config(Config, include_kid, true)).

preconfigure_token(Config) ->
    Token = generate_valid_token(Config),
    set_config(Config, {fixture_jwt, Token}).


%%
%% Test Cases
%%
test_successful_connection_for_rabbitmq1_audience_signed_by_provider_A(Config) ->
    Jwks = ?config(fixture_jwksA, Config),
    Scopes = <<"rabbitmq1.configure:*/* rabbitmq1.write:*/* rabbitmq1.read:*/*">>,
    Audience = <<"rabbitmq1">>,
    test_queue_declare(Config, Jwks, Scopes, Audience).
test_unsuccessful_connection_for_rabbitmq1_signed_by_provider_A(Config) ->
    Jwks = ?config(fixture_jwksA, Config),
    Scopes = <<"rabbitmq1.configure:*/* rabbitmq1.write:*/* rabbitmq1.read:*/*">>,
    Audience = <<"rabbitmq1">>,
    {_Alg, Token} = generate_valid_token(
        Config,
        Jwks,
        Scopes,
        [Audience]
    ),
    ?assertMatch({error, {auth_failure, _}},
         open_unmanaged_connection(Config, 0, <<"vhost1">>, <<"username">>, Token)).

test_successful_connection_for_rabbitmq2_audience_signed_by_provider_B_with_static_key(Config) ->
    Jwks = ?config(fixture_staticB, Config),
    Scopes = <<"rabbitmq2.configure:*/* rabbitmq2.write:*/* rabbitmq2.read:*/*">>,
    Audience = <<"rabbitmq2">>,
    test_queue_declare(Config, Jwks, Scopes, Audience).
test_successful_connection_for_rabbitmq2_audience_signed_by_provider_B_with_jwks_key_1(Config) ->
    Jwks = ?config(fixture_jwksB_1, Config),
    Scopes = <<"rabbitmq2.configure:*/* rabbitmq2.write:*/* rabbitmq2.read:*/*">>,
    Audience = <<"rabbitmq2">>,
    test_queue_declare(Config, Jwks, Scopes, Audience).
test_successful_connection_for_rabbitmq2_audience_signed_by_provider_B_with_jwks_key_2(Config) ->
    Jwks = ?config(fixture_jwksB_2, Config),
    Scopes = <<"rabbitmq2.configure:*/* rabbitmq2.write:*/* rabbitmq2.read:*/*">>,
    Audience = <<"rabbitmq2">>,
    test_queue_declare(Config, Jwks, Scopes, Audience).
test_successful_connection_for_rabbitmq3_audience_signed_by_provider_C_with_static_key_1(Config) ->
    Jwks = ?config(fixture_staticC_1, Config),
    Scopes = <<"rabbitmq3.configure:*/* rabbitmq3.write:*/* rabbitmq3.read:*/*">>,
    Audience = <<"rabbitmq3">>,
    test_queue_declare(Config, Jwks, Scopes, Audience).
test_successful_connection_for_rabbitmq3_audience_signed_by_provider_C_with_static_key_2(Config) ->
    Jwks = ?config(fixture_staticC_2, Config),
    Scopes = <<"rabbitmq3.configure:*/* rabbitmq3.write:*/* rabbitmq3.read:*/*">>,
    Audience = <<"rabbitmq3">>,
    test_queue_declare(Config, Jwks, Scopes, Audience).
test_successful_connection_for_rabbitmq_audience_signed_by_root_oauth_provider_with_static_key_1(Config) ->
    Jwks = ?config(fixture_static_1, Config),
    Scopes = <<"rabbitmq.configure:*/* rabbitmq.write:*/* rabbitmq.read:*/*">>,
    Audience = <<"rabbitmq">>,
    test_queue_declare(Config, Jwks, Scopes, Audience).
test_successful_connection_for_rabbitmq_audience_signed_by_root_oauth_provider_with_static_key_2(Config) ->
    Jwks = ?config(fixture_static_2, Config),
    Scopes = <<"rabbitmq.configure:*/* rabbitmq.write:*/* rabbitmq.read:*/*">>,
    Audience = <<"rabbitmq">>,
    test_queue_declare(Config, Jwks, Scopes, Audience).
test_successful_connection_for_rabbitmq_audience_signed_by_root_oauth_provider_with_jwks_key(Config) ->
    Jwks = ?config(fixture_jwk, Config),
    Scopes = <<"rabbitmq.configure:*/* rabbitmq.write:*/* rabbitmq.read:*/*">>,
    Audience = <<"rabbitmq">>,
    test_queue_declare(Config, Jwks, Scopes, Audience).
test_successful_connection_for_rabbitmq2_audience_signed_by_root_oauth_provider_with_jwks_key(Config) ->
    Jwks = ?config(fixture_jwk, Config),
    Scopes = <<"rabbitmq2.configure:*/* rabbitmq2.write:*/* rabbitmq2.read:*/*">>,
    Audience = <<"rabbitmq2">>,
    test_queue_declare(Config, Jwks, Scopes, Audience).
test_unsuccessful_connection_for_rabbitmq2_audience_signed_by_root_oauth_provider_with_jwks_key(Config) ->
    Jwks = ?config(fixture_jwk, Config),
    Scopes = <<"rabbitmq2.configure:*/* rabbitmq2.write:*/* rabbitmq2.read:*/*">>,
    Audience = <<"rabbitmq2">>,
    {_Alg, Token} = generate_valid_token(
        Config,
        Jwks,
        Scopes,
        [Audience]
    ),
    ?assertMatch({error, {auth_failure, _}},
         open_unmanaged_connection(Config, 0, <<"vhost1">>, <<"username">>, Token)).
test_unsuccessful_connection_for_rabbitmq2_signed_by_provider_B_with_static_key(Config) ->
    Jwks = ?config(fixture_staticB, Config),
    Scopes = <<"rabbitmq2.configure:*/* rabbitmq2.write:*/* rabbitmq2.read:*/*">>,
    Audience = <<"rabbitmq2">>,
    {_Alg, Token} = generate_valid_token(
        Config,
        Jwks,
        Scopes,
        [Audience]
    ),
    ?assertMatch({error, {auth_failure, _}},
         open_unmanaged_connection(Config, 0, <<"vhost1">>, <<"username">>, Token)).
test_unsuccessful_connection_for_rabbitmq3_audience_signed_by_provider_C_with_static_key_1(Config) ->
    Jwks = ?config(fixture_staticC_1, Config),
    Scopes = <<"rabbitmq3.configure:*/* rabbitmq3.write:*/* rabbitmq3.read:*/*">>,
    Audience = <<"rabbitmq3">>,
    {_Alg, Token} = generate_valid_token(
        Config,
        Jwks,
        Scopes,
        [Audience]
    ),
    ?assertMatch({error, {auth_failure, _}},
         open_unmanaged_connection(Config, 0, <<"vhost1">>, <<"username">>, Token)).
test_unsuccessful_connection_for_rabbitmq_audience_signed_by_root_oauth_provider_with_static_key_1(Config) ->
    Jwks = ?config(fixture_static_1, Config),
    Scopes = <<"rabbitmq.configure:*/* rabbitmq.write:*/* rabbitmq.read:*/*">>,
    Audience = <<"rabbitmq">>,
    {_Alg, Token} = generate_valid_token(
        Config,
        Jwks,
        Scopes,
        [Audience]
    ),
    ?assertMatch({error, {auth_failure, _}},
      open_unmanaged_connection(Config, 0, <<"vhost1">>, <<"username">>, Token)).
test_successful_connection_with_a_full_permission_token_and_all_defaults(Config) ->
    {_Algo, Token} = get_config(Config, fixture_jwt),
    verify_queue_declare_with_token(Config, Token).

verify_queue_declare_with_token(Config, Token) ->
    Conn     = open_unmanaged_connection(Config, 0, <<"username">>, Token),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    #'queue.declare_ok'{queue = _} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
    close_connection_and_channel(Conn, Ch).

test_queue_declare(Config, Jwks, Scopes, Audience) ->
    {_Alg, Token1} = generate_valid_token(
        Config,
        Jwks,
        Scopes,
        [Audience]
    ),
    verify_queue_declare_with_token(Config, Token1).

c(Config) ->
    TestCases = [
        {?config(fixture_jwk, Config),
        <<"rabbitmq1.configure:*/* rabbitmq1.write:*/* rabbitmq1.read:*/*">>,
        <<"rabbitmq1">>},
        {?config(fixture_jwk, Config),
        <<"rabbitmq2.configure:*/* rabbitmq2.write:*/* rabbitmq2.read:*/*">>,
        <<"rabbitmq2">>},
        {?config(fixture_jwk, Config),
        <<"rabbitmq1.configure:*/* rabbitmq1.write:*/* rabbitmq1.read:*/*">>,
        <<"rabbitmq1">>}
    ],
    [test_queue_declare(Config, Jwks, Scopes, Audience) ||
        {Jwks, Scopes, Audience} <- TestCases].


test_successful_queue_declaration_using_multiple_keys_and_audiences(Config) ->
    TestCases = [
        {lists:nth(1, ?config(fixture_jwks_1, Config)),
        <<"rabbitmq1.configure:*/* rabbitmq1.write:*/* rabbitmq1.read:*/*">>,
        <<"rabbitmq1">>},
        {lists:nth(2, ?config(fixture_jwks_1, Config)),
        <<"rabbitmq1.configure:*/* rabbitmq1.write:*/* rabbitmq1.read:*/*">>,
        <<"rabbitmq1">>},
        {lists:nth(1, ?config(fixture_jwks_2, Config)),
        <<"rabbitmq2.configure:*/* rabbitmq2.write:*/* rabbitmq2.read:*/*">>,
        <<"rabbitmq2">>}
    ],
    [test_queue_declare(Config, Jwks, Scopes, Audience) ||
        {Jwks, Scopes, Audience} <- TestCases].


test_successful_connection_with_a_full_permission_token_and_explicitly_configured_vhost(Config) ->
    {_Algo, Token} = generate_valid_token(Config, [
                        <<"rabbitmq.configure:vhost1/*">>,
                        <<"rabbitmq.write:vhost1/*">>,
                        <<"rabbitmq.read:vhost1/*">>]),
    Conn     = open_unmanaged_connection(Config, 0, <<"vhost1">>, <<"username">>, 
                Token),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    #'queue.declare_ok'{queue = _} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
    close_connection_and_channel(Conn, Ch).

test_successful_connection_with_simple_strings_for_aud_and_scope(Config) ->
    {_Algo, Token} = generate_valid_token(
        Config,
        <<"rabbitmq.configure:*/* rabbitmq.write:*/* rabbitmq.read:*/*">>,
        [<<"hare">>, <<"rabbitmq">>]
    ),
    Conn     = open_unmanaged_connection(Config, 0, <<"username">>, Token),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    #'queue.declare_ok'{queue = _} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
    close_connection_and_channel(Conn, Ch).

test_successful_connection_with_complex_claim_as_a_map(Config) ->
    {_Algo, Token} = generate_valid_token_with_extra_fields(
        Config,
        #{<<"additional_rabbitmq_scopes">> => #{
            <<"rabbitmq">> => [
                                <<"configure:*/*">>,
                                <<"read:*/*">>, 
                                <<"write:*/*">>
            ]}
        }
    ),
    Conn     = open_unmanaged_connection(Config, 0, <<"username">>, Token),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    #'queue.declare_ok'{queue = _} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
    close_connection_and_channel(Conn, Ch).

test_successful_connection_with_complex_claim_as_a_list(Config) ->
    {_Algo, Token} = generate_valid_token_with_extra_fields(
        Config,
        #{<<"additional_rabbitmq_scopes">> => [
            <<"rabbitmq.configure:*/*">>, 
            <<"rabbitmq.read:*/*">>, 
            <<"rabbitmq.write:*/*">>
        ]}
    ),
    Conn     = open_unmanaged_connection(Config, 0, <<"username">>, Token),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    #'queue.declare_ok'{queue = _} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
    close_connection_and_channel(Conn, Ch).

test_successful_connection_with_complex_claim_as_a_binary(Config) ->
    {_Algo, Token} = generate_valid_token_with_extra_fields(
        Config,
        #{<<"additional_rabbitmq_scopes">> => 
            <<"rabbitmq.configure:*/* rabbitmq.read:*/* rabbitmq.write:*/*">>}
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
    {_Algo, Token} = generate_expirable_token(Config, [
            <<"rabbitmq.configure:vhost1/*">>,
            <<"rabbitmq.write:vhost1/*">>,
            <<"rabbitmq.read:vhost1/*">>
        ], Duration),
    Conn     = open_unmanaged_connection(Config, 0, <<"vhost1">>, 
                <<"username">>, Token),
    {ok, Ch} = amqp_connection:open_channel(Conn),

    {_Algo2, Token2} = generate_valid_token(Config, [
        <<"rabbitmq.configure:vhost1/*">>,
        <<"rabbitmq.write:vhost1/*">>,
        <<"rabbitmq.read:vhost1/*">>]),
    ?UTIL_MOD:wait_for_token_to_expire(timer:seconds(Duration)),
    ?assertEqual(ok, amqp_connection:update_secret(Conn, Token2, 
        <<"token refresh">>)),
    {ok, Ch2} = amqp_connection:open_channel(Conn),

    #'queue.declare_ok'{queue = _} = amqp_channel:call(Ch, 
                                        #'queue.declare'{exclusive = true}),
    #'queue.declare_ok'{queue = _} = amqp_channel:call(Ch2, 
                                        #'queue.declare'{exclusive = true}),

    amqp_channel:close(Ch2),
    close_connection_and_channel(Conn, Ch).

test_successful_connection_with_algorithm_restriction(Config) ->
    {_Algo, Token} = get_config(Config, fixture_jwt),
    Conn = open_unmanaged_connection(Config, 0, <<"username">>, Token),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    #'queue.declare_ok'{queue = _} = amqp_channel:call(Ch, 
                                        #'queue.declare'{exclusive = true}),
    close_connection_and_channel(Conn, Ch).

test_failed_connection_with_expired_token(Config) ->
    {_Algo, Token} = generate_expired_token(Config, [
        <<"rabbitmq.configure:vhost1/*">>,
        <<"rabbitmq.write:vhost1/*">>,
        <<"rabbitmq.read:vhost1/*">>]),
    ?assertMatch({error, {auth_failure, _}},
                 open_unmanaged_connection(Config, 0, <<"vhost1">>, 
                    <<"username">>, Token)).

test_failed_connection_with_a_non_token(Config) ->
    ?assertMatch({error, {auth_failure, _}},
                 open_unmanaged_connection(Config, 0, <<"vhost1">>, 
                    <<"username">>, <<"a-non-token-value">>)).

test_failed_connection_with_a_token_with_insufficient_vhost_permission(Config) ->
    {_Algo, Token} = generate_valid_token(Config, [
        <<"rabbitmq.configure:alt-vhost/*">>,
        <<"rabbitmq.write:alt-vhost/*">>,
        <<"rabbitmq.read:alt-vhost/*">>]),
    ?assertEqual({error, not_allowed},
                 open_unmanaged_connection(Config, 0, <<"off-limits-vhost">>, 
                    <<"username">>, Token)).

test_failed_connection_with_a_token_with_insufficient_resource_permission(Config) ->
    {_Algo, Token} = generate_valid_token(Config, [
        <<"rabbitmq.configure:vhost2/jwt*">>,
        <<"rabbitmq.write:vhost2/jwt*">>,
        <<"rabbitmq.read:vhost2/jwt*">>]),
    Conn     = open_unmanaged_connection(Config, 0, <<"vhost2">>, <<"username">>, 
                Token),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    ?assertExit({{shutdown, {server_initiated_close, 403, _}}, _},
       amqp_channel:call(Ch, #'queue.declare'{queue = <<"alt-prefix.eq.1">>, 
            exclusive = true})),
    close_connection(Conn).

test_failed_token_refresh_case1(Config) ->
    {_Algo, Token} = generate_valid_token(Config, [
        <<"rabbitmq.configure:vhost4/*">>,
        <<"rabbitmq.write:vhost4/*">>,
        <<"rabbitmq.read:vhost4/*">>]),
    Conn     = open_unmanaged_connection(Config, 0, <<"vhost4">>, <<"username">>, 
                Token),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    #'queue.declare_ok'{queue = _} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),

    {_Algo2, Token2} = generate_expired_token(Config, [
        <<"rabbitmq.configure:vhost4/*">>,
        <<"rabbitmq.write:vhost4/*">>,
        <<"rabbitmq.read:vhost4/*">>]),
    %% the error is communicated asynchronously via a connection-level error
    ?assertEqual(ok, amqp_connection:update_secret(Conn, Token2, 
        <<"token refresh">>)),

    {ok, Ch2} = amqp_connection:open_channel(Conn),
    ?assertExit({{shutdown, {server_initiated_close, 403, _}}, _},
       amqp_channel:call(Ch2, #'queue.declare'{queue = <<"a.q">>, exclusive = true})),

    close_connection(Conn).

test_failed_token_refresh_case2(Config) ->
    {_Algo, Token} = generate_valid_token(Config, [
        <<"rabbitmq.configure:vhost4/*">>,
        <<"rabbitmq.write:vhost4/*">>,
        <<"rabbitmq.read:vhost4/*">>]),
    Conn     = open_unmanaged_connection(Config, 0, <<"vhost4">>, 
                <<"username">>, Token),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    #'queue.declare_ok'{queue = _} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),

    %% the error is communicated asynchronously via a connection-level error
    ?assertEqual(ok, amqp_connection:update_secret(Conn, <<"not-a-token-^^^^5%">>, 
        <<"token refresh">>)),

    ?assertExit({{shutdown, {connection_closing, {server_initiated_close, 530, _}}}, _},
       amqp_connection:open_channel(Conn)),

    close_connection(Conn).

test_failed_connection_with_algorithm_restriction(Config) ->
    {_Algo, Token} = get_config(Config, fixture_jwt),
    ?assertMatch({error, {auth_failure, _}},
                 open_unmanaged_connection(Config, 0, <<"username">>, Token)).

%%% HELPERS
rpc_unset_env(Config, Par) ->
    rpc(Config, 0, application, unset_env,
        [rabbitmq_auth_backend_oauth2, Par]).
rpc_set_env(Config, Par, Val) ->
    rpc(Config, 0, application, set_env, 
        [rabbitmq_auth_backend_oauth2, Par, Val]).
rpc_get_env(Config, Par) ->
    rpc(Config, 0, application, get_env,
        [rabbitmq_auth_backend_oauth2, Par]).
rpc_get_env(Config, Par, Default) ->
    rpc(Config, 0, application, get_env,
        [rabbitmq_auth_backend_oauth2, Par, Default]).