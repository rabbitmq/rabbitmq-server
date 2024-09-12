%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_oauth2_oauth_provider_SUITE).

-compile(export_all).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("oauth2_client/include/oauth2_client.hrl").

-define(RABBITMQ,<<"rabbitmq">>).
-define(RABBITMQ_RESOURCE_ONE,<<"rabbitmq1">>).
-define(RABBITMQ_RESOURCE_TWO,<<"rabbitmq2">>).
-define(AUTH_PORT, 8000).

-import(rabbit_oauth2_oauth_provider, [
    get_internal_oauth_provider/2,
    add_signing_key/2, add_signing_key/3, replace_signing_keys/1,
    replace_signing_keys/2,
    get_signing_keys/0, get_signing_keys/1, get_signing_key/1, get_signing_key/2
]).
-import(oauth2_client, [get_oauth_provider/2]).

all() -> [
    {group, with_rabbitmq_node},
    {group, with_resource_server_id},
    {group, with_resource_servers}
].
groups() -> [
    {with_rabbitmq_node, [], [
        add_signing_keys_for_specific_oauth_provider,
        add_signing_keys_for_root_oauth_provider,

        replace_signing_keys_for_root_oauth_provider,
        replace_signing_keys_for_specific_oauth_provider,
        {with_root_static_signing_keys, [], [
            replace_merge_root_static_keys_with_newly_added_keys,
            replace_override_root_static_keys_with_newly_added_keys
        ]},
        {with_static_signing_keys_for_specific_oauth_provider, [], [
            replace_merge_static_keys_with_newly_added_keys,
            replace_override_static_keys_with_newly_added_keys
        ]}
    ]},
    {verify_oauth_provider_A, [], [
        internal_oauth_provider_A_has_no_default_key,
        {oauth_provider_A_with_default_key, [], [
            internal_oauth_provider_A_has_default_key
        ]},
        internal_oauth_provider_A_has_no_algorithms,
        {oauth_provider_A_with_algorithms, [], [
            internal_oauth_provider_A_has_algorithms
        ]},
        oauth_provider_A_with_jwks_uri_returns_error,
        {oauth_provider_A_with_jwks_uri, [], [
            oauth_provider_A_has_jwks_uri
        ]},
        {oauth_provider_A_with_issuer, [], [
            {oauth_provider_A_with_jwks_uri, [], [
                oauth_provider_A_has_jwks_uri
            ]},
            oauth_provider_A_has_to_discover_jwks_uri_endpoint
        ]}
    ]},
    {verify_oauth_provider_root, [], [
        internal_oauth_provider_root_has_no_default_key,
        {with_default_key, [], [
            internal_oauth_provider_root_has_default_key
        ]},
        internal_oauth_provider_root_has_no_algorithms,
        {with_algorithms, [], [
            internal_oauth_provider_root_has_algorithms
        ]},
        oauth_provider_root_with_jwks_uri_returns_error,
        {with_jwks_uri, [], [
            oauth_provider_root_has_jwks_uri
        ]},
        {with_issuer, [], [
            {with_jwks_uri, [], [
                oauth_provider_root_has_jwks_uri
            ]},
            oauth_provider_root_has_to_discover_jwks_uri_endpoint
        ]}
    ]}
].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(with_rabbitmq_node, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, with_rabbitmq_node},
        {rmq_nodes_count, 1}
    ]),
    rabbit_ct_helpers:run_steps(Config1, rabbit_ct_broker_helpers:setup_steps());
init_per_group(with_default_key, Config) ->
    KeyConfig = get_env(key_config, []),
    set_env(key_config, proplists:delete(default_key, KeyConfig) ++
        [{default_key,<<"default-key">>}]),
    Config;
init_per_group(with_root_static_signing_keys, Config) ->
    KeyConfig = call_get_env(Config, key_config, []),
    SigningKeys = #{
        <<"mykey-root-1">> => <<"some key root-1">>,
        <<"mykey-root-2">> => <<"some key root-2">>
    },
    call_set_env(Config, key_config,
        proplists:delete(default_key, KeyConfig) ++ [{signing_keys,SigningKeys}]),
    Config;
init_per_group(with_static_signing_keys_for_specific_oauth_provider, Config) ->
    OAuthProviders = call_get_env(Config, oauth_providers, #{}),
    OAuthProvider = maps:get(<<"A">>, OAuthProviders, []),
    SigningKeys = #{
        <<"mykey-root-1">> => <<"some key root-1">>,
        <<"mykey-root-2">> => <<"some key root-2">>
    },
    OAuthProvider1 = proplists:delete(signing_keys, OAuthProvider) ++
        [{signing_keys, SigningKeys}],

    call_set_env(Config, oauth_providers, maps:put(<<"A">>, OAuthProvider1,
        OAuthProviders)),
    Config;


init_per_group(with_jwks_url, Config) ->
    KeyConfig = get_env(key_config, []),
    set_env(key_config, KeyConfig ++ [{jwks_url,build_url_to_oauth_provider(<<"/keys">>)}]),
    [{key_config_before_group_with_jwks_url, KeyConfig} | Config];

init_per_group(with_issuer, Config) ->
    {ok, _} = application:ensure_all_started(inets),
    {ok, _} = application:ensure_all_started(ssl),
    application:ensure_all_started(cowboy),
    CertsDir = ?config(rmq_certsdir, Config),
    CaCertFile = filename:join([CertsDir, "testca", "cacert.pem"]),
    SslOptions = ssl_options(verify_peer, false, CaCertFile),

    HttpOauthServerExpectations = get_openid_configuration_expectations(),
    ListOfExpectations = maps:values(proplists:to_map(HttpOauthServerExpectations)),

    start_https_oauth_server(?AUTH_PORT, CertsDir, ListOfExpectations),
    set_env(use_global_locks, false),
    set_env(issuer,
        build_url_to_oauth_provider(<<"/">>)),
    KeyConfig = get_env(key_config, []),
    set_env(key_config,
        KeyConfig ++ SslOptions),

    [{key_config_before_group_with_issuer, KeyConfig},
        {ssl_options, SslOptions} | Config];

init_per_group(with_oauth_providers_A_with_jwks_uri, Config) ->
    set_env(oauth_providers,
        #{<<"A">> => [
            {issuer, build_url_to_oauth_provider(<<"/A">>) },
            {jwks_uri,build_url_to_oauth_provider(<<"/A/keys">>) }
        ] } ),
    Config;

init_per_group(with_oauth_providers_A_with_issuer, Config) ->
    set_env(oauth_providers,
        #{<<"A">> => [
            {issuer, build_url_to_oauth_provider(<<"/A">>) },
            {https, ?config(ssl_options, Config)}
        ] } ),
    Config;

init_per_group(with_oauth_providers_A_B_with_jwks_uri, Config) ->
    set_env(oauth_providers,
        #{ <<"A">> => [
            {issuer, build_url_to_oauth_provider(<<"/A">>) },
            {jwks_uri, build_url_to_oauth_provider(<<"/A/keys">>)}
        ],
        <<"B">> => [
            {issuer, build_url_to_oauth_provider(<<"/B">>) },
            {jwks_uri, build_url_to_oauth_provider(<<"/B/keys">>)}
        ] }),
    Config;

init_per_group(with_oauth_providers_A_B_with_issuer, Config) ->
    set_env(oauth_providers,
        #{ <<"A">> => [
            {issuer, build_url_to_oauth_provider(<<"/A">>) },
            {https, ?config(ssl_options, Config)}
            ],
        <<"B">> => [
            {issuer, build_url_to_oauth_provider(<<"/B">>) },
            {https, ?config(ssl_options, Config)}
        ] }),
    Config;

init_per_group(with_default_oauth_provider_A, Config) ->
    set_env(default_oauth_provider, <<"A">>),
    Config;

init_per_group(with_default_oauth_provider_B, Config) ->
    set_env(default_oauth_provider, <<"B">>),
    Config;

init_per_group(with_resource_server_id, Config) ->
    set_env(resource_server_id, ?RABBITMQ),
    Config;

init_per_group(with_algorithms, Config) ->
    KeyConfig = get_env(key_config, []),
    set_env(key_config, KeyConfig ++ [{algorithms, [<<"HS256">>, <<"RS256">>]}]),
    [{algorithms, [<<"HS256">>, <<"RS256">>]} | Config];

init_per_group(with_algorithms_for_provider_A, Config) ->
    OAuthProviders = get_env(oauth_providers, #{}),
    OAuthProvider = maps:get(<<"A">>, OAuthProviders, []),
    set_env(oauth_providers, maps:put(<<"A">>,
        [{algorithms, [<<"HS256">>, <<"RS256">>]} | OAuthProvider], OAuthProviders)),
    [{algorithms, [<<"HS256">>, <<"RS256">>]} | Config];

init_per_group(with_different_oauth_provider_for_each_resource, Config) ->
    {ok, ResourceServers} = get_env(resource_servers),
    Rabbit1 = maps:get(?RABBITMQ_RESOURCE_ONE, ResourceServers) ++
        [ {oauth_provider_id, <<"A">>} ],
    Rabbit2 = maps:get(?RABBITMQ_RESOURCE_TWO, ResourceServers) ++
        [ {oauth_provider_id, <<"B">>} ],
    ResourceServers1 = maps:update(?RABBITMQ_RESOURCE_ONE, Rabbit1, ResourceServers),
    set_env(resource_servers, maps:update(?RABBITMQ_RESOURCE_TWO, Rabbit2,
        ResourceServers1)),
    Config;

init_per_group(with_resource_servers, Config) ->
    set_env(resource_servers,
        #{?RABBITMQ_RESOURCE_ONE =>  [
            { key_config, [
                {jwks_url,<<"https://oauth-for-rabbitmq1">> }
            ]}
        ],
        ?RABBITMQ_RESOURCE_TWO =>  [
            { key_config, [
                {jwks_url,<<"https://oauth-for-rabbitmq2">> }
            ]}
        ],
        <<"0">> => [ {id, <<"rabbitmq-0">> } ],
        <<"1">> => [ {id, <<"rabbitmq-1">> } ]

      }),
  Config;

init_per_group(verify_oauth_provider_A, Config) ->
    set_env(oauth_providers,
        #{ <<"A">> => [
            {id, <<"A">>}
            ]
        ] }),
    Config;

init_per_group(_any, Config) ->
    Config.

end_per_group(with_rabbitmq_node, Config) ->
    rabbit_ct_helpers:run_steps(Config, rabbit_ct_broker_helpers:teardown_steps());

end_per_group(with_root_static_signing_keys, Config) ->
    KeyConfig = call_get_env(Config, key_config, []),
    call_set_env(Config, key_config, KeyConfig),
    Config;

end_per_group(with_resource_server_id, Config) ->
    unset_env(resource_server_id),
    Config;

end_per_group(with_verify_aud_false, Config) ->
    unset_env(verify_aud),
    Config;

end_per_group(with_verify_aud_false_for_resource_two, Config) ->
    ResourceServers = get_env(resource_servers, #{}),
    Proplist = maps:get(?RABBITMQ_RESOURCE_TWO, ResourceServers, []),
    set_env(resource_servers, maps:put(?RABBITMQ_RESOURCE_TWO,
        proplists:delete(verify_aud, Proplist), ResourceServers)),
    Config;

end_per_group(with_empty_scope_prefix_for_resource_one, Config) ->
    ResourceServers = get_env(resource_servers, #{}),
    Proplist = maps:get(?RABBITMQ_RESOURCE_ONE, ResourceServers, []),
    set_env(resource_servers, maps:put(?RABBITMQ_RESOURCE_ONE,
        proplists:delete(scope_prefix, Proplist), ResourceServers)),
    Config;

end_per_group(with_default_key, Config) ->
    KeyConfig = get_env(key_config, []),
    set_env(key_config, proplists:delete(default_key, KeyConfig)),
    Config;

end_per_group(with_algorithms, Config) ->
    KeyConfig = get_env(key_config, []),
    set_env(key_config, proplists:delete(algorithms, KeyConfig)),
    Config;

end_per_group(with_algorithms_for_provider_A, Config) ->
    OAuthProviders = get_env(oauth_providers, #{}),
    OAuthProvider = maps:get(<<"A">>, OAuthProviders, []),
    set_env(oauth_providers, maps:put(<<"A">>,
        proplists:delete(algorithms, OAuthProvider), OAuthProviders)),
    Config;


end_per_group(with_jwks_url, Config) ->
    KeyConfig = ?config(key_config_before_group_with_jwks_url, Config),
    set_env(key_config, KeyConfig),
    Config;

end_per_group(with_issuer, Config) ->
    KeyConfig = ?config(key_config_before_group_with_issuer, Config),
    unset_env(issuer),
    set_env(key_config, KeyConfig),
    stop_http_auth_server(),
    Config;

end_per_group(with_oauth_providers_A_with_jwks_uri, Config) ->
    unset_env(oauth_providers),
    Config;

end_per_group(with_oauth_providers_A_with_issuer, Config) ->
    unset_env(oauth_providers),
    Config;

end_per_group(with_oauth_providers_A_B_with_jwks_uri, Config) ->
    unset_env(oauth_providers),
    Config;

end_per_group(with_oauth_providers_A_B_with_issuer, Config) ->
    unset_env(oauth_providers),
    Config;

end_per_group(with_oauth_providers_A, Config) ->
    unset_env(oauth_providers),
    Config;

end_per_group(with_oauth_providers_A_B, Config) ->
    unset_env(oauth_providers),
    Config;

end_per_group(with_default_oauth_provider_B, Config) ->
    unset_env(default_oauth_provider),
    Config;

end_per_group(with_default_oauth_provider_A, Config) ->
    unset_env(default_oauth_provider),
    Config;

end_per_group(get_oauth_provider_for_resource_server_id, Config) ->
    unset_env(resource_server_id),
    Config;

end_per_group(with_resource_servers_and_resource_server_id, Config) ->
    unset_env(resource_server_id),
    Config;

end_per_group(with_resource_servers, Config) ->
    unset_env(resource_servers),
    Config;

end_per_group(with_root_scope_prefix, Config) ->
    unset_env(scope_prefix),
    Config;

end_per_group(_any, Config) ->
    Config.

%% ----- Utility functions

call_set_env(Config, Par, Value) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
        [rabbitmq_auth_backend_oauth2, Par, Value]).

call_get_env(Config, Par, Def) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, application, get_env,
        [rabbitmq_auth_backend_oauth2, Par, Def]).

call_add_signing_key(Config, Args) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_oauth2_config, add_signing_key, Args).

call_get_signing_keys(Config, Args) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_oauth2_config, get_signing_keys, Args).

call_get_signing_keys(Config) ->
    call_get_signing_keys(Config, []).

call_get_signing_key(Config, Args) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_oauth2_config, get_signing_key, Args).

call_add_signing_keys(Config, Args) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_oauth2_config, add_signing_keys, Args).

call_replace_signing_keys(Config, Args) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_oauth2_config, replace_signing_keys, Args).

%% ----- Test cases

add_signing_keys_for_root_oauth_provider(Config) ->
    #{<<"mykey-1">> := <<"some key 1">>} =
        call_add_signing_key(Config, [<<"mykey-1">>, <<"some key 1">>]),
    #{<<"mykey-1">> := <<"some key 1">>} =
        call_get_signing_keys(Config),

    #{<<"mykey-1">> := <<"some key 1">>, <<"mykey-2">> := <<"some key 2">>} =
        call_add_signing_key(Config, [<<"mykey-2">>, <<"some key 2">>]),
    #{<<"mykey-1">> := <<"some key 1">>, <<"mykey-2">> := <<"some key 2">>} =
        call_get_signing_keys(Config),

    ?assertEqual(<<"some key 1">>,
        call_get_signing_key(Config, [<<"mykey-1">>])).

add_signing_keys_for_specific_oauth_provider(Config) ->
    #{<<"mykey-3-1">> := <<"some key 3-1">>} =
        call_add_signing_key(Config,
            [<<"mykey-3-1">>, <<"some key 3-1">>, <<"my-oauth-provider-3">>]),
    #{<<"mykey-4-1">> := <<"some key 4-1">>} =
        call_add_signing_key(Config,
            [<<"mykey-4-1">>, <<"some key 4-1">>, <<"my-oauth-provider-4">>]),
    #{<<"mykey-3-1">> := <<"some key 3-1">>} =
        call_get_signing_keys(Config, [<<"my-oauth-provider-3">>]),
    #{<<"mykey-4-1">> := <<"some key 4-1">>} =
        call_get_signing_keys(Config, [<<"my-oauth-provider-4">>]),

    #{<<"mykey-3-1">> := <<"some key 3-1">>,
      <<"mykey-3-2">> := <<"some key 3-2">>} =
        call_add_signing_key(Config, [
            <<"mykey-3-2">>, <<"some key 3-2">>, <<"my-oauth-provider-3">>]),

    #{<<"mykey-1">> := <<"some key 1">>} =
        call_add_signing_key(Config, [<<"mykey-1">>, <<"some key 1">>]),
    #{<<"mykey-1">> := <<"some key 1">>} =
        call_get_signing_keys(Config, []),

    ?assertEqual(<<"some key 3-1">>,
        call_get_signing_key(Config, [<<"mykey-3-1">> , <<"my-oauth-provider-3">>])).

replace_merge_root_static_keys_with_newly_added_keys(Config) ->
    NewKeys = #{<<"key-2">> => <<"some key 2">>, <<"key-3">> => <<"some key 3">>},
    call_replace_signing_keys(Config, [NewKeys]),
    #{  <<"mykey-root-1">> := <<"some key root-1">>,
        <<"mykey-root-2">> := <<"some key root-2">>,
        <<"key-2">> := <<"some key 2">>,
        <<"key-3">> := <<"some key 3">>
    } = call_get_signing_keys(Config).

replace_merge_static_keys_with_newly_added_keys(Config) ->
    NewKeys = #{<<"key-2">> => <<"some key 2">>, <<"key-3">> => <<"some key 3">>},
    call_replace_signing_keys(Config, [NewKeys, <<"A">>]),
    #{  <<"mykey-root-1">> := <<"some key root-1">>,
        <<"mykey-root-2">> := <<"some key root-2">>,
        <<"key-2">> := <<"some key 2">>,
        <<"key-3">> := <<"some key 3">>
    } = call_get_signing_keys(Config, [<<"A">>]).

replace_override_root_static_keys_with_newly_added_keys(Config) ->
    NewKeys = #{<<"mykey-root-1">> => <<"new key root-1">>,
        <<"key-3">> => <<"some key 3">>},
    call_replace_signing_keys(Config, [NewKeys]),
    #{  <<"mykey-root-1">> := <<"new key root-1">>,
        <<"mykey-root-2">> := <<"some key root-2">>,
        <<"key-3">> := <<"some key 3">>
    } = call_get_signing_keys(Config).
replace_override_static_keys_with_newly_added_keys(Config) ->
    NewKeys = #{<<"mykey-root-1">> => <<"new key root-1">>,
        <<"key-3">> => <<"some key 3">>},
    call_replace_signing_keys(Config, [NewKeys, <<"A">>]),
    #{  <<"mykey-root-1">> := <<"new key root-1">>,
        <<"mykey-root-2">> := <<"some key root-2">>,
        <<"key-3">> := <<"some key 3">>
    } = call_get_signing_keys(Config, [<<"A">>]).

replace_signing_keys_for_root_oauth_provider(Config) ->
    call_add_signing_key(Config, [<<"mykey-1">>, <<"some key 1">>]),
    NewKeys = #{<<"key-2">> => <<"some key 2">>, <<"key-3">> => <<"some key 3">>},
    call_replace_signing_keys(Config, [NewKeys]),
    #{<<"key-2">> := <<"some key 2">>, <<"key-3">> := <<"some key 3">>} =
        call_get_signing_keys(Config).

replace_signing_keys_for_specific_oauth_provider(Config) ->
    OAuthProviderId = <<"my-oauth-provider-3">>,
    #{<<"mykey-3-1">> := <<"some key 3-1">>} =
        call_add_signing_key(Config,
            [<<"mykey-3-1">>, <<"some key 3-1">>, OAuthProviderId]),
    NewKeys = #{<<"key-2">> => <<"some key 2">>,
                <<"key-3">> => <<"some key 3">>},
    call_replace_signing_keys(Config, [NewKeys, OAuthProviderId]),
    #{<<"key-2">> := <<"some key 2">>, <<"key-3">> := <<"some key 3">>} =
        call_get_signing_keys(Config, [OAuthProviderId]).


get_algorithms_should_return_undefined(_Config) ->
    OAuthProvider = get_internal_oauth_provider(),
    undefined = OAuthProvider#internal_oauth_provider.algorithms.

get_algorithms(Config) ->
    OAuthProvider = get_internal_oauth_provider(),
    Algorithms = OAuthProvider#internal_oauth_provider.algorithms,
    ?assertEqual(?config(algorithms, Config), Algorithms).

get_algorithms_for_provider_A_should_return_undefined(_Config) ->
    OAuthProvider = get_internal_oauth_provider(<<"A">>),
    undefined = OAuthProvider#internal_oauth_provider.algorithms.

get_algorithms_for_provider_A(Config) ->
    OAuthProvider = get_internal_oauth_provider(<<"A">>),
    Algorithms = OAuthProvider#internal_oauth_provider.algorithms,
    ?assertEqual(?config(algorithms, Config), Algorithms).

get_oauth_provider_root_with_jwks_uri_should_fail(_Config) ->
    {error, _Message} = get_oauth_provider(root, [jwks_uri]).

get_oauth_provider_A_with_jwks_uri_should_fail(_Config) ->
    {error, _Message} = get_oauth_provider(<<"A">>, [jwks_uri]).

get_oauth_provider_should_return_root_oauth_provider_with_jwks_uri(_Config) ->
    {ok, OAuthProvider} = get_oauth_provider(root, [jwks_uri]),
    ?assertEqual(build_url_to_oauth_provider(<<"/keys">>), OAuthProvider#oauth_provider.jwks_uri).

get_oauth_provider_for_both_resources_should_return_root_oauth_provider(_Config) ->
    {ok, OAuthProvider} = get_oauth_provider(root, [jwks_uri]),
    ?assertEqual(build_url_to_oauth_provider(<<"/keys">>), OAuthProvider#oauth_provider.jwks_uri).

get_oauth_provider_for_resource_one_should_return_oauth_provider_A(_Config) ->
    {ok, OAuthProvider} = get_oauth_provider(<<"A">>, [jwks_uri]),
    ?assertEqual(build_url_to_oauth_provider(<<"/A/keys">>), OAuthProvider#oauth_provider.jwks_uri).

get_oauth_provider_for_both_resources_should_return_oauth_provider_A(_Config) ->
    {ok, OAuthProvider} = get_oauth_provider(<<"A">>, [jwks_uri]),
    ?assertEqual(build_url_to_oauth_provider(<<"/A/keys">>), OAuthProvider#oauth_provider.jwks_uri).

get_oauth_provider_for_resource_two_should_return_oauth_provider_B(_Config) ->
    {ok, OAuthProvider} = get_oauth_provider(<<"B">>, [jwks_uri]),
    ?assertEqual(build_url_to_oauth_provider(<<"/B/keys">>), OAuthProvider#oauth_provider.jwks_uri).

get_oauth_provider_should_return_root_oauth_provider_with_all_discovered_endpoints(_Config) ->
    {ok, OAuthProvider} = get_oauth_provider(root, [jwks_uri]),
    ?assertEqual(build_url_to_oauth_provider(<<"/keys">>), OAuthProvider#oauth_provider.jwks_uri),
    ?assertEqual(build_url_to_oauth_provider(<<"/">>), OAuthProvider#oauth_provider.issuer).

append_paths(Path1, Path2) ->
    erlang:iolist_to_binary([Path1, Path2]).

get_oauth_provider_should_return_oauth_provider_B_with_jwks_uri(_Config) ->
    {ok, OAuthProvider} = get_oauth_provider(<<"B">>, [jwks_uri]),
    ?assertEqual(build_url_to_oauth_provider(<<"/B/keys">>), OAuthProvider#oauth_provider.jwks_uri).

get_oauth_provider_should_return_oauth_provider_B_with_all_discovered_endpoints(_Config) ->
    {ok, OAuthProvider} = get_oauth_provider(<<"B">>, [jwks_uri]),
    ?assertEqual(build_url_to_oauth_provider(<<"/B/keys">>), OAuthProvider#oauth_provider.jwks_uri),
    ?assertEqual(build_url_to_oauth_provider(<<"/B">>), OAuthProvider#oauth_provider.issuer).

get_oauth_provider_should_return_oauth_provider_A_with_jwks_uri(_Config) ->
    {ok, OAuthProvider} = get_oauth_provider(<<"A">>, [jwks_uri]),
    ?assertEqual(build_url_to_oauth_provider(<<"/A/keys">>), OAuthProvider#oauth_provider.jwks_uri).

get_oauth_provider_should_return_oauth_provider_A_with_all_discovered_endpoints(_Config) ->
    {ok, OAuthProvider} = get_oauth_provider(<<"A">>, [jwks_uri]),
    ?assertEqual(build_url_to_oauth_provider(<<"/A/keys">>), OAuthProvider#oauth_provider.jwks_uri),
    ?assertEqual(build_url_to_oauth_provider(<<"/A">>), OAuthProvider#oauth_provider.issuer).

%% ---- Utility functions

get_env(Par) ->
    application:get_env(rabbitmq_auth_backend_oauth2, Par).
get_env(Par, Def) ->
    application:get_env(rabbitmq_auth_backend_oauth2, Par, Def).
set_env(Par, Val) ->
    application:set_env(rabbitmq_auth_backend_oauth2, Par, Val).
unset_env(Par) ->
    application:unset_env(rabbitmq_auth_backend_oauth2, Par).

get_openid_configuration_expectations() ->
   [ {get_root_openid_configuration,

      #{request => #{
         method => <<"GET">>,
         path => <<"/.well-known/openid-configuration">>
       },
       response => [
         {code, 200},
         {content_type, ?CONTENT_JSON},
         {payload, [
           {issuer, build_url_to_oauth_provider(<<"/">>) },
           {jwks_uri, build_url_to_oauth_provider(<<"/keys">>)}
         ]}
       ]
     }
     },
   {get_A_openid_configuration,

      #{request => #{
         method => <<"GET">>,
         path => <<"/A/.well-known/openid-configuration">>
       },
       response => [
         {code, 200},
         {content_type, ?CONTENT_JSON},
         {payload, [
           {issuer, build_url_to_oauth_provider(<<"/A">>) },
           {jwks_uri, build_url_to_oauth_provider(<<"/A/keys">>)}
         ]}
       ]
     }
     },
    {get_B_openid_configuration,

       #{request => #{
          method => <<"GET">>,
          path => <<"/B/.well-known/openid-configuration">>
        },
        response => [
          {code, 200},
          {content_type, ?CONTENT_JSON},
          {payload, [
            {issuer, build_url_to_oauth_provider(<<"/B">>) },
            {jwks_uri, build_url_to_oauth_provider(<<"/B/keys">>)}
          ]}
        ]
      }
      }
  ].

start_https_oauth_server(Port, CertsDir, Expectations) when is_list(Expectations) ->
    Dispatch = cowboy_router:compile([
        {'_', [{Path, oauth2_http_mock, Expected} || #{request := #{path := Path}} = Expected <- Expectations ]}
        ]),
    ct:log("start_https_oauth_server (port:~p) with expectation list : ~p -> dispatch: ~p", [Port, Expectations, Dispatch]),
    {ok, Pid} = cowboy:start_tls(
        mock_http_auth_listener,
        [{port, Port},
         {certfile, filename:join([CertsDir, "server", "cert.pem"])},
         {keyfile, filename:join([CertsDir, "server", "key.pem"])}
        ],
        #{env => #{dispatch => Dispatch}}),
    ct:log("Started on Port ~p and pid ~p", [ranch:get_port(mock_http_auth_listener), Pid]).

build_url_to_oauth_provider(Path) ->
    uri_string:recompose(#{scheme => "https",
                         host => "localhost",
                         port => rabbit_data_coercion:to_integer(?AUTH_PORT),
                         path => Path}).

stop_http_auth_server() ->
    cowboy:stop_listener(mock_http_auth_listener).

-spec ssl_options(ssl:verify_type(), boolean(), file:filename()) -> list().
ssl_options(PeerVerification, FailIfNoPeerCert, CaCertFile) ->
    [{verify, PeerVerification},
    {depth, 10},
    {fail_if_no_peer_cert, FailIfNoPeerCert},
    {crl_check, false},
    {crl_cache, {ssl_crl_cache, {internal, [{http, 10000}]}}},
    {cacertfile, CaCertFile}].
