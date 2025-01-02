%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_oauth2_config_SUITE).

-compile(export_all).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("oauth2_client/include/oauth2_client.hrl").

-define(RABBITMQ,<<"rabbitmq">>).
-define(RABBITMQ_RESOURCE_ONE,<<"rabbitmq1">>).
-define(RABBITMQ_RESOURCE_TWO,<<"rabbitmq2">>).
-define(AUTH_PORT, 8000).


all() -> [
    {group, with_rabbitmq_node},
    {group, with_resource_server_id},
    {group, without_resource_server_id},
    {group, with_resource_servers},
    {group, with_resource_servers_and_resource_server_id},
    {group, inheritance_group}
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
    {with_resource_server_id, [], [
        get_default_resource_server_id,
        get_allowed_resource_server_ids_returns_resource_server_id,
        get_resource_server_id_for_rabbit_audience_returns_rabbit,
        get_resource_server_id_for_none_audience_should_fail,
        get_resource_server_id_for_unknown_audience_should_fail,
        {with_verify_aud_false, [], [
            get_resource_server_id_for_rabbit_audience_returns_rabbit,
            get_resource_server_id_for_none_audience_returns_rabbit,
            get_resource_server_id_for_unknown_audience_returns_rabbit
        ]},
        find_audience_in_resource_server_ids_found_resource_server_id,
        get_oauth_provider_root_with_jwks_uri_should_fail,
        get_default_key_should_fail,
        {with_default_key, [], [
            get_default_key
        ]},
        {with_static_signing_keys, [], [
            get_signing_keys
        ]},
        {with_static_signing_keys_for_oauth_provider_A, [], [
            get_signing_keys_for_oauth_provider_A
        ]},
        get_algorithms_should_return_undefined,
        {with_algorithms, [], [
            get_algorithms
        ]},
        {with_jwks_url, [], [
            get_oauth_provider_should_return_root_oauth_provider_with_jwks_uri,
            {with_oauth_providers_A_with_jwks_uri, [], [
                get_oauth_provider_should_return_root_oauth_provider_with_jwks_uri,
                {with_default_oauth_provider_A, [], [
                    get_oauth_provider_should_return_oauth_provider_A_with_jwks_uri
                ]}
            ]},
            {with_oauth_providers_A_B_with_jwks_uri, [], [
                get_default_key_for_provider_A_should_fail,
                {with_default_key, [], [
                    get_default_key_for_provider_A_should_fail
                ]},
                {with_default_key_for_provider_A, [], [
                    get_default_key_for_provider_A
                ]},
                get_algorithms_for_provider_A_should_return_undefined,
                {with_algorithms_for_provider_A, [], [
                    get_algorithms_for_provider_A
                ]},
                get_oauth_provider_should_return_root_oauth_provider_with_jwks_uri,
                {with_default_oauth_provider_B, [], [
                    get_oauth_provider_should_return_oauth_provider_B_with_jwks_uri
                ]}
            ]}
        ]},
        {with_oauth_providers_A_with_jwks_uri, [], [
            get_oauth_provider_root_with_jwks_uri_should_fail,
            {with_default_oauth_provider_A, [], [
                get_oauth_provider_should_return_oauth_provider_A_with_jwks_uri
            ]}
        ]},
        {with_issuer, [], [
            get_oauth_provider_should_return_root_oauth_provider_with_all_discovered_endpoints,
            {with_oauth_providers_A_with_issuer, [], [
                get_oauth_provider_should_return_root_oauth_provider_with_all_discovered_endpoints,
                {with_default_oauth_provider_A, [], [
                    get_oauth_provider_should_return_oauth_provider_A_with_all_discovered_endpoints
                ]}
            ]},
            {with_oauth_providers_A_B_with_issuer, [], [
                get_oauth_provider_should_return_root_oauth_provider_with_all_discovered_endpoints,
                {with_default_oauth_provider_B, [], [
                    get_oauth_provider_should_return_oauth_provider_B_with_all_discovered_endpoints
                ]}
            ]}
        ]}
    ]},
    {without_resource_server_id, [], [
        get_default_resource_server_id_returns_error,
        get_allowed_resource_server_ids_returns_empty_list
    ]},
    {with_resource_servers, [], [
        get_allowed_resource_server_ids_returns_resource_servers_ids,
        find_audience_in_resource_server_ids_found_one_resource_servers,
        index_resource_servers_by_id_else_by_key,
        is_verify_aud_for_resource_two_returns_true,
        {with_verify_aud_false_for_resource_two, [], [
            is_verify_aud_for_resource_one_returns_true,
            is_verify_aud_for_resource_two_returns_false
        ]},
        get_scope_prefix_for_resource_one_returns_default_scope_prefix,
        {with_root_scope_prefix, [], [
            get_scope_prefix_for_resource_one_returns_root_scope_prefix,
            {with_empty_scope_prefix_for_resource_one, [], [
                get_scope_prefix_for_resource_one_returns_empty_scope_prefix,
                get_scope_prefix_for_resource_two_returns_root_scope_prefix
            ]}
        ]},
        {with_jwks_url, [], [
            get_oauth_provider_for_both_resources_should_return_root_oauth_provider,
            {with_oauth_providers_A_with_jwks_uri, [], [
                {with_default_oauth_provider_A, [], [
                    get_oauth_provider_for_both_resources_should_return_oauth_provider_A
                ]}
            ]},
            {with_different_oauth_provider_for_each_resource, [], [
                {with_oauth_providers_A_B_with_jwks_uri, [], [
                    get_oauth_provider_for_resource_one_should_return_oauth_provider_A,
                    get_oauth_provider_for_resource_two_should_return_oauth_provider_B
                ]}
            ]}
        ]}
    ]},
    {with_resource_servers_and_resource_server_id, [], [
        get_allowed_resource_server_ids_returns_all_resource_servers_ids,
        find_audience_in_resource_server_ids_found_resource_server_id,
        find_audience_in_resource_server_ids_found_one_resource_servers,
        find_audience_in_resource_server_ids_using_binary_audience
    ]},

    {inheritance_group, [], [
        get_additional_scopes_key,
        get_additional_scopes_key_when_not_defined,
        is_verify_aud,
        is_verify_aud_when_is_false,
        get_default_preferred_username_claims,
        get_preferred_username_claims,
        get_scope_prefix,
        get_empty_scope_prefix,
        get_scope_prefix_when_not_defined,
        get_resource_server_type,
        get_resource_server_type_when_not_defined,
        has_scope_aliases,
        has_scope_aliases_when_not_defined,
        get_scope_aliases
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
    KeyConfig = application:get_env(rabbitmq_auth_backend_oauth2, key_config, []),
    application:set_env(rabbitmq_auth_backend_oauth2, key_config,
        proplists:delete(default_key, KeyConfig) ++ [{default_key,<<"default-key">>}]),
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
    OAuthProvider1 = proplists:delete(signing_keys, OAuthProvider) ++ [{signing_keys, SigningKeys}],

    call_set_env(Config, oauth_providers, maps:put(<<"A">>, OAuthProvider1, OAuthProviders)),
    Config;

init_per_group(with_default_key_for_provider_A, Config) ->
    OAuthProviders = application:get_env(rabbitmq_auth_backend_oauth2, oauth_providers, #{}),
    OAuthProvider = maps:get(<<"A">>, OAuthProviders, []),
    application:set_env(rabbitmq_auth_backend_oauth2, oauth_providers, maps:put(<<"A">>,
        proplists:delete(default_key, OAuthProvider) ++ [{default_key,<<"A-default-key">>}],
        OAuthProviders)),
    Config;
init_per_group(with_static_signing_keys, Config) ->
    KeyConfig = application:get_env(rabbitmq_auth_backend_oauth2, key_config, []),
    SigningKeys = #{<<"mykey-1-1">> => <<"some key 1-1">>,
                    <<"mykey-1-2">> => <<"some key 1-2">>},
    application:set_env(rabbitmq_auth_backend_oauth2, key_config,
        proplists:delete(signing_keys, KeyConfig) ++ [{signing_keys, SigningKeys}]),
    Config;
init_per_group(with_static_signing_keys_for_oauth_provider_A, Config) ->
    OAuthProviders = application:get_env(rabbitmq_auth_backend_oauth2, oauth_providers, #{}),
    OAuthProvider = maps:get(<<"A">>, OAuthProviders, []),
    SigningKeys = #{<<"A-mykey-1-1">> => <<"A-some key 1-1">>,
                    <<"A-mykey-1-2">> => <<"A-some key 1-2">>},

    OAuthProvider0 = proplists:delete(signing_keys, OAuthProvider) ++
        [{signing_keys, SigningKeys}],
    application:set_env(rabbitmq_auth_backend_oauth2, oauth_providers,
        maps:put(<<"A">>, OAuthProvider0, OAuthProviders)),
    Config;

init_per_group(with_jwks_url, Config) ->
    KeyConfig = application:get_env(rabbitmq_auth_backend_oauth2, key_config, []),
    application:set_env(rabbitmq_auth_backend_oauth2, key_config, KeyConfig
        ++ [{jwks_url,build_url_to_oauth_provider(<<"/keys">>)}]),
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
    application:set_env(rabbitmq_auth_backend_oauth2, use_global_locks, false),
    application:set_env(rabbitmq_auth_backend_oauth2, issuer, build_url_to_oauth_provider(<<"/">>)),
    KeyConfig = application:get_env(rabbitmq_auth_backend_oauth2, key_config, []),
    application:set_env(rabbitmq_auth_backend_oauth2, key_config, KeyConfig ++ SslOptions),

    [{key_config_before_group_with_issuer, KeyConfig}, {ssl_options, SslOptions} | Config];

init_per_group(with_oauth_providers_A_with_jwks_uri, Config) ->
    application:set_env(rabbitmq_auth_backend_oauth2, oauth_providers,
    #{<<"A">> => [
        {issuer,build_url_to_oauth_provider(<<"/A">>) },
        {jwks_uri,build_url_to_oauth_provider(<<"/A/keys">>) }
    ] } ),
    Config;
init_per_group(with_oauth_providers_A_with_issuer, Config) ->
    application:set_env(rabbitmq_auth_backend_oauth2, oauth_providers,
    #{<<"A">> => [
        {issuer,build_url_to_oauth_provider(<<"/A">>) },
        {https, ?config(ssl_options, Config)}
    ] } ),
    Config;
init_per_group(with_oauth_providers_A_B_with_jwks_uri, Config) ->
    application:set_env(rabbitmq_auth_backend_oauth2, oauth_providers,
    #{ <<"A">> => [
        {issuer,build_url_to_oauth_provider(<<"/A">>) },
        {jwks_uri,build_url_to_oauth_provider(<<"/A/keys">>)}
    ],
    <<"B">> => [
        {issuer,build_url_to_oauth_provider(<<"/B">>) },
        {jwks_uri,build_url_to_oauth_provider(<<"/B/keys">>)}
    ] }),
    Config;
init_per_group(with_oauth_providers_A_B_with_issuer, Config) ->
    application:set_env(rabbitmq_auth_backend_oauth2, oauth_providers,
    #{ <<"A">> => [
        {issuer,build_url_to_oauth_provider(<<"/A">>) },
        {https, ?config(ssl_options, Config)}
        ],
    <<"B">> => [
        {issuer,build_url_to_oauth_provider(<<"/B">>) },
        {https, ?config(ssl_options, Config)}
    ] }),
    Config;

init_per_group(with_default_oauth_provider_A, Config) ->
    application:set_env(rabbitmq_auth_backend_oauth2, default_oauth_provider, <<"A">>),
    Config;

init_per_group(with_default_oauth_provider_B, Config) ->
    application:set_env(rabbitmq_auth_backend_oauth2, default_oauth_provider, <<"B">>),
    Config;



init_per_group(with_resource_server_id, Config) ->
    application:set_env(rabbitmq_auth_backend_oauth2, resource_server_id, ?RABBITMQ),
    Config;

init_per_group(with_root_scope_prefix, Config) ->
    application:set_env(rabbitmq_auth_backend_oauth2, scope_prefix, <<"some-prefix:">>),
    Config;
init_per_group(with_empty_scope_prefix_for_resource_one, Config) ->
    ResourceServers = application:get_env(rabbitmq_auth_backend_oauth2, resource_servers, #{}),
    Proplist = maps:get(?RABBITMQ_RESOURCE_ONE, ResourceServers, []),
     application:set_env(rabbitmq_auth_backend_oauth2, resource_servers,
        maps:put(?RABBITMQ_RESOURCE_ONE, [{scope_prefix, <<"">>} | proplists:delete(scope_prefix, Proplist)], ResourceServers)),
    Config;

init_per_group(with_verify_aud_false, Config) ->
    application:set_env(rabbitmq_auth_backend_oauth2, verify_aud, false),
    Config;
init_per_group(with_verify_aud_false_for_resource_two, Config) ->
    ResourceServers = application:get_env(rabbitmq_auth_backend_oauth2, resource_servers, #{}),
    Proplist = maps:get(?RABBITMQ_RESOURCE_TWO, ResourceServers, []),
     application:set_env(rabbitmq_auth_backend_oauth2, resource_servers,
        maps:put(?RABBITMQ_RESOURCE_TWO, [{verify_aud, false} | proplists:delete(verify_aud, Proplist)], ResourceServers)),
    Config;
init_per_group(with_algorithms, Config) ->
    KeyConfig = application:get_env(rabbitmq_auth_backend_oauth2, key_config, []),
    application:set_env(rabbitmq_auth_backend_oauth2, key_config, KeyConfig ++
        [{algorithms, [<<"HS256">>, <<"RS256">>]}]),
    [{algorithms, [<<"HS256">>, <<"RS256">>]} | Config];
init_per_group(with_algorithms_for_provider_A, Config) ->
    OAuthProviders = application:get_env(rabbitmq_auth_backend_oauth2, oauth_providers, #{}),
    OAuthProvider = maps:get(<<"A">>, OAuthProviders, []),
    application:set_env(rabbitmq_auth_backend_oauth2, oauth_providers,
        maps:put(<<"A">>, [{algorithms, [<<"HS256">>, <<"RS256">>]} | OAuthProvider], OAuthProviders)),
    [{algorithms, [<<"HS256">>, <<"RS256">>]} | Config];

init_per_group(with_resource_servers_and_resource_server_id, Config) ->
    application:set_env(rabbitmq_auth_backend_oauth2, resource_server_id, ?RABBITMQ),
    application:set_env(rabbitmq_auth_backend_oauth2, key_config, [{jwks_url,<<"https://oauth-for-rabbitmq">> }]),
    application:set_env(rabbitmq_auth_backend_oauth2, resource_servers,
    #{?RABBITMQ_RESOURCE_ONE =>  [
        { key_config, [
            {jwks_url,<<"https://oauth-for-rabbitmq1">> }
        ]}

    ],
      ?RABBITMQ_RESOURCE_TWO =>  [
        { key_config, [
            {jwks_url,<<"https://oauth-for-rabbitmq2">> }
        ]}
      ]
    }),
    Config;
init_per_group(with_different_oauth_provider_for_each_resource, Config) ->
    {ok, ResourceServers} = application:get_env(rabbitmq_auth_backend_oauth2, resource_servers),
    Rabbit1 = maps:get(?RABBITMQ_RESOURCE_ONE, ResourceServers) ++ [ {oauth_provider_id, <<"A">>} ],
    Rabbit2 = maps:get(?RABBITMQ_RESOURCE_TWO, ResourceServers) ++ [ {oauth_provider_id, <<"B">>} ],
    ResourceServers1 = maps:update(?RABBITMQ_RESOURCE_ONE, Rabbit1, ResourceServers),
    application:set_env(rabbitmq_auth_backend_oauth2, resource_servers, maps:update(?RABBITMQ_RESOURCE_TWO, Rabbit2, ResourceServers1)),
    Config;

init_per_group(with_resource_servers, Config) ->
    application:set_env(rabbitmq_auth_backend_oauth2, resource_servers,
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

init_per_group(inheritance_group, Config) ->
    application:set_env(rabbitmq_auth_backend_oauth2, resource_server_id, ?RABBITMQ),
    application:set_env(rabbitmq_auth_backend_oauth2, resource_server_type, <<"rabbitmq-type">>),
    application:set_env(rabbitmq_auth_backend_oauth2, scope_prefix, <<"some-prefix-">>),
    application:set_env(rabbitmq_auth_backend_oauth2, extra_scopes_source, <<"roles">>),
    application:set_env(rabbitmq_auth_backend_oauth2, scope_aliases, #{}),

    application:set_env(rabbitmq_auth_backend_oauth2, key_config, [ {jwks_url,<<"https://oauth-for-rabbitmq">> } ]),

    application:set_env(rabbitmq_auth_backend_oauth2, resource_servers,
        #{?RABBITMQ_RESOURCE_ONE =>  [
            { extra_scopes_source, <<"extra-scope-1">>},
            { verify_aud, false},
            { preferred_username_claims, [<<"email-address">>] },
            { scope_prefix, <<"my-prefix:">> },
            { resource_server_type, <<"my-type">> },
            { scope_aliases, #{} }
        ],
        ?RABBITMQ_RESOURCE_TWO =>  [ {id, ?RABBITMQ_RESOURCE_TWO  } ]
        }
    ),
    Config;

init_per_group(_any, Config) ->
    Config.

end_per_group(with_rabbitmq_node, Config) ->
    rabbit_ct_helpers:run_steps(Config, rabbit_ct_broker_helpers:teardown_steps());
end_per_group(with_root_static_signing_keys, Config) ->
    KeyConfig = call_get_env(Config, key_config, []),
    call_set_env(Config, key_config, KeyConfig),
    Config;
end_per_group(get_empty_scope_prefix, Config) ->
    application:unset_env(rabbitmq_auth_backend_oauth2, scope_prefix),
    Config;
end_per_group(with_resource_server_id, Config) ->
    application:unset_env(rabbitmq_auth_backend_oauth2, resource_server_id),
    Config;
end_per_group(with_verify_aud_false, Config) ->
    application:unset_env(rabbitmq_auth_backend_oauth2, verify_aud),
    Config;
end_per_group(with_verify_aud_false_for_resource_two, Config) ->
    ResourceServers = application:get_env(rabbitmq_auth_backend_oauth2, resource_servers, #{}),
    Proplist = maps:get(?RABBITMQ_RESOURCE_TWO, ResourceServers, []),
     application:set_env(rabbitmq_auth_backend_oauth2, resource_servers,
        maps:put(?RABBITMQ_RESOURCE_TWO, proplists:delete(verify_aud, Proplist), ResourceServers)),
    Config;
end_per_group(with_empty_scope_prefix_for_resource_one, Config) ->
    ResourceServers = application:get_env(rabbitmq_auth_backend_oauth2, resource_servers, #{}),
    Proplist = maps:get(?RABBITMQ_RESOURCE_ONE, ResourceServers, []),
     application:set_env(rabbitmq_auth_backend_oauth2, resource_servers,
        maps:put(?RABBITMQ_RESOURCE_ONE, proplists:delete(scope_prefix, Proplist), ResourceServers)),
    Config;

end_per_group(with_default_key, Config) ->
    KeyConfig = application:get_env(rabbitmq_auth_backend_oauth2, key_config, []),
    application:set_env(rabbitmq_auth_backend_oauth2, key_config,
        proplists:delete(default_key, KeyConfig)),
    Config;
end_per_group(with_algorithms, Config) ->
    KeyConfig = application:get_env(rabbitmq_auth_backend_oauth2, key_config, []),
    application:set_env(rabbitmq_auth_backend_oauth2, key_config,
        proplists:delete(algorithms, KeyConfig)),
    Config;
end_per_group(with_algorithms_for_provider_A, Config) ->
    OAuthProviders = application:get_env(rabbitmq_auth_backend_oauth2, oauth_providers, #{}),
    OAuthProvider = maps:get(<<"A">>, OAuthProviders, []),
    application:set_env(rabbitmq_auth_backend_oauth2, oauth_providers,
        maps:put(<<"A">>, proplists:delete(algorithms, OAuthProvider),OAuthProviders)),
    Config;
end_per_group(with_static_signing_keys_for_oauth_provider_A, Config) ->
    OAuthProviders = application:get_env(rabbitmq_auth_backend_oauth2, oauth_providers, #{}),
    OAuthProvider = maps:get(<<"A">>, OAuthProviders),
    OAuthProvider0 = proplists:delete(signing_keys, OAuthProvider),
    application:set_env(rabbitmq_auth_backend_oauth2, oauth_providers,
        maps:put(<<"A">>, OAuthProvider0, OAuthProviders)),
    Config;
end_per_group(with_jwks_url, Config) ->
    KeyConfig = ?config(key_config_before_group_with_jwks_url, Config),
    application:set_env(rabbitmq_auth_backend_oauth2, key_config, KeyConfig),
    Config;
end_per_group(with_issuer, Config) ->
    KeyConfig = ?config(key_config_before_group_with_issuer, Config),
    application:unset_env(rabbitmq_auth_backend_oauth2, issuer),
    application:set_env(rabbitmq_auth_backend_oauth2, key_config, KeyConfig),
    stop_http_auth_server(),
    Config;
end_per_group(with_oauth_providers_A_with_jwks_uri, Config) ->
    application:unset_env(rabbitmq_auth_backend_oauth2, oauth_providers),
    Config;
end_per_group(with_oauth_providers_A_with_issuer, Config) ->
    application:unset_env(rabbitmq_auth_backend_oauth2, oauth_providers),
    Config;
end_per_group(with_oauth_providers_A_B_with_jwks_uri, Config) ->
    application:unset_env(rabbitmq_auth_backend_oauth2, oauth_providers),
    Config;
end_per_group(with_oauth_providers_A_B_with_issuer, Config) ->
    application:unset_env(rabbitmq_auth_backend_oauth2, oauth_providers),
    Config;

end_per_group(with_oauth_providers_A, Config) ->
    application:unset_env(rabbitmq_auth_backend_oauth2, oauth_providers),
    Config;
end_per_group(with_oauth_providers_A_B, Config) ->
    application:unset_env(rabbitmq_auth_backend_oauth2, oauth_providers),
    Config;
end_per_group(with_default_oauth_provider_B, Config) ->
    application:unset_env(rabbitmq_auth_backend_oauth2, default_oauth_provider),
    Config;
end_per_group(with_default_oauth_provider_A, Config) ->
    application:unset_env(rabbitmq_auth_backend_oauth2, default_oauth_provider),
    Config;

end_per_group(get_oauth_provider_for_resource_server_id, Config) ->
    application:unset_env(rabbitmq_auth_backend_oauth2, resource_server_id),
    Config;

end_per_group(with_resource_servers_and_resource_server_id, Config) ->
    application:unset_env(rabbitmq_auth_backend_oauth2, resource_server_id),
    Config;

end_per_group(with_resource_servers, Config) ->
    application:unset_env(rabbitmq_auth_backend_oauth2, resource_servers),
    Config;

end_per_group(with_different_oauth_provider_for_each_resource, Config) ->
    {ok, ResourceServers} = application:get_env(rabbitmq_auth_backend_oauth2, resource_servers),
    Rabbit1 = proplists:delete(oauth_provider_id, maps:get(?RABBITMQ_RESOURCE_ONE, ResourceServers)),
    Rabbit2 = proplists:delete(oauth_provider_id, maps:get(?RABBITMQ_RESOURCE_TWO, ResourceServers)),
    ResourceServers1 = maps:update(?RABBITMQ_RESOURCE_ONE, Rabbit1, ResourceServers),
    application:set_env(rabbitmq_auth_backend_oauth2, resource_servers, maps:update(?RABBITMQ_RESOURCE_TWO, Rabbit2, ResourceServers1)),
    Config;

end_per_group(inheritance_group, Config) ->
    application:unset_env(rabbitmq_auth_backend_oauth2, resource_server_id),
    application:unset_env(rabbitmq_auth_backend_oauth2, scope_prefix),
    application:unset_env(rabbitmq_auth_backend_oauth2, extra_scopes_source),

    application:unset_env(rabbitmq_auth_backend_oauth2, key_config),

    application:unset_env(rabbitmq_auth_backend_oauth2, resource_servers),
    Config;

end_per_group(with_root_scope_prefix, Config) ->
    application:unset_env(rabbitmq_auth_backend_oauth2, scope_prefix),
    Config;

end_per_group(_any, Config) ->
    Config.

init_per_testcase(get_preferred_username_claims, Config) ->
    application:set_env(rabbitmq_auth_backend_oauth2, preferred_username_claims, [<<"username">>]),
    Config;

init_per_testcase(get_additional_scopes_key_when_not_defined, Config) ->
    application:unset_env(rabbitmq_auth_backend_oauth2, extra_scopes_source),
    Config;
init_per_testcase(is_verify_aud_when_is_false, Config) ->
    application:set_env(rabbitmq_auth_backend_oauth2, verify_aud, false),
    Config;
init_per_testcase(get_empty_scope_prefix, Config) ->
    application:set_env(rabbitmq_auth_backend_oauth2, scope_prefix, <<"">>),
    Config;
init_per_testcase(get_scope_prefix_when_not_defined, Config) ->
    application:unset_env(rabbitmq_auth_backend_oauth2, scope_prefix),
    Config;
init_per_testcase(get_resource_server_type_when_not_defined, Config) ->
    application:unset_env(rabbitmq_auth_backend_oauth2, resource_server_type),
    Config;
init_per_testcase(has_scope_aliases_when_not_defined, Config) ->
    application:unset_env(rabbitmq_auth_backend_oauth2, scope_aliases),
    Config;

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(get_preferred_username_claims, Config) ->
    application:unset_env(rabbitmq_auth_backend_oauth2, preferred_username_claims),
    Config;


end_per_testcase(_Testcase, Config) ->
    Config.

%% -----

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
    NewKeys = #{<<"mykey-root-1">> => <<"new key root-1">>, <<"key-3">> => <<"some key 3">>},
    call_replace_signing_keys(Config, [NewKeys]),
    #{  <<"mykey-root-1">> := <<"new key root-1">>,
        <<"mykey-root-2">> := <<"some key root-2">>,
        <<"key-3">> := <<"some key 3">>
    } = call_get_signing_keys(Config).
replace_override_static_keys_with_newly_added_keys(Config) ->
    NewKeys = #{<<"mykey-root-1">> => <<"new key root-1">>, <<"key-3">> => <<"some key 3">>},
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


get_default_resource_server_id_returns_error(_Config) ->
    {error, _} = rabbit_oauth2_config:get_default_resource_server_id().

get_resource_server_id_for_rabbit_audience_returns_rabbit(_Config) ->
    ?assertEqual(?RABBITMQ, rabbit_oauth2_config:get_resource_server_id_for_audience(?RABBITMQ)).
get_resource_server_id_for_none_audience_returns_rabbit(_Config) ->
    ?assertEqual(?RABBITMQ, rabbit_oauth2_config:get_resource_server_id_for_audience(none)).
get_resource_server_id_for_unknown_audience_returns_rabbit(_Config) ->
    ?assertEqual(?RABBITMQ, rabbit_oauth2_config:get_resource_server_id_for_audience(<<"unknown">>)).

get_resource_server_id_for_none_audience_should_fail(_Config) ->
    ?assertEqual({error, no_matching_aud_found}, rabbit_oauth2_config:get_resource_server_id_for_audience(none)).
get_resource_server_id_for_unknown_audience_should_fail(_Config) ->
    ?assertEqual({error, no_matching_aud_found}, rabbit_oauth2_config:get_resource_server_id_for_audience(<<"unknown">>)).

get_default_resource_server_id(_Config) ->
    ?assertEqual(?RABBITMQ, rabbit_oauth2_config:get_default_resource_server_id()).

get_allowed_resource_server_ids_returns_empty_list(_Config) ->
    [] = rabbit_oauth2_config:get_allowed_resource_server_ids().

get_allowed_resource_server_ids_returns_resource_server_id(_Config) ->
    [?RABBITMQ] = rabbit_oauth2_config:get_allowed_resource_server_ids().

get_allowed_resource_server_ids_returns_all_resource_servers_ids(_Config) ->
    [ <<"rabbitmq1">>, <<"rabbitmq2">>, ?RABBITMQ] = rabbit_oauth2_config:get_allowed_resource_server_ids().

get_allowed_resource_server_ids_returns_resource_servers_ids(_Config) ->
    [<<"rabbitmq-0">>, <<"rabbitmq-1">>, <<"rabbitmq1">>, <<"rabbitmq2">> ] =
        lists:sort(rabbit_oauth2_config:get_allowed_resource_server_ids()).

index_resource_servers_by_id_else_by_key(_Config) ->
    {error, no_matching_aud_found} = rabbit_oauth2_config:find_audience_in_resource_server_ids(<<"0">>),
    {ok, <<"rabbitmq-0">>} = rabbit_oauth2_config:find_audience_in_resource_server_ids([<<"rabbitmq-0">>]),
    {ok, <<"rabbitmq-0">>} = rabbit_oauth2_config:find_audience_in_resource_server_ids(<<"rabbitmq-0">>).

find_audience_in_resource_server_ids_returns_key_not_found(_Config) ->
    {error, no_matching_aud_found} = rabbit_oauth2_config:find_audience_in_resource_server_ids(?RABBITMQ).

find_audience_in_resource_server_ids_returns_found_too_many(_Config) ->
    {error, only_one_resource_server_as_audience_found_many} = rabbit_oauth2_config:find_audience_in_resource_server_ids([?RABBITMQ, <<"rabbitmq1">>]).

find_audience_in_resource_server_ids_found_one_resource_servers(_Config) ->
    {ok, <<"rabbitmq1">>} = rabbit_oauth2_config:find_audience_in_resource_server_ids(<<"rabbitmq1">>),
    {ok, <<"rabbitmq1">>} = rabbit_oauth2_config:find_audience_in_resource_server_ids([<<"rabbitmq1">>, <<"other">>]).

find_audience_in_resource_server_ids_found_resource_server_id(_Config) ->
    {ok, ?RABBITMQ} = rabbit_oauth2_config:find_audience_in_resource_server_ids(?RABBITMQ),
    {ok, ?RABBITMQ} = rabbit_oauth2_config:find_audience_in_resource_server_ids([?RABBITMQ, <<"other">>]).

find_audience_in_resource_server_ids_using_binary_audience(_Config) ->
    {ok, ?RABBITMQ} = rabbit_oauth2_config:find_audience_in_resource_server_ids(<<"rabbitmq other">>).

get_additional_scopes_key(_Config) ->
    ?assertEqual({ok, <<"roles">>}, rabbit_oauth2_config:get_additional_scopes_key()),
    ?assertEqual({ok, <<"extra-scope-1">>}, rabbit_oauth2_config:get_additional_scopes_key(<<"rabbitmq1">> )),
    ?assertEqual(rabbit_oauth2_config:get_additional_scopes_key(), rabbit_oauth2_config:get_additional_scopes_key(<<"rabbitmq2">>)),
    ?assertEqual({ok, <<"roles">>}, rabbit_oauth2_config:get_additional_scopes_key(?RABBITMQ)).

get_additional_scopes_key_when_not_defined(_Config) ->
    ?assertEqual({error, not_found}, rabbit_oauth2_config:get_additional_scopes_key()),
    ?assertEqual(rabbit_oauth2_config:get_additional_scopes_key(), rabbit_oauth2_config:get_additional_scopes_key(<<"rabbitmq2">>)).

is_verify_aud(_Config) ->
    ?assertEqual(true, rabbit_oauth2_config:is_verify_aud()),
    ?assertEqual(rabbit_oauth2_config:is_verify_aud(?RABBITMQ), rabbit_oauth2_config:is_verify_aud()),
    ?assertEqual(false, rabbit_oauth2_config:is_verify_aud(<<"rabbitmq1">>)),
    ?assertEqual(rabbit_oauth2_config:is_verify_aud(), rabbit_oauth2_config:is_verify_aud(<<"rabbitmq2">>)).
is_verify_aud_for_resource_one_returns_false(_Config) ->
    ?assertEqual(false, rabbit_oauth2_config:is_verify_aud(?RABBITMQ_RESOURCE_ONE)).

is_verify_aud_for_resource_two_returns_true(_Config) ->
    ?assertEqual(true, rabbit_oauth2_config:is_verify_aud(?RABBITMQ_RESOURCE_TWO)).

is_verify_aud_when_is_false(_Config) ->
    ?assertEqual(false, rabbit_oauth2_config:is_verify_aud()),
    ?assertEqual(rabbit_oauth2_config:is_verify_aud(), rabbit_oauth2_config:is_verify_aud(<<"rabbitmq2">>)).

is_verify_aud_for_resource_one_returns_true(_Config) ->
    ?assertEqual(true, rabbit_oauth2_config:is_verify_aud(?RABBITMQ_RESOURCE_ONE)).
is_verify_aud_for_resource_two_returns_false(_Config) ->
    ?assertEqual(false, rabbit_oauth2_config:is_verify_aud(?RABBITMQ_RESOURCE_TWO)).

get_default_preferred_username_claims(_Config) ->
    ?assertEqual(rabbit_oauth2_config:get_default_preferred_username_claims(), rabbit_oauth2_config:get_preferred_username_claims()).

get_preferred_username_claims(_Config) ->
    ?assertEqual([<<"username">>] ++ rabbit_oauth2_config:get_default_preferred_username_claims(),
        rabbit_oauth2_config:get_preferred_username_claims()),
    ?assertEqual([<<"email-address">>] ++ rabbit_oauth2_config:get_default_preferred_username_claims(),
        rabbit_oauth2_config:get_preferred_username_claims(<<"rabbitmq1">>)),
    ?assertEqual(rabbit_oauth2_config:get_preferred_username_claims(),
        rabbit_oauth2_config:get_preferred_username_claims(<<"rabbitmq2">>)).

get_scope_prefix_when_not_defined(_Config) ->
    ?assertEqual(<<"rabbitmq.">>, rabbit_oauth2_config:get_scope_prefix()),
    ?assertEqual(<<"rabbitmq2.">>, rabbit_oauth2_config:get_scope_prefix(<<"rabbitmq2">>)).

get_empty_scope_prefix(_Config) ->
    ?assertEqual(<<"">>, rabbit_oauth2_config:get_scope_prefix()),
    ?assertEqual(<<"">>, rabbit_oauth2_config:get_scope_prefix(<<"rabbitmq2">>)).

get_scope_prefix(_Config) ->
    ?assertEqual(<<"some-prefix-">>, rabbit_oauth2_config:get_scope_prefix()),
    ?assertEqual(<<"my-prefix:">>, rabbit_oauth2_config:get_scope_prefix(<<"rabbitmq1">>)),
    ?assertEqual(rabbit_oauth2_config:get_scope_prefix(), rabbit_oauth2_config:get_scope_prefix(<<"rabbitmq2">>)).

get_scope_prefix_for_resource_one_returns_default_scope_prefix(_Config) ->
    ?assertEqual(undefined, application:get_env(rabbitmq_auth_backend_oauth2, scope_prefix)),
    ?assertEqual(append_paths(?RABBITMQ_RESOURCE_ONE, <<".">>),
        rabbit_oauth2_config:get_scope_prefix(?RABBITMQ_RESOURCE_ONE)).
get_scope_prefix_for_resource_one_returns_root_scope_prefix(_Config) ->
    {ok, Prefix} = application:get_env(rabbitmq_auth_backend_oauth2, scope_prefix),
    ?assertEqual(rabbit_oauth2_config:get_scope_prefix(),
        rabbit_oauth2_config:get_scope_prefix(?RABBITMQ_RESOURCE_ONE)),
    ?assertEqual(Prefix,
        rabbit_oauth2_config:get_scope_prefix(?RABBITMQ_RESOURCE_ONE)).
get_scope_prefix_for_resource_one_returns_empty_scope_prefix(_Config) ->
    ?assertEqual(<<"">>,
        rabbit_oauth2_config:get_scope_prefix(?RABBITMQ_RESOURCE_ONE)).
get_scope_prefix_for_resource_two_returns_root_scope_prefix(_Config) ->
    {ok, Prefix} = application:get_env(rabbitmq_auth_backend_oauth2, scope_prefix),
    ?assertEqual(rabbit_oauth2_config:get_scope_prefix(),
        rabbit_oauth2_config:get_scope_prefix(?RABBITMQ_RESOURCE_TWO)),
    ?assertEqual(Prefix,
        rabbit_oauth2_config:get_scope_prefix(?RABBITMQ_RESOURCE_TWO)).

get_resource_server_type_when_not_defined(_Config) ->
    ?assertEqual(<<>>, rabbit_oauth2_config:get_resource_server_type()),
    ?assertEqual(<<>>, rabbit_oauth2_config:get_resource_server_type(<<"rabbitmq2">>)).

get_resource_server_type(_Config) ->
    ?assertEqual(<<"rabbitmq-type">>, rabbit_oauth2_config:get_resource_server_type()),
    ?assertEqual(<<"my-type">>, rabbit_oauth2_config:get_resource_server_type(<<"rabbitmq1">>)),
    ?assertEqual(rabbit_oauth2_config:get_resource_server_type(), rabbit_oauth2_config:get_resource_server_type(<<"rabbitmq2">>)).

has_scope_aliases_when_not_defined(_Config) ->
    ?assertEqual(false, rabbit_oauth2_config:has_scope_aliases(?RABBITMQ)),
    ?assertEqual(true, rabbit_oauth2_config:has_scope_aliases(<<"rabbitmq1">>)),
    ?assertEqual(rabbit_oauth2_config:has_scope_aliases(?RABBITMQ), rabbit_oauth2_config:has_scope_aliases(<<"rabbitmq2">>)).

has_scope_aliases(_Config) ->
    ?assertEqual(true, rabbit_oauth2_config:has_scope_aliases(?RABBITMQ)),
    ?assertEqual(true, rabbit_oauth2_config:has_scope_aliases(<<"rabbitmq1">>)),
    ?assertEqual(rabbit_oauth2_config:has_scope_aliases(?RABBITMQ), rabbit_oauth2_config:has_scope_aliases(<<"rabbitmq2">>)).

get_scope_aliases(_Config) ->
    ?assertEqual(#{}, rabbit_oauth2_config:get_scope_aliases(?RABBITMQ)),
    ?assertEqual(#{}, rabbit_oauth2_config:get_scope_aliases(<<"rabbitmq1">>)),
    ?assertEqual(rabbit_oauth2_config:get_scope_aliases(?RABBITMQ), rabbit_oauth2_config:get_scope_aliases(<<"rabbitmq2">>)).

get_default_key_should_fail(_Config) ->
    {error, no_default_key_configured} = rabbit_oauth2_config:get_default_key().
get_default_key(_Config) ->
    {ok, <<"default-key">>} = rabbit_oauth2_config:get_default_key().
get_default_key_for_provider_A_should_fail(_Config) ->
    {error, no_default_key_configured} = rabbit_oauth2_config:get_default_key(<<"A">>).
get_default_key_for_provider_A(_Config) ->
    {ok, <<"A-default-key">>} = rabbit_oauth2_config:get_default_key(<<"A">>).

get_signing_keys(_Config) ->
    #{<<"mykey-1-1">> := <<"some key 1-1">>,
      <<"mykey-1-2">> := <<"some key 1-2">>} = rabbit_oauth2_config:get_signing_keys(),
    <<"some key 1-1">> = rabbit_oauth2_config:get_signing_key(<<"mykey-1-1">>),
    undefined = rabbit_oauth2_config:get_signing_key(<<"unknown">>).
get_signing_keys_for_oauth_provider_A(_Config) ->
    #{<<"A-mykey-1-1">> := <<"A-some key 1-1">>,
      <<"A-mykey-1-2">> := <<"A-some key 1-2">>} = rabbit_oauth2_config:get_signing_keys(<<"A">>),
    <<"A-some key 1-1">> = rabbit_oauth2_config:get_signing_key(<<"A-mykey-1-1">>, <<"A">>),
    undefined = rabbit_oauth2_config:get_signing_key(<<"unknown">>, <<"A">>).

get_algorithms_should_return_undefined(_Config) ->
    undefined = rabbit_oauth2_config:get_algorithms().
get_algorithms(Config) ->
    ?assertEqual(?config(algorithms, Config), rabbit_oauth2_config:get_algorithms()).
get_algorithms_for_provider_A_should_return_undefined(_Config) ->
    undefined = rabbit_oauth2_config:get_algorithms(<<"A">>).
get_algorithms_for_provider_A(Config) ->
    ?assertEqual(?config(algorithms, Config), rabbit_oauth2_config:get_algorithms(<<"A">>)).

get_oauth_provider_root_with_jwks_uri_should_fail(_Config) ->
    root = rabbit_oauth2_config:get_oauth_provider_id_for_resource_server_id(?RABBITMQ),
    {error, _Message} = rabbit_oauth2_config:get_oauth_provider(root, [jwks_uri]).
get_oauth_provider_A_with_jwks_uri_should_fail(_Config) ->
    <<"A">> = rabbit_oauth2_config:get_oauth_provider_id_for_resource_server_id(?RABBITMQ),
    {error, _Message} = rabbit_oauth2_config:get_oauth_provider(<<"A">>, [jwks_uri]).
get_oauth_provider_should_return_root_oauth_provider_with_jwks_uri(_Config) ->
    root = rabbit_oauth2_config:get_oauth_provider_id_for_resource_server_id(?RABBITMQ),
    {ok, OAuthProvider} = rabbit_oauth2_config:get_oauth_provider(root, [jwks_uri]),
    ?assertEqual(build_url_to_oauth_provider(<<"/keys">>), OAuthProvider#oauth_provider.jwks_uri).
get_oauth_provider_for_both_resources_should_return_root_oauth_provider(_Config) ->
    root = rabbit_oauth2_config:get_oauth_provider_id_for_resource_server_id(?RABBITMQ_RESOURCE_ONE),
    {ok, OAuthProvider} = rabbit_oauth2_config:get_oauth_provider(root, [jwks_uri]),
    ?assertEqual(build_url_to_oauth_provider(<<"/keys">>), OAuthProvider#oauth_provider.jwks_uri),
    root = rabbit_oauth2_config:get_oauth_provider_id_for_resource_server_id(?RABBITMQ_RESOURCE_TWO).
get_oauth_provider_for_resource_one_should_return_oauth_provider_A(_Config) ->
    <<"A">> = rabbit_oauth2_config:get_oauth_provider_id_for_resource_server_id(?RABBITMQ_RESOURCE_ONE),
    {ok, OAuthProvider} = rabbit_oauth2_config:get_oauth_provider(<<"A">>, [jwks_uri]),
    ?assertEqual(build_url_to_oauth_provider(<<"/A/keys">>), OAuthProvider#oauth_provider.jwks_uri).
get_oauth_provider_for_both_resources_should_return_oauth_provider_A(_Config) ->
    <<"A">> = rabbit_oauth2_config:get_oauth_provider_id_for_resource_server_id(?RABBITMQ_RESOURCE_ONE),
    {ok, OAuthProvider} = rabbit_oauth2_config:get_oauth_provider(<<"A">>, [jwks_uri]),
    ?assertEqual(build_url_to_oauth_provider(<<"/A/keys">>), OAuthProvider#oauth_provider.jwks_uri),
    <<"A">> = rabbit_oauth2_config:get_oauth_provider_id_for_resource_server_id(?RABBITMQ_RESOURCE_TWO).
get_oauth_provider_for_resource_two_should_return_oauth_provider_B(_Config) ->
    <<"B">> = rabbit_oauth2_config:get_oauth_provider_id_for_resource_server_id(?RABBITMQ_RESOURCE_TWO),
    {ok, OAuthProvider} = rabbit_oauth2_config:get_oauth_provider(<<"B">>, [jwks_uri]),
    ?assertEqual(build_url_to_oauth_provider(<<"/B/keys">>), OAuthProvider#oauth_provider.jwks_uri).

get_oauth_provider_should_return_root_oauth_provider_with_all_discovered_endpoints(_Config) ->
    root = rabbit_oauth2_config:get_oauth_provider_id_for_resource_server_id(?RABBITMQ),
    {ok, OAuthProvider} = rabbit_oauth2_config:get_oauth_provider(root, [jwks_uri]),
    ?assertEqual(build_url_to_oauth_provider(<<"/keys">>), OAuthProvider#oauth_provider.jwks_uri),
    ?assertEqual(build_url_to_oauth_provider(<<"/">>), OAuthProvider#oauth_provider.issuer).
append_paths(Path1, Path2) ->
    erlang:iolist_to_binary([Path1, Path2]).

get_oauth_provider_should_return_oauth_provider_B_with_jwks_uri(_Config) ->
    <<"B">> = rabbit_oauth2_config:get_oauth_provider_id_for_resource_server_id(?RABBITMQ),
    {ok, OAuthProvider} = rabbit_oauth2_config:get_oauth_provider(<<"B">>, [jwks_uri]),
    ?assertEqual(build_url_to_oauth_provider(<<"/B/keys">>), OAuthProvider#oauth_provider.jwks_uri).

get_oauth_provider_should_return_oauth_provider_B_with_all_discovered_endpoints(_Config) ->
    <<"B">> = rabbit_oauth2_config:get_oauth_provider_id_for_resource_server_id(?RABBITMQ),
    {ok, OAuthProvider} = rabbit_oauth2_config:get_oauth_provider(<<"B">>, [jwks_uri]),
    ?assertEqual(build_url_to_oauth_provider(<<"/B/keys">>), OAuthProvider#oauth_provider.jwks_uri),
    ?assertEqual(build_url_to_oauth_provider(<<"/B">>), OAuthProvider#oauth_provider.issuer).

get_oauth_provider_should_return_oauth_provider_A_with_jwks_uri(_Config) ->
    <<"A">> = rabbit_oauth2_config:get_oauth_provider_id_for_resource_server_id(?RABBITMQ),
    {ok, OAuthProvider} = rabbit_oauth2_config:get_oauth_provider(<<"A">>, [jwks_uri]),
    ?assertEqual(build_url_to_oauth_provider(<<"/A/keys">>), OAuthProvider#oauth_provider.jwks_uri).

get_oauth_provider_should_return_oauth_provider_A_with_all_discovered_endpoints(_Config) ->
    <<"A">> = rabbit_oauth2_config:get_oauth_provider_id_for_resource_server_id(?RABBITMQ),
    {ok, OAuthProvider} = rabbit_oauth2_config:get_oauth_provider(<<"A">>, [jwks_uri]),
    ?assertEqual(build_url_to_oauth_provider(<<"/A/keys">>), OAuthProvider#oauth_provider.jwks_uri),
    ?assertEqual(build_url_to_oauth_provider(<<"/A">>), OAuthProvider#oauth_provider.issuer).

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
