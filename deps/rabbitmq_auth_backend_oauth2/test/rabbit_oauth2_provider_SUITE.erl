%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_oauth2_provider_SUITE).

-compile(export_all).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("oauth2.hrl").

-define(RABBITMQ,<<"rabbitmq">>).
-define(RABBITMQ_RESOURCE_ONE,<<"rabbitmq1">>).
-define(RABBITMQ_RESOURCE_TWO,<<"rabbitmq2">>).
-define(AUTH_PORT, 8000).
-define(JWKS_STATUS_PORT, 8001).

-import(rabbit_oauth2_provider, [
    get_internal_oauth_provider/0,get_internal_oauth_provider/1,
    add_signing_key/2, add_signing_key/3, replace_signing_keys/1,
    replace_signing_keys/2,
    get_signing_keys/0, get_signing_keys/1, get_signing_key/1, get_signing_key/2
]).
-import(oauth2_client, [get_oauth_provider/2]).

all() -> [
    {group, with_rabbitmq_node},
    {group, verify_oauth_provider_A},
    {group, verify_oauth_provider_root}
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
        ]},
        {jwks_response_handling, [], [
            scenario_a,
            scenario_b,
            scenario_c,
            scenario_d
        ]}
    ]},
    {verify_oauth_provider_A, [], verify_provider()},
    {verify_oauth_provider_root, [], verify_provider()}
].

verify_provider() -> [
    internal_oauth_provider_has_no_default_key,
    {oauth_provider_with_default_key, [], [
        internal_oauth_provider_has_default_key
    ]},
    internal_oauth_provider_has_no_algorithms,
    {oauth_provider_with_algorithms, [], [
        internal_oauth_provider_has_algorithms
    ]},
    get_oauth_provider_with_jwks_uri_returns_error,
    {oauth_provider_with_jwks_uri, [], [
        get_oauth_provider_has_jwks_uri
    ]},
    {oauth_provider_with_issuer, [], [
        get_oauth_provider_has_jwks_uri
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

init_per_group(jwks_response_handling, Config) ->
    {ok, _} = application:ensure_all_started(ssl),
    {ok, _} = application:ensure_all_started(cowboy),
    CertsDir = ?config(rmq_certsdir, Config),
    Routes = [
        {"/fail", #{status => 500,
                     body => rabbit_json:encode(#{error => <<"unavailable">>})}},
        {"/no-keys", #{status => 200,
                        body => rabbit_json:encode(#{keys => []})}},
        {"/garbage", #{status => 200,
                        headers => #{<<"content-type">> => <<"text/html">>},
                        body => <<"<html>maintenance</html>">>}},
        {"/ok", #{status => 200,
                  body => rabbit_json:encode(#{keys => [
                      #{<<"kid">> => <<"fresh-key">>,
                        <<"kty">> => <<"oct">>,
                        <<"k">> => <<"YW5vdGhlci1zZWNyZXQ">>}]})}}
    ],
    Dispatch = cowboy_router:compile([{'_', [
        {Path, configurable_http_handler, State} || {Path, State} <- Routes
    ]}]),
    {ok, _} = cowboy:start_tls(jwks_status_listener,
        [{port, ?JWKS_STATUS_PORT},
         {certfile, filename:join([CertsDir, "server", "cert.pem"])},
         {keyfile, filename:join([CertsDir, "server", "key.pem"])}],
        #{env => #{dispatch => Dispatch}}),
    OriginalKeyConfig = call_get_env(Config, key_config, []),
    rabbit_ct_helpers:set_config(Config, [
        {saved_key_config, OriginalKeyConfig}
    ]);

init_per_group(oauth_provider_with_jwks_uri, Config) ->
    URL = case ?config(oauth_provider_id, Config) of
        root ->
            RootUrl = build_url_to_oauth_provider(<<"/keys">>),
            set_env(jwks_uri, RootUrl),
            RootUrl;
        <<"A">> ->
            AUrl = build_url_to_oauth_provider(<<"/A/keys">>),
            set_oauth_provider_properties(<<"A">>, [{jwks_uri, AUrl}]),
            AUrl
    end,
    [{jwks_uri, URL} | Config];

init_per_group(oauth_provider_with_issuer, Config) ->
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
    {Issuer, JwksUri} = case ?config(oauth_provider_id, Config) of
        root ->
            Url = build_url_to_oauth_provider(<<"/">>),
            set_env(issuer, Url),
            set_env(key_config, SslOptions),
            {Url, build_url_to_oauth_provider(<<"/keys">>)};
        <<"A">> ->
            Url = build_url_to_oauth_provider(<<"/A">>),
            set_oauth_provider_properties(<<"A">>, [{issuer, Url}, {https, SslOptions}]),
            {Url, build_url_to_oauth_provider(<<"/A/keys">>)}
    end,
    [{issuer, Issuer}, {jwks_uri, JwksUri}] ++ Config;

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


init_per_group(verify_oauth_provider_A, Config) ->
    set_env(oauth_providers,
        #{ <<"A">> => [
            {id, <<"A">>}
            ]
        }),
    [{oauth_provider_id, <<"A">>} |Config];

init_per_group(verify_oauth_provider_root, Config) ->
    [{oauth_provider_id, root} |Config];

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

end_per_group(jwks_response_handling, Config) ->
    ok = cowboy:stop_listener(jwks_status_listener),
    call_set_env(Config, key_config,
        ?config(saved_key_config, Config)),
    call_unset_env(Config, jwks_uri),
    Config;

end_per_group(oauth_provider_with_issuer, Config) ->
    case ?config(oauth_provider_id, Config) of
        root ->
            unset_env(issuer),
            unset_env(https);
        Id ->
            unset_oauth_provider_properties(Id, [issuer, https])
    end,
    stop_http_auth_server(),
    Config;
end_per_group(oauth_provider_with_jwks_uri, Config) ->
    case ?config(oauth_provider_id, Config) of
        root -> unset_env(jwks_uri);
        Id -> unset_oauth_provider_properties(Id, [jwks_uri])
    end,
    Config;

end_per_group(oauth_provider_with_default_key, Config) ->
case ?config(oauth_provider_id, Config) of
        root -> unset_env(default_key);
        Id -> unset_oauth_provider_properties(Id, [default_key])
    end,
    Config;

end_per_group(_any, Config) ->
    Config.

init_per_testcase(Testcase, Config) when
        Testcase =:= scenario_a orelse
        Testcase =:= scenario_b orelse
        Testcase =:= scenario_c orelse
        Testcase =:= scenario_d ->
    %% Every test case in this group starts from the same baseline: one
    %% statically configured signing key and no dynamically downloaded ones.
    StaticKey = #{<<"kid">> => <<"static-key">>, <<"kty">> => <<"oct">>,
                  <<"k">> => <<"c29tZS1zZWNyZXQ">>},
    call_set_env(Config, key_config, [
        {peer_verification, verify_none},
        {signing_keys, #{<<"static-key">> => {map, StaticKey}}}
    ]),
    Config;
init_per_testcase(_Testcase, Config) ->
    Config.

end_per_testcase(_Testcase, Config) ->
    Config.

%% ----- Utility functions

call_set_env(Config, Par, Value) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
        [rabbitmq_auth_backend_oauth2, Par, Value]).

call_get_env(Config, Par, Def) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, application, get_env,
        [rabbitmq_auth_backend_oauth2, Par, Def]).

call_unset_env(Config, Par) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, application, unset_env,
        [rabbitmq_auth_backend_oauth2, Par]).

call_get_internal_oauth_provider(Config, Args) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_oauth2_provider,
        get_internal_oauth_provider, Args).

call_get_jwk(Config, Args) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, uaa_jwt, get_jwk, Args).

build_jwks_status_url(Path) ->
    "https://localhost:" ++ integer_to_list(?JWKS_STATUS_PORT) ++ Path.

call_add_signing_key(Config, Args) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_oauth2_provider,
        add_signing_key, Args).

call_get_signing_keys(Config, Args) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_oauth2_provider,
        get_signing_keys, Args).

call_get_signing_keys(Config) ->
    call_get_signing_keys(Config, []).

call_get_signing_key(Config, Args) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_oauth2_provider,
        get_signing_key, Args).

call_add_signing_keys(Config, Args) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_oauth2_provider,
        add_signing_keys, Args).

call_replace_signing_keys(Config, Args) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_oauth2_provider,
        replace_signing_keys, Args).

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

%% A non-200 refresh response leaves the cached static key untouched.
scenario_a(Config) ->
    call_set_env(Config, jwks_uri, build_jwks_status_url("/fail")),
    InternalOAuthProvider = call_get_internal_oauth_provider(Config, [root]),
    ?assertEqual({error, {unexpected_status_code, 500}},
        call_get_jwk(Config, [<<"missing-key">>, InternalOAuthProvider])),
    static_key_only(Config).

%% A 200 refresh response with no keys leaves the cached static key untouched.
scenario_b(Config) ->
    call_set_env(Config, jwks_uri, build_jwks_status_url("/no-keys")),
    InternalOAuthProvider = call_get_internal_oauth_provider(Config, [root]),
    ?assertEqual({error, empty_jwks_response},
        call_get_jwk(Config, [<<"missing-key">>, InternalOAuthProvider])),
    static_key_only(Config).

%% A 200 refresh response with an unparseable body returns a clean error
%% without crashing the caller, and the cached static key is untouched.
scenario_c(Config) ->
    call_set_env(Config, jwks_uri, build_jwks_status_url("/garbage")),
    InternalOAuthProvider = call_get_internal_oauth_provider(Config, [root]),
    ?assertEqual({error, invalid_jwks_response},
        call_get_jwk(Config, [<<"missing-key">>, InternalOAuthProvider])),
    static_key_only(Config).

%% A healthy 200 refresh response merges the fetched key alongside the static one.
scenario_d(Config) ->
    call_set_env(Config, jwks_uri, build_jwks_status_url("/ok")),
    InternalOAuthProvider = call_get_internal_oauth_provider(Config, [root]),
    {ok, _JWK} = call_get_jwk(Config, [<<"fresh-key">>, InternalOAuthProvider]),
    #{<<"static-key">> := _, <<"fresh-key">> := _} =
        call_get_signing_keys(Config).

static_key_only(Config) ->
    #{<<"static-key">> := _} = Keys = call_get_signing_keys(Config),
    ?assertEqual(1, maps:size(Keys)),
    Keys.

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

append_paths(Path1, Path2) ->
    erlang:iolist_to_binary([Path1, Path2]).



internal_oauth_provider_has_no_default_key(Config) ->
    InternalOAuthProvider = get_internal_oauth_provider(
        ?config(oauth_provider_id, Config)),
    ?assertEqual(undefined,
        InternalOAuthProvider#internal_oauth_provider.default_key).

internal_oauth_provider_has_default_key(Config) ->
    InternalOAuthProvider = get_internal_oauth_provider(
        ?config(oauth_provider_id, Config)),
    ?assertEqual(?config(default_key, Config),
        InternalOAuthProvider#internal_oauth_provider.default_key).

internal_oauth_provider_has_no_algorithms(Config) ->
    InternalOAuthProvider = get_internal_oauth_provider(
        ?config(oauth_provider_id, Config)),
    ?assertEqual(undefined,
        InternalOAuthProvider#internal_oauth_provider.algorithms).

internal_oauth_provider_has_algorithms(Config) ->
    InternalOAuthProvider = get_internal_oauth_provider(
        ?config(oauth_provider_id, Config)),
    ?assertEqual(?config(algorithms, Config),
        InternalOAuthProvider#internal_oauth_provider.algorithms).

get_oauth_provider_with_jwks_uri_returns_error(Config) ->
    {error, _} = get_oauth_provider(
        ?config(oauth_provider_id, Config), [jwks_uri]).

get_oauth_provider_has_jwks_uri(Config) ->
    {ok, OAuthProvider} = get_oauth_provider(
        ?config(oauth_provider_id, Config), [jwks_uri]),
        ct:log("OAuthProvider: ~p", [OAuthProvider]),
    ?assertEqual(?config(jwks_uri, Config), OAuthProvider#oauth_provider.jwks_uri).


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
        {'_', [{Path, oauth2_http_mock, Expected} ||
            #{request := #{path := Path}} = Expected <- Expectations ]}
        ]),
    {ok, _Pid} = cowboy:start_tls(
        mock_http_auth_listener,
        [{port, Port},
         {certfile, filename:join([CertsDir, "server", "cert.pem"])},
         {keyfile, filename:join([CertsDir, "server", "key.pem"])}
        ],
        #{env => #{dispatch => Dispatch}}).

build_url_to_oauth_provider(Path) ->
    uri_string:recompose(#{scheme => "https",
                         host => "localhost",
                         port => rabbit_data_coercion:to_integer(?AUTH_PORT),
                         path => Path}).

stop_http_auth_server() ->
    cowboy:stop_listener(mock_http_auth_listener).

set_oauth_provider_properties(OAuthProviderId, Proplist) ->
    OAuthProviders = get_env(oauth_providers, #{}),
    CurProplist = maps:get(OAuthProviderId, OAuthProviders),
    CurMap = proplists:to_map(CurProplist),
    Map = proplists:to_map(Proplist),
    set_env(oauth_providers, maps:put(OAuthProviderId,
        maps:to_list(maps:merge(CurMap, Map)), OAuthProviders)).

unset_oauth_provider_properties(OAuthProviderId, PropertyNameList) ->
    OAuthProviders = get_env(oauth_providers, #{}),
    CurProplist = maps:get(OAuthProviderId, OAuthProviders),
    CurMap = proplists:to_map(CurProplist),
    set_env(oauth_providers, maps:put(OAuthProviderId,
        maps:to_list(maps:filter(fun(K,_V) ->
            not proplists:is_defined(K, PropertyNameList) end, CurMap)),
        OAuthProviders)).

-spec ssl_options(ssl:verify_type(), boolean(), file:filename()) -> list().
ssl_options(PeerVerification, FailIfNoPeerCert, CaCertFile) ->
    [{verify, PeerVerification},
    {depth, 10},
    {fail_if_no_peer_cert, FailIfNoPeerCert},
    {crl_check, false},
    {crl_cache, {ssl_crl_cache, {internal, [{http, 10000}]}}},
    {cacertfile, CaCertFile}].
