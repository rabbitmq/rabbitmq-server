%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(system_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include_lib("oauth2_client.hrl").
-import(oauth2_client, [
    build_openid_discovery_endpoint/3
]).

-compile(export_all).

-define(MOCK_TOKEN_ENDPOINT, <<"/token">>).
-define(AUTH_PORT, 8000).
-define(ISSUER_PATH, "/somepath").
-define(CUSTOM_OPENID_CONFIGURATION_ENDPOINT, "/somepath").
-define(UTIL_MOD, oauth2_client_test_util).
-define(EXPIRES_IN_SECONDS, 10000).

all() ->
[
    {group, https_down},
    {group, https},
    {group, with_all_oauth_provider_settings}

].

groups() ->
[

    {with_all_oauth_provider_settings, [], [
        {group, verify_get_oauth_provider},
        jwks_uri_takes_precedence_over_jwks_url,
        jwks_url_is_used_in_absense_of_jwks_uri
    ]},
    {without_all_oauth_providers_settings, [], [
        {group, verify_get_oauth_provider}        
    ]},
    {verify_openid_configuration, [], [
        get_openid_configuration,
        get_openid_configuration_returns_partial_payload,
        get_openid_configuration_using_path,
        get_openid_configuration_using_path_and_custom_endpoint,
        get_openid_configuration_using_custom_endpoint
    ]},
    {verify_access_token, [], [
        grants_access_token,
        denies_access_token,
        auth_server_error,
        non_json_payload,
        grants_refresh_token,
        expiration_time_in_response_payload,
        expiration_time_in_token
    ]},
    {verify_get_oauth_provider, [], [
        get_oauth_provider,        
        {with_default_oauth_provider, [], [
            get_oauth_provider
        ]},
        get_oauth_provider_given_oauth_provider_id
    ]},

    {https_down, [], [
        connection_error
    ]},
    {https, [], [
        {group, verify_openid_configuration},
        grants_access_token,
        grants_refresh_token,
        ssl_connection_error,
        {group, without_all_oauth_providers_settings}
    ]}
].

init_per_suite(Config) ->
    [
        {jwks_url, build_jwks_uri("https", "/certs4url")},
        {jwks_uri, build_jwks_uri("https")},
        {denies_access_token, [ 
            {token_endpoint, denies_access_token_expectation()} ]},
        {auth_server_error, [ 
            {token_endpoint, auth_server_error_when_access_token_request_expectation()} ]},
        {non_json_payload, [ 
            {token_endpoint, non_json_payload_when_access_token_request_expectation()} ]},
        {grants_refresh_token, [ 
            {token_endpoint, grants_refresh_token_expectation()} ]}
      | Config].

end_per_suite(Config) ->
    Config.

init_per_group(https, Config) ->
    {ok, _} = application:ensure_all_started(inets),
    {ok, _} = application:ensure_all_started(ssl),
    application:ensure_all_started(cowboy),
    Config0 = rabbit_ct_helpers:run_setup_steps(Config),
    CertsDir = ?config(rmq_certsdir, Config0),
    CaCertFile = filename:join([CertsDir, "testca", "cacert.pem"]),
    WrongCaCertFile = filename:join([CertsDir, "server", "server.pem"]),
    [{group, https},        
        {oauth_provider_id, <<"uaa">>},
        {oauth_provider, build_https_oauth_provider(<<"uaa">>, CaCertFile)},
        {oauth_provider_with_issuer, keep_only_issuer_and_ssl_options(
            build_https_oauth_provider(<<"uaa">>, CaCertFile))},
        {issuer, build_issuer("https")},
        {oauth_provider_with_wrong_ca, build_https_oauth_provider(<<"uaa">>, WrongCaCertFile)} |
        Config0];

init_per_group(https_down, Config) ->
    {ok, _} = application:ensure_all_started(inets),
    {ok, _} = application:ensure_all_started(ssl),
    Config0 = rabbit_ct_helpers:run_setup_steps(Config),
    CertsDir = ?config(rmq_certsdir, Config0),
    CaCertFile = filename:join([CertsDir, "testca", "cacert.pem"]),

    [{issuer, build_issuer("https")},
        {oauth_provider_id, <<"uaa">>},
        {oauth_provider, build_https_oauth_provider(<<"uaa">>, CaCertFile)} | Config];

init_per_group(openid_configuration_with_path, Config) ->
    [{use_openid_configuration_with_path, true} | Config];

init_per_group(with_all_oauth_provider_settings, Config) ->
    Config0 = rabbit_ct_helpers:run_setup_steps(Config),
    CertsDir = ?config(rmq_certsdir, Config0),
    CaCertFile = filename:join([CertsDir, "testca", "cacert.pem"]),

    [{with_all_oauth_provider_settings, true},
     {oauth_provider_id, <<"uaa">>},
     {oauth_provider, build_https_oauth_provider(<<"uaa">>, CaCertFile)} | Config];

init_per_group(without_all_oauth_providers_settings, Config) ->
    Config0 = rabbit_ct_helpers:run_setup_steps(Config),
    CertsDir = ?config(rmq_certsdir, Config0),
    CaCertFile = filename:join([CertsDir, "testca", "cacert.pem"]),

    [{with_all_oauth_provider_settings, false},
        {oauth_provider_id, <<"uaa">>},
        {oauth_provider, keep_only_issuer_and_ssl_options(
            build_https_oauth_provider(<<"uaa">>, CaCertFile))} | Config];

init_per_group(with_default_oauth_provider, Config) ->
    OAuthProvider = ?config(oauth_provider, Config),
    application:set_env(rabbitmq_auth_backend_oauth2, default_oauth_provider,
        OAuthProvider#oauth_provider.id),
    Config;

init_per_group(_, Config) ->
    Config.


get_http_oauth_server_expectations(TestCase, Config) ->
    case ?config(TestCase, Config) of
        undefined ->
            [   {token_endpoint, build_http_mock_behaviour(build_http_access_token_request(),
                    build_http_200_access_token_response())},
                {get_openid_configuration, get_openid_configuration_http_expectation(TestCase)}
            ];
        Expectations ->
            Expectations
    end.
get_openid_configuration_http_expectation(TestCaseAtom) ->
    TestCase = binary_to_list(atom_to_binary(TestCaseAtom)),
    Payload = case string:find(TestCase, "returns_partial_payload") of
        nomatch ->
            build_http_get_openid_configuration_payload();
         _ ->
            List0 = proplists:delete(authorization_endpoint,
                build_http_get_openid_configuration_payload()),
            proplists:delete(end_session_endpoint, List0)
    end,
    Path = case string:find(TestCase, "path") of
        nomatch ->  "";
        _ ->        ?ISSUER_PATH
    end,
    Endpoint = case string:find(TestCase, "custom_endpoint") of
        nomatch ->  ?DEFAULT_OPENID_CONFIGURATION_PATH;
        _ ->        ?CUSTOM_OPENID_CONFIGURATION_ENDPOINT
    end,
    build_http_mock_behaviour(build_http_get_openid_configuration_request(Endpoint, Path),
        build_http_200_json_response(Payload)).

lookup_expectation(Endpoint, Config) ->
    proplists:get_value(Endpoint, ?config(oauth_server_expectations, Config)).



configure_all_oauth_provider_settings(Config) ->
    OAuthProvider = ?config(oauth_provider, Config),
    OAuthProviders = #{ ?config(oauth_provider_id, Config) =>
        oauth_provider_to_proplist(OAuthProvider) },

    application:set_env(rabbitmq_auth_backend_oauth2, issuer,
        OAuthProvider#oauth_provider.issuer),
    application:set_env(rabbitmq_auth_backend_oauth2, oauth_providers,
        OAuthProviders),
    application:set_env(rabbitmq_auth_backend_oauth2, token_endpoint,
        OAuthProvider#oauth_provider.token_endpoint),
    application:set_env(rabbitmq_auth_backend_oauth2, end_session_endpoint,
        OAuthProvider#oauth_provider.end_session_endpoint),
    application:set_env(rabbitmq_auth_backend_oauth2, authorization_endpoint,
        OAuthProvider#oauth_provider.authorization_endpoint),
    KeyConfig0 = 
        case OAuthProvider#oauth_provider.ssl_options of
            undefined ->
                [];
            _ ->
                [ {peer_verification, proplists:get_value(verify,
                    OAuthProvider#oauth_provider.ssl_options) },
                    {cacertfile, proplists:get_value(cacertfile,
                        OAuthProvider#oauth_provider.ssl_options) }
                ]
        end,
    KeyConfig = 
        case ?config(jwks_uri_type_of_config, Config) of 
            undefined ->
                application:set_env(rabbitmq_auth_backend_oauth2, jwks_uri,
                    OAuthProvider#oauth_provider.jwks_uri),
                KeyConfig0;
            only_jwks_uri -> 
                application:set_env(rabbitmq_auth_backend_oauth2, jwks_uri,
                    OAuthProvider#oauth_provider.jwks_uri),
                KeyConfig0; 
            only_jwks_url -> 
                [ { jwks_url, ?config(jwks_url, Config) } | KeyConfig0 ];
            both -> 
                application:set_env(rabbitmq_auth_backend_oauth2, jwks_uri,
                    OAuthProvider#oauth_provider.jwks_uri),
                [ { jwks_url, ?config(jwks_url, Config) } | KeyConfig0 ]
        end,
    application:set_env(rabbitmq_auth_backend_oauth2, key_config, KeyConfig).

configure_minimum_oauth_provider_settings(Config) ->
    OAuthProvider = ?config(oauth_provider, Config),
    OAuthProviders = #{ ?config(oauth_provider_id, Config) =>
        oauth_provider_to_proplist(OAuthProvider) },
    application:set_env(rabbitmq_auth_backend_oauth2, oauth_providers,
        OAuthProviders),
    application:set_env(rabbitmq_auth_backend_oauth2, issuer,
        OAuthProvider#oauth_provider.issuer),
    KeyConfig =
        case OAuthProvider#oauth_provider.ssl_options of
            undefined ->
                [];
            _ ->
                [{peer_verification, proplists:get_value(verify,
                    OAuthProvider#oauth_provider.ssl_options) },
                 {cacertfile, proplists:get_value(cacertfile,
                    OAuthProvider#oauth_provider.ssl_options) }
                ]
        end,
    application:set_env(rabbitmq_auth_backend_oauth2, key_config, KeyConfig).

init_per_testcase(TestCase, Config0) ->
    application:set_env(rabbitmq_auth_backend_oauth2, use_global_locks, false),

    Config = [case TestCase of 
        jwks_url_is_used_in_absense_of_jwks_uri -> 
            {jwks_uri_type_of_config, only_jwks_url};
        jwks_uri_takes_precedence_over_jwks_url -> 
            {jwks_uri_type_of_config, both};
        _ -> 
            {jwks_uri_type_of_config, only_jwks_uri}
        end | Config0],

    case ?config(with_all_oauth_provider_settings, Config) of
        false -> configure_minimum_oauth_provider_settings(Config);
        true -> configure_all_oauth_provider_settings(Config);
        undefined -> configure_all_oauth_provider_settings(Config)
    end,

    HttpOauthServerExpectations = get_http_oauth_server_expectations(TestCase, Config),
    ListOfExpectations = maps:values(proplists:to_map(HttpOauthServerExpectations)),

    case ?config(group, Config) of
        https ->
            start_https_oauth_server(?AUTH_PORT, ?config(rmq_certsdir, Config),
                ListOfExpectations);
        without_all_oauth_providers_settings ->
            start_https_oauth_server(?AUTH_PORT, ?config(rmq_certsdir, Config),
                ListOfExpectations);    
        _ ->
            do_nothing
    end,
    [{oauth_server_expectations, HttpOauthServerExpectations} | Config ].

end_per_testcase(_, Config) ->
    application:unset_env(rabbitmq_auth_backend_oauth2, oauth_providers),
    application:unset_env(rabbitmq_auth_backend_oauth2, issuer),
    application:unset_env(rabbitmq_auth_backend_oauth2, jwks_uri),
    application:unset_env(rabbitmq_auth_backend_oauth2, token_endpoint),
    application:unset_env(rabbitmq_auth_backend_oauth2, authorization_endpoint),
    application:unset_env(rabbitmq_auth_backend_oauth2, end_session_endpoint),
    application:unset_env(rabbitmq_auth_backend_oauth2, key_config),
    case ?config(group, Config) of
        https ->
            stop_https_auth_server();
        without_all_oauth_providers_settings ->
            stop_https_auth_server();
        _ ->
            do_nothing
    end,
    Config.

end_per_group(https_and_rabbitmq_node, Config) ->
    rabbit_ct_helpers:run_steps(Config, rabbit_ct_broker_helpers:teardown_steps());

end_per_group(with_default_oauth_provider, Config) ->
    application:unset_env(rabbitmq_auth_backend_oauth2, default_oauth_provider),
    Config;

end_per_group(_, Config) ->
    Config.

build_openid_discovery_endpoint(Issuer) ->
    build_openid_discovery_endpoint(Issuer, undefined, undefined).

build_openid_discovery_endpoint(Issuer, Path) ->
    build_openid_discovery_endpoint(Issuer, Path, undefined).

get_openid_configuration(Config) ->
    ExpectedOAuthProvider = ?config(oauth_provider, Config),
    SslOptions = [{ssl, ExpectedOAuthProvider#oauth_provider.ssl_options}],
    {ok, ActualOpenId} = oauth2_client:get_openid_configuration(
        build_openid_discovery_endpoint(build_issuer("https")),
        SslOptions),
    ExpectedOpenId = map_oauth_provider_to_openid_configuration(ExpectedOAuthProvider),
    assertOpenIdConfiguration(ExpectedOpenId, ActualOpenId).

map_oauth_provider_to_openid_configuration(OAuthProvider) ->
    #openid_configuration{
        issuer = OAuthProvider#oauth_provider.issuer,
        token_endpoint = OAuthProvider#oauth_provider.token_endpoint,
        end_session_endpoint = OAuthProvider#oauth_provider.end_session_endpoint,
        jwks_uri = OAuthProvider#oauth_provider.jwks_uri,
        authorization_endpoint = OAuthProvider#oauth_provider.authorization_endpoint
    }.
get_openid_configuration_returns_partial_payload(Config) ->
    ExpectedOAuthProvider0 = ?config(oauth_provider, Config),
    ExpectedOAuthProvider = #oauth_provider{
        issuer = ExpectedOAuthProvider0#oauth_provider.issuer,
        token_endpoint = ExpectedOAuthProvider0#oauth_provider.token_endpoint,
        jwks_uri = ExpectedOAuthProvider0#oauth_provider.jwks_uri},

    SslOptions = [{ssl, ExpectedOAuthProvider0#oauth_provider.ssl_options}],
    {ok, Actual} = oauth2_client:get_openid_configuration(
        build_openid_discovery_endpoint(build_issuer("https")),
        SslOptions),
    ExpectedOpenId = map_oauth_provider_to_openid_configuration(ExpectedOAuthProvider),
    assertOpenIdConfiguration(ExpectedOpenId, Actual).

get_openid_configuration_using_path(Config) ->
    ExpectedOAuthProvider = ?config(oauth_provider, Config),
    SslOptions = [{ssl, ExpectedOAuthProvider#oauth_provider.ssl_options}],
    {ok, Actual} = oauth2_client:get_openid_configuration(
        build_openid_discovery_endpoint(build_issuer("https", ?ISSUER_PATH)),
        SslOptions),
    ExpectedOpenId = map_oauth_provider_to_openid_configuration(ExpectedOAuthProvider),
    assertOpenIdConfiguration(ExpectedOpenId,Actual).
get_openid_configuration_using_path_and_custom_endpoint(Config) ->
    ExpectedOAuthProvider = ?config(oauth_provider, Config),
    SslOptions = [{ssl, ExpectedOAuthProvider#oauth_provider.ssl_options}],
    {ok, Actual} = oauth2_client:get_openid_configuration(
        build_openid_discovery_endpoint(build_issuer("https", ?ISSUER_PATH),
        ?CUSTOM_OPENID_CONFIGURATION_ENDPOINT), SslOptions),
    ExpectedOpenId = map_oauth_provider_to_openid_configuration(ExpectedOAuthProvider),
    assertOpenIdConfiguration(ExpectedOpenId, Actual).
get_openid_configuration_using_custom_endpoint(Config) ->
    ExpectedOAuthProvider = ?config(oauth_provider, Config),
    SslOptions = [{ssl, ExpectedOAuthProvider#oauth_provider.ssl_options}],
    {ok, Actual} = oauth2_client:get_openid_configuration(
        build_openid_discovery_endpoint(build_issuer("https"),
        ?CUSTOM_OPENID_CONFIGURATION_ENDPOINT), SslOptions),
    ExpectedOpenId = map_oauth_provider_to_openid_configuration(ExpectedOAuthProvider),
    assertOpenIdConfiguration(ExpectedOpenId, Actual).


assertOpenIdConfiguration(ExpectedOpenIdProvider, ActualOpenIdProvider) ->
    ?assertEqual(ExpectedOpenIdProvider#openid_configuration.issuer,
        ActualOpenIdProvider#openid_configuration.issuer),
    ?assertEqual(ExpectedOpenIdProvider#openid_configuration.jwks_uri,
        ActualOpenIdProvider#openid_configuration.jwks_uri),
    ?assertEqual(ExpectedOpenIdProvider#openid_configuration.end_session_endpoint,
        ActualOpenIdProvider#openid_configuration.end_session_endpoint),
    ?assertEqual(ExpectedOpenIdProvider#openid_configuration.token_endpoint,
        ActualOpenIdProvider#openid_configuration.token_endpoint),
    ?assertEqual(ExpectedOpenIdProvider#openid_configuration.authorization_endpoint,
        ActualOpenIdProvider#openid_configuration.authorization_endpoint).

expiration_time_in_response_payload(Config) ->
    #{request := #{parameters := Parameters},
        response := [ {code, 200}, {content_type, _CT}, {payload, _JsonPayload}] } =
            lookup_expectation(token_endpoint, Config),

    {ok, #successful_access_token_response{} = Response } =
            oauth2_client:get_access_token(?config(oauth_provider, Config),
                build_access_token_request(Parameters)),

    {ok, [{expires_in, 10000}]} = oauth2_client:get_expiration_time(
        Response#successful_access_token_response{expires_in = 10000}).

expiration_time_in_token(Config) ->
    #{request := #{parameters := Parameters},
        response := [ {code, 200}, {content_type, _CT}, {payload, _JsonPayload}] } =
            lookup_expectation(token_endpoint, Config),

    {ok, #successful_access_token_response{} = Response } =
            oauth2_client:get_access_token(?config(oauth_provider, Config),
                build_access_token_request(Parameters)),

    {ok, [{exp, ?EXPIRES_IN_SECONDS}]} = oauth2_client:get_expiration_time(Response).

grants_access_token_dynamically_resolving_oauth_provider(Config) ->
    #{request := #{parameters := Parameters},
        response := [ {code, 200}, {content_type, _CT}, {payload, JsonPayload}] } =
            lookup_expectation(token_endpoint, Config),

    {ok, #successful_access_token_response{access_token = AccessToken,
        token_type = TokenType} } =
            oauth2_client:get_access_token(?config(oauth_provider_id, Config),
                build_access_token_request(Parameters)),

    ?assertEqual(proplists:get_value(token_type, JsonPayload), TokenType),
    ?assertEqual(proplists:get_value(access_token, JsonPayload), AccessToken).

grants_access_token(Config) ->
    #{request := #{parameters := Parameters},
        response := [ {code, 200}, {content_type, _CT}, {payload, JsonPayload}] }
        = lookup_expectation(token_endpoint, Config),

    {ok, #successful_access_token_response{access_token = AccessToken,
        token_type = TokenType} } =
            oauth2_client:get_access_token(?config(oauth_provider, Config),
                build_access_token_request(Parameters)),
    ?assertEqual(proplists:get_value(token_type, JsonPayload), TokenType),
    ?assertEqual(proplists:get_value(access_token, JsonPayload), AccessToken).

grants_access_token_optional_parameters(Config) ->
    #{request := #{parameters := Parameters},
        response := [ {code, 200}, {content_type, _CT}, {payload, JsonPayload}] }
        = lookup_expectation(token_endpoint, Config),

    AccessTokenRequest0 = build_access_token_request(Parameters),
    AccessTokenRequest = AccessTokenRequest0#access_token_request{
        scope = "some-scope",
        extra_parameters = [{"param1", "value1"}]
    },
    {ok, #successful_access_token_response{access_token = AccessToken,
        token_type = TokenType} } =
            oauth2_client:get_access_token(?config(oauth_provider, Config),
                AccessTokenRequest),
    ?assertEqual(proplists:get_value(token_type, JsonPayload), TokenType),
    ?assertEqual(proplists:get_value(access_token, JsonPayload), AccessToken).

grants_refresh_token(Config) ->
    #{request := #{parameters := Parameters},
        response := [ {code, 200}, {content_type, _CT}, {payload, JsonPayload}] }
        = lookup_expectation(token_endpoint, Config),

    {ok, #successful_access_token_response{access_token = AccessToken,
        token_type = TokenType} } =
            oauth2_client:refresh_access_token(?config(oauth_provider, Config),
                build_refresh_token_request(Parameters)),
    ?assertEqual(proplists:get_value(token_type, JsonPayload), TokenType),
    ?assertEqual(proplists:get_value(access_token, JsonPayload), AccessToken).

denies_access_token(Config) ->
    #{request := #{parameters := Parameters},
        response := [ {code, 400}, {content_type, _CT}, {payload, JsonPayload}] }
        = lookup_expectation(token_endpoint, Config),
    {error, #unsuccessful_access_token_response{error = Error,
        error_description = ErrorDescription} } =
            oauth2_client:get_access_token(?config(oauth_provider, Config),
                build_access_token_request(Parameters)),
    ?assertEqual(proplists:get_value(error, JsonPayload), Error),
    ?assertEqual(proplists:get_value(error_description, JsonPayload), ErrorDescription).

auth_server_error(Config) ->
    #{request := #{parameters := Parameters},
        response := [ {code, 500} ] } = lookup_expectation(token_endpoint, Config),
    {error, "Internal Server Error"} =
        oauth2_client:get_access_token(?config(oauth_provider, Config),
            build_access_token_request(Parameters)).

non_json_payload(Config) ->
    #{request := #{parameters := Parameters}} = lookup_expectation(token_endpoint, Config),
    {error, {failed_to_decode_json, _ErrorArgs}} =
        oauth2_client:get_access_token(?config(oauth_provider, Config),
            build_access_token_request(Parameters)).

connection_error(Config) ->
    #{request := #{parameters := Parameters}} = lookup_expectation(token_endpoint, Config),
    {error, {failed_connect, _ErrorArgs} } = oauth2_client:get_access_token(
        ?config(oauth_provider, Config), build_access_token_request(Parameters)).


ssl_connection_error(Config) ->
    #{request := #{parameters := Parameters}} = lookup_expectation(token_endpoint, Config),

    {error, {failed_connect, _} } = oauth2_client:get_access_token(
        ?config(oauth_provider_with_wrong_ca, Config), build_access_token_request(Parameters)).

verify_get_oauth_provider_returns_root_oauth_provider() ->
    {ok, #oauth_provider{id = Id,
                        issuer = Issuer,
                        token_endpoint = TokenEndPoint,
                        jwks_uri = Jwks_uri}} =
        oauth2_client:get_oauth_provider([issuer, token_endpoint, jwks_uri]),
    ExpectedIssuer = get_env(issuer),
    ExpectedTokenEndPoint = get_env(token_endpoint),
    ExpectedJwks_uri = get_env(jwks_uri),
    ?assertEqual(root, Id),
    ?assertEqual(ExpectedIssuer, Issuer),
    ?assertEqual(ExpectedTokenEndPoint, TokenEndPoint),
    ?assertEqual(ExpectedJwks_uri, Jwks_uri).

verify_get_oauth_provider_returns_default_oauth_provider(DefaultOAuthProviderId) ->
    {ok, OAuthProvider1} =
        oauth2_client:get_oauth_provider([issuer, token_endpoint, jwks_uri]),
    {ok, OAuthProvider2} =
        oauth2_client:get_oauth_provider(DefaultOAuthProviderId,
            [issuer, token_endpoint, jwks_uri]),
    ?assertEqual(OAuthProvider1, OAuthProvider2).

get_oauth_provider(Config) ->
    case ?config(with_all_oauth_provider_settings, Config) of
        true ->
            case get_env(default_oauth_provider) of
                undefined ->
                    verify_get_oauth_provider_returns_root_oauth_provider();
                DefaultOAuthProviderId ->
                    verify_get_oauth_provider_returns_default_oauth_provider(DefaultOAuthProviderId)
            end;
        false ->
            #{response := [ {code, 200}, {content_type, _CT}, {payload, JsonPayload}] }
                = lookup_expectation(get_openid_configuration, Config),
            {ok, #oauth_provider{issuer = Issuer,
                                 token_endpoint = TokenEndPoint,
                                 jwks_uri = Jwks_uri}
            } = oauth2_client:get_oauth_provider([issuer, token_endpoint, jwks_uri]),

            ?assertEqual(proplists:get_value(issuer, JsonPayload), Issuer),
            ?assertEqual(proplists:get_value(token_endpoint, JsonPayload), TokenEndPoint),
            ?assertEqual(proplists:get_value(jwks_uri, JsonPayload), Jwks_uri)
    end.

get_oauth_provider_given_oauth_provider_id(Config) ->
    case ?config(with_all_oauth_provider_settings, Config) of
        true ->
            {ok, #oauth_provider{
                    id = Id,
                    issuer = Issuer,
                    token_endpoint = TokenEndPoint,
                    authorization_endpoint = AuthorizationEndpoint,
                    end_session_endpoint = EndSessionEndpoint,
                    jwks_uri = Jwks_uri}} =
                oauth2_client:get_oauth_provider(?config(oauth_provider_id, Config),
                    [issuer, token_endpoint, jwks_uri, authorization_endpoint,
                        end_session_endpoint]),

            OAuthProviders = get_env(oauth_providers, #{}),
            ExpectedProvider = maps:get(Id, OAuthProviders, []),
            ?assertEqual(proplists:get_value(issuer, ExpectedProvider),
                Issuer),
            ?assertEqual(proplists:get_value(token_endpoint, ExpectedProvider),
                TokenEndPoint),
            ?assertEqual(proplists:get_value(authorization_endpoint, ExpectedProvider),
                AuthorizationEndpoint),
            ?assertEqual(proplists:get_value(end_session_endpoint, ExpectedProvider),
                EndSessionEndpoint),
            ?assertEqual(proplists:get_value(jwks_uri, ExpectedProvider),
                Jwks_uri);
        false ->
            #{response := [ {code, 200}, {content_type, _CT}, {payload, JsonPayload}] }
                = lookup_expectation(get_openid_configuration, Config),

            {ok, #oauth_provider{
                    issuer = Issuer,
                    token_endpoint = TokenEndPoint,
                    authorization_endpoint = AuthorizationEndpoint,
                    end_session_endpoint = EndSessionEndpoint,
                    jwks_uri = Jwks_uri}} =
                oauth2_client:get_oauth_provider(?config(oauth_provider_id, Config),
                    [issuer, token_endpoint, jwks_uri, authorization_endpoint,
                        end_session_endpoint]),

            ?assertEqual(proplists:get_value(issuer, JsonPayload),
                Issuer),
            ?assertEqual(proplists:get_value(token_endpoint, JsonPayload),
                TokenEndPoint),
            ?assertEqual(proplists:get_value(authorization_endpoint, JsonPayload),
                AuthorizationEndpoint),
            ?assertEqual(proplists:get_value(end_session_endpoint, JsonPayload),
                EndSessionEndpoint),
            ?assertEqual(proplists:get_value(jwks_uri, JsonPayload),
                Jwks_uri)
    end.

jwks_url_is_used_in_absense_of_jwks_uri(Config) ->
    {ok, #oauth_provider{
        jwks_uri = Jwks_uri}} = oauth2_client:get_oauth_provider([jwks_uri]),                
    ?assertEqual(
        proplists:get_value(jwks_url, get_env(key_config, []), undefined), 
        Jwks_uri).

jwks_uri_takes_precedence_over_jwks_url(Config) ->
    {ok, #oauth_provider{
        jwks_uri = Jwks_uri}} = oauth2_client:get_oauth_provider([jwks_uri]),
    ?assertEqual(get_env(jwks_uri), Jwks_uri).


%%% HELPERS

build_issuer(Scheme) ->
    build_issuer(Scheme, "").
build_issuer(Scheme, Path) ->
    uri_string:recompose(#{scheme => Scheme,
                         host => "localhost",
                         port => rabbit_data_coercion:to_integer(?AUTH_PORT),
                         path => Path}).


build_token_endpoint_uri(Scheme) ->
    uri_string:recompose(#{scheme => Scheme,
                         host => "localhost",
                         port => rabbit_data_coercion:to_integer(?AUTH_PORT),
                         path => "/token"}).

build_jwks_uri(Scheme) ->
    build_jwks_uri(Scheme, "/certs").

build_jwks_uri(Scheme, Path) ->
    uri_string:recompose(#{scheme => Scheme,
                         host => "localhost",
                         port => rabbit_data_coercion:to_integer(?AUTH_PORT),
                         path => Path}).

build_access_token_request(Request) ->
    #access_token_request {
        client_id = proplists:get_value(?REQUEST_CLIENT_ID, Request),
        client_secret = proplists:get_value(?REQUEST_CLIENT_SECRET, Request)
    }.
build_refresh_token_request(Request) ->
    #refresh_token_request{
        client_id = proplists:get_value(?REQUEST_CLIENT_ID, Request),
        client_secret = proplists:get_value(?REQUEST_CLIENT_SECRET, Request),
        refresh_token = proplists:get_value(?REQUEST_REFRESH_TOKEN, Request)
    }.
keep_only_issuer_and_ssl_options(OauthProvider) ->
    #oauth_provider {
      id = OauthProvider#oauth_provider.id,
      issuer = OauthProvider#oauth_provider.issuer,
      ssl_options = OauthProvider#oauth_provider.ssl_options
    }.
build_https_oauth_provider(Id, CaCertFile) ->
    #oauth_provider {
        id = Id,
        issuer = build_issuer("https"),
        authorization_endpoint = "https://localhost:8000/authorize",
        end_session_endpoint = "https://localhost:8000/logout",
        token_endpoint = build_token_endpoint_uri("https"),
        jwks_uri = build_jwks_uri("https"),
        ssl_options = ssl_options(verify_peer, false, CaCertFile)
    }.
oauth_provider_to_proplist(#oauth_provider{
        issuer = Issuer,
        token_endpoint = TokenEndpoint,
        end_session_endpoint = EndSessionEndpoint,
        authorization_endpoint = AuthorizationEndpoint,
        ssl_options = SslOptions,
        jwks_uri = Jwks_uri}) ->
    [   {issuer, Issuer},
        {token_endpoint, TokenEndpoint},
        {end_session_endpoint, EndSessionEndpoint},
        {authorization_endpoint, AuthorizationEndpoint},
        {https,
            case SslOptions of
                undefined -> [];
                Value -> Value
            end},
        {jwks_uri, Jwks_uri} ].


start_https_oauth_server(Port, CertsDir, Expectations) when is_list(Expectations) ->
    Dispatch = cowboy_router:compile([
        {'_', [{Path, oauth_http_mock, Expected} || #{request := #{path := Path}}
            = Expected <- Expectations ]}
    ]),
    {ok, _} = cowboy:start_tls(
        mock_http_auth_listener,
            [{port, Port},
            {certfile, filename:join([CertsDir, "server", "cert.pem"])},
            {keyfile, filename:join([CertsDir, "server", "key.pem"])}
            ],
            #{env => #{dispatch => Dispatch}});

start_https_oauth_server(Port, CertsDir, #{request := #{path := Path}} = Expected) ->
    Dispatch = cowboy_router:compile([{'_', [{Path, oauth_http_mock, Expected}]}]),
    {ok, _} = cowboy:start_tls(
        mock_http_auth_listener,
            [{port, Port},
            {certfile, filename:join([CertsDir, "server", "cert.pem"])},
            {keyfile, filename:join([CertsDir, "server", "key.pem"])}
            ],
            #{env => #{dispatch => Dispatch}}).

stop_https_auth_server() ->
    cowboy:stop_listener(mock_http_auth_listener).

-spec ssl_options(ssl:verify_type(), boolean(), file:filename()) -> list().
ssl_options(PeerVerification, FailIfNoPeerCert, CaCertFile) ->
    [{verify, PeerVerification},
        {depth, 10},
        {fail_if_no_peer_cert, FailIfNoPeerCert},
        {crl_check, false},
        {crl_cache, {ssl_crl_cache, {internal, [{http, 10000}]}}},
        {cacertfile, CaCertFile}].

token(ExpiresIn) ->
    Jwk = ?UTIL_MOD:fixture_jwk(),
    AccessToken = ?UTIL_MOD:expirable_token_with_expiration_time(ExpiresIn),
    {_, EncodedToken} = ?UTIL_MOD:sign_token_hs(AccessToken, Jwk),
    EncodedToken.


get_env(Par) ->
    application:get_env(rabbitmq_auth_backend_oauth2, Par, undefined).
get_env(Par, Default) ->
    application:get_env(rabbitmq_auth_backend_oauth2, Par, Default).


build_http_mock_behaviour(Request, Response) ->
    #{request => Request, response => Response}.
build_http_get_request(Path) ->
    build_http_get_request(Path, undefined).
build_http_get_request(Path, Parameters) ->
    build_http_request(<<"GET">>, Path, Parameters).
build_http_request(Method, Path, Parameters) when is_binary(Path) ->
    #{
        method => Method,
        path => Path,
        parameters => Parameters
    };
build_http_request(Method, Path, Parameters) ->
    Request = #{
        method => Method,
        path => list_to_binary(Path)
    },
    case Parameters of
        [] -> Request;
        undefined -> Request;
        _ -> maps:put(parameters, Parameters, Request)
    end.

build_http_get_openid_configuration_request() ->
    build_http_get_openid_configuration_request(?DEFAULT_OPENID_CONFIGURATION_PATH).
build_http_get_openid_configuration_request(Endpoint) ->
    build_http_get_openid_configuration_request(Endpoint, "").
build_http_get_openid_configuration_request(Endpoint, Path) ->
    build_http_get_request(Path ++ Endpoint).


build_http_200_json_response(Payload) ->
    build_http_response(200, ?CONTENT_JSON, Payload).

build_http_response(Code, ContentType, Payload) ->
    [
        {code, Code},
        {content_type, ContentType},
        {payload, Payload}
  ].
build_http_get_openid_configuration_payload() ->
    Scheme = "https",
    [
        {issuer, build_issuer(Scheme) },
        {authorization_endpoint, Scheme ++ "://localhost:8000/authorize"},
        {token_endpoint, build_token_endpoint_uri(Scheme)},
        {end_session_endpoint, Scheme ++ "://localhost:8000/logout"},
        {jwks_uri, build_jwks_uri(Scheme)}
    ].

build_http_access_token_request() ->
    build_http_request(
        <<"POST">>,
        ?MOCK_TOKEN_ENDPOINT,
        [
            {?REQUEST_CLIENT_ID, <<"guest">>},
            {?REQUEST_CLIENT_SECRET, <<"password">>}
        ]).
build_http_200_access_token_response() ->
    [
        {code, 200},
        {content_type, ?CONTENT_JSON},
        {payload, [
            {access_token, token(?EXPIRES_IN_SECONDS)},
            {token_type, <<"Bearer">>}
        ]}
    ].
build_http_400_access_token_response() ->
    [
        {code, 400},
        {content_type, ?CONTENT_JSON},
        {payload, [
            {error, <<"invalid_client">>},
            {error_description, <<"invalid client found">>}
        ]}
    ].
denies_access_token_expectation() ->
    build_http_mock_behaviour(build_http_request(
        <<"POST">>,
        ?MOCK_TOKEN_ENDPOINT,
        [
            {?REQUEST_CLIENT_ID, <<"invalid_client">>},
            {?REQUEST_CLIENT_SECRET, <<"password">>}
        ]), build_http_400_access_token_response()
    ).
auth_server_error_when_access_token_request_expectation() ->
    build_http_mock_behaviour(build_http_request(
        <<"POST">>,
        ?MOCK_TOKEN_ENDPOINT,
        [
            {?REQUEST_CLIENT_ID, <<"guest">>},
            {?REQUEST_CLIENT_SECRET, <<"password">>}
        ]), [{code, 500}]
    ).
non_json_payload_when_access_token_request_expectation() ->
    build_http_mock_behaviour(build_http_request(
        <<"POST">>,
        ?MOCK_TOKEN_ENDPOINT,
        [
            {?REQUEST_CLIENT_ID, <<"guest">>},
            {?REQUEST_CLIENT_SECRET, <<"password">>}
        ]), [
            {code, 400},
            {content_type, ?CONTENT_JSON},
            {payload, <<"{ some illegal json}">>}
        ]
    ).

grants_refresh_token_expectation() ->
    build_http_mock_behaviour(build_http_request(
        <<"POST">>,
        ?MOCK_TOKEN_ENDPOINT,
        [
            {?REQUEST_CLIENT_ID, <<"guest">>},
            {?REQUEST_CLIENT_SECRET, <<"password">>},
            {?REQUEST_REFRESH_TOKEN, <<"some refresh token">>}
        ]), build_http_200_access_token_response()
    ).
