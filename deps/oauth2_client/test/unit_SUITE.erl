%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include_lib("oauth2_client.hrl").
-include_lib("public_key/include/public_key.hrl").

-compile(export_all).

-import(oauth2_client, [build_openid_discovery_endpoint/3]).

-define(UTIL_MOD, oauth2_client_test_util).

all() ->
[
    build_openid_discovery_endpoint,
    {group, ssl_options},
    {group, merge},
    {group, get_expiration_time}
].

groups() ->
[
    {ssl_options, [], [
        no_ssl_options_triggers_verify_peer,
        choose_verify_over_peer_verification,
        verify_set_to_verify_none,
        peer_verification_set_to_verify_none,
        peer_verification_set_to_verify_peer_with_cacertfile,
        verify_set_to_verify_peer_with_cacertfile
    ]},
    {get_expiration_time, [], [
        access_token_response_without_expiration_time,
        access_token_response_with_expires_in,
        access_token_response_with_exp_in_access_token
    ]},
    {merge, [], [
        merge_openid_configuration,
        merge_oauth_provider
    ]}
].

build_openid_discovery_endpoint(_) ->
    Issuer = "https://issuer",
    ?assertEqual(Issuer ++ ?DEFAULT_OPENID_CONFIGURATION_PATH, 
        build_openid_discovery_endpoint(Issuer, undefined, undefined)),

    IssuerWithPath = "https://issuer/v2",
    ?assertEqual(IssuerWithPath ++ ?DEFAULT_OPENID_CONFIGURATION_PATH, 
        build_openid_discovery_endpoint(IssuerWithPath, undefined, undefined)),

    IssuerWithPathAndExtraPathSeparator = "https://issuer/v2/",
    ?assertEqual("https://issuer/v2" ++ ?DEFAULT_OPENID_CONFIGURATION_PATH, 
        build_openid_discovery_endpoint(IssuerWithPathAndExtraPathSeparator, 
            undefined, undefined)),

    IssuerWithPath = "https://issuer/v2",
    CustomPath = "/.well-known/other",
    ?assertEqual(IssuerWithPath ++ CustomPath, 
        build_openid_discovery_endpoint(IssuerWithPath, CustomPath, undefined)),
    
    IssuerWithPath = "https://issuer/v2",
    CustomPath = "/.well-known/other",
    WithParams = [{"param1", "v1"}, {"param2", "v2"}],
    ?assertEqual("https://issuer/v2/.well-known/other?param1=v1&param2=v2", 
        build_openid_discovery_endpoint(IssuerWithPath, CustomPath, WithParams)).
    

merge_oauth_provider(_) ->
    OAuthProvider = #oauth_provider{
        id = "some_id", 
        issuer = "https://issuer",
        discovery_endpoint = "https://issuer/.well-known/openid_configuration",
        ssl_options = [ {verify, verify_none} ]},
    Proplist = [],
    Proplist1 = oauth2_client:merge_oauth_provider(OAuthProvider, Proplist),
    ?assertEqual([], Proplist),

    OAuthProvider1 = OAuthProvider#oauth_provider{jwks_uri = "https://jwks_uri"},
    Proplist2 = oauth2_client:merge_oauth_provider(OAuthProvider1, Proplist1),
    ?assertEqual([{jwks_uri, OAuthProvider1#oauth_provider.jwks_uri}], Proplist2),

    OAuthProvider2 = OAuthProvider1#oauth_provider{end_session_endpoint = "https://end_session_endpoint"},
    Proplist3 = oauth2_client:merge_oauth_provider(OAuthProvider2, Proplist2),
    ?assertEqual([{jwks_uri, OAuthProvider2#oauth_provider.jwks_uri},
                  {end_session_endpoint, OAuthProvider2#oauth_provider.end_session_endpoint}],
                  Proplist3),

    OAuthProvider3 = OAuthProvider2#oauth_provider{authorization_endpoint = "https://authorization_endpoint"},
    Proplist4 = oauth2_client:merge_oauth_provider(OAuthProvider3, Proplist3),
    ?assertEqual([{jwks_uri, OAuthProvider3#oauth_provider.jwks_uri},
                  {end_session_endpoint, OAuthProvider3#oauth_provider.end_session_endpoint},
                  {authorization_endpoint, OAuthProvider3#oauth_provider.authorization_endpoint}],
                  Proplist4),

    OAuthProvider4 = OAuthProvider3#oauth_provider{token_endpoint = "https://token_endpoint"},
    Proplist5 = oauth2_client:merge_oauth_provider(OAuthProvider4, Proplist4),
    ?assertEqual([{jwks_uri, OAuthProvider4#oauth_provider.jwks_uri},
                  {end_session_endpoint, OAuthProvider4#oauth_provider.end_session_endpoint},
                  {authorization_endpoint, OAuthProvider4#oauth_provider.authorization_endpoint},
                  {token_endpoint, OAuthProvider4#oauth_provider.token_endpoint}],
                  Proplist5),

    % ensure id, issuer, ssl_options and discovery_endpoint are not affected
    ?assertEqual(OAuthProvider#oauth_provider.id, 
        OAuthProvider4#oauth_provider.id),
    ?assertEqual(OAuthProvider#oauth_provider.issuer, 
        OAuthProvider4#oauth_provider.issuer),
    ?assertEqual(OAuthProvider#oauth_provider.discovery_endpoint, 
        OAuthProvider4#oauth_provider.discovery_endpoint),
    ?assertEqual(OAuthProvider#oauth_provider.ssl_options, 
        OAuthProvider4#oauth_provider.ssl_options).

merge_openid_configuration(_) ->
    OpenIdConfiguration = #openid_configuration{},
    OAuthProvider = #oauth_provider{
        id = "some_id", 
        issuer = "https://issuer",
        discovery_endpoint = "https://issuer/.well-known/openid_configuration",
        ssl_options = [ {verify, verify_none} ]},
    OAuthProvider1 = oauth2_client:merge_openid_configuration(
        OpenIdConfiguration, OAuthProvider),
    ?assertEqual(OAuthProvider#oauth_provider.id, OAuthProvider1#oauth_provider.id),
    ?assertEqual([{verify, verify_none}], OAuthProvider1#oauth_provider.ssl_options),
    ?assertEqual(undefined, OAuthProvider1#oauth_provider.jwks_uri),
    ?assertEqual(undefined, OAuthProvider1#oauth_provider.token_endpoint),
    ?assertEqual(undefined, OAuthProvider1#oauth_provider.authorization_endpoint),
    ?assertEqual(undefined, OAuthProvider1#oauth_provider.end_session_endpoint),

    OpenIdConfiguration1 = #openid_configuration{jwks_uri = "https://jwks_uri"},
    OAuthProvider2 = oauth2_client:merge_openid_configuration(
        OpenIdConfiguration1, OAuthProvider1),
    ?assertEqual(OpenIdConfiguration1#openid_configuration.jwks_uri,
        OAuthProvider2#oauth_provider.jwks_uri),
    ?assertEqual(undefined, OAuthProvider2#oauth_provider.token_endpoint),
    ?assertEqual(undefined, OAuthProvider2#oauth_provider.authorization_endpoint),
    ?assertEqual(undefined, OAuthProvider2#oauth_provider.end_session_endpoint),

    OpenIdConfiguration2 = #openid_configuration{end_session_endpoint = "https://end_session_endpoint"},
    OAuthProvider3 = oauth2_client:merge_openid_configuration(
        OpenIdConfiguration2, OAuthProvider2),
    ?assertEqual(OpenIdConfiguration2#openid_configuration.end_session_endpoint,
        OAuthProvider3#oauth_provider.end_session_endpoint),
    ?assertEqual(undefined, OAuthProvider3#oauth_provider.authorization_endpoint),
    ?assertEqual(undefined, OAuthProvider3#oauth_provider.token_endpoint),

    OpenIdConfiguration3 = #openid_configuration{authorization_endpoint = "https://authorization_endpoint"},
    OAuthProvider4 = oauth2_client:merge_openid_configuration(
        OpenIdConfiguration3, OAuthProvider3),
    ?assertEqual(OpenIdConfiguration3#openid_configuration.authorization_endpoint,
        OAuthProvider4#oauth_provider.authorization_endpoint),
    ?assertEqual(undefined, OAuthProvider4#oauth_provider.token_endpoint),

    OpenIdConfiguration4 = #openid_configuration{token_endpoint = "https://token_endpoint"},
    OAuthProvider5 = oauth2_client:merge_openid_configuration(
        OpenIdConfiguration4, OAuthProvider4),
    ?assertEqual(OpenIdConfiguration4#openid_configuration.token_endpoint,
        OAuthProvider5#oauth_provider.token_endpoint),

    ?assertEqual(OpenIdConfiguration2#openid_configuration.end_session_endpoint,
        OAuthProvider5#oauth_provider.end_session_endpoint),
    ?assertEqual(OpenIdConfiguration3#openid_configuration.authorization_endpoint,
        OAuthProvider5#oauth_provider.authorization_endpoint),
    ?assertEqual(OpenIdConfiguration2#openid_configuration.end_session_endpoint,
        OAuthProvider5#oauth_provider.end_session_endpoint),
    ?assertEqual(OpenIdConfiguration1#openid_configuration.jwks_uri,
        OAuthProvider5#oauth_provider.jwks_uri),

     % ensure id, issuer, ssl_options and discovery_endpoint are not affected
    ?assertEqual(OAuthProvider#oauth_provider.id, 
        OAuthProvider5#oauth_provider.id),
    ?assertEqual(OAuthProvider#oauth_provider.issuer, 
        OAuthProvider5#oauth_provider.issuer),
    ?assertEqual(OAuthProvider#oauth_provider.discovery_endpoint, 
        OAuthProvider5#oauth_provider.discovery_endpoint),
    ?assertEqual(OAuthProvider#oauth_provider.ssl_options, 
        OAuthProvider5#oauth_provider.ssl_options).    


no_ssl_options_triggers_verify_peer(_) ->
    ?assertMatch([
        {verify, verify_peer},
        {depth, 10},
        {crl_check,false},
        {fail_if_no_peer_cert,false},
        {cacerts, _CaCerts}
    ], oauth2_client:extract_ssl_options_as_list(#{})).

choose_verify_over_peer_verification(_) ->
    Expected1 = [
        {verify, verify_none}
    ],
    ?assertEqual(Expected1, oauth2_client:extract_ssl_options_as_list(
        #{verify => verify_none, peer_verification => verify_peer })).

verify_set_to_verify_none(_) ->
    Expected1 = [
        {verify, verify_none}
    ],
    ?assertEqual(Expected1, oauth2_client:extract_ssl_options_as_list(#{verify => verify_none})),

    Expected2 = [
        {verify, verify_none}
    ],
    ?assertEqual(Expected2, oauth2_client:extract_ssl_options_as_list(#{
        verify => verify_none,
        cacertfile => "/tmp"
    })).


peer_verification_set_to_verify_none(_) ->
    Expected1 = [
        {verify, verify_none}
    ],
    ?assertEqual(Expected1, oauth2_client:extract_ssl_options_as_list(#{peer_verification => verify_none})),

    Expected2 = [
        {verify, verify_none}
    ],
    ?assertEqual(Expected2, oauth2_client:extract_ssl_options_as_list(#{
        peer_verification => verify_none,
        cacertfile => "/tmp"
    })).


peer_verification_set_to_verify_peer_with_cacertfile(_) ->
    Expected = [
        {verify, verify_peer},
        {depth, 10},
        {crl_check,false},
        {fail_if_no_peer_cert,false},
        {cacertfile, "/tmp"}
    ],
    ?assertEqual(Expected, oauth2_client:extract_ssl_options_as_list(#{
        cacertfile => "/tmp",
        peer_verification => verify_peer
    })).


verify_set_to_verify_peer_with_cacertfile(_) ->
    Expected = [
        {verify, verify_peer},
        {depth, 10},
        {crl_check,false},
        {fail_if_no_peer_cert,false},
        {cacertfile, "/tmp"}
    ],
    ?assertEqual(Expected, oauth2_client:extract_ssl_options_as_list(#{
        cacertfile => "/tmp",
        verify => verify_peer
    })).

access_token_response_with_expires_in(_) ->
    Jwk = ?UTIL_MOD:fixture_jwk(),
    ExpiresIn = os:system_time(seconds),
    AccessToken = ?UTIL_MOD:expirable_token_with_expiration_time(ExpiresIn),
    {_, EncodedToken} = ?UTIL_MOD:sign_token_hs(AccessToken, Jwk),
    AccessTokenResponse = #successful_access_token_response{
        access_token = EncodedToken,
        expires_in = ExpiresIn
    },
    ?assertEqual({ok, [{expires_in, ExpiresIn}]}, oauth2_client:get_expiration_time(AccessTokenResponse)).

access_token_response_with_exp_in_access_token(_) ->
    Jwk = ?UTIL_MOD:fixture_jwk(),
    ExpiresIn = os:system_time(seconds),
    AccessToken = ?UTIL_MOD:expirable_token_with_expiration_time(ExpiresIn),
    {_, EncodedToken} = ?UTIL_MOD:sign_token_hs(AccessToken, Jwk),
    AccessTokenResponse = #successful_access_token_response{
        access_token = EncodedToken
    },
    ?assertEqual({ok, [{exp, ExpiresIn}]}, oauth2_client:get_expiration_time(AccessTokenResponse)).

access_token_response_without_expiration_time(_) ->
    Jwk = ?UTIL_MOD:fixture_jwk(),
    AccessToken = maps:remove(<<"exp">>, ?UTIL_MOD:fixture_token()),
    ct:log("AccesToken ~p", [AccessToken]),
    {_, EncodedToken} = ?UTIL_MOD:sign_token_hs(AccessToken, Jwk),
    AccessTokenResponse = #successful_access_token_response{
        access_token = EncodedToken
    },
    ct:log("AccessTokenResponse ~p", [AccessTokenResponse]),
    ?assertEqual({error, missing_exp_field}, oauth2_client:get_expiration_time(AccessTokenResponse)).
