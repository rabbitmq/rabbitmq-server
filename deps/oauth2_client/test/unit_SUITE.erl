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

-define(UTIL_MOD, oauth2_client_test_util).

all() ->
[
   {group, ssl_options},
   {group, get_expiration_time}
].

groups() ->
[
  {ssl_options, [], [
    no_ssl_options_triggers_verify_peer,
    peer_verification_verify_none,
    peer_verification_verify_peer_with_cacertfile
  ]},
  {get_expiration_time, [], [
    access_token_response_without_expiration_time,
    access_token_response_with_expires_in,
    access_token_response_with_exp_in_access_token
  ]}
].


no_ssl_options_triggers_verify_peer(_) ->
  [
    {verify, verify_peer},
    {depth, 10},
    {crl_check,false},
    {fail_if_no_peer_cert,false},
    {cacerts, _CaCerts}
  ] = oauth2_client:extract_ssl_options_as_list(#{ }).

peer_verification_verify_none(_) ->
  [
    {verify, verify_none}
  ] = oauth2_client:extract_ssl_options_as_list(#{ peer_verification => verify_none }),
  [
    {verify, verify_none}
  ] = oauth2_client:extract_ssl_options_as_list(#{
    peer_verification => verify_none,
    cacertfile => "/tmp"
  }).


peer_verification_verify_peer_with_cacertfile(_) ->
  [
    {verify, verify_peer},
    {depth, 10},
    {crl_check,false},
    {fail_if_no_peer_cert,false},
    {cacertfile, "/tmp"}
  ] = oauth2_client:extract_ssl_options_as_list(#{
    cacertfile => "/tmp",
    peer_verification => verify_peer
  }).


access_token_response_with_expires_in(_) ->
  Jwk = ?UTIL_MOD:fixture_jwk(),
  ExpiresIn = os:system_time(seconds),
  AccessToken = ?UTIL_MOD:expirable_token_with_expiration_time(ExpiresIn),
  {_, EncodedToken} = ?UTIL_MOD:sign_token_hs(AccessToken, Jwk),
  AccessTokenResponse = #successful_access_token_response{
    access_token = EncodedToken,
    expires_in = ExpiresIn
  },
  ?assertEqual({ok, ExpiresIn}, oauth2_client:get_expiration_time(AccessTokenResponse)).

access_token_response_with_exp_in_access_token(_) ->
  Jwk = ?UTIL_MOD:fixture_jwk(),
  ExpiresIn = os:system_time(seconds),
  AccessToken = ?UTIL_MOD:expirable_token_with_expiration_time(ExpiresIn),
  {_, EncodedToken} = ?UTIL_MOD:sign_token_hs(AccessToken, Jwk),
  AccessTokenResponse = #successful_access_token_response{
    access_token = EncodedToken
  },
  ?assertEqual({ok, ExpiresIn}, oauth2_client:get_expiration_time(AccessTokenResponse)).

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
