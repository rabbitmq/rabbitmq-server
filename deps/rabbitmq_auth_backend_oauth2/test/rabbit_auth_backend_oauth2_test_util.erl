%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(rabbit_auth_backend_oauth2_test_util).

-compile(export_all).

-define(DEFAULT_EXPIRATION_IN_SECONDS, 2).

%%
%% API
%%

sign_token_hs(Token, #{<<"kid">> := TokenKey} = Jwk) ->
    sign_token_hs(Token, Jwk, TokenKey).

sign_token_hs(Token, Jwk, TokenKey) ->
    Jws = #{
      <<"alg">> => <<"HS256">>,
      <<"kid">> => TokenKey
    },
    sign_token(Token, Jwk, Jws).

sign_token_rsa(Token, Jwk, TokenKey) ->
    Jws = #{
      <<"alg">> => <<"RS256">>,
      <<"kid">> => TokenKey
    },
    sign_token(Token, Jwk, Jws).

sign_token_no_kid(Token, Jwk) ->
    Signed = jose_jwt:sign(Jwk, Token),
    jose_jws:compact(Signed).

sign_token(Token, Jwk, Jws) ->
    Signed = jose_jwt:sign(Jwk, Jws, Token),
    jose_jws:compact(Signed).

fixture_jwk() ->
    #{<<"alg">> => <<"HS256">>,
      <<"k">> => <<"dG9rZW5rZXk">>,
      <<"kid">> => <<"token-key">>,
      <<"kty">> => <<"oct">>,
      <<"use">> => <<"sig">>,
      <<"value">> => <<"tokenkey">>}.

full_permission_scopes() ->
    [<<"rabbitmq.configure:*/*">>,
     <<"rabbitmq.write:*/*">>,
     <<"rabbitmq.read:*/*">>].

expirable_token() ->
    expirable_token(?DEFAULT_EXPIRATION_IN_SECONDS).

expirable_token(Seconds) ->
    TokenPayload = fixture_token(),
    %% expiration is a timestamp with precision in seconds
    TokenPayload#{<<"exp">> := os:system_time(seconds) + Seconds}.

wait_for_token_to_expire() ->
    timer:sleep(timer:seconds(?DEFAULT_EXPIRATION_IN_SECONDS)).

wait_for_token_to_expire(DurationInMs) ->
    timer:sleep(DurationInMs).

expired_token() ->
    expired_token_with_scopes(full_permission_scopes()).

expired_token_with_scopes(Scopes) ->
    token_with_scopes_and_expiration(Scopes, os:system_time(seconds) - 10).

fixture_token_with_scopes(Scopes) ->
    token_with_scopes_and_expiration(Scopes, os:system_time(seconds) + 30).

token_with_scopes_and_expiration(Scopes, Expiration) ->
    %% expiration is a timestamp with precision in seconds
    #{<<"exp">> => Expiration,
      <<"kid">> => <<"token-key">>,
      <<"iss">> => <<"unit_test">>,
      <<"foo">> => <<"bar">>,
      <<"aud">> => [<<"rabbitmq">>],
      <<"scope">> => Scopes}.

fixture_token() ->
    fixture_token([]).

fixture_token(ExtraScopes) ->
    Scopes = [<<"rabbitmq.configure:vhost/foo">>,
              <<"rabbitmq.write:vhost/foo">>,
              <<"rabbitmq.read:vhost/foo">>,
              <<"rabbitmq.read:vhost/bar">>,
              <<"rabbitmq.read:vhost/bar/%23%2Ffoo">>] ++ ExtraScopes,
    fixture_token_with_scopes(Scopes).

fixture_token_with_full_permissions() ->
    fixture_token_with_scopes(full_permission_scopes()).
