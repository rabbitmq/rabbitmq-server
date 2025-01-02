%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(rabbit_auth_backend_oauth2_test_util).

-compile(export_all).

-define(DEFAULT_EXPIRATION_IN_SECONDS, 2).

%%
%% API
%%

sign_token_hs(Token, Jwk) ->
    sign_token_hs(Token, Jwk, true).

sign_token_hs(Token, #{<<"kid">> := TokenKey} = Jwk, IncludeKid) ->
    sign_token_hs(Token, Jwk, TokenKey, IncludeKid).

%%sign_token_hs(Token, Jwk, TokenKey) ->
%%    sign_token_hs(Token, Jwk, TokenKey, true).

sign_token_hs(Token, Jwk, TokenKey, IncludeKid) ->
    Jws0 = #{
      <<"alg">> => <<"HS256">>,
      <<"kid">> => TokenKey
    },
    case IncludeKid of
        true ->
            Jws = maps:put(<<"kid">>, TokenKey, Jws0),
            sign_token(Token, Jwk, Jws);
        false ->
            sign_token_no_kid(Token, Jwk)
    end.

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

token_key(#{<<"kid">> := TokenKey} = _Token) ->
    TokenKey.

fixture_jwk() ->
    fixture_jwk(<<"token-key">>).

fixture_jwk(TokenKey) ->
    fixture_jwk(TokenKey, <<"dG9rZW5rZXk">>).

fixture_jwk(TokenKey, K) ->
    #{<<"alg">> => <<"HS256">>,
      <<"k">> => K,
      <<"kid">> => TokenKey,
      <<"kty">> => <<"oct">>,
      <<"use">> => <<"sig">>,
      <<"value">> => TokenKey}.

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
    token_with_scopes_and_expiration(Scopes, seconds_in_the_past(10)).

fixture_token_with_scopes(Scopes) ->
    token_with_scopes_and_expiration(Scopes, default_expiration_moment()).

token_with_scopes_and_expiration(Scopes, Expiration) ->
    %% expiration is a timestamp with precision in seconds
    #{<<"exp">> => Expiration,
      <<"iss">> => <<"unit_test">>,
      <<"foo">> => <<"bar">>,
      <<"aud">> => [<<"rabbitmq">>],
      <<"scope">> => Scopes}.

token_without_scopes() ->
    %% expiration is a timestamp with precision in seconds
    #{
      <<"iss">> => <<"unit_test">>,
      <<"foo">> => <<"bar">>,
      <<"aud">> => [<<"rabbitmq">>]
      }.

fixture_token() ->
    fixture_token([]).

token_with_sub(TokenFixture, Sub) ->
    maps:put(<<"sub">>, Sub, TokenFixture).
token_with_scopes(TokenFixture, Scopes) ->
    maps:put(<<"scope">>, Scopes, TokenFixture).

fixture_token(ExtraScopes) ->
    Scopes = [<<"rabbitmq.configure:vhost/foo">>,
              <<"rabbitmq.write:vhost/foo">>,
              <<"rabbitmq.read:vhost/foo">>,
              <<"rabbitmq.read:vhost/bar">>,
              <<"rabbitmq.read:vhost/bar/%23%2Ffoo">>] ++ ExtraScopes,
    fixture_token_with_scopes(Scopes).

fixture_token_with_full_permissions() ->
    fixture_token_with_scopes(full_permission_scopes()).

plain_token_without_scopes_and_aud() ->
  %% expiration is a timestamp with precision in seconds
  #{<<"exp">> => default_expiration_moment(),
    <<"iss">> => <<"unit_test">>,
    <<"foo">> => <<"bar">>}.

token_with_scope_alias_in_scope_field(Value) ->
    %% expiration is a timestamp with precision in seconds
    #{<<"exp">> => default_expiration_moment(),
      <<"iss">> => <<"unit_test">>,
      <<"foo">> => <<"bar">>,
      <<"aud">> => [<<"rabbitmq">>],
      <<"scope">> => Value}.

token_with_scope_alias_in_claim_field(Claims, Scopes) ->
    %% expiration is a timestamp with precision in seconds
    #{<<"exp">> => default_expiration_moment(),
      <<"iss">> => <<"unit_test">>,
      <<"foo">> => <<"bar">>,
      <<"aud">> => [<<"rabbitmq">>],
      <<"scope">>  => Scopes,
      <<"claims">> => Claims}.

seconds_in_the_future() ->
    seconds_in_the_future(30).

seconds_in_the_future(N) ->
    os:system_time(seconds) + N.

seconds_in_the_past() ->
    seconds_in_the_past(10).

seconds_in_the_past(N) ->
    os:system_time(seconds) - N.

default_expiration_moment() ->
    seconds_in_the_future(30).
