-module(rabbit_auth_backend_uaa_test_util).

-compile(export_all).

-define(EXPIRATION_TIME, 2000).

%%
%% API
%%

expirable_token() ->
    TokenPayload = fixture_token(),
    TokenPayload#{<<"exp">> := os:system_time(seconds) + timer:seconds(?EXPIRATION_TIME)}.

wait_for_token_to_expire() ->
    timer:sleep(?EXPIRATION_TIME).

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

fixture_token_with_scopes(Scopes) ->
    #{<<"exp">> => os:system_time(seconds) + 3000,
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

full_permission_scopes() ->
    [<<"rabbitmq.configure:*/*">>,
     <<"rabbitmq.write:*/*">>,
     <<"rabbitmq.read:*/*">>].

fixture_token_with_full_permissions() ->
    fixture_token_with_scopes(full_permission_scopes()).
