%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(uaa_jwt_jwk).

-export([make_jwk/1, from_pem/1, from_pem_file/1]).

-include_lib("jose/include/jose_jwk.hrl").

-spec make_jwk(binary() | map()) -> {ok, #{binary() => binary()}} | {error, term()}.
make_jwk(Json) when is_binary(Json); is_list(Json) ->
    JsonMap = jose:decode(iolist_to_binary(Json)),
    make_jwk(JsonMap);

make_jwk(JsonMap) when is_map(JsonMap) ->
    case JsonMap of
        #{<<"kty">> := <<"MAC">>, <<"value">> := _Value} ->
            {ok, mac_to_oct(JsonMap)};
        #{<<"kty">> := <<"RSA">>, <<"n">> := _N, <<"e">> := _E} ->
            {ok, fix_alg(JsonMap)};
        #{<<"kty">> := <<"oct">>, <<"k">> := _K} ->
            {ok, fix_alg(JsonMap)};
        #{<<"kty">> := <<"OKP">>, <<"crv">> := _Crv, <<"x">> := _X} ->
            {ok, fix_alg(JsonMap)};
        #{<<"kty">> := <<"EC">>} ->
            {ok, fix_alg(JsonMap)};
        #{<<"kty">> := Kty} when Kty == <<"oct">>;
                                 Kty == <<"MAC">>;
                                 Kty == <<"RSA">>;
                                 Kty == <<"OKP">>;
                                 Kty == <<"EC">> ->
            {error, {fields_missing_for_kty, Kty}};
        #{<<"kty">> := _Kty} ->
            {error, unknown_kty};
        #{} ->
            {error, no_kty}
    end.

from_pem(Pem) ->
    case jose_jwk:from_pem(Pem) of
        #jose_jwk{} = Jwk -> {ok, Jwk};
        Other             ->
            error_logger:warning_msg("Error parsing jwk from pem: ", [Other]),
            {error, invalid_pem_string}
    end.

from_pem_file(FileName) ->
    case filelib:is_file(FileName) of
        false ->
            {error, enoent};
        true  ->
            case jose_jwk:from_pem_file(FileName) of
                #jose_jwk{} = Jwk -> {ok, Jwk};
                Other             ->
                    error_logger:warning_msg("Error parsing jwk from pem file: ", [Other]),
                    {error, invalid_pem_file}
            end
    end.

mac_to_oct(#{<<"kty">> := <<"MAC">>, <<"value">> := Value} = Key) ->
    OktKey = maps:merge(Key,
                        #{<<"kty">> => <<"oct">>,
                          <<"k">> => base64url:encode(Value)}),
    fix_alg(OktKey).

fix_alg(#{<<"alg">> := Alg} = Key) ->
    Algs = uaa_algs(),
    case maps:get(Alg, Algs, undefined) of
        undefined -> Key;
        Val       -> Key#{<<"alg">> := Val}
    end;
fix_alg(#{} = Key) -> Key.

uaa_algs() ->
    UaaEnv = application:get_env(rabbitmq_auth_backend_oauth2, uaa_jwt_decoder, []),
    DefaultAlgs = #{<<"HMACSHA256">> => <<"HS256">>,
                    <<"HMACSHA384">> => <<"HS384">>,
                    <<"HMACSHA512">> => <<"HS512">>,
                    <<"SHA256withRSA">> => <<"RS256">>,
                    <<"SHA512withRSA">> => <<"RS512">>},
    proplists:get_value(uaa_algs, UaaEnv, DefaultAlgs).
