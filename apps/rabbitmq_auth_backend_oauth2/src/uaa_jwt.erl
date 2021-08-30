%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(uaa_jwt).

-export([add_signing_key/3,
         remove_signing_key/1,
         decode_and_verify/1,
         get_jwk/1,
         verify_signing_key/2,
         signing_keys/0]).

-export([client_id/1, sub/1, client_id/2, sub/2]).

-include_lib("jose/include/jose_jwk.hrl").

-define(APP, rabbitmq_auth_backend_oauth2).

-type key_type() :: json | pem | map.

-spec add_signing_key(binary(), key_type(), binary() | map()) -> {ok, map()} | {error, term()}.

add_signing_key(KeyId, Type, Value) ->
    case verify_signing_key(Type, Value) of
        ok ->
            SigningKeys0 = signing_keys(),
            SigningKeys1 = maps:put(KeyId, {Type, Value}, SigningKeys0),
            ok = update_uaa_jwt_signing_keys(SigningKeys1),
            {ok, SigningKeys1};
        {error, _} = Err ->
            Err
    end.

remove_signing_key(KeyId) ->
    UaaEnv = application:get_env(?APP, key_config, []),
    Keys0 = proplists:get_value(signing_keys, UaaEnv),
    Keys1 = maps:remove(KeyId, Keys0),
    update_uaa_jwt_signing_keys(UaaEnv, Keys1).

-spec update_uaa_jwt_signing_keys(map()) -> ok.
update_uaa_jwt_signing_keys(SigningKeys) ->
    UaaEnv0 = application:get_env(?APP, key_config, []),
    update_uaa_jwt_signing_keys(UaaEnv0, SigningKeys).

-spec update_uaa_jwt_signing_keys([term()], map()) -> ok.
update_uaa_jwt_signing_keys(UaaEnv0, SigningKeys) ->
    UaaEnv1 = proplists:delete(signing_keys, UaaEnv0),
    UaaEnv2 = [{signing_keys, SigningKeys} | UaaEnv1],
    application:set_env(?APP, key_config, UaaEnv2).

-spec update_jwks_signing_keys() -> ok | {error, term()}.
update_jwks_signing_keys() ->
    UaaEnv = application:get_env(?APP, key_config, []),
    case proplists:get_value(jwks_url, UaaEnv) of
        undefined ->
            {error, no_jwks_url};
        JwksUrl ->
            case httpc:request(JwksUrl) of
                {ok, {_, _, JwksBody}} ->
                    KeyList = maps:get(<<"keys">>, jose:decode(erlang:iolist_to_binary(JwksBody)), []),
                    Keys = maps:from_list(lists:map(fun(Key) -> {maps:get(<<"kid">>, Key, undefined), {json, Key}} end, KeyList)),
                    update_uaa_jwt_signing_keys(UaaEnv, Keys);
                {error, _} = Err ->
                    Err
            end
    end.

-spec decode_and_verify(binary()) -> {boolean(), map()} | {error, term()}.
decode_and_verify(Token) ->
    case uaa_jwt_jwt:get_key_id(Token) of
        {ok, KeyId} ->
            case get_jwk(KeyId) of
                {ok, JWK} ->
                    uaa_jwt_jwt:decode_and_verify(JWK, Token);
                {error, _} = Err ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

-spec get_jwk(binary()) -> {ok, map()} | {error, term()}.
get_jwk(KeyId) ->
    get_jwk(KeyId, true).

get_jwk(KeyId, AllowUpdateJwks) ->
    Keys = signing_keys(),
    case maps:get(KeyId, Keys, undefined) of
        undefined ->
            if
                AllowUpdateJwks ->
                    case update_jwks_signing_keys() of
                        ok ->
                            get_jwk(KeyId, false);
                        {error, no_jwks_url} ->
                            {error, key_not_found};
                        {error, _} = Err ->
                            Err
                    end;
                true            ->
                    {error, key_not_found}
            end;
        {Type, Value} ->
            case Type of
                json     -> uaa_jwt_jwk:make_jwk(Value);
                pem      -> uaa_jwt_jwk:from_pem(Value);
                pem_file -> uaa_jwt_jwk:from_pem_file(Value);
                map      -> uaa_jwt_jwk:make_jwk(Value);
                _        -> {error, unknown_signing_key_type}
            end
    end.

verify_signing_key(Type, Value) ->
    Verified = case Type of
        json     -> uaa_jwt_jwk:make_jwk(Value);
        pem      -> uaa_jwt_jwk:from_pem(Value);
        pem_file -> uaa_jwt_jwk:from_pem_file(Value);
        map      -> uaa_jwt_jwk:make_jwk(Value);
        _         -> {error, unknown_signing_key_type}
    end,
    case Verified of
        {ok, Key} ->
            case jose_jwk:from(Key) of
                #jose_jwk{}     -> ok;
                {error, Reason} -> {error, Reason}
            end;
        Err -> Err
    end.

signing_keys() ->
    UaaEnv = application:get_env(?APP, key_config, []),
    proplists:get_value(signing_keys, UaaEnv, #{}).

-spec client_id(map()) -> binary() | undefined.
client_id(DecodedToken) ->
    maps:get(<<"client_id">>, DecodedToken, undefined).

-spec client_id(map(), any()) -> binary() | undefined.
client_id(DecodedToken, Default) ->
    maps:get(<<"client_id">>, DecodedToken, Default).

-spec sub(map()) -> binary() | undefined.
sub(DecodedToken) ->
    maps:get(<<"sub">>, DecodedToken, undefined).

-spec sub(map(), any()) -> binary() | undefined.
sub(DecodedToken, Default) ->
    maps:get(<<"sub">>, DecodedToken, Default).
