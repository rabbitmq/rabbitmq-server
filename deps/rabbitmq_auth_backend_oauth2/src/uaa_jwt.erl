%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(uaa_jwt).

-export([add_signing_key/3,
         decode_and_verify/1,
         get_jwk/2,
         verify_signing_key/2]).

-export([client_id/1, sub/1, client_id/2, sub/2]).

-include_lib("jose/include/jose_jwk.hrl").
-include_lib("oauth2_client/include/oauth2_client.hrl").

-define(APP, rabbitmq_auth_backend_oauth2).

-type key_type() :: json | pem | map.

-spec add_signing_key(binary(), key_type(), binary() | map()) -> {ok, map()} | {error, term()}.
add_signing_key(KeyId, Type, Value) ->
    case verify_signing_key(Type, Value) of
        ok ->
            {ok, rabbit_oauth2_config:add_signing_key(KeyId, {Type, Value})};
        {error, _} = Err ->
            Err
    end.

-spec update_jwks_signing_keys(oauth_provider()) -> ok | {error, term()}.
update_jwks_signing_keys(#oauth_provider{id = Id, jwks_uri = JwksUrl,
        ssl_options = SslOptions}) ->
    rabbit_log:debug("Downloading signing keys from ~tp (TLS options: ~p)",
        [JwksUrl, SslOptions]),
    case uaa_jwks:get(JwksUrl, SslOptions) of
        {ok, {_, _, JwksBody}} ->
            KeyList = maps:get(<<"keys">>,
                jose:decode(erlang:iolist_to_binary(JwksBody)), []),
            Keys = maps:from_list(lists:map(fun(Key) ->
                {maps:get(<<"kid">>, Key, undefined), {json, Key}} end, KeyList)),
            rabbit_log:debug("Downloaded signing keys ~tp", [Keys]),
            case rabbit_oauth2_config:replace_signing_keys(Keys, Id) of
              {error, _} = Err -> Err;
              _ -> ok
            end;
        {error, _} = Err ->
            rabbit_log:error("Failed to download signing keys: ~tp", [Err]),
            Err
    end.

-spec decode_and_verify(binary()) -> {boolean(), binary(), map()} | {error, term()}.
decode_and_verify(Token) ->
    case resolve_resource_server_id(Token) of
        {error, _} = Err ->
            Err;
        ResourceServerId ->
            decode_and_verify(Token, ResourceServerId,
                rabbit_oauth2_config:get_oauth_provider_id_for_resource_server_id(
                    ResourceServerId))
    end.

decode_and_verify(Token, ResourceServerId, OAuthProviderId) ->
    rabbit_log:debug("Resolved resource_server_id: ~p -> oauth_provider_id: ~p",
        [ResourceServerId, OAuthProviderId]),
    case uaa_jwt_jwt:get_key_id(rabbit_oauth2_config:get_default_key(OAuthProviderId), Token) of
        {ok, KeyId} ->
            case get_jwk(KeyId, OAuthProviderId) of
                {ok, JWK} ->
                    Algorithms = rabbit_oauth2_config:get_algorithms(OAuthProviderId),
                    rabbit_log:debug("Verifying signature using signing_key_id : '~tp' and algorithms: ~p",
                        [KeyId, Algorithms]),
                    case uaa_jwt_jwt:decode_and_verify(Algorithms, JWK, Token) of
                        {true, Payload} -> {true, ResourceServerId, Payload};
                        {false, Payload} -> {false, ResourceServerId, Payload}
                    end;
                {error, _} = Err ->
                    Err
            end;
        {error, _} = Err -> Err
  end.

resolve_resource_server_id(Token) ->
    case uaa_jwt_jwt:get_aud(Token) of
        {error, _} = Error ->
            Error;
        {ok, Audience} ->
            rabbit_oauth2_config:get_resource_server_id_for_audience(Audience)
    end.

-spec get_jwk(binary(), oauth_provider_id()) -> {ok, map()} | {error, term()}.
get_jwk(KeyId, OAuthProviderId) ->
    get_jwk(KeyId, OAuthProviderId, true).

get_jwk(KeyId, OAuthProviderId, AllowUpdateJwks) ->
    case rabbit_oauth2_config:get_signing_key(KeyId, OAuthProviderId) of
        undefined ->
            if
                AllowUpdateJwks ->
                    rabbit_log:debug("OAuth 2 JWT: signing key '~tp' not found. Downloading it... ", [KeyId]),
                    case rabbit_oauth2_config:get_oauth_provider(OAuthProviderId, [jwks_uri]) of
                        {ok, OAuthProvider} ->
                            case update_jwks_signing_keys(OAuthProvider) of
                                ok ->
                                    get_jwk(KeyId, OAuthProviderId, false);
                                {error, no_jwks_url} ->
                                    {error, key_not_found};
                                {error, _} = Err ->
                                    Err
                            end;
                        {error, _} = Error ->
                            rabbit_log:debug("OAuth 2 JWT: unable to download keys due to ~p", [Error]),
                            Error
                    end;
                true            ->
                    rabbit_log:debug("OAuth 2 JWT: signing key '~tp' not found. Downloading is not allowed", [KeyId]),
                    {error, key_not_found}
            end;
        {Type, Value} ->
            rabbit_log:debug("OAuth 2 JWT: signing key found: '~tp', '~tp'", [Type, Value]),
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
