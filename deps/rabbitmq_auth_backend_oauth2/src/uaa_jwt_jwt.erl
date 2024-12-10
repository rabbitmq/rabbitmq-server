%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(uaa_jwt_jwt).

<<<<<<< HEAD
-export([decode_and_verify/3, get_key_id/2, get_aud/1]).
=======
-export([decode_and_verify/3, get_key_id/1, get_aud/1]).
>>>>>>> f3540ee7d2 (web_mqtt_shared_SUITE: propagate flow_classic_queue to mqtt_shared_SUITE #12907 12906)

-include_lib("jose/include/jose_jwt.hrl").
-include_lib("jose/include/jose_jws.hrl").


<<<<<<< HEAD
decode_and_verify(OauthProviderId, Jwk, Token) ->
    Verify =
        case rabbit_oauth2_config:get_algorithms(OauthProviderId) of
            undefined -> jose_jwt:verify(Jwk, Token);
            Algs -> jose_jwt:verify_strict(Jwk, Algs, Token)
        end,
=======
-spec decode_and_verify(list() | undefined, map(), binary()) -> {boolean(), map()}.
decode_and_verify(Algs, Jwk, Token) ->
    Verify = case Algs of
        undefined -> jose_jwt:verify(Jwk, Token);
        _ -> jose_jwt:verify_strict(Jwk, Algs, Token)
    end,
>>>>>>> f3540ee7d2 (web_mqtt_shared_SUITE: propagate flow_classic_queue to mqtt_shared_SUITE #12907 12906)
    case Verify of
        {true, #jose_jwt{fields = Fields}, _}  -> {true, Fields};
        {false, #jose_jwt{fields = Fields}, _} -> {false, Fields}
    end.

<<<<<<< HEAD

get_key_id(DefaultKey, Token) ->
    try
        case jose_jwt:peek_protected(Token) of
            #jose_jws{fields = #{<<"kid">> := Kid}} -> {ok, Kid};
            #jose_jws{}                             -> DefaultKey
=======
get_key_id(Token) ->
    try
        case jose_jwt:peek_protected(Token) of
            #jose_jws{fields = #{<<"kid">> := Kid}} -> {ok, Kid};
            #jose_jws{}                             -> undefined
>>>>>>> f3540ee7d2 (web_mqtt_shared_SUITE: propagate flow_classic_queue to mqtt_shared_SUITE #12907 12906)
        end
    catch Type:Err:Stacktrace ->
        {error, {invalid_token, Type, Err, Stacktrace}}
    end.

get_aud(Token) ->
    try
        case jose_jwt:peek_payload(Token) of
            #jose_jwt{fields = #{<<"aud">> := Aud}} -> {ok, Aud};
            #jose_jwt{}                             -> {ok, none}
        end
    catch Type:Err:Stacktrace ->
        {error, {invalid_token, Type, Err, Stacktrace}}
    end.
