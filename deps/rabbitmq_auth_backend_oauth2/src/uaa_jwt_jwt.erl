%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(uaa_jwt_jwt).

-export([decode_and_verify/3, get_key_id/1, get_aud/1]).

-include_lib("jose/include/jose_jwt.hrl").
-include_lib("jose/include/jose_jws.hrl").


-spec decode_and_verify(list() | undefined, map(), binary()) -> {boolean(), map()}.
decode_and_verify(Algs, Jwk, Token) ->
    Verify = case Algs of
        undefined -> jose_jwt:verify(Jwk, Token);
        _ -> jose_jwt:verify_strict(Jwk, Algs, Token)
    end,
    case Verify of
        {true, #jose_jwt{fields = Fields}, _}  -> {true, Fields};
        {false, #jose_jwt{fields = Fields}, _} -> {false, Fields}
    end.

get_key_id(Token) ->
    try
        case jose_jwt:peek_protected(Token) of
            #jose_jws{fields = #{<<"kid">> := Kid}} -> {ok, Kid};
            #jose_jws{}                             -> undefined
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
