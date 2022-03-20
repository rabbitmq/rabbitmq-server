%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(uaa_jwt_jwt).

%% Transitional step until we can require Erlang/OTP 21 and
%% use the now recommended try/catch syntax for obtaining the stack trace.
-compile(nowarn_deprecated_function).

-export([decode/1, decode_and_verify/2, get_key_id/1]).

-include_lib("jose/include/jose_jwt.hrl").
-include_lib("jose/include/jose_jws.hrl").

decode(Token) ->
    try
        #jose_jwt{fields = Fields} = jose_jwt:peek_payload(Token),
        Fields
    catch Type:Err:Stacktrace ->
        {error, {invalid_token, Type, Err, Stacktrace}}
    end.

decode_and_verify(Jwk, Token) ->
    UaaEnv = application:get_env(rabbitmq_auth_backend_oauth2, key_config, []),
    Verify =
        case proplists:get_value(algorithms, UaaEnv) of
            undefined ->
                jose_jwt:verify(Jwk, Token);
            Algs ->
                jose_jwt:verify_strict(Jwk, Algs, Token)
        end,
    case Verify of
        {true, #jose_jwt{fields = Fields}, _}  -> {true, Fields};
        {false, #jose_jwt{fields = Fields}, _} -> {false, Fields}
    end.

get_key_id(Token) ->
    try
        case jose_jwt:peek_protected(Token) of
            #jose_jws{fields = #{<<"kid">> := Kid}} -> {ok, Kid};
            #jose_jws{}                             -> get_default_key()
        end
    catch Type:Err:Stacktrace ->
        {error, {invalid_token, Type, Err, Stacktrace}}
    end.


get_default_key() ->
    UaaEnv = application:get_env(rabbitmq_auth_backend_oauth2, key_config, []),
    case proplists:get_value(default_key, UaaEnv, undefined) of
        undefined -> {error, no_key};
        Val       -> {ok, Val}
    end.
