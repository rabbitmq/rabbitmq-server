%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%
-module(uaa_jwt_jwt).

-export([decode/1, decode_and_verify/3, get_key_id/2, get_aud/1, resolve_resource_server_id/1]).

-include_lib("jose/include/jose_jwt.hrl").
-include_lib("jose/include/jose_jws.hrl").

decode(Token) ->
    try
        #jose_jwt{fields = Fields} = jose_jwt:peek_payload(Token),
        Fields
    catch Type:Err:Stacktrace ->
        {error, {invalid_token, Type, Err, Stacktrace}}
    end.

decode_and_verify(ResourceServerId, Jwk, Token) ->
    KeyConfig = rabbit_oauth2_config:get_key_config(ResourceServerId),
    Verify =
        case proplists:get_value(algorithms, KeyConfig) of
            undefined ->
                jose_jwt:verify(Jwk, Token);
            Algs ->
                jose_jwt:verify_strict(Jwk, Algs, Token)
        end,
    case Verify of
        {true, #jose_jwt{fields = Fields}, _}  -> {true, Fields};
        {false, #jose_jwt{fields = Fields}, _} -> {false, Fields}
    end.


resolve_resource_server_id(Token) ->
  case get_aud(Token) of
    {error, _} = Error -> Error;
    undefined ->
      case rabbit_oauth2_config:is_verify_aud() of
        true -> {error, no_matching_aud_found};
        false -> rabbit_oauth2_config:get_default_resource_server_id()
      end;
    {ok, Audience} ->
      case rabbit_oauth2_config:find_audience_in_resource_server_ids(Audience) of
          {ok, ResourceServerId} -> ResourceServerId;
          {error, only_one_resource_server_as_audience_found_many} = Error -> Error;
          {error, no_matching_aud_found} ->
            case rabbit_oauth2_config:is_verify_aud() of
              true -> {error, no_matching_aud_found};
              false -> rabbit_oauth2_config:get_default_resource_server_id()
            end
      end
  end.

get_key_id(ResourceServerId, Token) ->
    try
        case jose_jwt:peek_protected(Token) of
            #jose_jws{fields = #{<<"kid">> := Kid}} -> {ok, Kid};
            #jose_jws{}                             -> get_default_key(ResourceServerId)
        end
    catch Type:Err:Stacktrace ->
        {error, {invalid_token, Type, Err, Stacktrace}}
    end.

get_aud(Token) ->
    try
        case jose_jwt:peek_payload(Token) of
            #jose_jwt{fields = #{<<"aud">> := Aud}} -> {ok, Aud};
            #jose_jwt{}                             -> undefined
        end
    catch Type:Err:Stacktrace ->
        {error, {invalid_token, Type, Err, Stacktrace}}
    end.


get_default_key(ResourceServerId) ->
    KeyConfig = rabbit_oauth2_config:get_key_config(ResourceServerId),
    case proplists:get_value(default_key, KeyConfig, undefined) of
        undefined -> {error, no_key};
        Val       -> {ok, Val}
    end.
