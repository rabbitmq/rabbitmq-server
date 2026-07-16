%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_json).

-export([decode/1, decode/2, try_decode/1, try_decode/2,
	 encode/1, encode/2, try_encode/1, try_encode/2]).

-spec decode(iodata()) -> json:decode_value().
decode(JSON) ->
    json:decode(iolist_to_binary(JSON)).

-spec decode(iodata(), term()) -> json:decode_value().
decode(JSON, _Opts) ->
    decode(JSON).

-spec try_decode(iodata()) -> {ok, json:decode_value()} | {error, Reason :: term()}.
try_decode(JSON) ->
    try
        {ok, decode(JSON)}
    catch error:Reason ->
        {error, Reason}
    end.

-spec try_decode(iodata(), term()) -> {ok, json:decode_value()} | {error, Reason :: term()}.
try_decode(JSON, _Opts) ->
    try_decode(JSON).

-spec encode(term()) -> binary().
encode(Term) ->
    iolist_to_binary(json:encode(Term, fun encode_value/2)).

-spec encode(term(), term()) -> binary().
encode(Term, _Opts) ->
    encode(Term).

-spec try_encode(term()) -> {ok, binary()} | {error, Reason :: term()}.
try_encode(Term) ->
    try
        {ok, encode(Term)}
    catch error:Reason ->
	    {error, Reason}
    end.

-spec try_encode(term(), term()) -> {ok, binary()} | {error, Reason :: term()}.
try_encode(Term, _Opts) ->
    try_encode(Term).

encode_value(V, Encode) when is_function(V) ->
    json:encode_value(rabbit_data_coercion:to_binary(V), Encode);
encode_value([{_, _} | _] = List, Encode) ->
    json:encode_key_value_list(List, Encode);
%% IPv4 address tuple, e.g. {192, 168, 1, 1}
encode_value({A, B, C, D} = IP, Encode)
  when is_integer(A), is_integer(B), is_integer(C), is_integer(D) ->
    json:encode_value(list_to_binary(rabbit_misc:ntoa(IP)), Encode);
%% IPv6 address tuple, e.g. {0, 0, 0, 0, 0, 0, 0, 1}
encode_value({A, B, C, D, E, F, G, H} = IP, Encode)
  when is_integer(A), is_integer(B), is_integer(C), is_integer(D),
       is_integer(E), is_integer(F), is_integer(G), is_integer(H) ->
    json:encode_value(list_to_binary(rabbit_misc:ntoa(IP)), Encode);
%% Unix domain socket address, e.g. the peer_host of a connection over a
%% Unix domain socket listener.
encode_value({local, Path} = Addr, Encode)
  when is_list(Path); is_binary(Path) ->
    json:encode_value(rabbit_misc:ntoab(Addr), Encode);
%% OTP 27's json:key/2 does not support string (list) keys.
%% Convert any list keys to binary to avoid a function_clause error.
encode_value(Map, Encode) when is_map(Map) ->
    NormMap = maps:fold(fun
        (K, V, Acc) when is_list(K) -> maps:put(list_to_binary(K), V, Acc);
        (K, V, Acc)                 -> maps:put(K, V, Acc)
    end, #{}, Map),
    json:encode_map(NormMap, Encode);
encode_value(Other, Encode) ->
    json:encode_value(Other, Encode).
