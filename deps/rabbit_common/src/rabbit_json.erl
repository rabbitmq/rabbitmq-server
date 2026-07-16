%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_json).

-export([decode/1, decode/2, try_decode/1, try_decode/2,
	 encode/1, encode/2, try_encode/1, try_encode/2]).

-define(DEFAULT_DECODE_OPTIONS, #{}).
-define(DEFAULT_ENCODE_OPTIONS, #{}).

-spec decode(iodata()) -> thoas:json_term().
decode(JSON) ->
    decode(JSON, ?DEFAULT_DECODE_OPTIONS).

-spec decode(iodata(), thoas:decode_options()) -> thoas:json_term().
decode(JSON, Opts) ->
    case thoas:decode(JSON, Opts) of
        {ok, Value}     -> Value;
        {error, _Error} -> error({error, {failed_to_decode_json, JSON}})
    end.

-spec try_decode(iodata()) -> {ok, thoas:json_term()} |
				     {error, Reason :: term()}.
try_decode(JSON) ->
    try_decode(JSON, ?DEFAULT_DECODE_OPTIONS).

-spec try_decode(iodata(), thoas:decode_options()) ->
			{ok, thoas:json_term()} | {error, Reason :: term()}.
try_decode(JSON, Opts) ->
    try
        {ok, decode(JSON, Opts)}
    catch error:Reason ->
        {error, Reason}
    end.

-spec encode(thoas:input_term()) -> iodata().
encode(Term) ->
    encode(Term, ?DEFAULT_ENCODE_OPTIONS).

-spec encode(thoas:input_term(), thoas:encode_options()) -> iodata().
encode(Term, Opts) ->
    %% Fixup for JSON encoding
    %% * Transforms any Funs into strings
    %% * Transforms IPv4/IPv6 address tuples into their textual form
    %% See rabbit_mgmt_format:prepare_for_encoding/1
    F = fun
            (V) when is_function(V) ->
                rabbit_data_coercion:to_binary(V);
            ({A, B, C, D} = IP)
              when is_integer(A), is_integer(B), is_integer(C), is_integer(D) ->
                list_to_binary(rabbit_misc:ntoa(IP));
            ({A, B, C, D, E, G, H, I} = IP)
              when is_integer(A), is_integer(B), is_integer(C), is_integer(D),
                   is_integer(E), is_integer(G), is_integer(H), is_integer(I) ->
                list_to_binary(rabbit_misc:ntoa(IP));
            (V) ->
                V
        end,
    thoas:encode(fixup_terms(Term, F), Opts).

-spec try_encode(thoas:input_term()) -> {ok, iodata()} |
				     {error, Reason :: term()}.
try_encode(Term) ->
    try_encode(Term, ?DEFAULT_ENCODE_OPTIONS).

-spec try_encode(thoas:input_term(), thoas:encode_options()) ->
			{ok, iodata()} | {error, Reason :: term()}.
try_encode(Term, Opts) ->
    try
        {ok, encode(Term, Opts)}
    catch error:Reason ->
	    {error, Reason}
    end.

fixup_terms(Items, FixupFun) when is_list(Items) ->
    [fixup_item(Pair, FixupFun) || Pair <- Items];
fixup_terms(Items, FixupFun) when is_map(Items) ->
    maps:map(fun(_K, V) -> fixup_terms(V, FixupFun) end, Items);
fixup_terms(Item, FixupFun) ->
    fixup_item(Item, FixupFun).

<<<<<<< HEAD
fixup_item({Key, Value}, FixupFun) when is_list(Value); is_map(Value) ->
    {Key, fixup_terms(Value, FixupFun)};
fixup_item({Key, Value}, FixupFun) ->
    {Key, FixupFun(Value)};
fixup_item([{_K, _V} | _T] = L, FixupFun) ->
    fixup_terms(L, FixupFun);
fixup_item(Value, FixupFun) ->
    FixupFun(Value).
=======
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
>>>>>>> 57dcacb5eb (Encode Unix domain socket addresses in `rabbit_json`)
