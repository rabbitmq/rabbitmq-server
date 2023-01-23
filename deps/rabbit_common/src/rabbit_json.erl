%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
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
    %% See rabbit_mgmt_format:format_nulls/1
    F = fun
            (V) when is_function(V) ->
                rabbit_data_coercion:to_binary(V);
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
fixup_terms(Item, FixupFun) ->
    fixup_item(Item, FixupFun).

fixup_item({Key, Value}, FixupFun) when is_list(Value) ->
    {Key, fixup_terms(Value, FixupFun)};
fixup_item({Key, Value}, FixupFun) ->
    {Key, FixupFun(Value)};
fixup_item([{_K, _V} | _T] = L, FixupFun) ->
    fixup_terms(L, FixupFun);
fixup_item(Value, FixupFun) ->
    FixupFun(Value).
