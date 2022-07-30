%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_json).

-export([decode/1, decode/2, try_decode/1, try_decode/2,
	 encode/1, encode/2, try_encode/1, try_encode/2]).

-define(DEFAULT_DECODE_OPTIONS, #{}).


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
    catch error: Reason ->
        {error, Reason}
    end.

-spec encode(thoas:json_term()) -> iodata().
encode(Term) ->
    encode(Term, []).

-spec encode(thoas:json_term(), thoas:encode_options()) -> iodata().
encode(Term, []) ->
    thoas:encode(fixup_terms(Term));
encode(Term, Opts) ->
    thoas:encode(fixup_terms(Term), Opts).

-spec try_encode(thoas:json_term()) -> {ok, iodata()} |
				     {error, Reason :: term()}.
try_encode(Term) ->
    try_encode(Term, []).

-spec try_encode(thoas:json_term(), thoas:decode_options()) ->
			{ok, iodata()} | {error, Reason :: term()}.
try_encode(Term, Opts) ->
    try
        {ok, encode(Term, Opts)}
    catch error: Reason ->
	    {error, Reason}
    end.

%% Fixup for JSON encoding. Transforms any Funs into strings
%% See rabbit_mgmt_format:format_nulls/1
fixup_terms(Items) when is_list(Items) ->
    [fixup_item(Pair) || Pair <- Items];
fixup_terms(Item) ->
    fixup_item(Item).

fixup_item({Key, Value}) when is_function(Value) ->
    {Key, rabbit_data_coercion:to_binary(Value)};
fixup_item({Key, Value}) when is_list(Value) ->
    {Key, fixup_terms(Value)};
fixup_item({Key, Value}) ->
    {Key, Value};
fixup_item([{_K, _V} | _T] = L) ->
    fixup_terms(L);
fixup_item(Value) when is_function(Value) ->
    rabbit_data_coercion:to_binary(Value);
fixup_item(Value) ->
    Value.
