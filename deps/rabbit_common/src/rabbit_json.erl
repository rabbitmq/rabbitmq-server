%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_json).

-export([decode/1, decode/2, try_decode/1, try_decode/2,
	 encode/1, encode/2, try_encode/1, try_encode/2]).

-define(DEFAULT_DECODE_OPTIONS, [return_maps]).


-spec decode(jsx:json_text()) -> jsx:json_term().
decode(JSON) ->
    decode(JSON, ?DEFAULT_DECODE_OPTIONS).


-spec decode(jsx:json_text(), jsx_to_term:config()) -> jsx:json_term().
decode(JSON, Opts) ->
    jsx:decode(JSON, Opts).


-spec try_decode(jsx:json_text()) -> {ok, jsx:json_term()} |
				     {error, Reason :: term()}.
try_decode(JSON) ->
    try_decode(JSON, ?DEFAULT_DECODE_OPTIONS).


-spec try_decode(jsx:json_text(), jsx_to_term:config()) -> 
			{ok, jsx:json_term()} | {error, Reason :: term()}.
try_decode(JSON, Opts) ->
    try
        {ok, decode(JSON, Opts)}
    catch error: Reason ->
        {error, Reason}
    end.

-spec encode(jsx:json_term()) -> jsx:json_text().
encode(Term) ->
    encode(Term, []).

-spec encode(jsx:json_term(), jsx_to_json:config()) -> jsx:json_text().
encode(Term, Opts) ->
    jsx:encode(Term, Opts).


-spec try_encode(jsx:json_term()) -> {ok, jsx:json_text()} | 
				     {error, Reason :: term()}.
try_encode(Term) ->
    try_encode(Term, []).


-spec try_encode(jsx:json_term(), jsx_to_term:config()) ->
			{ok, jsx:json_text()} | {error, Reason :: term()}.
try_encode(Term, Opts) ->
    try
        {ok, encode(Term, Opts)}
    catch error: Reason ->
	    {error, Reason}
    end.
