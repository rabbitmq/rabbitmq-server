%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd.,
%%   Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd., Cohesive Financial
%%   Technologies LLC., and Rabbit Technologies Ltd. are Copyright (C) 
%%   2007 LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit 
%%   Technologies Ltd.; 
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

%% stomp_frame implements the STOMP framing protocol "version 1.0", as
%% per http://stomp.codehaus.org/Protocol

-module(stomp_frame).

-include("stomp_frame.hrl").

-export([parse_headers/2, initial_headers_state/0]).
-export([parse_body/2, initial_body_state/1]).
-export([parse/2, initial_state/0]).
-export([header/2, header/3, boolean_header/2, boolean_header/3, integer_header/2, integer_header/3, binary_header/2, binary_header/3]).
-export([serialize/1]).

-record(hstate, {state, acc, key, command, headers}).
-record(bstate, {acc, remaining}).

%% States:
%%  command . u(H) . key . eatspace . value . (H + 0)

initial_headers_state() ->
    #hstate{state = command, acc = [], headers = []}.

parse_headers([], ParseState) ->
    {more, ParseState};
parse_headers([$\r | Rest], ParseState = #hstate{state = State})
  when State == command orelse State == key orelse State == eatspace orelse State == value ->
    parse_headers(Rest, ParseState);
parse_headers([$\n | Rest], ParseState = #hstate{state = command, acc = []}) ->
    parse_headers(Rest, ParseState);
parse_headers([$\n | Rest], ParseState = #hstate{state = command, acc = Acc}) ->
    parse_headers(Rest, ParseState#hstate{state = key, acc = [],
					  command = lists:reverse(Acc)});
parse_headers([$\n | Rest], _ParseState = #hstate{state = key, acc = Acc,
						  command = Command, headers = Headers}) ->
    case Acc of
	[] ->
	    {ok, Command, Headers, Rest};
	_ ->
	    {error, {bad_header_key, lists:reverse(Acc)}}
    end;
parse_headers([$: | Rest], ParseState = #hstate{state = key, acc = Acc}) ->
    parse_headers(Rest, ParseState#hstate{state = eatspace, acc = [], key = lists:reverse(Acc)});
parse_headers([$  | Rest], ParseState = #hstate{state = eatspace}) ->
    parse_headers(Rest, ParseState);
parse_headers(Input, ParseState = #hstate{state = eatspace}) ->
    parse_headers(Input, ParseState#hstate{state = value});
parse_headers([$\n | Rest], ParseState = #hstate{state = value, acc = Acc, key = Key,
						 headers = Headers}) ->
    parse_headers(Rest, ParseState#hstate{state = key, acc = [],
					  headers = [{Key, lists:reverse(Acc)}
						     | Headers]});
parse_headers([Ch | Rest], ParseState = #hstate{acc = Acc}) ->
    if
	Ch < 32 ->
	    {error, {bad_character, Ch}};
	true ->
	    parse_headers(Rest, ParseState#hstate{acc = [Ch | Acc]})
    end.

header(#stomp_frame{headers = Headers}, Key) ->
    case lists:keysearch(Key, 1, Headers) of
	{value, {_, Str}} ->
	    {ok, Str};
	_ ->
	    not_found
    end.

header(#stomp_frame{headers = Headers}, Key, DefaultValue) ->
    case lists:keysearch(Key, 1, Headers) of
	{value, {_, Str}} ->
	    Str;
	_ ->
	    DefaultValue
    end.

boolean_header(#stomp_frame{headers = Headers}, Key) ->
    boolean_header(Headers, Key);
boolean_header(Headers, Key) ->
    case lists:keysearch(Key, 1, Headers) of
	{value, {_, "true"}} ->
	    {ok, true};
	{value, {_, "false"}} ->
	    {ok, false};
	_ ->
	    not_found
    end.

boolean_header(H, Key, D) ->
    case boolean_header(H, Key) of
	{ok, V} ->
	    V;
	not_found ->
	    D
    end.

integer_header(#stomp_frame{headers = Headers}, Key) ->
    integer_header(Headers, Key);
integer_header(Headers, Key) ->
    case lists:keysearch(Key, 1, Headers) of
	{value, {_, Str}} ->
	    {ok, list_to_integer(string:strip(Str))};
	_ ->
	    not_found
    end.

integer_header(H, Key, D) ->
    case integer_header(H, Key) of
	{ok, V} ->
	    V;
	not_found ->
	    D
    end.

binary_header(F, K) ->
    case header(F, K) of
	{ok, Str} -> {ok, list_to_binary(Str)};
	not_found -> not_found
    end.

binary_header(F, K, V) ->
    case header(F, K) of
	{ok, Str} -> list_to_binary(Str);
	not_found -> V
    end.

content_length(Headers) ->
    integer_header(Headers, "content-length").

initial_body_state(Headers) ->
    case content_length(Headers) of
	{ok, ByteCount} ->
	    #bstate{acc = [], remaining = ByteCount};
	not_found ->
	    #bstate{acc = [], remaining = unknown}
    end.

parse_body([], State) ->
    {more, State};
parse_body([0 | Rest], _State = #bstate{acc = Acc, remaining = 0}) ->
    {ok, lists:reverse(Acc), Rest};
parse_body([0 | Rest], _State = #bstate{acc = Acc, remaining = unknown}) ->
    {ok, lists:reverse(Acc), Rest};
parse_body([Ch | Rest], State = #bstate{acc = Acc, remaining = unknown}) ->
    parse_body(Rest, State#bstate{acc = [Ch | Acc]});
parse_body([Ch | Rest], State = #bstate{acc = Acc, remaining = N}) ->
    if
	N > 0 ->
	    parse_body(Rest, State#bstate{acc = [Ch | Acc], remaining = N - 1});
	true ->
	    {error, missing_body_terminator}
    end.

initial_state() ->
    {headers, initial_headers_state()}.

parse(Rest, {headers, HState}) ->
    case parse_headers(Rest, HState) of
	{more, HState1} ->
	    {more, {headers, HState1}};
	{ok, Command, Headers, Rest1} ->
	    parse(Rest1, #stomp_frame{command = Command,
				      headers = Headers,
				      body = initial_body_state(Headers)});
	E = {error, _} ->
	    E
    end;
parse(Rest, Frame = #stomp_frame{body = BState}) ->
    case parse_body(Rest, BState) of
	{more, BState1} ->
	    {more, Frame#stomp_frame{body = BState1}};
	{ok, Body, Rest1} ->
	    {ok, Frame#stomp_frame{body = Body}, Rest1};
	E = {error, _} ->
	    E
    end.

serialize(#stomp_frame{command = Command,
		       headers = Headers,
		       body = Body}) ->
    Len = length(Body),
    [Command, $\n,
     lists:map(fun serialize_header/1, lists:keydelete("content-length", 1, Headers)),
     if
	 Len > 0 ->
	     ["content-length:", integer_to_list(length(Body)), $\n];
	 true ->
	     []
     end,
     $\n,
     Body,
     0].

serialize_header({K, V}) when is_integer(V) ->
    [K, $:, integer_to_list(V), $\n];
serialize_header({K, V}) when is_list(V) ->
    [K, $:, V, $\n].

%% vi:noet:ts=8:sts=4:sw=4:cindent
