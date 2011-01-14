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
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

%% stomp_frame implements the STOMP framing protocol "version 1.0", as
%% per http://stomp.codehaus.org/Protocol

-module(rabbit_stomp_frame).

-include("rabbit_stomp_frame.hrl").

-export([parse_headers/2, initial_headers_state/0]).
-export([parse_body/2, initial_body_state/1]).
-export([parse/2, initial_state/0]).
-export([header/2, header/3,
         boolean_header/2, boolean_header/3,
         integer_header/2, integer_header/3,
         binary_header/2, binary_header/3]).
-export([serialize/1]).

-record(hstate, {state, acc, key, command, headers}).
-record(bstate, {chunk, chunks, chunksize, remaining}).

-define(CHUNK_SIZE_LIMIT, 32768).

%% States:
%%  command . u(H) . key . value . (H + 0)

initial_headers_state() ->
    #hstate{state = command, acc = [], headers = []}.

parse_headers([], ParseState) ->
    {more, ParseState};
parse_headers([$\r | Rest], ParseState = #hstate{state = State})
  when State == command  orelse
       State == key      orelse
       State == value ->
    parse_headers(Rest, ParseState);
parse_headers([$\n | Rest], ParseState = #hstate{state = command, acc = []}) ->
    parse_headers(Rest, ParseState);
parse_headers([0 | Rest], ParseState = #hstate{state = command, acc = []}) ->
    parse_headers(Rest, ParseState);
parse_headers([$\n | Rest], ParseState = #hstate{state = command, acc = Acc}) ->
    parse_headers(Rest, ParseState#hstate{state = key, acc = [],
                                          command = lists:reverse(Acc)});
parse_headers([$\n | Rest], _ParseState = #hstate{state = key, acc = Acc,
                                                  command = Command,
                                                  headers = Headers}) ->
    case Acc of
        [] -> {ok, Command, Headers, Rest};
        _  -> {error, {bad_header_key, lists:reverse(Acc)}}
    end;
parse_headers([$: | Rest], ParseState = #hstate{state = key, acc = Acc}) ->
    parse_headers(Rest, ParseState#hstate{state = value, acc = [],
                                          key = lists:reverse(Acc)});
parse_headers([$\\, Ch | Rest], ParseState = #hstate{state = value,
                                                     acc   = Acc}) ->
    case unescape(Ch) of
        {ok, EscCh} ->
            parse_headers(Rest, ParseState#hstate{acc = [EscCh | Acc]});
        error ->
            {error, {bad_escape, Ch}}
    end;
parse_headers([$\n | Rest], ParseState = #hstate{state = value, acc = Acc,
                                                 key = Key,
                                                 headers = Headers}) ->
    NewHeaders = case lists:keysearch(Key, 1, Headers) of
                     {value, _} -> Headers;
                     false      -> [{Key, lists:reverse(Acc)} | Headers]
                 end,
    parse_headers(Rest, ParseState#hstate{state = key, acc = [],
                                          headers = NewHeaders});
parse_headers([Ch | Rest], ParseState = #hstate{acc = Acc}) ->
    if
        Ch < 32 -> {error, {bad_character, Ch}};
        true    -> parse_headers(Rest, ParseState#hstate{acc = [Ch | Acc]})
    end.

default_value({ok, Value}, _DefaultValue) ->
    Value;
default_value(not_found, DefaultValue) ->
    DefaultValue.

header(#stomp_frame{headers = Headers}, Key) ->
    case lists:keysearch(Key, 1, Headers) of
        {value, {_, Str}} -> {ok, Str};
        _                 -> not_found
    end.

header(Frame, Key, DefaultValue) ->
    default_value(header(Frame, Key), DefaultValue).

boolean_header(#stomp_frame{headers = Headers}, Key) ->
    case lists:keysearch(Key, 1, Headers) of
        {value, {_, "true"}}  -> {ok, true};
        {value, {_, "false"}} -> {ok, false};
        _                     -> not_found
    end.

boolean_header(H, Key, D) ->
    default_value(boolean_header(H, Key), D).

internal_integer_header(Headers, Key) ->
    case lists:keysearch(Key, 1, Headers) of
        {value, {_, Str}} -> {ok, list_to_integer(string:strip(Str))};
        _                 -> not_found
    end.

integer_header(#stomp_frame{headers = Headers}, Key) ->
    internal_integer_header(Headers, Key).

integer_header(H, Key, D) ->
    default_value(integer_header(H, Key), D).

binary_header(F, K) ->
    case header(F, K) of
        {ok, Str} -> {ok, list_to_binary(Str)};
        not_found -> not_found
    end.

binary_header(F, K, V) ->
    default_value(binary_header(F, K), V).

initial_body_state(Headers) ->
    Remaining = case internal_integer_header(Headers, "content-length") of
                    {ok, ByteCount} -> ByteCount;
                    not_found       -> unknown
                end,
    #bstate{chunk = [], chunks = [], chunksize = 0, remaining = Remaining}.

finalize_body(_State = #bstate{chunk = Chunk, chunks = Chunks}) ->
    lists:reverse(case Chunk of
                      [] -> Chunks;
                      _ -> [finalize_chunk(Chunk) | Chunks]
                  end).

finalize_chunk(Chunk) ->
    list_to_binary(lists:reverse(Chunk)).

accumulate_byte(Ch, NewRemaining,
                State = #bstate{chunk = Chunk,
                                chunks = Chunks,
                                chunksize = ?CHUNK_SIZE_LIMIT}) ->
    State#bstate{chunk = [Ch],
                 chunks = [finalize_chunk(Chunk) | Chunks],
                 chunksize = 0,
                 remaining = NewRemaining};
accumulate_byte(Ch, NewRemaining, State = #bstate{chunk = Chunk,
                                                  chunksize = ChunkSize}) ->
    State#bstate{chunk = [Ch | Chunk],
                 chunksize = ChunkSize + 1,
                 remaining = NewRemaining}.

parse_body([], State) ->
    {more, State};
parse_body([0 | Rest], State = #bstate{remaining = 0}) ->
    {ok, finalize_body(State), Rest};
parse_body([0 | Rest], State = #bstate{remaining = unknown}) ->
    {ok, finalize_body(State), Rest};
parse_body([Ch | Rest], State = #bstate{remaining = Remaining}) ->
    case Remaining of
        unknown ->
            parse_body(Rest, accumulate_byte(Ch, unknown, State));
        0 ->
            {error, missing_body_terminator};
        N ->
            parse_body(Rest, accumulate_byte(Ch, N - 1, State))
    end.

initial_state() ->
    {headers, initial_headers_state()}.

parse(Rest, {headers, HState}) ->
    case parse_headers(Rest, HState) of
        {more, HState1} ->
            {more, {headers, HState1}};
        {ok, Command, Headers, Rest1} ->
            parse(Rest1,
                  #stomp_frame{command = Command,
                               headers = Headers,
                               body_iolist = initial_body_state(Headers)});
        E = {error, _} ->
            E
    end;
parse(Rest, Frame = #stomp_frame{body_iolist = BState}) ->
    case parse_body(Rest, BState) of
        {more, BState1} ->
            {more, Frame#stomp_frame{body_iolist = BState1}};
        {ok, Body, Rest1} ->
            {ok, Frame#stomp_frame{body_iolist = Body}, Rest1};
        E = {error, _} ->
            E
    end.

serialize(#stomp_frame{command = Command,
                       headers = Headers,
                       body_iolist = BodyFragments}) ->
    Len = iolist_size(BodyFragments),
    [Command, $\n,
     lists:map(fun serialize_header/1,
               lists:keydelete("content-length", 1, Headers)),
     if
         Len > 0 -> ["content-length:", integer_to_list(Len), $\n];
         true    -> []
     end,
     $\n,
     BodyFragments,
     0].

serialize_header({K, V}) when is_integer(V) ->
    [K, $:, integer_to_list(V), $\n];
serialize_header({K, V}) when is_list(V) ->
    [K, $:, [escape(C) || C <- V], $\n].

unescape($n) ->
    {ok, $\n};
unescape($\\) ->
    {ok, $\\};
unescape($c) ->
    {ok, $:};
unescape(_) ->
    error.

escape($:) ->
    "\\c";
escape($\\) ->
    "\\\\";
escape($\n) ->
    "\\n";
escape(C) ->
    C.
