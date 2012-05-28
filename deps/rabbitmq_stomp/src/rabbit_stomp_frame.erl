%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

%% stomp_frame implements the STOMP framing protocol "version 1.0", as
%% per http://stomp.codehaus.org/Protocol

-module(rabbit_stomp_frame).

-include("rabbit_stomp_frame.hrl").

-export([parse/2, initial_state/0]).
-export([header/2, header/3,
         boolean_header/2, boolean_header/3,
         integer_header/2, integer_header/3,
         binary_header/2, binary_header/3]).
-export([serialize/1]).

initial_state() -> none.

parse(Content, {resume, Fun}) -> Fun(Content);
parse(Content, none)          -> parse_command(Content, []).

parse_command(<<$\n,  Rest/binary>>, []) ->
    parse_command(Rest, []);
parse_command(<<$\r, Rest/binary>>, Acc) ->
    parse_command(Rest, Acc);
parse_command(<<0,  Rest/binary>>, []) ->
    parse_command(Rest, []);
parse_command(<<$\n, Rest/binary>>, Acc) ->
    parse_headers(Rest, #stomp_frame{command = lists:reverse(Acc)}, [], []);
parse_command(<<Ch:8, Rest/binary>>, Acc) ->
    parse_command(Rest, [Ch | Acc]);
parse_command(<<>>, Acc) ->
    more(fun(Rest) -> parse_command(Rest, Acc) end).

parse_headers(<<>>, Frame, HeaderAcc, KeyAcc) ->
    more(fun(Rest) -> parse_headers(Rest, Frame, HeaderAcc, KeyAcc) end);
parse_headers(<<$\r, Rest/binary>>, Frame, HeaderAcc, KeyAcc) ->
    parse_headers(Rest, Frame, HeaderAcc, KeyAcc);
parse_headers(<<$\n, Rest/binary>>, Frame, HeaderAcc, _KeyAcc) ->
    Remaining = case internal_integer_header(HeaderAcc, "content-length") of
                    {ok, ByteCount} -> ByteCount;
                    not_found       -> unknown
                end,
    parse_body(Rest, Frame#stomp_frame{headers = HeaderAcc}, [], Remaining);
parse_headers(<<$:, Rest/binary>>, Frame, HeaderAcc, KeyAcc) ->
    parse_header_value(Rest, Frame, HeaderAcc, KeyAcc, []);
parse_headers(<<Ch:8, Rest/binary>>, Frame, HeaderAcc, KeyAcc) ->
    parse_headers(Rest, Frame, HeaderAcc, [Ch | KeyAcc]).

parse_header_value(<<>>, Frame, HeaderAcc, KeyAcc, ValAcc) ->
    more(fun(Rest) ->
                     parse_header_value(Rest, Frame, HeaderAcc, KeyAcc, ValAcc)
             end);
parse_header_value(<<$\r, Rest/binary>>, Frame, HeaderAcc, KeyAcc, ValAcc) ->
    parse_header_value(Rest, Frame, HeaderAcc, KeyAcc, ValAcc);
parse_header_value(<<$\n, Rest/binary>>, Frame, HeaderAcc, KeyAcc, ValAcc) ->
    NewKey = lists:reverse(KeyAcc),
    NewHeaders = case lists:keysearch(NewKey, 1, HeaderAcc) of
                     {value, _} -> HeaderAcc;
                     false      -> [{NewKey, lists:reverse(ValAcc)} |
                                    HeaderAcc]
                 end,
    parse_headers(Rest, Frame, NewHeaders, []);
parse_header_value(<<$\\, Rest/binary>>, Frame,
                   HeaderAcc, KeyAcc, ValAcc) ->
    parse_header_value_escape(Rest, Frame, HeaderAcc, KeyAcc, ValAcc);
parse_header_value(<<Ch:8, Rest/binary>>, Frame, HeaderAcc, KeyAcc,
                   ValAcc) ->
    parse_header_value(Rest, Frame, HeaderAcc, KeyAcc, [Ch | ValAcc]).

parse_header_value_escape(<<>>, Frame, HeaderAcc, KeyAcc, ValAcc) ->
    more(fun(Rest) ->
           parse_header_value_escape(Rest, Frame, HeaderAcc, KeyAcc, ValAcc)
         end);
parse_header_value_escape(<<Ch:8,  Rest/binary>>, Frame,
                          HeaderAcc, KeyAcc, ValAcc) ->
    case unescape(Ch) of
        {ok, EscCh} -> parse_header_value(Rest, Frame, HeaderAcc, KeyAcc,
                                          [EscCh | ValAcc]);
        error       -> {error, {bad_escape, Ch}}
    end.

parse_body(Content, Frame, Chunks, unknown) ->
    case firstnull(Content) of
        -1  -> Chunks1 = finalize_chunk(Content, Chunks),
               more(fun(Rest) -> parse_body(Rest, Frame, Chunks1, unknown) end);
        Pos -> <<Chunk:Pos/binary, 0, Rest/binary>> = Content,
               Body = lists:reverse(finalize_chunk(Chunk, Chunks)),
               {ok, Frame#stomp_frame{body_iolist = Body}, Rest}
    end;
parse_body(Content, Frame, Chunks, Remaining) ->
    Size = byte_size(Content),
    case Remaining >= Size of
        true  -> Chunks1 = finalize_chunk(Content, Chunks),
                 Left = Remaining - Size,
                 more(fun(Rest) -> parse_body(Rest, Frame, Chunks1, Left) end);
        false -> <<Chunk:Remaining/binary, 0, Remainder/binary>> = Content,
                 Body = lists:reverse(finalize_chunk(Chunk, Chunks)),
                 {ok, Frame#stomp_frame{body_iolist = Body}, Remainder}
    end.

finalize_chunk(<<>>,  Chunks) -> Chunks;
finalize_chunk(Chunk, Chunks) -> [Chunk | Chunks].

more(Continuation) -> {more, {resume, Continuation}}.

default_value({ok, Value}, _DefaultValue) -> Value;
default_value(not_found,    DefaultValue) -> DefaultValue.

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

boolean_header(H, Key, D) -> default_value(boolean_header(H, Key), D).

internal_integer_header(Headers, Key) ->
    case lists:keysearch(Key, 1, Headers) of
        {value, {_, Str}} -> {ok, list_to_integer(string:strip(Str))};
        _                 -> not_found
    end.

integer_header(#stomp_frame{headers = Headers}, Key) ->
    internal_integer_header(Headers, Key).

integer_header(H, Key, D) -> default_value(integer_header(H, Key), D).

binary_header(F, K) ->
    case header(F, K) of
        {ok, Str} -> {ok, list_to_binary(Str)};
        not_found -> not_found
    end.

binary_header(F, K, V) -> default_value(binary_header(F, K), V).

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
     $\n, BodyFragments, 0].

serialize_header({K, V}) when is_integer(V) -> [K, $:, integer_to_list(V), $\n];
serialize_header({K, V}) when is_list(V) -> [K, $:, [escape(C) || C <- V], $\n].

unescape($n)  -> {ok, $\n};
unescape($\\) -> {ok, $\\};
unescape($c)  -> {ok, $:};
unescape(_)   -> error.

escape($:)  -> "\\c";
escape($\\) -> "\\\\";
escape($\n) -> "\\n";
escape(C)   -> C.

firstnull(Content) -> firstnull(Content, 0).

firstnull(<<>>,                _N) -> -1;
firstnull(<<0,  _Rest/binary>>, N) -> N;
firstnull(<<_Ch, Rest/binary>>, N) -> firstnull(Rest, N+1).
