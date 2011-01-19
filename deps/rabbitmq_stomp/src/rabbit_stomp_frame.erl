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

-export([parse/2, initial_state/0]).
-export([header/2, header/3,
         boolean_header/2, boolean_header/3,
         integer_header/2, integer_header/3,
         binary_header/2, binary_header/3]).
-export([serialize/1]).

-define(CHUNK_SIZE_LIMIT, 32768).

initial_state() ->
    none.

parse(Content, State) ->
    case State of
        {resume, Fun} ->
            Fun(Content);
        none  ->
            parse_command(Content, <<>>)
    end.

parse_command(<<$\n,  Rest/binary>>, <<>>) ->
    parse_command(Rest, <<>>);
parse_command(<<0,  Rest/binary>>, <<>>) ->
    parse_command(Rest, <<>>);
parse_command(<<$\n, Rest/binary>>, Acc) ->
    parse_headers(Rest, #stomp_frame{command = binary_to_list(Acc)}, [], <<>>);
parse_command(<<Ch:1/binary, Rest/binary>>, Acc) ->
    parse_command(Rest, <<Acc/binary, Ch/binary>>);
parse_command(<<>>, Acc) ->
    more(fun(Rest) -> parse_command(Rest, Acc) end).

parse_headers(<<>>, Frame, HeaderAcc, KeyAcc) ->
    more(fun(Rest) -> parse_headers(Rest, Frame, HeaderAcc, KeyAcc) end);
parse_headers(<<$\n, Rest/binary>>, Frame, HeaderAcc, _KeyAcc) ->
    Remaining = case internal_integer_header(HeaderAcc, "content-length") of
                    {ok, ByteCount} -> ByteCount;
                    not_found       -> unknown
                end,
    parse_body(Rest, Frame#stomp_frame{headers = HeaderAcc}, [], Remaining);
parse_headers(<<$:, Rest/binary>>, Frame, HeaderAcc, KeyAcc) ->
    parse_header_value(Rest, Frame, HeaderAcc, KeyAcc, <<>>);
parse_headers(<<Ch:1/binary, Rest/binary>>, Frame, HeaderAcc, KeyAcc) ->
    parse_headers(Rest, Frame, HeaderAcc, <<KeyAcc/binary, Ch/binary>>).

parse_header_value(<<>>, Frame, HeaderAcc, KeyAcc, ValAcc) ->
    more(fun(Rest) ->
                     parse_header_value(Rest, Frame, HeaderAcc, KeyAcc, ValAcc)
             end);
parse_header_value(<<$\n, Rest/binary>>, Frame, HeaderAcc, KeyAcc, ValAcc) ->
    NewKey = binary_to_list(KeyAcc),
    NewHeaders = case lists:keysearch(NewKey, 1, HeaderAcc) of
                     {value, _} -> HeaderAcc;
                     false      -> [{NewKey, binary_to_list(ValAcc)} |
                                    HeaderAcc]
                 end,
    parse_headers(Rest, Frame, NewHeaders, <<>>);
parse_header_value(<<$\\, Ch:1/binary,  Rest/binary>>, Frame,
                   HeaderAcc, KeyAcc, ValAcc) ->
    case unescape(Ch) of
        {ok, EscCh} ->
            parse_header_value(Rest, Frame, HeaderAcc, KeyAcc,
                               <<ValAcc/binary, EscCh/binary>>);
        error ->
            {error, {bad_escape, Ch}}
    end;
parse_header_value(<<Ch:1/binary, Rest/binary>>, Frame, HeaderAcc, KeyAcc,
                   ValAcc) ->
    parse_header_value(Rest, Frame, HeaderAcc, KeyAcc,
                       <<ValAcc/binary, Ch/binary>>).

parse_body(Content, Frame, Chunks, Remaining) ->
    case binary:match(Content, <<0>>) of
        nomatch ->
            finalize_chunk(Content, Chunks, Remaining,
                          fun(NewChunks, NewRemaining) ->
                                  more(fun(Rest) ->
                                               parse_body(Rest,
                                                          Frame,
                                                          NewChunks,
                                                          NewRemaining)
                                       end)
                          end);
        {Index, 1} ->
            <<Chunk:Index/binary, 0, Rest/binary>> = Content,

            finalize_chunk(Chunk, Chunks, Remaining,
                           fun(NewChunks, _) ->
                                   {ok,
                                    Frame#stomp_frame{
                                      body_iolist =
                                          lists:reverse(NewChunks)}, Rest}
                           end)
    end.

finalize_chunk(Chunk, Chunks, Remaining, ReturnFun) ->
    NewRemaining = case Remaining of
                       unknown -> Remaining;
                       _       -> Remaining - erlang:byte_size(Chunk)
                   end,
    case NewRemaining =:= unknown orelse NewRemaining >= 0 of
        true ->  ReturnFun(case Chunk of
                               [] -> Chunks;
                               _  -> [Chunk | Chunks]
                           end, NewRemaining);
        false -> {error, missing_body_terminator}
    end.

more(Continuation) ->
    {more, {resume, Continuation}}.

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
serialize_header({K, V}) when is_binary(V) ->
    serialize_header({K, binary_to_list(V)});
serialize_header({K, V}) when is_list(V) ->
    [K, $:, [escape(C) || C <- V], $\n].

unescape(<<$n>>) ->
    {ok, <<$\n>>};
unescape(<<$\\>>) ->
    {ok, <<$\\>>};
unescape(<<$c>>) ->
    {ok, <<$:>>};
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
