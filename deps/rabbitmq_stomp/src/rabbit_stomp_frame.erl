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
-include("rabbit_stomp_headers.hrl").

-export([parse/2, initial_state/0]).
-export([header/2, header/3,
         boolean_header/2, boolean_header/3,
         integer_header/2, integer_header/3,
         binary_header/2, binary_header/3]).
-export([serialize/1]).

initial_state() -> none.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% STOMP 1.1 frames basic syntax
%%  Rabbit modifications:
%%  o   CR LF is equivalent to LF in all element terminators (eol).
%%  o   Escape codes for header names and values include \r for CR
%%      and CR is not allowed.
%%  o   Header names and values are not limited to UTF-8 strings.
%%
%%  frame_seq   ::= (noise frame)*
%%  noise       ::= (NUL | eol)*
%%  eol         ::= LF | CR LF
%%  frame       ::= cmd hdrs body NUL
%%  body        ::= OCTET*
%%  cmd         ::= NOTEOL*1 eol
%%  hdrs        ::= hdr* eol
%%  hdr         ::= hdrname COLON hdrvalue eol
%%  hdrname     ::= esc_char*1
%%  hdrvalue    ::= esc_char*
%%  esc_char    ::= HDROCT | BACKSLASH ESCCODE
%%
%% Terms in CAPS all represent sets (alternatives) of single octets.
%% They are defined here using a small extension of BNF.
%%
%%  OCTET       ::= '00'x..'FF'x            % any octet
%%  NUL         ::= '00'x                   % the zero octet
%%  LF          ::= '\n'                    % '0a'x newline or linefeed
%%  CR          ::= '\r'                    % '0d'x carriage return
%%  NOTEOL      ::= OCTET - (CR | LF)       % any octet except CR or LF
%%  BACKSLASH   ::= '\\'                    % '5c'x
%%  ESCCODE     ::= 'c' | 'n' | 'r' | BACKSLASH
%%  COLON       ::= ':'
%%  HDROCT      ::= OCTET - (COLON | CR | LF | BACKSLASH)
%%                                          % octets allowed in a header
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% explicit frame characters
-define(NUL,   0).
-define(CR,    $\r).
-define(LF,    $\n).
-define(BSL,   $\\).
-define(COLON, $:).

%% header escape codes
-define(LF_ESC,    $n).
-define(BSL_ESC,   $\\).
-define(COLON_ESC, $c).
-define(CR_ESC,    $r).

parse(Content, {resume, Fun}) -> Fun(Content);
parse(Content, none         ) -> parse_noise(Content).

parse_noise(<<>>                       ) -> more(fun(Rest) -> parse_noise(Rest) end);
parse_noise(<<?CR>>                    ) -> more(fun(Rest) -> parse_noise(<<?CR, Rest/binary>>) end);
parse_noise(<<?NUL,       Rest/binary>>) -> parse_noise(Rest);
parse_noise(<<?LF,        Rest/binary>>) -> parse_noise(Rest);
parse_noise(<<?CR, ?LF,   Rest/binary>>) -> parse_noise(Rest);
parse_noise(<<?CR, Ch:8, _Rest/binary>>) -> {error, {unexpected_chars_between_frames, [?CR, Ch]}};
parse_noise(<<Ch:8,       Rest/binary>>) -> parse_command(Rest, [Ch]).

parse_command(<<>>,                         Acc) -> more(fun(Rest) -> parse_command(Rest, Acc) end);
parse_command(<<?CR>>,                      Acc) -> more(fun(Rest) -> parse_command(<<?CR, Rest/binary>>, Acc) end);
parse_command(<<?LF,        Rest/binary>>,  Acc) -> parse_headers(Rest, lists:reverse(Acc));
parse_command(<<?CR, ?LF,   Rest/binary>>,  Acc) -> parse_headers(Rest, lists:reverse(Acc));
parse_command(<<?CR, Ch:8, _Rest/binary>>, _Acc) -> {error, {unexpected_chars_in_command, [?CR, Ch]}};
parse_command(<<Ch:8,       Rest/binary>>,  Acc) -> parse_command(Rest, [Ch | Acc]).

parse_headers(Rest, Command) -> parse_headers(Rest, #stomp_frame{command = Command}, []).

parse_headers(<<>>,                         Frame,  HeaderAcc) -> more(fun(Rest) -> parse_headers(Rest, Frame, HeaderAcc) end);
parse_headers(<<?CR>>,                      Frame,  HeaderAcc) -> more(fun(Rest) -> parse_headers(<<?CR, Rest/binary>>, Frame, HeaderAcc) end);
parse_headers(<<?LF,        Rest/binary>>,  Frame,  HeaderAcc) -> parse_body(Rest, Frame#stomp_frame{headers = HeaderAcc});
parse_headers(<<?CR, ?LF,   Rest/binary>>,  Frame,  HeaderAcc) -> parse_body(Rest, Frame#stomp_frame{headers = HeaderAcc});
parse_headers(<<?CR, Ch:8, _Rest/binary>>, _Frame, _HeaderAcc) -> {error, {unexpected_chars_in_header, [?CR, Ch]}};
parse_headers(<<Ch:8,       Rest/binary>>,  Frame,  HeaderAcc) -> parse_header_name(Rest, Frame, HeaderAcc, [Ch]).

parse_header_name(<<>>,                           Frame,  HeaderAcc,  KeyAcc) -> more(fun(Rest) -> parse_header_name(Rest, Frame, HeaderAcc, KeyAcc) end);
parse_header_name(<<?CR>>,                        Frame,  HeaderAcc,  KeyAcc) -> more(fun(Rest) -> parse_header_name(<<?CR, Rest/binary>>, Frame, HeaderAcc, KeyAcc) end);
parse_header_name(<<?BSL>>,                       Frame,  HeaderAcc,  KeyAcc) -> more(fun(Rest) -> parse_header_name(<<?BSL, Rest/binary>>, Frame, HeaderAcc, KeyAcc) end);
parse_header_name(<<?COLON,       Rest/binary>>,  Frame,  HeaderAcc,  KeyAcc) -> parse_header_value(Rest, Frame, HeaderAcc, lists:reverse(KeyAcc));
parse_header_name(<<?LF,          Rest/binary>>,  Frame,  HeaderAcc, _KeyAcc) -> parse_headers(Rest, Frame, HeaderAcc);  % ignore header name
parse_header_name(<<?CR,  ?LF,    Rest/binary>>,  Frame,  HeaderAcc, _KeyAcc) -> parse_headers(Rest, Frame, HeaderAcc);  % ignore header name
parse_header_name(<<?CR,  Ch:8,  _Rest/binary>>, _Frame, _HeaderAcc, _KeyAcc) -> {error, {unexpected_chars_in_header, [?CR, Ch]}};
parse_header_name(<<?BSL, Ch:8,   Rest/binary>>,  Frame,  HeaderAcc,  KeyAcc) -> unescape(Ch, fun(Ech) -> parse_header_name(Rest, Frame, HeaderAcc, [Ech | KeyAcc]) end);
parse_header_name(<<Ch:8,         Rest/binary>>,  Frame,  HeaderAcc,  KeyAcc) -> parse_header_name(Rest, Frame, HeaderAcc, [Ch | KeyAcc]).

parse_header_value(Rest, Frame, HeaderAcc, Key) -> parse_header_value(Rest, Frame, HeaderAcc, Key, []).

parse_header_value(<<>>,                           Frame,  HeaderAcc,  Key,  ValAcc) -> more(fun(Rest) -> parse_header_value(Rest, Frame, HeaderAcc, Key, ValAcc) end);
parse_header_value(<<?CR>>,                        Frame,  HeaderAcc,  Key,  ValAcc) -> more(fun(Rest) -> parse_header_value(<<?CR, Rest/binary>>, Frame, HeaderAcc, Key, ValAcc) end);
parse_header_value(<<?BSL>>,                       Frame,  HeaderAcc,  Key,  ValAcc) -> more(fun(Rest) -> parse_header_value(<<?BSL, Rest/binary>>, Frame, HeaderAcc, Key, ValAcc) end);
parse_header_value(<<?COLON,      _Rest/binary>>, _Frame, _HeaderAcc, _Key, _ValAcc) -> {error, {unexpected_colon_in_header}};
parse_header_value(<<?LF,          Rest/binary>>,  Frame,  HeaderAcc,  Key,  ValAcc) -> parse_headers(Rest, Frame, insert_header(HeaderAcc, Key, lists:reverse(ValAcc)));
parse_header_value(<<?CR,  ?LF,    Rest/binary>>,  Frame,  HeaderAcc,  Key,  ValAcc) -> parse_headers(Rest, Frame, insert_header(HeaderAcc, Key, lists:reverse(ValAcc)));
parse_header_value(<<?CR,  Ch:8,  _Rest/binary>>, _Frame, _HeaderAcc, _Key, _ValAcc) -> {error, {unexpected_chars_in_header, [?CR, Ch]}};
parse_header_value(<<?BSL, Ch:8,   Rest/binary>>,  Frame,  HeaderAcc,  Key,  ValAcc) -> unescape(Ch, fun(Ech) -> parse_header_value(Rest, Frame, HeaderAcc, Key, [Ech | ValAcc]) end);
parse_header_value(<<Ch:8,         Rest/binary>>,  Frame,  HeaderAcc,  Key,  ValAcc) -> parse_header_value(Rest, Frame, HeaderAcc, Key, [Ch | ValAcc]).

unescape(?LF_ESC,    Fun) -> Fun(?LF);
unescape(?BSL_ESC,   Fun) -> Fun(?BSL);
unescape(?COLON_ESC, Fun) -> Fun(?COLON);
unescape(?CR_ESC,    Fun) -> Fun(?CR);
unescape(Ch,        _Fun) -> {error, {bad_escape, Ch}}.

insert_header(Headers, Key, Value) ->
    case lists:keymember(Key, 1, Headers) of
        true  -> Headers; % first header only
        false -> [{Key, Value} | Headers]
    end.

parse_body(Content, Frame) ->
    parse_body(Content, Frame, [],
               integer_header(Frame, ?HEADER_CONTENT_LENGTH, unknown)).

parse_body(Content, Frame, Chunks, unknown) ->
    parse_body2(Content, Frame, Chunks, case firstnull(Content) of
                                            -1  -> {more, unknown};
                                            Pos -> {done, Pos}
                                        end);
parse_body(Content, Frame, Chunks, Remaining) ->
    Size = byte_size(Content),
    parse_body2(Content, Frame, Chunks, case Remaining >= Size of
                                            true  -> {more, Remaining - Size};
                                            false -> {done, Remaining}
                                        end).

parse_body2(Content, Frame, Chunks, {more, Left}) ->
    Chunks1 = finalize_chunk(Content, Chunks),
    more(fun(Rest) -> parse_body(Rest, Frame, Chunks1, Left) end);
parse_body2(Content, Frame, Chunks, {done, Pos}) ->
    <<Chunk:Pos/binary, 0, Rest/binary>> = Content,
    Body = lists:reverse(finalize_chunk(Chunk, Chunks)),
    {ok, Frame#stomp_frame{body_iolist = Body}, Rest}.

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

header(F, K, D) -> default_value(header(F, K), D).

boolean_header(#stomp_frame{headers = Headers}, Key) ->
    case lists:keysearch(Key, 1, Headers) of
        {value, {_, "true"}}  -> {ok, true};
        {value, {_, "false"}} -> {ok, false};
        _                     -> not_found
    end.

boolean_header(F, K, D) -> default_value(boolean_header(F, K), D).

internal_integer_header(Headers, Key) ->
    case lists:keysearch(Key, 1, Headers) of
        {value, {_, Str}} -> {ok, list_to_integer(string:strip(Str))};
        _                 -> not_found
    end.

integer_header(#stomp_frame{headers = Headers}, Key) ->
    internal_integer_header(Headers, Key).

integer_header(F, K, D) -> default_value(integer_header(F, K), D).

binary_header(F, K) ->
    case header(F, K) of
        {ok, Str} -> {ok, list_to_binary(Str)};
        not_found -> not_found
    end.

binary_header(F, K, D) -> default_value(binary_header(F, K), D).

serialize(#stomp_frame{command = Command,
                       headers = Headers,
                       body_iolist = BodyFragments}) ->
    Len = iolist_size(BodyFragments),
    [Command, ?LF,
     lists:map(fun serialize_header/1,
               lists:keydelete(?HEADER_CONTENT_LENGTH, 1, Headers)),
     if
         Len > 0 -> [?HEADER_CONTENT_LENGTH ++ ":", integer_to_list(Len), ?LF];
         true    -> []
     end,
     ?LF, BodyFragments, 0].

serialize_header({K, V}) when is_integer(V) -> hdr(escape(K), integer_to_list(V));
serialize_header({K, V}) when is_list(V)    -> hdr(escape(K), escape(V)).

hdr(K, V) -> [K, ?COLON, V, ?LF].

escape(Str) -> [escape1(Ch) || Ch <- Str].

escape1(?COLON) -> [?BSL, ?COLON_ESC];
escape1(?BSL)   -> [?BSL, ?BSL_ESC];
escape1(?LF)    -> [?BSL, ?LF_ESC];
escape1(?CR)    -> [?BSL, ?CR_ESC];
escape1(C)      -> C.

firstnull(Content) -> firstnull(Content, 0).

firstnull(<<>>,                _N) -> -1;
firstnull(<<0,  _Rest/binary>>, N) -> N;
firstnull(<<_Ch, Rest/binary>>, N) -> firstnull(Rest, N+1).
