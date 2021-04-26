%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% stomp_frame implements the STOMP framing protocol "version 1.0", as
%% per https://stomp.codehaus.org/Protocol

-module(rabbit_stomp_frame).

-include("rabbit_stomp_frame.hrl").
-include("rabbit_stomp_headers.hrl").

-export([parse/2, initial_state/0]).
-export([header/2, header/3,
         boolean_header/2, boolean_header/3,
         integer_header/2, integer_header/3,
         binary_header/2, binary_header/3]).
-export([stream_offset_header/2]).
-export([serialize/1, serialize/2]).

initial_state() -> none.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% STOMP 1.1 frames basic syntax
%%  Rabbit modifications:
%%  o   CR LF is equivalent to LF in all element terminators (eol).
%%  o   Escape codes for header names and values include \r for CR
%%      and CR is not allowed.
%%  o   Header names and values are not limited to UTF-8 strings.
%%  o   Header values may contain unescaped colons
%%
%%  frame_seq   ::= *(noise frame)
%%  noise       ::= *(NUL | eol)
%%  eol         ::= LF | CR LF
%%  frame       ::= cmd hdrs body NUL
%%  body        ::= *OCTET
%%  cmd         ::= 1*NOTEOL eol
%%  hdrs        ::= *hdr eol
%%  hdr         ::= hdrname COLON hdrvalue eol
%%  hdrname     ::= 1*esc_char
%%  hdrvalue    ::= *esc_char
%%  esc_char    ::= HDROCT | BACKSLASH ESCCODE
%%
%% Terms in CAPS all represent sets (alternatives) of single octets.
%% They are defined here using a small extension of BNF, minus (-):
%%
%%    term1 - term2         denotes any of the possibilities in term1
%%                          excluding those in term2.
%% In this grammar minus is only used for sets of single octets.
%%
%%  OCTET       ::= '00'x..'FF'x            % any octet
%%  NUL         ::= '00'x                   % the zero octet
%%  LF          ::= '\n'                    % '0a'x newline or linefeed
%%  CR          ::= '\r'                    % '0d'x carriage return
%%  NOTEOL      ::= OCTET - (CR | LF)       % any octet except CR or LF
%%  BACKSLASH   ::= '\\'                    % '5c'x
%%  ESCCODE     ::= 'c' | 'n' | 'r' | BACKSLASH
%%  COLON       ::= ':'
%%  HDROCT      ::= NOTEOL - (COLON | BACKSLASH)
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

%% parser state
-record(state, {acc, cmd, hdrs, hdrname}).

parse(Content, {resume, Continuation}) -> Continuation(Content);
parse(Content, none                  ) -> parser(Content, noframe, #state{}).

more(Continuation) -> {more, {resume, Continuation}}.

%% Single-function parser: Term :: noframe | command | headers | hdrname | hdrvalue
%% general more and line-end detection
parser(<<>>,                        Term    ,  State) -> more(fun(Rest) -> parser(Rest, Term, State) end);
parser(<<?CR>>,                     Term    ,  State) -> more(fun(Rest) -> parser(<<?CR, Rest/binary>>, Term, State) end);
parser(<<?CR, ?LF,   Rest/binary>>, Term    ,  State) -> parser(<<?LF, Rest/binary>>, Term, State);
parser(<<?CR, Ch:8, _Rest/binary>>, Term    , _State) -> {error, {unexpected_chars(Term), [?CR, Ch]}};
%% escape processing (only in hdrname and hdrvalue terms)
parser(<<?BSL>>,                    Term    ,  State) -> more(fun(Rest) -> parser(<<?BSL, Rest/binary>>, Term, State) end);
parser(<<?BSL, Ch:8, Rest/binary>>, Term    ,  State)
                               when Term == hdrname;
                                    Term == hdrvalue  -> unescape(Ch, fun(Ech) -> parser(Rest, Term, accum(Ech, State)) end);
%% inter-frame noise
parser(<<?NUL,       Rest/binary>>, noframe ,  State) -> parser(Rest, noframe, State);
parser(<<?LF,        Rest/binary>>, noframe ,  State) -> parser(Rest, noframe, State);
%% detect transitions
parser(              Rest,          noframe ,  State) -> goto(noframe,  command,  Rest, State);
parser(<<?LF,        Rest/binary>>, command ,  State) -> goto(command,  headers,  Rest, State);
parser(<<?LF,        Rest/binary>>, headers ,  State) -> goto(headers,  body,     Rest, State);
parser(              Rest,          headers ,  State) -> goto(headers,  hdrname,  Rest, State);
parser(<<?COLON,     Rest/binary>>, hdrname ,  State) -> goto(hdrname,  hdrvalue, Rest, State);
parser(<<?LF,        Rest/binary>>, hdrname ,  State) -> goto(hdrname,  headers,  Rest, State);
parser(<<?LF,        Rest/binary>>, hdrvalue,  State) -> goto(hdrvalue, headers,  Rest, State);
%% accumulate
parser(<<Ch:8,       Rest/binary>>, Term    ,  State) -> parser(Rest, Term, accum(Ch, State)).

%% state transitions
goto(noframe,  command,  Rest, State                                 ) -> parser(Rest, command, State#state{acc = []});
goto(command,  headers,  Rest, State = #state{acc = Acc}             ) -> parser(Rest, headers, State#state{cmd = lists:reverse(Acc), hdrs = []});
goto(headers,  body,     Rest,         #state{cmd = Cmd, hdrs = Hdrs}) -> parse_body(Rest, #stomp_frame{command = Cmd, headers = Hdrs});
goto(headers,  hdrname,  Rest, State                                 ) -> parser(Rest, hdrname, State#state{acc = []});
goto(hdrname,  hdrvalue, Rest, State = #state{acc = Acc}             ) -> parser(Rest, hdrvalue, State#state{acc = [], hdrname = lists:reverse(Acc)});
goto(hdrname,  headers, _Rest,         #state{acc = Acc}             ) -> {error, {header_no_value, lists:reverse(Acc)}};  % badly formed header -- fatal error
goto(hdrvalue, headers,  Rest, State = #state{acc = Acc, hdrs = Headers, hdrname = HdrName}) ->
    parser(Rest, headers, State#state{hdrs = insert_header(Headers, HdrName, lists:reverse(Acc))}).

%% error atom
unexpected_chars(noframe)  -> unexpected_chars_between_frames;
unexpected_chars(command)  -> unexpected_chars_in_command;
unexpected_chars(hdrname)  -> unexpected_chars_in_header;
unexpected_chars(hdrvalue) -> unexpected_chars_in_header;
unexpected_chars(_Term)    -> unexpected_chars.

%% general accumulation
accum(Ch, State = #state{acc = Acc}) -> State#state{acc = [Ch | Acc]}.

%% resolve escapes (with error processing)
unescape(?LF_ESC,    Fun) -> Fun(?LF);
unescape(?BSL_ESC,   Fun) -> Fun(?BSL);
unescape(?COLON_ESC, Fun) -> Fun(?COLON);
unescape(?CR_ESC,    Fun) -> Fun(?CR);
unescape(Ch,        _Fun) -> {error, {bad_escape, [?BSL, Ch]}}.

%% insert header unless aleady seen
insert_header(Headers, Name, Value) ->
    case lists:keymember(Name, 1, Headers) of
        true  -> Headers; % first header only
        false -> [{Name, Value} | Headers]
    end.

parse_body(Content, Frame = #stomp_frame{command = Command}) ->
    case Command of
        "SEND" -> parse_body(Content, Frame, [], integer_header(Frame, ?HEADER_CONTENT_LENGTH, unknown));
        _ -> parse_body(Content, Frame, [], unknown)
    end.

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
        %% some Python clients serialize True/False as "True"/"False"
        {value, {_, "True"}}  -> {ok, true};
        {value, {_, "False"}} -> {ok, false};
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

stream_offset_header(F, D) ->
    OffsetPrefix = <<"offset:">>,
    OffsetPrefixLength = byte_size(OffsetPrefix),
    TimestampPrefix = <<"timestamp:">>,
    TimestampPrefixLength = byte_size(TimestampPrefix),
    case binary_header(F, ?HEADER_X_STREAM_OFFSET, D) of
        <<"first">> ->
            {longstr, <<"first">>};
        <<"last">> ->
            {longstr, <<"last">>};
        <<"next">> ->
            {longstr, <<"next">>};
        <<OffsetPrefix:OffsetPrefixLength/binary, OffsetValue/binary>> ->
            {long, binary_to_integer(OffsetValue)};
        <<TimestampPrefix:TimestampPrefixLength/binary, TimestampValue/binary>> ->
            {timestamp, binary_to_integer(TimestampValue)};
        _ ->
            D
    end.

serialize(Frame) ->
    serialize(Frame, true).

%% second argument controls whether a trailing linefeed
%% character should be added, see rabbitmq/rabbitmq-stomp#39.
serialize(Frame, true) ->
    serialize(Frame, false) ++ [?LF];
serialize(#stomp_frame{command = Command,
                       headers = Headers,
                       body_iolist = BodyFragments}, false) ->
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
serialize_header({K, V}) when is_boolean(V) -> hdr(escape(K), boolean_to_list(V));
serialize_header({K, V}) when is_list(V)    -> hdr(escape(K), escape(V)).

boolean_to_list(true) -> "true";
boolean_to_list(_)    -> "false".

hdr(K, V) -> [K, ?COLON, V, ?LF].

escape(Str) -> [escape1(Ch) || Ch <- Str].

escape1(?COLON) -> [?BSL, ?COLON_ESC];
escape1(?BSL)   -> [?BSL, ?BSL_ESC];
escape1(?LF)    -> [?BSL, ?LF_ESC];
escape1(?CR)    -> [?BSL, ?CR_ESC];
escape1(Ch)     -> Ch.

firstnull(Content) -> firstnull(Content, 0).

firstnull(<<>>,                _N) -> -1;
firstnull(<<0,  _Rest/binary>>, N) -> N;
firstnull(<<_Ch, Rest/binary>>, N) -> firstnull(Rest, N+1).
