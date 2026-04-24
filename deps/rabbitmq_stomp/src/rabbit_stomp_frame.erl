%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_stomp_frame).

-include("rabbit_stomp_frame.hrl").
-include("rabbit_stomp_headers.hrl").

-export([parse/2, initial_state/0, initial_state/1]).
-export([header/2, header/3,
         boolean_header/2, boolean_header/3,
         integer_header/2, integer_header/3,
         binary_header/2, binary_header/3]).
-export([stream_offset_header/1, stream_filter_header/1]).
-export([serialize/1, serialize/2]).

%% Only used by tests. Production code uses `initial_state/1` with an explicitly provided config.
initial_state() -> {none, ?DEFAULT_STOMP_PARSER_CONFIG}.
initial_state(Config) -> {none, Config}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% STOMP 1.0/1.1/1.2 frame syntax
%%
%%  Rabbit modifications:
%%  - CR LF is equivalent to LF in all element terminators (eol)
%%  - Escape codes for header names and values include \r for CR
%%    and CR is not allowed
%%  - Header names and values are not limited to UTF-8 strings
%%  - Header values may contain unescaped colons
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
%%  OCTET       ::= '00'x..'FF'x
%%  NUL         ::= '00'x
%%  LF          ::= '\n'
%%  CR          ::= '\r'
%%  NOTEOL      ::= OCTET - (CR | LF)
%%  BACKSLASH   ::= '\\'
%%  ESCCODE     ::= 'c' | 'n' | 'r' | BACKSLASH
%%  COLON       ::= ':'
%%  HDROCT      ::= NOTEOL - (COLON | BACKSLASH)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Frame characters
-define(NUL,   0).
-define(CR,    $\r).
-define(LF,    $\n).
-define(BSL,   $\\).
-define(COLON, $:).

%% Header escape codes
-define(LF_ESC,    $n).
-define(BSL_ESC,   $\\).
-define(COLON_ESC, $c).
-define(CR_ESC,    $r).

%% Command lookup: binary -> atom for known STOMP commands.
%% Unknown commands pass through as binaries.
-define(KNOWN_COMMANDS,
        #{<<"SEND">>        => 'SEND',
          <<"SUBSCRIBE">>   => 'SUBSCRIBE',
          <<"UNSUBSCRIBE">> => 'UNSUBSCRIBE',
          <<"STOMP">>       => 'STOMP',
          <<"CONNECT">>     => 'CONNECT',
          <<"CONNECTED">>   => 'CONNECTED',
          <<"DISCONNECT">>  => 'DISCONNECT',
          <<"BEGIN">>       => 'BEGIN',
          <<"COMMIT">>      => 'COMMIT',
          <<"ABORT">>       => 'ABORT',
          <<"ACK">>         => 'ACK',
          <<"NACK">>        => 'NACK',
          <<"MESSAGE">>     => 'MESSAGE',
          <<"RECEIPT">>     => 'RECEIPT',
          <<"ERROR">>       => 'ERROR'}).

%% The longest known STOMP command is UNSUBSCRIBE (11 bytes).
%% Allow some headroom for unknown commands but bound memory usage.
-define(MAX_COMMAND_LENGTH, 32).

%% Parser state.
%% acc is only used for header values with escape sequences.
%% Commands and headers without escapes use sub-binary extraction.
-record(ps, {acc     = [] :: [byte()],
             acc_len = 0  :: non_neg_integer(),
             cmd          :: atom() | binary() | undefined,
             hdrs    = #{} :: #{binary() => binary()},
             hdrname      :: binary() | undefined,
             config       :: #stomp_parser_config{}}).

%%
%% Public API
%%

parse(Content, {resume, Continuation}) -> Continuation(Content);
parse(Content, {none, Config})         -> parse_noise(Content, #ps{config = Config}).

%%
%% Phase: noise — skip NULs and LFs between frames
%%

parse_noise(<<>>, S) ->
    more(fun(Rest) -> parse_noise(Rest, S) end);
parse_noise(<<?NUL, Rest/binary>>, S) -> parse_noise(Rest, S);
parse_noise(<<?LF,  Rest/binary>>, S) -> parse_noise(Rest, S);
parse_noise(<<?CR, ?LF, Rest/binary>>, S) -> parse_noise(Rest, S);
parse_noise(<<?CR>>, S) -> more(fun(Rest) -> parse_noise(<<?CR, Rest/binary>>, S) end);
parse_noise(<<?CR, Ch:8, _/binary>>, _S) -> {error, {unexpected_chars_between_frames, [?CR, Ch]}};
parse_noise(Bin, S) -> parse_command(Bin, S).

%%
%% Phase: command — scan for LF, extract as sub-binary
%%

parse_command(Bin, S) ->
    case scan_until_lf(Bin) of
        {ok, CmdBin, Rest} ->
            case byte_size(CmdBin) > ?MAX_COMMAND_LENGTH of
                true  -> {error, {command_too_long, ?MAX_COMMAND_LENGTH}};
                false ->
                    Cmd = maps:get(CmdBin, ?KNOWN_COMMANDS, CmdBin),
                    parse_headers(Rest, S#ps{cmd = Cmd, hdrs = #{}})
            end;
        {more, Len} ->
            case Len > ?MAX_COMMAND_LENGTH of
                true  -> {error, {command_too_long, ?MAX_COMMAND_LENGTH}};
                false -> more(fun(Rest) -> parse_command(<<Bin/binary, Rest/binary>>, S) end)
            end;
        {error, _} = Err ->
            Err
    end.

%%
%% Phase: headers — dispatch to hdrname or body
%%

parse_headers(<<?LF, Rest/binary>>, S) ->
    parse_body(Rest, S);
parse_headers(<<?CR, ?LF, Rest/binary>>, S) ->
    parse_body(Rest, S);
parse_headers(<<>>, S) ->
    more(fun(Rest) -> parse_headers(Rest, S) end);
parse_headers(<<?CR>>, S) ->
    more(fun(Rest) -> parse_headers(<<?CR, Rest/binary>>, S) end);
parse_headers(Bin, S) ->
    parse_hdr(Bin, S).

%%
%% Phase: header line — scan for COLON and LF in bulk.
%% Fast path: no escapes or CR in the header line.
%% Slow path: escape sequences present, fall back to byte-by-byte.
%%

parse_hdr(Bin, S = #ps{config = #stomp_parser_config{max_header_length = MaxHL}}) ->
    case scan_header_line(Bin) of
        {ok, Name, Value, Rest} ->
            case byte_size(Name) > MaxHL orelse byte_size(Value) > MaxHL of
                true  -> {error, {max_header_length, MaxHL}};
                false ->
                    case insert_header(Name, Value, S) of
                        {ok, S1}       -> parse_headers(Rest, S1);
                        {error, _} = E -> E
                    end
            end;
        has_escapes ->
            parse_hdrname_esc(Bin, S#ps{acc = [], acc_len = 0});
        {no_value, Name} ->
            {error, {header_no_value, Name}};
        {more, Len} ->
            case Len > MaxHL of
                true  -> {error, {max_header_length, MaxHL}};
                false -> more(fun(Rest) -> parse_hdr(<<Bin/binary, Rest/binary>>, S) end)
            end;
        {error, _} = Err ->
            Err
    end.

%% Slow path for header names with escapes or CR
parse_hdrname_esc(<<>>, S) ->
    more(fun(Rest) -> parse_hdrname_esc(Rest, S) end);
parse_hdrname_esc(<<?CR>>, S) ->
    more(fun(Rest) -> parse_hdrname_esc(<<?CR, Rest/binary>>, S) end);
parse_hdrname_esc(<<?CR, ?LF, _/binary>>, #ps{acc = Acc}) ->
    {error, {header_no_value, list_to_binary(lists:reverse(Acc))}};
parse_hdrname_esc(<<?CR, Ch:8, _/binary>>, _) ->
    {error, {unexpected_chars_in_header, [?CR, Ch]}};
parse_hdrname_esc(<<?LF, _/binary>>, #ps{acc = Acc}) ->
    {error, {header_no_value, list_to_binary(lists:reverse(Acc))}};
parse_hdrname_esc(<<?COLON, Rest/binary>>, S = #ps{acc = Acc}) ->
    parse_hdrvalue_esc(Rest, S#ps{acc = [], acc_len = 0,
                                  hdrname = list_to_binary(lists:reverse(Acc))});
parse_hdrname_esc(<<?BSL>>, S) ->
    more(fun(Rest) -> parse_hdrname_esc(<<?BSL, Rest/binary>>, S) end);
parse_hdrname_esc(<<?BSL, Ch:8, Rest/binary>>, S) ->
    unescape(Ch, fun(Ech) -> parse_hdrname_esc(Rest, accum(Ech, S)) end);
parse_hdrname_esc(<<Ch:8, Rest/binary>>, S = #ps{acc_len = Len,
                                                   config = #stomp_parser_config{
                                                               max_header_length = Max}}) ->
    case Len >= Max of
        true  -> {error, {max_header_length, Max}};
        false -> parse_hdrname_esc(Rest, accum(Ch, S))
    end.

%% Slow path for header values with escapes
parse_hdrvalue_esc(<<>>, S) ->
    more(fun(Rest) -> parse_hdrvalue_esc(Rest, S) end);
parse_hdrvalue_esc(<<?CR>>, S) ->
    more(fun(Rest) -> parse_hdrvalue_esc(<<?CR, Rest/binary>>, S) end);
parse_hdrvalue_esc(<<?CR, ?LF, Rest/binary>>, S) ->
    finish_hdr_esc(Rest, S);
parse_hdrvalue_esc(<<?CR, Ch:8, _/binary>>, _) ->
    {error, {unexpected_chars_in_header, [?CR, Ch]}};
parse_hdrvalue_esc(<<?LF, Rest/binary>>, S) ->
    finish_hdr_esc(Rest, S);
parse_hdrvalue_esc(<<?BSL>>, S) ->
    more(fun(Rest) -> parse_hdrvalue_esc(<<?BSL, Rest/binary>>, S) end);
parse_hdrvalue_esc(<<?BSL, Ch:8, Rest/binary>>, S) ->
    unescape(Ch, fun(Ech) -> parse_hdrvalue_esc(Rest, accum(Ech, S)) end);
parse_hdrvalue_esc(<<Ch:8, Rest/binary>>, S = #ps{acc_len = Len,
                                                    config = #stomp_parser_config{
                                                                max_header_length = Max}}) ->
    case Len >= Max of
        true  -> {error, {max_header_length, Max}};
        false -> parse_hdrvalue_esc(Rest, accum(Ch, S))
    end.

finish_hdr_esc(Rest, #ps{acc = Acc, hdrname = HdrName} = S) ->
    case insert_header(HdrName, list_to_binary(lists:reverse(Acc)), S) of
        {ok, S1}       -> parse_headers(Rest, S1);
        {error, _} = E -> E
    end.

%%
%% Binary scanning helpers — bulk operations, no per-byte allocation
%%

more(Continuation) -> {more, {resume, Continuation}}.

%% Scan for LF in a binary. Handles CR LF normalization.
%% Returns {ok, BeforeLF, AfterLF} | {more, CurrentLen} | {error, _}
scan_until_lf(Bin) ->
    scan_until_lf(Bin, 0).

scan_until_lf(Bin, Pos) ->
    case Bin of
        <<_:Pos/binary, ?LF, _/binary>> ->
            <<Before:Pos/binary, ?LF, Rest/binary>> = Bin,
            {ok, Before, Rest};
        <<_:Pos/binary, ?CR, ?LF, _/binary>> ->
            <<Before:Pos/binary, ?CR, ?LF, Rest/binary>> = Bin,
            {ok, Before, Rest};
        <<_:Pos/binary, ?CR>> ->
            {more, Pos};
        <<_:Pos/binary, ?CR, Ch:8, _/binary>> ->
            {error, {unexpected_chars_in_command, [?CR, Ch]}};
        <<_:Pos/binary, _:8, _/binary>> ->
            scan_until_lf(Bin, Pos + 1);
        <<_:Pos/binary>> ->
            {more, Pos}
    end.

%% Scan a complete header line: Name:Value\n
%% Fast path: no backslash or CR in the line.
%% Returns:
%%   {ok, NameBin, ValueBin, Rest}  — fast path, no escapes
%%   has_escapes                     — contains \ or CR, use slow path
%%   {no_value, NameBin}            — LF before COLON
%%   {more, Len}                    — need more data
%%   {error, _}                     — CR not followed by LF
scan_header_line(Bin) ->
    scan_hdr_name(Bin, 0).

scan_hdr_name(Bin, Pos) ->
    case Bin of
        <<_:Pos/binary, ?COLON, _/binary>> ->
            <<Name:Pos/binary, ?COLON, Rest/binary>> = Bin,
            scan_hdr_value(Rest, Name, 0);
        <<_:Pos/binary, ?LF, _/binary>> ->
            <<Name:Pos/binary, _/binary>> = Bin,
            {no_value, Name};
        <<_:Pos/binary, ?CR, ?LF, _/binary>> ->
            <<Name:Pos/binary, _/binary>> = Bin,
            {no_value, Name};
        <<_:Pos/binary, ?BSL, _/binary>> ->
            has_escapes;
        <<_:Pos/binary, ?CR>> ->
            {more, Pos};
        <<_:Pos/binary, ?CR, Ch:8, _/binary>> ->
            {error, {unexpected_chars_in_header, [?CR, Ch]}};
        <<_:Pos/binary, _:8, _/binary>> ->
            scan_hdr_name(Bin, Pos + 1);
        <<_:Pos/binary>> ->
            {more, Pos}
    end.

scan_hdr_value(Bin, Name, Pos) ->
    case Bin of
        <<_:Pos/binary, ?LF, _/binary>> ->
            <<Value:Pos/binary, ?LF, Rest/binary>> = Bin,
            {ok, Name, Value, Rest};
        <<_:Pos/binary, ?CR, ?LF, _/binary>> ->
            <<Value:Pos/binary, ?CR, ?LF, Rest/binary>> = Bin,
            {ok, Name, Value, Rest};
        <<_:Pos/binary, ?BSL, _/binary>> ->
            has_escapes;
        <<_:Pos/binary, ?CR>> ->
            {more, Pos};
        <<_:Pos/binary, ?CR, Ch:8, _/binary>> ->
            {error, {unexpected_chars_in_header, [?CR, Ch]}};
        <<_:Pos/binary, _:8, _/binary>> ->
            scan_hdr_value(Bin, Name, Pos + 1);
        <<_:Pos/binary>> ->
            {more, Pos}
    end.

%%
%% Helpers
%%

accum(Ch, S = #ps{acc = Acc, acc_len = Len}) ->
    S#ps{acc = [Ch | Acc], acc_len = Len + 1}.

unescape(?LF_ESC,    Fun) -> Fun(?LF);
unescape(?BSL_ESC,   Fun) -> Fun(?BSL);
unescape(?COLON_ESC, Fun) -> Fun(?COLON);
unescape(?CR_ESC,    Fun) -> Fun(?CR);
unescape(Ch,        _Fun) -> {error, {bad_escape, [?BSL, Ch]}}.

%% First occurrence of a header name wins.
%% Duplicates are discarded without allocation.
%% The limit is checked only when a genuinely new header would be added.
insert_header(Name, Value, S = #ps{hdrs = Hdrs,
                                    config = #stomp_parser_config{
                                                max_headers = MaxHeaders}}) ->
    case Hdrs of
        #{Name := _} ->
            {ok, S};
        _ when map_size(Hdrs) >= MaxHeaders ->
            {error, {max_headers, MaxHeaders}};
        _ ->
            {ok, S#ps{hdrs = Hdrs#{Name => Value}}}
    end.

%%
%% Body parsing
%%

parse_body(Content, #ps{cmd = Cmd, hdrs = Hdrs,
                        config = #stomp_parser_config{
                                   max_body_length = MaxBodyLength}}) ->
    Frame = #stomp_frame{command = Cmd, headers = Hdrs},
    case Cmd of
        'SEND' ->
            case integer_header(Frame, ?HEADER_CONTENT_LENGTH, unknown) of
                ContentLength when is_integer(ContentLength),
                                   ContentLength < 0 ->
                    {error, {invalid_content_length, ContentLength}};
                ContentLength when is_integer(ContentLength),
                                   ContentLength > MaxBodyLength ->
                    {error, {max_body_length, ContentLength}};
                ContentLength when is_integer(ContentLength) ->
                    parse_known_body(Content, Frame, [], ContentLength);
                _ ->
                    parse_unknown_body(Content, Frame, [], MaxBodyLength)
            end;
        _ ->
            parse_unknown_body(Content, Frame, [], MaxBodyLength)
    end.

-define(MORE_BODY(Content, Frame, Chunks, Remaining),
            Chunks1 = finalize_chunk(Content, Chunks),
            more(fun(Rest) -> ?FUNCTION_NAME(Rest, Frame, Chunks1, Remaining) end)).

parse_unknown_body(Content, Frame, Chunks, Remaining) ->
    case firstnull(Content) of
        -1 ->
            ChunkSize = byte_size(Content),
            case ChunkSize > Remaining of
                true  -> {error, {max_body_length, unknown}};
                false -> ?MORE_BODY(Content, Frame, Chunks, Remaining - ChunkSize)
            end;
        Pos ->
            case Pos > Remaining of
                true  -> {error, {max_body_length, unknown}};
                false -> finish_body(Content, Frame, Chunks, Pos)
            end
    end.

parse_known_body(Content, Frame, Chunks, Remaining) ->
    Size = byte_size(Content),
    case Remaining >= Size of
        true  -> ?MORE_BODY(Content, Frame, Chunks, Remaining - Size);
        false -> finish_body(Content, Frame, Chunks, Remaining)
    end.

finish_body(Content, Frame, Chunks, Pos) ->
    case Content of
        <<Chunk:Pos/binary, 0, Rest/binary>> ->
            Body = finalize_chunk(Chunk, Chunks),
            {ok, Frame#stomp_frame{body_iolist_rev = Body}, Rest};
        _ ->
            {error, missing_body_terminator}
    end.

finalize_chunk(<<>>,  Chunks) -> Chunks;
finalize_chunk(Chunk, Chunks) -> [Chunk | Chunks].

firstnull(Content) -> firstnull(Content, 0).

firstnull(<<>>,                _N) -> -1;
firstnull(<<0,  _Rest/binary>>, N) -> N;
firstnull(<<_Ch, Rest/binary>>, N) -> firstnull(Rest, N + 1).

%%
%% Header accessors
%%

default_value({ok, Value}, _DefaultValue) -> Value;
default_value(not_found,    DefaultValue) -> DefaultValue.

header(#stomp_frame{headers = Headers}, Key) ->
    case maps:find(Key, Headers) of
        {ok, _} = Ok -> Ok;
        error        -> not_found
    end.

header(F, K, D) -> default_value(header(F, K), D).

boolean_header(F, Key) ->
    case header(F, Key) of
        {ok, <<"true">>}  -> {ok, true};
        {ok, <<"false">>} -> {ok, false};
        {ok, <<"True">>}  -> {ok, true};
        {ok, <<"False">>} -> {ok, false};
        _                 -> not_found
    end.

boolean_header(F, K, D) -> default_value(boolean_header(F, K), D).

integer_header(F, Key) ->
    case header(F, Key) of
        {ok, Str} ->
            try {ok, binary_to_integer(string:trim(Str))}
            catch _:_ -> not_found
            end;
        not_found -> not_found
    end.

integer_header(F, K, D) -> default_value(integer_header(F, K), D).

binary_header(F, K) -> header(F, K).

binary_header(F, K, D) -> default_value(binary_header(F, K), D).

stream_offset_header(F) ->
    case binary_header(F, ?HEADER_X_STREAM_OFFSET) of
        {ok, <<"first">>}                    -> {longstr, <<"first">>};
        {ok, <<"last">>}                     -> {longstr, <<"last">>};
        {ok, <<"next">>}                     -> {longstr, <<"next">>};
        {ok, <<"offset=", V/binary>>}        -> {long, binary_to_integer(V)};
        {ok, <<"timestamp=", V/binary>>}     -> {timestamp, binary_to_integer(V)};
        _                                    -> not_found
    end.

stream_filter_header(F) ->
    case binary_header(F, ?HEADER_X_STREAM_FILTER) of
        {ok, Str} ->
            {array, lists:reverse(
                      lists:foldl(fun(V, Acc) ->
                                          [{longstr, V} | Acc]
                                  end,
                                  [],
                                  binary:split(Str, <<",">>, [global])))};
        not_found ->
            not_found
    end.

%%
%% Serialization
%%

serialize(Frame) ->
    serialize(Frame, true).

serialize(Frame, true) ->
    serialize(Frame, false) ++ [?LF];
serialize(#stomp_frame{command = Command,
                       headers = Headers,
                       body_iolist_rev = BodyFragments}, false) ->
    Len = iolist_size(BodyFragments),
    [serialize_command(Command), ?LF,
     serialize_headers(Headers),
     if
         Len > 0 -> [?HEADER_CONTENT_LENGTH, ?COLON, integer_to_list(Len), ?LF];
         true    -> []
     end,
     ?LF, case BodyFragments of
              _ when is_binary(BodyFragments) -> BodyFragments;
              _ -> lists:reverse(BodyFragments)
          end, 0].

serialize_headers(Headers) ->
    maps:fold(fun(K, _V, Acc) when K =:= ?HEADER_CONTENT_LENGTH -> Acc;
                 (K, V, Acc) -> [serialize_header(K, V) | Acc]
              end, [], Headers).

serialize_command(Command) when is_atom(Command) ->
    atom_to_binary(Command, utf8);
serialize_command(Command) -> Command.

serialize_header(K, V) when is_integer(V) -> hdr(escape(K), integer_to_list(V));
serialize_header(K, V) when is_boolean(V) -> hdr(escape(K), boolean_to_list(V));
serialize_header(K, V) when is_binary(V)  -> hdr(escape(K), escape(V)).

boolean_to_list(true) -> "true";
boolean_to_list(_)    -> "false".

hdr(K, V) -> [K, ?COLON, V, ?LF].

escape(Bin) -> escape(Bin, []).

escape(<<>>, Acc) -> lists:reverse(Acc);
escape(<<?COLON, Rest/binary>>, Acc) -> escape(Rest, [?COLON_ESC, ?BSL | Acc]);
escape(<<?BSL, Rest/binary>>, Acc) -> escape(Rest, [?BSL_ESC, ?BSL | Acc]);
escape(<<?LF, Rest/binary>>, Acc) -> escape(Rest, [?LF_ESC, ?BSL | Acc]);
escape(<<?CR, Rest/binary>>, Acc) -> escape(Rest, [?CR_ESC, ?BSL | Acc]);
escape(<<Ch:8, Rest/binary>>, Acc) -> escape(Rest, [Ch | Acc]).
