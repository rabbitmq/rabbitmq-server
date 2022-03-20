%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(frame_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_stomp_frame.hrl").
-include("rabbit_stomp_headers.hrl").
-compile(export_all).

all() ->
    [
    parse_simple_frame,
    parse_simple_frame_crlf,
    parse_command_only,
    parse_command_prefixed_with_newline,
    parse_ignore_empty_frames,
    parse_heartbeat_interframe,
    parse_crlf_interframe,
    parse_carriage_return_not_ignored_interframe,
    parse_carriage_return_mid_command,
    parse_carriage_return_end_command,
    parse_resume_mid_command,
    parse_resume_mid_header_key,
    parse_resume_mid_header_val,
    parse_resume_mid_body,
    parse_no_header_stripping,
    parse_multiple_headers,
    header_no_colon,
    no_nested_escapes,
    header_name_with_cr,
    header_value_with_cr,
    header_value_with_colon,
    headers_escaping_roundtrip,
    headers_escaping_roundtrip_without_trailing_lf,
    stream_offset_header
    ].

parse_simple_frame(_) ->
    parse_simple_frame_gen("\n").

parse_simple_frame_crlf(_) ->
    parse_simple_frame_gen("\r\n").

parse_simple_frame_gen(Term) ->
    Headers = [{"header1", "value1"}, {"header2", "value2"}],
    Content = frame_string("COMMAND",
                           Headers,
                           "Body Content",
                           Term),
    {"COMMAND", Frame, _State} = parse_complete(Content),
    [?assertEqual({ok, Value},
                  rabbit_stomp_frame:header(Frame, Key)) ||
        {Key, Value} <- Headers],
    #stomp_frame{body_iolist = Body} = Frame,
    ?assertEqual(<<"Body Content">>, iolist_to_binary(Body)).

parse_command_only(_) ->
    {ok, #stomp_frame{command = "COMMAND"}, _Rest} = parse("COMMAND\n\n\0").

parse_command_prefixed_with_newline(_) ->
    {ok, #stomp_frame{command = "COMMAND"}, _Rest} = parse("\nCOMMAND\n\n\0").

parse_ignore_empty_frames(_) ->
    {ok, #stomp_frame{command = "COMMAND"}, _Rest} = parse("\0\0COMMAND\n\n\0").

parse_heartbeat_interframe(_) ->
    {ok, #stomp_frame{command = "COMMAND"}, _Rest} = parse("\nCOMMAND\n\n\0").

parse_crlf_interframe(_) ->
    {ok, #stomp_frame{command = "COMMAND"}, _Rest} = parse("\r\nCOMMAND\n\n\0").

parse_carriage_return_not_ignored_interframe(_) ->
    {error, {unexpected_chars_between_frames, "\rC"}} = parse("\rCOMMAND\n\n\0").

parse_carriage_return_mid_command(_) ->
    {error, {unexpected_chars_in_command, "\rA"}} = parse("COMM\rAND\n\n\0").

parse_carriage_return_end_command(_) ->
    {error, {unexpected_chars_in_command, "\r\r"}} = parse("COMMAND\r\r\n\n\0").

parse_resume_mid_command(_) ->
    First = "COMM",
    Second = "AND\n\n\0",
    {more, Resume} = parse(First),
    {ok, #stomp_frame{command = "COMMAND"}, _Rest} = parse(Second, Resume).

parse_resume_mid_header_key(_) ->
    First = "COMMAND\nheade",
    Second = "r1:value1\n\n\0",
    {more, Resume} = parse(First),
    {ok, Frame = #stomp_frame{command = "COMMAND"}, _Rest} =
        parse(Second, Resume),
    ?assertEqual({ok, "value1"},
                 rabbit_stomp_frame:header(Frame, "header1")).

parse_resume_mid_header_val(_) ->
    First = "COMMAND\nheader1:val",
    Second = "ue1\n\n\0",
    {more, Resume} = parse(First),
    {ok, Frame = #stomp_frame{command = "COMMAND"}, _Rest} =
        parse(Second, Resume),
    ?assertEqual({ok, "value1"},
                 rabbit_stomp_frame:header(Frame, "header1")).

parse_resume_mid_body(_) ->
    First = "COMMAND\n\nABC",
    Second = "DEF\0",
    {more, Resume} = parse(First),
    {ok, #stomp_frame{command = "COMMAND", body_iolist = Body}, _Rest} =
         parse(Second, Resume),
    ?assertEqual([<<"ABC">>, <<"DEF">>], Body).

parse_no_header_stripping(_) ->
    Content = "COMMAND\nheader: foo \n\n\0",
    {ok, Frame, _} = parse(Content),
    {ok, Val} = rabbit_stomp_frame:header(Frame, "header"),
    ?assertEqual(" foo ", Val).

parse_multiple_headers(_) ->
    Content = "COMMAND\nheader:correct\nheader:incorrect\n\n\0",
    {ok, Frame, _} = parse(Content),
    {ok, Val} = rabbit_stomp_frame:header(Frame, "header"),
    ?assertEqual("correct", Val).

header_no_colon(_) ->
    Content = "COMMAND\n"
              "hdr1:val1\n"
              "hdrerror\n"
              "hdr2:val2\n"
              "\n\0",
    ?assertEqual(parse(Content), {error, {header_no_value, "hdrerror"}}).

no_nested_escapes(_) ->
    Content = "COM\\\\rAND\n"      % no escapes
              "hdr\\\\rname:"      % one escape
              "hdr\\\\rval\n\n\0", % one escape
    {ok, Frame, _} = parse(Content),
    ?assertEqual(Frame,
                 #stomp_frame{command = "COM\\\\rAND",
                              headers = [{"hdr\\rname", "hdr\\rval"}],
                              body_iolist = []}).

header_name_with_cr(_) ->
    Content = "COMMAND\nhead\rer:val\n\n\0",
    {error, {unexpected_chars_in_header, "\re"}} = parse(Content).

header_value_with_cr(_) ->
    Content = "COMMAND\nheader:val\rue\n\n\0",
    {error, {unexpected_chars_in_header, "\ru"}} = parse(Content).

header_value_with_colon(_) ->
    Content = "COMMAND\nheader:val:ue\n\n\0",
    {ok, Frame, _} = parse(Content),
    ?assertEqual(Frame,
                 #stomp_frame{ command     = "COMMAND",
                               headers     = [{"header", "val:ue"}],
                               body_iolist = []}).

stream_offset_header(_) ->
    TestCases = [
        {{"x-stream-offset", "first"}, {longstr, <<"first">>}},
        {{"x-stream-offset", "last"}, {longstr, <<"last">>}},
        {{"x-stream-offset", "next"}, {longstr, <<"next">>}},
        {{"x-stream-offset", "offset=5000"}, {long, 5000}},
        {{"x-stream-offset", "timestamp=1000"}, {timestamp, 1000}},
        {{"x-stream-offset", "foo"}, undefined},
        {{"some-header", "some value"}, undefined}
    ],

    lists:foreach(fun({Header, Expected}) ->
        ?assertEqual(
            Expected,
            rabbit_stomp_frame:stream_offset_header(#stomp_frame{headers = [Header]}, undefined)
            )
    end, TestCases).

test_frame_serialization(Expected, TrailingLF) ->
    {ok, Frame, _} = parse(Expected),
    {ok, Val} = rabbit_stomp_frame:header(Frame, "head\r:\ner"),
    ?assertEqual(":\n\r\\", Val),
    Serialized = lists:flatten(rabbit_stomp_frame:serialize(Frame, TrailingLF)),
    ?assertEqual(Expected, rabbit_misc:format("~s", [Serialized])).

headers_escaping_roundtrip(_) ->
    test_frame_serialization("COMMAND\nhead\\r\\c\\ner:\\c\\n\\r\\\\\n\n\0\n", true).

headers_escaping_roundtrip_without_trailing_lf(_) ->
    test_frame_serialization("COMMAND\nhead\\r\\c\\ner:\\c\\n\\r\\\\\n\n\0", false).

parse(Content) ->
    parse(Content, rabbit_stomp_frame:initial_state()).
parse(Content, State) ->
    rabbit_stomp_frame:parse(list_to_binary(Content), State).

parse_complete(Content) ->
    {ok, Frame = #stomp_frame{command = Command}, State} = parse(Content),
    {Command, Frame, State}.

frame_string(Command, Headers, BodyContent, Term) ->
    HeaderString =
        lists:flatten([Key ++ ":" ++ Value ++ Term || {Key, Value} <- Headers]),
    Command ++ Term ++ HeaderString ++ Term ++ BodyContent ++ "\0" ++ "\n".

