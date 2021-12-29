%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(unit_amqp091_content_framing_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").

-compile(export_all).

all() ->
    [
      {group, parallel_tests}
    ].

groups() ->
    [
      {parallel_tests, [parallel], [
          write_table_with_invalid_existing_type,
          invalid_existing_headers,
          disparate_invalid_header_entries_accumulate_separately,
          corrupt_or_invalid_headers_are_overwritten,
          invalid_same_header_entry_accumulation,
          content_framing,
          content_transcoding,
          table_codec
        ]}
    ].

%% -------------------------------------------------------------------
%% Test Cases
%% -------------------------------------------------------------------

-define(XDEATH_TABLE,
        [{<<"reason">>,       longstr,   <<"blah">>},
         {<<"queue">>,        longstr,   <<"foo.bar.baz">>},
         {<<"exchange">>,     longstr,   <<"my-exchange">>},
         {<<"routing-keys">>, array,     []}]).

-define(ROUTE_TABLE, [{<<"redelivered">>, bool, <<"true">>}]).

-define(BAD_HEADER(K), {<<K>>, longstr, <<"bad ", K>>}).
-define(BAD_HEADER2(K, Suf), {<<K>>, longstr, <<"bad ", K, Suf>>}).
-define(FOUND_BAD_HEADER(K), {<<K>>, array, [{longstr, <<"bad ", K>>}]}).

write_table_with_invalid_existing_type(_Config) ->
    prepend_check(<<"header1">>, ?XDEATH_TABLE, [?BAD_HEADER("header1")]).

invalid_existing_headers(_Config) ->
    Headers =
        prepend_check(<<"header2">>, ?ROUTE_TABLE, [?BAD_HEADER("header2")]),
    {array, [{table, ?ROUTE_TABLE}]} =
        rabbit_misc:table_lookup(Headers, <<"header2">>),
    passed.

disparate_invalid_header_entries_accumulate_separately(_Config) ->
    BadHeaders = [?BAD_HEADER("header2")],
    Headers = prepend_check(<<"header2">>, ?ROUTE_TABLE, BadHeaders),
    Headers2 = prepend_check(<<"header1">>, ?XDEATH_TABLE,
                             [?BAD_HEADER("header1") | Headers]),
    {table, [?FOUND_BAD_HEADER("header1"),
             ?FOUND_BAD_HEADER("header2")]} =
        rabbit_misc:table_lookup(Headers2, ?INVALID_HEADERS_KEY),
    passed.

corrupt_or_invalid_headers_are_overwritten(_Config) ->
    Headers0 = [?BAD_HEADER("header1"),
                ?BAD_HEADER("x-invalid-headers")],
    Headers1 = prepend_check(<<"header1">>, ?XDEATH_TABLE, Headers0),
    {table,[?FOUND_BAD_HEADER("header1"),
            ?FOUND_BAD_HEADER("x-invalid-headers")]} =
        rabbit_misc:table_lookup(Headers1, ?INVALID_HEADERS_KEY),
    passed.

invalid_same_header_entry_accumulation(_Config) ->
    BadHeader1 = ?BAD_HEADER2("header1", "a"),
    Headers = prepend_check(<<"header1">>, ?ROUTE_TABLE, [BadHeader1]),
    Headers2 = prepend_check(<<"header1">>, ?ROUTE_TABLE,
                             [?BAD_HEADER2("header1", "b") | Headers]),
    {table, InvalidHeaders} =
        rabbit_misc:table_lookup(Headers2, ?INVALID_HEADERS_KEY),
    {array, [{longstr,<<"bad header1b">>},
             {longstr,<<"bad header1a">>}]} =
        rabbit_misc:table_lookup(InvalidHeaders, <<"header1">>),
    passed.

prepend_check(HeaderKey, HeaderTable, Headers) ->
    Headers1 = rabbit_basic:prepend_table_header(
                 HeaderKey, HeaderTable, Headers),
    {table, Invalid} =
        rabbit_misc:table_lookup(Headers1, ?INVALID_HEADERS_KEY),
    {Type, Value} = rabbit_misc:table_lookup(Headers, HeaderKey),
    {array, [{Type, Value} | _]} =
        rabbit_misc:table_lookup(Invalid, HeaderKey),
    Headers1.


%% Test that content frames don't exceed frame-max
content_framing(_Config) ->
    %% no content
    passed = test_content_framing(4096, <<>>),
    %% easily fit in one frame
    passed = test_content_framing(4096, <<"Easy">>),
    %% exactly one frame (empty frame = 8 bytes)
    passed = test_content_framing(11, <<"One">>),
    %% more than one frame
    passed = test_content_framing(11, <<"More than one frame">>),
    passed.

test_content_framing(FrameMax, BodyBin) ->
    [Header | Frames] =
        rabbit_binary_generator:build_simple_content_frames(
          1,
          rabbit_binary_generator:ensure_content_encoded(
            rabbit_basic:build_content(#'P_basic'{}, BodyBin),
            rabbit_framing_amqp_0_9_1),
          FrameMax,
          rabbit_framing_amqp_0_9_1),
    %% header is formatted correctly and the size is the total of the
    %% fragments
    <<_FrameHeader:7/binary, _ClassAndWeight:4/binary,
      BodySize:64/unsigned, _Rest/binary>> = list_to_binary(Header),
    BodySize = size(BodyBin),
    true = lists:all(
             fun (ContentFrame) ->
                     FrameBinary = list_to_binary(ContentFrame),
                     %% assert
                     <<_TypeAndChannel:3/binary,
                       Size:32/unsigned, _Payload:Size/binary, 16#CE>> =
                         FrameBinary,
                     size(FrameBinary) =< FrameMax
             end, Frames),
    passed.

content_transcoding(_Config) ->
    %% there are no guarantees provided by 'clear' - it's just a hint
    ClearDecoded = fun rabbit_binary_parser:clear_decoded_content/1,
    ClearEncoded = fun rabbit_binary_generator:clear_encoded_content/1,
    EnsureDecoded =
        fun (C0) ->
                C1 = rabbit_binary_parser:ensure_content_decoded(C0),
                true = C1#content.properties =/= none,
                C1
        end,
    EnsureEncoded =
        fun (Protocol) ->
                fun (C0) ->
                        C1 = rabbit_binary_generator:ensure_content_encoded(
                               C0, Protocol),
                        true = C1#content.properties_bin =/= none,
                        C1
                end
        end,
    %% Beyond the assertions in Ensure*, the only testable guarantee
    %% is that the operations should never fail.
    %%
    %% If we were using quickcheck we'd simply stuff all the above
    %% into a generator for sequences of operations. In the absence of
    %% quickcheck we pick particularly interesting sequences that:
    %%
    %% - execute every op twice since they are idempotent
    %% - invoke clear_decoded, clear_encoded, decode and transcode
    %%   with one or both of decoded and encoded content present
    [begin
         sequence_with_content([Op]),
         sequence_with_content([ClearEncoded, Op]),
         sequence_with_content([ClearDecoded, Op])
     end || Op <- [ClearDecoded, ClearEncoded, EnsureDecoded,
                   EnsureEncoded(rabbit_framing_amqp_0_9_1),
                   EnsureEncoded(rabbit_framing_amqp_0_8)]],
    passed.

sequence_with_content(Sequence) ->
    lists:foldl(fun (F, V) -> F(F(V)) end,
                rabbit_binary_generator:ensure_content_encoded(
                  rabbit_basic:build_content(#'P_basic'{}, <<>>),
                  rabbit_framing_amqp_0_9_1),
                Sequence).

table_codec(_Config) ->
    %% Note: this does not test inexact numbers (double and float) at the moment.
    %% They won't pass the equality assertions.
    Table = [{<<"longstr">>,   longstr,   <<"Here is a long string">>},
             {<<"signedint">>, signedint, 12345},
             {<<"decimal">>,   decimal,   {3, 123456}},
             {<<"timestamp">>, timestamp, 109876543209876},
             {<<"table">>,     table,     [{<<"one">>, signedint, 54321},
                                           {<<"two">>, longstr,
                                            <<"A long string">>}]},
             {<<"byte">>,      byte,      -128},
             {<<"long">>,      long,      1234567890},
             {<<"short">>,     short,     655},
             {<<"bool">>,      bool,      true},
             {<<"binary">>,    binary,    <<"a binary string">>},
             {<<"unsignedbyte">>, unsignedbyte, 250},
             {<<"unsignedshort">>, unsignedshort, 65530},
             {<<"unsignedint">>, unsignedint, 4294967290},
             {<<"void">>,      void,      undefined},
             {<<"array">>,     array,     [{signedint, 54321},
                                           {longstr, <<"A long string">>}]}
            ],
    Binary = <<
               7,"longstr",   "S", 21:32, "Here is a long string",
               9,"signedint", "I", 12345:32/signed,
               7,"decimal",   "D", 3, 123456:32,
               9,"timestamp", "T", 109876543209876:64,
               5,"table",     "F", 31:32, % length of table
               3,"one",       "I", 54321:32,
               3,"two",       "S", 13:32, "A long string",
               4,"byte",      "b", -128:8/signed,
               4,"long",      "l", 1234567890:64,
               5,"short",     "s", 655:16,
               4,"bool",      "t", 1,
               6,"binary",    "x", 15:32, "a binary string",
               12,"unsignedbyte", "B", 250:8/unsigned,
               13,"unsignedshort", "u", 65530:16/unsigned,
               11,"unsignedint", "i", 4294967290:32/unsigned,
               4,"void",      "V",
               5,"array",     "A", 23:32,
               "I", 54321:32,
               "S", 13:32, "A long string"
             >>,
    Binary = rabbit_binary_generator:generate_table(Table),
    Table  = rabbit_binary_parser:parse_table(Binary),
    passed.
