-module(binary_parser_SUITE).

-compile(export_all).

-export([
         ]).

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, tests}
    ].


all_tests() ->
    [
     roundtrip,
     array_with_extra_input,
     unsupported_type,
     peek_described_section_total_sizes,
     peek_described_section,
     peek_non_described_throws,
     peek_total_size_matches_parse
    ].

groups() ->
    [
     {tests, [parallel], all_tests()}
    ].

roundtrip(_Config) ->
    Terms = [
             null,
             {described,
              {symbol, <<"URL">>},
              {utf8, <<"http://example.org/hello-world">>}},
             {described,
              {symbol, <<"URL">>},
              {binary, <<"https://rabbitmq.com">>}},
             %% "The descriptor portion of a described format code is itself
             %% any valid AMQP encoded value, including other described values." [1.2]
             {described,
              {described,
               {symbol, <<"inner constructor">>},
               {binary, <<"inner value">>}},
              {binary, <<"outer value">>}},
             {array, ubyte, [{ubyte, 1}, {ubyte, 255}]},
             true,
             {list, [{utf8, <<"hi">>},
                     {described,
                      {symbol, <<"URL">>},
                      {utf8, <<"http://example.org/hello-world">>}}
                    ]},
             {list, [{int, 123},
                     {array, int, [{int, 1}, {int, 2}, {int, 3}]}
                    ]},
             {map, [
                    {{utf8, <<"key1">>}, {utf8, <<"value1">>}},
                    {{utf8, <<"key2">>}, {int, 33}}
                   ]},
             {array, {described, {utf8, <<"URL">>}, utf8}, []},
             false
            ],

    Bin = lists:foldl(
            fun(T, Acc) ->
                    B = iolist_to_binary(amqp10_binary_generator:generate(T)),
                    <<Acc/binary, B/binary>>
            end, <<>>, Terms),

    ?assertEqual(Terms, amqp10_binary_parser:parse_many(Bin, [])).

array_with_extra_input(_Config) ->
    Bin = <<83,16,192,85,10,177,0,0,0,1,48,161,12,114,97,98,98,105,116, 109,113,45,98,111,120,112,255,255,0,0,96,0,50,112,0,0,19,136,163,5,101,110,45,85,83,224,14,2,65,5,102,105,45,70,73,5,101,110,45,85,83,64,64,193,24,2,163,20,68,69,70,69,78,83,73,67,83,46,84,69,83,84,46,83,85,73,84,69,65>>,

    Expected = {failed_to_parse_array_extra_input_remaining,
                %% element type, input, accumulated result
                65, <<105,45,70,73,5,101,110,45,85,83>>, [true,true]},

    ?assertExit(Expected, amqp10_binary_parser:parse_many(Bin, [])).

unsupported_type(_Config) ->
    UnsupportedType = 16#02,
    Bin = <<UnsupportedType, "hey">>,
    Expected = {primitive_type_unsupported, UnsupportedType, {position, 0}},
    ?assertThrow(Expected, amqp10_binary_parser:parse_many(Bin, [])).

%%%===================================================================
%%% peek/1 (exercises peek_value_size internally via described types)
%%%===================================================================

%% Asserts peek returns total size equal to byte_size for described sections.
%% Covers the same value-size logic as peek_value_size without calling it,
%% so the suite works when the dep is built without exporting peek_value_size (e.g. CI).
peek_described_section_total_sizes(_Config) ->
    Sections = [
        {described, {ulong, 16#75}, {binary, <<>>}},
        {described, {ulong, 16#75}, {binary, <<"x">>}},
        {described, {ulong, 16#75}, {binary, <<"payload">>}},
        {described, {symbol, <<"amqp:data:binary">>}, {binary, <<"x">>}},
        {described, {symbol, <<"amqp:properties:list">>}, {list, []}},
        {described, {symbol, <<"URL">>}, {utf8, <<"http://example.org">>}}
    ],
    lists:foreach(
      fun(Term) ->
              Bin = iolist_to_binary(amqp10_binary_generator:generate(Term)),
              {_Descriptor, TotalSize} = amqp10_binary_parser:peek(Bin),
              ?assertEqual(byte_size(Bin), TotalSize,
                          "peek total size must equal section byte size")
      end,
      Sections).

peek_described_section(_Config) ->
    %% v1_0.data: described {ulong, 0x75}, value {binary, <<"x">>}
    DataSection = {described, {ulong, 16#75}, {binary, <<"x">>}},
    Bin = iolist_to_binary(amqp10_binary_generator:generate(DataSection)),
    {Descriptor, TotalSize} = amqp10_binary_parser:peek(Bin),
    ?assertEqual('v1_0.data', element(1, amqp10_framing0:record_for(Descriptor))),
    ?assertEqual(6, TotalSize),
    %% v1_0.properties (symbol descriptor) with empty list value
    PropsSection = {described, {symbol, <<"amqp:properties:list">>}, {list, []}},
    BinProps = iolist_to_binary(amqp10_binary_generator:generate(PropsSection)),
    {DescriptorProps, TotalSizeProps} = amqp10_binary_parser:peek(BinProps),
    ?assertEqual('v1_0.properties', element(1, amqp10_framing0:record_for(DescriptorProps))),
    ?assertEqual(byte_size(BinProps), TotalSizeProps).

peek_non_described_throws(_Config) ->
    %% First byte must be ?DESCRIBED (0); any other type throws
    ?assertThrow({not_described_type, 16#40}, amqp10_binary_parser:peek(<<16#40>>)),
    ?assertThrow({not_described_type, 16#41}, amqp10_binary_parser:peek(<<16#41, 0>>)).

peek_total_size_matches_parse(_Config) ->
    %% For any described type, peek total size must equal bytes consumed by parse
    Sections = [
        {described, {ulong, 16#75}, {binary, <<>>}},
        {described, {ulong, 16#75}, {binary, <<"payload">>}},
        {described, {symbol, <<"amqp:data:binary">>}, {binary, <<"x">>}},
        {described, {symbol, <<"URL">>}, {utf8, <<"http://example.org">>}}
    ],
    lists:foreach(
      fun(Term) ->
              Bin = iolist_to_binary(amqp10_binary_generator:generate(Term)),
              {_Descriptor, TotalSize} = amqp10_binary_parser:peek(Bin),
              {_Parsed, BytesParsed} = amqp10_binary_parser:parse(Bin),
              ?assertEqual(BytesParsed, TotalSize,
                          "peek total size must match parse bytes consumed")
      end,
      Sections).
