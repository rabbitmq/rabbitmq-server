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
     array32_count_exceeds_data,
     array_of_zero_width_elements,
     array_of_described_zero_width_elements_unsupported,
     unsupported_type,
     server_mode_symbolic_body_descriptors,
     server_mode_body_descriptor_prefix_collision,
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
             {array, boolean, [true, false, true]},
             %% Zero-width array elements (e.g. null repeated N times) are
             %% represented opaquely rather than expanded into N terms.
             {as_is, 16#f0, <<5:32, 3:32, 16#40>>},
             {as_is, 16#f0, <<5:32, 0:32, 16#40>>},
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
                %% element type, remaining input size
                65, 12},

    ?assertExit(Expected, amqp10_binary_parser:parse_many(Bin, [])).

array32_count_exceeds_data(_Config) ->
    TypeArray32 = 16#f0,
    TypeUByte = 16#50,
    %% 10 elements declared but only 3 data bytes available.
    Count = 10,
    Payload = <<Count:32, TypeUByte, 1, 2, 3>>,
    Bin = <<TypeArray32, (byte_size(Payload)):32, Payload/binary>>,
    ?assertExit({failed_to_parse_array_count_exceeds_input, TypeUByte, Count, 3},
                amqp10_binary_parser:parse(Bin)).

%% Zero-width array elements (null, booleans, uint0, ulong0, list0) cost zero
%% octets on the wire no matter how large Count is. Rather than materializing
%% Count terms (CWE-770: a small frame could otherwise amplify into gigabytes
%% of heap), the parser represents such an array as an opaque, constant-size value.
array_of_zero_width_elements(_Config) ->
    TypeArray32 = 16#f0,
    TypeArray8 = 16#e0,

    Check32 = fun(Type, Count) ->
                      Encoded = <<5:32, Count:32, Type>>,
                      Bin = <<TypeArray32, Encoded/binary>>,
                      {Parsed, ParsedSize} = amqp10_binary_parser:parse(Bin),
                      ?assertEqual({as_is, TypeArray32, Encoded}, Parsed),
                      ?assertEqual(byte_size(Bin), ParsedSize),
                      %% Re-encoding reproduces the exact original bytes.
                      ?assertEqual(Bin, iolist_to_binary(amqp10_binary_generator:generate(Parsed)))
              end,
    [Check32(Type, 10) || Type <- [16#40, 16#41, 16#42, 16#43, 16#44, 16#45]],

    %% No limit on Count: an absurd 32-bit count is represented exactly as cheaply as a small one.
    Check32(16#40, 16#FFFFFFFF),

    %% Array8 form (0xe0) is parsed just as cheaply, and the selector is
    %% preserved rather than normalized to array32: re-encoding reproduces
    %% the exact original 4 bytes, not an inflated 10-byte array32 form.
    Check8 = fun(Type, Count) ->
                     Encoded = <<2, Count, Type>>,
                     Bin = <<TypeArray8, Encoded/binary>>,
                     {Parsed, ParsedSize} = amqp10_binary_parser:parse(Bin),
                     ?assertEqual({as_is, TypeArray8, Encoded}, Parsed),
                     ?assertEqual(byte_size(Bin), ParsedSize),
                     ?assertEqual(Bin, iolist_to_binary(amqp10_binary_generator:generate(Parsed)))
             end,
    [Check8(Type, 10) || Type <- [16#40, 16#41, 16#42, 16#43, 16#44, 16#45]],
    Check8(16#40, 255),

    %% Nested as an element of an outer array of arrays, the preserved array8
    %% selector must be upgraded to array32 framing: RabbitMQ's generator
    %% always uses the large (32-bit) form for nested array elements (see
    %% constructor/1: "use large array type for all nested arrays").
    {Inner8, _} = amqp10_binary_parser:parse(<<TypeArray8, 2:8, 10:8, 16#40>>),
    OuterBin = iolist_to_binary(amqp10_binary_generator:generate({array, array, [Inner8]})),
    {{array, array, [InnerParsedBack]}, _} = amqp10_binary_parser:parse(OuterBin),
    ?assertEqual({as_is, TypeArray32, <<5:32, 10:32, 16#40>>}, InnerParsedBack).

%% An array of described zero-width elements (e.g. a described null repeated
%% Count times) carries no information beyond its count and descriptor. This
%% combination isn't supported: it is rejected cleanly instead of being
%% expanded (unbounded memory) or given a bespoke opaque representation.
array_of_described_zero_width_elements_unsupported(_Config) ->
    %% ?DESCRIBED, utf8 "URL" descriptor, null (zero-width) element type.
    DescribedNull = <<0, 16#a1, 3, "URL", 16#40>>,
    Count = 16#FFFFFFFF,
    CountAndV = <<Count:32, DescribedNull/binary>>,
    Bin = <<16#f0, (byte_size(CountAndV)):32, CountAndV/binary>>,
    ?assertExit({array_of_described_zero_width_elements_unsupported, Count},
                amqp10_binary_parser:parse(Bin)).

unsupported_type(_Config) ->
    UnsupportedType = 16#02,
    Bin = <<UnsupportedType, "hey">>,
    Expected = {primitive_type_unsupported, UnsupportedType, {position, 0}},
    ?assertThrow(Expected, amqp10_binary_parser:parse_many(Bin, [])).

%% A symbolic body descriptor of the exact registered length is classified as
%% the corresponding standard body section in server_mode.
server_mode_symbolic_body_descriptors(_Config) ->
    Data8 = <<0, 16#a3, 16, "amqp:data:binary", 16#a0, 1, "x">>,
    Seq8 = <<0, 16#a3, 23, "amqp:amqp-sequence:list", 16#45>>,
    Value8 = <<0, 16#a3, 17, "amqp:amqp-value:*", 16#a1, 1, "x">>,
    ?assertEqual([{{pos, 0}, {body, 16#75}}],
                 amqp10_binary_parser:parse_many(Data8, [server_mode])),
    ?assertEqual([{{pos, 0}, {body, 16#76}}],
                 amqp10_binary_parser:parse_many(Seq8, [server_mode])),
    ?assertEqual([{{pos, 0}, {body, 16#77}}],
                 amqp10_binary_parser:parse_many(Value8, [server_mode])),
    %% The sym32 forms must be classified identically.
    Data32 = <<0, 16#b3, 16:32, "amqp:data:binary", 16#a0, 1, "x">>,
    Seq32 = <<0, 16#b3, 23:32, "amqp:amqp-sequence:list", 16#45>>,
    Value32 = <<0, 16#b3, 17:32, "amqp:amqp-value:*", 16#a1, 1, "x">>,
    ?assertEqual([{{pos, 0}, {body, 16#75}}],
                 amqp10_binary_parser:parse_many(Data32, [server_mode])),
    ?assertEqual([{{pos, 0}, {body, 16#76}}],
                 amqp10_binary_parser:parse_many(Seq32, [server_mode])),
    ?assertEqual([{{pos, 0}, {body, 16#77}}],
                 amqp10_binary_parser:parse_many(Value32, [server_mode])).

server_mode_body_descriptor_prefix_collision(_Config) ->
    %% Well-formed but unknown descriptor "amqp:data:binary@" (length 17).
    Collision8 = <<0, 16#a3, 17, "amqp:data:binary", $@, 16#a0, 3, "abc">>,
    ?assertEqual(
       [{described, {symbol, <<"amqp:data:binary@">>}, {binary, <<"abc">>}}],
       amqp10_binary_parser:parse_many(Collision8, [server_mode])),

    %% The same collision via the sym32 constructor.
    Collision32 = <<0, 16#b3, 17:32, "amqp:data:binary", $@, 16#a0, 3, "abc">>,
    ?assertEqual(
       [{described, {symbol, <<"amqp:data:binary@">>}, {binary, <<"abc">>}}],
       amqp10_binary_parser:parse_many(Collision32, [server_mode])),

    %% A prefix collision on the amqp-value descriptor.
    ValueCollision = <<0, 16#a3, 18, "amqp:amqp-value:*!", 16#a1, 1, "x">>,
    ?assertEqual(
       [{described, {symbol, <<"amqp:amqp-value:*!">>}, {utf8, <<"x">>}}],
       amqp10_binary_parser:parse_many(ValueCollision, [server_mode])).

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
