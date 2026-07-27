-module(binary_parser_SUITE).

-compile(export_all).

-export([
         ]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").

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
     validate_valid_messages,
     validate_section_after_body,
     validate_section_out_of_order,
     validate_duplicate_sections,
     validate_body_sections,
     validate_too_many_sections,
     validate_footer,
     validate_unknown_section,
     validate_section_value_type,
     validate_nested_section_descriptor,
     validate_malformed_encodings,
     validate_symbolic_descriptors,
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
                 amqp10_binary_parser:parse_many(Data8, [{server_mode, false}])),
    ?assertEqual([{{pos, 0}, {body, 16#76}}],
                 amqp10_binary_parser:parse_many(Seq8, [{server_mode, false}])),
    ?assertEqual([{{pos, 0}, {body, 16#77}}],
                 amqp10_binary_parser:parse_many(Value8, [{server_mode, false}])),
    %% The sym32 forms must be classified identically.
    Data32 = <<0, 16#b3, 16:32, "amqp:data:binary", 16#a0, 1, "x">>,
    Seq32 = <<0, 16#b3, 23:32, "amqp:amqp-sequence:list", 16#45>>,
    Value32 = <<0, 16#b3, 17:32, "amqp:amqp-value:*", 16#a1, 1, "x">>,
    ?assertEqual([{{pos, 0}, {body, 16#75}}],
                 amqp10_binary_parser:parse_many(Data32, [{server_mode, false}])),
    ?assertEqual([{{pos, 0}, {body, 16#76}}],
                 amqp10_binary_parser:parse_many(Seq32, [{server_mode, false}])),
    ?assertEqual([{{pos, 0}, {body, 16#77}}],
                 amqp10_binary_parser:parse_many(Value32, [{server_mode, false}])).

server_mode_body_descriptor_prefix_collision(_Config) ->
    %% Well-formed but unknown descriptor "amqp:data:binary@" (length 17).
    Collision8 = <<0, 16#a3, 17, "amqp:data:binary", $@, 16#a0, 3, "abc">>,
    ?assertEqual(
       [{described, {symbol, <<"amqp:data:binary@">>}, {binary, <<"abc">>}}],
       amqp10_binary_parser:parse_many(Collision8, [{server_mode, false}])),

    %% The same collision via the sym32 constructor.
    Collision32 = <<0, 16#b3, 17:32, "amqp:data:binary", $@, 16#a0, 3, "abc">>,
    ?assertEqual(
       [{described, {symbol, <<"amqp:data:binary@">>}, {binary, <<"abc">>}}],
       amqp10_binary_parser:parse_many(Collision32, [{server_mode, false}])),

    %% A prefix collision on the amqp-value descriptor.
    ValueCollision = <<0, 16#a3, 18, "amqp:amqp-value:*!", 16#a1, 1, "x">>,
    ?assertEqual(
       [{described, {symbol, <<"amqp:amqp-value:*!">>}, {utf8, <<"x">>}}],
       amqp10_binary_parser:parse_many(ValueCollision, [{server_mode, false}])).

%%%===================================================================
%%% Strict server mode: message section order and cardinality [§3.2]
%%%===================================================================

%% Descriptor codes of the message sections [§3.2].
-define(HEADER, 16#70).
-define(DELIVERY_ANNOTATIONS, 16#71).
-define(MESSAGE_ANNOTATIONS, 16#72).
-define(PROPERTIES, 16#73).
-define(APPLICATION_PROPERTIES, 16#74).
-define(DATA, 16#75).
-define(AMQP_SEQUENCE, 16#76).
-define(AMQP_VALUE, 16#77).
-define(FOOTER, 16#78).

validate_valid_messages(_Config) ->
    Messages = [
                [data()],
                [data(), data(), data()],
                [sequence()],
                [sequence(), sequence()],
                [value()],
                [data(), footer()],
                [value(), footer()],
                [sequence(), sequence(), footer()],
                [header(), data()],
                [delivery_annotations(), data()],
                [message_annotations(), data()],
                [properties(), data()],
                [application_properties(), data()],
                [header(), delivery_annotations(), message_annotations(),
                 properties(), application_properties(), data(), footer()],
                [header(), message_annotations(), properties(), value()],
                %% Sections in between may be omitted.
                [header(), properties(), data()],
                [delivery_annotations(), application_properties(), sequence()]
               ],
    lists:foreach(
      fun(Sections) ->
              Bin = iolist_to_binary(Sections),
              %% Strict mode accepts the message and returns exactly what the
              %% non-strict server mode returns.
              ?assertEqual(amqp10_binary_parser:parse_many(Bin, [{server_mode, false}]),
                           amqp10_binary_parser:parse_many(Bin, [{server_mode, true}]))
      end, Messages),
    %% An empty payload is left to the caller to report: no body section is
    %% returned, but parsing itself does not fail.
    ?assertEqual([], amqp10_binary_parser:parse_many(<<>>, [{server_mode, true}])).

%% The security relevant case: RabbitMQ stops parsing at the body, so a section
%% that follows the body would be invisible to the broker while remaining
%% visible to a consumer whose parser tolerates it. The prime example is a
%% properties section carrying a forged user-id.
validate_section_after_body(_Config) ->
    ForgedProperties = section(?PROPERTIES,
                               list([<<16#40>>, binary(<<"admin">>)])),
    lists:foreach(
      fun({Body, Late}) ->
              Bin = iolist_to_binary([Body, Late]),
              %% The non-strict server mode neither sees nor rejects it.
              ?assertMatch([{{pos, 0}, {body, _}}],
                           amqp10_binary_parser:parse_many(Bin, [{server_mode, false}])),
              ?assertThrow({unexpected_message_section, _, _},
                           amqp10_binary_parser:parse_many(Bin, [{server_mode, true}]))
      end,
      [{Body, Late} || Body <- [data(), sequence(), value()],
                       Late <- [ForgedProperties,
                                header(),
                                delivery_annotations(),
                                message_annotations(),
                                properties(),
                                application_properties()]]).

validate_section_out_of_order(_Config) ->
    OutOfOrder = [
                  [delivery_annotations(), header(), data()],
                  [message_annotations(), header(), data()],
                  [message_annotations(), delivery_annotations(), data()],
                  [properties(), header(), data()],
                  [properties(), message_annotations(), data()],
                  [application_properties(), properties(), data()],
                  [application_properties(), header(), data()]
                 ],
    lists:foreach(
      fun(Sections) ->
              Bin = iolist_to_binary(Sections),
              ?assertThrow({unexpected_message_section, _, _},
                           amqp10_binary_parser:parse_many(Bin, [{server_mode, true}]))
      end, OutOfOrder).

validate_duplicate_sections(_Config) ->
    Duplicated = [header(),
                  delivery_annotations(),
                  message_annotations(),
                  properties(),
                  application_properties()],
    lists:foreach(
      fun(Section) ->
              Bin = iolist_to_binary([Section, Section, data()]),
              ?assertThrow({unexpected_message_section, _, _},
                           amqp10_binary_parser:parse_many(Bin, [{server_mode, true}]))
      end, Duplicated).

%% "The body consists of one of the following three choices: one or more data
%% sections, one or more amqp-sequence sections, or a single amqp-value
%% section." [§3.2]
validate_body_sections(_Config) ->
    Invalid = [
               [data(), sequence()],
               [data(), value()],
               [sequence(), data()],
               [sequence(), value()],
               [value(), data()],
               [value(), sequence()],
               [value(), value()],
               %% The body is mandatory, so a footer alone is not a message.
               [footer()],
               [message_annotations(), footer()]
              ],
    lists:foreach(
      fun(Sections) ->
              Bin = iolist_to_binary(Sections),
              ?assertThrow({unexpected_message_section, _, _},
                           amqp10_binary_parser:parse_many(Bin, [{server_mode, true}]))
      end, Invalid),
    %% A message without any body section is left to the caller to report.
    NoBody = iolist_to_binary([header(), message_annotations()]),
    ?assertEqual(amqp10_binary_parser:parse_many(NoBody, [{server_mode, false}]),
                 amqp10_binary_parser:parse_many(NoBody, [{server_mode, true}])).

%% The spec does not limit how many body sections a message may consist of.
%% Since every section has to be validated, the total number of sections is
%% capped so that a single message cannot consume disproportionate CPU time.
validate_too_many_sections(_Config) ->
    Data = iolist_to_binary(data()),
    Sections = fun(N) -> binary:copy(Data, N) end,
    %% The limit is part of the error, so this test does not duplicate it.
    Max = try amqp10_binary_parser:parse_many(Sections(100_000), [{server_mode, true}]) of
              _ -> ct:fail(expected_too_many_message_sections)
          catch throw:{too_many_message_sections, M, {position, _}} ->
                    M
          end,
    ?assertMatch([{{pos, 0}, {body, ?DATA}}],
                 amqp10_binary_parser:parse_many(Sections(Max), [{server_mode, true}])),
    ?assertThrow({too_many_message_sections, Max, _},
                 amqp10_binary_parser:parse_many(Sections(Max + 1), [{server_mode, true}])),
    %% Every section counts towards the limit, not only the body sections.
    Prefix = iolist_to_binary([header(), properties()]),
    ?assertMatch([_Header, {{pos, _}, _Properties}, {{pos, _}, {body, ?DATA}}],
                 amqp10_binary_parser:parse_many(
                   <<Prefix/binary, (Sections(Max - 2))/binary>>, [{server_mode, true}])),
    ?assertThrow({too_many_message_sections, Max, _},
                 amqp10_binary_parser:parse_many(
                   <<Prefix/binary, (Sections(Max - 1))/binary>>, [{server_mode, true}])),
    %% Without strict mode, parsing stops at the body: there is nothing to cap.
    ?assertMatch([{{pos, 0}, {body, ?DATA}}],
                 amqp10_binary_parser:parse_many(Sections(100_000), [{server_mode, false}])).

%% "Zero or one footer sections." [§3.2] The footer terminates the message.
validate_footer(_Config) ->
    ?assertThrow({unexpected_message_section, footer, _},
                 amqp10_binary_parser:parse_many(
                   iolist_to_binary([data(), footer(), footer()]), [{server_mode, true}])),
    ?assertThrow({unexpected_message_section, data, _},
                 amqp10_binary_parser:parse_many(
                   iolist_to_binary([data(), footer(), data()]), [{server_mode, true}])),
    ?assertThrow({not_a_message_section, _},
                 amqp10_binary_parser:parse_many(
                   iolist_to_binary([data(), footer(), <<16#40>>]), [{server_mode, true}])),
    %% Trailing bytes that are not a section are rejected as well.
    ?assertThrow({not_a_message_section, _},
                 amqp10_binary_parser:parse_many(
                   iolist_to_binary([data(), <<16#40>>]), [{server_mode, true}])).

validate_unknown_section(_Config) ->
    %% A numeric descriptor that is not a message section descriptor.
    ?assertThrow({unexpected_message_section, 16#7f, {position, 0}},
                 amqp10_binary_parser:parse_many(
                   iolist_to_binary([section(16#7f, <<16#40>>), data()]),
                   [{server_mode, true}])),
    %% A symbolic descriptor that is not a message section descriptor.
    Unknown = <<0, 16#a3, 3, "URL", 16#40>>,
    ?assertThrow({unexpected_message_section, unknown_section_descriptor,
                  {position, 0}},
                 amqp10_binary_parser:parse_many(
                   iolist_to_binary([Unknown, data()]), [{server_mode, true}])),
    %% A descriptor that is neither a ulong nor a symbol.
    ?assertThrow({not_a_message_section, {position, 0}},
                 amqp10_binary_parser:parse_many(<<0, 16#40, 16#40>>, [{server_mode, true}])).

%% Every section value has a fixed outer type: a list, a map or, for the data
%% section, a binary. Only the amqp-value section holds any AMQP type [§3.2].
validate_section_value_type(_Config) ->
    Map = <<16#c1, 1, 0>>,
    List = <<16#c0, 1, 0>>,
    Bin = <<16#a0, 0>>,
    Null = <<16#40>>,
    %% A described value would let a section descriptor hide inside a section
    %% that must hold a list, a map or a binary.
    Described = iolist_to_binary(section(?PROPERTIES, List)),
    WrongTypes = [{?HEADER, Map}, {?HEADER, Bin}, {?HEADER, Null},
                  {?HEADER, Described},
                  {?DELIVERY_ANNOTATIONS, List}, {?DELIVERY_ANNOTATIONS, Null},
                  {?MESSAGE_ANNOTATIONS, List}, {?MESSAGE_ANNOTATIONS, Bin},
                  {?MESSAGE_ANNOTATIONS, Described},
                  {?PROPERTIES, Map}, {?PROPERTIES, Null},
                  {?PROPERTIES, Described},
                  {?APPLICATION_PROPERTIES, List},
                  {?DATA, List}, {?DATA, Map}, {?DATA, Null},
                  {?DATA, Described},
                  {?AMQP_SEQUENCE, Map}, {?AMQP_SEQUENCE, Bin}],
    lists:foreach(
      fun({Code, Value}) ->
              Payload = iolist_to_binary([section(Code, Value), data()]),
              ?assertThrow({invalid_section_value, _, _},
                           amqp10_binary_parser:parse_many(Payload, [{server_mode, true}]))
      end, WrongTypes),
    %% The footer value must be a map too.
    ?assertThrow({invalid_section_value, map, _},
                 amqp10_binary_parser:parse_many(
                   iolist_to_binary([data(), section(?FOOTER, List)]), [{server_mode, true}])),
    %% An amqp-value section accepts any AMQP type, including a described one.
    lists:foreach(
      fun(Value) ->
              Payload = iolist_to_binary(section(?AMQP_VALUE, Value)),
              ?assertEqual([{{pos, 0}, {body, ?AMQP_VALUE}}],
                           amqp10_binary_parser:parse_many(Payload, [{server_mode, true}]))
      end, [Null, Bin, List, Map,
            <<16#45>>,
            <<16#a1, 2, "hi">>,
            <<16#83, 0:64>>,
            <<16#98, 0:128>>,
            <<16#f0, 5:32, 3:32, 16#40>>,
            <<0, 16#a1, 3, "URL", 16#a1, 1, $x>>,
            <<0, 0, 16#a1, 3, "URL", 16#40, 16#a1, 1, $x>>]).

%% "The descriptor portion of a described format code is itself any valid AMQP
%% encoded value, including other described values." [§1.2] A section
%% descriptor nested inside a section value must not be taken for a section:
%% otherwise a properties section hidden inside a message-annotations map could
%% become the properties of the message.
validate_nested_section_descriptor(_Config) ->
    Nested = [iolist_to_binary(data()),
              iolist_to_binary(properties()),
              iolist_to_binary(header()),
              iolist_to_binary(footer())],
    %% Nested inside a message-annotations map.
    Key = <<16#a3, 2, "x-">>,
    lists:foreach(
      fun(N) ->
              Bin = iolist_to_binary(
                      [section(?MESSAGE_ANNOTATIONS, map([Key, N])), data()]),
              [{described, {ulong, ?MESSAGE_ANNOTATIONS}, {map, Content}},
               {{pos, _}, {body, ?DATA}}] =
                  amqp10_binary_parser:parse_many(Bin, [{server_mode, true}]),
              %% The nested described type stays the annotation value.
              ?assertMatch([{{symbol, <<"x-">>}, {described, _, _}}], Content)
      end, Nested),
    %% Nested inside the properties list, that is as the message-id.
    lists:foreach(
      fun(N) ->
              Bin = iolist_to_binary(
                      [section(?PROPERTIES, list([N])), data()]),
              [{{pos, 0}, {described, {ulong, ?PROPERTIES}, {list, Fields}}},
               {{pos, _}, {body, ?DATA}}] =
                  amqp10_binary_parser:parse_many(Bin, [{server_mode, true}]),
              ?assertMatch([{described, _, _}], Fields)
      end, Nested).

validate_malformed_encodings(_Config) ->
    %% A compound whose size is too small to hold its count field.
    ?assertThrow({invalid_compound_size, _},
                 amqp10_binary_parser:parse_many(
                   iolist_to_binary([section(?MESSAGE_ANNOTATIONS, <<16#c1, 0>>),
                                     data()]), [{server_mode, true}])),
    %% A map with an odd number of elements.
    ?assertThrow(map_with_odd_number_of_elements,
                 amqp10_binary_parser:parse_many(
                   iolist_to_binary(
                     [section(?MESSAGE_ANNOTATIONS,
                              <<16#c1, 5, 1, 16#a3, 2, "x-">>),
                      data()]), [{server_mode, true}])),
    %% A data section whose declared size exceeds the remaining input.
    ?assertThrow({invalid_section_value, binary, _},
                 amqp10_binary_parser:parse_many(
                   <<0, 16#53, ?DATA, 16#a0, 200, "short">>, [{server_mode, true}])),
    %% An amqp-value section whose declared size exceeds the remaining input.
    ?assertThrow({invalid_section_value, any, _},
                 amqp10_binary_parser:parse_many(
                   <<0, 16#53, ?AMQP_VALUE, 16#a1, 200, "short">>, [{server_mode, true}])),
    %% A truncated section descriptor.
    ?assertThrow({not_a_message_section, _},
                 amqp10_binary_parser:parse_many(<<0, 16#53>>, [{server_mode, true}])).

%% All four descriptor encodings are validated identically.
validate_symbolic_descriptors(_Config) ->
    Symbols = #{?HEADER => <<"amqp:header:list">>,
                ?DELIVERY_ANNOTATIONS => <<"amqp:delivery-annotations:map">>,
                ?MESSAGE_ANNOTATIONS => <<"amqp:message-annotations:map">>,
                ?PROPERTIES => <<"amqp:properties:list">>,
                ?APPLICATION_PROPERTIES => <<"amqp:application-properties:map">>,
                ?DATA => <<"amqp:data:binary">>,
                ?AMQP_SEQUENCE => <<"amqp:amqp-sequence:list">>,
                ?AMQP_VALUE => <<"amqp:amqp-value:*">>,
                ?FOOTER => <<"amqp:footer:map">>},
    Encode = fun(Descriptor, Code, Value) ->
                     Sym = maps:get(Code, Symbols),
                     Size = byte_size(Sym),
                     D = case Descriptor of
                             small_ulong -> <<16#53, Code>>;
                             ulong -> <<16#80, Code:64>>;
                             sym8 -> <<16#a3, Size:8, Sym/binary>>;
                             sym32 -> <<16#b3, Size:32, Sym/binary>>
                         end,
                     <<0, D/binary, Value/binary>>
             end,
    lists:foreach(
      fun(Descriptor) ->
              E = fun(Code, Value) -> Encode(Descriptor, Code, Value) end,
              Data = E(?DATA, <<16#a0, 1, "x">>),
              Props = E(?PROPERTIES, <<16#c0, 1, 0>>),
              Valid = <<(E(?HEADER, <<16#c0, 1, 0>>))/binary,
                        (E(?MESSAGE_ANNOTATIONS, <<16#c1, 1, 0>>))/binary,
                        Props/binary,
                        Data/binary,
                        (E(?FOOTER, <<16#c1, 1, 0>>))/binary>>,
              %% Strict mode validates the sections. Which of the four
              %% descriptor encodings was used on the wire is preserved by
              %% the parsed sections, just like in non-strict server mode.
              Parsed = amqp10_binary_parser:parse_many(Valid, [{server_mode, true}]),
              ?assertEqual(amqp10_binary_parser:parse_many(Valid, [{server_mode, false}]),
                           Parsed),
              ?assertMatch([#'v1_0.header'{},
                            #'v1_0.message_annotations'{},
                            {{pos, _}, #'v1_0.properties'{}},
                            {{pos, _}, {body, ?DATA}}],
                           amqp10_framing:decode_bin(Valid, [{server_mode, true}])),
              %% Properties after the body are rejected for every encoding.
              ?assertThrow({unexpected_message_section, properties, _},
                           amqp10_binary_parser:parse_many(
                             <<Data/binary, Props/binary>>, [{server_mode, true}]))
      end, [small_ulong, ulong, sym8, sym32]).

%%%===================================================================
%%% Section encoding helpers
%%%===================================================================

section(Code, Value) ->
    [<<0, 16#53, Code>>, Value].

list(Elements) ->
    Bin = iolist_to_binary(Elements),
    <<16#c0, (byte_size(Bin) + 1), (length(Elements)), Bin/binary>>.

map(Elements) ->
    Bin = iolist_to_binary(Elements),
    <<16#c1, (byte_size(Bin) + 1), (length(Elements)), Bin/binary>>.

binary(Bin) ->
    <<16#a0, (byte_size(Bin)), Bin/binary>>.

header() -> section(?HEADER, list([<<16#41>>])).
delivery_annotations() -> section(?DELIVERY_ANNOTATIONS, map([])).
message_annotations() -> section(?MESSAGE_ANNOTATIONS, map([])).
properties() -> section(?PROPERTIES, list([])).
application_properties() -> section(?APPLICATION_PROPERTIES, map([])).
data() -> section(?DATA, binary(<<"body">>)).
sequence() -> section(?AMQP_SEQUENCE, <<16#45>>).
value() -> section(?AMQP_VALUE, <<16#a1, 2, "hi">>).
footer() -> section(?FOOTER, map([])).

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
