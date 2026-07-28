%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% NB: When compiling this file with "ERL_COMPILER_OPTIONS=bin_opt_info"
%% make sure that all code outputs "OPTIMIZED: match context reused",
%% i.e. neither "BINARY CREATED" nor "NOT OPTIMIZED" should be output.
%% The only exception are arrays since arrays aren't used in the hot path.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-module(amqp10_binary_parser).

-include("amqp10_framing.hrl").

-export([parse/1,
         parse_many/2,
         peek/1]).

-ifdef(TEST).
-export([peek_value_size_fixed_test/0,
         peek_value_size_variable_test/0]).
-endif.

%% §1.6
-define(CODE_ULONG, 16#80).
-define(CODE_SMALL_ULONG, 16#53).
-define(CODE_SYM_8, 16#a3).
-define(CODE_SYM_32, 16#b3).
%% §3.2
-define(DESCRIPTOR_CODE_HEADER, 16#70).
-define(DESCRIPTOR_CODE_DELIVERY_ANNOTATIONS, 16#71).
-define(DESCRIPTOR_CODE_MESSAGE_ANNOTATIONS, 16#72).
-define(DESCRIPTOR_CODE_PROPERTIES, 16#73).
-define(DESCRIPTOR_CODE_APPLICATION_PROPERTIES, 16#74).
-define(DESCRIPTOR_CODE_DATA, 16#75).
-define(DESCRIPTOR_CODE_AMQP_SEQUENCE, 16#76).
-define(DESCRIPTOR_CODE_AMQP_VALUE, 16#77).
-define(DESCRIPTOR_CODE_FOOTER, 16#78).

%% The spec does not limit how many body sections a message may consist of.
%% Since a message must be validated section by section, an unlimited number of
%% tiny sections would let a single message consume disproportionate CPU time.
%% No real world sender should come anywhere near this limit.
-define(MAX_MSG_SECTIONS, 10_000).

%% server_mode is a special parsing mode used by RabbitMQ when parsing
%% AMQP message sections from an AMQP client. This mode:
%% 1. stops parsing when the body starts, and
%% 2. returns the start byte position of each parsed bare message section.
%%
%% If Validate is true, the order and cardinality of all message sections is
%% additionally validated as defined in §3.2.
-type opt() :: {server_mode, Validate :: boolean()}.
-type opts() :: [opt()].

-export_type([opts/0]).

%% Parses only the 1st AMQP type (including possible nested AMQP types).
-spec parse(binary()) ->
    {amqp10_binary_generator:amqp10_type(), BytesParsed :: non_neg_integer()}.
parse(Binary) ->
    parse(Binary, 0).

parse(<<?DESCRIBED, Rest/binary>>, B) ->
    {Descriptor, B1} = parse(Rest),
    <<_ParsedDescriptorBin:B1/binary, Rest1/binary>> = Rest,
    {Value, B2} = parse(Rest1),
    {{described, Descriptor, Value}, B+1+B1+B2};
parse(<<16#40, _/binary>>, B) -> {null,        B+1};
parse(<<16#41, _/binary>>, B) -> {true,        B+1};
parse(<<16#42, _/binary>>, B) -> {false,       B+1};
parse(<<16#43, _/binary>>, B) -> {{uint, 0},   B+1};
parse(<<16#44, _/binary>>, B) -> {{ulong, 0},  B+1};
%% Fixed-widths. Most integral types have a compact encoding as a byte.
parse(<<16#50, V:8/unsigned,  _/binary>>, B) -> {{ubyte, V},      B+2};
parse(<<16#51, V:8/signed,    _/binary>>, B) -> {{byte, V},       B+2};
parse(<<16#52, V:8/unsigned,  _/binary>>, B) -> {{uint, V},       B+2};
parse(<<?CODE_SMALL_ULONG, V:8/unsigned, _/binary>>, B) -> {{ulong, V}, B+2};
parse(<<16#54, V:8/signed,    _/binary>>, B) -> {{int, V},        B+2};
parse(<<16#55, V:8/signed,    _/binary>>, B) -> {{long, V},       B+2};
parse(<<16#56, 0:8/unsigned,  _/binary>>, B) -> {false,           B+2};
parse(<<16#56, 1:8/unsigned,  _/binary>>, B) -> {true,            B+2};
parse(<<16#60, V:16/unsigned, _/binary>>, B) -> {{ushort, V},     B+3};
parse(<<16#61, V:16/signed,   _/binary>>, B) -> {{short, V},      B+3};
parse(<<16#70, V:32/unsigned, _/binary>>, B) -> {{uint, V},       B+5};
parse(<<16#71, V:32/signed,   _/binary>>, B) -> {{int, V},        B+5};
parse(<<16#72, V:32/float,    _/binary>>, B) -> {{float, V},      B+5};
parse(<<16#73, V:32,          _/binary>>, B) -> {{char, V},       B+5};
parse(<<?CODE_ULONG, V:64/unsigned, _/binary>>, B) -> {{ulong, V},B+9};
parse(<<16#81, V:64/signed,   _/binary>>, B) -> {{long, V},       B+9};
parse(<<16#82, V:64/float,    _/binary>>, B) -> {{double, V},     B+9};
parse(<<16#83, TS:64/signed,  _/binary>>, B) -> {{timestamp, TS}, B+9};
parse(<<16#98, Uuid:16/binary,_/binary>>, B) -> {{uuid, Uuid},    B+17};
%% Variable-widths
parse(<<16#a0, S:8, V:S/binary,_/binary>>, B)-> {{binary, V}, B+2+S};
parse(<<16#a1, S:8, V:S/binary,_/binary>>, B)-> {{utf8, V},   B+2+S};
parse(<<?CODE_SYM_8, S:8, V:S/binary,_/binary>>, B) -> {{symbol, V}, B+2+S};
parse(<<?CODE_SYM_32, S:32,V:S/binary,_/binary>>, B) -> {{symbol, V}, B+5+S};
parse(<<16#b0, S:32,V:S/binary,_/binary>>, B)-> {{binary, V}, B+5+S};
parse(<<16#b1, S:32,V:S/binary,_/binary>>, B)-> {{utf8, V},   B+5+S};
%% Compounds
parse(<<16#45, _/binary>>, B) ->
    {{list, []}, B+1};
parse(<<16#c0, Size, _IgnoreCount, Value:(Size-1)/binary, _/binary>>, B) ->
    {{list, parse_many(Value, [])}, B+2+Size};
parse(<<16#c1, Size, _IgnoreCount, Value:(Size-1)/binary, _/binary>>, B) ->
    List = parse_many(Value, []),
    {{map, mapify(List)}, B+2+Size};
parse(<<16#d0, Size:32, _IgnoreCount:32, Value:(Size-4)/binary, _/binary>>, B) ->
    {{list, parse_many(Value, [])}, B+5+Size};
parse(<<16#d1, Size:32, _IgnoreCount:32, Value:(Size-4)/binary, _/binary>>, B) ->
    List = parse_many(Value, []),
    {{map, mapify(List)}, B+5+Size};
%% Arrays
parse(<<16#e0, S:8,CountAndV:S/binary,_/binary>>, B) ->
    {parse_array(8, CountAndV), B+2+S};
parse(<<16#f0, S:32,CountAndV:S/binary,_/binary>>, B) ->
    {parse_array(32, CountAndV), B+5+S};
%% NaN or +-inf
parse(<<16#72, V:4/binary, _/binary>>, B) ->
    {{as_is, 16#72, V}, B+5};
parse(<<16#82, V:8/binary, _/binary>>, B) ->
    {{as_is, 16#82, V}, B+9};
%% decimals
parse(<<16#74, V:4/binary, _/binary>>, B) ->
    {{as_is, 16#74, V}, B+5};
parse(<<16#84, V:8/binary, _/binary>>, B) ->
    {{as_is, 16#84, V}, B+9};
parse(<<16#94, V:16/binary, _/binary>>, B) ->
    {{as_is, 16#94, V}, B+17};
parse(<<Type, _/binary>>, B) ->
    throw({primitive_type_unsupported, Type, {position, B}}).

parse_array(UnitSize, Bin) ->
    <<Count:UnitSize, Bin1/binary>> = Bin,
    parse_array1(UnitSize, Count, Bin1).

parse_array1(UnitSize, Count, <<?DESCRIBED, Rest/binary>>) ->
    {Descriptor, B1} = parse(Rest),
    <<_ParsedDescriptorBin:B1/binary, Rest1/binary>> = Rest,
    case parse_array1(UnitSize, Count, Rest1) of
        {array, Type, List} ->
            Values = [{described, Descriptor, Value} || Value <- List],
            % this format cannot represent an empty array of described types
            {array, {described, Descriptor, Type}, Values};
        {as_is, _, _} ->
            exit({array_of_described_zero_width_elements_unsupported, Count})
    end;
parse_array1(UnitSize, Count, <<Type, ArrayBin/binary>>)
  when Type >= 16#40 andalso Type =< 16#45 ->
    %% This is an array that must have zero octets of data.
    case byte_size(ArrayBin) of
        0 ->
            %% "Count zero-width elements" costs a handful of bytes on the
            %% wire no matter how large Count is. Since RabbitMQ has no need to
            %% interpret this special type of array and to protect against CWE-770,
            %% instead of materialized as Count terms, keep it as an opaque,
            %% constant-size value.
            case UnitSize of
                8 -> {as_is, 16#e0, <<2:8, Count:8, Type>>};
                32 -> {as_is, 16#f0, <<5:32, Count:32, Type>>}
            end;
        Size ->
            exit({failed_to_parse_array_extra_input_remaining, Type, Size})
    end;
parse_array1(_UnitSize, Count, <<Type, ArrayBin/binary>>)
  when Count > byte_size(ArrayBin) ->
    exit({failed_to_parse_array_count_exceeds_input, Type, Count, byte_size(ArrayBin)});
parse_array1(_UnitSize, Count, <<Type, ArrayBin/binary>>) ->
    parse_array2(Count, Type, ArrayBin, []).

parse_array2(0, Type, <<>>, Acc) ->
    {array, parse_constructor(Type), lists:reverse(Acc)};
parse_array2(0, Type, Bin, Acc) ->
    exit({failed_to_parse_array_extra_input_remaining, Type, Bin, Acc});
parse_array2(Count, Type, <<>>, Acc) when Count > 0 ->
    exit({failed_to_parse_array_insufficient_input, Type, Count, Acc});
parse_array2(Count, Type, Bin, Acc) ->
    Size = array_element_size(Type, Bin),
    case Bin of
        <<Element:Size/binary, Rest/binary>> ->
            TotalSize = Size + 1,
            %% assertion
            {Value, TotalSize} = parse(<<Type, Element/binary>>),
            parse_array2(Count - 1, Type, Rest, [Value | Acc]);
        _ ->
            exit({failed_to_parse_array_insufficient_input, Type, Count, Acc})
    end.

%% Returns the byte size of a single array element, excluding the constructor
%% that all elements of the array share.
array_element_size(Type, _Bin) when Type >= 16#40 andalso Type =< 16#4f -> 0;
array_element_size(Type, _Bin) when Type >= 16#50 andalso Type =< 16#5f -> 1;
array_element_size(Type, _Bin) when Type >= 16#60 andalso Type =< 16#6f -> 2;
array_element_size(Type, _Bin) when Type >= 16#70 andalso Type =< 16#7f -> 4;
array_element_size(Type, _Bin) when Type >= 16#80 andalso Type =< 16#8f -> 8;
array_element_size(Type, _Bin) when Type >= 16#90 andalso Type =< 16#9f -> 16;
array_element_size(Type, <<Size:8, _/binary>>)
  when Type >= 16#a0 andalso Type =< 16#af;
       Type >= 16#c0 andalso Type =< 16#cf;
       Type >= 16#e0 andalso Type =< 16#ef ->
    1 + Size;
array_element_size(Type, <<Size:32, _/binary>>)
  when Type >= 16#b0 andalso Type =< 16#bf;
       Type >= 16#d0 andalso Type =< 16#df;
       Type >= 16#f0 andalso Type =< 16#ff ->
    4 + Size;
array_element_size(Type, _Bin) ->
    exit({failed_to_parse_array_element_size, Type}).

parse_constructor(?CODE_SYM_8) -> symbol;
parse_constructor(?CODE_SYM_32) -> symbol;
parse_constructor(16#a0) -> binary;
parse_constructor(16#a1) -> utf8;
parse_constructor(16#b0) -> binary;
parse_constructor(16#b1) -> utf8;
parse_constructor(16#50) -> ubyte;
parse_constructor(16#51) -> byte;
parse_constructor(16#60) -> ushort;
parse_constructor(16#61) -> short;
parse_constructor(16#70) -> uint;
parse_constructor(16#71) -> int;
parse_constructor(16#72) -> float;
parse_constructor(16#73) -> char;
parse_constructor(16#82) -> double;
parse_constructor(?CODE_ULONG) -> ulong;
parse_constructor(16#81) -> long;
parse_constructor(16#40) -> null;
parse_constructor(16#41) -> boolean;
parse_constructor(16#42) -> boolean;
parse_constructor(16#43) -> uint;
parse_constructor(16#44) -> ulong;
parse_constructor(16#45) -> list;
parse_constructor(16#56) -> boolean;
parse_constructor(16#83) -> timestamp;
parse_constructor(16#98) -> uuid;
parse_constructor(16#d0) -> list;
parse_constructor(16#d1) -> map;
parse_constructor(16#f0) -> array;
parse_constructor(0) -> described;
parse_constructor(X) ->
    exit({failed_to_parse_constructor, X}).

mapify([]) ->
    [];
mapify([Key, Value | Rest]) ->
    [{Key, Value} | mapify(Rest)];
mapify([_]) ->
    %% "Map encodings MUST contain an even number of items
    %% (i.e. an equal number of keys and values)." [1.6.23]
    throw(map_with_odd_number_of_elements).

%% Parses all AMQP types (or, in server_mode, stops when the body is reached).
%% This is an optimisation over calling parse/1 repeatedly.
%% We re-use the match context avoiding creation of sub binaries.
-spec parse_many(binary(), opts()) ->
    [amqp10_binary_generator:amqp10_type() |
     {{pos, non_neg_integer()},
      amqp10_binary_generator:amqp10_type() | {body, pos_integer()}}].
parse_many(Binary, Opts) ->
    case lists:keyfind(server_mode, 1, Opts) of
        {server_mode, Validate} ->
            Validate andalso validate_msg_sections(Binary),
            pm(Binary, true, 0);
        false ->
            pm(Binary, false, 0)
    end.

pm(<<>>, _, _) ->
    [];

%% We put function clauses that are more likely to match to the top as this results in better performance.
%% Constants.
pm(<<16#40, R/binary>>, O, B) -> [null | pm(R, O, B+1)];
pm(<<16#41, R/binary>>, O, B) -> [true | pm(R, O, B+1)];
pm(<<16#42, R/binary>>, O, B) -> [false | pm(R, O, B+1)];
pm(<<16#43, R/binary>>, O, B) -> [{uint, 0} | pm(R, O, B+1)];
%% Fixed-widths.
pm(<<16#44, R/binary>>, O, B)                            -> [{ulong, 0} | pm(R, O, B+1)];
pm(<<16#50, V:8/unsigned,  R/binary>>, O, B)             -> [{ubyte, V} | pm(R, O, B+2)];
pm(<<16#52, V:8/unsigned,  R/binary>>, O, B)             -> [{uint, V} | pm(R, O, B+2)];
pm(<<?CODE_SMALL_ULONG, V:8/unsigned,  R/binary>>, O, B) -> [{ulong, V} | pm(R, O, B+2)];
pm(<<16#70, V:32/unsigned, R/binary>>, O, B)             -> [{uint, V} | pm(R, O, B+5)];
pm(<<?CODE_ULONG, V:64/unsigned, R/binary>>, O, B)       -> [{ulong, V} | pm(R, O, B+9)];
%% Variable-widths
pm(<<16#a0, S:8, V:S/binary,R/binary>>, O, B)            -> [{binary, V} | pm(R, O, B+2+S)];
pm(<<16#a1, S:8, V:S/binary,R/binary>>, O, B)            -> [{utf8, V} | pm(R, O, B+2+S)];
pm(<<?CODE_SYM_8, S:8, V:S/binary,R/binary>>, O, B)      -> [{symbol, V} | pm(R, O, B+2+S)];
%% Compounds
pm(<<16#45, R/binary>>, O, B) ->
    [{list, []} | pm(R, O, B+1)];
pm(<<16#c0, S:8,CountAndValue:S/binary,R/binary>>, O, B) ->
    [{list, pm_compound(8, CountAndValue, B+2)} | pm(R, O, B+2+S)];
pm(<<16#c1, S:8,CountAndValue:S/binary,R/binary>>, O, B) ->
    List = pm_compound(8, CountAndValue, B+2),
    [{map, mapify(List)} | pm(R, O, B+2+S)];

%% We avoid guard tests: they improve readability, but result in worse performance.
%%
%% In server mode:
%% * Stop when we reach the message body (data or amqp-sequence or amqp-value section).
%% * Include byte positions for parsed bare message sections.
pm(<<?DESCRIBED, ?CODE_SMALL_ULONG, ?DESCRIPTOR_CODE_DATA, _Rest/binary>>, true, B) ->
    reached_body(B, ?DESCRIPTOR_CODE_DATA);
pm(<<?DESCRIBED, ?CODE_SMALL_ULONG, ?DESCRIPTOR_CODE_AMQP_SEQUENCE, _Rest/binary>>, true, B) ->
    reached_body(B, ?DESCRIPTOR_CODE_AMQP_SEQUENCE);
pm(<<?DESCRIBED, ?CODE_SMALL_ULONG, ?DESCRIPTOR_CODE_AMQP_VALUE, _Rest/binary>>, true, B) ->
    reached_body(B, ?DESCRIPTOR_CODE_AMQP_VALUE);
pm(<<?DESCRIBED, ?CODE_SMALL_ULONG, ?DESCRIPTOR_CODE_PROPERTIES, Rest0/binary>>, O = true, B) ->
    [Value | Rest] = pm(Rest0, O, B+3),
    [{{pos, B}, {described, {ulong, ?DESCRIPTOR_CODE_PROPERTIES}, Value}} | Rest];
pm(<<?DESCRIBED, ?CODE_SMALL_ULONG, ?DESCRIPTOR_CODE_APPLICATION_PROPERTIES, Rest0/binary>>, O = true, B) ->
    [Value | Rest] = pm(Rest0, O, B+3),
    [{{pos, B}, {described, {ulong, ?DESCRIPTOR_CODE_APPLICATION_PROPERTIES}, Value}} | Rest];
pm(<<?DESCRIBED, ?CODE_ULONG, ?DESCRIPTOR_CODE_DATA:64, _Rest/binary>>, true, B) ->
    reached_body(B, ?DESCRIPTOR_CODE_DATA);
pm(<<?DESCRIBED, ?CODE_ULONG, ?DESCRIPTOR_CODE_AMQP_SEQUENCE:64, _Rest/binary>>, true, B) ->
    reached_body(B, ?DESCRIPTOR_CODE_AMQP_SEQUENCE);
pm(<<?DESCRIBED, ?CODE_ULONG, ?DESCRIPTOR_CODE_AMQP_VALUE:64, _Rest/binary>>, true, B) ->
    reached_body(B, ?DESCRIPTOR_CODE_AMQP_VALUE);
pm(<<?DESCRIBED, ?CODE_SYM_8, 16:8, "amqp:data:binary", _Rest/binary>>, true, B) ->
    reached_body(B, ?DESCRIPTOR_CODE_DATA);
pm(<<?DESCRIBED, ?CODE_SYM_8, 23:8, "amqp:amqp-sequence:list", _Rest/binary>>, true, B) ->
    reached_body(B, ?DESCRIPTOR_CODE_AMQP_SEQUENCE);
pm(<<?DESCRIBED, ?CODE_SYM_8, 17:8, "amqp:amqp-value:*", _Rest/binary>>, true, B) ->
    reached_body(B, ?DESCRIPTOR_CODE_AMQP_VALUE);
pm(<<?DESCRIBED, ?CODE_SYM_32, 16:32, "amqp:data:binary", _Rest/binary>>, true, B) ->
    reached_body(B, ?DESCRIPTOR_CODE_DATA);
pm(<<?DESCRIBED, ?CODE_SYM_32, 23:32, "amqp:amqp-sequence:list", _Rest/binary>>, true, B) ->
    reached_body(B, ?DESCRIPTOR_CODE_AMQP_SEQUENCE);
pm(<<?DESCRIBED, ?CODE_SYM_32, 17:32, "amqp:amqp-value:*", _Rest/binary>>, true, B) ->
    reached_body(B, ?DESCRIPTOR_CODE_AMQP_VALUE);
pm(<<?DESCRIBED, ?CODE_ULONG, ?DESCRIPTOR_CODE_PROPERTIES:64, Rest0/binary>>, O = true, B) ->
    [Value | Rest] = pm(Rest0, O, B+10),
    [{{pos, B}, {described, {ulong, ?DESCRIPTOR_CODE_PROPERTIES}, Value}} | Rest];
pm(<<?DESCRIBED, ?CODE_ULONG, ?DESCRIPTOR_CODE_APPLICATION_PROPERTIES:64, Rest0/binary>>, O = true, B) ->
    [Value | Rest] = pm(Rest0, O, B+10),
    [{{pos, B}, {described, {ulong, ?DESCRIPTOR_CODE_APPLICATION_PROPERTIES}, Value}} | Rest];
pm(<<?DESCRIBED, ?CODE_SYM_8, 20, "amqp:properties:list", Rest0/binary>>, O = true, B) ->
    [Value | Rest] = pm(Rest0, O, B+23),
    [{{pos, B}, {described, {symbol, <<"amqp:properties:list">>}, Value}} | Rest];
pm(<<?DESCRIBED, ?CODE_SYM_8, 31, "amqp:application-properties:map", Rest0/binary>>, O = true, B) ->
    [Value | Rest] = pm(Rest0, O, B+34),
    [{{pos, B}, {described, {symbol, <<"amqp:application-properties:map">>}, Value}} | Rest];
pm(<<?DESCRIBED, ?CODE_SYM_32, 20:32, "amqp:properties:list", Rest0/binary>>, O = true, B) ->
    [Value | Rest] = pm(Rest0, O, B+26),
    [{{pos, B}, {described, {symbol, <<"amqp:properties:list">>}, Value}} | Rest];
pm(<<?DESCRIBED, ?CODE_SYM_32, 31:32, "amqp:application-properties:map", Rest0/binary>>, O = true, B) ->
    [Value | Rest] = pm(Rest0, O, B+37),
    [{{pos, B}, {described, {symbol, <<"amqp:application-properties:map">>}, Value}} | Rest];

%% Described Types
pm(<<?DESCRIBED, Rest0/binary>>, O, B) ->
    [Descriptor, Value | Rest] = pm(Rest0, O, B+1),
    [{described, Descriptor, Value} | Rest];

%% Primitives Types
%%
%% Fixed-widths.
pm(<<16#51, V:8/signed,    R/binary>>, O, B) -> [{byte, V} | pm(R, O, B+2)];
pm(<<16#54, V:8/signed,    R/binary>>, O, B) -> [{int, V} | pm(R, O, B+2)];
pm(<<16#55, V:8/signed,    R/binary>>, O, B) -> [{long, V} | pm(R, O, B+2)];
pm(<<16#56, 0:8/unsigned,  R/binary>>, O, B) -> [false | pm(R, O, B+2)];
pm(<<16#56, 1:8/unsigned,  R/binary>>, O, B) -> [true  | pm(R, O, B+2)];
pm(<<16#60, V:16/unsigned, R/binary>>, O, B) -> [{ushort, V} | pm(R, O, B+3)];
pm(<<16#61, V:16/signed,   R/binary>>, O, B) -> [{short, V} | pm(R, O, B+3)];
pm(<<16#71, V:32/signed,   R/binary>>, O, B) -> [{int, V} | pm(R, O, B+5)];
pm(<<16#72, V:32/float,    R/binary>>, O, B) -> [{float, V} | pm(R, O, B+5)];
pm(<<16#73, V:32,          R/binary>>, O, B) -> [{char, V} | pm(R, O, B+5)];
pm(<<16#81, V:64/signed,   R/binary>>, O, B) -> [{long, V} | pm(R, O, B+9)];
pm(<<16#82, V:64/float,    R/binary>>, O, B) -> [{double, V} | pm(R, O, B+9)];
pm(<<16#83, TS:64/signed,  R/binary>>, O, B) -> [{timestamp, TS} | pm(R, O, B+9)];
pm(<<16#98, Uuid:16/binary,R/binary>>, O, B) -> [{uuid, Uuid} | pm(R, O, B+17)];
%% Variable-widths
pm(<<?CODE_SYM_32, S:32,V:S/binary,R/binary>>, O, B) -> [{symbol, V} | pm(R, O, B+5+S)];
pm(<<16#b0, S:32,V:S/binary,R/binary>>, O, B)        -> [{binary, V} | pm(R, O, B+5+S)];
pm(<<16#b1, S:32,V:S/binary,R/binary>>, O, B)        -> [{utf8, V} | pm(R, O, B+5+S)];
%% Compounds
pm(<<16#d0, S:32,CountAndValue:S/binary,R/binary>>, O, B) ->
    [{list, pm_compound(32, CountAndValue, B+5)} | pm(R, O, B+5+S)];
pm(<<16#d1, S:32,CountAndValue:S/binary,R/binary>>, O, B) ->
    List = pm_compound(32, CountAndValue, B+5),
    [{map, mapify(List)} | pm(R, O, B+5+S)];
%% Arrays
pm(<<16#e0, S:8,CountAndV:S/binary,R/binary>>, O, B) ->
    [parse_array(8, CountAndV) | pm(R, O, B+2+S)];
pm(<<16#f0, S:32,CountAndV:S/binary,R/binary>>, O, B) ->
    [parse_array(32, CountAndV) | pm(R, O, B+5+S)];
%% NaN or +-inf
pm(<<16#72, V:4/binary, R/binary>>, O, B) ->
    [{as_is, 16#72, V} | pm(R, O, B+5)];
pm(<<16#82, V:8/binary, R/binary>>, O, B) ->
    [{as_is, 16#82, V} | pm(R, O, B+9)];
%% decimals
pm(<<16#74, V:4/binary, R/binary>>, O, B) ->
    [{as_is, 16#74, V} | pm(R, O, B+5)];
pm(<<16#84, V:8/binary, R/binary>>, O, B) ->
    [{as_is, 16#84, V} | pm(R, O, B+9)];
pm(<<16#94, V:16/binary, R/binary>>, O, B) ->
    [{as_is, 16#94, V} | pm(R, O, B+17)];
pm(<<Type, _Bin/binary>>, _O, B) ->
    throw({primitive_type_unsupported, Type, {position, B}}).

pm_compound(UnitSize, Bin, B) ->
    case Bin of
        <<_IgnoreCount:UnitSize, Value/binary>> ->
            pm(Value, false, B + UnitSize div 8);
        _ ->
            throw({invalid_compound_size, {position, B}})
    end.

reached_body(Position, DescriptorCode) ->
    [{{pos, Position}, {body, DescriptorCode}}].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% "Altogether a message consists of the following sections:
%%  * Zero or one header sections.
%%  * Zero or one delivery-annotation sections.
%%  * Zero or one message-annotation sections.
%%  * Zero or one properties sections.
%%  * Zero or one application-properties sections.
%%  * The body consists of one of the following three choices: one or more data
%%    sections, one or more amqp-sequence sections, or a single amqp-value
%%    section.
%%  * Zero or one footer sections." [§3.2]
%%
%% The descriptor codes of these sections are assigned in exactly the order in
%% which the sections must appear. Therefore, validating the order and the
%% cardinality of all sections preceding the body reduces to requiring strictly
%% increasing descriptor codes.
%%
%% This validation jumps from one section to the next without building any
%% Erlang term. erlang:binary_part/2,3 is avoided to avoid creating sub binaries.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

validate_msg_sections(Binary) ->
    validate(Binary, 0, ?DESCRIPTOR_CODE_HEADER, ?MAX_MSG_SECTIONS).

%% While the sections preceding the body are validated, State is the lowest
%% descriptor code that is still allowed. From the first body section onwards,
%% State is the atom 'data', 'sequence' or 'value', and 'eof' after the footer.
%% Left is the number of sections that may still follow.
validate(<<>>, _Pos, _State, _Left) ->
    %% A missing body section will be reported by the caller.
    ok;
validate(<<_, _/binary>>, Pos, _State, 0) ->
    throw({too_many_message_sections, ?MAX_MSG_SECTIONS, {position, Pos}});
validate(<<?DESCRIBED, ?CODE_SMALL_ULONG, Code:8, R/binary>>, Pos, State, Left) ->
    State1 = validate_state(Code, State, Pos),
    validate_value(R, Pos+3, State1, section_value_kind(Code), Left-1);
validate(<<?DESCRIBED, ?CODE_ULONG, Code:64, R/binary>>, Pos, State, Left) ->
    State1 = validate_state(Code, State, Pos),
    validate_value(R, Pos+10, State1, section_value_kind(Code), Left-1);
validate(<<?DESCRIBED, ?CODE_SYM_8, Size:8, Symbol:Size/binary, R/binary>>, Pos, State, Left) ->
    Code = descriptor_code(Symbol),
    State1 = validate_state(Code, State, Pos),
    validate_value(R, Pos+3+Size, State1, section_value_kind(Code), Left-1);
validate(<<?DESCRIBED, ?CODE_SYM_32, Size:32, Symbol:Size/binary, R/binary>>, Pos, State, Left) ->
    Code = descriptor_code(Symbol),
    State1 = validate_state(Code, State, Pos),
    validate_value(R, Pos+6+Size, State1, section_value_kind(Code), Left-1);
validate(_, Pos, _State, _Left) ->
    throw({not_a_message_section, {position, Pos}}).

%% Rejects an unknown section descriptor as well as a known section that
%% violates the section order or cardinality.
validate_state(?DESCRIPTOR_CODE_HEADER, State, _Pos)
  when State =< ?DESCRIPTOR_CODE_HEADER ->
    ?DESCRIPTOR_CODE_HEADER + 1;
validate_state(?DESCRIPTOR_CODE_DELIVERY_ANNOTATIONS, State, _Pos)
  when State =< ?DESCRIPTOR_CODE_DELIVERY_ANNOTATIONS ->
    ?DESCRIPTOR_CODE_DELIVERY_ANNOTATIONS + 1;
validate_state(?DESCRIPTOR_CODE_MESSAGE_ANNOTATIONS, State, _Pos)
  when State =< ?DESCRIPTOR_CODE_MESSAGE_ANNOTATIONS ->
    ?DESCRIPTOR_CODE_MESSAGE_ANNOTATIONS + 1;
validate_state(?DESCRIPTOR_CODE_PROPERTIES, State, _Pos)
  when State =< ?DESCRIPTOR_CODE_PROPERTIES ->
    ?DESCRIPTOR_CODE_PROPERTIES + 1;
validate_state(?DESCRIPTOR_CODE_APPLICATION_PROPERTIES, State, _Pos)
  when State =< ?DESCRIPTOR_CODE_APPLICATION_PROPERTIES ->
    ?DESCRIPTOR_CODE_APPLICATION_PROPERTIES + 1;
validate_state(?DESCRIPTOR_CODE_DATA, State, _Pos)
  when is_integer(State) orelse State =:= data ->
    data;
validate_state(?DESCRIPTOR_CODE_AMQP_SEQUENCE, State, _Pos)
  when is_integer(State) orelse State =:= sequence ->
    sequence;
validate_state(?DESCRIPTOR_CODE_AMQP_VALUE, State, _Pos)
  when is_integer(State) ->
    value;
validate_state(?DESCRIPTOR_CODE_FOOTER, State, _Pos)
  when State =:= data orelse
       State =:= sequence orelse
       State =:= value ->
    eof;
validate_state(Descriptor, _State, Pos) ->
    throw({unexpected_message_section, section_name(Descriptor), {position, Pos}}).

section_value_kind(?DESCRIPTOR_CODE_HEADER) -> list;
section_value_kind(?DESCRIPTOR_CODE_DELIVERY_ANNOTATIONS) -> map;
section_value_kind(?DESCRIPTOR_CODE_MESSAGE_ANNOTATIONS) -> map;
section_value_kind(?DESCRIPTOR_CODE_PROPERTIES) -> list;
section_value_kind(?DESCRIPTOR_CODE_APPLICATION_PROPERTIES) -> map;
section_value_kind(?DESCRIPTOR_CODE_DATA) -> binary;
section_value_kind(?DESCRIPTOR_CODE_AMQP_SEQUENCE) -> list;
section_value_kind(?DESCRIPTOR_CODE_AMQP_VALUE) -> any;
section_value_kind(?DESCRIPTOR_CODE_FOOTER) -> map.

%% Jumps over the section value: neither an Erlang term nor a sub binary is created.
validate_value(<<16#45, R/binary>>, Pos, State, Kind, Left)
  when Kind =:= list orelse Kind =:= any ->
    validate(R, Pos+1, State, Left);
validate_value(<<16#c0, Size:8, _:Size/binary, R/binary>>, Pos, State, Kind, Left)
  when Kind =:= list orelse Kind =:= any ->
    validate(R, Pos+2+Size, State, Left);
validate_value(<<16#d0, Size:32, _:Size/binary, R/binary>>, Pos, State, Kind, Left)
  when Kind =:= list orelse Kind =:= any ->
    validate(R, Pos+5+Size, State, Left);
validate_value(<<16#c1, Size:8, _:Size/binary, R/binary>>, Pos, State, Kind, Left)
  when Kind =:= map orelse Kind =:= any ->
    validate(R, Pos+2+Size, State, Left);
validate_value(<<16#d1, Size:32, _:Size/binary, R/binary>>, Pos, State, Kind, Left)
  when Kind =:= map orelse Kind =:= any ->
    validate(R, Pos+5+Size, State, Left);
validate_value(<<16#a0, Size:8, _:Size/binary, R/binary>>, Pos, State, Kind, Left)
  when Kind =:= binary orelse Kind =:= any ->
    validate(R, Pos+2+Size, State, Left);
validate_value(<<16#b0, Size:32, _:Size/binary, R/binary>>, Pos, State, Kind, Left)
  when Kind =:= binary orelse Kind =:= any ->
    validate(R, Pos+5+Size, State, Left);
%% Any other AMQP type is valid only as the value of an amqp-value section.
%% Its size is determined without building any Erlang term either.
validate_value(Bin, Pos, State, any, Left) ->
    validate_skip(Bin, Pos, State, Left, peek_value_size(Bin));
validate_value(_, Pos, _State, Kind, _Left) ->
    throw({invalid_section_value, Kind, {position, Pos}}).

validate_skip(Bin, Pos, State, Left, Size) ->
    case Bin of
        <<_:Size/binary, R/binary>> ->
            validate(R, Pos+Size, State, Left);
        _ ->
            throw({invalid_section_value, any, {position, Pos}})
    end.

descriptor_code(<<"amqp:header:list">>) -> ?DESCRIPTOR_CODE_HEADER;
descriptor_code(<<"amqp:delivery-annotations:map">>) -> ?DESCRIPTOR_CODE_DELIVERY_ANNOTATIONS;
descriptor_code(<<"amqp:message-annotations:map">>) -> ?DESCRIPTOR_CODE_MESSAGE_ANNOTATIONS;
descriptor_code(<<"amqp:properties:list">>) -> ?DESCRIPTOR_CODE_PROPERTIES;
descriptor_code(<<"amqp:application-properties:map">>) -> ?DESCRIPTOR_CODE_APPLICATION_PROPERTIES;
descriptor_code(<<"amqp:data:binary">>) -> ?DESCRIPTOR_CODE_DATA;
descriptor_code(<<"amqp:amqp-sequence:list">>) -> ?DESCRIPTOR_CODE_AMQP_SEQUENCE;
descriptor_code(<<"amqp:amqp-value:*">>) -> ?DESCRIPTOR_CODE_AMQP_VALUE;
descriptor_code(<<"amqp:footer:map">>) -> ?DESCRIPTOR_CODE_FOOTER;
descriptor_code(_UnknownSymbol) -> unknown_section_descriptor.

%% Only used to report an error.
section_name(?DESCRIPTOR_CODE_HEADER) -> header;
section_name(?DESCRIPTOR_CODE_DELIVERY_ANNOTATIONS) -> delivery_annotations;
section_name(?DESCRIPTOR_CODE_MESSAGE_ANNOTATIONS) -> message_annotations;
section_name(?DESCRIPTOR_CODE_PROPERTIES) -> properties;
section_name(?DESCRIPTOR_CODE_APPLICATION_PROPERTIES) -> application_properties;
section_name(?DESCRIPTOR_CODE_DATA) -> data;
section_name(?DESCRIPTOR_CODE_AMQP_SEQUENCE) -> amqp_sequence;
section_name(?DESCRIPTOR_CODE_AMQP_VALUE) -> amqp_value;
section_name(?DESCRIPTOR_CODE_FOOTER) -> footer;
section_name(UnknownDescriptor) -> UnknownDescriptor.

%% Returns the descriptor and total byte size (1 + B1 + B2) of the described type
%% at the start of the binary, without parsing the value.
-spec peek(binary()) ->
    {({ulong, non_neg_integer()} | {symbol, binary()}), TotalSize :: non_neg_integer()}.
peek(<<?DESCRIBED, Rest/binary>>) ->
    {Descriptor, B1} = parse(Rest),
    <<_:B1/binary, Rest1/binary>> = Rest,
    B2 = peek_value_size(Rest1),
    {Descriptor, 1 + B1 + B2};
peek(<<Type, _/binary>>) ->
    throw({not_described_type, Type}).

%% Returns the byte size of the AMQP value at the start of the binary
%% without parsing it (no term construction).
peek_value_size(<<?DESCRIBED, Rest/binary>>) ->
    %% "The descriptor portion of a described format code is itself any valid AMQP
    %% encoded value, including other described values." [§1.2]
    DescriptorSize = peek_value_size(Rest),
    case Rest of
        <<_:DescriptorSize/binary, Value/binary>> ->
            1 + DescriptorSize + peek_value_size(Value);
        _ ->
            throw({insufficient_input, described_type, peek_value_size})
    end;
peek_value_size(<<16#40, _/binary>>) -> 1;
peek_value_size(<<16#41, _/binary>>) -> 1;
peek_value_size(<<16#42, _/binary>>) -> 1;
peek_value_size(<<16#43, _/binary>>) -> 1;
peek_value_size(<<16#44, _/binary>>) -> 1;
peek_value_size(<<16#45, _/binary>>) -> 1;
peek_value_size(<<16#50, _/binary>>) -> 2;
peek_value_size(<<16#51, _/binary>>) -> 2;
peek_value_size(<<16#52, _/binary>>) -> 2;
peek_value_size(<<?CODE_SMALL_ULONG, _/binary>>) -> 2;
peek_value_size(<<16#54, _/binary>>) -> 2;
peek_value_size(<<16#55, _/binary>>) -> 2;
peek_value_size(<<16#56, _/binary>>) -> 2;
peek_value_size(<<16#60, _/binary>>) -> 3;
peek_value_size(<<16#61, _/binary>>) -> 3;
peek_value_size(<<16#70, _/binary>>) -> 5;
peek_value_size(<<16#71, _/binary>>) -> 5;
peek_value_size(<<16#72, _/binary>>) -> 5;
peek_value_size(<<16#73, _/binary>>) -> 5;
peek_value_size(<<16#74, _/binary>>) -> 5;
peek_value_size(<<?CODE_ULONG, _/binary>>) -> 9;
peek_value_size(<<16#81, _/binary>>) -> 9;
peek_value_size(<<16#82, _/binary>>) -> 9;
peek_value_size(<<16#83, _/binary>>) -> 9;
peek_value_size(<<16#84, _/binary>>) -> 9;
peek_value_size(<<16#94, _/binary>>) -> 17;
peek_value_size(<<16#98, _/binary>>) -> 17;
peek_value_size(<<16#a0, S:8, _/binary>>) -> 2 + S;
peek_value_size(<<16#a1, S:8, _/binary>>) -> 2 + S;
peek_value_size(<<?CODE_SYM_8, S:8, _/binary>>) -> 2 + S;
peek_value_size(<<?CODE_SYM_32, S:32, _/binary>>) -> 5 + S;
peek_value_size(<<16#b0, S:32, _/binary>>) -> 5 + S;
peek_value_size(<<16#b1, S:32, _/binary>>) -> 5 + S;
peek_value_size(<<16#c0, Size, _/binary>>) -> 2 + Size;
peek_value_size(<<16#c1, Size, _/binary>>) -> 2 + Size;
peek_value_size(<<16#d0, Size:32, _/binary>>) -> 5 + Size;
peek_value_size(<<16#d1, Size:32, _/binary>>) -> 5 + Size;
peek_value_size(<<16#e0, S:8, _/binary>>) -> 2 + S;
peek_value_size(<<16#f0, S:32, _/binary>>) -> 5 + S;
peek_value_size(<<Type, _/binary>>) ->
    throw({primitive_type_unsupported, Type, peek_value_size}).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

peek_value_size_fixed_test() ->
    %% 1-byte primitives (type code only)
    ?assertEqual(1, peek_value_size(<<16#40, 0>>)),
    ?assertEqual(1, peek_value_size(<<16#41, 0>>)),
    ?assertEqual(1, peek_value_size(<<16#45, 0>>)),
    %% 2-byte (type + 1 byte)
    ?assertEqual(2, peek_value_size(<<16#50, 42>>)),
    ?assertEqual(2, peek_value_size(<<16#53, 16#75>>)),
    %% 3-byte (type + 2 bytes)
    ?assertEqual(3, peek_value_size(<<16#60, 0, 1>>)),
    %% 5-byte (type + 4 bytes)
    ?assertEqual(5, peek_value_size(<<16#70, 0, 0, 0, 0>>)),
    %% 9-byte (type + 8 bytes)
    ?assertEqual(9, peek_value_size(<<16#80, 0:64>>)),
    %% 17-byte (uuid)
    ?assertEqual(17, peek_value_size(<<16#98, 0:128>>)).

peek_value_size_variable_test() ->
    %% Binary: 0xa0 + size (1 byte) + payload -> 2 + S
    ?assertEqual(5, peek_value_size(<<16#a0, 3, "foo">>)),
    %% UTF8: 0xa1 + size + payload
    ?assertEqual(6, peek_value_size(<<16#a1, 4, "test">>)),
    %% Symbol (CODE_SYM_8 = 0xa3)
    ?assertEqual(7, peek_value_size(<<16#a3, 5, "hello">>)),
    %% List: 0xc0 + size byte -> 2 + Size
    ?assertEqual(4, peek_value_size(<<16#c0, 2, 0, 16#40>>)).

-endif.
