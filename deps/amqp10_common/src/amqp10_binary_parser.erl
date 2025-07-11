%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
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
         parse_many/2]).

%% §1.6
-define(CODE_ULONG, 16#80).
-define(CODE_SMALL_ULONG, 16#53).
-define(CODE_SYM_8, 16#a3).
-define(CODE_SYM_32, 16#b3).
%% §3.2
-define(DESCRIPTOR_CODE_PROPERTIES, 16#73).
-define(DESCRIPTOR_CODE_APPLICATION_PROPERTIES, 16#74).
-define(DESCRIPTOR_CODE_DATA, 16#75).
-define(DESCRIPTOR_CODE_AMQP_SEQUENCE, 16#76).
-define(DESCRIPTOR_CODE_AMQP_VALUE, 16#77).


%% server_mode is a special parsing mode used by RabbitMQ when parsing
%% AMQP message sections from an AMQP client. This mode:
%% 1. stops parsing when the body starts, and
%% 2. returns the start byte position of each parsed bare message section.
-type opts() :: [server_mode].

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

%% array structure is {array, Ctor, [Data]}
%% e.g. {array, symbol, [<<"amqp:accepted:list">>]}
parse_array(UnitSize, Bin) ->
    <<Count:UnitSize, Bin1/binary>> = Bin,
    parse_array1(Count, Bin1).

parse_array1(Count, <<?DESCRIBED, Rest/binary>>) ->
    {Descriptor, B1} = parse(Rest),
    <<_ParsedDescriptorBin:B1/binary, Rest1/binary>> = Rest,
    {array, Type, List} = parse_array1(Count, Rest1),
    Values = lists:map(fun (Value) ->
                               {described, Descriptor, Value}
                       end, List),
    % this format cannot represent an empty array of described types
    {array, {described, Descriptor, Type}, Values};
parse_array1(Count, <<Type, ArrayBin/binary>>) ->
    parse_array2(Count, Type, ArrayBin, []).

parse_array2(0, Type, <<>>, Acc) ->
    {array, parse_constructor(Type), lists:reverse(Acc)};
parse_array2(0, Type, Bin, Acc) ->
    exit({failed_to_parse_array_extra_input_remaining, Type, Bin, Acc});
parse_array2(Count, Type, <<>>, Acc) when Count > 0 ->
    exit({failed_to_parse_array_insufficient_input, Type, Count, Acc});
parse_array2(Count, Type, Bin, Acc) ->
    {Value, B} = parse_array_primitive(Type, Bin),
    <<_ParsedValue:B/binary, Rest/binary>> = Bin,
    parse_array2(Count - 1, Type, Rest, [Value | Acc]).

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
parse_constructor(16#56) -> boolean;
parse_constructor(16#83) -> timestamp;
parse_constructor(16#98) -> uuid;
parse_constructor(16#d0) -> list;
parse_constructor(16#d1) -> map;
parse_constructor(16#f0) -> array;
parse_constructor(0) -> described;
parse_constructor(X) ->
    exit({failed_to_parse_constructor, X}).

parse_array_primitive(16#40, <<_:8/unsigned, _/binary>>) -> {null, 1};
parse_array_primitive(16#41, <<_:8/unsigned, _/binary>>) -> {true, 1};
parse_array_primitive(16#42, <<_:8/unsigned, _/binary>>) -> {false, 1};
parse_array_primitive(16#43, <<_:8/unsigned, _/binary>>) -> {{uint, 0}, 1};
parse_array_primitive(16#44, <<_:8/unsigned, _/binary>>) -> {{ulong, 0}, 1};
parse_array_primitive(ElementType, Data) ->
    {Val, B} = parse(<<ElementType, Data/binary>>),
    {Val, B-1}.

mapify([]) ->
    [];
mapify([Key, Value | Rest]) ->
    [{Key, Value} | mapify(Rest)].

%% Parses all AMQP types (or, in server_mode, stops when the body is reached).
%% This is an optimisation over calling parse/1 repeatedly.
%% We re-use the match context avoiding creation of sub binaries.
-spec parse_many(binary(), opts()) ->
    [amqp10_binary_generator:amqp10_type() |
     {{pos, non_neg_integer()}, amqp10_binary_generator:amqp10_type() | body}].
parse_many(Binary, Opts) ->
    OptionServerMode = lists:member(server_mode, Opts),
    pm(Binary, OptionServerMode, 0).

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
    [{list, pm_compound(8, CountAndValue, O, B+2)} | pm(R, O, B+2+S)];
pm(<<16#c1, S:8,CountAndValue:S/binary,R/binary>>, O, B) ->
    List = pm_compound(8, CountAndValue, O, B+2),
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
pm(<<?DESCRIBED, ?CODE_SYM_8, _S:8, "amqp:data:binary", _Rest/binary>>, true, B) ->
    reached_body(B, ?DESCRIPTOR_CODE_DATA);
pm(<<?DESCRIBED, ?CODE_SYM_8, _S:8, "amqp:amqp-sequence:list", _Rest/binary>>, true, B) ->
    reached_body(B, ?DESCRIPTOR_CODE_AMQP_SEQUENCE);
pm(<<?DESCRIBED, ?CODE_SYM_8, _S:8, "amqp:amqp-value:*", _Rest/binary>>, true, B) ->
    reached_body(B, ?DESCRIPTOR_CODE_AMQP_VALUE);
pm(<<?DESCRIBED, ?CODE_SYM_32, _S:32, "amqp:data:binary", _Rest/binary>>, true, B) ->
    reached_body(B, ?DESCRIPTOR_CODE_DATA);
pm(<<?DESCRIBED, ?CODE_SYM_32, _S:32, "amqp:amqp-sequence:list", _Rest/binary>>, true, B) ->
    reached_body(B, ?DESCRIPTOR_CODE_AMQP_SEQUENCE);
pm(<<?DESCRIBED, ?CODE_SYM_32, _S:32, "amqp:amqp-value:*", _Rest/binary>>, true, B) ->
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
    [{list, pm_compound(32, CountAndValue, O, B+5)} | pm(R, O, B+5+S)];
pm(<<16#d1, S:32,CountAndValue:S/binary,R/binary>>, O, B) ->
    List = pm_compound(32, CountAndValue, O, B+5),
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

pm_compound(UnitSize, Bin, O, B) ->
    <<_IgnoreCount:UnitSize, Value/binary>> = Bin,
    pm(Value, O, B + UnitSize div 8).

reached_body(Position, DescriptorCode) ->
    [{{pos, Position}, {body, DescriptorCode}}].
