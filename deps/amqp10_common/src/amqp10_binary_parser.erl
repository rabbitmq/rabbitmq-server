%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(amqp10_binary_parser).

%%TODO consolidate parse/1 and parse_all/1 into a single function
-export([parse/1, parse_all/1]).

-include("amqp10_framing.hrl").

-ifdef(TEST).

-export([parse_all_int/1]).

parse_all_int(ValueBin) when is_binary(ValueBin) ->
    lists:reverse(parse_all_int([], parse(ValueBin))).

parse_all_int(Acc, {Value, <<>>}) -> [Value | Acc];
parse_all_int(Acc, {Value, Rest}) -> parse_all_int([Value | Acc], parse(Rest)).

-endif.

-spec parse(binary()) ->
    {amqp10_binary_generator:amqp10_type(), Rest :: binary()}.
parse(<<?DESCRIBED, Bin/binary>>) ->
    {Descriptor, Rest0} = parse(Bin),
    {Value, Rest} = parse(Rest0),
    {{described, Descriptor, Value}, Rest};
parse(<<16#40, R/binary>>) -> {null, R};
parse(<<16#41, R/binary>>) -> {true, R};
parse(<<16#42, R/binary>>) -> {false, R};
parse(<<16#43, R/binary>>) -> {{uint, 0}, R};
parse(<<16#44, R/binary>>) -> {{ulong, 0}, R};
%% Fixed-widths. Most integral types have a compact encoding as a byte.
parse(<<16#50, V:8/unsigned,  R/binary>>) -> {{ubyte, V},      R};
parse(<<16#51, V:8/signed,    R/binary>>) -> {{byte, V},       R};
parse(<<16#52, V:8/unsigned,  R/binary>>) -> {{uint, V},       R};
parse(<<16#53, V:8/unsigned,  R/binary>>) -> {{ulong, V},      R};
parse(<<16#54, V:8/signed,    R/binary>>) -> {{int, V},        R};
parse(<<16#55, V:8/signed,    R/binary>>) -> {{long, V},       R};
parse(<<16#56, 0:8/unsigned,  R/binary>>) -> {false,           R};
parse(<<16#56, 1:8/unsigned,  R/binary>>) -> {true,            R};
parse(<<16#60, V:16/unsigned, R/binary>>) -> {{ushort, V},     R};
parse(<<16#61, V:16/signed,   R/binary>>) -> {{short, V},      R};
parse(<<16#70, V:32/unsigned, R/binary>>) -> {{uint, V},       R};
parse(<<16#71, V:32/signed,   R/binary>>) -> {{int, V},        R};
parse(<<16#72, V:32/float,    R/binary>>) -> {{float, V},      R};
parse(<<16#73, Utf32:4/binary,R/binary>>) -> {{char, Utf32},   R};
parse(<<16#80, V:64/unsigned, R/binary>>) -> {{ulong, V},      R};
parse(<<16#81, V:64/signed,   R/binary>>) -> {{long, V},       R};
parse(<<16#82, V:64/float,    R/binary>>) -> {{double, V},     R};
parse(<<16#83, TS:64/signed,  R/binary>>) -> {{timestamp, TS}, R};
parse(<<16#98, Uuid:16/binary,R/binary>>) -> {{uuid, Uuid},    R};
%% Variable-widths
parse(<<16#a0, S:8/unsigned, V:S/binary,R/binary>>)-> {{binary, V}, R};
parse(<<16#a1, S:8/unsigned, V:S/binary,R/binary>>)-> {{utf8, V},   R};
parse(<<16#a3, S:8/unsigned, V:S/binary,R/binary>>)-> {{symbol, V}, R};
parse(<<16#b3, S:32/unsigned,V:S/binary,R/binary>>)-> {{symbol, V}, R};
parse(<<16#b0, S:32/unsigned,V:S/binary,R/binary>>)-> {{binary, V}, R};
parse(<<16#b1, S:32/unsigned,V:S/binary,R/binary>>)-> {{utf8, V},   R};
%% Compounds
parse(<<16#45, R/binary>>) ->
    {{list, []}, R};
parse(<<16#c0, Size, Count, Value:(Size-1)/binary, R/binary>>) ->
    %%TODO avoid lists:foldl => use recursion instead
    {L, <<>>} = lists:foldl(fun(_, {AccL, AccBin}) ->
                                    {V, Rest} = parse(AccBin),
                                    {[V | AccL], Rest}
                            end, {[], Value}, lists:seq(1, Count)),
    {{list, lists:reverse(L)}, R};
parse(<<16#c1, Size, Count, Value:(Size-1)/binary, R/binary>>) ->
    {L, <<>>} = lists:foldl(fun(_, {AccL, AccBin}) ->
                                    {V, Rest} = parse(AccBin),
                                    {[V | AccL], Rest}
                            end, {[], Value}, lists:seq(1, Count)),
    {{map, mapify(lists:reverse(L))}, R};
parse(<<16#d0, Size:32, Count:32, Value:(Size-4)/binary, R/binary>>) ->
    {L, <<>>} = lists:foldl(fun(_, {AccL, AccBin}) ->
                                    {V, Rest} = parse(AccBin),
                                    {[V | AccL], Rest}
                            end, {[], Value}, lists:seq(1, Count)),
    {{list, lists:reverse(L)}, R};
parse(<<16#d1, Size:32, Count:32, Value:(Size-4)/binary,R/binary>>) ->
    {L, <<>>} = lists:foldl(fun(_, {AccL, AccBin}) ->
                                    {V, Rest} = parse(AccBin),
                                    {[V | AccL], Rest}
                            end, {[], Value}, lists:seq(1, Count)),
    {{map, mapify(lists:reverse(L))}, R};
%% Arrays
parse(<<16#e0, S:8/unsigned,CountAndV:S/binary,R/binary>>) ->
    {parse_array(8, CountAndV), R};
parse(<<16#f0, S:32/unsigned,CountAndV:S/binary,R/binary>>) ->
    {parse_array(32, CountAndV), R};
%% NaN or +-inf
parse(<<16#72, V:32, R/binary>>) ->
    {{as_is, 16#72, <<V:32>>}, R};
parse(<<16#82, V:64, R/binary>>) ->
    {{as_is, 16#82, <<V:64>>}, R};
%% decimals
parse(<<16#74, V:32, R/binary>>) ->
    {{as_is, 16#74, <<V:32>>}, R};
parse(<<16#84, V:64, R/binary>>) ->
    {{as_is, 16#84, <<V:64>>}, R};
parse(<<16#94, V:128, R/binary>>) ->
    {{as_is, 16#94, <<V:128>>}, R};
parse(<<Type, Bin/binary>>) ->
    throw({primitive_type_unsupported, Type, Bin}).

parse_array_primitive(16#40, <<_:8/unsigned, R/binary>>) -> {null, R};
parse_array_primitive(16#41, <<_:8/unsigned, R/binary>>) -> {true, R};
parse_array_primitive(16#42, <<_:8/unsigned, R/binary>>) -> {false, R};
parse_array_primitive(16#43, <<_:8/unsigned, R/binary>>) -> {{uint, 0}, R};
parse_array_primitive(16#44, <<_:8/unsigned, R/binary>>) -> {{ulong, 0}, R};
parse_array_primitive(ElementType, Data) ->
    parse(<<ElementType, Data/binary>>).

%% array structure is {array, Ctor, [Data]}
%% e.g. {array, symbol, [<<"amqp:accepted:list">>]}
parse_array(UnitSize, Bin) ->
    <<Count:UnitSize, Bin1/binary>> = Bin,
    parse_array1(Count, Bin1).

parse_array1(Count, <<?DESCRIBED, Rest/binary>>) ->
    {Descriptor, Rest1} = parse(Rest),
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
    {Value, Rest} = parse_array_primitive(Type, Bin),
    parse_array2(Count - 1, Type, Rest, [Value | Acc]).

parse_constructor(16#a3) -> symbol;
parse_constructor(16#b3) -> symbol;
parse_constructor(16#a1) -> utf8;
parse_constructor(16#b1) -> utf8;
parse_constructor(16#50) -> ubyte;
parse_constructor(16#51) -> byte;
parse_constructor(16#60) -> ushort;
parse_constructor(16#61) -> short;
parse_constructor(16#70) -> uint;
parse_constructor(16#71) -> int;
parse_constructor(16#80) -> ulong;
parse_constructor(16#81) -> long;
parse_constructor(16#40) -> null;
parse_constructor(16#56) -> boolean;
parse_constructor(16#f0) -> array;
parse_constructor(0) -> described;
parse_constructor(X) ->
    exit({failed_to_parse_constructor, X}).

mapify([]) ->
    [];
mapify([Key, Value | Rest]) ->
    [{Key, Value} | mapify(Rest)].

%% parse_all/1 is much faster and much more memory efficient than parse/1.
%%
%% When compiling this module with environment variable ERL_COMPILER_OPTIONS=bin_opt_info,
%% for parse/1 the compiler prints many times:
%% "BINARY CREATED: binary is used in a term that is returned from the function"
%% because sub binaries are created.
%%
%% For parse_all/1 the compiler prints many times:
%% "OPTIMIZED: match context reused"
%% because sub binaries are not created.
%%
%% See also https://www.erlang.org/doc/efficiency_guide/binaryhandling.html
-spec parse_all(binary()) ->
    [amqp10_binary_generator:amqp10_type()].

parse_all(<<>>) ->
    [];

%% Described Types
parse_all(<<?DESCRIBED, Rest0/binary>>) ->
    [Descriptor, Value | Rest] = parse_all(Rest0),
    [{described, Descriptor, Value} | Rest];

%% Primitives Types
%%
%% Constants
parse_all(<<16#40, R/binary>>) -> [null | parse_all(R)];
parse_all(<<16#41, R/binary>>) -> [true | parse_all(R)];
parse_all(<<16#42, R/binary>>) -> [false | parse_all(R)];
parse_all(<<16#43, R/binary>>) -> [{uint, 0} | parse_all(R)];
parse_all(<<16#44, R/binary>>) -> [{ulong, 0} | parse_all(R)];

%% Fixed-widths. Most integral types have a compact encoding as a byte.
parse_all(<<16#50, V:8/unsigned,  R/binary>>) -> [{ubyte, V} | parse_all(R)];
parse_all(<<16#51, V:8/signed,    R/binary>>) -> [{byte, V} | parse_all(R)];
parse_all(<<16#52, V:8/unsigned,  R/binary>>) -> [{uint, V} | parse_all(R)];
parse_all(<<16#53, V:8/unsigned,  R/binary>>) -> [{ulong, V} | parse_all(R)];
parse_all(<<16#54, V:8/signed,    R/binary>>) -> [{int, V} | parse_all(R)];
parse_all(<<16#55, V:8/signed,    R/binary>>) -> [{long, V} | parse_all(R)];
parse_all(<<16#56, 0:8/unsigned,  R/binary>>) -> [false | parse_all(R)];
parse_all(<<16#56, 1:8/unsigned,  R/binary>>) -> [true  | parse_all(R)];
parse_all(<<16#60, V:16/unsigned, R/binary>>) -> [{ushort, V} | parse_all(R)];
parse_all(<<16#61, V:16/signed,   R/binary>>) -> [{short, V} | parse_all(R)];
parse_all(<<16#70, V:32/unsigned, R/binary>>) -> [{uint, V} | parse_all(R)];
parse_all(<<16#71, V:32/signed,   R/binary>>) -> [{int, V} | parse_all(R)];
parse_all(<<16#72, V:32/float,    R/binary>>) -> [{float, V} | parse_all(R)];
parse_all(<<16#73, Utf32:4/binary,R/binary>>) -> [{char, Utf32} | parse_all(R)];
parse_all(<<16#80, V:64/unsigned, R/binary>>) -> [{ulong, V} | parse_all(R)];
parse_all(<<16#81, V:64/signed,   R/binary>>) -> [{long, V} | parse_all(R)];
parse_all(<<16#82, V:64/float,    R/binary>>) -> [{double, V} | parse_all(R)];
parse_all(<<16#83, TS:64/signed,  R/binary>>) -> [{timestamp, TS} | parse_all(R)];
parse_all(<<16#98, Uuid:16/binary,R/binary>>) -> [{uuid, Uuid} | parse_all(R)];

%% Variable-widths
parse_all(<<16#a0, S:8/unsigned, V:S/binary,R/binary>>) -> [{binary, V} | parse_all(R)];
parse_all(<<16#a1, S:8/unsigned, V:S/binary,R/binary>>) -> [{utf8, V} | parse_all(R)];
parse_all(<<16#a3, S:8/unsigned, V:S/binary,R/binary>>) -> [{symbol, V} | parse_all(R)];
parse_all(<<16#b3, S:32/unsigned,V:S/binary,R/binary>>) -> [{symbol, V} | parse_all(R)];
parse_all(<<16#b0, S:32/unsigned,V:S/binary,R/binary>>) -> [{binary, V} | parse_all(R)];
parse_all(<<16#b1, S:32/unsigned,V:S/binary,R/binary>>) -> [{utf8, V} | parse_all(R)];

%% Compounds
parse_all(<<16#45, R/binary>>) ->
    [{list, []} | parse_all(R)];
parse_all(<<16#c0, S:8/unsigned,CountAndValue:S/binary,R/binary>>) ->
    [{list, parse_compound_all(8, CountAndValue)} | parse_all(R)];
parse_all(<<16#c1, S:8/unsigned,CountAndValue:S/binary,R/binary>>) ->
    List = parse_compound_all(8, CountAndValue),
    [{map, mapify(List)} | parse_all(R)];
parse_all(<<16#d0, S:32/unsigned,CountAndValue:S/binary,R/binary>>) ->
    [{list, parse_compound_all(32, CountAndValue)} | parse_all(R)];
parse_all(<<16#d1, S:32/unsigned,CountAndValue:S/binary,R/binary>>) ->
    List = parse_compound_all(32, CountAndValue),
    [{map, mapify(List)} | parse_all(R)];

%% Arrays
parse_all(<<16#e0, S:8/unsigned,CountAndV:S/binary,R/binary>>) ->
    [parse_array(8, CountAndV) | parse_all(R)];
parse_all(<<16#f0, S:32/unsigned,CountAndV:S/binary,R/binary>>) ->
    [parse_array(32, CountAndV) | parse_all(R)];

%% NaN or +-inf
parse_all(<<16#72, V:32, R/binary>>) ->
    [{as_is, 16#72, <<V:32>>} | parse_all(R)];
parse_all(<<16#82, V:64, R/binary>>) ->
    [{as_is, 16#82, <<V:64>>} | parse_all(R)];

%% decimals
parse_all(<<16#74, V:32, R/binary>>) ->
    [{as_is, 16#74, <<V:32>>} | parse_all(R)];
parse_all(<<16#84, V:64, R/binary>>) ->
    [{as_is, 16#84, <<V:64>>} | parse_all(R)];
parse_all(<<16#94, V:128, R/binary>>) ->
    [{as_is, 16#94, <<V:128>>} | parse_all(R)];

parse_all(<<Type, _Bin/binary>>) ->
    throw({primitive_type_unsupported, Type, _Bin}).

parse_compound_all(UnitSize, Bin) ->
    <<_Count:UnitSize, Bin1/binary>> = Bin,
    parse_all(Bin1).
