%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(amqp10_binary_parser).

-export([parse/1, parse_all/1]).

-include("amqp10_framing.hrl").

% -spec parse(binary()) -> tuple().

parse_all(ValueBin) when is_binary(ValueBin) ->
    lists:reverse(parse_all([], parse(ValueBin))).

parse_all(Acc, {Value, <<>>}) -> [Value | Acc];
parse_all(Acc, {Value, Rest}) -> parse_all([Value | Acc], parse(Rest)).

-spec parse(binary()) ->
    {amqp10_binary_generator:amqp10_type(), binary()}.
parse(<<?DESCRIBED,Rest/binary>>) ->
    parse_described(Rest);
parse(Rest) ->
    parse_primitive0(Rest).

parse_described(Bin) ->
    {Descriptor, Rest1} = parse(Bin),
    {Value, Rest2} = parse(Rest1),
    {{described, Descriptor, Value}, Rest2}.

parse_primitive0(<<Type, Rest/binary>>) ->
    parse_primitive(Type, Rest).

%% Constants
parse_primitive(16#40, R) -> {null, R};
parse_primitive(16#41, R) -> {true, R};
parse_primitive(16#42, R) -> {false, R};
parse_primitive(16#43, R) -> {{uint, 0}, R};
parse_primitive(16#44, R) -> {{ulong, 0}, R};

%% Fixed-widths. Most integral types have a compact encoding as a byte.
parse_primitive(16#50, <<V:8/unsigned,  R/binary>>) -> {{ubyte, V},      R};
parse_primitive(16#51, <<V:8/signed,    R/binary>>) -> {{byte, V},       R};
parse_primitive(16#52, <<V:8/unsigned,  R/binary>>) -> {{uint, V},       R};
parse_primitive(16#53, <<V:8/unsigned,  R/binary>>) -> {{ulong, V},      R};
parse_primitive(16#54, <<V:8/signed,    R/binary>>) -> {{int, V},        R};
parse_primitive(16#55, <<V:8/signed,    R/binary>>) -> {{long, V},       R};
parse_primitive(16#56, <<0:8/unsigned,  R/binary>>) -> {{boolean, false},R};
parse_primitive(16#56, <<1:8/unsigned,  R/binary>>) -> {{boolean, true}, R};
parse_primitive(16#60, <<V:16/unsigned, R/binary>>) -> {{ushort, V},     R};
parse_primitive(16#61, <<V:16/signed,   R/binary>>) -> {{short, V},      R};
parse_primitive(16#70, <<V:32/unsigned, R/binary>>) -> {{uint, V},       R};
parse_primitive(16#71, <<V:32/signed,   R/binary>>) -> {{int, V},        R};
parse_primitive(16#72, <<V:32/float,    R/binary>>) -> {{float, V},      R};
parse_primitive(16#73, <<Utf32:4/binary,R/binary>>) -> {{char, Utf32},   R};
parse_primitive(16#80, <<V:64/unsigned, R/binary>>) -> {{ulong, V},      R};
parse_primitive(16#81, <<V:64/signed,   R/binary>>) -> {{long, V},       R};
parse_primitive(16#82, <<V:64/float,    R/binary>>) -> {{double, V},     R};
parse_primitive(16#83, <<TS:64/signed,  R/binary>>) -> {{timestamp, TS}, R};
parse_primitive(16#98, <<Uuid:16/binary,R/binary>>) -> {{uuid, Uuid},    R};

%% Variable-widths
parse_primitive(16#a0,<<S:8/unsigned, V:S/binary,R/binary>>)-> {{binary, V}, R};
parse_primitive(16#a1,<<S:8/unsigned, V:S/binary,R/binary>>)-> {{utf8, V},   R};
parse_primitive(16#a3,<<S:8/unsigned, V:S/binary,R/binary>>)-> {{symbol, V}, R};
parse_primitive(16#b3,<<S:32/unsigned,V:S/binary,R/binary>>)-> {{symbol, V}, R};
parse_primitive(16#b0,<<S:32/unsigned,V:S/binary,R/binary>>)-> {{binary, V}, R};
parse_primitive(16#b1,<<S:32/unsigned,V:S/binary,R/binary>>)-> {{utf8, V},   R};

%% Compounds
parse_primitive(16#45, R) ->
    {{list, []}, R};
parse_primitive(16#c0,<<S:8/unsigned,CountAndValue:S/binary,R/binary>>) ->
    {{list, parse_compound(8, CountAndValue)}, R};
parse_primitive(16#c1,<<S:8/unsigned,CountAndValue:S/binary,R/binary>>) ->
    List = parse_compound(8, CountAndValue),
    {{map, mapify(List)}, R};
parse_primitive(16#d0,<<S:32/unsigned,CountAndValue:S/binary,R/binary>>) ->
    {{list, parse_compound(32, CountAndValue)}, R};
parse_primitive(16#d1,<<S:32/unsigned,CountAndValue:S/binary,R/binary>>) ->
    List = parse_compound(32, CountAndValue),
    {{map, mapify(List)}, R};

%% Arrays
parse_primitive(16#e0,<<S:8/unsigned,CountAndV:S/binary,R/binary>>) ->
    {parse_array(8, CountAndV), R};
parse_primitive(16#f0,<<S:32/unsigned,CountAndV:S/binary,R/binary>>) ->
    {parse_array(32, CountAndV), R};

%% NaN or +-inf
parse_primitive(16#72, <<V:32, R/binary>>) ->
    {{as_is, 16#72, <<V:32>>}, R};
parse_primitive(16#82, <<V:64, R/binary>>) ->
    {{as_is, 16#82, <<V:64>>}, R};

%% decimals
parse_primitive(16#74, <<V:32, R/binary>>) ->
    {{as_is, 16#74, <<V:32>>}, R};
parse_primitive(16#84, <<V:64, R/binary>>) ->
    {{as_is, 16#84, <<V:64>>}, R};
parse_primitive(16#94, <<V:128, R/binary>>) ->
    {{as_is, 16#94, <<V:128>>}, R};

parse_primitive(Type, _Bin) ->
    throw({primitive_type_unsupported, Type, _Bin}).

parse_compound(UnitSize, Bin) ->
    <<Count:UnitSize, Bin1/binary>> = Bin,
    parse_compound1(Count, Bin1, []).

parse_compound1(0, <<>>, List) ->
    lists:reverse(List);
parse_compound1(_Left, <<>>, List) ->
    case application:get_env(rabbitmq_amqp1_0, protocol_strict_mode) of
        {ok, false} -> lists:reverse(List); %% ignore miscount
        {ok, true}  -> throw(compound_datatype_miscount)
    end;
parse_compound1(Count, Bin, Acc) ->
    {Value, Rest} = parse(Bin),
    parse_compound1(Count - 1, Rest, [Value | Acc]).

parse_array_primitive(16#40, <<_:8/unsigned, R/binary>>) -> {null, R};
parse_array_primitive(16#41, <<_:8/unsigned, R/binary>>) -> {true, R};
parse_array_primitive(16#42, <<_:8/unsigned, R/binary>>) -> {false, R};
parse_array_primitive(16#43, <<_:8/unsigned, R/binary>>) -> {{uint, 0}, R};
parse_array_primitive(16#44, <<_:8/unsigned, R/binary>>) -> {{ulong, 0}, R};
parse_array_primitive(ElementType, Data) ->
    parse_primitive(ElementType, Data).

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
