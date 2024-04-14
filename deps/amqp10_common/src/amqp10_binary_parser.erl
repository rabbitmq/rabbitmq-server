%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(amqp10_binary_parser).

-include("amqp10_framing.hrl").

-export([parse/1,
         parse_many/2]).

-ifdef(TEST).
-export([parse_many_slow/1]).
-endif.

%% TODO put often matched binary function clauses to the top?

%% server_mode is a special parsing mode used by RabbitMQ when parsing
%% AMQP message sections from an AMQP client. This mode:
%% 1. stops parsing when the body is reached, and
%% 2. returns the start byte position of each parsed bare message section.
-type opts() :: [server_mode].

-export_type([opts/0]).

%% AMQP 3.2.4 & 3.2.5
-define(NUMERIC_DESCRIPTOR_IS_PROPERTIES_OR_APPLICATION_PROPERTIES(Code),
        Code =:= 16#73 orelse
        Code =:= 16#74).
-define(SYMBOLIC_DESCRIPTOR_IS_PROPERTIES_OR_APPLICATION_PROPERTIES(Name),
        Name =:= <<"amqp:properties:list">> orelse
        Name =:= <<"amqp:application-properties:map">>).
%% AMQP 3.2.6 - 3.2.8
-define(NUMERIC_DESCRIPTOR_IS_BODY(Code),
        Code >= 16#75 andalso
        Code =< 16#77).
-define(SYMBOLIC_DESCRIPTOR_IS_BODY(Name),
        Name =:= <<"amqp:data:binary">> orelse
        Name =:= <<"amqp:amqp-sequence:list">> orelse
        Name =:= <<"amqp:amqp-value:*">>).

%% Parses only the 1st AMQP type (including possible nested AMQP types).
-spec parse(binary()) ->
    {amqp10_binary_generator:amqp10_type(), Rest :: binary()}.
parse(<<?DESCRIBED, Bin/binary>>) ->
    {Descriptor, Rest0} = parse(Bin),
    {Value, Rest} = parse(Rest0),
    {{described, Descriptor, Value}, Rest};
parse(<<16#40, R/binary>>) -> {null,        R};
parse(<<16#41, R/binary>>) -> {true,        R};
parse(<<16#42, R/binary>>) -> {false,       R};
parse(<<16#43, R/binary>>) -> {{uint, 0},   R};
parse(<<16#44, R/binary>>) -> {{ulong, 0},  R};
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
parse(<<16#a0, S:8, V:S/binary,R/binary>>)-> {{binary, V}, R};
parse(<<16#a1, S:8, V:S/binary,R/binary>>)-> {{utf8, V},   R};
parse(<<16#a3, S:8, V:S/binary,R/binary>>)-> {{symbol, V}, R};
parse(<<16#b3, S:32,V:S/binary,R/binary>>)-> {{symbol, V}, R};
parse(<<16#b0, S:32,V:S/binary,R/binary>>)-> {{binary, V}, R};
parse(<<16#b1, S:32,V:S/binary,R/binary>>)-> {{utf8, V},   R};
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
parse(<<16#e0, S:8,CountAndV:S/binary,R/binary>>) ->
    {parse_array(8, CountAndV), R};
parse(<<16#f0, S:32,CountAndV:S/binary,R/binary>>) ->
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
parse(<<Type, _R/binary>>) ->
    throw({primitive_type_unsupported, Type}).

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

-ifdef(TEST).
%% This is the old, slow, original parser implemenation before
%% https://github.com/rabbitmq/rabbitmq-server/pull/4811
%% where many sub binaries are being created.
parse_many_slow(ValueBin)
  when is_binary(ValueBin) ->
    Res = parse_many_slow(parse(ValueBin), []),
    lists:reverse(Res).

parse_many_slow({Value, <<>>}, Acc) ->
    [Value | Acc];
parse_many_slow({Value, Rest}, Acc) ->
    parse_many_slow(parse(Rest), [Value | Acc]).
-endif.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% NB: When compiling this file with "ERL_COMPILER_OPTIONS=bin_opt_info"
%% make sure that all code below here outputs only "OPTIMIZED: match context reused"!
%% Neither "BINARY CREATED" nor "NOT OPTIMIZED" must be output!
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Parses all AMQP types (or, in server_mode, stops when the body is reached).
%% This is an optimisation over calling parse/1 repeatedly which is done by parse_many_slow/1
%% We re-use the match context avoiding creation of sub binaries.
-spec parse_many(binary(), opts()) ->
    [amqp10_binary_generator:amqp10_type() |
     {{pos, non_neg_integer()}, amqp10_binary_generator:amqp10_type() | body}].
parse_many(Binary, Opts) ->
    OptionServerMode = lists:member(server_mode, Opts),
    pm(Binary, OptionServerMode, 0).

pm(<<>>, _, _) ->
    [];

%%TODO is it faster to remove all when guards and instead match dirctly in the function head?

%% In server mode, stop when we reach the message body (data or amqp-sequence or amqp-value section).
pm(<<?DESCRIBED, 16#53, V:8, _Rest/binary>>, true, B)
  when ?NUMERIC_DESCRIPTOR_IS_BODY(V) ->
    reached_body(B);
pm(<<?DESCRIBED, 16#80, V:64, _Rest/binary>>, true, B)
  when ?NUMERIC_DESCRIPTOR_IS_BODY(V) ->
    reached_body(B);
pm(<<?DESCRIBED, 16#a3, S:8, V:S/binary, _Rest/binary>>, true, B)
  when ?SYMBOLIC_DESCRIPTOR_IS_BODY(V) ->
    reached_body(B);
pm(<<?DESCRIBED, 16#b3, S:32, V:S/binary, _Rest/binary>>, true, B)
  when ?SYMBOLIC_DESCRIPTOR_IS_BODY(V) ->
    reached_body(B);

%% In server mode, include number of bytes left for properties and application-properties sections.
pm(<<?DESCRIBED, 16#53, V:8, Rest0/binary>>, O = true, B)
  when ?NUMERIC_DESCRIPTOR_IS_PROPERTIES_OR_APPLICATION_PROPERTIES(V) ->
    Descriptor = {ulong, V},
    [Value | Rest] = pm(Rest0, O, B+3),
    [{{pos, B}, {described, Descriptor, Value}} | Rest];
pm(<<?DESCRIBED, 16#80, V:64, Rest0/binary>>, O = true, B)
  when ?NUMERIC_DESCRIPTOR_IS_PROPERTIES_OR_APPLICATION_PROPERTIES(V) ->
    Descriptor = {ulong, V},
    [Value | Rest] = pm(Rest0, O, B+10),
    [{{pos, B}, {described, Descriptor, Value}} | Rest];
pm(<<?DESCRIBED, 16#a3, S:8, V:S/binary, Rest0/binary>>, O = true, B)
  when ?SYMBOLIC_DESCRIPTOR_IS_PROPERTIES_OR_APPLICATION_PROPERTIES(V) ->
    Descriptor = {symbol, V},
    [Value | Rest] = pm(Rest0, O, B+3+S),
    [{{pos, B}, {described, Descriptor, Value}} | Rest];
pm(<<?DESCRIBED, 16#b3, S:32, V:S/binary, Rest0/binary>>, O = true, B)
  when ?SYMBOLIC_DESCRIPTOR_IS_PROPERTIES_OR_APPLICATION_PROPERTIES(V) ->
    Descriptor = {symbol, V},
    [Value | Rest] = pm(Rest0, O, B+6+S),
    [{{pos, B}, {described, Descriptor, Value}} | Rest];

%% Described Types
pm(<<?DESCRIBED, Rest0/binary>>, O, B) ->
    [Descriptor, Value | Rest] = pm(Rest0, O, B+1),
    [{described, Descriptor, Value} | Rest];
%% Primitives Types
%%
%% Constants
pm(<<16#40, R/binary>>, O, B) -> [null | pm(R, O, B+1)];
pm(<<16#41, R/binary>>, O, B) -> [true | pm(R, O, B+1)];
pm(<<16#42, R/binary>>, O, B) -> [false | pm(R, O, B+1)];
pm(<<16#43, R/binary>>, O, B) -> [{uint, 0} | pm(R, O, B+1)];
pm(<<16#44, R/binary>>, O, B) -> [{ulong, 0} | pm(R, O, B+1)];
%% Fixed-widths. Most integral types have a compact encoding as a byte.
pm(<<16#50, V:8/unsigned,  R/binary>>, O, B) -> [{ubyte, V} | pm(R, O, B+2)];
pm(<<16#51, V:8/signed,    R/binary>>, O, B) -> [{byte, V} | pm(R, O, B+2)];
pm(<<16#52, V:8/unsigned,  R/binary>>, O, B) -> [{uint, V} | pm(R, O, B+2)];
pm(<<16#53, V:8/unsigned,  R/binary>>, O, B) -> [{ulong, V} | pm(R, O, B+2)];
pm(<<16#54, V:8/signed,    R/binary>>, O, B) -> [{int, V} | pm(R, O, B+2)];
pm(<<16#55, V:8/signed,    R/binary>>, O, B) -> [{long, V} | pm(R, O, B+2)];
pm(<<16#56, 0:8/unsigned,  R/binary>>, O, B) -> [false | pm(R, O, B+2)];
pm(<<16#56, 1:8/unsigned,  R/binary>>, O, B) -> [true  | pm(R, O, B+2)];
pm(<<16#60, V:16/unsigned, R/binary>>, O, B) -> [{ushort, V} | pm(R, O, B+3)];
pm(<<16#61, V:16/signed,   R/binary>>, O, B) -> [{short, V} | pm(R, O, B+3)];
pm(<<16#70, V:32/unsigned, R/binary>>, O, B) -> [{uint, V} | pm(R, O, B+5)];
pm(<<16#71, V:32/signed,   R/binary>>, O, B) -> [{int, V} | pm(R, O, B+5)];
pm(<<16#72, V:32/float,    R/binary>>, O, B) -> [{float, V} | pm(R, O, B+5)];
pm(<<16#73, Utf32:4/binary,R/binary>>, O, B) -> [{char, Utf32} | pm(R, O, B+5)];
pm(<<16#80, V:64/unsigned, R/binary>>, O, B) -> [{ulong, V} | pm(R, O, B+9)];
pm(<<16#81, V:64/signed,   R/binary>>, O, B) -> [{long, V} | pm(R, O, B+9)];
pm(<<16#82, V:64/float,    R/binary>>, O, B) -> [{double, V} | pm(R, O, B+9)];
pm(<<16#83, TS:64/signed,  R/binary>>, O, B) -> [{timestamp, TS} | pm(R, O, B+9)];
pm(<<16#98, Uuid:16/binary,R/binary>>, O, B) -> [{uuid, Uuid} | pm(R, O, B+17)];
%% Variable-widths
pm(<<16#a0, S:8, V:S/binary,R/binary>>, O, B) -> [{binary, V} | pm(R, O, B+2+S)];
pm(<<16#a1, S:8, V:S/binary,R/binary>>, O, B) -> [{utf8, V} | pm(R, O, B+2+S)];
pm(<<16#a3, S:8, V:S/binary,R/binary>>, O, B) -> [{symbol, V} | pm(R, O, B+2+S)];
pm(<<16#b3, S:32,V:S/binary,R/binary>>, O, B) -> [{symbol, V} | pm(R, O, B+5+S)];
pm(<<16#b0, S:32,V:S/binary,R/binary>>, O, B) -> [{binary, V} | pm(R, O, B+5+S)];
pm(<<16#b1, S:32,V:S/binary,R/binary>>, O, B) -> [{utf8, V} | pm(R, O, B+5+S)];
%% Compounds
pm(<<16#45, R/binary>>, O, B) ->
    [{list, []} | pm(R, O, B+1)];
pm(<<16#c0, S:8,CountAndValue:S/binary,R/binary>>, O, B) ->
    [{list, pm_compound(8, CountAndValue, O, B)} | pm(R, O, B+2+S)];
pm(<<16#c1, S:8,CountAndValue:S/binary,R/binary>>, O, B) ->
    List = pm_compound(8, CountAndValue, O, B),
    [{map, mapify(List)} | pm(R, O, B+2+S)];
pm(<<16#d0, S:32,CountAndValue:S/binary,R/binary>>, O, B) ->
    [{list, pm_compound(32, CountAndValue, O, B)} | pm(R, O, B+5+S)];
pm(<<16#d1, S:32,CountAndValue:S/binary,R/binary>>, O, B) ->
    List = pm_compound(32, CountAndValue, O, B),
    [{map, mapify(List)} | pm(R, O, B+5+S)];
%% Arrays
pm(<<16#e0, S:8,CountAndV:S/binary,R/binary>>, O, B) ->
    [parse_array(8, CountAndV) | pm(R, O, B+2+S)];
pm(<<16#f0, S:32,CountAndV:S/binary,R/binary>>, O, B) ->
    [parse_array(32, CountAndV) | pm(R, O, B+5+S)];
%% NaN or +-inf
pm(<<16#72, V:32, R/binary>>, O, B) ->
    [{as_is, 16#72, <<V:32>>} | pm(R, O, B+5)];
pm(<<16#82, V:64, R/binary>>, O, B) ->
    [{as_is, 16#82, <<V:64>>} | pm(R, O, B+9)];
%% decimals
pm(<<16#74, V:32, R/binary>>, O, B) ->
    [{as_is, 16#74, <<V:32>>} | pm(R, O, B+5)];
pm(<<16#84, V:64, R/binary>>, O, B) ->
    [{as_is, 16#84, <<V:64>>} | pm(R, O, B+9)];
pm(<<16#94, V:128, R/binary>>, O, B) ->
    [{as_is, 16#94, <<V:128>>} | pm(R, O, B+17)];
pm(<<Type, _Bin/binary>>, _O, B) ->
    throw({primitive_type_unsupported, Type, {position, B}}).

%%TODO inline this function
pm_compound(UnitSize, Bin, O, B) ->
    <<_Count:UnitSize, Bin1/binary>> = Bin,
    pm(Bin1, O, B).

reached_body(Position) ->
    [{{pos, Position}, body}].
