%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(amqp10_binary_generator).

-export([generate/1, build_frame/2, build_frame/3,
         build_heartbeat_frame/0]).

-include("amqp10_framing.hrl").

-type signed_byte() :: -128 .. 127.

-type amqp10_ctor() :: ubyte | ushort | byte | short | int | uing | float |
                       double |
                       char | timestamp | uuid | utf8 | symbol | binary |
                       list | map | array |
                       {described, amqp10_type(), amqp10_ctor()}.

-type amqp10_prim() ::
    null |
    boolean() |
    {boolean, boolean()} |
    {ubyte, byte()} |
    {ushort, non_neg_integer()} |
    {uint, non_neg_integer()} |
    {ulong, non_neg_integer()} |
    {byte, signed_byte()} |
    {short, integer()} |
    {int, integer()} |
    {float, float()} |
    {double, float()} |
    {char, binary()} |
    {timestamp, integer()} |
    {uuid, binary()} |
    {utf8, binary()} |
    {symbol, binary()} |
    {binary, binary()} |
    {list, [amqp10_type()]} |
    {map, [{amqp10_prim(), amqp10_prim()}]} |
    {array, amqp10_ctor(), [amqp10_type()]}.

-type amqp10_described() ::
    {described, amqp10_type(), amqp10_prim()}.

-type amqp10_type() ::
    amqp10_prim() | amqp10_described().

-export_type([
              amqp10_ctor/0,
              amqp10_type/0,
              amqp10_prim/0,
              amqp10_described/0
             ]).


-define(AMQP_FRAME_TYPE, 0).
-define(DOFF, 2).
-define(VAR_1_LIMIT, 16#FF).

-spec build_frame(non_neg_integer(), iolist()) -> iolist().
build_frame(Channel, Body) ->
    build_frame(Channel, ?AMQP_FRAME_TYPE, Body).

-spec build_frame(non_neg_integer(), non_neg_integer(), iolist()) -> iolist().
build_frame(Channel, FrameType, Body) ->
    Size = iolist_size(Body) + 8, % frame header and no extension
    [<<Size:32, 2:8, FrameType:8, Channel:16>>, Body].

build_heartbeat_frame() ->
    %% length is inclusive
    <<8:32, ?DOFF:8, ?AMQP_FRAME_TYPE:8, 0:16>>.

-spec generate(amqp10_type()) -> iodata().
generate(Type) ->
    case generate1(Type) of
        Byte when is_integer(Byte) ->
            [Byte];
        IoData ->
            IoData
    end.

generate1({described, Descriptor, Value}) ->
    DescBin = generate1(Descriptor),
    ValueBin = generate1(Value),
    [?DESCRIBED, DescBin, ValueBin];

generate1(null)  -> 16#40;
generate1(true)  -> 16#41;
generate1(false) -> 16#42;
generate1({boolean, true}) -> [16#56, 16#01];
generate1({boolean, false}) -> [16#56, 16#00];

%% some integral types have a compact encoding as a byte; this is in
%% particular for the descriptors of AMQP types, which have the domain
%% bits set to zero and values < 256.
generate1({ubyte,    V})                           -> [16#50, V];
generate1({ushort,   V})                           -> <<16#60,V:16/unsigned>>;
generate1({uint,     V}) when V =:= 0              -> 16#43;
generate1({uint,     V}) when V < 256              -> [16#52, V];
generate1({uint,     V})                           -> <<16#70,V:32/unsigned>>;
generate1({ulong,    V}) when V =:= 0              -> 16#44;
generate1({ulong,    V}) when V < 256              -> [16#53, V];
generate1({ulong,    V})                           -> <<16#80,V:64/unsigned>>;
generate1({byte,     V})                           -> <<16#51,V:8/signed>>;
generate1({short,    V})                           -> <<16#61,V:16/signed>>;
generate1({int,      V}) when V<128 andalso V>-129 -> <<16#54,V:8/signed>>;
generate1({int,      V})                           -> <<16#71,V:32/signed>>;
generate1({long,     V}) when V<128 andalso V>-129 -> <<16#55,V:8/signed>>;
generate1({long,     V})                           -> <<16#81,V:64/signed>>;
generate1({float,    V})                           -> <<16#72,V:32/float>>;
generate1({double,   V})                           -> <<16#82,V:64/float>>;
generate1({char,V}) when V>=0 andalso V=<16#10ffff -> <<16#73,V:32>>;
%% AMQP timestamp is "64-bit two's-complement integer representing milliseconds since the unix epoch".
%% For small integers (i.e. values that can be stored in a single word),
%% Erlang uses two’s complement to represent the signed integers.
generate1({timestamp,V})                           -> <<16#83,V:64/signed>>;
generate1({uuid,     V})                           -> <<16#98,V:16/binary>>;

generate1({utf8, V})
  when byte_size(V) =< ?VAR_1_LIMIT                -> [16#a1, byte_size(V), V];
generate1({utf8, V})                               -> [<<16#b1, (byte_size(V)):32>>, V];
generate1({symbol, V})
  when byte_size(V) =< ?VAR_1_LIMIT                -> [16#a3, byte_size(V), V];
generate1({symbol, V})                             -> [<<16#b3, (byte_size(V)):32>>, V];
generate1({binary, V}) ->
    Size = iolist_size(V),
    case Size =< ?VAR_1_LIMIT  of
        true ->
            [16#a0, Size, V];
        false ->
            [<<16#b0, Size:32>>, V]
    end;

generate1({list, []}) ->
    16#45;
generate1({list, List}) ->
    Count = length(List),
    Compound = lists:map(fun generate1/1, List),
    S = iolist_size(Compound),
    %% If the list contains less than (256 - 1) elements and if the
    %% encoded size (including the encoding of "Count", thus S + 1
    %% in the test) is less than 256 bytes, we use the short form.
    %% Otherwise, we use the large form.
    if Count >= (256 - 1) orelse (S + 1) >= 256 ->
           [<<16#d0, (S + 4):32, Count:32>>, Compound];
       true ->
           [16#c0, S + 1, Count, Compound]
    end;

generate1({map, KvList}) ->
    Count = length(KvList) * 2,
    Compound = lists:map(fun ({Key, Val}) ->
                                 [(generate1(Key)),
                                  (generate1(Val))]
                         end, KvList),
    S = iolist_size(Compound),
    %% See generate1({list, ...}) for an explanation of this test.
    if Count >= (256 - 1) orelse (S + 1) >= 256 ->
           [<<16#d1, (S + 4):32, Count:32>>, Compound];
       true ->
           [16#c1, S + 1, Count, Compound]
    end;

generate1({array, Type, List}) ->
    Count = length(List),
    Array = [constructor(Type),
             [generate2(Type, I) || I <- List]],
    S = iolist_size(Array),
    %% See generate1({list, ...}) for an explanation of this test.
    if Count >= (256 - 1) orelse (S + 1) >= 256 ->
           [<<16#f0, (S + 4):32, Count:32>>, Array];
       true ->
           [16#e0, S + 1, Count, Array]
    end;

generate1({as_is, TypeCode, Bin}) when is_binary(Bin) ->
    [TypeCode, Bin].

constructor(symbol) -> 16#b3;
constructor(ubyte) -> 16#50;
constructor(ushort) -> 16#60;
constructor(short) -> 16#61;
constructor(uint) -> 16#70;
constructor(ulong) -> 16#80;
constructor(byte) -> 16#51;
constructor(int) -> 16#71;
constructor(long) -> 16#81;
constructor(float) -> 16#72;
constructor(double) -> 16#82;
constructor(char) -> 16#73;
constructor(timestamp) -> 16#83;
constructor(uuid) -> 16#98;
constructor(null) -> 16#40;
constructor(boolean) -> 16#56;
constructor(binary) -> 16#b0;
constructor(utf8) -> 16#b1;
constructor(list) -> 16#d0;  % use large list type for all array elements
constructor(map) -> 16#d1;   % use large map type for all array elements
constructor(array) -> 16#f0; % use large array type for all nested arrays
constructor({described, Descriptor, Primitive}) ->
    [16#00, generate1(Descriptor), constructor(Primitive)].

generate2(symbol, {symbol, V}) -> [<<(size(V)):32>>, V];
generate2(utf8, {utf8, V}) -> [<<(size(V)):32>>, V];
generate2(binary, {binary, V}) -> [<<(size(V)):32>>, V];
generate2(boolean, true) -> 16#01;
generate2(boolean, false) -> 16#00;
generate2(boolean, {boolean, true}) -> 16#01;
generate2(boolean, {boolean, false}) -> 16#00;
generate2(null, null) -> 16#40;
generate2(char, {char,V}) when V>=0 andalso V=<16#10ffff -> <<V:32>>;
generate2(ubyte, {ubyte, V}) -> V;
generate2(byte, {byte, V}) -> <<V:8/signed>>;
generate2(ushort, {ushort, V}) -> <<V:16/unsigned>>;
generate2(short, {short, V}) -> <<V:16/signed>>;
generate2(uint, {uint, V}) -> <<V:32/unsigned>>;
generate2(int, {int, V}) -> <<V:32/signed>>;
generate2(ulong, {ulong, V}) -> <<V:64/unsigned>>;
generate2(long, {long, V}) -> <<V:64/signed>>;
generate2(float, {float, V}) -> <<V:32/float>>;
generate2(double, {double, V}) -> <<V:64/float>>;
generate2(timestamp, {timestamp,V}) -> <<V:64/signed>>;
generate2(uuid, {uuid, V}) -> <<V:16/binary>>;
generate2({described, D, P}, {described, D, V}) ->
    generate2(P, V);
generate2(list, {list, List}) ->
    Count = length(List),
    Compound = lists:map(fun generate1/1, List),
    S = iolist_size(Compound),
    [<<(S + 4):32, Count:32>>, Compound];
generate2(map, {map, KvList}) ->
    Count = length(KvList) * 2,
    Compound = lists:map(fun ({Key, Val}) ->
                                 [(generate1(Key)),
                                  (generate1(Val))]
                         end, KvList),
    S = iolist_size(Compound),
    [<<(S + 4):32, Count:32>>, Compound];
generate2(array, {array, Type, List}) ->
    Count = length(List),
    Array = [constructor(Type),
             [generate2(Type, I) || I <- List]],
    S = iolist_size(Array),
    [<<(S + 4):32, Count:32>>, Array].
