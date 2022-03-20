%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
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
    true |
    false |
    {boolean, true | false} |
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
    {map, [{amqp10_prim(), amqp10_prim()}]} | %% TODO: make map a map
    {array, amqp10_ctor(), [amqp10_type()]}.

-type amqp10_described() ::
    {described, amqp10_type(), amqp10_prim()}.

-type amqp10_type() ::
    amqp10_prim() | amqp10_described().

-export_type([
              amqp10_ctor/0,
              amqp10_type/0,
              amqp10_described/0
             ]).


-define(AMQP_FRAME_TYPE, 0).
-define(DOFF, 2).
-define(VAR_1_LIMIT, 16#FF).

-spec build_frame(integer(), iolist()) -> iolist().
build_frame(Channel, Payload) ->
    build_frame(Channel, ?AMQP_FRAME_TYPE, Payload).

build_frame(Channel, FrameType, Payload) ->
    Size = iolist_size(Payload) + 8, % frame header and no extension
    [ <<Size:32/unsigned, 2:8, FrameType:8, Channel:16/unsigned>>, Payload ].

build_heartbeat_frame() ->
    %% length is inclusive
    <<8:32, ?DOFF:8, ?AMQP_FRAME_TYPE:8, 0:16>>.

-spec generate(amqp10_type()) -> iolist().
generate({described, Descriptor, Value}) ->
    DescBin = generate(Descriptor),
    ValueBin = generate(Value),
    [ ?DESCRIBED_BIN, DescBin, ValueBin ];

generate(null)  -> <<16#40>>;
generate(true)  -> <<16#41>>;
generate(false) -> <<16#42>>;
generate({boolean, true}) -> <<16#56, 16#01>>;
generate({boolean, false}) -> <<16#56, 16#00>>;

%% some integral types have a compact encoding as a byte; this is in
%% particular for the descriptors of AMQP types, which have the domain
%% bits set to zero and values < 256.
generate({ubyte,    V})                           -> <<16#50,V:8/unsigned>>;
generate({ushort,   V})                           -> <<16#60,V:16/unsigned>>;
generate({uint,     V}) when V =:= 0              -> <<16#43>>;
generate({uint,     V}) when V < 256              -> <<16#52,V:8/unsigned>>;
generate({uint,     V})                           -> <<16#70,V:32/unsigned>>;
generate({ulong,    V}) when V =:= 0              -> <<16#44>>;
generate({ulong,    V}) when V < 256              -> <<16#53,V:8/unsigned>>;
generate({ulong,    V})                           -> <<16#80,V:64/unsigned>>;
generate({byte,     V})                           -> <<16#51,V:8/signed>>;
generate({short,    V})                           -> <<16#61,V:16/signed>>;
generate({int,      V}) when V<128 andalso V>-129 -> <<16#54,V:8/signed>>;
generate({int,      V})                           -> <<16#71,V:32/signed>>;
generate({long,     V}) when V<128 andalso V>-129 -> <<16#55,V:8/signed>>;
generate({long,     V})                           -> <<16#81,V:64/signed>>;
generate({float,    V})                           -> <<16#72,V:32/float>>;
generate({double,   V})                           -> <<16#82,V:64/float>>;
generate({char,     V})                           -> <<16#73,V:4/binary>>;
generate({timestamp,V})                           -> <<16#83,V:64/signed>>;
generate({uuid,     V})                           -> <<16#98,V:16/binary>>;

generate({utf8, V}) when size(V) < ?VAR_1_LIMIT -> [<<16#a1,(size(V)):8>>,  V];
generate({utf8, V})                             -> [<<16#b1,(size(V)):32>>, V];
generate({symbol, V})                           -> [<<16#a3,(size(V)):8>>,  V];
generate({binary, V}) ->
    Size = iolist_size(V),
    if  Size < ?VAR_1_LIMIT -> [<<16#a0,Size:8>>,  V];
        true                -> [<<16#b0,Size:32>>, V]
    end;

generate({list, []}) ->
    <<16#45>>;
generate({list, List}) ->
    Count = length(List),
    Compound = lists:map(fun generate/1, List),
    S = iolist_size(Compound),
    %% If the list contains less than (256 - 1) elements and if the
    %% encoded size (including the encoding of "Count", thus S + 1
    %% in the test) is less than 256 bytes, we use the short form.
    %% Otherwise, we use the large form.
    if Count >= (256 - 1) orelse (S + 1) >= 256 ->
            [<<16#d0, (S + 4):32/unsigned, Count:32/unsigned>>, Compound];
        true ->
            [<<16#c0, (S + 1):8/unsigned,  Count:8/unsigned>>,  Compound]
    end;

generate({map, ListOfPairs}) ->
    Count = length(ListOfPairs) * 2,
    Compound = lists:map(fun ({Key, Val}) ->
                                 [(generate(Key)),
                                  (generate(Val))]
                         end, ListOfPairs),
    S = iolist_size(Compound),
    %% See generate({list, ...}) for an explanation of this test.
    if Count >= (256 - 1) orelse (S + 1) >= 256 ->
            [<<16#d1, (S + 4):32, Count:32>>, Compound];
        true ->
            [<<16#c1, (S + 1):8,  Count:8>>,  Compound]
    end;

generate({array, Type, List}) ->
    Count = length(List),
    Body = iolist_to_binary([constructor(Type),
                             [generate(Type, I) || I <- List]]),
    S = size(Body),
    %% See generate({list, ...}) for an explanation of this test.
    if Count >= (256 - 1) orelse (S + 1) >= 256 ->
            [<<16#f0, (S + 4):32/unsigned, Count:32/unsigned>>, Body];
        true ->
            [<<16#e0, (S + 1):8/unsigned,  Count:8/unsigned>>,  Body]
    end;

generate({as_is, TypeCode, Bin}) ->
    <<TypeCode, Bin>>.

%% TODO again these are a stub to get SASL working. New codec? Will
%% that ever happen? If not we really just need to split generate/1
%% up into things like these...
%% for these constructors map straight-forwardly
constructor(symbol) -> <<16#b3>>;
constructor(ubyte) -> <<16#50>>;
constructor(ushort) -> <<16#60>>;
constructor(short) -> <<16#61>>;
constructor(uint) -> <<16#70>>;
constructor(ulong) -> <<16#80>>;
constructor(byte) -> <<16#51>>;
constructor(int) -> <<16#71>>;
constructor(long) -> <<16#81>>;
constructor(float) -> <<16#72>>;
constructor(double) -> <<16#82>>;
constructor(char) -> <<16#73>>;
constructor(timestamp) -> <<16#83>>;
constructor(uuid) -> <<16#98>>;
constructor(null) -> <<16#40>>;
constructor(boolean) -> <<16#56>>;
constructor(array) -> <<16#f0>>; % use large array type for all nested arrays
constructor(utf8) -> <<16#b1>>;
constructor({described, Descriptor, Primitive}) ->
    [<<16#00>>, generate(Descriptor), constructor(Primitive)].

% returns io_list
generate(symbol, {symbol, V}) -> [<<(size(V)):32>>, V];
generate(utf8, {utf8, V}) -> [<<(size(V)):32>>, V];
generate(boolean, true) -> <<16#01>>;
generate(boolean, false) -> <<16#00>>;
generate(boolean, {boolean, true}) -> <<16#01>>;
generate(boolean, {boolean, false}) -> <<16#00>>;
generate(ubyte, {ubyte, V}) -> <<V:8/unsigned>>;
generate(byte, {byte, V}) -> <<V:8/signed>>;
generate(ushort, {ushort, V}) -> <<V:16/unsigned>>;
generate(short, {short, V}) -> <<V:16/signed>>;
generate(uint, {uint, V}) -> <<V:32/unsigned>>;
generate(int, {int, V}) -> <<V:32/signed>>;
generate(ulong, {ulong, V}) -> <<V:64/unsigned>>;
generate(long, {long, V}) -> <<V:64/signed>>;
generate({described, D, P}, {described, D, V}) ->
    generate(P, V);
generate(array, {array, Type, List}) ->
    Count = length(List),
    Body = iolist_to_binary([constructor(Type),
                             [generate(Type, I) || I <- List]]),
    S = size(Body),
    %% See generate({list, ...}) for an explanation of this test.
    [<<(S + 4):32/unsigned, Count:32/unsigned>>, Body].
