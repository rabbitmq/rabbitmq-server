%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_amqp1_0_binary_generator).

-export([generate/1, build_frame/2, build_frame/3,
         build_heartbeat_frame/0]).

-include("rabbit_amqp1_0.hrl").

-ifdef(use_specs).
-spec(generate/1 :: (tuple()) -> iolist()).
-spec(build_frame/2 :: (int(), iolist()) -> iolist()).
-endif.

-define(AMQP_FRAME_TYPE, 0).
-define(DOFF, 2).
-define(VAR_1_LIMIT, 16#FF).

build_frame(Channel, Payload) ->
    build_frame(Channel, ?AMQP_FRAME_TYPE, Payload).

build_frame(Channel, FrameType, Payload) ->
    Size = iolist_size(Payload) + 8, % frame header and no extension
    [ <<Size:32/unsigned, 2:8, FrameType:8, Channel:16/unsigned>>, Payload ].

build_heartbeat_frame() ->
    %% length is inclusive
    <<8:32, ?DOFF:8, ?AMQP_FRAME_TYPE:8, 0:16>>.

generate({described, Descriptor, Value}) ->
    DescBin = generate(Descriptor),
    ValueBin = generate(Value),
    [ ?DESCRIBED_BIN, DescBin, ValueBin ];

generate(null)  -> <<16#40>>;
generate(true)  -> <<16#41>>;
generate(false) -> <<16#42>>;

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
generate({symbol, V})                           -> [<<16#a3,(length(V)):8>>,
                                                    list_to_binary(V)];
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
    %% S < 256 -> Count < 256
    if S > 255 -> [<<16#d0, (S + 4):32/unsigned, Count:32/unsigned>>, Compound];
       true    -> [<<16#c0, (S + 1):8/unsigned, Count:8/unsigned>>,   Compound]
    end;

generate({map, ListOfPairs}) ->
    Count = length(ListOfPairs) * 2,
    Compound = lists:map(fun ({Key, Val}) ->
                                 [(generate(Key)),
                                  (generate(Val))]
                         end, ListOfPairs),
    S = iolist_size(Compound),
    if S > 255 -> [<<16#d1,(S + 4):32,Count:32>>, Compound];
       true    -> [<<16#c1,(S + 1):8,Count:8>>,   Compound]
    end;

generate({array, Type, List}) ->
    Count = length(List),
    Body = iolist_to_binary(
             [constructor(Type), [generate(Type, I) || I <- List]]),
    S = size(Body),
    %% S < 256 -> Count < 256
    if S > 255 -> [<<16#f0, (S + 4):32/unsigned, Count:32/unsigned>>, Body];
       true    -> [<<16#e0, (S + 1):8/unsigned, Count:8/unsigned>>,   Body]
    end;

generate({as_is, TypeCode, Bin}) ->
    <<TypeCode, Bin>>.

%% TODO again these are a stub to get SASL working. New codec? Will
%% that ever happen? If not we really just need to split generate/1
%% up into things like these...
constructor(symbol) ->
     <<16#a3>>.

generate(symbol, Value) ->
    [<<(length(Value)):8>>, list_to_binary(Value)].
