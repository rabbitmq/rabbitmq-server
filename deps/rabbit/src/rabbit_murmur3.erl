%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% @doc MurmurHash3 32-bit implementation.
%%
%% This module provides an efficient pure Erlang implementation of the
%% MurmurHash3 32-bit hash algorithm. MurmurHash3 is a non-cryptographic
%% hash function suitable for general hash-based lookup.
%%
%% Reference implementation:
%% https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp
%%
%% @end
-module(rabbit_murmur3).

-export([hash_32/1, hash_32/2]).

-compile({inline, [rotl32/2, fmix32/1, process_tail/2]}).

%% MurmurHash3 constants
-define(C1, 16#cc9e2d51).
-define(C2, 16#1b873593).
-define(MASK32, 16#ffffffff).

%% @doc Compute the 32-bit MurmurHash3 of the given data with seed 0.
-spec hash_32(Data :: iodata()) -> non_neg_integer().
hash_32(Data) ->
    hash_32(Data, 0).

%% @doc Compute the 32-bit MurmurHash3 of the given data with the specified seed.
-spec hash_32(Data :: iodata(), Seed :: non_neg_integer()) -> non_neg_integer().
hash_32(Data, Seed) when is_list(Data) ->
    hash_32(iolist_to_binary(Data), Seed);
hash_32(Data, Seed) when is_binary(Data), is_integer(Seed), Seed >= 0 ->
    Len = byte_size(Data),
    {Tail, H1} = process_blocks(Data, Seed band ?MASK32),
    H2 = process_tail(Tail, H1),
    fmix32(H2 bxor Len).

%% Process 4-byte blocks, returns remaining tail and accumulated hash
-spec process_blocks(binary(), non_neg_integer()) -> {binary(), non_neg_integer()}.
process_blocks(<<K:32/unsigned-little-integer, Rest/binary>>, H) ->
    K1 = (K * ?C1) band ?MASK32,
    K2 = rotl32(K1, 15),
    K3 = (K2 * ?C2) band ?MASK32,
    H1 = H bxor K3,
    H2 = rotl32(H1, 13),
    H3 = ((H2 * 5) + 16#e6546b64) band ?MASK32,
    process_blocks(Rest, H3);
process_blocks(Tail, H) ->
    {Tail, H}.

%% Process remaining bytes (tail: 0-3 bytes)
-spec process_tail(binary(), non_neg_integer()) -> non_neg_integer().
process_tail(<<>>, H) ->
    H;
process_tail(<<B1:8>>, H) ->
    K1 = (B1 * ?C1) band ?MASK32,
    K2 = rotl32(K1, 15),
    K3 = (K2 * ?C2) band ?MASK32,
    H bxor K3;
process_tail(<<B1:8, B2:8>>, H) ->
    K = (B2 bsl 8) bor B1,
    K1 = (K * ?C1) band ?MASK32,
    K2 = rotl32(K1, 15),
    K3 = (K2 * ?C2) band ?MASK32,
    H bxor K3;
process_tail(<<B1:8, B2:8, B3:8>>, H) ->
    K = (B3 bsl 16) bor (B2 bsl 8) bor B1,
    K1 = (K * ?C1) band ?MASK32,
    K2 = rotl32(K1, 15),
    K3 = (K2 * ?C2) band ?MASK32,
    H bxor K3.

%% Final mix function
-spec fmix32(non_neg_integer()) -> non_neg_integer().
fmix32(H) ->
    H1 = H bxor (H bsr 16),
    H2 = (H1 * 16#85ebca6b) band ?MASK32,
    H3 = H2 bxor (H2 bsr 13),
    H4 = (H3 * 16#c2b2ae35) band ?MASK32,
    H4 bxor (H4 bsr 16).

%% 32-bit rotate left
-spec rotl32(non_neg_integer(), 0..31) -> non_neg_integer().
rotl32(X, R) ->
    ((X bsl R) bor (X bsr (32 - R))) band ?MASK32.
