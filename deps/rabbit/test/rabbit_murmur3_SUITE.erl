%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_murmur3_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
      {group, parallel_tests}
    ].

groups() ->
    [
      {parallel_tests, [parallel], [
          hash_empty_string,
          hash_with_seed_zero,
          hash_with_custom_seed,
          hash_known_test_vectors,
          hash_binary_data,
          hash_iolist,
          hash_consistency,
          hash_distribution
        ]}
    ].

%% -------------------------------------------------------------------
%% Test Cases
%% -------------------------------------------------------------------

%% Test empty string hashing
hash_empty_string(_Config) ->
    %% Empty string with seed 0 should produce 0
    %% Reference: with zero data and zero seed, everything becomes zero
    ?assertEqual(0, rabbit_murmur3:hash_32(<<>>)),
    ?assertEqual(0, rabbit_murmur3:hash_32(<<>>, 0)),
    %% Empty string with seed 1 - ignores nearly all the math
    ?assertEqual(16#514E28B7, rabbit_murmur3:hash_32(<<>>, 1)),
    %% Empty string with seed 0xffffffff - make sure seed uses unsigned 32-bit math
    ?assertEqual(16#81F16F39, rabbit_murmur3:hash_32(<<>>, 16#ffffffff)),
    passed.

%% Test hashing with seed 0
hash_with_seed_zero(_Config) ->
    %% Known test vectors from reference implementation with seed 0
    %% Reference: https://gist.github.com/vladimirgamalyan/defb2482feefbf5c3ea25b14c557753b
    ?assertEqual(16#B3DD93FA, rabbit_murmur3:hash_32(<<"abc">>)),
    ?assertEqual(16#EE925B90, rabbit_murmur3:hash_32(<<"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq">>)),
    passed.

%% Test hashing with custom seeds
hash_with_custom_seed(_Config) ->
    %% Test with seed 0x9747b28c (common test seed)
    Seed = 16#9747b28c,
    
    %% Reference test vectors with seed 0x9747b28c
    ?assertEqual(16#5A97808A, rabbit_murmur3:hash_32(<<"aaaa">>, Seed)),
    ?assertEqual(16#283E0130, rabbit_murmur3:hash_32(<<"aaa">>, Seed)),
    ?assertEqual(16#5D211726, rabbit_murmur3:hash_32(<<"aa">>, Seed)),
    ?assertEqual(16#7FA09EA6, rabbit_murmur3:hash_32(<<"a">>, Seed)),
    
    %% Endian order within chunks
    ?assertEqual(16#F0478627, rabbit_murmur3:hash_32(<<"abcd">>, Seed)),
    ?assertEqual(16#C84A62DD, rabbit_murmur3:hash_32(<<"abc">>, Seed)),
    ?assertEqual(16#74875592, rabbit_murmur3:hash_32(<<"ab">>, Seed)),
    
    ?assertEqual(16#24884CBA, rabbit_murmur3:hash_32(<<"Hello, world!">>, Seed)),
    ?assertEqual(16#2FA826CD, rabbit_murmur3:hash_32(<<"The quick brown fox jumps over the lazy dog">>, Seed)),
    
    %% Different seeds should produce different hashes
    Hash1 = rabbit_murmur3:hash_32(<<"test">>, Seed),
    Hash2 = rabbit_murmur3:hash_32(<<"test">>, 0),
    Hash3 = rabbit_murmur3:hash_32(<<"test">>, 42),
    ?assertNotEqual(Hash1, Hash2),
    ?assertNotEqual(Hash1, Hash3),
    ?assertNotEqual(Hash2, Hash3),
    passed.

%% Test known test vectors from reference implementation
hash_known_test_vectors(_Config) ->
    %% Test vectors verified against reference C implementation
    %% Reference: https://gist.github.com/vladimirgamalyan/defb2482feefbf5c3ea25b14c557753b
    
    %% Binary array tests with seed 0
    %% Make sure 4-byte chunks use unsigned math
    ?assertEqual(16#76293B50, rabbit_murmur3:hash_32(<<16#ff, 16#ff, 16#ff, 16#ff>>)),
    
    %% Endian order test: bytes 0x21, 0x43, 0x65, 0x87 should be read as little-endian
    ?assertEqual(16#F55B516B, rabbit_murmur3:hash_32(<<16#21, 16#43, 16#65, 16#87>>)),
    
    %% Special seed value test
    ?assertEqual(16#2362F9DE, rabbit_murmur3:hash_32(<<16#21, 16#43, 16#65, 16#87>>, 16#5082EDEE)),
    
    %% Tail length tests (3, 2, 1 bytes)
    ?assertEqual(16#7E4A8634, rabbit_murmur3:hash_32(<<16#21, 16#43, 16#65>>)),
    ?assertEqual(16#A0F7B07A, rabbit_murmur3:hash_32(<<16#21, 16#43>>)),
    ?assertEqual(16#72661CF4, rabbit_murmur3:hash_32(<<16#21>>)),
    
    %% Zero bytes tests
    ?assertEqual(16#2362F9DE, rabbit_murmur3:hash_32(<<0, 0, 0, 0>>)),
    ?assertEqual(16#85F0B427, rabbit_murmur3:hash_32(<<0, 0, 0>>)),
    ?assertEqual(16#30F4C306, rabbit_murmur3:hash_32(<<0, 0>>)),
    ?assertEqual(16#514E28B7, rabbit_murmur3:hash_32(<<0>>)),
    passed.

%% Test binary data hashing
hash_binary_data(_Config) ->
    %% Test with raw binary data
    Bin = <<0, 1, 2, 3, 4, 5, 6, 7, 8, 9>>,
    Hash = rabbit_murmur3:hash_32(Bin),
    ?assert(is_integer(Hash)),
    ?assert(Hash >= 0),
    ?assert(Hash =< 16#ffffffff),
    
    %% Test with binary containing high bytes
    Bin2 = <<255, 254, 253, 252, 251>>,
    Hash2 = rabbit_murmur3:hash_32(Bin2),
    ?assert(is_integer(Hash2)),
    passed.

%% Test iolist input
hash_iolist(_Config) ->
    %% iolist should produce same result as equivalent binary
    Binary = <<"hello world">>,
    IoList = [<<"hello">>, <<" ">>, <<"world">>],
    ?assertEqual(rabbit_murmur3:hash_32(Binary), rabbit_murmur3:hash_32(IoList)),
    
    %% Nested iolist
    NestedIoList = [[<<"hel">>, <<"lo">>], [<<" ">>, [<<"wor">>, <<"ld">>]]],
    ?assertEqual(rabbit_murmur3:hash_32(Binary), rabbit_murmur3:hash_32(NestedIoList)),
    passed.

%% Test hash consistency
hash_consistency(_Config) ->
    %% Same input should always produce same output
    Input = <<"consistent input">>,
    Hash1 = rabbit_murmur3:hash_32(Input),
    Hash2 = rabbit_murmur3:hash_32(Input),
    Hash3 = rabbit_murmur3:hash_32(Input),
    ?assertEqual(Hash1, Hash2),
    ?assertEqual(Hash2, Hash3),
    
    %% Same input with same seed
    Hash4 = rabbit_murmur3:hash_32(Input, 12345),
    Hash5 = rabbit_murmur3:hash_32(Input, 12345),
    ?assertEqual(Hash4, Hash5),
    passed.

%% Test hash distribution (basic avalanche property)
hash_distribution(_Config) ->
    %% Generate hashes for sequential integers and verify reasonable distribution
    Hashes = [rabbit_murmur3:hash_32(integer_to_binary(N)) || N <- lists:seq(1, 1000)],
    UniqueHashes = length(lists:usort(Hashes)),
    
    %% All hashes should be unique for this small set
    ?assertEqual(1000, UniqueHashes),
    
    %% Check that hashes are well distributed across the 32-bit range
    %% by verifying we have hashes in different "buckets"
    Buckets = lists:foldl(
        fun(H, Acc) ->
            Bucket = H div (16#ffffffff div 16),
            maps:update_with(Bucket, fun(V) -> V + 1 end, 1, Acc)
        end,
        #{},
        Hashes
    ),
    %% We should have hashes in at least 10 of the 16 buckets
    ?assert(maps:size(Buckets) >= 10),
    passed.
