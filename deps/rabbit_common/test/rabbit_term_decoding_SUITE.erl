%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_term_decoding_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(PROP_ITERATIONS, 200).

all() ->
    [{group, atom_tag_scanner},
     {group, decompression},
     {group, decode_bounded},
     {group, properties}].

groups() ->
    [{atom_tag_scanner, [parallel], [
        accepts_payload_containing_no_atom_tags,
        rejects_payload_exceeding_atom_count_threshold,
        rejects_deeply_nested_terms_without_stack_overflow,
        rejects_truncated_or_malformed_etf_streams,
        rejects_payloads_containing_fun_or_export_tags,
        rejects_payloads_containing_pid_port_ref_tags,
        does_not_misinterpret_atom_like_bytes_inside_binary_or_string_payloads,
        threshold_is_off_by_one_correct
     ]},
     {decompression, [parallel], [
        aborts_when_declared_inflated_size_exceeds_cap,
        aborts_when_observed_inflated_size_exceeds_cap,
        passes_through_uncompressed_etf,
        rejects_non_etf_input
     ]},
     {decode_bounded, [parallel], [
        decodes_legitimate_term,
        rejects_oversized_input,
        rejects_input_with_too_many_atoms,
        rejects_garbage_input
     ]},
     {properties, [], [
        prop_scanner_count_agrees_with_reference_on_random_safe_terms,
        prop_scanner_does_not_mint_atoms_on_arbitrary_input
     ]}].

init_per_suite(Config) -> Config.
end_per_suite(_)       -> ok.

%%
%% Atom-tag scanner
%%

accepts_payload_containing_no_atom_tags(_) ->
    Bin = erlang:term_to_binary(<<"binary with no atoms">>),
    ?assertEqual({ok, 0}, rabbit_term_decoding:count_atom_tags_bounded(Bin, 100)).

rejects_payload_exceeding_atom_count_threshold(_) ->
    Bin = many_atoms_etf(200),
    ?assertEqual({error, too_many_atom_tags},
                 rabbit_term_decoding:count_atom_tags_bounded(Bin, 100)).

rejects_deeply_nested_terms_without_stack_overflow(_) ->
    Bin = deeply_nested_list_etf(512),
    ?assertMatch({error, max_nesting_depth_exceeded},
                 rabbit_term_decoding:count_atom_tags_bounded(Bin, 1000)).

rejects_truncated_or_malformed_etf_streams(_) ->
    lists:foreach(
      fun(Bin) ->
              ?assertMatch({error, _},
                           rabbit_term_decoding:count_atom_tags_bounded(Bin, 1000))
      end,
      [<<>>,
       <<131>>,
       <<131, 118, 0, 100>>,           % ATOM_UTF8_EXT claims 100 bytes
       <<131, 118, 0, 1>>,             % claims 1 byte, body missing
       <<131, 0>>,                     % unknown tag 0
       <<131, 255>>,                   % unknown tag 255
       <<131, 104, 5, 119, 1, "a">>]). % SMALL_TUPLE_EXT arity 5, only 1 elem

rejects_payloads_containing_fun_or_export_tags(_) ->
    ?assertMatch({error, fun_tag_not_allowed},
                 rabbit_term_decoding:count_atom_tags_bounded(
                   erlang:term_to_binary(fun() -> ok end), 1000)).

rejects_payloads_containing_pid_port_ref_tags(_) ->
    lists:foreach(
      fun(Bin) ->
              ?assertMatch({error, pid_or_port_or_ref_not_allowed},
                           rabbit_term_decoding:count_atom_tags_bounded(Bin, 1000))
      end,
      [erlang:term_to_binary(self()),
       erlang:term_to_binary(make_ref())]).

does_not_misinterpret_atom_like_bytes_inside_binary_or_string_payloads(_) ->
    %% A binary whose body literally contains the bytes of an
    %% `ATOM_UTF8_EXT` tag must NOT be counted as an atom.
    BinaryCase = erlang:term_to_binary(<<118, 0, 5, "fake!">>),
    StringCase = erlang:term_to_binary([118, 0, 5, $f, $a, $k, $e]),
    ?assertEqual({ok, 0},
                 rabbit_term_decoding:count_atom_tags_bounded(BinaryCase, 100)),
    ?assertEqual({ok, 0},
                 rabbit_term_decoding:count_atom_tags_bounded(StringCase, 100)).

threshold_is_off_by_one_correct(_) ->
    N = 10,
    AcceptedBin = many_atoms_etf(N),
    OneOverBin  = many_atoms_etf(N + 1),
    ?assertEqual({ok, N},
                 rabbit_term_decoding:count_atom_tags_bounded(AcceptedBin, N)),
    ?assertMatch({error, too_many_atom_tags},
                 rabbit_term_decoding:count_atom_tags_bounded(OneOverBin, N)).

%%
%% Decompression
%%

aborts_when_declared_inflated_size_exceeds_cap(_) ->
    %% `UncompressedSize` header claims more than the cap; the inflate
    %% must never run.
    Bin = <<131, 80, 10_000_000:32, 0:32>>,
    ?assertEqual({error, declared_inflated_size_exceeds_cap},
                 rabbit_term_decoding:bounded_decompress(Bin, 1_000_000)).

aborts_when_observed_inflated_size_exceeds_cap(_) ->
    Compressed = erlang:term_to_binary(binary:copy(<<"A">>, 2_000_000),
                                       [{compressed, 9}]),
    ?assertEqual({error, declared_inflated_size_exceeds_cap},
                 rabbit_term_decoding:bounded_decompress(Compressed, 1_000_000)).

passes_through_uncompressed_etf(_) ->
    Bin = erlang:term_to_binary(#{a => 1}),
    ?assertEqual({ok, Bin},
                 rabbit_term_decoding:bounded_decompress(Bin, 1000)).

rejects_non_etf_input(_) ->
    ?assertEqual({error, not_etf},
                 rabbit_term_decoding:bounded_decompress(<<"not etf">>, 1000)).

%%
%% `decode_bounded/3` end-to-end
%%

decodes_legitimate_term(_) ->
    Term = #{users => [#{<<"name">> => <<"alice">>}]},
    Bin  = erlang:term_to_binary(Term, [{compressed, 9}]),
    ?assertEqual({ok, Term},
                 rabbit_term_decoding:decode_bounded(Bin, 1_000_000, 1000)).

rejects_oversized_input(_) ->
    Big = binary:copy(<<"x">>, 1000),
    ?assertEqual({error, payload_too_large},
                 rabbit_term_decoding:decode_bounded(Big, 100, 1000)).

rejects_input_with_too_many_atoms(_) ->
    Bin = many_atoms_etf(50),
    ?assertEqual({error, too_many_atom_tags},
                 rabbit_term_decoding:decode_bounded(Bin, 1_000_000, 10)).

rejects_garbage_input(_) ->
    ?assertMatch({error, _},
                 rabbit_term_decoding:decode_bounded(<<"garbage">>, 1000, 100)),
    ?assertMatch({error, not_a_binary},
                 rabbit_term_decoding:decode_bounded(not_a_binary, 1000, 100)).

%%
%% Property tests
%%

proper_opts() ->
    [{numtests, ?PROP_ITERATIONS},
     {on_output, fun(".", _) -> ok;
                    (F, A) -> ct:pal(?LOW_IMPORTANCE, F, A)
                 end}].

prop_scanner_count_agrees_with_reference_on_random_safe_terms(_) ->
    ?assert(
       proper:quickcheck(
         ?FORALL(T, safe_term(),
                 begin
                     Bin = erlang:term_to_binary(T),
                     Expected = count_atoms_in_term(T),
                     {ok, Got} = rabbit_term_decoding:count_atom_tags_bounded(
                                   Bin, 1_000_000),
                     Got =:= Expected
                 end),
         proper_opts())).

prop_scanner_does_not_mint_atoms_on_arbitrary_input(_) ->
    ?assert(
       proper:quickcheck(
         ?FORALL(Bytes, binary(),
                 begin
                     Before = erlang:system_info(atom_count),
                     _ = (catch rabbit_term_decoding:count_atom_tags_bounded(
                                  Bytes, 1000)),
                     After = erlang:system_info(atom_count),
                     After - Before =< 8
                 end),
         proper_opts())).

%%
%% Payload builders
%%

many_atoms_etf(N) ->
    Atoms = [list_to_atom("rabbit_term_decoding_test_a_" ++ integer_to_list(I))
             || I <- lists:seq(1, N)],
    erlang:term_to_binary(list_to_tuple(Atoms)).

deeply_nested_list_etf(Depth) ->
    Term = lists:foldl(fun(_, Acc) -> [Acc] end, [], lists:seq(1, Depth)),
    erlang:term_to_binary(Term).

%%
%% Reference walker for the property test
%%

count_atoms_in_term(A) when is_atom(A)    -> 1;
count_atoms_in_term(B) when is_binary(B)  -> 0;
count_atoms_in_term(I) when is_integer(I) -> 0;
count_atoms_in_term(F) when is_float(F)   -> 0;
count_atoms_in_term([])                   -> 0;
count_atoms_in_term([H | T]) ->
    count_atoms_in_term(H) + count_atoms_in_term(T);
count_atoms_in_term(T) when is_tuple(T) ->
    lists:sum([count_atoms_in_term(E) || E <- tuple_to_list(T)]);
count_atoms_in_term(M) when is_map(M) ->
    maps:fold(fun(K, V, Acc) ->
                      Acc + count_atoms_in_term(K) + count_atoms_in_term(V)
              end, 0, M).

%%
%% Generators
%%

safe_term() ->
    ?SIZED(N, safe_term(N)).

safe_term(0) ->
    oneof([safe_atom(), small_int(), float(), binary(), []]);
safe_term(N) ->
    ?LAZY(oneof([safe_atom(),
                 small_int(),
                 float(),
                 binary(),
                 [],
                 list(safe_term(N div 3)),
                 ?LET(L, list(safe_term(N div 3)), list_to_tuple(L)),
                 ?LET(L, list({safe_term(N div 3), safe_term(N div 3)}),
                      maps:from_list(L))])).

safe_atom() ->
    elements([ok, undefined, error, foo, bar, baz, none, nil, leaf, inner]).

small_int() ->
    integer(-1_000_000, 1_000_000).
