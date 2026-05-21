%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% Bounded decoding of External Term Format (ETF) payloads. Caps the
%% incoming wire size, the post-decompression size, and the number of
%% atom-creating positions in the term, before calling
%% `binary_to_term/1`. Rejects fun, export, pid, port, ref, and
%% atom-cache-ref tags, which have no place in a data payload.

-module(rabbit_term_decoding).

-export([
    bounded_decompress/2,
    count_atom_tags_bounded/2,
    decode_bounded/3
]).

-define(MAX_ETF_NESTING_DEPTH, 256).

%% Combined entry point: enforce a wire-size cap, optionally inflate
%% under a separate output cap, run the atom-tag scanner under a count
%% cap, and only then call `binary_to_term/1`. Returns the decoded term
%% on success.
-spec decode_bounded(binary(), pos_integer(), pos_integer()) ->
    {ok, term()} | {error, atom()}.
decode_bounded(Bin, MaxBytes, MaxAtoms) when is_binary(Bin) ->
    case byte_size(Bin) =< MaxBytes of
        false ->
            {error, payload_too_large};
        true ->
            case bounded_decompress(Bin, MaxBytes) of
                {ok, Etf} ->
                    case count_atom_tags_bounded(Etf, MaxAtoms) of
                        {ok, _Count} ->
                            try {ok, erlang:binary_to_term(Etf)}
                            catch _:_ -> {error, decode_failure}
                            end;
                        {error, _} = E -> E
                    end;
                {error, _} = E ->
                    E
            end
    end;
decode_bounded(_, _, _) ->
    {error, not_a_binary}.

%% Returns uncompressed ETF (always starting with `<<131, _/binary>>`).
%% For compressed inputs the inner zlib stream is inflated under both a
%% header-claim check and a streaming observed-size cap.
-spec bounded_decompress(binary(), pos_integer()) ->
    {ok, binary()} | {error, atom()}.
bounded_decompress(<<131, 80, UncompressedSize:32, _/binary>>, MaxBytes)
  when UncompressedSize > MaxBytes ->
    {error, declared_inflated_size_exceeds_cap};
bounded_decompress(<<131, 80, _UncompressedSize:32, Zlib/binary>>, MaxBytes) ->
    case streaming_inflate(Zlib, MaxBytes) of
        {ok, Inflated} -> {ok, <<131, Inflated/binary>>};
        {error, _} = E -> E
    end;
bounded_decompress(<<131, _/binary>> = Bin, _MaxBytes) ->
    {ok, Bin};
bounded_decompress(_, _) ->
    {error, not_etf}.

streaming_inflate(Zlib, MaxBytes) ->
    Z = zlib:open(),
    try
        ok = zlib:inflateInit(Z),
        do_streaming_inflate(Z, Zlib, MaxBytes, 0, [])
    catch
        _:_ ->
            {error, inflate_error}
    after
        zlib:close(Z)
    end.

do_streaming_inflate(Z, Input, MaxBytes, SoFar, Acc) ->
    case zlib:safeInflate(Z, Input) of
        {finished, Out} ->
            Total = SoFar + iolist_size(Out),
            if Total =< MaxBytes ->
                   {ok, iolist_to_binary(lists:reverse([Out | Acc]))};
               true ->
                   {error, inflated_size_exceeds_cap}
            end;
        {continue, Out} ->
            Total = SoFar + iolist_size(Out),
            if Total =< MaxBytes ->
                   do_streaming_inflate(Z, [], MaxBytes, Total, [Out | Acc]);
               true ->
                   {error, inflated_size_exceeds_cap}
            end
    end.

-spec count_atom_tags_bounded(binary(), pos_integer()) ->
    {ok, non_neg_integer()} | {error, atom()}.
count_atom_tags_bounded(<<131, Rest/binary>>, MaxAtoms) ->
    case scan_one(Rest, 0, MaxAtoms, 0) of
        {ok, Count, <<>>}    -> {ok, Count};
        {ok, _, _Trailing}   -> {error, trailing_bytes_after_term};
        {error, _} = E       -> E
    end;
count_atom_tags_bounded(_, _) ->
    {error, not_etf}.

scan_one(_Bin, _Count, _Max, Depth) when Depth > ?MAX_ETF_NESTING_DEPTH ->
    {error, max_nesting_depth_exceeded};
scan_one(_Bin, Count, Max, _Depth) when Count > Max ->
    {error, too_many_atom_tags};
scan_one(<<100, Len:16, _Name:Len/binary, Rest/binary>>, Count, Max, _Depth) ->
    bump_atoms(Count, Max, Rest);
scan_one(<<115, Len:8, _Name:Len/binary, Rest/binary>>, Count, Max, _Depth) ->
    bump_atoms(Count, Max, Rest);
scan_one(<<118, Len:16, _Name:Len/binary, Rest/binary>>, Count, Max, _Depth) ->
    bump_atoms(Count, Max, Rest);
scan_one(<<119, Len:8, _Name:Len/binary, Rest/binary>>, Count, Max, _Depth) ->
    bump_atoms(Count, Max, Rest);
scan_one(<<97, _:8, Rest/binary>>, Count, _Max, _Depth) ->
    {ok, Count, Rest};
scan_one(<<98, _:32, Rest/binary>>, Count, _Max, _Depth) ->
    {ok, Count, Rest};
scan_one(<<70, _:64, Rest/binary>>, Count, _Max, _Depth) ->
    {ok, Count, Rest};
scan_one(<<106, Rest/binary>>, Count, _Max, _Depth) ->
    {ok, Count, Rest};
%% Length-prefixed opaque bodies. The scanner MUST NOT descend into
%% the body; arbitrary bytes inside a `BINARY_EXT` or `STRING_EXT` can
%% otherwise look like atom tags to a byte-pattern scanner.
scan_one(<<109, Len:32, _Body:Len/binary, Rest/binary>>, Count, _Max, _Depth) ->
    {ok, Count, Rest};
scan_one(<<107, Len:16, _Body:Len/binary, Rest/binary>>, Count, _Max, _Depth) ->
    {ok, Count, Rest};
scan_one(<<110, N:8, _Sign:8, _Mag:N/binary, Rest/binary>>, Count, _Max, _Depth) ->
    {ok, Count, Rest};
scan_one(<<111, N:32, _Sign:8, _Mag:N/binary, Rest/binary>>, Count, _Max, _Depth) ->
    {ok, Count, Rest};
scan_one(<<104, Arity:8, Rest/binary>>, Count, Max, Depth) ->
    scan_n_elements(Arity, Rest, Count, Max, Depth + 1);
scan_one(<<105, Arity:32, Rest/binary>>, Count, Max, Depth) ->
    scan_n_elements(Arity, Rest, Count, Max, Depth + 1);
scan_one(<<108, Len:32, Rest/binary>>, Count, Max, Depth) ->
    case scan_n_elements(Len, Rest, Count, Max, Depth + 1) of
        {ok, NewCount, AfterElements} ->
            scan_one(AfterElements, NewCount, Max, Depth + 1);
        {error, _} = E ->
            E
    end;
scan_one(<<116, Arity:32, Rest/binary>>, Count, Max, Depth) ->
    scan_n_elements(Arity * 2, Rest, Count, Max, Depth + 1);
scan_one(<<112, _/binary>>, _Count, _Max, _Depth) -> {error, fun_tag_not_allowed};
scan_one(<<117, _/binary>>, _Count, _Max, _Depth) -> {error, fun_tag_not_allowed};
scan_one(<<113, _/binary>>, _Count, _Max, _Depth) -> {error, export_tag_not_allowed};
scan_one(<<82, _/binary>>,  _Count, _Max, _Depth) -> {error, atom_cache_ref_not_allowed};
scan_one(<<88, _/binary>>,  _Count, _Max, _Depth) -> {error, pid_or_port_or_ref_not_allowed};
scan_one(<<89, _/binary>>,  _Count, _Max, _Depth) -> {error, pid_or_port_or_ref_not_allowed};
scan_one(<<90, _/binary>>,  _Count, _Max, _Depth) -> {error, pid_or_port_or_ref_not_allowed};
scan_one(<<91, _/binary>>,  _Count, _Max, _Depth) -> {error, pid_or_port_or_ref_not_allowed};
scan_one(<<101, _/binary>>, _Count, _Max, _Depth) -> {error, pid_or_port_or_ref_not_allowed};
scan_one(<<102, _/binary>>, _Count, _Max, _Depth) -> {error, pid_or_port_or_ref_not_allowed};
scan_one(<<103, _/binary>>, _Count, _Max, _Depth) -> {error, pid_or_port_or_ref_not_allowed};
scan_one(<<114, _/binary>>, _Count, _Max, _Depth) -> {error, pid_or_port_or_ref_not_allowed};
scan_one(<<77, _/binary>>,  _Count, _Max, _Depth) -> {error, bit_binary_not_allowed};
scan_one(<<99, _/binary>>,  _Count, _Max, _Depth) -> {error, deprecated_float_not_allowed};
scan_one(_Bin, _Count, _Max, _Depth) ->
    {error, unknown_or_truncated_etf}.

scan_n_elements(0, Bin, Count, _Max, _Depth) ->
    {ok, Count, Bin};
scan_n_elements(N, Bin, Count, Max, Depth) when N > 0 ->
    case scan_one(Bin, Count, Max, Depth) of
        {ok, NewCount, Rest} ->
            scan_n_elements(N - 1, Rest, NewCount, Max, Depth);
        {error, _} = E ->
            E
    end.

bump_atoms(Count, Max, _Rest) when Count + 1 > Max ->
    {error, too_many_atom_tags};
bump_atoms(Count, _Max, Rest) ->
    {ok, Count + 1, Rest}.
