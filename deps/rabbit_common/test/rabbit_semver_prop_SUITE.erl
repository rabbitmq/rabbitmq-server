%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_semver_prop_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
        {group, basic_properties},
        {group, prerelease_properties},
        {group, prefixed_version_properties}
    ].

groups() ->
    [
        {basic_properties, [parallel], [
            prop_gt_asymmetric,
            prop_lt_asymmetric,
            prop_eql_symmetric,
            prop_eql_reflexive,
            prop_gte_definition,
            prop_lte_definition,
            prop_gt_lt_inverse,
            prop_gt_transitive,
            prop_lt_transitive,
            prop_parse_format_roundtrip,
            prop_normalize_idempotent
        ]},
        {prerelease_properties, [parallel], [
            prop_prerelease_lt_release,
            prop_prerelease_trichotomy
        ]},
        {prefixed_version_properties, [parallel], [
            prop_prefixed_normalize_extracts_version,
            prop_prefixed_gt_base_version,
            prop_prefixed_respects_version_order,
            prop_prefixed_trichotomy,
            prop_prefixed_gt_transitive,
            prop_prefixed_vs_regular_consistent
        ]}
    ].

init_per_suite(Config) -> Config.
end_per_suite(Config) -> Config.
init_per_group(_, Config) -> Config.
end_per_group(_, Config) -> Config.
init_per_testcase(_, Config) -> Config.
end_per_testcase(_, Config) -> Config.

%%
%% Generators
%%

semver_tuple_gen() ->
    oneof([
        {non_neg_integer(), {[], []}},
        {{non_neg_integer(), non_neg_integer()}, {[], []}},
        {{non_neg_integer(), non_neg_integer(), non_neg_integer()}, {[], []}},
        {{non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer()}, {[], []}}
    ]).

version_string_gen() ->
    ?LET({Maj, Min, Patch},
         {range(0, 100), range(0, 100), range(0, 100)},
         lists:flatten(io_lib:format("~b.~b.~b", [Maj, Min, Patch]))).

prerelease_gen() ->
    oneof([
        [],
        [<<"alpha">>],
        [<<"beta">>],
        [<<"rc">>, range(1, 10)]
    ]).

version_with_prerelease_gen() ->
    ?LET({Maj, Min, Patch, Pre},
         {range(0, 50), range(0, 50), range(0, 50), prerelease_gen()},
         {Maj, Min, Patch, Pre}).

prefix_name_gen() ->
    oneof([<<"tanzu">>, <<"custom">>, <<"vendor">>, <<"test">>]).

build_suffix_gen() ->
    oneof([
        [],
        [<<"dev">>],
        [<<"dev">>, range(1, 99)],
        [<<"dev">>, range(1, 10), range(1, 10), <<"gabcdef">>]
    ]).

prefixed_version_gen() ->
    ?LET({Prefix, Maj, Min, Patch, Suffix},
         {prefix_name_gen(), range(1, 50), range(0, 50), range(0, 50), build_suffix_gen()},
         {Prefix, Maj, Min, Patch, Suffix}).

%%
%% Helper functions
%%

vsn_with_pre_to_string({Maj, Min, Patch, []}) ->
    lists:flatten(io_lib:format("~b.~b.~b", [Maj, Min, Patch]));
vsn_with_pre_to_string({Maj, Min, Patch, [Pre]}) ->
    lists:flatten(io_lib:format("~b.~b.~b-~s", [Maj, Min, Patch, Pre]));
vsn_with_pre_to_string({Maj, Min, Patch, [Pre, Num]}) ->
    lists:flatten(io_lib:format("~b.~b.~b-~s.~b", [Maj, Min, Patch, Pre, Num])).

prefixed_vsn_to_string({Prefix, Maj, Min, Patch, Suffix}) ->
    Base = io_lib:format("~s+rabbitmq.v~b.~b.~b", [Prefix, Maj, Min, Patch]),
    case Suffix of
        [] -> lists:flatten(Base);
        _ -> lists:flatten([Base, ".", format_suffix(Suffix)])
    end.

format_suffix([]) -> [];
format_suffix([H]) when is_binary(H) -> H;
format_suffix([H]) when is_integer(H) -> integer_to_list(H);
format_suffix([H | T]) when is_binary(H) -> [H, ".", format_suffix(T)];
format_suffix([H | T]) when is_integer(H) -> [integer_to_list(H), ".", format_suffix(T)].

prefixed_vsn_embedded_version({_Prefix, Maj, Min, Patch, _Suffix}) ->
    lists:flatten(io_lib:format("~b.~b.~b", [Maj, Min, Patch])).

proper_opts() ->
    [{numtests, 100},
     {on_output, fun(".", _) -> ok;
                    (F, A) -> ct:pal(?LOW_IMPORTANCE, F, A)
                 end}].

%%
%% Basic property tests
%%

prop_gt_asymmetric(_Config) ->
    ?assert(
        proper:quickcheck(
            ?FORALL({VsnA, VsnB}, {version_string_gen(), version_string_gen()},
                    case rabbit_semver:gt(VsnA, VsnB) of
                        true -> not rabbit_semver:gt(VsnB, VsnA);
                        false -> true
                    end),
            proper_opts())).

prop_lt_asymmetric(_Config) ->
    ?assert(
        proper:quickcheck(
            ?FORALL({VsnA, VsnB}, {version_string_gen(), version_string_gen()},
                    case rabbit_semver:lt(VsnA, VsnB) of
                        true -> not rabbit_semver:lt(VsnB, VsnA);
                        false -> true
                    end),
            proper_opts())).

prop_eql_symmetric(_Config) ->
    ?assert(
        proper:quickcheck(
            ?FORALL({VsnA, VsnB}, {version_string_gen(), version_string_gen()},
                    rabbit_semver:eql(VsnA, VsnB) =:= rabbit_semver:eql(VsnB, VsnA)),
            proper_opts())).

prop_eql_reflexive(_Config) ->
    ?assert(
        proper:quickcheck(
            ?FORALL(Vsn, version_string_gen(),
                    rabbit_semver:eql(Vsn, Vsn)),
            proper_opts())).

prop_gte_definition(_Config) ->
    ?assert(
        proper:quickcheck(
            ?FORALL({VsnA, VsnB}, {version_string_gen(), version_string_gen()},
                    rabbit_semver:gte(VsnA, VsnB) =:=
                        (rabbit_semver:gt(VsnA, VsnB) orelse rabbit_semver:eql(VsnA, VsnB))),
            proper_opts())).

prop_lte_definition(_Config) ->
    ?assert(
        proper:quickcheck(
            ?FORALL({VsnA, VsnB}, {version_string_gen(), version_string_gen()},
                    rabbit_semver:lte(VsnA, VsnB) =:=
                        (rabbit_semver:lt(VsnA, VsnB) orelse rabbit_semver:eql(VsnA, VsnB))),
            proper_opts())).

prop_gt_lt_inverse(_Config) ->
    ?assert(
        proper:quickcheck(
            ?FORALL({VsnA, VsnB}, {version_string_gen(), version_string_gen()},
                    case {rabbit_semver:gt(VsnA, VsnB),
                          rabbit_semver:lt(VsnA, VsnB),
                          rabbit_semver:eql(VsnA, VsnB)} of
                        {true, false, false} -> rabbit_semver:lt(VsnB, VsnA);
                        {false, true, false} -> rabbit_semver:gt(VsnB, VsnA);
                        {false, false, true} -> true;
                        _ -> false
                    end),
            proper_opts())).

prop_gt_transitive(_Config) ->
    ?assert(
        proper:quickcheck(
            ?FORALL({VsnA, VsnB, VsnC},
                    {version_string_gen(), version_string_gen(), version_string_gen()},
                    case rabbit_semver:gt(VsnA, VsnB) andalso rabbit_semver:gt(VsnB, VsnC) of
                        true -> rabbit_semver:gt(VsnA, VsnC);
                        false -> true
                    end),
            proper_opts())).

prop_lt_transitive(_Config) ->
    ?assert(
        proper:quickcheck(
            ?FORALL({VsnA, VsnB, VsnC},
                    {version_string_gen(), version_string_gen(), version_string_gen()},
                    case rabbit_semver:lt(VsnA, VsnB) andalso rabbit_semver:lt(VsnB, VsnC) of
                        true -> rabbit_semver:lt(VsnA, VsnC);
                        false -> true
                    end),
            proper_opts())).

prop_parse_format_roundtrip(_Config) ->
    ?assert(
        proper:quickcheck(
            ?FORALL(Vsn, semver_tuple_gen(),
                    begin
                        Formatted = iolist_to_binary(rabbit_semver:format(Vsn)),
                        Reparsed = rabbit_semver:parse(Formatted),
                        rabbit_semver:eql(Vsn, Reparsed)
                    end),
            proper_opts())).

prop_normalize_idempotent(_Config) ->
    ?assert(
        proper:quickcheck(
            ?FORALL(Vsn, semver_tuple_gen(),
                    rabbit_semver:normalize(Vsn) =:=
                        rabbit_semver:normalize(rabbit_semver:normalize(Vsn))),
            proper_opts())).

%%
%% Pre-release property tests
%%

prop_prerelease_lt_release(_Config) ->
    ?assert(
        proper:quickcheck(
            ?FORALL(Tuple, version_with_prerelease_gen(),
                    begin
                        {Maj, Min, Patch, PreRelease} = Tuple,
                        case PreRelease of
                            [] -> true;
                            _ ->
                                VsnPre = vsn_with_pre_to_string(Tuple),
                                VsnRelease = lists:flatten(
                                    io_lib:format("~b.~b.~b", [Maj, Min, Patch])),
                                rabbit_semver:lt(VsnPre, VsnRelease)
                        end
                    end),
            proper_opts())).

prop_prerelease_trichotomy(_Config) ->
    ?assert(
        proper:quickcheck(
            ?FORALL({TupleA, TupleB},
                    {version_with_prerelease_gen(), version_with_prerelease_gen()},
                    begin
                        VsnA = vsn_with_pre_to_string(TupleA),
                        VsnB = vsn_with_pre_to_string(TupleB),
                        Gt = rabbit_semver:gt(VsnA, VsnB),
                        Lt = rabbit_semver:lt(VsnA, VsnB),
                        Eq = rabbit_semver:eql(VsnA, VsnB),
                        case {Gt, Lt, Eq} of
                            {true, false, false} -> true;
                            {false, true, false} -> true;
                            {false, false, true} -> true;
                            _ -> false
                        end
                    end),
            proper_opts())).

%%
%% Prefixed version property tests
%%

prop_prefixed_normalize_extracts_version(_Config) ->
    ?assert(
        proper:quickcheck(
            ?FORALL(Tuple, prefixed_version_gen(),
                    begin
                        {_Prefix, Maj, Min, Patch, _Suffix} = Tuple,
                        PrefixedVsn = prefixed_vsn_to_string(Tuple),
                        {{NMaj, NMin, NPatch, 0}, {[], _}} =
                            rabbit_semver:normalize(rabbit_semver:parse(PrefixedVsn)),
                        NMaj =:= Maj andalso NMin =:= Min andalso NPatch =:= Patch
                    end),
            proper_opts())).

prop_prefixed_gt_base_version(_Config) ->
    ?assert(
        proper:quickcheck(
            ?FORALL(Tuple, prefixed_version_gen(),
                    begin
                        PrefixedVsn = prefixed_vsn_to_string(Tuple),
                        BaseVsn = prefixed_vsn_embedded_version(Tuple),
                        rabbit_semver:gt(PrefixedVsn, BaseVsn)
                    end),
            proper_opts())).

prop_prefixed_respects_version_order(_Config) ->
    ?assert(
        proper:quickcheck(
            ?FORALL({TupleA, TupleB}, {prefixed_version_gen(), prefixed_version_gen()},
                    begin
                        {_PrefixA, MajA, MinA, PatchA, _SuffixA} = TupleA,
                        {_PrefixB, MajB, MinB, PatchB, _SuffixB} = TupleB,
                        PrefixedA = prefixed_vsn_to_string(TupleA),
                        PrefixedB = prefixed_vsn_to_string(TupleB),
                        case {MajA, MinA, PatchA} > {MajB, MinB, PatchB} of
                            true -> rabbit_semver:gt(PrefixedA, PrefixedB);
                            false ->
                                case {MajA, MinA, PatchA} < {MajB, MinB, PatchB} of
                                    true -> rabbit_semver:lt(PrefixedA, PrefixedB);
                                    false -> true
                                end
                        end
                    end),
            proper_opts())).

prop_prefixed_trichotomy(_Config) ->
    ?assert(
        proper:quickcheck(
            ?FORALL({TupleA, TupleB}, {prefixed_version_gen(), prefixed_version_gen()},
                    begin
                        VsnA = prefixed_vsn_to_string(TupleA),
                        VsnB = prefixed_vsn_to_string(TupleB),
                        Gt = rabbit_semver:gt(VsnA, VsnB),
                        Lt = rabbit_semver:lt(VsnA, VsnB),
                        Eq = rabbit_semver:eql(VsnA, VsnB),
                        case {Gt, Lt, Eq} of
                            {true, false, false} -> true;
                            {false, true, false} -> true;
                            {false, false, true} -> true;
                            _ -> false
                        end
                    end),
            proper_opts())).

prop_prefixed_gt_transitive(_Config) ->
    ?assert(
        proper:quickcheck(
            ?FORALL({TupleA, TupleB, TupleC},
                    {prefixed_version_gen(), prefixed_version_gen(), prefixed_version_gen()},
                    begin
                        VsnA = prefixed_vsn_to_string(TupleA),
                        VsnB = prefixed_vsn_to_string(TupleB),
                        VsnC = prefixed_vsn_to_string(TupleC),
                        case rabbit_semver:gt(VsnA, VsnB) andalso rabbit_semver:gt(VsnB, VsnC) of
                            true -> rabbit_semver:gt(VsnA, VsnC);
                            false -> true
                        end
                    end),
            proper_opts())).

prop_prefixed_vs_regular_consistent(_Config) ->
    ?assert(
        proper:quickcheck(
            ?FORALL({PrefixedTuple, RegularVsn}, {prefixed_version_gen(), version_string_gen()},
                    begin
                        {_Prefix, PMaj, PMin, PPatch, _Suffix} = PrefixedTuple,
                        PrefixedVsn = prefixed_vsn_to_string(PrefixedTuple),
                        {RMaj, RMin, RPatch} = case string:tokens(RegularVsn, ".") of
                            [M, N, P] -> {list_to_integer(M), list_to_integer(N), list_to_integer(P)}
                        end,
                        case {PMaj, PMin, PPatch} > {RMaj, RMin, RPatch} of
                            true -> rabbit_semver:gt(PrefixedVsn, RegularVsn);
                            false ->
                                case {PMaj, PMin, PPatch} < {RMaj, RMin, RPatch} of
                                    true -> rabbit_semver:lt(PrefixedVsn, RegularVsn);
                                    false ->
                                        %% Same version tuple: prefixed > regular due to build metadata
                                        rabbit_semver:gt(PrefixedVsn, RegularVsn)
                                end
                        end
                    end),
            proper_opts())).
