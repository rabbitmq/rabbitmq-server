%%% vi:ts=4 sw=4 et

%%% Imported from https://github.com/erlware/erlware_commons.git
%%% Commit 09168347525916e291c8aa6e3073e260e5f4a116
%%% - We export normalize/1.
%%% - We add a few more testcases around string/binary comparison.

%%%-------------------------------------------------------------------
%%% @copyright (C) 2011, Erlware LLC
%%% @doc
%%%  Helper functions for working with semver versioning strings.
%%%  See https://semver.org/ for the spec.
%%% @end
%%%-------------------------------------------------------------------
-module(rabbit_semver).

-export([parse/1,
         format/1,
         normalize_then_format/1,
         eql/2,
         gt/2,
         gte/2,
         lt/2,
         lte/2,
         pes/2,
         normalize/1,
         between/3]).

%% For internal use by the rabbit_semver_parser peg
-export([internal_parse_version/1]).

-export_type([semver/0,
              version_string/0,
              any_version/0]).

%%%===================================================================
%%% Public Types
%%%===================================================================

-type version_element() :: non_neg_integer() | binary().

-type major_minor_patch_minpatch() ::
        version_element()
      | {version_element(), version_element()}
      | {version_element(), version_element(), version_element()}
      | {version_element(), version_element(),
         version_element(), version_element()}.

-type alpha_part() :: integer() | binary() | string().
-type alpha_info() :: {PreRelease::[alpha_part()],
                       BuildVersion::[alpha_part()]}.

-type semver() :: {major_minor_patch_minpatch(), alpha_info()}.

-type version_string() :: string() | binary().

-type any_version() :: version_string() | semver().

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Parse a version string or binary into a semver tuple.
-spec parse(any_version()) -> semver().
parse(Version) when is_list(Version) ->
    case rabbit_semver_parser:parse(Version) of
        {fail, _} ->
            {iolist_to_binary(Version), {[],[]}};
        Good ->
            Good
    end;
parse(Version) when is_binary(Version) ->
    case rabbit_semver_parser:parse(Version) of
        {fail, _} ->
            {Version, {[],[]}};
        Good ->
            Good
    end;
parse(Version) ->
    Version.

-spec format(semver()) -> iolist().
format({Maj, {AlphaPart, BuildPart}})
  when is_integer(Maj);
       is_binary(Maj) ->
    [format_version_part(Maj),
     format_vsn_rest(<<"-">>, AlphaPart),
     format_vsn_rest(<<"+">>, BuildPart)];
format({{Maj, Min}, {AlphaPart, BuildPart}}) ->
    [format_version_part(Maj), ".",
     format_version_part(Min),
     format_vsn_rest(<<"-">>, AlphaPart),
     format_vsn_rest(<<"+">>, BuildPart)];
format({{Maj, Min, Patch}, {AlphaPart, BuildPart}}) ->
    [format_version_part(Maj), ".",
     format_version_part(Min), ".",
     format_version_part(Patch),
     format_vsn_rest(<<"-">>, AlphaPart),
     format_vsn_rest(<<"+">>, BuildPart)];
format({{Maj, Min, Patch, MinPatch}, {AlphaPart, BuildPart}}) ->
    [format_version_part(Maj), ".",
     format_version_part(Min), ".",
     format_version_part(Patch), ".",
     format_version_part(MinPatch),
     format_vsn_rest(<<"-">>, AlphaPart),
     format_vsn_rest(<<"+">>, BuildPart)].

-spec normalize_then_format(any_version()) -> binary().
normalize_then_format(Version) ->
    {{Maj, Min, Patch, MinPatch}, _} = normalize(parse(Version)),
    iolist_to_binary(format({{Maj, Min, Patch, MinPatch}, {[], []}})).

-spec format_version_part(integer() | binary()) -> iolist().
format_version_part(Vsn) when is_integer(Vsn) ->
    integer_to_list(Vsn);
format_version_part(Vsn) when is_binary(Vsn) ->
    Vsn.

%% @doc Test for equality between semver versions.
-spec eql(any_version(), any_version()) -> boolean().
eql(VsnA, VsnB) ->
    NVsnA = normalize(parse(VsnA)),
    NVsnB = normalize(parse(VsnB)),
    NVsnA =:= NVsnB.

%% @doc Test that VsnA is greater than VsnB.
-spec gt(any_version(), any_version()) -> boolean().
gt(VsnA, VsnB) ->
    gt_normalized(normalize(parse(VsnA)), normalize(parse(VsnB))).

-spec gt_normalized(semver(), semver()) -> boolean().
gt_normalized({MMPA, {AlphaA, PatchA}}, {MMPB, {AlphaB, PatchB}}) ->
    (MMPA > MMPB)
    orelse
      ((MMPA =:= MMPB)
       andalso
         ((AlphaA =:= [] andalso AlphaB =/= [])
          orelse
            ((not (AlphaB =:= [] andalso AlphaA =/= []))
             andalso
               (AlphaA > AlphaB))))
    orelse
      ((MMPA =:= MMPB)
       andalso
         (AlphaA =:= AlphaB)
       andalso
         ((PatchB =:= [] andalso PatchA =/= [])
          orelse
          PatchA > PatchB)).

%% @doc Test that VsnA is greater than or equal to VsnB.
-spec gte(any_version(), any_version()) -> boolean().
gte(VsnA, VsnB) ->
    NVsnA = normalize(parse(VsnA)),
    NVsnB = normalize(parse(VsnB)),
    NVsnA =:= NVsnB orelse gt_normalized(NVsnA, NVsnB).

%% @doc Test that VsnA is less than VsnB.
-spec lt(any_version(), any_version()) -> boolean().
lt(VsnA, VsnB) ->
    lt_normalized(normalize(parse(VsnA)), normalize(parse(VsnB))).

-spec lt_normalized(semver(), semver()) -> boolean().
lt_normalized({MMPA, {AlphaA, PatchA}}, {MMPB, {AlphaB, PatchB}}) ->
    (MMPA < MMPB)
    orelse
      ((MMPA =:= MMPB)
       andalso
         ((AlphaB =:= [] andalso AlphaA =/= [])
          orelse
            ((not (AlphaA =:= [] andalso AlphaB =/= []))
             andalso
               (AlphaA < AlphaB))))
    orelse
      ((MMPA =:= MMPB)
       andalso
         (AlphaA =:= AlphaB)
       andalso
         ((PatchA =:= [] andalso PatchB =/= [])
          orelse
          PatchA < PatchB)).

%% @doc Test that VsnA is less than or equal to VsnB.
-spec lte(any_version(), any_version()) -> boolean().
lte(VsnA, VsnB) ->
    NVsnA = normalize(parse(VsnA)),
    NVsnB = normalize(parse(VsnB)),
    NVsnA =:= NVsnB orelse lt_normalized(NVsnA, NVsnB).

%% @doc Test that VsnMatch is greater than or equal to Vsn1 and
%% less than or equal to Vsn2.
-spec between(any_version(), any_version(), any_version()) -> boolean().
between(Vsn1, Vsn2, VsnMatch) ->
    NVsn1 = normalize(parse(Vsn1)),
    NVsn2 = normalize(parse(Vsn2)),
    NVsnMatch = normalize(parse(VsnMatch)),
    (NVsnMatch =:= NVsn1 orelse gt_normalized(NVsnMatch, NVsn1)) andalso
        (NVsnMatch =:= NVsn2 orelse lt_normalized(NVsnMatch, NVsn2)).

%% @doc Pessimistic version constraint check (the ~> operator).
%%
%% "~> 2.6" matches versions >= 2.6.0 and < 3.0.0
%% "~> 2.6.5" matches versions >= 2.6.5 and < 2.7.0
-spec pes(any_version(), any_version()) -> boolean().
pes(VsnA, VsnB) ->
    internal_pes(parse(VsnA), parse(VsnB)).

%%%===================================================================
%%% Friend Functions
%%%===================================================================

%% @doc Callback for the PEG parser to build a semver tuple from parsed tokens.
-spec internal_parse_version(iolist()) -> semver().
internal_parse_version([MMP, AlphaPart, BuildPart, _]) ->
    {parse_major_minor_patch_minpatch(MMP), {parse_alpha_part(AlphaPart),
                                             parse_alpha_part(BuildPart)}}.

-spec parse_major_minor_patch_minpatch(iolist()) -> major_minor_patch_minpatch().
parse_major_minor_patch_minpatch([MajVsn, [], [], []]) ->
    strip_maj_version(MajVsn);
parse_major_minor_patch_minpatch([MajVsn, [<<".">>, MinVsn], [], []]) ->
    {strip_maj_version(MajVsn), MinVsn};
parse_major_minor_patch_minpatch([MajVsn,
                                  [<<".">>, MinVsn],
                                  [<<".">>, PatchVsn], []]) ->
    {strip_maj_version(MajVsn), MinVsn, PatchVsn};
parse_major_minor_patch_minpatch([MajVsn,
                                  [<<".">>, MinVsn],
                                  [<<".">>, PatchVsn],
                                  [<<".">>, MinPatch]]) ->
    {strip_maj_version(MajVsn), MinVsn, PatchVsn, MinPatch}.

-spec parse_alpha_part(iolist()) -> [alpha_part()].
parse_alpha_part([]) ->
    [];
parse_alpha_part([_, AV1, Rest]) ->
    [iolist_to_binary(AV1) |
     [format_alpha_part(Part) || Part <- Rest]].

%% Per semver spec, numeric identifiers must be compared as integers.
-spec format_alpha_part(iolist()) -> integer() | binary().
format_alpha_part([<<".">>, AlphaPart]) ->
    Bin = iolist_to_binary(AlphaPart),
    try
        binary_to_integer(Bin)
    catch
        error:badarg ->
            Bin
    end.

%%%===================================================================
%%% Internal Functions
%%%===================================================================
-spec strip_maj_version(iolist()) -> version_element().
strip_maj_version([<<"v">>, MajVsn]) ->
    MajVsn;
strip_maj_version([[], MajVsn]) ->
    MajVsn;
strip_maj_version(MajVsn) ->
    MajVsn.

-spec to_list(integer() | binary() | string()) -> string() | binary().
to_list(Detail) when is_integer(Detail) ->
    integer_to_list(Detail);
to_list(Detail) when is_list(Detail); is_binary(Detail) ->
    Detail.

-spec format_vsn_rest(binary() | string(), [integer() | binary()]) -> iolist().
format_vsn_rest(_TypeMark, []) ->
    [];
format_vsn_rest(TypeMark, [Head | Rest]) ->
    [TypeMark, Head |
     [[".", to_list(Detail)] || Detail <- Rest]].

%% @doc Normalize a semver tuple to a 4-part version for comparison.
-spec normalize(semver()) -> semver().
normalize({Vsn, {Alpha, Build}}) when is_binary(Vsn) ->
    case extract_version_from_build(Build) of
        {ok, ExtractedVsn} ->
            {normalize_version(ExtractedVsn), {Alpha, Build}};
        none ->
            {{Vsn, 0, 0, 0}, {Alpha, Build}}
    end;
normalize({Vsn, Rest}) when is_integer(Vsn) ->
    {{Vsn, 0, 0, 0}, Rest};
normalize({{Maj, Min}, Rest}) ->
    {{Maj, Min, 0, 0}, Rest};
normalize({{Maj, Min, Patch}, Rest}) ->
    {{Maj, Min, Patch, 0}, Rest};
normalize({{_, _, _, _}, {_, _}} = Vsn) ->
    Vsn.

-spec normalize_version(major_minor_patch_minpatch()) ->
    {version_element(), non_neg_integer(), non_neg_integer(), non_neg_integer()}.
normalize_version(Vsn) when is_integer(Vsn) ->
    {Vsn, 0, 0, 0};
normalize_version({Maj, Min}) ->
    {Maj, Min, 0, 0};
normalize_version({Maj, Min, Patch}) ->
    {Maj, Min, Patch, 0};
normalize_version({Maj, Min, Patch, MinPatch}) ->
    {Maj, Min, Patch, MinPatch}.

%% Extracts version from build metadata for prefixed versions like
%% "tanzu+rabbitmq.v3.13.12.dev" where the actual version is embedded.
-spec extract_version_from_build([alpha_part()]) ->
    {ok, major_minor_patch_minpatch()} | none.
extract_version_from_build([]) ->
    none;
extract_version_from_build([<<"v", Rest/binary>> | T]) ->
    try
        Major = binary_to_integer(Rest),
        build_version_tuple(Major, T)
    catch
        error:badarg ->
            extract_version_from_build(T)
    end;
extract_version_from_build([_ | T]) ->
    extract_version_from_build(T).

-spec build_version_tuple(non_neg_integer(), [alpha_part()]) ->
    {ok, major_minor_patch_minpatch()}.
build_version_tuple(Major, [Minor, Patch, MinPatch | _])
  when is_integer(Minor), is_integer(Patch), is_integer(MinPatch) ->
    {ok, {Major, Minor, Patch, MinPatch}};
build_version_tuple(Major, [Minor, Patch | _])
  when is_integer(Minor), is_integer(Patch) ->
    {ok, {Major, Minor, Patch}};
build_version_tuple(Major, [Minor | _]) when is_integer(Minor) ->
    {ok, {Major, Minor}};
build_version_tuple(Major, _) ->
    {ok, Major}.

-spec internal_pes(semver(), semver()) -> boolean().
internal_pes(VsnA, {{LM, LMI}, _})
  when is_integer(LM), is_integer(LMI) ->
    gte(VsnA, {{LM, LMI, 0}, {[], []}}) andalso
        lt(VsnA, {{LM + 1, 0, 0, 0}, {[], []}});
internal_pes(VsnA, {{LM, LMI, LP}, _})
  when is_integer(LM), is_integer(LMI), is_integer(LP) ->
    gte(VsnA, {{LM, LMI, LP}, {[], []}}) andalso
        lt(VsnA, {{LM, LMI + 1, 0, 0}, {[], []}});
internal_pes(VsnA, {{LM, LMI, LP, LMP}, _})
  when is_integer(LM), is_integer(LMI), is_integer(LP), is_integer(LMP) ->
    gte(VsnA, {{LM, LMI, LP, LMP}, {[], []}}) andalso
        lt(VsnA, {{LM, LMI, LP + 1, 0}, {[], []}});
internal_pes(Vsn, LVsn) ->
    gte(Vsn, LVsn).
