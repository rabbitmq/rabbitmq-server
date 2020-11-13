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

%% @doc parse a string or binary into a valid semver representation
-spec parse(any_version()) -> semver().
parse(Version) when erlang:is_list(Version) ->
    case rabbit_semver_parser:parse(Version) of
        {fail, _} ->
            {erlang:iolist_to_binary(Version), {[],[]}};
        Good ->
            Good
    end;
parse(Version) when erlang:is_binary(Version) ->
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
  when erlang:is_integer(Maj);
       erlang:is_binary(Maj) ->
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

-spec format_version_part(integer() | binary()) -> iolist().
format_version_part(Vsn)
  when erlang:is_integer(Vsn) ->
    erlang:integer_to_list(Vsn);
format_version_part(Vsn)
  when erlang:is_binary(Vsn) ->
    Vsn.



%% @doc test for quality between semver versions
-spec eql(any_version(), any_version()) -> boolean().
eql(VsnA, VsnB) ->
    NVsnA = normalize(parse(VsnA)),
    NVsnB = normalize(parse(VsnB)),
    NVsnA =:= NVsnB.

%% @doc Test that VsnA is greater than VsnB
-spec gt(any_version(), any_version()) -> boolean().
gt(VsnA, VsnB) ->
    {MMPA, {AlphaA, PatchA}} = normalize(parse(VsnA)),
    {MMPB, {AlphaB, PatchB}} = normalize(parse(VsnB)),
    ((MMPA > MMPB)
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
           PatchA > PatchB))).

%% @doc Test that VsnA is greater than or equal to VsnB
-spec gte(any_version(), any_version()) -> boolean().
gte(VsnA, VsnB) ->
    NVsnA = normalize(parse(VsnA)),
    NVsnB = normalize(parse(VsnB)),
    gt(NVsnA, NVsnB) orelse eql(NVsnA, NVsnB).

%% @doc Test that VsnA is less than VsnB
-spec lt(any_version(), any_version()) -> boolean().
lt(VsnA, VsnB) ->
    {MMPA, {AlphaA, PatchA}} = normalize(parse(VsnA)),
    {MMPB, {AlphaB, PatchB}} = normalize(parse(VsnB)),
    ((MMPA < MMPB)
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
           PatchA < PatchB))).

%% @doc Test that VsnA is less than or equal to VsnB
-spec lte(any_version(), any_version()) -> boolean().
lte(VsnA, VsnB) ->
    NVsnA = normalize(parse(VsnA)),
    NVsnB = normalize(parse(VsnB)),
    lt(NVsnA, NVsnB) orelse eql(NVsnA, NVsnB).

%% @doc Test that VsnMatch is greater than or equal to Vsn1 and
%% less than or equal to Vsn2
-spec between(any_version(), any_version(), any_version()) -> boolean().
between(Vsn1, Vsn2, VsnMatch) ->
    NVsnA = normalize(parse(Vsn1)),
    NVsnB = normalize(parse(Vsn2)),
    NVsnMatch = normalize(parse(VsnMatch)),
    gte(NVsnMatch, NVsnA) andalso
        lte(NVsnMatch, NVsnB).

%% @doc check that VsnA is Approximately greater than VsnB
%%
%% Specifying ">= 2.6.5" is an optimistic version constraint. All
%% versions greater than the one specified, including major releases
%% (e.g. 3.0.0) are allowed.
%%
%% Conversely, specifying "~> 2.6" is pessimistic about future major
%% revisions and "~> 2.6.5" is pessimistic about future minor
%% revisions.
%%
%%  "~> 2.6" matches cookbooks >= 2.6.0 AND &lt; 3.0.0
%% "~> 2.6.5" matches cookbooks >= 2.6.5 AND &lt; 2.7.0
pes(VsnA, VsnB) ->
    internal_pes(parse(VsnA), parse(VsnB)).

%%%===================================================================
%%% Friend Functions
%%%===================================================================
%% @doc helper function for the peg grammar to parse the iolist into a semver
-spec internal_parse_version(iolist()) -> semver().
internal_parse_version([MMP, AlphaPart, BuildPart, _]) ->
    {parse_major_minor_patch_minpatch(MMP), {parse_alpha_part(AlphaPart),
                                             parse_alpha_part(BuildPart)}}.

%% @doc helper function for the peg grammar to parse the iolist into a major_minor_patch
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

%% @doc helper function for the peg grammar to parse the iolist into an alpha part
-spec parse_alpha_part(iolist()) -> [alpha_part()].
parse_alpha_part([]) ->
    [];
parse_alpha_part([_, AV1, Rest]) ->
    [erlang:iolist_to_binary(AV1) |
     [format_alpha_part(Part) || Part <- Rest]].

%% @doc according to semver alpha parts that can be treated like
%% numbers must be. We implement that here by taking the alpha part
%% and trying to convert it to a number, if it succeeds we use
%% it. Otherwise we do not.
-spec format_alpha_part(iolist()) -> integer() | binary().
format_alpha_part([<<".">>, AlphaPart]) ->
    Bin = erlang:iolist_to_binary(AlphaPart),
    try
        erlang:list_to_integer(erlang:binary_to_list(Bin))
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
to_list(Detail) when erlang:is_integer(Detail) ->
    erlang:integer_to_list(Detail);
to_list(Detail) when erlang:is_list(Detail); erlang:is_binary(Detail) ->
    Detail.

-spec format_vsn_rest(binary() | string(), [integer() | binary()]) -> iolist().
format_vsn_rest(_TypeMark, []) ->
    [];
format_vsn_rest(TypeMark, [Head | Rest]) ->
    [TypeMark, Head |
     [[".", to_list(Detail)] || Detail <- Rest]].

%% @doc normalize the semver so they can be compared
-spec normalize(semver()) -> semver().
normalize({Vsn, Rest})
  when erlang:is_binary(Vsn);
       erlang:is_integer(Vsn) ->
    {{Vsn, 0, 0, 0}, Rest};
normalize({{Maj, Min}, Rest}) ->
    {{Maj, Min, 0, 0}, Rest};
normalize({{Maj, Min, Patch}, Rest}) ->
    {{Maj, Min, Patch, 0}, Rest};
normalize(Other = {{_, _, _, _}, {_,_}}) ->
    Other.

%% @doc to do the pessimistic compare we need a parsed semver. This is
%% the internal implementation of the of the pessimistic run. The
%% external just ensures that versions are parsed.
-spec internal_pes(semver(), semver()) -> boolean().
internal_pes(VsnA, {{LM, LMI}, _})
  when erlang:is_integer(LM),
       erlang:is_integer(LMI) ->
    gte(VsnA, {{LM, LMI, 0}, {[], []}}) andalso
        lt(VsnA, {{LM + 1, 0, 0, 0}, {[], []}});
internal_pes(VsnA, {{LM, LMI, LP}, _})
    when erlang:is_integer(LM),
         erlang:is_integer(LMI),
         erlang:is_integer(LP) ->
    gte(VsnA, {{LM, LMI, LP}, {[], []}})
        andalso
        lt(VsnA, {{LM, LMI + 1, 0, 0}, {[], []}});
internal_pes(VsnA, {{LM, LMI, LP, LMP}, _})
    when erlang:is_integer(LM),
         erlang:is_integer(LMI),
         erlang:is_integer(LP),
         erlang:is_integer(LMP) ->
    gte(VsnA, {{LM, LMI, LP, LMP}, {[], []}})
        andalso
        lt(VsnA, {{LM, LMI, LP + 1, 0}, {[], []}});
internal_pes(Vsn, LVsn) ->
    gte(Vsn, LVsn).

%%%===================================================================
%%% Test Functions
%%%===================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

eql_test() ->
    ?assertMatch(true, eql("1.0.0-alpha",
                           "1.0.0-alpha")),
    ?assertMatch(true, eql(<<"1.0.0-alpha">>,
                           "1.0.0-alpha")),
    ?assertMatch(true, eql("1.0.0-alpha",
                           <<"1.0.0-alpha">>)),
    ?assertMatch(true, eql(<<"1.0.0-alpha">>,
                           <<"1.0.0-alpha">>)),
    ?assertMatch(true, eql("v1.0.0-alpha",
                           "1.0.0-alpha")),
    ?assertMatch(true, eql("1",
                           "1.0.0")),
    ?assertMatch(true, eql("v1",
                           "v1.0.0")),
    ?assertMatch(true, eql("1.0",
                           "1.0.0")),
    ?assertMatch(true, eql("1.0.0",
                           "1")),
    ?assertMatch(true, eql("1.0.0.0",
                           "1")),
    ?assertMatch(true, eql("1.0+alpha.1",
                           "1.0.0+alpha.1")),
    ?assertMatch(true, eql("1.0-alpha.1+build.1",
                           "1.0.0-alpha.1+build.1")),
    ?assertMatch(true, eql("1.0-alpha.1+build.1",
                           "1.0.0.0-alpha.1+build.1")),
    ?assertMatch(true, eql("1.0-alpha.1+build.1",
                           "v1.0.0.0-alpha.1+build.1")),
    ?assertMatch(true, eql("aa", "aa")),
    ?assertMatch(true, eql("AA.BB", "AA.BB")),
    ?assertMatch(true, eql("BBB-super", "BBB-super")),
    ?assertMatch(true, not eql("1.0.0",
                               "1.0.1")),
    ?assertMatch(true, not eql(<<"1.0.0">>,
                               "1.0.1")),
    ?assertMatch(true, not eql("1.0.0",
                               <<"1.0.1">>)),
    ?assertMatch(true, not eql(<<"1.0.0">>,
                               <<"1.0.1">>)),
    ?assertMatch(true, not eql("1.0.0-alpha",
                               "1.0.1+alpha")),
    ?assertMatch(true, not eql("1.0.0+build.1",
                               "1.0.1+build.2")),
    ?assertMatch(true, not eql("1.0.0.0+build.1",
                               "1.0.0.1+build.2")),
    ?assertMatch(true, not eql("FFF", "BBB")),
    ?assertMatch(true, not eql("1", "1BBBB")).

gt_test() ->
    ?assertMatch(true, gt("1.0.0-alpha.1",
                          "1.0.0-alpha")),
    ?assertMatch(true, gt("1.0.0.1-alpha.1",
                          "1.0.0.1-alpha")),
    ?assertMatch(true, gt("1.0.0.4-alpha.1",
                          "1.0.0.2-alpha")),
    ?assertMatch(true, gt("1.0.0.0-alpha.1",
                          "1.0.0-alpha")),
    ?assertMatch(true, gt("1.0.0-beta.2",
                          "1.0.0-alpha.1")),
    ?assertMatch(true, gt("1.0.0-beta.11",
                          "1.0.0-beta.2")),
    ?assertMatch(true, gt("1.0.0-beta.11",
                          "1.0.0.0-beta.2")),
    ?assertMatch(true, gt("1.0.0-rc.1", "1.0.0-beta.11")),
    ?assertMatch(true, gt("1.0.0-rc.1+build.1", "1.0.0-rc.1")),
    ?assertMatch(true, gt("1.0.0", "1.0.0-rc.1+build.1")),
    ?assertMatch(true, gt("1.0.0+0.3.7", "1.0.0")),
    ?assertMatch(true, gt("1.3.7+build", "1.0.0+0.3.7")),
    ?assertMatch(true, gt("1.3.7+build.2.b8f12d7",
                          "1.3.7+build")),
    ?assertMatch(true, gt("1.3.7+build.2.b8f12d7",
                          "1.3.7.0+build")),
    ?assertMatch(true, gt("1.3.7+build.11.e0f985a",
                          "1.3.7+build.2.b8f12d7")),
    ?assertMatch(true, gt("aa.cc",
                          "aa.bb")),
    ?assertMatch(true, not gt("1.0.0-alpha",
                              "1.0.0-alpha.1")),
    ?assertMatch(true, not gt("1.0.0-alpha",
                              "1.0.0.0-alpha.1")),
    ?assertMatch(true, not gt("1.0.0-alpha.1",
                              "1.0.0-beta.2")),
    ?assertMatch(true, not gt("1.0.0-beta.2",
                              "1.0.0-beta.11")),
    ?assertMatch(true, not gt("1.0.0-beta.11",
                              "1.0.0-rc.1")),
    ?assertMatch(true, not gt("1.0.0-rc.1",
                              "1.0.0-rc.1+build.1")),
    ?assertMatch(true, not gt("1.0.0-rc.1+build.1",
                              "1.0.0")),
    ?assertMatch(true, not gt("1.0.0",
                              "1.0.0+0.3.7")),
    ?assertMatch(true, not gt("1.0.0+0.3.7",
                              "1.3.7+build")),
    ?assertMatch(true, not gt("1.3.7+build",
                              "1.3.7+build.2.b8f12d7")),
    ?assertMatch(true, not gt("1.3.7+build.2.b8f12d7",
                              "1.3.7+build.11.e0f985a")),
    ?assertMatch(true, not gt("1.0.0-alpha",
                              "1.0.0-alpha")),
    ?assertMatch(true, not gt("1",
                              "1.0.0")),
    ?assertMatch(true, not gt("aa.bb",
                              "aa.bb")),
    ?assertMatch(true, not gt("aa.cc",
                              "aa.dd")),
    ?assertMatch(true, not gt("1.0",
                              "1.0.0")),
    ?assertMatch(true, not gt("1.0.0",
                              "1")),
    ?assertMatch(true, not gt("1.0+alpha.1",
                              "1.0.0+alpha.1")),
    ?assertMatch(true, not gt("1.0-alpha.1+build.1",
                              "1.0.0-alpha.1+build.1")).

lt_test() ->
    ?assertMatch(true, lt("1.0.0-alpha",
                          "1.0.0-alpha.1")),
    ?assertMatch(true, lt("1.0.0-alpha",
                          "1.0.0.0-alpha.1")),
    ?assertMatch(true, lt("1.0.0-alpha.1",
                          "1.0.0-beta.2")),
    ?assertMatch(true, lt("1.0.0-beta.2",
                          "1.0.0-beta.11")),
    ?assertMatch(true, lt("1.0.0-beta.11",
                          "1.0.0-rc.1")),
    ?assertMatch(true, lt("1.0.0.1-beta.11",
                          "1.0.0.1-rc.1")),
    ?assertMatch(true, lt("1.0.0-rc.1",
                          "1.0.0-rc.1+build.1")),
    ?assertMatch(true, lt("1.0.0-rc.1+build.1",
                          "1.0.0")),
    ?assertMatch(true, lt("1.0.0",
                          "1.0.0+0.3.7")),
    ?assertMatch(true, lt("1.0.0+0.3.7",
                          "1.3.7+build")),
    ?assertMatch(true, lt("1.3.7+build",
                          "1.3.7+build.2.b8f12d7")),
    ?assertMatch(true, lt("1.3.7+build.2.b8f12d7",
                          "1.3.7+build.11.e0f985a")),
    ?assertMatch(true, not lt("1.0.0-alpha",
                              "1.0.0-alpha")),
    ?assertMatch(true, not lt("1",
                              "1.0.0")),
    ?assertMatch(true, lt("1",
                          "1.0.0.1")),
    ?assertMatch(true, lt("AA.DD",
                          "AA.EE")),
    ?assertMatch(true, not lt("1.0",
                              "1.0.0")),
    ?assertMatch(true, not lt("1.0.0.0",
                              "1")),
    ?assertMatch(true, not lt("1.0+alpha.1",
                              "1.0.0+alpha.1")),
    ?assertMatch(true, not lt("AA.DD", "AA.CC")),
    ?assertMatch(true, not lt("1.0-alpha.1+build.1",
                              "1.0.0-alpha.1+build.1")),
    ?assertMatch(true, not lt("1.0.0-alpha.1",
                              "1.0.0-alpha")),
    ?assertMatch(true, not lt("1.0.0-beta.2",
                              "1.0.0-alpha.1")),
    ?assertMatch(true, not lt("1.0.0-beta.11",
                              "1.0.0-beta.2")),
    ?assertMatch(true, not lt("1.0.0-rc.1", "1.0.0-beta.11")),
    ?assertMatch(true, not lt("1.0.0-rc.1+build.1", "1.0.0-rc.1")),
    ?assertMatch(true, not lt("1.0.0", "1.0.0-rc.1+build.1")),
    ?assertMatch(true, not lt("1.0.0+0.3.7", "1.0.0")),
    ?assertMatch(true, not lt("1.3.7+build", "1.0.0+0.3.7")),
    ?assertMatch(true, not lt("1.3.7+build.2.b8f12d7",
                              "1.3.7+build")),
    ?assertMatch(true, not lt("1.3.7+build.11.e0f985a",
                              "1.3.7+build.2.b8f12d7")).

gte_test() ->
    ?assertMatch(true, gte("1.0.0-alpha",
                           "1.0.0-alpha")),

    ?assertMatch(true, gte("1",
                           "1.0.0")),

    ?assertMatch(true, gte("1.0",
                           "1.0.0")),

    ?assertMatch(true, gte("1.0.0",
                           "1")),

    ?assertMatch(true, gte("1.0.0.0",
                           "1")),

    ?assertMatch(true, gte("1.0+alpha.1",
                           "1.0.0+alpha.1")),

    ?assertMatch(true, gte("1.0-alpha.1+build.1",
                           "1.0.0-alpha.1+build.1")),

    ?assertMatch(true, gte("1.0.0-alpha.1+build.1",
                           "1.0.0.0-alpha.1+build.1")),
    ?assertMatch(true, gte("1.0.0-alpha.1",
                           "1.0.0-alpha")),
    ?assertMatch(true, gte("1.0.0-beta.2",
                           "1.0.0-alpha.1")),
    ?assertMatch(true, gte("1.0.0-beta.11",
                           "1.0.0-beta.2")),
    ?assertMatch(true, gte("aa.bb", "aa.bb")),
    ?assertMatch(true, gte("dd", "aa")),
    ?assertMatch(true, gte("1.0.0-rc.1", "1.0.0-beta.11")),
    ?assertMatch(true, gte("1.0.0-rc.1+build.1", "1.0.0-rc.1")),
    ?assertMatch(true, gte("1.0.0", "1.0.0-rc.1+build.1")),
    ?assertMatch(true, gte("1.0.0+0.3.7", "1.0.0")),
    ?assertMatch(true, gte("1.3.7+build", "1.0.0+0.3.7")),
    ?assertMatch(true, gte("1.3.7+build.2.b8f12d7",
                           "1.3.7+build")),
    ?assertMatch(true, gte("1.3.7+build.11.e0f985a",
                           "1.3.7+build.2.b8f12d7")),
    ?assertMatch(true, not gte("1.0.0-alpha",
                               "1.0.0-alpha.1")),
    ?assertMatch(true, not gte("CC", "DD")),
    ?assertMatch(true, not gte("1.0.0-alpha.1",
                               "1.0.0-beta.2")),
    ?assertMatch(true, not gte("1.0.0-beta.2",
                               "1.0.0-beta.11")),
    ?assertMatch(true, not gte("1.0.0-beta.11",
                               "1.0.0-rc.1")),
    ?assertMatch(true, not gte("1.0.0-rc.1",
                               "1.0.0-rc.1+build.1")),
    ?assertMatch(true, not gte("1.0.0-rc.1+build.1",
                               "1.0.0")),
    ?assertMatch(true, not gte("1.0.0",
                               "1.0.0+0.3.7")),
    ?assertMatch(true, not gte("1.0.0+0.3.7",
                               "1.3.7+build")),
    ?assertMatch(true, not gte("1.0.0",
                               "1.0.0+build.1")),
    ?assertMatch(true, not gte("1.3.7+build",
                               "1.3.7+build.2.b8f12d7")),
    ?assertMatch(true, not gte("1.3.7+build.2.b8f12d7",
                               "1.3.7+build.11.e0f985a")).
lte_test() ->
    ?assertMatch(true, lte("1.0.0-alpha",
                           "1.0.0-alpha.1")),
    ?assertMatch(true, lte("1.0.0-alpha.1",
                           "1.0.0-beta.2")),
    ?assertMatch(true, lte("1.0.0-beta.2",
                           "1.0.0-beta.11")),
    ?assertMatch(true, lte("1.0.0-beta.11",
                           "1.0.0-rc.1")),
    ?assertMatch(true, lte("1.0.0-rc.1",
                           "1.0.0-rc.1+build.1")),
    ?assertMatch(true, lte("1.0.0-rc.1+build.1",
                           "1.0.0")),
    ?assertMatch(true, lte("1.0.0",
                           "1.0.0+0.3.7")),
    ?assertMatch(true, lte("1.0.0+0.3.7",
                           "1.3.7+build")),
    ?assertMatch(true, lte("1.3.7+build",
                           "1.3.7+build.2.b8f12d7")),
    ?assertMatch(true, lte("1.3.7+build.2.b8f12d7",
                           "1.3.7+build.11.e0f985a")),
    ?assertMatch(true, lte("1.0.0-alpha",
                           "1.0.0-alpha")),
    ?assertMatch(true, lte("1",
                           "1.0.0")),
    ?assertMatch(true, lte("1.0",
                           "1.0.0")),
    ?assertMatch(true, lte("1.0.0",
                           "1")),
    ?assertMatch(true, lte("1.0+alpha.1",
                           "1.0.0+alpha.1")),
    ?assertMatch(true, lte("1.0.0.0+alpha.1",
                           "1.0.0+alpha.1")),
    ?assertMatch(true, lte("1.0-alpha.1+build.1",
                           "1.0.0-alpha.1+build.1")),
    ?assertMatch(true, lte("aa","cc")),
    ?assertMatch(true, lte("cc","cc")),
    ?assertMatch(true, not lte("1.0.0-alpha.1",
                              "1.0.0-alpha")),
    ?assertMatch(true, not lte("cc", "aa")),
    ?assertMatch(true, not lte("1.0.0-beta.2",
                              "1.0.0-alpha.1")),
    ?assertMatch(true, not lte("1.0.0-beta.11",
                              "1.0.0-beta.2")),
    ?assertMatch(true, not lte("1.0.0-rc.1", "1.0.0-beta.11")),
    ?assertMatch(true, not lte("1.0.0-rc.1+build.1", "1.0.0-rc.1")),
    ?assertMatch(true, not lte("1.0.0", "1.0.0-rc.1+build.1")),
    ?assertMatch(true, not lte("1.0.0+0.3.7", "1.0.0")),
    ?assertMatch(true, not lte("1.3.7+build", "1.0.0+0.3.7")),
    ?assertMatch(true, not lte("1.3.7+build.2.b8f12d7",
                              "1.3.7+build")),
    ?assertMatch(true, not lte("1.3.7+build.11.e0f985a",
                              "1.3.7+build.2.b8f12d7")).

between_test() ->
    ?assertMatch(true, between("1.0.0-alpha",
                               "1.0.0-alpha.3",
                               "1.0.0-alpha.2")),
    ?assertMatch(true, between("1.0.0-alpha.1",
                               "1.0.0-beta.2",
                               "1.0.0-alpha.25")),
    ?assertMatch(true, between("1.0.0-beta.2",
                               "1.0.0-beta.11",
                               "1.0.0-beta.7")),
    ?assertMatch(true, between("1.0.0-beta.11",
                               "1.0.0-rc.3",
                               "1.0.0-rc.1")),
    ?assertMatch(true, between("1.0.0-rc.1",
                               "1.0.0-rc.1+build.3",
                               "1.0.0-rc.1+build.1")),

    ?assertMatch(true, between("1.0.0.0-rc.1",
                               "1.0.0-rc.1+build.3",
                               "1.0.0-rc.1+build.1")),
    ?assertMatch(true, between("1.0.0-rc.1+build.1",
                               "1.0.0",
                               "1.0.0-rc.33")),
    ?assertMatch(true, between("1.0.0",
                               "1.0.0+0.3.7",
                               "1.0.0+0.2")),
    ?assertMatch(true, between("1.0.0+0.3.7",
                               "1.3.7+build",
                               "1.2")),
    ?assertMatch(true, between("1.3.7+build",
                               "1.3.7+build.2.b8f12d7",
                               "1.3.7+build.1")),
    ?assertMatch(true, between("1.3.7+build.2.b8f12d7",
                               "1.3.7+build.11.e0f985a",
                               "1.3.7+build.10.a36faa")),
    ?assertMatch(true, between("1.0.0-alpha",
                               "1.0.0-alpha",
                               "1.0.0-alpha")),
    ?assertMatch(true, between("1",
                               "1.0.0",
                               "1.0.0")),
    ?assertMatch(true, between("1.0",
                               "1.0.0",
                               "1.0.0")),

    ?assertMatch(true, between("1.0",
                               "1.0.0.0",
                               "1.0.0.0")),
    ?assertMatch(true, between("1.0.0",
                               "1",
                               "1")),
    ?assertMatch(true, between("1.0+alpha.1",
                               "1.0.0+alpha.1",
                               "1.0.0+alpha.1")),
    ?assertMatch(true, between("1.0-alpha.1+build.1",
                               "1.0.0-alpha.1+build.1",
                               "1.0.0-alpha.1+build.1")),
    ?assertMatch(true, between("aaa",
                               "ddd",
                               "cc")),
    ?assertMatch(true, not between("1.0.0-alpha.1",
                                   "1.0.0-alpha.22",
                                   "1.0.0")),
    ?assertMatch(true, not between("1.0.0",
                                   "1.0.0-alpha.1",
                                   "2.0")),
    ?assertMatch(true, not between("1.0.0-beta.1",
                                   "1.0.0-beta.11",
                                   "1.0.0-alpha")),
    ?assertMatch(true, not between("1.0.0-beta.11", "1.0.0-rc.1",
                                   "1.0.0-rc.22")),
    ?assertMatch(true, not between("aaa", "ddd", "zzz")).

pes_test() ->
    ?assertMatch(true, pes("2.6.0", "2.6")),
    ?assertMatch(true, pes("2.7", "2.6")),
    ?assertMatch(true, pes("2.8", "2.6")),
    ?assertMatch(true, pes("2.9", "2.6")),
    ?assertMatch(true, pes("A.B", "A.A")),
    ?assertMatch(true, not pes("3.0.0", "2.6")),
    ?assertMatch(true, not pes("2.5", "2.6")),
    ?assertMatch(true, pes("2.6.5", "2.6.5")),
    ?assertMatch(true, pes("2.6.6", "2.6.5")),
    ?assertMatch(true, pes("2.6.7", "2.6.5")),
    ?assertMatch(true, pes("2.6.8", "2.6.5")),
    ?assertMatch(true, pes("2.6.9", "2.6.5")),
    ?assertMatch(true, pes("2.6.0.9", "2.6.0.5")),
    ?assertMatch(true, not pes("2.7", "2.6.5")),
    ?assertMatch(true, not pes("2.1.7", "2.1.6.5")),
    ?assertMatch(true, not pes("A.A", "A.B")),
    ?assertMatch(true, not pes("2.5", "2.6.5")).

version_format_test() ->
    ?assertEqual(["1", [], []], format({1, {[],[]}})),
    ?assertEqual(["1", ".", "2", ".", "34", [], []], format({{1,2,34},{[],[]}})),
    ?assertEqual(<<"a">>, erlang:iolist_to_binary(format({<<"a">>, {[],[]}}))),
    ?assertEqual(<<"a.b">>, erlang:iolist_to_binary(format({{<<"a">>,<<"b">>}, {[],[]}}))),
    ?assertEqual(<<"1">>, erlang:iolist_to_binary(format({1, {[],[]}}))),
    ?assertEqual(<<"1.2">>, erlang:iolist_to_binary(format({{1,2}, {[],[]}}))),
    ?assertEqual(<<"1.2.2">>, erlang:iolist_to_binary(format({{1,2,2}, {[],[]}}))),
    ?assertEqual(<<"1.99.2">>, erlang:iolist_to_binary(format({{1,99,2}, {[],[]}}))),
    ?assertEqual(<<"1.99.2-alpha">>, erlang:iolist_to_binary(format({{1,99,2}, {[<<"alpha">>],[]}}))),
    ?assertEqual(<<"1.99.2-alpha.1">>, erlang:iolist_to_binary(format({{1,99,2}, {[<<"alpha">>,1], []}}))),
    ?assertEqual(<<"1.99.2+build.1.a36">>,
                 erlang:iolist_to_binary(format({{1,99,2}, {[], [<<"build">>, 1, <<"a36">>]}}))),
    ?assertEqual(<<"1.99.2.44+build.1.a36">>,
                 erlang:iolist_to_binary(format({{1,99,2,44}, {[], [<<"build">>, 1, <<"a36">>]}}))),
    ?assertEqual(<<"1.99.2-alpha.1+build.1.a36">>,
                 erlang:iolist_to_binary(format({{1,99,2}, {[<<"alpha">>, 1], [<<"build">>, 1, <<"a36">>]}}))),
    ?assertEqual(<<"1">>, erlang:iolist_to_binary(format({1, {[],[]}}))).

-endif.
