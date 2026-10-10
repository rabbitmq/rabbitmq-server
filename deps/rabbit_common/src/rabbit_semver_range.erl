%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_semver_range).

-export([parse/1,
         matches/2]).

-export_type([version/0,
              op/0,
              comparator/0,
              range/0]).

-type component() :: non_neg_integer().
-type version() :: {component(), component(), component(), component()}.
-type op() :: gte | lt.
-type comparator() :: {op(), version()}.
-type range() :: [[comparator()]].

-type partial() :: {component() | any, component() | any, component() | any}.
-type prefix() :: none | eq | tilde | pessimistic | caret | gt | gte | lt | lte.

-define(PREFIXES, [{"~>", pessimistic}, {">=", gte}, {"<=", lte},
                   {">", gt}, {"<", lt}, {"=", eq}, {"~", tilde}, {"^", caret}]).

-spec matches(range(), version()) -> boolean().
matches(Range, Version) ->
    lists:any(fun(Conjunction) ->
                      lists:all(fun(Comparator) -> compare(Comparator, Version) end,
                                Conjunction)
              end, Range).

-spec compare(comparator(), version()) -> boolean().
compare({gte, V}, Version) -> Version >= V;
compare({lt, V}, Version) -> Version < V.

-spec parse(string() | binary()) ->
    {ok, range()} | {error, {invalid_range, string()}}.
parse(Expression) ->
    Str = to_string(Expression),
    try
        Alternatives = string:split(Str, "||", all),
        {ok, [parse_conjunction(string:lexemes(Alt, " \t")) || Alt <- Alternatives]}
    catch
        throw:invalid ->
            {error, {invalid_range, Str}}
    end.

-spec to_string(string() | binary()) -> string().
to_string(Bin) when is_binary(Bin) ->
    binary_to_list(Bin);
to_string(Str) ->
    Str.

-spec parse_conjunction([string()]) -> [comparator()].
parse_conjunction([]) ->
    throw(invalid);
parse_conjunction(Tokens) ->
    lists:append(parse_tokens(Tokens)).

-spec parse_tokens([string()]) -> [[comparator()]].
parse_tokens([]) ->
    [];
parse_tokens([Lower, "-", Upper | Rest]) ->
    [hyphen(parse_partial(Lower), parse_partial(Upper)) | parse_tokens(Rest)];
parse_tokens([Token | Rest]) ->
    case split_prefix(Token) of
        {Prefix, ""} when Prefix =/= none ->
            case Rest of
                [Version | Rest1] ->
                    [desugar(Prefix, parse_partial(Version)) | parse_tokens(Rest1)];
                [] ->
                    throw(invalid)
            end;
        {none, Version} ->
            [desugar(none, parse_partial(strip_suffix(Version))) | parse_tokens(Rest)];
        {Prefix, Version} ->
            [desugar(Prefix, parse_partial(Version)) | parse_tokens(Rest)]
    end.

%% Packaging uses `PROJECT_VERSION` as the requirement of built-in plugins,
%% and it can carry a pre-release or build suffix.
-spec strip_suffix(string()) -> string().
strip_suffix(Version) ->
    case re:run(Version, "^([0-9]+\\.[0-9]+\\.[0-9]+)(-[A-Za-z][0-9A-Za-z-]*(\\.[0-9A-Za-z-]+)*)?(\\+[0-9A-Za-z-]+(\\.[0-9A-Za-z-]+)*)?$",
                [{capture, [1], list}]) of
        {match, [Base]} -> Base;
        nomatch -> Version
    end.

-spec split_prefix(string()) -> {prefix(), string()}.
split_prefix(Token) ->
    split_prefix(Token, ?PREFIXES).

split_prefix(Token, []) ->
    {none, Token};
split_prefix(Token, [{Str, Prefix} | Rest]) ->
    case lists:prefix(Str, Token) of
        true -> {Prefix, lists:nthtail(length(Str), Token)};
        false -> split_prefix(Token, Rest)
    end.

-spec parse_partial(string()) -> partial().
parse_partial(Str) ->
    Partial = case [parse_component(C) || C <- string:split(Str, ".", all)] of
                  [Maj] -> {Maj, any, any};
                  [Maj, Min] -> {Maj, Min, any};
                  [Maj, Min, Patch] -> {Maj, Min, Patch};
                  _ -> throw(invalid)
              end,
    case Partial of
        {any, Min1, Patch1} when Min1 =/= any; Patch1 =/= any -> throw(invalid);
        {_, any, Patch2} when Patch2 =/= any -> throw(invalid);
        _ -> Partial
    end.

-spec parse_component(string()) -> component() | any.
parse_component(C) when C =:= "x"; C =:= "X"; C =:= "*" ->
    any;
parse_component(C) ->
    case re:run(C, "^[0-9]+$", [{capture, none}]) of
        match -> list_to_integer(C);
        nomatch -> throw(invalid)
    end.

-spec desugar(prefix(), partial()) -> [comparator()].
desugar(Prefix, {any, any, any}) when Prefix =:= gt; Prefix =:= lt ->
    throw(invalid);
desugar(_, {any, any, any}) ->
    [];
desugar(Prefix, Partial) when Prefix =:= none; Prefix =:= eq ->
    [{gte, lower(Partial)}, {lt, next(Partial)}];
desugar(gt, Partial) ->
    [{gte, next(Partial)}];
desugar(gte, Partial) ->
    [{gte, lower(Partial)}];
desugar(lt, Partial) ->
    [{lt, lower(Partial)}];
desugar(lte, Partial) ->
    [{lt, next(Partial)}];
desugar(tilde, {Maj, any, any}) ->
    [{gte, {Maj, 0, 0, 0}}, {lt, {Maj + 1, 0, 0, 0}}];
desugar(tilde, {Maj, Min, _} = Partial) ->
    [{gte, lower(Partial)}, {lt, {Maj, Min + 1, 0, 0}}];
desugar(pessimistic, {Maj, Min, any} = Partial) when Min =/= any ->
    [{gte, lower(Partial)}, {lt, {Maj + 1, 0, 0, 0}}];
desugar(pessimistic, Partial) ->
    desugar(tilde, Partial);
desugar(caret, {Maj, Min, Patch} = Partial) ->
    Upper = case {Maj, Min, Patch} of
                {0, 0, P} when P =/= any -> {0, 0, P + 1, 0};
                {0, M, _} when M =/= any -> {0, M + 1, 0, 0};
                _ -> {Maj + 1, 0, 0, 0}
            end,
    [{gte, lower(Partial)}, {lt, Upper}].

-spec hyphen(partial(), partial()) -> [comparator()].
hyphen(Lower, Upper) ->
    desugar(gte, Lower) ++ desugar(lte, Upper).

-spec lower(partial()) -> version().
lower({Maj, Min, Patch}) ->
    {Maj, zero(Min), zero(Patch), 0}.

-spec next(partial()) -> version().
next({Maj, any, any}) -> {Maj + 1, 0, 0, 0};
next({Maj, Min, any}) -> {Maj, Min + 1, 0, 0};
next({Maj, Min, Patch}) -> {Maj, Min, Patch + 1, 0}.

zero(any) -> 0;
zero(N) -> N.
