%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_re).

-export([run/2, run/3,
         matches/2, matches/3,
         escape/1,
         compile/1, compile/2,
         match_limit/0,
         max_pattern_length/0]).

-define(MATCH_LIMIT, 50_000).
-define(MAX_PATTERN_LENGTH, 4096).
%% For the backtracking frames in use. PCRE2 doubles their vector as it grows,
%% so it may allocate about three times as much while copying it.
-define(MAX_MATCH_MEMORY, 8 * 1024 * 1024).

-spec match_limit() -> pos_integer().
match_limit() -> ?MATCH_LIMIT.

-spec max_pattern_length() -> pos_integer().
max_pattern_length() -> ?MAX_PATTERN_LENGTH.

-spec run(iodata() | unicode:charlist(), re:mp() | iodata()) ->
    match | nomatch | {error, term()}.
run(Subject, Pattern) ->
    run(Subject, Pattern, [{capture, none}]).

-spec run(iodata() | unicode:charlist(),
          re:mp() | iodata(),
          re:options()) ->
    {match, [{integer(), integer()} | binary() | string()]}
  | match
  | nomatch
  | {error, term()}.
run(Subject, MP, Opts) when element(1, MP) =:= re_pattern ->
    %% The second element of a compiled pattern is its number of captures.
    run1(Subject, MP, element(2, MP), Opts);
run(Subject, Pattern, Opts) ->
    case to_binary(Pattern, Opts) of
        error ->
            {error, badarg};
        Bin ->
            case sets_heap_limit(Bin) of
                true ->
                    {error, heap_limit_not_allowed};
                false ->
                    %% Every capture group starts with a parenthesis.
                    run1(Subject, Bin, length(binary:matches(Bin, <<"(">>)), Opts)
            end
    end.

run1(Subject, Pattern, Captures, Opts) ->
    try
        re:run(Subject, Pattern,
               [{match_limit, match_steps(Opts)},
                {match_limit_recursion, match_depth(Captures, Opts)}
                | [Opt || Opt <- Opts, not is_match_limit_option(Opt)]])
    catch
        %% `re:run/3` raises on malformed patterns and other shape errors.
        %% Surface them as a safe error tuple so callers do not need to
        %% wrap every call in `try`.
        error:Reason -> {error, Reason}
    end.

%% Each level of depth keeps a copy of the offsets of every capture group.
match_depth(Captures, Opts) ->
    Depth = lists:last([?MATCH_LIMIT | [D || {match_limit_recursion, D} <- Opts,
                                             is_integer(D)]]),
    min(min(Depth, ?MATCH_LIMIT), ?MAX_MATCH_MEMORY div (128 + 16 * (Captures + 1))).

%% Callers can lower the limit but not raise it.
match_steps(Opts) ->
    lists:min([?MATCH_LIMIT | [N || {match_limit, N} <- Opts, is_integer(N)]]).

is_match_limit_option({match_limit, N})           -> is_integer(N);
is_match_limit_option({match_limit_recursion, D}) -> is_integer(D);
is_match_limit_option(_)                          -> false.

to_binary(Pattern, _Opts) when is_binary(Pattern) ->
    Pattern;
to_binary(Pattern, Opts) ->
    try
        case lists:member(unicode, Opts) of
            true  -> unicode:characters_to_binary(Pattern);
            false -> iolist_to_binary(Pattern)
        end
    of
        Bin when is_binary(Bin) -> Bin;
        _                       -> error
    catch
        error:badarg -> error
    end.

sets_heap_limit(Bin) when is_binary(Bin) ->
    binary:match(Bin, <<"(*LIMIT_HEAP=">>) =/= nomatch;
sets_heap_limit(error) ->
    false.

-spec matches(iodata() | unicode:charlist(), re:mp() | iodata()) -> boolean().
matches(Subject, Pattern) ->
    matches(Subject, Pattern, []).

-spec matches(iodata() | unicode:charlist(),
              re:mp() | iodata(),
              re:options()) -> boolean().
matches(Subject, Pattern, Opts) ->
    %% `{capture, none}` must come last because `re:run/3` resolves
    %% repeated capture options with last-wins precedence.
    case run(Subject, Pattern, Opts ++ [{capture, none}]) of
        match -> true;
        _     -> false
    end.

%% Escape every regex metacharacter in a binary so it matches literally, for
%% embedding untrusted values (topic-permission variables, OAuth2 scopes) in a
%% pattern without letting them alter its meaning.
-spec escape(binary()) -> binary().
escape(Bin) when is_binary(Bin) ->
    << <<(escape_char(C))/binary>> || <<C>> <= Bin >>.

%% "-" is only special inside a "[...]" character class, where it forms a
%% range, so it must be escaped alongside the metacharacters that are special
%% outside a class.
-spec escape_char(byte()) -> binary().
escape_char(C)
  when C =:= $\\; C =:= $^; C =:= $$; C =:= $.;
       C =:= $|; C =:= $?; C =:= $*; C =:= $+;
       C =:= $(; C =:= $); C =:= $[; C =:= $];
       C =:= ${; C =:= $}; C =:= $- ->
    <<$\\, C>>;
escape_char(C) ->
    <<C>>.

-spec compile(iodata()) -> {ok, re:mp()} | {error, term()}.
compile(Pattern) ->
    compile(Pattern, []).

-spec compile(iodata(), re:compile_options()) ->
    {ok, re:mp()} | {error, term()}.
compile(Pattern, _Opts) when is_binary(Pattern),
                             byte_size(Pattern) > ?MAX_PATTERN_LENGTH ->
    {error, pattern_too_long};
compile(Pattern, _Opts) when is_list(Pattern),
                             length(Pattern) > ?MAX_PATTERN_LENGTH ->
    {error, pattern_too_long};
compile(Pattern, Opts) ->
    case sets_heap_limit(to_binary(Pattern, Opts)) of
        true  -> {error, heap_limit_not_allowed};
        false -> re:compile(Pattern, Opts)
    end.
