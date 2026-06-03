%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_re).

-export([run/2, run/3,
         matches/2, matches/3,
         compile/1, compile/2,
         match_limit/0,
         max_pattern_length/0]).

-define(MATCH_LIMIT, 50_000).
-define(MAX_PATTERN_LENGTH, 1024).

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
run(Subject, Pattern, Opts) ->
    try
        re:run(Subject, Pattern,
               [{match_limit, ?MATCH_LIMIT},
                {match_limit_recursion, ?MATCH_LIMIT}
                | Opts])
    catch
        %% `re:run/3` raises on malformed patterns and other shape errors.
        %% Surface them as a safe error tuple so callers do not need to
        %% wrap every call in `try`.
        error:Reason -> {error, Reason}
    end.

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
    re:compile(Pattern, Opts).
