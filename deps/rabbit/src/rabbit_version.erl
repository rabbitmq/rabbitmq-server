%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_version).

-export([recorded/0, matches/2, desired/0, desired_for_scope/1,
         record_desired/0, record_desired_for_scope/1,
         upgrades_required/1, all_upgrades_required/1,
         check_version_consistency/3,
         check_version_consistency/4, check_otp_consistency/1,
         version_error/3]).

%% -------------------------------------------------------------------

-export_type([scope/0, step/0]).

-type scope() :: atom().
-type scope_version() :: [atom()].
-type step() :: {atom(), atom()}.

-type version() :: [atom()].

%% -------------------------------------------------------------------

-define(VERSION_FILENAME, "schema_version").
-define(SCOPES, [mnesia, local]).

%% -------------------------------------------------------------------

-spec recorded() -> rabbit_types:ok_or_error2(version(), any()).

recorded() -> case rabbit_file:read_term_file(schema_filename()) of
                  {ok, [V]}        -> {ok, V};
                  {error, _} = Err -> Err
              end.

record(V) -> ok = rabbit_file:write_term_file(schema_filename(), [V]).

record_for_scope(Scope, ScopeVersion) ->
    case recorded() of
        {error, _} = Err ->
            Err;
        {ok, Version} ->
            Version1 = lists:keystore(Scope, 1, categorise_by_scope(Version),
                                      {Scope, ScopeVersion}),
            ok = record([Name || {_Scope, Names} <- Version1, Name <- Names])
    end.

%% -------------------------------------------------------------------

-spec matches([A], [A]) -> boolean().

matches(VerA, VerB) ->
    lists:usort(VerA) =:= lists:usort(VerB).

%% -------------------------------------------------------------------

-spec desired() -> version().

desired() -> [Name || Scope <- ?SCOPES, Name <- desired_for_scope(Scope)].

-spec desired_for_scope(scope()) -> scope_version().

desired_for_scope(_Scope) -> [].

-spec record_desired() -> 'ok'.

record_desired() -> record(desired()).

-spec record_desired_for_scope
        (scope()) -> rabbit_types:ok_or_error(any()).

record_desired_for_scope(Scope) ->
    record_for_scope(Scope, desired_for_scope(Scope)).

-spec upgrades_required
        (scope()) -> rabbit_types:ok_or_error2([step()], any()).

upgrades_required(_Scope) ->
    {ok, []}.

all_upgrades_required(Scopes) ->
    case recorded() of
        {error, enoent} ->
            case filelib:is_file(rabbit_guid:filename()) of
                false -> {error, starting_from_scratch};
                true  -> {error, version_not_available}
            end;
        {ok, _} ->
            lists:foldl(
                fun
                (_, {error, Err}) -> {error, Err};
                (Scope, {ok, Acc}) ->
                    case upgrades_required(Scope) of
                        %% Lift errors from any scope.
                        {error, Err}   -> {error, Err};
                        %% Filter non-upgradable scopes
                        {ok, []}       -> {ok, Acc};
                        {ok, Upgrades} -> {ok, [{Scope, Upgrades} | Acc]}
                    end
                end,
                {ok, []},
                Scopes)
    end.

%% -------------------------------------------------------------------

categorise_by_scope(Version) when is_list(Version) ->
    [].

dir() -> rabbit_mnesia:dir().

schema_filename() -> filename:join(dir(), ?VERSION_FILENAME).

%% --------------------------------------------------------------------

-spec check_version_consistency
        (string(), string(), string()) -> rabbit_types:ok_or_error(any()).

check_version_consistency(This, Remote, Name) ->
    check_version_consistency(This, Remote, Name, fun (A, B) -> A =:= B end).

-spec check_version_consistency
        (string(), string(), string(),
         fun((string(), string()) -> boolean())) ->
    rabbit_types:ok_or_error(any()).

check_version_consistency(This, Remote, Name, Comp) ->
    case Comp(This, Remote) of
        true  -> ok;
        false -> version_error(Name, This, Remote)
    end.

version_error(Name, This, Remote) ->
    {error, {inconsistent_cluster,
             rabbit_misc:format("~s version mismatch: local node is ~s, "
                                "remote node ~s", [Name, This, Remote])}}.

-spec check_otp_consistency
        (string()) -> rabbit_types:ok_or_error(any()).

check_otp_consistency(Remote) ->
    check_version_consistency(rabbit_misc:otp_release(), Remote, "OTP").
