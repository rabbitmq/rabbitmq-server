%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
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

recorded_for_scope(Scope) ->
    case recorded() of
        {error, _} = Err ->
            Err;
        {ok, Version} ->
            {ok, case lists:keysearch(Scope, 1, categorise_by_scope(Version)) of
                     false                 -> [];
                     {value, {Scope, SV1}} -> SV1
                 end}
    end.

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

desired_for_scope(Scope) -> with_upgrade_graph(fun heads/1, Scope).

-spec record_desired() -> 'ok'.

record_desired() -> record(desired()).

-spec record_desired_for_scope
        (scope()) -> rabbit_types:ok_or_error(any()).

record_desired_for_scope(Scope) ->
    record_for_scope(Scope, desired_for_scope(Scope)).

-spec upgrades_required
        (scope()) -> rabbit_types:ok_or_error2([step()], any()).

upgrades_required(Scope) ->
    case recorded_for_scope(Scope) of
        {error, enoent} ->
            case filelib:is_file(rabbit_guid:filename()) of
                false -> {error, starting_from_scratch};
                true  -> {error, version_not_available}
            end;
        {ok, CurrentHeads} ->
            with_upgrade_graph(
              fun (G) ->
                      case unknown_heads(CurrentHeads, G) of
                          []      -> {ok, upgrades_to_apply(CurrentHeads, G)};
                          Unknown -> {error, {future_upgrades_found, Unknown}}
                      end
              end, Scope)
    end.

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

with_upgrade_graph(Fun, Scope) ->
    case rabbit_misc:build_acyclic_graph(
           fun ({_App, Module, Steps}) -> vertices(Module, Steps, Scope) end,
           fun ({_App, Module, Steps}) -> edges(Module, Steps, Scope) end,
           rabbit_misc:all_module_attributes(rabbit_upgrade)) of
        {ok, G} -> try
                       Fun(G)
                   after
                       true = digraph:delete(G)
                   end;
        {error, {vertex, duplicate, StepName}} ->
            throw({error, {duplicate_upgrade_step, StepName}});
        {error, {edge, {bad_vertex, StepName}, _From, _To}} ->
            throw({error, {dependency_on_unknown_upgrade_step, StepName}});
        {error, {edge, {bad_edge, StepNames}, _From, _To}} ->
            throw({error, {cycle_in_upgrade_steps, StepNames}})
    end.

vertices(Module, Steps, Scope0) ->
    [{StepName, {Module, StepName}} || {StepName, Scope1, _Reqs} <- Steps,
                                       Scope0 == Scope1].

edges(_Module, Steps, Scope0) ->
    [{Require, StepName} || {StepName, Scope1, Requires} <- Steps,
                            Require <- Requires,
                            Scope0 == Scope1].
unknown_heads(Heads, G) ->
    [H || H <- Heads, digraph:vertex(G, H) =:= false].

upgrades_to_apply(Heads, G) ->
    %% Take all the vertices which can reach the known heads. That's
    %% everything we've already applied. Subtract that from all
    %% vertices: that's what we have to apply.
    Unsorted = sets:to_list(
                 sets:subtract(
                   sets:from_list(digraph:vertices(G)),
                   sets:from_list(digraph_utils:reaching(Heads, G)))),
    %% Form a subgraph from that list and find a topological ordering
    %% so we can invoke them in order.
    [element(2, digraph:vertex(G, StepName)) ||
        StepName <- digraph_utils:topsort(digraph_utils:subgraph(G, Unsorted))].

heads(G) ->
    lists:sort([V || V <- digraph:vertices(G), digraph:out_degree(G, V) =:= 0]).

%% -------------------------------------------------------------------

categorise_by_scope(Version) when is_list(Version) ->
    Categorised =
        [{Scope, Name} || {_App, _Module, Attributes} <-
                              rabbit_misc:all_module_attributes(rabbit_upgrade),
                          {Name, Scope, _Requires} <- Attributes,
                          lists:member(Name, Version)],
    maps:to_list(
      lists:foldl(fun ({Scope, Name}, CatVersion) ->
                          rabbit_misc:maps_cons(Scope, Name, CatVersion)
                  end, maps:new(), Categorised)).

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
