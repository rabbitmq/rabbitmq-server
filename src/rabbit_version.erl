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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_version).

-export([read/0, '=~='/2, desired_version/0, desired_scope_version/1,
         write_desired_version/0, write_desired_scope_version/1,
         upgrades_required/1]).

%% -------------------------------------------------------------------
-ifdef(use_specs).

-export_type([scope/0, step/0, scope_version/0]).

-type(scope() :: atom()).
-type(scope_version() :: [atom()]).
-type(step() :: {atom(), atom()}).

-type(version() :: [atom()]).

-spec(read/0 :: () -> rabbit_types:ok_or_error2(version(), any())).
-spec('=~='/2 :: (version(), version()) -> boolean()).
-spec(desired_version/0 :: () -> version()).
-spec(desired_scope_version/1 :: (scope()) -> scope_version()).
-spec(write_desired_version/0 :: () -> 'ok').
-spec(write_desired_scope_version/1 ::
        (scope()) -> rabbit_types:ok_or_error(any())).
-spec(upgrades_required/1 ::
        (scope()) -> rabbit_types:ok_or_error2([step()], any())).

-endif.
%% -------------------------------------------------------------------

-define(VERSION_FILENAME, "schema_version").
-define(SCOPES, [mnesia, local]).

%% -------------------------------------------------------------------

read() -> case rabbit_misc:read_term_file(schema_filename()) of
              {ok, [V]}        -> {ok, V};
              {error, _} = Err -> Err
          end.

write(V) -> ok = rabbit_misc:write_term_file(schema_filename(), [V]).

read_scope_version(Scope) ->
    case read() of
        {error, _} = Err ->
            Err;
        {ok, Version} ->
            {ok, case lists:keysearch(Scope, 1, categorise_by_scope(Version)) of
                     false                 -> [];
                     {value, {Scope, SV1}} -> SV1
                 end}
    end.

write_scope_version(Scope, ScopeVersion) ->
    case read() of
        {error, _} = Err ->
            Err;
        {ok, Version} ->
            Version1 = lists:keystore(Scope, 1, categorise_by_scope(Version),
                                      {Scope, ScopeVersion}),
            ok = write([Name || {_Scope, Names} <- Version1, Name <- Names])
    end.

%% -------------------------------------------------------------------

'=~='(VerA, VerB) ->
    lists:usort(VerA) =:= lists:usort(VerB).

%% -------------------------------------------------------------------

desired_version() ->
    [Name || Scope <- ?SCOPES, Name <- desired_scope_version(Scope)].

desired_scope_version(Scope) -> with_upgrade_graph(fun heads/1, Scope).

write_desired_version() -> write(desired_version()).

write_desired_scope_version(Scope) ->
    write_scope_version(Scope, desired_scope_version(Scope)).

upgrades_required(Scope) ->
    case read_scope_version(Scope) of
        {error, enoent} ->
            {error, version_not_available};
        {ok, CurrentHeads} ->
            with_upgrade_graph(
              fun (G) ->
                      case unknown_heads(CurrentHeads, G) of
                          []      -> {ok, upgrades_to_apply(CurrentHeads, G)};
                          Unknown -> {error, {future_upgrades_found, Unknown}}
                      end
              end, Scope)
    end.

%% -------------------------------------------------------------------

with_upgrade_graph(Fun, Scope) ->
    case rabbit_misc:build_acyclic_graph(
           fun (Module, Steps) -> vertices(Module, Steps, Scope) end,
           fun (Module, Steps) -> edges(Module, Steps, Scope) end,
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
        [{Scope, Name} || {_Module, Attributes} <-
                              rabbit_misc:all_module_attributes(rabbit_upgrade),
                          {Name, Scope, _Requires} <- Attributes,
                          lists:member(Name, Version)],
    orddict:to_list(
      lists:foldl(fun ({Scope, Name}, CatVersion) ->
                          rabbit_misc:orddict_cons(Scope, Name, CatVersion)
                  end, orddict:new(), Categorised)).

dir() -> rabbit_mnesia:dir().

schema_filename() -> filename:join(dir(), ?VERSION_FILENAME).
