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

-export([read/0, write/1, with_scope_version/3, '=~='/2]).

%% -------------------------------------------------------------------
-ifdef(use_specs).

-export_type([step/0, version/0, scope/0]).

-type(scope() :: atom()).
-type(step() :: atom()).
-type(version() :: [{scope(), [step()]}]).

-spec(read/0 :: () -> rabbit_types:ok_or_error2(version(), any())).
-spec(write/1 :: (version()) -> 'ok').
-spec(with_scope_version/3 ::
        (scope(),
         fun (({'error', any()}) -> E),
         fun (([step()]) -> {[step()], A})) -> E | A).
-spec('=~='/2 :: (version(), version()) -> boolean()).

-endif.
%% -------------------------------------------------------------------

-define(VERSION_FILENAME, "schema_version").

%% -------------------------------------------------------------------

read() ->
    case rabbit_misc:read_term_file(schema_filename()) of
        {ok, [V]}        -> {ok, categorise_by_scope(V)};
        {error, _} = Err -> Err
    end.

write(Version) ->
    V = [Name || {_Scope, Names} <- Version, Name <- Names],
    ok = rabbit_misc:write_term_file(schema_filename(), [V]).

with_scope_version(Scope, ErrorHandler, Fun) ->
    case read() of
        {error, _} = Err ->
            ErrorHandler(Err);
        {ok, Version} ->
            SV = case lists:keysearch(Scope, 1, Version) of
                     false                 -> [];
                     {value, {Scope, SV1}} -> SV1
                 end,
            {SV2, Result} = Fun(SV),
            ok = case SV =:= SV2 of
                     true  -> ok;
                     false -> write(lists:keystore(Scope, 1, Version,
                                                   {Scope, SV2}))
                 end,
            Result
    end.

'=~='(VerA, VerB) ->
    matches(lists:usort(VerA), lists:usort(VerB)).

%% -------------------------------------------------------------------

matches([], []) ->
    true;
matches([{Scope, SV}|VerA], [{Scope, SV}|VerB]) ->
    matches(VerA, VerB);
matches([{Scope, SVA}|VerA], [{Scope, SVB}|VerB]) ->
    case {lists:usort(SVA), lists:usort(SVB)} of
        {SV, SV} -> matches(VerA, VerB);
        _        -> false
    end;
matches(_VerA, _VerB) ->
    false.

categorise_by_scope(Heads) when is_list(Heads) ->
    Categorised =
        [{Scope, Name} || {_Module, Attributes} <-
                              rabbit_misc:all_module_attributes(rabbit_upgrade),
                          {Name, Scope, _Requires} <- Attributes,
                          lists:member(Name, Heads)],
    orddict:to_list(
      lists:foldl(fun ({Scope, Name}, Version) ->
                          rabbit_misc:orddict_cons(Scope, Name, Version)
                  end, orddict:new(), Categorised)).

dir() -> rabbit_mnesia:dir().

schema_filename() -> filename:join(dir(), ?VERSION_FILENAME).
