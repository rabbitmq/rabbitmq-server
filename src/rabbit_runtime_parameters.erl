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
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_runtime_parameters).

-include("rabbit.hrl").

-export([parse/1, set/3, clear/2, list/0, list/1, list_formatted/0, lookup/2,
         value/2, value/3, info_keys/0]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(ok_or_error_string() :: 'ok' | {'error_string', string()}).

-spec(parse/1 :: (string()) -> term()).
-spec(set/3 :: (binary(), binary(), term()) -> ok_or_error_string()).
-spec(clear/2 :: (binary(), binary()) -> ok_or_error_string()).
-spec(list/0 :: () -> [rabbit_types:infos()]).
-spec(list/1 :: (binary()) -> [rabbit_types:infos()]).
-spec(list_formatted/0 :: () -> [rabbit_types:infos()]).
-spec(lookup/2 :: (binary(), binary()) -> rabbit_types:infos()).
-spec(value/2 :: (binary(), binary()) -> term()).
-spec(value/3 :: (binary(), binary(), term()) -> term()).
-spec(info_keys/0 :: () -> rabbit_types:info_keys()).

-endif.

%%---------------------------------------------------------------------------

-import(rabbit_misc, [pget/2, pset/3]).

-define(TABLE, rabbit_runtime_parameters).

%%---------------------------------------------------------------------------

set(AppName, Key, Term) ->
    case set0(AppName, Key, Term) of
        ok          -> ok;
        {errors, L} -> format_error(L)
    end.

format_error(L) ->
    {error_string, rabbit_misc:format_many([{"Validation failed~n", []} | L])}.

set0(AppName, Key, Term) ->
    case lookup_app(AppName) of
        {ok, Mod} -> case flatten_errors(validate(Term)) of
                         ok -> case flatten_errors(
                                      Mod:validate(AppName, Key, Term)) of
                                   ok -> mnesia_update(AppName, Key, Term),
                                         Mod:notify(AppName, Key, Term),
                                         ok;
                                   E  -> E
                               end;
                         E  -> E
                     end;
        E         -> E
    end.

mnesia_update(AppName, Key, Term) ->
    ok = rabbit_misc:execute_mnesia_transaction(
           fun () ->
                   ok = mnesia:write(?TABLE, c(AppName, Key, Term), write)
           end).

clear(AppName, Key) ->
    case clear0(AppName, Key) of
        ok          -> ok;
        {errors, L} -> format_error(L)
    end.

clear0(AppName, Key) ->
    case lookup_app(AppName) of
        {ok, Mod} -> case flatten_errors(Mod:validate_clear(AppName, Key)) of
                         ok -> mnesia_clear(AppName, Key),
                               Mod:notify_clear(AppName, Key),
                               ok;
                         E  -> E
                     end;
        E         -> E
    end.

mnesia_clear(AppName, Key) ->
    ok = rabbit_misc:execute_mnesia_transaction(
           fun () ->
                   ok = mnesia:delete(?TABLE, {AppName, Key}, write)
           end).

list() ->
    [p(P) || P <- rabbit_misc:dirty_read_all(?TABLE)].

list(Name) ->
    [p(P) || P <- mnesia:dirty_match_object(
                    ?TABLE, #runtime_parameters{key = {Name, '_'}, _ = '_'})].

list_formatted() ->
    [pset(value, format(pget(value, P)), P) || P <- list()].

lookup(AppName, Key) ->
    case lookup0(AppName, Key, rabbit_misc:const(not_found)) of
        not_found -> not_found;
        Params    -> p(Params)
    end.

value(AppName, Key) ->
    case lookup0(AppName, Key, rabbit_misc:const(not_found)) of
        not_found -> not_found;
        Params    -> Params#runtime_parameters.value
    end.

value(AppName, Key, Default) ->
    Params = lookup0(AppName, Key,
                     fun () -> lookup_missing(AppName, Key, Default) end),
    Params#runtime_parameters.value.

lookup0(AppName, Key, DefaultFun) ->
    case mnesia:dirty_read(?TABLE, {AppName, Key}) of
        []  -> DefaultFun();
        [R] -> R
    end.

lookup_missing(AppName, Key, Default) ->
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              case mnesia:read(?TABLE, {AppName, Key}) of
                  []  -> mnesia:write(?TABLE, c(AppName, Key, Default), write),
                         Default;
                  [R] -> R
              end
      end).

c(AppName, Key, Default) -> #runtime_parameters{key = {AppName, Key},
                                                value = Default}.

p(#runtime_parameters{key = {AppName, Key}, value = Value}) ->
    [{app_name, AppName},
     {key,      Key},
     {value,    Value}].

info_keys() -> [app_name, key, value].

%%---------------------------------------------------------------------------

lookup_app(App) ->
    case rabbit_registry:lookup_module(
           runtime_parameter, list_to_atom(binary_to_list(App))) of
        {error, not_found} -> {errors, [{"application ~s not found", [App]}]};
        {ok, Module}       -> {ok, Module}
    end.

parse(Src0) ->
    Src = case lists:reverse(Src0) of
              [$. |_] -> Src0;
              _       -> Src0 ++ "."
          end,
    case erl_scan:string(Src) of
        {ok, Scanned, _} ->
            case erl_parse:parse_term(Scanned) of
                {ok, Parsed} ->
                    Parsed;
                {error, E} ->
                    exit({could_not_parse_value, format_parse_error(E)})
            end;
        {error, E, _} ->
            exit({could_not_scan_value, format_parse_error(E)})
    end.

format_parse_error({_Line, Mod, Err}) ->
    lists:flatten(Mod:format_error(Err)).

format(Term) ->
    list_to_binary(rabbit_misc:format("~p", [Term])).

%%---------------------------------------------------------------------------

%% We will want to be able to biject these to JSON. So we have some
%% generic restrictions on what we consider acceptable.
validate(Proplist = [T | _]) when is_tuple(T) -> validate_proplist(Proplist);
validate(L) when is_list(L)                   -> validate_list(L);
validate(T) when is_tuple(T)                  -> {error, "tuple: ~p", [T]};
validate(true)                                -> ok;
validate(false)                               -> ok;
validate(null)                                -> ok;
validate(A) when is_atom(A)                   -> {error, "atom: ~p", [A]};
validate(N) when is_number(N)                 -> ok;
validate(B) when is_binary(B)                 -> ok.

validate_list(L) -> [validate(I) || I <- L].
validate_proplist(L) -> [vp(I) || I <- L].

vp({K, V}) when is_binary(K) -> validate(V);
vp({K, _V})                  -> {error, "bad key: ~p", [K]};
vp(H)                        -> {error, "not two tuple: ~p", [H]}.

flatten_errors(L) ->
    case [{F, A} || I <- lists:flatten([L]), {error, F, A} <- [I]] of
        [] -> ok;
        E  -> {errors, E}
    end.
