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

-export([set/3, clear/2, list/0, lookup/3, info_keys/0]).

-define(TABLE, rabbit_runtime_parameters).

%%---------------------------------------------------------------------------

set(AppName, Key, Value) ->
    Module = lookup_app(AppName),
    Term = parse(Value),
    validate(Term),
    Module:validate(Key, Term),
    ok = rabbit_misc:execute_mnesia_transaction(
           fun () ->
                   ok = mnesia:write(?TABLE, c(AppName, Key, Term), write)
           end),
    Module:notify(Key, Term),
    ok.

clear(AppName, Key) ->
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              ok = mnesia:delete(?TABLE, {AppName, Key}, write)
      end).

list() ->
    All = rabbit_misc:dirty_read_all(?TABLE),
    [[{app_name, AppName},
      {key,      Key},
      {value,    format(Value)}] || #runtime_parameters{key = {AppName, Key},
                                                        value = Value} <- All].

lookup(AppName, Key, Default) ->
    case mnesia:dirty_read(?TABLE, {AppName, Key}) of
        []  -> lookup_missing(AppName, Key, Default);
        [R] -> R#runtime_parameters.value
    end.

lookup_missing(AppName, Key, Default) ->
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              case mnesia:read(?TABLE, {AppName, Key}) of
                  []  -> mnesia:write(?TABLE, c(AppName, Key, Default), write),
                         Default;
                  [R] -> R#runtime_parameters.value
              end
      end).

c(AppName, Key, Default) -> #runtime_parameters{key = {AppName, Key},
                                                value = Default}.

info_keys() -> [app_name, key, value].

%%---------------------------------------------------------------------------

lookup_app(App) ->
    case rabbit_registry:lookup_module(runtime_parameter, App) of
        {error, not_found} -> exit({application_not_found, App});
        {ok, Module}       -> Module
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
validate(L) when is_list(L)                   -> [validate(I) || I <- L];
validate(T) when is_tuple(T)                  -> exit({tuple, T});
validate(true)                                -> ok;
validate(false)                               -> ok;
validate(null)                                -> ok;
validate(A) when is_atom(A)                   -> exit({non_bool_atom, A});
validate(N) when is_number(N)                 -> ok;
validate(B) when is_binary(B)                 -> ok.

validate_proplist([])                              -> ok;
validate_proplist([{K, V} | Rest]) when is_atom(K) -> validate(V),
                                                      validate_proplist(Rest);
validate_proplist([{K, V} | Rest]) when is_list(K) -> validate(V),
                                                      validate_proplist(Rest);
validate_proplist([{K, _V} | _Rest])               -> exit({bad_key, K});
validate_proplist([H | _Rest])                     -> exit({not_two_tuple, H}).
