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

-export([parse_set/4, set/4, clear/3,
         list/0, list/1, list_strict/1, list/2, list_strict/2, list_formatted/1,
         lookup/3, value/3, value/4, info_keys/0]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(ok_or_error_string() :: 'ok' | {'error_string', string()}).

-spec(parse_set/4 :: (rabbit_types:vhost(), binary(), binary(), string())
                     -> ok_or_error_string()).
-spec(set/4 :: (rabbit_types:vhost(), binary(), binary(), term())
               -> ok_or_error_string()).
-spec(clear/3 :: (rabbit_types:vhost(), binary(), binary())
                 -> ok_or_error_string()).
-spec(list/0 :: () -> [rabbit_types:infos()]).
-spec(list/1 :: (rabbit_types:vhost()) -> [rabbit_types:infos()]).
-spec(list_strict/1 :: (binary()) -> [rabbit_types:infos()] | 'not_found').
-spec(list/2 :: (rabbit_types:vhost(), binary()) -> [rabbit_types:infos()]).
-spec(list_strict/2 :: (rabbit_types:vhost(), binary())
                       -> [rabbit_types:infos()] | 'not_found').
-spec(list_formatted/1 :: (rabbit_types:vhost()) -> [rabbit_types:infos()]).
-spec(lookup/3 :: (rabbit_types:vhost(), binary(), binary())
                  -> rabbit_types:infos()).
-spec(value/3 :: (rabbit_types:vhost(), binary(), binary()) -> term()).
-spec(value/4 :: (rabbit_types:vhost(), binary(), binary(), term()) -> term()).
-spec(info_keys/0 :: () -> rabbit_types:info_keys()).

-endif.

%%---------------------------------------------------------------------------

-import(rabbit_misc, [pget/2, pset/3]).

-define(TABLE, rabbit_runtime_parameters).

%%---------------------------------------------------------------------------

parse_set(VHost, Component, Key, String) ->
    case parse(String) of
        {ok, Term}  -> set(VHost, Component, Key, Term);
        {errors, L} -> format_error(L)
    end.

set(VHost, Component, Key, Term) ->
    case set0(VHost, Component, Key, Term) of
        ok          -> ok;
        {errors, L} -> format_error(L)
    end.

format_error(L) ->
    {error_string, rabbit_misc:format_many([{"Validation failed~n", []} | L])}.

set0(VHost, Component, Key, Term) ->
    case lookup_component(Component) of
        {ok, Mod} ->
            case flatten_errors(validate(Term)) of
                ok ->
                    case flatten_errors(
                           Mod:validate(VHost, Component, Key, Term)) of
                        ok ->
                            case mnesia_update(VHost, Component, Key, Term) of
                                {old, Term} -> ok;
                                _           -> Mod:notify(
                                                 VHost, Component, Key, Term)
                            end,
                            ok;
                        E ->
                            E
                    end;
                E ->
                    E
            end;
        E ->
            E
    end.

mnesia_update(VHost, Component, Key, Term) ->
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              Res = case mnesia:read(?TABLE, {VHost, Component, Key}, read) of
                        []       -> new;
                        [Params] -> {old, Params#runtime_parameters.value}
                    end,
              ok = mnesia:write(?TABLE, c(VHost, Component, Key, Term), write),
              Res
      end).

clear(VHost, Component, Key) ->
    case clear0(VHost, Component, Key) of
        ok          -> ok;
        {errors, L} -> format_error(L)
    end.

clear0(VHost, Component, Key) ->
    case lookup_component(Component) of
        {ok, Mod} -> case flatten_errors(
                            Mod:validate_clear(VHost, Component, Key)) of
                         ok -> mnesia_clear(VHost, Component, Key),
                               Mod:notify_clear(VHost, Component, Key),
                               ok;
                         E  -> E
                     end;
        E         -> E
    end.

mnesia_clear(VHost, Component, Key) ->
    ok = rabbit_misc:execute_mnesia_transaction(
           fun () ->
                   ok = mnesia:delete(?TABLE, {VHost, Component, Key}, write)
           end).

list() ->
    [p(P) || P <- rabbit_misc:dirty_read_all(?TABLE)].

list(VHost)                   -> list(VHost, '_', []).
list_strict(Component)        -> list('_',   Component, not_found).
list(VHost, Component)        -> list(VHost, Component, []).
list_strict(VHost, Component) -> list(VHost, Component, not_found).

list(VHost, Component, Default) ->
    case component_good(Component) of
        true -> Match = #runtime_parameters{key = {VHost, Component, '_'},
                                            _ = '_'},
                [p(P) || P <- mnesia:dirty_match_object(?TABLE, Match)];
        _    -> Default
    end.

list_formatted(VHost) ->
    [pset(value, format(pget(value, P)), P) || P <- list(VHost)].

lookup(VHost, Component, Key) ->
    case lookup0(VHost, Component, Key, rabbit_misc:const(not_found)) of
        not_found -> not_found;
        Params    -> p(Params)
    end.

value(VHost, Component, Key) ->
    case lookup0(VHost, Component, Key, rabbit_misc:const(not_found)) of
        not_found -> not_found;
        Params    -> Params#runtime_parameters.value
    end.

value(VHost, Component, Key, Default) ->
    Params = lookup0(VHost, Component, Key,
                     fun () ->
                             lookup_missing(VHost, Component, Key, Default)
                     end),
    Params#runtime_parameters.value.

lookup0(VHost, Component, Key, DefaultFun) ->
    case mnesia:dirty_read(?TABLE, {VHost, Component, Key}) of
        []  -> DefaultFun();
        [R] -> R
    end.

lookup_missing(VHost, Component, Key, Default) ->
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              case mnesia:read(?TABLE, {VHost, Component, Key}, read) of
                  []  -> Record = c(VHost, Component, Key, Default),
                         mnesia:write(?TABLE, Record, write),
                         Record;
                  [R] -> R
              end
      end).

c(VHost, Component, Key, Default) ->
    #runtime_parameters{key = {VHost, Component, Key},
                        value = Default}.

p(#runtime_parameters{key = {VHost, Component, Key}, value = Value}) ->
    [{vhost,     VHost},
     {component, Component},
     {key,       Key},
     {value,     Value}].

info_keys() -> [component, key, value].

%%---------------------------------------------------------------------------

component_good('_')       -> true;
component_good(Component) -> case lookup_component(Component) of
                                 {ok, _} -> true;
                                 _       -> false
                             end.

lookup_component(Component) ->
    case rabbit_registry:lookup_module(
           runtime_parameter, list_to_atom(binary_to_list(Component))) of
        {error, not_found} -> {errors,
                               [{"component ~s not found", [Component]}]};
        {ok, Module}       -> {ok, Module}
    end.

parse(Src0) ->
    Src1 = string:strip(Src0),
    Src = case lists:reverse(Src1) of
              [$. |_] -> Src1;
              _       -> Src1 ++ "."
          end,
    case erl_scan:string(Src) of
        {ok, Scanned, _} ->
            case erl_parse:parse_term(Scanned) of
                {ok, Parsed} ->
                    {ok, Parsed};
                {error, E} ->
                    {errors,
                     [{"Could not parse value: ~s", [format_parse_error(E)]}]}
            end;
        {error, E, _} ->
            {errors, [{"Could not scan value: ~s", [format_parse_error(E)]}]}
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
validate(B) when is_boolean(B)                -> ok;
validate(null)                                -> ok;
validate(A) when is_atom(A)                   -> {error, "atom: ~p", [A]};
validate(N) when is_number(N)                 -> ok;
validate(B) when is_binary(B)                 -> ok;
validate(B) when is_bitstring(B)              -> {error, "bitstring: ~p", [B]}.

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
