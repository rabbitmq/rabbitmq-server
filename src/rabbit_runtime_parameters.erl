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
%% Copyright (c) 2007-2013 VMware, Inc.  All rights reserved.
%%

-module(rabbit_runtime_parameters).

-include("rabbit.hrl").

-export([parse_set/4, set/4, set_any/4, clear/3, clear_any/3, list/0, list/1,
         list_component/1, list/2, list_formatted/1, lookup/3,
         value/3, value/4, info_keys/0]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(ok_or_error_string() :: 'ok' | {'error_string', string()}).

-spec(parse_set/4 :: (rabbit_types:vhost(), binary(), binary(), string())
                     -> ok_or_error_string()).
-spec(set/4 :: (rabbit_types:vhost(), binary(), binary(), term())
               -> ok_or_error_string()).
-spec(set_any/4 :: (rabbit_types:vhost(), binary(), binary(), term())
                   -> ok_or_error_string()).
-spec(clear/3 :: (rabbit_types:vhost(), binary(), binary())
                 -> ok_or_error_string()).
-spec(clear_any/3 :: (rabbit_types:vhost(), binary(), binary())
                     -> ok_or_error_string()).
-spec(list/0 :: () -> [rabbit_types:infos()]).
-spec(list/1 :: (rabbit_types:vhost() | '_') -> [rabbit_types:infos()]).
-spec(list_component/1 :: (binary()) -> [rabbit_types:infos()]).
-spec(list/2 :: (rabbit_types:vhost() | '_', binary() | '_')
                -> [rabbit_types:infos()]).
-spec(list_formatted/1 :: (rabbit_types:vhost()) -> [rabbit_types:infos()]).
-spec(lookup/3 :: (rabbit_types:vhost(), binary(), binary())
                  -> rabbit_types:infos() | 'not_found').
-spec(value/3 :: (rabbit_types:vhost(), binary(), binary()) -> term()).
-spec(value/4 :: (rabbit_types:vhost(), binary(), binary(), term()) -> term()).
-spec(info_keys/0 :: () -> rabbit_types:info_keys()).

-endif.

%%---------------------------------------------------------------------------

-import(rabbit_misc, [pget/2, pset/3]).

-define(TABLE, rabbit_runtime_parameters).

%%---------------------------------------------------------------------------

parse_set(_, <<"policy">>, _, _) ->
    {error_string, "policies may not be set using this method"};
parse_set(VHost, Component, Name, String) ->
    case rabbit_misc:json_decode(String) of
        {ok, JSON} -> set(VHost, Component, Name,
                          rabbit_misc:json_to_term(JSON));
        error      -> {error_string, "JSON decoding error"}
    end.

set(_, <<"policy">>, _, _) ->
    {error_string, "policies may not be set using this method"};
set(VHost, Component, Name, Term) ->
    set_any(VHost, Component, Name, Term).

format_error(L) ->
    {error_string, rabbit_misc:format_many([{"Validation failed~n", []} | L])}.

set_any(VHost, Component, Name, Term) ->
    case set_any0(VHost, Component, Name, Term) of
        ok          -> ok;
        {errors, L} -> format_error(L)
    end.

set_any0(VHost, Component, Name, Term) ->
    case lookup_component(Component) of
        {ok, Mod} ->
            case flatten_errors(Mod:validate(VHost, Component, Name, Term)) of
                ok ->
                    case mnesia_update(VHost, Component, Name, Term) of
                        {old, Term} -> ok;
                        _           -> Mod:notify(VHost, Component, Name, Term)
                    end,
                    ok;
                E ->
                    E
            end;
        E ->
            E
    end.

mnesia_update(VHost, Comp, Name, Term) ->
    F = fun () ->
                Res = case mnesia:read(?TABLE, {VHost, Comp, Name}, read) of
                          []       -> new;
                          [Params] -> {old, Params#runtime_parameters.value}
                      end,
                ok = mnesia:write(?TABLE, c(VHost, Comp, Name, Term), write),
                Res
        end,
    rabbit_misc:execute_mnesia_transaction(rabbit_vhost:with(VHost, F)).

clear(_, <<"policy">> , _) ->
    {error_string, "policies may not be cleared using this method"};
clear(VHost, Component, Name) ->
    clear_any(VHost, Component, Name).

clear_any(VHost, Component, Name) ->
    case lookup(VHost, Component, Name) of
        not_found -> {error_string, "Parameter does not exist"};
        _         -> mnesia_clear(VHost, Component, Name),
                     case lookup_component(Component) of
                         {ok, Mod} -> Mod:notify_clear(VHost, Component, Name);
                         _         -> ok
                     end
    end.

mnesia_clear(VHost, Component, Name) ->
    F = fun () ->
                ok = mnesia:delete(?TABLE, {VHost, Component, Name}, write)
        end,
    ok = rabbit_misc:execute_mnesia_transaction(rabbit_vhost:with(VHost, F)).

list() ->
    [p(P) || #runtime_parameters{ key = {_VHost, Comp, _Name}} = P <-
             rabbit_misc:dirty_read_all(?TABLE), Comp /= <<"policy">>].

list(VHost)               -> list(VHost, '_').
list_component(Component) -> list('_',   Component).

list(VHost, Component) ->
    case VHost of
        '_' -> ok;
        _   -> rabbit_vhost:assert(VHost)
    end,
    Match = #runtime_parameters{key = {VHost, Component, '_'}, _ = '_'},
    [p(P) || #runtime_parameters{key = {_VHost, Comp, _Name}} = P <-
                 mnesia:dirty_match_object(?TABLE, Match),
             Comp =/= <<"policy">> orelse Component =:= <<"policy">>].

list_formatted(VHost) ->
    [pset(value, format(pget(value, P)), P) || P <- list(VHost)].

lookup(VHost, Component, Name) ->
    case lookup0(VHost, Component, Name, rabbit_misc:const(not_found)) of
        not_found -> not_found;
        Params    -> p(Params)
    end.

value(VHost, Component, Name) ->
    case lookup0(VHost, Component, Name, rabbit_misc:const(not_found)) of
        not_found -> not_found;
        Params    -> Params#runtime_parameters.value
    end.

value(VHost, Component, Name, Default) ->
    Params = lookup0(VHost, Component, Name,
                     fun () ->
                             lookup_missing(VHost, Component, Name, Default)
                     end),
    Params#runtime_parameters.value.

lookup0(VHost, Component, Name, DefaultFun) ->
    case mnesia:dirty_read(?TABLE, {VHost, Component, Name}) of
        []  -> DefaultFun();
        [R] -> R
    end.

lookup_missing(VHost, Component, Name, Default) ->
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              case mnesia:read(?TABLE, {VHost, Component, Name}, read) of
                  []  -> Record = c(VHost, Component, Name, Default),
                         mnesia:write(?TABLE, Record, write),
                         Record;
                  [R] -> R
              end
      end).

c(VHost, Component, Name, Default) ->
    #runtime_parameters{key = {VHost, Component, Name},
                        value = Default}.

p(#runtime_parameters{key = {VHost, Component, Name}, value = Value}) ->
    [{vhost,     VHost},
     {component, Component},
     {name,      Name},
     {value,     Value}].

info_keys() -> [component, name, value].

%%---------------------------------------------------------------------------

lookup_component(Component) ->
    case rabbit_registry:lookup_module(
           runtime_parameter, list_to_atom(binary_to_list(Component))) of
        {error, not_found} -> {errors,
                               [{"component ~s not found", [Component]}]};
        {ok, Module}       -> {ok, Module}
    end.

format(Term) ->
    {ok, JSON} = rabbit_misc:json_encode(rabbit_misc:term_to_json(Term)),
    list_to_binary(JSON).

flatten_errors(L) ->
    case [{F, A} || I <- lists:flatten([L]), {error, F, A} <- [I]] of
        [] -> ok;
        E  -> {errors, E}
    end.
