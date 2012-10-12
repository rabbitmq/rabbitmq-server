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

-export([parse_set_param/4, set_param/4,
         parse_set_policy/4, parse_set_policy/5, set_policy/5,
         clear_param/3, clear_policy/2,
         list_param/0, list_param/1, list_param/2,
         list_param_strict/1, list_param_strict/2,
         list_policies/0, list_policies/1,
         list_formatted_param/1, list_formatted_policies/1, list_policies_raw/1,
         lookup_param/3, lookup_policy/2,
         value/3, value/4, info_keys/0, info_keys_policy/0]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(ok_or_error_string() :: 'ok' | {'error_string', string()}).

-spec(parse_set_param/4 :: (rabbit_types:vhost(), binary(), binary(), string())
                            -> ok_or_error_string()).
-spec(set_param/4 :: (rabbit_types:vhost(), binary(), binary(), term())
                      -> ok_or_error_string()).
-spec(parse_set_policy/4 :: (rabbit_types:vhost(), binary(), string(),
                             string()) -> ok_or_error_string()).
-spec(parse_set_policy/5 :: (rabbit_types:vhost(), binary(), string(), string(),
                             string()) -> ok_or_error_string()).
-spec(set_policy/5 :: (rabbit_types:vhost(), binary(), term(), term(), term())
                       -> ok_or_error_string()).
-spec(clear_param/3 :: (rabbit_types:vhost(), binary(), binary())
                        -> ok_or_error_string()).
-spec(clear_policy/2 :: (rabbit_types:vhost(), binary())
                         -> ok_or_error_string()).
-spec(list_param/0 :: () -> [rabbit_types:infos()]).
-spec(list_param/1 :: (rabbit_types:vhost()) -> [rabbit_types:infos()]).
-spec(list_param_strict/1 :: (binary())
                              -> [rabbit_types:infos()] | 'not_found').
-spec(list_param/2 :: (rabbit_types:vhost(), binary())
                       -> [rabbit_types:infos()]).
-spec(list_param_strict/2 :: (rabbit_types:vhost(), binary())
                       -> [rabbit_types:infos()] | 'not_found').
-spec(list_formatted_param/1 :: (rabbit_types:vhost())
                                 -> [rabbit_types:infos()]).
-spec(list_formatted_policies/1 :: (rabbit_types:vhost()) ->
                                    [rabbit_types:infos()]).
-spec(lookup_param/3 :: (rabbit_types:vhost(), binary(), binary())
                         -> rabbit_types:infos()).
-spec(lookup_policy/2 :: (rabbit_types:vhost(), binary())
                         -> rabbit_types:infos()).
-spec(value/3 :: (rabbit_types:vhost(), binary(), binary()) -> term()).
-spec(value/4 :: (rabbit_types:vhost(), binary(), binary(), term()) -> term()).
-spec(info_keys/0 :: () -> rabbit_types:info_keys()).
-spec(info_keys_policy/0 :: () -> rabbit_types:info_keys()).

-endif.

%%---------------------------------------------------------------------------

-import(rabbit_misc, [pget/2, pget/3, pset/3]).

-define(TABLE, rabbit_runtime_parameters).

%%---------------------------------------------------------------------------

parse_set_param(_, <<"policy">>, _, _) ->
    {error_string, "policies may not be set using this method"};
parse_set_param(VHost, Component, Key, String) ->
    case rabbit_misc:json_decode(String) of
        {ok, JSON} -> set_param(VHost, Component, Key,
                                rabbit_misc:json_to_term(JSON));
        error      -> {error_string, "JSON decoding error"}
    end.

set_param(_, <<"policy">>, _, _) ->
    {error_string, "policies may not be set using this method"};
set_param(VHost, Component, Key, Term) ->
    case set0(VHost, Component, Key, Term) of
        ok          -> ok;
        {errors, L} -> format_error(L)
    end.

parse_set_policy(VHost, Key, Pat, Defn) ->
    parse_set_policy0(VHost, Key, Pat, Defn, []).
parse_set_policy(VHost, Key, Pat, Defn, Priority) ->
    try list_to_integer(Priority) of
        Num -> parse_set_policy0(VHost, Key, Pat, Defn, [{<<"priority">>, Num}])
    catch
        _:_ -> {error, "~p priority must be a number", [Priority]}
    end.

parse_set_policy0(VHost, Key, Pattern, Defn, Priority) ->
    case rabbit_misc:json_decode(Defn) of
        {ok, JSON} ->
            set_policy0(VHost, Key, pset(<<"pattern">>, list_to_binary(Pattern),
                                         pset(<<"policy">>,
                                              rabbit_misc:json_to_term(JSON),
                                              Priority)));
        error ->
            {error_string, "JSON decoding error"}
    end.

set_policy(VHost, Key, Pattern, Defn, Priority) ->
    PolicyProps = [{<<"pattern">>, Pattern}, {<<"policy">>, Defn}],
    set_policy0(VHost, Key, case Priority of
                                undefined -> [];
                                _         -> [{<<"priority">>, Priority}]
                            end ++ PolicyProps).

set_policy0(VHost, Key, Term) ->
    case set0(VHost, <<"policy">>, Key, Term) of
        ok          -> ok;
        {errors, L} -> format_error(L)
    end.

set0(VHost, Component, Key, Term) ->
    case lookup_component(Component) of
        {ok, Mod} ->
            case flatten_errors(Mod:validate(VHost, Component, Key, Term)) of
                ok ->
                    case mnesia_update(VHost, Component, Key, Term) of
                        {old, Term} -> ok;
                        _           -> Mod:notify(VHost, Component, Key, Term)
                    end,
                    ok;
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

%%---------------------------------------------------------------------------

clear_param(_, <<"policy">> , _) ->
    {error_string, "policies may not be cleared using this method"};
clear_param(VHost, Component, Key) ->
    case clear0(VHost, Component, Key) of
        ok          -> ok;
        {errors, L} -> format_error(L)
    end.

clear_policy(VHost, Key) ->
    case clear0(VHost, <<"policy">>, Key) of
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

%%---------------------------------------------------------------------------

list_param() ->
    [p(P) || #runtime_parameters{ key = {_VHost, Comp, _Key}} = P <-
             rabbit_misc:dirty_read_all(?TABLE), Comp /= <<"policy">>].

list_param(VHost)                   -> list_param(VHost, '_', []).
list_param_strict(Component)        -> list_param('_',   Component, not_found).
list_param(VHost, Component)        -> list_param(VHost, Component, []).
list_param_strict(VHost, Component) -> list_param(VHost, Component, not_found).

list_param(VHost, Component, Default) ->
    case component_good(Component) of
        true -> Match = #runtime_parameters{key = {VHost, Component, '_'},
                                            _ = '_'},
                [p(P) || #runtime_parameters{ key = {_VHost, Comp, _Key}} = P <-
                         mnesia:dirty_match_object(?TABLE, Match),
                         Comp /= <<"policy">>];
        _    -> Default
    end.

list_policies() ->
    list_policies('_').

list_policies(VHost) ->
    list_policies0(VHost, fun ident/1).

list_formatted_param(VHost) ->
    [pset(value, format(pget(value, P)), P) || P <- list_param(VHost)].

list_formatted_policies(VHost) ->
     order_policies(list_policies0(VHost, fun format/1)).

list_policies0(VHost, DefnFun) ->
    Match = #runtime_parameters{key = {VHost, <<"policy">>, '_'}, _ = '_'},
    [pol(P, DefnFun) || P <- mnesia:dirty_match_object(?TABLE, Match)].

order_policies(PropList) ->
    lists:sort(fun (A, B) -> pget(priority, A, 0) < pget(priority, B, 0) end,
               PropList).

list_policies_raw(VHost) ->
    Match = #runtime_parameters{key = {VHost, <<"policy">>, '_'}, _ = '_'},
    [p(P) || P <- mnesia:dirty_match_object(?TABLE, Match)].

%%---------------------------------------------------------------------------

lookup_param(VHost, Component, Key) ->
    case lookup0(VHost, Component, Key, rabbit_misc:const(not_found)) of
        not_found -> not_found;
        Params    -> p(Params)
    end.

lookup_policy(VHost, Key) ->
    case lookup0(VHost, <<"policy">>, Key, rabbit_misc:const(not_found)) of
        not_found -> not_found;
        Policy    -> pol(Policy, fun ident/1)
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

pol(#runtime_parameters{key = {VHost, <<"policy">>, Key}, value = Value},
    DefnFun) ->
    [{vhost,      VHost},
     {key,        Key},
     {pattern,    pget(<<"pattern">>, Value)},
     {definition, DefnFun(pget(<<"policy">>,  Value))}] ++
     case pget(<<"priority">>, Value) of
         undefined -> [];
         Priority  -> [{priority, Priority}]
     end.

info_keys()        -> [component, key, value].
info_keys_policy() -> [vhost, key, pattern, definition, priority].

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

format_error(L) ->
    {error_string, rabbit_misc:format_many([{"Validation failed~n", []} | L])}.

format(Term) ->
    {ok, JSON} = rabbit_misc:json_encode(rabbit_misc:term_to_json(Term)),
    list_to_binary(JSON).

ident(X) -> X.

flatten_errors(L) ->
    case [{F, A} || I <- lists:flatten([L]), {error, F, A} <- [I]] of
        [] -> ok;
        E  -> {errors, E}
    end.
