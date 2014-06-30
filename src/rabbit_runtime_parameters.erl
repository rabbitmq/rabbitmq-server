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
%% Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_runtime_parameters).

-include("rabbit.hrl").

-export([parse_set/5, set/5, set_any/5, clear/3, clear_any/3, list/0, list/1,
         list_component/1, list/2, list_formatted/1, lookup/3,
         value/3, value/4, info_keys/0]).

-export([set_global/2, value_global/1, value_global/2]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(ok_or_error_string() :: 'ok' | {'error_string', string()}).
-type(ok_thunk_or_error_string() :: ok_or_error_string() | fun(() -> 'ok')).

-spec(parse_set/5 :: (rabbit_types:vhost(), binary(), binary(), string(),
                      rabbit_types:user() | 'none') -> ok_or_error_string()).
-spec(set/5 :: (rabbit_types:vhost(), binary(), binary(), term(),
                rabbit_types:user() | 'none') -> ok_or_error_string()).
-spec(set_any/5 :: (rabbit_types:vhost(), binary(), binary(), term(),
                    rabbit_types:user() | 'none') -> ok_or_error_string()).
-spec(set_global/2 :: (atom(), term()) -> 'ok').
-spec(clear/3 :: (rabbit_types:vhost(), binary(), binary())
                 -> ok_thunk_or_error_string()).
-spec(clear_any/3 :: (rabbit_types:vhost(), binary(), binary())
                     -> ok_thunk_or_error_string()).
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
-spec(value_global/1 :: (atom()) -> term() | 'not_found').
-spec(value_global/2 :: (atom(), term()) -> term()).
-spec(info_keys/0 :: () -> rabbit_types:info_keys()).

-endif.

%%---------------------------------------------------------------------------

-import(rabbit_misc, [pget/2, pset/3]).

-define(TABLE, rabbit_runtime_parameters).

%%---------------------------------------------------------------------------

parse_set(_, <<"policy">>, _, _, _) ->
    {error_string, "policies may not be set using this method"};
parse_set(VHost, Component, Name, String, User) ->
    case rabbit_misc:json_decode(String) of
        {ok, JSON} -> set(VHost, Component, Name,
                          rabbit_misc:json_to_term(JSON), User);
        error      -> {error_string, "JSON decoding error"}
    end.

set(_, <<"policy">>, _, _, _) ->
    {error_string, "policies may not be set using this method"};
set(VHost, Component, Name, Term, User) ->
    set_any(VHost, Component, Name, Term, User).

set_global(Name, Term) ->
    mnesia_update(Name, Term),
    event_notify(parameter_set, none, global, [{name,  Name},
                                               {value, Term}]),
    ok.

format_error(L) ->
    {error_string, rabbit_misc:format_many([{"Validation failed~n", []} | L])}.

set_any(VHost, Component, Name, Term, User) ->
    case set_any0(VHost, Component, Name, Term, User) of
        ok          -> ok;
        {errors, L} -> format_error(L)
    end.

set_any0(VHost, Component, Name, Term, User) ->
    case lookup_component(Component) of
        {ok, Mod} ->
            case flatten_errors(
                   Mod:validate(VHost, Component, Name, Term, User)) of
                ok ->
                    case mnesia_update(VHost, Component, Name, Term) of
                        {old, Term} -> ok;
                        _           -> event_notify(
                                         parameter_set, VHost, Component,
                                         [{name,  Name},
                                          {value, Term}]),
                                       Mod:notify(VHost, Component, Name, Term)
                    end,
                    ok;
                E ->
                    E
            end;
        E ->
            E
    end.

mnesia_update(Key, Term) ->
    rabbit_misc:execute_mnesia_transaction(mnesia_update_fun(Key, Term)).

mnesia_update(VHost, Comp, Name, Term) ->
    rabbit_misc:execute_mnesia_transaction(
      rabbit_vhost:with(VHost, mnesia_update_fun({VHost, Comp, Name}, Term))).

mnesia_update_fun(Key, Term) ->
    fun () ->
            Res = case mnesia:read(?TABLE, Key, read) of
                      []       -> new;
                      [Params] -> {old, Params#runtime_parameters.value}
                      end,
            ok = mnesia:write(?TABLE, c(Key, Term), write),
            Res
    end.

clear(_, <<"policy">> , _) ->
    {error_string, "policies may not be cleared using this method"};
clear(VHost, Component, Name) ->
    clear_any(VHost, Component, Name).

clear_any(VHost, Component, Name) ->
    Notify = fun () ->
                     case lookup_component(Component) of
                         {ok, Mod} -> event_notify(
                                        parameter_cleared, VHost, Component,
                                        [{name, Name}]),
                                      Mod:notify_clear(VHost, Component, Name);
                         _         -> ok
                     end
             end,
    case lookup(VHost, Component, Name) of
        not_found -> {error_string, "Parameter does not exist"};
        _         -> mnesia_clear(VHost, Component, Name),
                     case mnesia:is_transaction() of
                         true  -> Notify;
                         false -> Notify()
                     end
    end.

mnesia_clear(VHost, Component, Name) ->
    F = fun () ->
                ok = mnesia:delete(?TABLE, {VHost, Component, Name}, write)
        end,
    ok = rabbit_misc:execute_mnesia_transaction(rabbit_vhost:with(VHost, F)).

event_notify(_Event, _VHost, <<"policy">>, _Props) ->
    ok;
event_notify(Event, none, Component, Props) ->
    rabbit_event:notify(Event, [{component, Component} | Props]);
event_notify(Event, VHost, Component, Props) ->
    rabbit_event:notify(Event, [{vhost,     VHost},
                                {component, Component} | Props]).

list() ->
    [p(P) || #runtime_parameters{ key = {_VHost, Comp, _Name}} = P <-
             rabbit_misc:dirty_read_all(?TABLE), Comp /= <<"policy">>].

list(VHost)               -> list(VHost, '_').
list_component(Component) -> list('_',   Component).

%% Not dirty_match_object since that would not be transactional when used in a
%% tx context
list(VHost, Component) ->
    mnesia:async_dirty(
      fun () ->
              case VHost of
                  '_' -> ok;
                  _   -> rabbit_vhost:assert(VHost)
              end,
              Match = #runtime_parameters{key = {VHost, Component, '_'},
                                          _   = '_'},
              [p(P) || #runtime_parameters{key = {_VHost, Comp, _Name}} = P <-
                           mnesia:match_object(?TABLE, Match, read),
                       Comp =/= <<"policy">> orelse Component =:= <<"policy">>]
      end).

list_formatted(VHost) ->
    [pset(value, format(pget(value, P)), P) || P <- list(VHost)].

lookup(VHost, Component, Name) ->
    case lookup0({VHost, Component, Name}, rabbit_misc:const(not_found)) of
        not_found -> not_found;
        Params    -> p(Params)
    end.

value(VHost, Comp, Name)      -> value0({VHost, Comp, Name}).
value(VHost, Comp, Name, Def) -> value0({VHost, Comp, Name}, Def).

value_global(Key)          -> value0(Key).
value_global(Key, Default) -> value0(Key, Default).

value0(Key) ->
    case lookup0(Key, rabbit_misc:const(not_found)) of
        not_found -> not_found;
        Params    -> Params#runtime_parameters.value
    end.

value0(Key, Default) ->
    Params = lookup0(Key, fun () -> lookup_missing(Key, Default) end),
    Params#runtime_parameters.value.

lookup0(Key, DefaultFun) ->
    case mnesia:dirty_read(?TABLE, Key) of
        []  -> DefaultFun();
        [R] -> R
    end.

lookup_missing(Key, Default) ->
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              case mnesia:read(?TABLE, Key, read) of
                  []  -> Record = c(Key, Default),
                         mnesia:write(?TABLE, Record, write),
                         Record;
                  [R] -> R
              end
      end).

c(Key, Default) ->
    #runtime_parameters{key   = Key,
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
