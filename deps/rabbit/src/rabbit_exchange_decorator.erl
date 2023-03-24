%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_exchange_decorator).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([select/2, set/1, active/1]).

-behaviour(rabbit_registry_class).

-export([added_to_rabbit_registry/2, removed_from_rabbit_registry/1]).

%% This is like an exchange type except that:
%%
%% 1) It applies to all exchanges as soon as it is installed, therefore
%% 2) It is not allowed to affect validation, so no validate/1 or
%%    assert_args_equivalence/2
%%
%% It's possible in the future we might make decorators
%% able to manipulate messages as they are published.

-type(serial() :: pos_integer() | 'none').

-callback description() -> [proplists:property()].

%% Should Rabbit ensure that all binding events that are
%% delivered to an individual exchange can be serialised? (they
%% might still be delivered out of order, but there'll be a
%% serial number).
-callback serialise_events(rabbit_types:exchange()) -> boolean().

%% called after declaration and recovery
-callback create(serial(), rabbit_types:exchange()) -> 'ok'.

%% called after exchange (auto)deletion.
-callback delete(serial(), rabbit_types:exchange()) ->
    'ok'.

%% called when the policy attached to this exchange changes.
-callback policy_changed(rabbit_types:exchange(), rabbit_types:exchange()) ->
    'ok'.

%% called after a binding has been added or recovered
-callback add_binding(serial(), rabbit_types:exchange(),
                      rabbit_types:binding()) -> 'ok'.

%% called after bindings have been deleted.
-callback remove_bindings(serial(), rabbit_types:exchange(),
                          [rabbit_types:binding()]) -> 'ok'.

%% Allows additional destinations to be added to the routing decision.
-callback route(rabbit_types:exchange(), rabbit_types:delivery()) ->
    [rabbit_amqqueue:name() | rabbit_exchange:name()].

%% Whether the decorator wishes to receive callbacks for the exchange
%% none:no callbacks, noroute:all callbacks except route, all:all callbacks
-callback active_for(rabbit_types:exchange()) -> 'none' | 'noroute' | 'all'.

%%----------------------------------------------------------------------------

added_to_rabbit_registry(_Type, _ModuleName) ->
    [maybe_recover(X) || X <- rabbit_exchange:list()],
    ok.
removed_from_rabbit_registry(_Type) ->
    [maybe_recover(X) || X <- rabbit_exchange:list()],
    ok.

%% select a subset of active decorators
select(all,   {Route, NoRoute})  -> filter(Route ++ NoRoute);
select(route, {Route, _NoRoute}) -> filter(Route);
select(raw,   {Route, NoRoute})  -> Route ++ NoRoute;
select(_, undefined) -> [].

filter(Modules) ->
    [M || M <- Modules, code:which(M) =/= non_existing].

set(X) ->
    Decs = lists:foldl(fun (D, {Route, NoRoute}) ->
                               ActiveFor = D:active_for(X),
                               {cons_if_eq(all,     ActiveFor, D, Route),
                                cons_if_eq(noroute, ActiveFor, D, NoRoute)}
                       end, {[], []}, list()),
    X#exchange{decorators = Decs}.

%% TODO The list of decorators can probably be a parameter, to avoid multiple queries
%% when we're updating many exchanges
active(X) ->
    lists:foldl(fun (D, {Route, NoRoute}) ->
                        ActiveFor = D:active_for(X),
                        {cons_if_eq(all,     ActiveFor, D, Route),
                         cons_if_eq(noroute, ActiveFor, D, NoRoute)}
                end, {[], []}, list()).

list() -> [M || {_, M} <- rabbit_registry:lookup_all(exchange_decorator)].

cons_if_eq(Select,  Select, Item,  List) -> [Item | List];
cons_if_eq(_Select, _Other, _Item, List) -> List.

maybe_recover(X = #exchange{name       = Name,
                            decorators = Decs}) ->
    #exchange{decorators = Decs1} = set(X),
    Old = lists:sort(select(all, Decs)),
    New = lists:sort(select(all, Decs1)),
    case New of
        Old -> ok;
        _   -> %% TODO create a tx here for non-federation decorators
               _ = [M:create(none, X) || M <- New -- Old],
               rabbit_exchange:update_decorators(Name, Decs1)
    end.
