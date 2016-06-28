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
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_exchange_decorator).

-include("rabbit.hrl").

-export([select/2, set/1, register/2, unregister/1]).

%% This is like an exchange type except that:
%%
%% 1) It applies to all exchanges as soon as it is installed, therefore
%% 2) It is not allowed to affect validation, so no validate/1 or
%%    assert_args_equivalence/2
%%
%% It's possible in the future we might make decorators
%% able to manipulate messages as they are published.

-type(tx() :: 'transaction' | 'none').
-type(serial() :: pos_integer() | tx()).

-callback description() -> [proplists:property()].

%% Should Rabbit ensure that all binding events that are
%% delivered to an individual exchange can be serialised? (they
%% might still be delivered out of order, but there'll be a
%% serial number).
-callback serialise_events(rabbit_types:exchange()) -> boolean().

%% called after declaration and recovery
-callback create(tx(), rabbit_types:exchange()) -> 'ok'.

%% called after exchange (auto)deletion.
-callback delete(tx(), rabbit_types:exchange(), [rabbit_types:binding()]) ->
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

%% select a subset of active decorators
select(all,   {Route, NoRoute})  -> filter(Route ++ NoRoute);
select(route, {Route, _NoRoute}) -> filter(Route);
select(raw,   {Route, NoRoute})  -> Route ++ NoRoute.

filter(Modules) ->
    [M || M <- Modules, code:which(M) =/= non_existing].

set(X) ->
    Decs = lists:foldl(fun (D, {Route, NoRoute}) ->
                               ActiveFor = D:active_for(X),
                               {cons_if_eq(all,     ActiveFor, D, Route),
                                cons_if_eq(noroute, ActiveFor, D, NoRoute)}
                       end, {[], []}, list()),
    X#exchange{decorators = Decs}.

list() -> [M || {_, M} <- rabbit_registry:lookup_all(exchange_decorator)].

cons_if_eq(Select,  Select, Item,  List) -> [Item | List];
cons_if_eq(_Select, _Other, _Item, List) -> List.

register(TypeName, ModuleName) ->
    rabbit_registry:register(exchange_decorator, TypeName, ModuleName),
    [maybe_recover(X) || X <- rabbit_exchange:list()],
    ok.

unregister(TypeName) ->
    rabbit_registry:unregister(exchange_decorator, TypeName),
    [maybe_recover(X) || X <- rabbit_exchange:list()],
    ok.

maybe_recover(X = #exchange{name       = Name,
                            decorators = Decs}) ->
    #exchange{decorators = Decs1} = set(X),
    Old = lists:sort(select(all, Decs)),
    New = lists:sort(select(all, Decs1)),
    case New of
        Old -> ok;
        _   -> %% TODO create a tx here for non-federation decorators
               [M:create(none, X) || M <- New -- Old],
               rabbit_exchange:update_decorators(Name)
    end.
