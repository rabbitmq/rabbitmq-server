%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2018 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_event_exchange_decorator).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("rabbit_event_exchange.hrl").

-rabbit_boot_step({?MODULE,
                   [{description, "event exchange decorator"},
                    {mfa, {rabbit_registry, register,
                           [exchange_decorator, <<"event">>, ?MODULE]}},
                    {requires, rabbit_registry},
                    {cleanup, {rabbit_registry, unregister,
                               [exchange_decorator, <<"event">>]}},
                    {enables, recovery}]}).

-behaviour(rabbit_exchange_decorator).

-export([description/0, serialise_events/1]).
-export([create/2, delete/3, policy_changed/2,
         add_binding/3, remove_bindings/3, route/2, active_for/1]).

description() ->
    [{description, <<"Event exchange decorator">>}].

serialise_events(_) -> false.

create(_, _) ->
    ok.

delete(_, _, _) ->
    ok.

policy_changed(_, _) ->
    ok.

add_binding(transaction, #exchange{name = #resource{name = ?EXCH_NAME} = Name},
            _Bs) ->
    case rabbit_binding:list_for_source(Name) of
        [_] ->
            rpc:abcast(rabbit_event, {event_exchange, added_first_binding}),
            ok;
        _ ->
            ok
    end;
add_binding(_, _X, _Bs) ->
    ok.

remove_bindings(transaction, #exchange{name = #resource{name = ?EXCH_NAME} = Name},
                _Bs) ->
    case rabbit_binding:list_for_source(Name) of
        [] ->
            rpc:abcast(rabbit_event, {event_exchange, removed_last_binding}),
            ok;
        _ ->
            ok
    end;
remove_bindings(_, _X, _Bs) ->
    ok.

route(_, _) -> [].

active_for(_) -> noroute.
