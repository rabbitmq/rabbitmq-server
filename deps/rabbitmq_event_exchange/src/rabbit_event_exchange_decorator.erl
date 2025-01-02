%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_event_exchange_decorator).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("rabbit_event_exchange.hrl").

-rabbit_boot_step({?MODULE,
                   [{description, "event exchange decorator"},
                    {mfa, {rabbit_registry, register,
                           [exchange_decorator, <<"event">>, ?MODULE]}},
                    {cleanup, {rabbit_registry, unregister,
                               [exchange_decorator, <<"event">>]}},
                    {requires, [rabbit_registry, recovery]}]}).

-behaviour(rabbit_exchange_decorator).

-export([description/0, serialise_events/1]).
-export([create/2, delete/2, policy_changed/2,
         add_binding/3, remove_bindings/3, route/2, active_for/1]).

description() ->
    [{description, <<"Event exchange decorator">>}].

serialise_events(_) -> false.

create(_, _) ->
    ok.

delete(_, _) ->
    ok.

policy_changed(_, _) ->
    ok.

add_binding(none, #exchange{name = #resource{name = ?EXCH_NAME} = Name},
            _Bs) ->
    case rabbit_binding:list_for_source(Name) of
        [_] ->
            rpc:abcast(rabbit_event, {event_exchange, added_first_binding}),
            ok;
        _ ->
            ok
    end;
add_binding(_, _, _) ->
    ok.

remove_bindings(none, #exchange{name = #resource{name = ?EXCH_NAME} = Name},
                _Bs) ->
    case rabbit_binding:list_for_source(Name) of
        [] ->
            rpc:abcast(rabbit_event, {event_exchange, removed_last_binding}),
            ok;
        _ ->
            ok
    end;
remove_bindings(_, _, _) ->
    ok.

route(_, _) -> [].

active_for(_) -> noroute.
