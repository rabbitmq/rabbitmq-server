%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_exchange_type).

-behaviour(rabbit_registry_class).

-export([added_to_rabbit_registry/2, removed_from_rabbit_registry/1]).

-type(serial() :: pos_integer() | 'none').

-callback description() -> [proplists:property()].

%% Should Rabbit ensure that all binding events that are
%% delivered to an individual exchange can be serialised? (they
%% might still be delivered out of order, but there'll be a
%% serial number).
-callback serialise_events() -> boolean().

%% The no_return is there so that we can have an "invalid" exchange
%% type (see rabbit_exchange_type_invalid).
%% NB: This callback is deprecated in favour of route/3
%% and will be removed in the future
% -callback route(rabbit_types:exchange(), mc:state()) ->
%     rabbit_router:match_result().

-callback route(rabbit_types:exchange(), mc:state(), rabbit_exchange:route_opts()) ->
    [rabbit_types:binding_destination() |
     {rabbit_amqqueue:name(), rabbit_types:binding_key()}].

%% called BEFORE declaration, to check args etc; may exit with #amqp_error{}
-callback validate(rabbit_types:exchange()) -> 'ok'.

%% called BEFORE declaration, to check args etc
-callback validate_binding(rabbit_types:exchange(), rabbit_types:binding()) ->
    rabbit_types:ok_or_error({'binding_invalid', string(), [any()]}).

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

%% called when comparing exchanges for equivalence - should return ok or
%% exit with #amqp_error{}
-callback assert_args_equivalence(rabbit_types:exchange(),
                                  rabbit_framing:amqp_table()) ->
    'ok' | rabbit_types:connection_exit().

%% Exchange type specific info keys
-callback info(rabbit_types:exchange()) -> [{atom(), term()}].

-callback info(rabbit_types:exchange(), [atom()]) -> [{atom(), term()}].

added_to_rabbit_registry(Type, _ModuleName) ->
    persistent_term:erase(Type),
    ok.

removed_from_rabbit_registry(Type) ->
    persistent_term:erase(Type),
    ok.
