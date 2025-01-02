%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_exchange_type_fanout).
-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(rabbit_exchange_type).

-export([description/0, serialise_events/0, route/2, route/3]).
-export([validate/1, validate_binding/2,
         create/2, delete/2, policy_changed/2, add_binding/3,
         remove_bindings/3, assert_args_equivalence/2]).
-export([info/1, info/2]).

-rabbit_boot_step({?MODULE,
                   [{description, "exchange type fanout"},
                    {mfa,         {rabbit_registry, register,
                                   [exchange, <<"fanout">>, ?MODULE]}},
                    {requires,    rabbit_registry},
                    {enables,     kernel_ready}]}).

info(_X) -> [].
info(_X, _) -> [].

description() ->
    [{description, <<"AMQP fanout exchange, as per the AMQP specification">>}].

serialise_events() -> false.

route(#exchange{name = Name}, _Message) ->
    route(#exchange{name = Name}, _Message, #{}).

route(#exchange{name = Name}, _Message, _Opts) ->
    rabbit_router:match_routing_key(Name, ['_']).

validate(_X) -> ok.
validate_binding(_X, _B) -> ok.
create(_Serial, _X) -> ok.
delete(_Serial, _X) -> ok.
policy_changed(_X1, _X2) -> ok.
add_binding(_Serial, _X, _B) -> ok.
remove_bindings(_Serial, _X, _Bs) -> ok.
assert_args_equivalence(X, Args) ->
    rabbit_exchange:assert_args_equivalence(X, Args).
