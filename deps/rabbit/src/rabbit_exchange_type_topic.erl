%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_exchange_type_topic).

-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(rabbit_exchange_type).

-export([description/0, serialise_events/0, route/2]).
-export([validate/1, validate_binding/2,
         create/2, delete/2, policy_changed/2, add_binding/3,
         remove_bindings/3, assert_args_equivalence/2]).
-export([info/1, info/2]).

-rabbit_boot_step({?MODULE,
                   [{description, "exchange type topic"},
                    {mfa,         {rabbit_registry, register,
                                   [exchange, <<"topic">>, ?MODULE]}},
                    {requires,    rabbit_registry},
                    {enables,     kernel_ready}]}).

%%----------------------------------------------------------------------------

info(_X) -> [].
info(_X, _) -> [].

description() ->
    [{description, <<"AMQP topic exchange, as per the AMQP specification">>}].

serialise_events() -> false.

%% NB: This may return duplicate results in some situations (that's ok)
route(#exchange{name = XName},
      #delivery{message = #basic_message{routing_keys = Routes}}) ->
    lists:append([rabbit_db_topic_exchange:match(XName, RKey) || RKey <- Routes]).

validate(_X) -> ok.
validate_binding(_X, _B) -> ok.
create(_Tx, _X) -> ok.

delete(_Serial, #exchange{name = X}) ->
    rabbit_db_topic_exchange:delete_all_for_exchange(X).

policy_changed(_X1, _X2) -> ok.

add_binding(_Serial, _Exchange, Binding) ->
    rabbit_db_topic_exchange:set(Binding).

remove_bindings(_Serial, _X, Bs) ->
    rabbit_db_topic_exchange:delete(Bs).

assert_args_equivalence(X, Args) ->
    rabbit_exchange:assert_args_equivalence(X, Args).

%%----------------------------------------------------------------------------
