%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_exchange_type_topic).

-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(rabbit_exchange_type).

-export([description/0, serialise_events/0, route/2, route/3]).
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

route(Exchange, Delivery) ->
    route(Exchange, Delivery, []).

%% This function can return duplicate destinations or duplicate binding keys.
%% The caller of this function is responsible for filtering out duplicates.
-spec route(rabbit_types:exchange(), rabbit_types:delivery(), rabbit_exchange:route_opts()) ->
    [rabbit_types:binding_destination() |
     {rabbit_amqqueue:name(), [rabbit_types:binding_key(), ...]}].
route(#exchange{name = XName},
      #delivery{message = #basic_message{routing_keys = Routes}},
      Opts) ->
    {Destinations, QNamesWithBindings} =
    lists:foldl(fun(RKey, {L0, M0}) ->
                        {L1, M1} = rabbit_db_topic_exchange:match(XName, RKey, Opts),
                        L = L0 ++ L1,
                        M = maps:merge_with(fun(_QName, BindingKeys0, BindingKeys1) ->
                                                    BindingKeys0 ++ BindingKeys1
                                            end, M0, M1),
                        {L, M}
                end, {[], #{}}, Routes),
    if map_size(QNamesWithBindings) =:= 0 ->
           %% optimisation
           Destinations;
       true ->
           Destinations ++ maps:to_list(QNamesWithBindings)
    end.

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
