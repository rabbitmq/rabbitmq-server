%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_exchange_type_direct).
-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(rabbit_exchange_type).

-export([description/0, serialise_events/0, route/2]).
-export([validate/1, validate_binding/2,
         create/2, delete/3, policy_changed/2, add_binding/3,
         remove_bindings/3, assert_args_equivalence/2]).
-export([info/1, info/2]).

-rabbit_boot_step({?MODULE,
                   [{description, "exchange type direct"},
                    {mfa,         {rabbit_registry, register,
                                   [exchange, <<"direct">>, ?MODULE]}},
                    {requires,    rabbit_registry},
                    {enables,     kernel_ready}]}).

info(_X) -> [].
info(_X, _) -> [].

description() ->
    [{description, <<"AMQP direct exchange, as per the AMQP specification">>}].

serialise_events() -> false.

route(#exchange{name = Name, type = Type},
      #delivery{message = #basic_message{routing_keys = Routes}}) ->
    case {Type, rabbit_feature_flags:is_enabled(direct_exchange_routing_v2, non_blocking)} of
        {direct, true} ->
            route_v2(Name, Routes);
        _ ->
            rabbit_router:match_routing_key(Name, Routes)
    end.

validate(_X) -> ok.
validate_binding(_X, _B) -> ok.
create(_Tx, _X) -> ok.
delete(_Tx, _X, _Bs) -> ok.
policy_changed(_X1, _X2) -> ok.
add_binding(_Tx, _X, _B) -> ok.
remove_bindings(_Tx, _X, _Bs) -> ok.
assert_args_equivalence(X, Args) ->
    rabbit_exchange:assert_args_equivalence(X, Args).

%% rabbit_router:match_routing_key/2 uses ets:select/2 to get destinations.
%% ets:select/2 is expensive because it needs to compile the match spec every
%% time and lookup does not happen by a hash key.
%%
%% In contrast, route_v2/2 increases end-to-end message sending throughput
%% (i.e. from RabbitMQ client to the queue process) by up to 35% by using ets:lookup_element/3.
%% Only the direct exchange type uses the rabbit_index_route table to store its
%% bindings by table key tuple {SourceExchange, RoutingKey}.
-spec route_v2(rabbit_types:binding_source(), [rabbit_router:routing_key(), ...]) ->
    rabbit_router:match_result().
route_v2(SrcName, [RoutingKey]) ->
    %% optimization
    destinations(SrcName, RoutingKey);
route_v2(SrcName, [_|_] = RoutingKeys) ->
    lists:flatmap(fun(Key) ->
                          destinations(SrcName, Key)
                  end, RoutingKeys).

destinations(SrcName, RoutingKey) ->
    %% Prefer try-catch block over checking Key existence with ets:member/2.
    %% The latter reduces throughput by a few thousand messages per second because
    %% of function db_member_hash in file erl_db_hash.c.
    %% We optimise for the happy path, that is the binding / table key is present.
    try
        ets:lookup_element(rabbit_index_route,
                           {SrcName, RoutingKey},
                           #index_route.destination)
    catch
        error:badarg ->
            []
    end.
