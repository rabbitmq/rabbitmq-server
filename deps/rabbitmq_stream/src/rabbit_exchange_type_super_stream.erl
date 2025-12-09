%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_exchange_type_super_stream).

-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(rabbit_exchange_type).

-export([description/0,
         serialise_events/0,
         route/2,
         info/1,
         info/2]).
-export([validate/1,
         validate_binding/2,
         create/2,
         delete/2,
         policy_changed/2,
         add_binding/3,
         remove_bindings/3,
         assert_args_equivalence/2]).

-rabbit_boot_step({rabbit_exchange_type_super_stream_registry,
                   [{description, "exchange type x-super-stream: registry"},
                    {mfa, {rabbit_registry, register,
                           [exchange, <<"x-super-stream">>, ?MODULE]}},
                    {cleanup, {rabbit_registry, unregister,
                               [exchange, <<"x-super-stream">>]}},
                    {requires, rabbit_registry},
                    {enables, kernel_ready}]}).

-define(SEED, 104729).

description() ->
    [{description, <<"Super stream exchange type using murmur3 hashing">>}].

serialise_events() -> false.

route(#exchange{name = Name},
      #delivery{message = #basic_message{routing_keys = [RKey | _]}}) ->
    %% get all bindings for the exchange and use murmur3 to generate
    %% the binding key to match on
    case rabbit_binding:list_for_source(Name) of
        [] ->
            [];
        Bindings ->
            N = integer_to_binary(hash_mod(RKey, length(Bindings))),
            case lists:search(fun(#binding{key = Key}) ->
                                      Key =:= N
                              end, Bindings) of
                {value, #binding{destination = Dest}} ->
                    [Dest];
                false ->
                    []
            end
    end.

info(_) ->
    [].

info(_, _) ->
    [].

validate(_X) ->
    ok.

validate_binding(_X, #binding{key = K}) ->
    try
        %% just check the Key is an integer
        _ = binary_to_integer(K),
        ok
    catch
        error:badarg ->
            {error,
             {binding_invalid, "The binding key must be an integer: ~tp", [K]}}
    end.

create(_Serial, _X) ->
    ok.

delete(_Serial, _X) ->
    ok.

policy_changed(_X1, _X2) ->
    ok.

add_binding(_Serial, _X, _B) ->
    ok.

remove_bindings(_Serial, _X, _Bs) ->
    ok.

assert_args_equivalence(X, Args) ->
    rabbit_exchange:assert_args_equivalence(X, Args).

hash_mod(RKey, N) ->
    murmerl3:hash_32(RKey, ?SEED) rem N.
