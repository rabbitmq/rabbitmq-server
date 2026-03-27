%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_exchange_type_modulus_hash).

-behaviour(rabbit_exchange_type).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([description/0,
         route/3,
         serialise_events/0,
         info/1,
         info/2,
         validate/1,
         validate_binding/2,
         create/2,
         delete/2,
         policy_changed/2,
         add_binding/3,
         remove_bindings/3,
         assert_args_equivalence/2]).

-rabbit_boot_step({?MODULE,
                   [{description, "exchange type x-modulus-hash"},
                    {mfa, {rabbit_registry, register,
                           [exchange, <<"x-modulus-hash">>, ?MODULE]}},
                    {requires, rabbit_registry},
                    {enables, kernel_ready}]}).

%% 2^27
-define(PHASH2_RANGE, 134217728).

description() ->
    [{description, <<"Modulus Hashing Exchange">>}].

route(#exchange{name = Name}, Msg, _Options) ->
    Destinations = rabbit_router:match_routing_key(Name, ['_']),
    case length(Destinations) of
        0 ->
            [];
        Len ->
            %% We sort to guarantee stable routing after node restarts.
            DestinationsSorted = lists:sort(Destinations),
            Hash = erlang:phash2(mc:routing_keys(Msg), ?PHASH2_RANGE),
            Destination = lists:nth(Hash rem Len + 1, DestinationsSorted),
            [Destination]
    end.

info(_) -> [].
info(_, _) -> [].
serialise_events() -> false.
validate(_X) -> ok.
validate_binding(_X, _B) -> ok.
create(_Serial, _X) -> ok.
delete(_Serial, _X) -> ok.
policy_changed(_X1, _X2) -> ok.
add_binding(_Serial, _X, _B) -> ok.
remove_bindings(_Serial, _X, _Bs) -> ok.
assert_args_equivalence(X, Args) ->
    rabbit_exchange:assert_args_equivalence(X, Args).
