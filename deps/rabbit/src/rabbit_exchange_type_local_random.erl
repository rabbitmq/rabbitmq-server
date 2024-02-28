%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_exchange_type_local_random).
-behaviour(rabbit_exchange_type).
-include_lib("rabbit_common/include/rabbit.hrl").

-rabbit_boot_step({?MODULE,
                   [{description, "exchange type local random"},
                    {mfa, {rabbit_registry, register,
                           [exchange, <<"x-local-random">>, ?MODULE]}},
                    {requires, rabbit_registry},
                    {enables, kernel_ready}
                   ]}).

-export([add_binding/3,
         assert_args_equivalence/2,
         create/2,
         delete/2,
         policy_changed/2,
         description/0,
         recover/2,
         remove_bindings/3,
         validate_binding/2,
         route/3,
         serialise_events/0,
         validate/1,
         info/1,
         info/2
        ]).

description() ->
    [{name, <<"x-local-random">>},
     {description, <<"Picks one random local binding (queue) to route via (to).">>}].

route(#exchange{name = Name}, _Msg, _Opts) ->
    Matches = rabbit_router:match_routing_key(Name, [<<>>]),
    case lists:filter(fun filter_local_queue/1, Matches) of
        [] ->
            [];
        [_] = One ->
            One;
        LocalMatches ->
            Rand = rand:uniform(length(LocalMatches)),
            [lists:nth(Rand, LocalMatches)]
    end.

info(_X) -> [].
info(_X, _) -> [].
serialise_events() -> false.
validate(_X) -> ok.
create(_Serial, _X) -> ok.
recover(_X, _Bs) -> ok.
delete(_Serial, _X) -> ok.
policy_changed(_X1, _X2) -> ok.
add_binding(_Serial, _X, _B) -> ok.
remove_bindings(_Serial, _X, _Bs) -> ok.

validate_binding(_X, #binding{destination = Dest,
                              key = <<>>}) ->
    case rabbit_amqqueue:lookup(Dest) of
        {ok, Q} ->
            case amqqueue:get_type(Q) of
                rabbit_classic_queue ->
                    ok;
                Type ->
                    {error, {binding_invalid,
                             "Queue type ~ts not valid for this exchange type",
                             [Type]}}
            end;
        _ ->
            {error, {binding_invalid,
                     "Destination not found",
                     []}}
    end;
validate_binding(_X, #binding{key = BKey}) ->
    {error, {binding_invalid,
             "Non empty binding '~s' key not permitted",
             [BKey]}}.

assert_args_equivalence(X, Args) ->
    rabbit_exchange:assert_args_equivalence(X, Args).

filter_local_queue(QName) ->
    %% TODO: introduce lookup function that _only_ gets the pid
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} ->
            case amqqueue:get_pid(Q) of
                Pid when is_pid(Pid) andalso
                         node(Pid) =:= node() ->
                    is_process_alive(Pid);
                _ ->
                    false
            end;
        _ ->
            false
    end.
