%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_queue_location_validator).
-behaviour(rabbit_policy_validator).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("amqqueue.hrl").

-export([validate_policy/1, validate_strategy/1]).

-rabbit_boot_step({?MODULE,
                   [{description, "Queue location policy validation"},
                    {mfa, {rabbit_registry, register,
                           [policy_validator,
                            <<"queue-master-locator">>,
                            ?MODULE]}},
		    {requires, rabbit_registry},
		    {enables, recovery}]}).

validate_policy(KeyList) ->
    case proplists:lookup(<<"queue-master-locator">> , KeyList) of
        {_, Strategy} -> case validate_strategy(Strategy) of
                             {error, _, _} = Er -> Er;
                             _ -> ok
                         end;
        _             -> {error, "queue-master-locator undefined"}
    end.

validate_strategy(Strategy) ->
    case module(Strategy) of
        R = {ok, _M} -> R;
        _            ->
            {error, "~p invalid queue-master-locator value", [Strategy]}
    end.

policy(Policy, Q) ->
    case rabbit_policy:get(Policy, Q) of
        undefined -> none;
        P         -> P
    end.

module(Q) when ?is_amqqueue(Q) ->
    case policy(<<"queue-master-locator">>, Q) of
        undefined -> no_location_strategy;
        Mode      -> module(Mode)
    end;
module(Strategy) when is_binary(Strategy) ->
    case rabbit_registry:binary_to_type(Strategy) of
        {error, not_found} -> no_location_strategy;
        T ->
            case rabbit_registry:lookup_module(queue_master_locator, T) of
                {ok, Module} ->
                    case code:which(Module) of
                        non_existing -> no_location_strategy;
                        _            -> {ok, Module}
                    end;
                _            ->
                    no_location_strategy
            end
    end;
module(Strategy) ->
    module(rabbit_data_coercion:to_binary(Strategy)).
