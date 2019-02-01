%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
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
        {_, Strategy} -> validate_strategy(Strategy);
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
