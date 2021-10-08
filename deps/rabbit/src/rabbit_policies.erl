%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_policies).

%% Provides built-in policy parameter
%% validation functions.

-behaviour(rabbit_policy_validator).
-behaviour(rabbit_policy_merge_strategy).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([register/0, validate_policy/1, merge_policy_value/3]).

-rabbit_boot_step({?MODULE,
                   [{description, "internal policies"},
                    {mfa, {rabbit_policies, register, []}},
                    {requires, rabbit_registry},
                    {enables, recovery}]}).

register() ->
    %% Note: there are more validators registered from other modules,
    %% such as rabbit_mirror_queue_misc
    [rabbit_registry:register(Class, Name, ?MODULE) ||
        {Class, Name} <- [{policy_validator, <<"alternate-exchange">>},
                          {policy_validator, <<"dead-letter-exchange">>},
                          {policy_validator, <<"dead-letter-routing-key">>},
                          {policy_validator, <<"message-ttl">>},
                          {policy_validator, <<"expires">>},
                          {policy_validator, <<"max-length">>},
                          {policy_validator, <<"max-length-bytes">>},
                          {policy_validator, <<"max-in-memory-length">>},
                          {policy_validator, <<"max-in-memory-bytes">>},
                          {policy_validator, <<"queue-mode">>},
                          {policy_validator, <<"queue-version">>},
                          {policy_validator, <<"overflow">>},
                          {policy_validator, <<"delivery-limit">>},
                          {policy_validator, <<"max-age">>},
                          {policy_validator, <<"stream-max-segment-size-bytes">>},
                          {policy_validator, <<"queue-leader-locator">>},
                          {policy_validator, <<"initial-cluster-size">>},
                          {operator_policy_validator, <<"expires">>},
                          {operator_policy_validator, <<"message-ttl">>},
                          {operator_policy_validator, <<"max-length">>},
                          {operator_policy_validator, <<"max-length-bytes">>},
                          {operator_policy_validator, <<"max-in-memory-length">>},
                          {operator_policy_validator, <<"max-in-memory-bytes">>},
                          {operator_policy_validator, <<"delivery-limit">>},
                          {policy_merge_strategy, <<"expires">>},
                          {policy_merge_strategy, <<"message-ttl">>},
                          {policy_merge_strategy, <<"max-length">>},
                          {policy_merge_strategy, <<"max-length-bytes">>},
                          {policy_merge_strategy, <<"max-in-memory-length">>},
                          {policy_merge_strategy, <<"max-in-memory-bytes">>},
                          {policy_merge_strategy, <<"delivery-limit">>}]],
    ok.

-spec validate_policy([{binary(), term()}]) -> rabbit_policy_validator:validate_results().

validate_policy(Terms) ->
    lists:foldl(fun ({Key, Value}, ok) -> validate_policy0(Key, Value);
                    (_, Error)         -> Error
                end, ok, Terms).

validate_policy0(<<"alternate-exchange">>, Value)
  when is_binary(Value) ->
    ok;
validate_policy0(<<"alternate-exchange">>, Value) ->
    {error, "~p is not a valid alternate exchange name", [Value]};

validate_policy0(<<"dead-letter-exchange">>, Value)
  when is_binary(Value) ->
    ok;
validate_policy0(<<"dead-letter-exchange">>, Value) ->
    {error, "~p is not a valid dead letter exchange name", [Value]};

validate_policy0(<<"dead-letter-routing-key">>, Value)
  when is_binary(Value) ->
    ok;
validate_policy0(<<"dead-letter-routing-key">>, Value) ->
    {error, "~p is not a valid dead letter routing key", [Value]};

validate_policy0(<<"message-ttl">>, Value)
  when is_integer(Value), Value >= 0 ->
    ok;
validate_policy0(<<"message-ttl">>, Value) ->
    {error, "~p is not a valid message TTL", [Value]};

validate_policy0(<<"expires">>, Value)
  when is_integer(Value), Value >= 1 ->
    ok;
validate_policy0(<<"expires">>, Value) ->
    {error, "~p is not a valid queue expiry", [Value]};

validate_policy0(<<"max-length">>, Value)
  when is_integer(Value), Value >= 0 ->
    ok;
validate_policy0(<<"max-length">>, Value) ->
    {error, "~p is not a valid maximum length", [Value]};

validate_policy0(<<"max-length-bytes">>, Value)
  when is_integer(Value), Value >= 0 ->
    ok;
validate_policy0(<<"max-length-bytes">>, Value) ->
    {error, "~p is not a valid maximum length in bytes", [Value]};

validate_policy0(<<"max-in-memory-length">>, Value)
  when is_integer(Value), Value >= 0 ->
    ok;
validate_policy0(<<"max-in-memory-length">>, Value) ->
    {error, "~p is not a valid maximum memory in bytes", [Value]};

validate_policy0(<<"max-in-memory-bytes">>, Value)
  when is_integer(Value), Value >= 0 ->
    ok;
validate_policy0(<<"max-in-memory-bytes">>, Value) ->
    {error, "~p is not a valid maximum memory in bytes", [Value]};

validate_policy0(<<"queue-mode">>, <<"default">>) ->
    ok;
validate_policy0(<<"queue-mode">>, <<"lazy">>) ->
    ok;
validate_policy0(<<"queue-mode">>, Value) ->
    {error, "~p is not a valid queue-mode value", [Value]};

validate_policy0(<<"queue-version">>, 1) ->
    ok;
validate_policy0(<<"queue-version">>, 2) ->
    ok;
validate_policy0(<<"queue-version">>, Value) ->
    {error, "~p is not a valid queue-version value", [Value]};

validate_policy0(<<"overflow">>, <<"drop-head">>) ->
    ok;
validate_policy0(<<"overflow">>, <<"reject-publish">>) ->
    ok;
validate_policy0(<<"overflow">>, <<"reject-publish-dlx">>) ->
    ok;
validate_policy0(<<"overflow">>, Value) ->
    {error, "~p is not a valid overflow value", [Value]};

validate_policy0(<<"delivery-limit">>, Value)
  when is_integer(Value), Value >= 0 ->
    ok;
validate_policy0(<<"delivery-limit">>, Value) ->
    {error, "~p is not a valid delivery limit", [Value]};

validate_policy0(<<"max-age">>, Value) ->
    case rabbit_amqqueue:check_max_age(Value) of
        {error, _} ->
            {error, "~p is not a valid max age", [Value]};
        _ ->
            ok
    end;

validate_policy0(<<"queue-leader-locator">>, <<"client-local">>) ->
    ok;
validate_policy0(<<"queue-leader-locator">>, <<"random">>) ->
    ok;
validate_policy0(<<"queue-leader-locator">>, <<"least-leaders">>) ->
    ok;
validate_policy0(<<"queue-leader-locator">>, Value) ->
    {error, "~p is not a valid queue leader locator value", [Value]};

validate_policy0(<<"initial-cluster-size">>, Value)
  when is_integer(Value), Value >= 0 ->
    ok;
validate_policy0(<<"initial-cluster-size">>, Value) ->
    {error, "~p is not a valid cluster size", [Value]};

validate_policy0(<<"stream-max-segment-size-bytes">>, Value)
  when is_integer(Value), Value >= 0 ->
    ok;
validate_policy0(<<"stream-max-segment-size-bytes">>, Value) ->
    {error, "~p is not a valid segment size", [Value]}.

merge_policy_value(<<"message-ttl">>, Val, OpVal)      -> min(Val, OpVal);
merge_policy_value(<<"max-length">>, Val, OpVal)       -> min(Val, OpVal);
merge_policy_value(<<"max-length-bytes">>, Val, OpVal) -> min(Val, OpVal);
merge_policy_value(<<"max-in-memory-length">>, Val, OpVal) -> min(Val, OpVal);
merge_policy_value(<<"max-in-memory-bytes">>, Val, OpVal) -> min(Val, OpVal);
merge_policy_value(<<"expires">>, Val, OpVal)          -> min(Val, OpVal);
merge_policy_value(<<"delivery-limit">>, Val, OpVal)   -> min(Val, OpVal);
%% use operator policy value for booleans
merge_policy_value(_Key, Val, OpVal) when is_boolean(Val) andalso is_boolean(OpVal) -> OpVal.
