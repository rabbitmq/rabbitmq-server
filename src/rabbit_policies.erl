%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_policies).

%% Provides built-in policy parameter
%% validation functions.

-behaviour(rabbit_policy_validator).
-behaviour(rabbit_policy_merge_strategy).

-include("rabbit.hrl").

-export([register/0, validate_policy/1, merge_policy_value/3]).

-rabbit_boot_step({?MODULE,
                   [{description, "internal policies"},
                    {mfa, {rabbit_policies, register, []}},
                    {requires, rabbit_registry},
                    {enables, recovery}]}).

register() ->
    [rabbit_registry:register(Class, Name, ?MODULE) ||
        {Class, Name} <- [{policy_validator, <<"alternate-exchange">>},
                          {policy_validator, <<"dead-letter-exchange">>},
                          {policy_validator, <<"dead-letter-routing-key">>},
                          {policy_validator, <<"message-ttl">>},
                          {policy_validator, <<"expires">>},
                          {policy_validator, <<"max-length">>},
                          {policy_validator, <<"max-length-bytes">>},
                          {policy_validator, <<"queue-mode">>},
                          {policy_validator, <<"overflow">>},
                          {policy_validator, <<"delivery-limit">>},
                          {operator_policy_validator, <<"expires">>},
                          {operator_policy_validator, <<"message-ttl">>},
                          {operator_policy_validator, <<"max-length">>},
                          {operator_policy_validator, <<"max-length-bytes">>},
                          {operator_policy_validator, <<"delivery-limit">>},
                          {policy_merge_strategy, <<"expires">>},
                          {policy_merge_strategy, <<"message-ttl">>},
                          {policy_merge_strategy, <<"max-length">>},
                          {policy_merge_strategy, <<"max-length-bytes">>},
                          {policy_merge_strategy, <<"delivery-limit">>}]],
    ok.

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

validate_policy0(<<"queue-mode">>, <<"default">>) ->
    ok;
validate_policy0(<<"queue-mode">>, <<"lazy">>) ->
    ok;
validate_policy0(<<"queue-mode">>, Value) ->
    {error, "~p is not a valid queue-mode value", [Value]};
validate_policy0(<<"overflow">>, <<"drop-head">>) ->
    ok;
validate_policy0(<<"overflow">>, <<"reject-publish">>) ->
    ok;
validate_policy0(<<"overflow">>, Value) ->
    {error, "~p is not a valid overflow value", [Value]};

validate_policy0(<<"delivery-limit">>, Value)
  when is_integer(Value), Value >= 0 ->
    ok;
validate_policy0(<<"delivery-limit">>, Value) ->
    {error, "~p is not a valid delivery limit", [Value]}.

merge_policy_value(<<"message-ttl">>, Val, OpVal)      -> min(Val, OpVal);
merge_policy_value(<<"max-length">>, Val, OpVal)       -> min(Val, OpVal);
merge_policy_value(<<"max-length-bytes">>, Val, OpVal) -> min(Val, OpVal);
merge_policy_value(<<"expires">>, Val, OpVal)          -> min(Val, OpVal);
merge_policy_value(<<"delivery-limit">>, Val, OpVal)   -> min(Val, OpVal).
