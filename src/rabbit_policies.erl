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
%% Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_policies).
-behaviour(rabbit_policy_validator).

-include("rabbit.hrl").

-export([register/0, validate_policy/1]).

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
                          {policy_validator, <<"max-length">>}]],
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
  when is_integer(Value), Value >= 0, Value =< ?MAX_EXPIRY_TIMER ->
    ok;
validate_policy0(<<"message-ttl">>, Value) ->
    {error, "~p is not a valid message TTL", [Value]};

validate_policy0(<<"expires">>, Value)
  when is_integer(Value), Value >= 1, Value =< ?MAX_EXPIRY_TIMER ->
    ok;
validate_policy0(<<"expires">>, Value) ->
    {error, "~p is not a valid queue expiry", [Value]};

validate_policy0(<<"max-length">>, Value)
  when is_integer(Value), Value >= 0 ->
    ok;
validate_policy0(<<"max-length">>, Value) ->
    {error, "~p is not a valid maximum length", [Value]}.


