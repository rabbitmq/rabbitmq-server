%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_credential_validator_min_password_length).

-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(rabbit_credential_validator).

%% accommodates default (localhost-only) user credentials,
%% guest/guest
-define(DEFAULT_MIN_LENGTH, 5).

%%
%% API
%%

-export([validate/2]).
%% for tests
-export([validate/3]).

-spec validate(rabbit_types:username(), rabbit_types:password()) -> 'ok' | {'error', string()}.

validate(Username, Password) ->
    MinLength = case application:get_env(rabbit, credential_validator) of
                    undefined ->
                        ?DEFAULT_MIN_LENGTH;
                    {ok, Proplist}  ->
                        case proplists:get_value(min_length, Proplist) of
                            undefined -> ?DEFAULT_MIN_LENGTH;
                            Value     -> rabbit_data_coercion:to_integer(Value)
                        end
                end,
    validate(Username, Password, MinLength).


-spec validate(rabbit_types:username(), rabbit_types:password(), integer()) -> 'ok' | {'error', string(), [any()]}.

%% passwordless users
validate(_Username, undefined, MinLength) ->
    {error, rabbit_misc:format("minimum required password length is ~B", [MinLength])};
validate(_Username, Password, MinLength) ->
    case size(Password) >= MinLength of
        true  -> ok;
        false -> {error, rabbit_misc:format("minimum required password length is ~B", [MinLength])}
    end.
