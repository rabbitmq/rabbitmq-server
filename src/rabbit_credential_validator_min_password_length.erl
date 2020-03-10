%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_credential_validator_min_password_length).

-include("rabbit.hrl").

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
