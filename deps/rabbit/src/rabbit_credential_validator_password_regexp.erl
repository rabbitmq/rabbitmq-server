%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%


%% A `rabbit_credential_validator` implementation that matches
%% password against a pre-configured regular expression.
-module(rabbit_credential_validator_password_regexp).

-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(rabbit_credential_validator).

%%
%% API
%%

-export([validate/2]).
%% for tests
-export([validate/3]).

-spec validate(rabbit_types:username(), rabbit_types:password()) -> 'ok' | {'error', string()}.

validate(Username, Password) ->
    {ok, Proplist} = application:get_env(rabbit, credential_validator),
    Regexp         = case proplists:get_value(regexp, Proplist) of
                         undefined -> {error, "rabbit.credential_validator.regexp config key is undefined"};
                         Value     -> rabbit_data_coercion:to_list(Value)
                     end,
    validate(Username, Password, Regexp).


-spec validate(rabbit_types:username(), rabbit_types:password(), string()) -> 'ok' | {'error', string(), [any()]}.

validate(_Username, Password, Pattern) ->
    case re:run(rabbit_data_coercion:to_list(Password), Pattern) of
        {match, _} -> ok;
        nomatch    -> {error, "provided password does not match the validator regular expression"}
    end.
