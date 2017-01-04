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
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_credential_validator_regexp).

-include("rabbit.hrl").

-behaviour(rabbit_credential_validator).

%%
%% API
%%

-export([validate_password/1]).
%% for tests
-export([validate_password/2]).

-spec validate_password(rabbit_types:password()) -> 'ok' | {'error', string()}.

validate_password(Password) ->
    {ok, Proplist} = application:get_env(rabbit, credential_validator),
    Regexp         = case proplists:get_value(regexp, Proplist) of
                         undefined -> {error, "rabbit.credential_validator.regexp config key is undefined"};
                         Value     -> rabbit_data_coercion:to_list(Value)
                     end,
    validate_password(Password, Regexp).


-spec validate_password(rabbit_types:password(), string()) -> 'ok' | {'error', string(), [any()]}.

validate_password(Password, Pattern) ->
    case re:run(rabbit_data_coercion:to_list(Password), Pattern) of
        {match, _} -> ok;
        nomatch    -> {error, "provided password does not match the validator regular expression"}
    end.
