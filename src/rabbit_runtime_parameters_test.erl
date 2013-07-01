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

-module(rabbit_runtime_parameters_test).
-behaviour(rabbit_runtime_parameter).
-behaviour(rabbit_policy_validator).

-export([validate/4, notify/4, notify_clear/3]).
-export([register/0, unregister/0]).
-export([validate_policy/1]).
-export([register_policy_validator/0, unregister_policy_validator/0]).

%----------------------------------------------------------------------------

register() ->
    rabbit_registry:register(runtime_parameter, <<"test">>, ?MODULE).

unregister() ->
    rabbit_registry:unregister(runtime_parameter, <<"test">>).

validate(_, <<"test">>, <<"good">>,  _Term)      -> ok;
validate(_, <<"test">>, <<"maybe">>, <<"good">>) -> ok;
validate(_, <<"test">>, _, _)                    -> {error, "meh", []}.

notify(_, _, _, _) -> ok.
notify_clear(_, _, _) -> ok.

%----------------------------------------------------------------------------

register_policy_validator() ->
    rabbit_registry:register(policy_validator, <<"testeven">>, ?MODULE),
    rabbit_registry:register(policy_validator, <<"testpos">>,  ?MODULE).

unregister_policy_validator() ->
    rabbit_registry:unregister(policy_validator, <<"testeven">>),
    rabbit_registry:unregister(policy_validator, <<"testpos">>).

validate_policy([{<<"testeven">>, Terms}]) when is_list(Terms) ->
    case  length(Terms) rem 2 =:= 0 of
        true  -> ok;
        false -> {error, "meh", []}
    end;

validate_policy([{<<"testpos">>, Terms}]) when is_list(Terms) ->
    case lists:all(fun (N) -> is_integer(N) andalso N > 0 end, Terms) of
        true  -> ok;
        false -> {error, "meh", []}
    end;

validate_policy(_) ->
    {error, "meh", []}.
