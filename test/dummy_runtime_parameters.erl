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

-module(dummy_runtime_parameters).
-behaviour(rabbit_runtime_parameter).
-behaviour(rabbit_policy_validator).

-include("rabbit.hrl").

-export([validate/5, notify/5, notify_clear/4]).
-export([register/0, unregister/0]).
-export([validate_policy/1]).
-export([register_policy_validator/0, unregister_policy_validator/0]).

%----------------------------------------------------------------------------

register() ->
    rabbit_registry:register(runtime_parameter, <<"test">>, ?MODULE).

unregister() ->
    rabbit_registry:unregister(runtime_parameter, <<"test">>).

validate(_, <<"test">>, <<"good">>,  _Term, _User)      -> ok;
validate(_, <<"test">>, <<"maybe">>, <<"good">>, _User) -> ok;
validate(_, <<"test">>, <<"admin">>, _Term, none)       -> ok;
validate(_, <<"test">>, <<"admin">>, _Term, User) ->
    case lists:member(administrator, User#user.tags) of
        true  -> ok;
        false -> {error, "meh", []}
    end;
validate(_, <<"test">>, _, _, _)                        -> {error, "meh", []}.

notify(_, _, _, _, _) -> ok.
notify_clear(_, _, _, _) -> ok.

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
