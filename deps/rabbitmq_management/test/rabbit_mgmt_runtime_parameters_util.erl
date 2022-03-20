%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_runtime_parameters_util).
-behaviour(rabbit_runtime_parameter).
-behaviour(rabbit_policy_validator).

-include_lib("rabbit_common/include/rabbit.hrl").

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
