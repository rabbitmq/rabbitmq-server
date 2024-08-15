%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_auth_mechanism_anonymous).
-behaviour(rabbit_auth_mechanism).

-export([description/0, should_offer/1, init/1, handle_response/2]).
-export([credentials/0]).

-define(STATE, []).

-rabbit_boot_step(
   {?MODULE,
    [{description, "auth mechanism anonymous"},
     {mfa, {rabbit_registry, register, [auth_mechanism, <<"ANONYMOUS">>, ?MODULE]}},
     {requires, rabbit_registry},
     {enables, kernel_ready}]}).

description() ->
    [{description, <<"SASL ANONYMOUS authentication mechanism">>}].

should_offer(_Sock) ->
    case credentials() of
        {ok, _, _} ->
            true;
        error ->
            false
    end.

init(_Sock) ->
    ?STATE.

handle_response(_Response, ?STATE) ->
    {ok, User, Pass} = credentials(),
    rabbit_access_control:check_user_pass_login(User, Pass).

-spec credentials() ->
    {ok, rabbit_types:username(), rabbit_types:password()} | error.
credentials() ->
    case application:get_env(rabbit, anonymous_login_user) of
        {ok, User} when is_binary(User) ->
            case application:get_env(rabbit, anonymous_login_pass) of
                {ok, Pass} when is_binary(Pass) ->
                    {ok, User, Pass};
                _ ->
                    error
            end;
        _ ->
            error
    end.
