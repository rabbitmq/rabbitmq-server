%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_auth_mechanism_cr_demo).
-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(rabbit_auth_mechanism).

-export([description/0, should_offer/1, init/1, handle_response/2]).

-rabbit_boot_step({?MODULE,
                   [{description, "auth mechanism cr-demo"},
                    {mfa,         {rabbit_registry, register,
                                   [auth_mechanism, <<"RABBIT-CR-DEMO">>,
                                    ?MODULE]}},
                    {requires,    rabbit_registry},
                    {enables,     kernel_ready}]}).

-record(state, {username = undefined}).

%% Provides equivalent security to PLAIN but demos use of Connection.Secure(Ok)
%% START-OK: Username
%% SECURE: "Please tell me your password"
%% SECURE-OK: "My password is ~s", [Password]

description() ->
    [{description, <<"RabbitMQ Demo challenge-response authentication "
                     "mechanism">>}].

should_offer(_Sock) ->
    true.

init(_Sock) ->
    #state{}.

handle_response(Response, State = #state{username = undefined}) ->
    {challenge, <<"Please tell me your password">>,
     State#state{username = Response}};

handle_response(<<"My password is ", Password/binary>>,
                #state{username = Username}) ->
    rabbit_access_control:check_user_pass_login(Username, Password);
handle_response(Response, _State) ->
    {protocol_error, "Invalid response '~s'", [Response]}.
