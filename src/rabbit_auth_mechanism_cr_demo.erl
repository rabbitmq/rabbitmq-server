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

-module(rabbit_auth_mechanism_cr_demo).
-include("rabbit.hrl").

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
