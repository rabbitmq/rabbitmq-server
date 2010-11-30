%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2010 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2010 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_auth_mechanism_cr_demo).
-include("rabbit.hrl").

-behaviour(rabbit_auth_mechanism).

-export([description/0, init/1, handle_response/2]).

-include("rabbit_auth_mechanism_spec.hrl").

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
%% SECURE-OK: Password

description() ->
    [{name, <<"RABBIT-CR-DEMO">>},
     {description, <<"RabbitMQ Demo challenge-response authentication "
                     "mechanism">>}].

init(_Sock) ->
    #state{}.

handle_response(Response, State = #state{username = undefined}) ->
    {challenge, <<"Please tell me your password">>,
     State#state{username = Response}};

handle_response(Response, #state{username = Username}) ->
    rabbit_access_control:check_user_pass_login(Username, Response).
