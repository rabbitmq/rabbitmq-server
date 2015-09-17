%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2011-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(credit_flow_test).

-export([test_credit_flow_settings/0]).

test_credit_flow_settings() ->
    %% default values
    passed = test_proc(200, 50),

    application:set_env(rabbit, credit_flow_default_credit, {100, 20}),
    passed = test_proc(100, 20),

    application:unset_env(rabbit, credit_flow_default_credit),
    % back to defaults
    passed = test_proc(200, 50),
    passed.

test_proc(InitialCredit, MoreCreditAfter) ->
    Pid = spawn(fun dummy/0),
    Pid ! {credit, self()},
    {InitialCredit, MoreCreditAfter} =
        receive
            {credit, Val} -> Val
        end,
    passed.

dummy() ->
    credit_flow:send(self()),
    receive
        {credit, From} ->
            From ! {credit, get(credit_flow_default_credit)};
        _      ->
            dummy()
    end.
