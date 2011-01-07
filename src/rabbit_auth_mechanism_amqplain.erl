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

-module(rabbit_auth_mechanism_amqplain).
-include("rabbit.hrl").

-behaviour(rabbit_auth_mechanism).

-export([description/0, init/1, handle_response/2]).

-include("rabbit_auth_mechanism_spec.hrl").

-rabbit_boot_step({?MODULE,
                   [{description, "auth mechanism amqplain"},
                    {mfa,         {rabbit_registry, register,
                                   [auth_mechanism, <<"AMQPLAIN">>, ?MODULE]}},
                    {requires,    rabbit_registry},
                    {enables,     kernel_ready}]}).

%% AMQPLAIN, as used by Qpid Python test suite. The 0-8 spec actually
%% defines this as PLAIN, but in 0-9 that definition is gone, instead
%% referring generically to "SASL security mechanism", i.e. the above.

description() ->
    [{name, <<"AMQPLAIN">>},
     {description, <<"QPid AMQPLAIN mechanism">>}].

init(_Sock) ->
    [].

handle_response(Response, _State) ->
    LoginTable = rabbit_binary_parser:parse_table(Response),
    case {lists:keysearch(<<"LOGIN">>, 1, LoginTable),
          lists:keysearch(<<"PASSWORD">>, 1, LoginTable)} of
        {{value, {_, longstr, User}},
         {value, {_, longstr, Pass}}} ->
            rabbit_access_control:check_user_pass_login(User, Pass);
        _ ->
            {protocol_error,
             "AMQPLAIN auth info ~w is missing LOGIN or PASSWORD field",
              [LoginTable]}
    end.
