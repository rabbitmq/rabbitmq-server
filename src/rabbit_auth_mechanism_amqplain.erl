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
%% Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_auth_mechanism_amqplain).
-include("rabbit.hrl").

-behaviour(rabbit_auth_mechanism).

-export([description/0, should_offer/1, init/1, handle_response/2]).

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
    [{description, <<"QPid AMQPLAIN mechanism">>}].

should_offer(_Sock) ->
    true.

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
