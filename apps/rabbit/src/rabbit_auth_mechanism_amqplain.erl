%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_auth_mechanism_amqplain).
-include_lib("rabbit_common/include/rabbit.hrl").

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

-define(IS_STRING_TYPE(Type), Type =:= longstr orelse Type =:= shortstr).

handle_response(Response, _State) ->
    LoginTable = rabbit_binary_parser:parse_table(Response),
    case {lists:keysearch(<<"LOGIN">>, 1, LoginTable),
          lists:keysearch(<<"PASSWORD">>, 1, LoginTable)} of
        {{value, {_, UserType, User}},
         {value, {_, PassType, Pass}}} when ?IS_STRING_TYPE(UserType);
                                            ?IS_STRING_TYPE(PassType) ->
            rabbit_access_control:check_user_pass_login(User, Pass);
        {{value, {_, _UserType, _User}},
         {value, {_, _PassType, _Pass}}} ->
           {protocol_error,
            "AMQPLAIN auth info ~w uses unsupported type for LOGIN or PASSWORD field",
            [LoginTable]};
        _ ->
            {protocol_error,
             "AMQPLAIN auth info ~w is missing LOGIN or PASSWORD field",
             [LoginTable]}
    end.
