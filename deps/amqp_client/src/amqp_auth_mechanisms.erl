%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @private
-module(amqp_auth_mechanisms).

-include("amqp_client.hrl").

-export([plain/3, amqplain/3, external/3, crdemo/3]).

%%---------------------------------------------------------------------------

plain(none, _, init) ->
    {<<"PLAIN">>, []};
plain(none, #amqp_params_network{username = Username,
                                 password = Password}, _State) ->
    DecryptedPassword = credentials_obfuscation:decrypt(Password),
    {<<0, Username/binary, 0, DecryptedPassword/binary>>, _State}.

amqplain(none, _, init) ->
    {<<"AMQPLAIN">>, []};
amqplain(none, #amqp_params_network{username = Username,
                                    password = Password}, _State) ->
    LoginTable = [{<<"LOGIN">>,    longstr, Username},
                  {<<"PASSWORD">>, longstr, credentials_obfuscation:decrypt(Password)}],
    {rabbit_binary_generator:generate_table(LoginTable), _State}.

external(none, _, init) ->
    {<<"EXTERNAL">>, []};
external(none, _, _State) ->
    {<<"">>, _State}.

crdemo(none, _, init) ->
    {<<"RABBIT-CR-DEMO">>, 0};
crdemo(none, #amqp_params_network{username = Username}, 0) ->
    {Username, 1};
crdemo(<<"Please tell me your password">>,
       #amqp_params_network{password = Password}, 1) ->
    DecryptedPassword = credentials_obfuscation:decrypt(Password),
    {<<"My password is ", DecryptedPassword/binary>>, 2}.
