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
%% Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
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
    {<<0, Username/binary, 0, Password/binary>>, _State}.

amqplain(none, _, init) ->
    {<<"AMQPLAIN">>, []};
amqplain(none, #amqp_params_network{username = Username,
                                    password = Password}, _State) ->
    LoginTable = [{<<"LOGIN">>,    longstr, Username},
                  {<<"PASSWORD">>, longstr, Password}],
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
    {<<"My password is ", Password/binary>>, 2}.
