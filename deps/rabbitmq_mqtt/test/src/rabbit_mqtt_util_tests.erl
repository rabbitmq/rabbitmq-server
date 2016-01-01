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
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_mqtt_util_tests).

-include_lib("eunit/include/eunit.hrl").

all_test_() ->
    {setup,
     fun setup/0,
     [fun coerce_exchange/0,
      fun coerce_vhost/0,
      fun coerce_default_user/0,
      fun coerce_default_pass/0]}.

setup() ->
    application:load(rabbitmq_mqtt).

coerce_exchange() ->
    ?assertEqual(<<"amq.topic">>, rabbit_mqtt_util:env(exchange)).

coerce_vhost() ->
    ?assertEqual(<<"/">>, rabbit_mqtt_util:env(vhost)).

coerce_default_user() ->
    ?assertEqual(<<"guest_user">>, rabbit_mqtt_util:env(default_user)).

coerce_default_pass() ->
    ?assertEqual(<<"guest_pass">>, rabbit_mqtt_util:env(default_pass)).
