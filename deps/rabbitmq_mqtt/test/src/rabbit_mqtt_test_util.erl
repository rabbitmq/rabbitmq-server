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
%% Copyright (c) 2007-2015 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_mqtt_test_util).

-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% coerce exchange and vhost to binaries
%%--------------------------------------------------------------------

%% coerce in case of string
coerce_exchange_test() ->
    <<"amq.direct">> = rabbit_mqtt_util:coerce(exchange, "amq.direct").

%% leave unchanged when already binary
not_coerce_exchange_test() ->
    <<"amq.direct">> = rabbit_mqtt_util:coerce(exchange, <<"amq.direct">>).

%% coerce in case of string
coerce_vhost_test() ->
    <<"/">> = rabbit_mqtt_util:coerce(vhost, "/").

%% leave unchanged when already binary
not_coerce_vhost_test() ->
    <<"/">> = rabbit_mqtt_util:coerce(exchange, <<"/">>).

%% coerce in case of string
coerce_default_user_test() ->
    <<"guest">> = rabbit_mqtt_util:coerce(default_user, "guest").

%% leave unchanged when already binary
not_coerce_default_user_test() ->
    <<"guest">> = rabbit_mqtt_util:coerce(default_user, <<"guest">>).

%% coerce in case of string
coerce_default_pass_test() ->
    <<"guest">> = rabbit_mqtt_util:coerce(default_pass, "guest").

%% leave unchanged when already binary
not_coerce_default_pass_test() ->
    <<"guest">> = rabbit_mqtt_util:coerce(default_pass, <<"guest">>).

%% leave unchanged any other (atom, value)
not_coerce_test() ->
    [1,2,3] = rabbit_mqtt_util:coerce(foo, [1,2,3]).
