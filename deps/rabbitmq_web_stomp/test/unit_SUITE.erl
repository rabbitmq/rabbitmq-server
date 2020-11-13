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
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(unit_SUITE).

-include_lib("common_test/include/ct.hrl").

-compile(export_all).

all() ->
    [
        get_tcp_conf,
        get_tcp_port,
        get_binding_address
    ].


%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

get_tcp_conf(_Config) ->
    [{ip, {127, 0, 0, 1}}, {port, 15674}] = lists:sort(rabbit_web_stomp_listener:get_tcp_conf(
        [{ip, {127, 0, 0, 1}}, {port, 1245}], 15674
    )),
    [{ip, {127, 0, 0, 1}}, {port, 15674}] = lists:sort(rabbit_web_stomp_listener:get_tcp_conf(
        [{ip, {127, 0, 0, 1}}], 15674
    )),
    [{port, 15674}] = lists:sort(rabbit_web_stomp_listener:get_tcp_conf(
        [], 15674
    )),
    ok.

get_tcp_port(_Config) ->
    15674 = rabbit_web_stomp_listener:get_tcp_port([{port, 15674}]),
    15674 = rabbit_web_stomp_listener:get_tcp_port([{port, 15674}, {tcp_config, []}]),
    12345 = rabbit_web_stomp_listener:get_tcp_port([{port, 15674}, {tcp_config, [{port, 12345}]}]),
    ok.

get_binding_address(_Config) ->
    "0.0.0.0" = rabbit_web_stomp_listener:get_binding_address([]),
    "192.168.1.10" = rabbit_web_stomp_listener:get_binding_address([{ip, "192.168.1.10"}]),
    "192.168.1.10" = rabbit_web_stomp_listener:get_binding_address([{ip, {192, 168, 1, 10}}]),
    ok.
