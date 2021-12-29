%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
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
