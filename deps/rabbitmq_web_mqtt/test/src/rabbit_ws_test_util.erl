%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_ws_test_util).

-include_lib("common_test/include/ct.hrl").

-export([update_app_env/3, get_web_mqtt_port_str/1,
         mqtt_3_1_1_connect_packet/0]).

update_app_env(Config, Key, Val) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
                                      application, set_env,
                                      [rabbitmq_web_mqtt, Key, Val]),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
                                      application, stop,
                                      [rabbitmq_web_mqtt]),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
                                      application, start,
                                      [rabbitmq_web_mqtt]).

get_web_mqtt_port_str(Config) ->
    Port = case ?config(protocol, Config) of
               "ws" ->
                   rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_web_mqtt);
               "wss" ->
                   rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_web_mqtt_tls)
           end,
    integer_to_list(Port).

mqtt_3_1_1_connect_packet() ->
    <<16,
    24,
    0,
    4,
    77,
    81,
    84,
    84,
    4,
    2,
    0,
    60,
    0,
    12,
    84,
    101,
    115,
    116,
    67,
    111,
    110,
    115,
    117,
    109,
    101,
    114>>.
