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
%%   The Original Code is RabbitMQ Management Console.
%%
%%   The Initial Developer of the Original Code is GoPivotal, Inc.
%%   Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_ws_test_util).

-export([update_app_env/3, get_web_mqtt_port_str/1]).

update_app_env(Config, Key, Val) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
                                      application, set_env,
                                      [rabbitmq_web_mqtt, Key, Val]),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
                                      application, stop,
                                      [rabbitmq_web_mqtt]),
    rabbit_ct_broker_helpers:rpc(Config, 0,
                                      ranch, stop_listener,
                                      [web_mqtt]),
    rabbit_ct_broker_helpers:rpc(Config, 0, ranch, stop_listener, [web_mqtt_secure]),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
                                      application, start,
                                      [rabbitmq_web_mqtt]).

get_web_mqtt_port_str(Config) ->
    Port = case rabbit_ct_helpers:get_config(Config, protocol) of
        "ws" ->
            rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_web_mqtt);
        "wss" ->
            rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_web_mqtt_tls)
    end,
    integer_to_list(Port).
