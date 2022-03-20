%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_ws_test_util).

-export([update_app_env/3, get_web_stomp_port_str/1]).

update_app_env(Config, Key, Val) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
                                      application, set_env,
                                      [rabbitmq_web_stomp, Key, Val]),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
                                      application, stop,
                                      [rabbitmq_web_stomp]),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
                                      application, start,
                                      [rabbitmq_web_stomp]).

get_web_stomp_port_str(Config) ->
    Port = case rabbit_ct_helpers:get_config(Config, protocol) of
        "ws" ->
            rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_web_stomp);
        "wss" ->
            rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_web_stomp_tls)
    end,
    integer_to_list(Port).
