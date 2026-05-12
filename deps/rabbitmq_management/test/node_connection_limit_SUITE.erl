%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% Tests `management.tcp.max_connections`.

-module(node_connection_limit_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Distinct from Ranch's internal default of 1024 so a silent
%% propagation failure surfaces as an assertion mismatch.
-define(CAP, 4096).

all() ->
    [configured_cap_reaches_ranch,
     limit_can_be_modified_at_runtime].

init_per_suite(Config0) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config0,
                [{rmq_nodename_suffix, ?MODULE},
                 {rmq_nodes_count, 1}]),
    rabbit_ct_helpers:run_setup_steps(
      Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

configured_cap_reaches_ranch(Config) ->
    Ref = mgmt_ranch_ref(Config),
    restart_listener_with_cap(Config, ?CAP),
    ?assertEqual(?CAP,
                 rabbit_ct_broker_helpers:rpc(
                   Config, 0, ranch, get_max_connections, [Ref])).

limit_can_be_modified_at_runtime(Config) ->
    Ref = mgmt_ranch_ref(Config),
    Original = rabbit_ct_broker_helpers:rpc(
                 Config, 0, ranch, get_max_connections, [Ref]),
    try
        ok = rabbit_ct_broker_helpers:rpc(
               Config, 0, ranch, set_max_connections, [Ref, 7]),
        ?assertEqual(7,
                     rabbit_ct_broker_helpers:rpc(
                       Config, 0, ranch, get_max_connections, [Ref]))
    after
        rabbit_ct_broker_helpers:rpc(
          Config, 0, ranch, set_max_connections, [Ref, Original])
    end.

%% --- helpers ---

restart_listener_with_cap(Config, Cap) ->
    Existing = rabbit_ct_broker_helpers:rpc(
                 Config, 0, application, get_env,
                 [rabbitmq_management, tcp_config, []]),
    Updated = lists:keystore(max_connections, 1, Existing,
                             {max_connections, Cap}),
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, application, set_env,
           [rabbitmq_management, tcp_config, Updated]),
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, rabbit_mgmt_app, reset_dispatcher, [[]]).

mgmt_ranch_ref(Config) ->
    Port = rabbit_ct_broker_helpers:get_node_config(
             Config, 0, tcp_port_mgmt),
    rabbit_ct_broker_helpers:rpc(
      Config, 0, rabbit_networking, ranch_ref, [[{port, Port}]]).
