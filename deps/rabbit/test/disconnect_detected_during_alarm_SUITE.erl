%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(disconnect_detected_during_alarm_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

all() ->
    [
      {group, disconnect_detected_during_alarm}
    ].

groups() ->
    [
      %% Test previously executed with the multi-node target.
      {disconnect_detected_during_alarm, [], [
          disconnect_detected_during_alarm %% Trigger alarm.
        ]}
    ].

group(_) ->
    [].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [
                                            {rmq_nodename_suffix, Group},
                                            {rmq_nodes_count, 1}
                                           ]),
    rabbit_ct_helpers:run_steps(Config1,
                                rabbit_ct_broker_helpers:setup_steps() ++
                                    rabbit_ct_client_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_client_helpers:teardown_steps() ++
                                    rabbit_ct_broker_helpers:teardown_steps()).

end_per_group1(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% ---------------------------------------------------------------------------
%% Testcase
%% ---------------------------------------------------------------------------

disconnect_detected_during_alarm(Config) ->
    A = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    %% Set a low memory high watermark.
    rabbit_ct_broker_helpers:rabbitmqctl(Config, A,
      ["set_vm_memory_high_watermark", "0.000000001"]),

    %% Open a connection and a channel.
    Port = rabbit_ct_broker_helpers:get_node_config(Config, A, tcp_port_amqp),
    Heartbeat = 1,
    {ok, Conn} = amqp_connection:start(
      #amqp_params_network{port = Port,
                           heartbeat = Heartbeat}),
    {ok, Ch} = amqp_connection:open_channel(Conn),

    amqp_connection:register_blocked_handler(Conn, self()),
    Publish = #'basic.publish'{routing_key = <<"nowhere-to-go">>},
    amqp_channel:cast(Ch, Publish, #amqp_msg{payload = <<"foobar">>}),
    receive
        % Check that connection was indeed blocked
        #'connection.blocked'{} -> ok
    after
        1000 -> exit(connection_was_not_blocked)
    end,

    %% Connection is blocked, now we should forcefully kill it
    {'EXIT', _} = (catch amqp_connection:close(Conn, 10)),

    ListConnections =
        fun() ->
            rpc:call(A, rabbit_networking, connection_info_all, [])
        end,

    %% We've already disconnected, but blocked connection still should still linger on.
    [SingleConn] = ListConnections(),
    blocked = rabbit_misc:pget(state, SingleConn),

    %% It should definitely go away after 2 heartbeat intervals.
    timer:sleep(round(2.5 * 1000 * Heartbeat)),
    [] = ListConnections(),

    passed.
