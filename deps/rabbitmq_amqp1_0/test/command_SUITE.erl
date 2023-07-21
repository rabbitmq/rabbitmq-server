%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.


-module(command_SUITE).
-compile([export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("rabbit_amqp1_0.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-define(COMMAND, 'Elixir.RabbitMQ.CLI.Ctl.Commands.ListAmqp10ConnectionsCommand').

all() ->
    [
     {group, tests}
    ].

groups() ->
    [
     {tests, [],
      [
       merge_defaults,
       validate,
       run
      ]}
    ].

init_per_suite(Config) ->
    application:ensure_all_started(amqp10_client),
    rabbit_ct_helpers:log_environment(),
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(_Group, Config) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(
                Config, [{rmq_nodename_suffix, Suffix}]),
    rabbit_ct_helpers:run_setup_steps(
      Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

merge_defaults(_Config) ->
    {[<<"pid">>], #{verbose := false}} =
        ?COMMAND:merge_defaults([], #{}),

    {[<<"other_key">>], #{verbose := true}} =
        ?COMMAND:merge_defaults([<<"other_key">>], #{verbose => true}),

    {[<<"other_key">>], #{verbose := false}} =
        ?COMMAND:merge_defaults([<<"other_key">>], #{verbose => false}).

validate(_Config) ->
    ok = ?COMMAND:validate([], #{}),
    ok = ?COMMAND:validate([<<"recv_oct">>, <<"ssl">>], #{}),
    ok = ?COMMAND:validate([atom_to_binary(K) || K <- ?INFO_ITEMS], #{}),
    {validation_failure,{bad_info_key,[other]}} =
        ?COMMAND:validate([<<"other">>], #{}).

run(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Opts = #{node => Node, timeout => 2000, verbose => true},

    ?assertEqual([],  'Elixir.Enum':to_list(?COMMAND:run([], Opts))),

    Host = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    OpnConf = #{address => Host,
                port => Port,
                container_id => <<"my container">>,
                sasl => {plain, <<"guest">>, <<"guest">>}},
    {ok, Connection1} = amqp10_client:open_connection(OpnConf),
    receive {amqp10_event, {connection, Connection1, opened}} -> ok
    after 2000 -> ct:fail("connection not opened")
    end,

    Connections = [Infos0] = 'Elixir.Enum':to_list(?COMMAND:run([], Opts)),
    Infos = maps:from_list(Infos0),
    ?assertMatch(
       #{pid := Pid,
         auth_mechanism := <<"PLAIN">>,
         frame_max := FrameMax,
         user := <<"guest">>,
         vhost := <<"/">>,
         ssl := false,
         node := Node,
         protocol := {'AMQP', {1, 0}},
         host := _,
         port := Port,
         peer_host := _,
         peer_port := PeerPort,
         connection_state := running,
         recv_oct := N1,
         recv_cnt := N2,
         send_oct := N3,
         send_cnt := N4}
         when is_pid(Pid) andalso
              is_integer(FrameMax) andalso
              is_integer(Port) andalso
              is_integer(PeerPort) andalso
              is_integer(N1) andalso
              is_integer(N2) andalso
              is_integer(N3) andalso
              is_integer(N4),

              Infos),

    %% Listing a single field (e.g. connection_state) should work.
    ?assertEqual([[{connection_state, running}]],
                 'Elixir.Enum':to_list(?COMMAND:run([<<"connection_state">>],
                                                    Opts#{verbose => false}))),

    %% Create 2 AMQP 0.9.1 network connections (1 network, 1 direct).
    {ok, C1} = amqp_connection:start(#amqp_params_network{port = Port}),
    {ok, C2} = amqp_connection:start(#amqp_params_direct{node = Node}),
    %% Still, the same AMQP 1.0 connection as previously should be listed.
    ?assertEqual(Connections, 'Elixir.Enum':to_list(?COMMAND:run([], Opts))),
    ok = amqp_connection:close(C1),
    ok = amqp_connection:close(C2),

    %% Create a 2nd AMQP 1.0 connection.
    {ok, Connection2} = amqp10_client:open_connection(OpnConf),
    receive {amqp10_event, {connection, Connection2, opened}} -> ok
    after 2000 -> ct:fail("connection not opened")
    end,
    %% Now, 2 AMQP 1.0 connections should be listed.
    ?assertEqual(2, length('Elixir.Enum':to_list(?COMMAND:run([], Opts)))),

    ok = amqp10_client:close_connection(Connection1),
    receive {amqp10_event, {connection, Connection1, {closed, normal}}} -> ok
    after 2000 -> ct:fail("connection not closed")
    end,
    ok = amqp10_client:close_connection(Connection2),
    receive {amqp10_event, {connection, Connection2, {closed, normal}}} -> ok
    after 2000 -> ct:fail("connection not closed")
    end,
    ?assertEqual([],  'Elixir.Enum':to_list(?COMMAND:run([], Opts))).
