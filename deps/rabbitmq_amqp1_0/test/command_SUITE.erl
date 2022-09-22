%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.


-module(command_SUITE).
-compile([export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("rabbit_amqp1_0.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-define(COMMAND, 'Elixir.RabbitMQ.CLI.Ctl.Commands.ListAmqp10ConnectionsCommand').

all() ->
    [
       {group, non_parallel_tests}
    ].

groups() ->
    [
     {non_parallel_tests, [], [
                               merge_defaults,
                               validate,
                               when_no_connections,
                               when_one_connection,
                               when_one_amqp091_connection
                              ]}
    ].

init_per_suite(Config) ->
    application:ensure_all_started(amqp10_client),
    rabbit_ct_helpers:log_environment(),
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(Group, Config) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(
                Config, [
                         {rmq_nodename_suffix, Suffix},
                         {amqp10_client_library, Group}
                        ]),
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
    ok = ?COMMAND:validate([atom_to_binary(K, utf8) || K <- ?INFO_ITEMS], #{}),
    {validation_failure,{bad_info_key,[other]}} =
        ?COMMAND:validate([<<"other">>], #{}).

println(What, Value) -> io:format("~p : ~p~n", [What, Value]).

when_no_connections(_Config) ->
    [A] = rabbit_ct_broker_helpers:get_node_configs(_Config, nodename),
    Opts = #{node => A, timeout => 2000, verbose => true},
    [] = 'Elixir.Enum':to_list(?COMMAND:run([], Opts)).

when_one_connection(_Config) ->
    [A] = rabbit_ct_broker_helpers:get_node_configs(_Config, nodename),
    Opts = #{node => A, timeout => 2000, verbose => true},

    Connection = open_amqp10_connection(_Config),

    [{tracked_connection, _, _, _, _, _, {'AMQP',"1.0"}, _, _, _, _, _}] =
    rabbit_ct_broker_helpers:rpc(_Config, A,
                                        rabbit_connection_tracking, list, []),

    [P1, P2] = [P
         || {_, P, _, [tcp_listener_sup]} <- rabbit_ct_broker_helpers:rpc(_Config, A, supervisor,which_children, [rabbit_sup])],
    println("Pid1", P1),
    println("Pid2", P2),

    Children1 =  rabbit_ct_broker_helpers:rpc(_Config, A, supervisor,which_children,[P1]),
    println("tcp_listener_sup1", Children1),
    Children2 =  rabbit_ct_broker_helpers:rpc(_Config, A, supervisor,which_children,[P2]),
    println("tcp_listener_sup2", Children2),

    [Res1, Res2] = [RanchSupPid
     || {_, TcpPid, _, [tcp_listener_sup]} <- rabbit_ct_broker_helpers:rpc(_Config, A, supervisor, which_children, [rabbit_sup]),
        {_, RanchSupPid, _, [ranch_embedded_sup]} <- rabbit_ct_broker_helpers:rpc(_Config, A, supervisor, which_children, [TcpPid])
        ],
    println("ranch_embedded_sup 1", rabbit_ct_broker_helpers:rpc(_Config, A, supervisor,which_children,[Res1])),
    println("ranch_embedded_sup 2", rabbit_ct_broker_helpers:rpc(_Config, A, supervisor,which_children,[Res2])),

    [Rl1, Rl2] = [RanchLPid
   || {_, TcpPid, _, [tcp_listener_sup]} <- rabbit_ct_broker_helpers:rpc(_Config, A, supervisor, which_children, [rabbit_sup]),
      {_, RanchEPid, _, [ranch_embedded_sup]} <- rabbit_ct_broker_helpers:rpc(_Config, A, supervisor, which_children, [TcpPid]),
      {_, RanchLPid, _, [ranch_listener_sup]} <- rabbit_ct_broker_helpers:rpc(_Config, A, supervisor, which_children, [RanchEPid])
      ],
    println("ranch_listener_sup", [Rl1, Rl2]),
    println("ranch_listener_sup 1", rabbit_ct_broker_helpers:rpc(_Config, A, supervisor,which_children,[Rl1])),
    println("ranch_listener_sup 2", rabbit_ct_broker_helpers:rpc(_Config, A, supervisor,which_children,[Rl2])),

    ReaderPids = [ReaderPid
   || {_, TcpPid, _, [tcp_listener_sup]} <- rabbit_ct_broker_helpers:rpc(_Config, A, supervisor, which_children, [rabbit_sup]),
      {_, RanchEPid, _, [ranch_embedded_sup]} <- rabbit_ct_broker_helpers:rpc(_Config, A, supervisor, which_children, [TcpPid]),
      {_, RanchLPid, _, [ranch_listener_sup]} <- rabbit_ct_broker_helpers:rpc(_Config, A, supervisor, which_children, [RanchEPid]),
      {_, RanchSPid, _, [ranch_conns_sup_sup]} <- rabbit_ct_broker_helpers:rpc(_Config, A, supervisor, which_children, [RanchLPid]),
      {_, RanchCPid, _, [ranch_conns_sup]} <- rabbit_ct_broker_helpers:rpc(_Config, A, supervisor, which_children, [RanchSPid]),
      {rabbit_connection_sup, ConnPid, _, _} <- supervisor:which_children(RanchCPid),
      {reader, ReaderPid, _, _} <- supervisor:which_children(ConnPid)
      ],
    println("ReaderPid", ReaderPids),
    ReaderInfos = [rabbit_ct_broker_helpers:rpc(_Config, A, rabbit_amqp1_0_reader, info, [Pid, [node,user]]) || Pid <- ReaderPids],
    println("ReaderPid info",ReaderInfos),

    [ExpectedConnection] = 'Elixir.Enum':to_list(?COMMAND:run([], Opts)),
    close_amqp10_connection(Connection).

when_one_amqp091_connection(_Config) ->
    [A] = rabbit_ct_broker_helpers:get_node_configs(_Config, nodename),
    Opts = #{node => A, timeout => 2000},

    Connection = open_amqp091_client_connection(_Config),
    println("amqp connection", Connection),
    [ Listed ] = rabbitmqctl_list_connections(_Config, A),
    println("listed connection:", Listed),
    close_amqp091_client_connection(Connection).

rabbitmqctl_list_connections(Config, Node) ->
    {ok, StdOut} = rabbit_ct_broker_helpers:rabbitmqctl(Config, Node,
      ["list_connections", "--no-table-headers"]),
    [<<"Listing connections", _/binary>> | Rows] = re:split(StdOut, <<"\n">>, [trim]),
    Rows.

open_amqp091_client_connection(_Config) ->
  ConnName = <<"Custom Name">>,
  Host = ?config(rmq_hostname, _Config),
  Port = rabbit_ct_broker_helpers:get_node_config(_Config, 0, tcp_port_amqp),
  AmqpParams = #amqp_params_network{port = Port,
                                    host = Host,
                                    virtual_host = <<"/">>
                                    },

  {ok, Connection} = amqp_connection:start(AmqpParams, ConnName),
  Connection.

close_amqp091_client_connection(Connection) ->
  ?assertEqual(ok, amqp_connection:close(Connection)).

open_amqp10_connection(_Config) ->
  Host = ?config(rmq_hostname, _Config),
  Port = rabbit_ct_broker_helpers:get_node_config(_Config, 0, tcp_port_amqp),
  % create a configuration map
  OpnConf = #{address => Host,
              port => Port,
              container_id => atom_to_binary(?FUNCTION_NAME, utf8),
              sasl => {plain, <<"guest">>, <<"guest">>}},
  {ok, Connection} = amqp10_client:open_connection(OpnConf),
  {ok, _} = amqp10_client:begin_session(Connection),
  Connection.

close_amqp10_connection(Connection) ->
  ok = amqp10_client:close_connection(Connection).
