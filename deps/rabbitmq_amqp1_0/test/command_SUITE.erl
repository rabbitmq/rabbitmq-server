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
                               when_one_connection
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

    Connection = open_client_connection(_Config),
    [ExpectedConnection|_] = 'Elixir.Enum':to_list(?COMMAND:run([], Opts)),
    println("connection", ExpectedConnection),
    close_client_connection(Connection).

open_client_connection(_Config) ->
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

close_client_connection(Connection) ->
  ok = amqp10_client:close_connection(Connection).
