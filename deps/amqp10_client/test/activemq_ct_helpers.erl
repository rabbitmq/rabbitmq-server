%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(activemq_ct_helpers).

-include_lib("common_test/include/ct.hrl").

-export([setup_steps/1,
         teardown_steps/0,
         ensure_activemq_cmd/1,
         init_config_filename/2,
         init_tcp_port_numbers/1,
         start_activemq_nodes/1,
         stop_activemq_nodes/1]).

setup_steps(ConfigFileName) ->
    [fun ensure_activemq_cmd/1,
     fun(Config) -> init_config_filename(Config, ConfigFileName) end,
     fun init_tcp_port_numbers/1,
     fun start_activemq_nodes/1].

teardown_steps() ->
    [fun stop_activemq_nodes/1].

ensure_activemq_cmd(Config) ->
    ActivemqCmd= case rabbit_ct_helpers:get_config(Config, activemq_cmd) of
        undefined ->
            case os:getenv("ACTIVEMQ") of
                false -> "activemq";
                M     -> M
            end;
        M ->
            M
    end,
    Cmd = [ActivemqCmd, "--version"],
    case rabbit_ct_helpers:exec(Cmd, [{match_stdout, "ActiveMQ"}]) of
        {ok, _} -> rabbit_ct_helpers:set_config(Config,
                                                {activemq_cmd, ActivemqCmd});
        _       -> {skip,
                    "ActiveMQ CLI required, " ++
                    "please set ACTIVEMQ or 'activemq_cmd' in ct config"}
    end.

init_config_filename(Config, FileName) ->
    ConfigFile = filename:join([?config(data_dir, Config),
                                "conf", FileName]),
    rabbit_ct_helpers:set_config(Config, {activemq_config_filename, ConfigFile}).

init_tcp_port_numbers(Config) ->
    TCPPort = 21000,
    NodeConfig = [{nodename, activemq},
                  {initial_nodename, activemq},
                  {tcp_port_amqp, TCPPort}],
    rabbit_ct_helpers:set_config(Config, {rmq_nodes, [NodeConfig]}).

start_activemq_nodes(Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_hostname, "localhost"}]),
    ActivemqCmd = ?config(activemq_cmd, Config1),
    TCPPort = rabbit_ct_broker_helpers:get_node_config(Config1, 0, tcp_port_amqp),
    ConfigFile = ?config(activemq_config_filename, Config1),
    Cmd = [ActivemqCmd,
           "start",
           {"-Dtestsuite.tcp_port_amqp=~b", [TCPPort]},
           {"xbean:file:~s", [ConfigFile]}],
    case rabbit_ct_helpers:exec(Cmd, []) of
        {ok, _} -> wait_for_activemq_nodes(Config1);
        Error   -> ct:pal("Error: ~p", [Error]),
                   {skip, "Failed to start ActiveMQ"}
    end.

wait_for_activemq_nodes(Config) ->
    Hostname = ?config(rmq_hostname, Config),
    Ports = rabbit_ct_broker_helpers:get_node_configs(Config, tcp_port_amqp),
    wait_for_activemq_ports(Config, Hostname, Ports).

wait_for_activemq_ports(Config, Hostname, [Port | Rest]) ->
    ct:pal(?LOW_IMPORTANCE, "Waiting for ActiveMQ on port ~b", [Port]),
    case wait_for_activemq_port(Hostname, Port, 60) of
        ok ->
            ct:pal(?LOW_IMPORTANCE, "ActiveMQ ready on port ~b", [Port]),
            wait_for_activemq_ports(Config, Hostname, Rest);
        {error, _} ->
            Msg = lists:flatten(
                    io_lib:format(
                      "Failed to start ActiveMQ on port ~b; see ActiveMQ logs",
                      [Port])),
            ct:pal(?LOW_IMPORTANCE, Msg, []),
            {skip, Msg}
    end;
wait_for_activemq_ports(Config, _, []) ->
    Config.

wait_for_activemq_port(_, _, 0) ->
    {error, econnrefused};
wait_for_activemq_port(Hostname, Port, Retries) ->
    case gen_tcp:connect(Hostname, Port, []) of
        {ok, Connection} ->
            gen_tcp:close(Connection),
            ok;
        {error, econnrefused} ->
            timer:sleep(1000),
            wait_for_activemq_port(Hostname, Port, Retries - 1);
        Error ->
            Error
    end.

stop_activemq_nodes(Config) ->
    ActivemqCmd = ?config(activemq_cmd, Config),
    Cmd = [ActivemqCmd, "stop"],
    case rabbit_ct_helpers:exec(Cmd, []) of
        {ok, _} -> Config;
        Error   -> ct:pal("Error: ~p", [Error]),
                   {skip, "Failed to stop ActiveMQ"}
    end.
