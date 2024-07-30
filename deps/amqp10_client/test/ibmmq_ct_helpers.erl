%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(ibmmq_ct_helpers).

-include_lib("common_test/include/ct.hrl").

-export([setup_steps/0,
         teardown_steps/0,
         init_config/1,
         start_ibmmq_server/1,
         stop_ibmmq_server/1]).

setup_steps() ->
    [fun init_config/1,
     fun start_ibmmq_server/1
    ].

teardown_steps() ->
    [
     fun stop_ibmmq_server/1
    ].

init_config(Config) ->
    NodeConfig = [{tcp_port_amqp, 5672}],
    rabbit_ct_helpers:set_config(Config, [  {rmq_nodes, [NodeConfig]},
                                            {rmq_hostname, "localhost"},
                                            {tcp_hostname_amqp, "localhost"},
                                            {sasl, {plain, <<"app">>, <<"passw0rd">>}} ]).

start_ibmmq_server(Config) ->
    IBMmqCmd = filename:join([?config(data_dir, Config), "ibmmq_runner"]),
    Cmd = [IBMmqCmd, "start"],
    ct:log("Running command ~p", [Cmd]),
    case rabbit_ct_helpers:exec(Cmd, []) of
        {ok, _} -> wait_for_ibmmq_nodes(Config);
        Error   -> ct:pal("Error: ~tp", [Error]),
                   {skip, "Failed to start IBM MQ"}
    end.

wait_for_ibmmq_nodes(Config) ->
    Hostname = ?config(rmq_hostname, Config),
    Ports = rabbit_ct_broker_helpers:get_node_configs(Config, tcp_port_amqp),
    wait_for_ibmmq_ports(Config, Hostname, Ports),
    timer:sleep(500),
    Config.

wait_for_ibmmq_ports(Config, Hostname, [Port | Rest]) ->
    ct:log("Waiting for IBM MQ on port ~b", [Port]),
    case wait_for_ibmmq_port(Hostname, Port, 60) of
        ok ->
            ct:log("IBM MQ ready on port ~b", [Port]),
            wait_for_ibmmq_ports(Config, Hostname, Rest);
        {error, _} ->
            Msg = lists:flatten(
                    io_lib:format(
                      "Failed to start IBM MQ on port ~b; see IBM MQ logs",
                      [Port])),
            ct:pal(?LOW_IMPORTANCE, Msg, []),
            {skip, Msg}
    end;
wait_for_ibmmq_ports(Config, _, []) ->
    Config.

wait_for_ibmmq_port(_, _, 0) ->
    {error, econnrefused};
wait_for_ibmmq_port(Hostname, Port, Retries) ->
    case gen_tcp:connect(Hostname, Port, []) of
        {ok, Connection} ->
            gen_tcp:close(Connection),
            ok;
        {error, econnrefused} ->
            timer:sleep(1000),
            wait_for_ibmmq_port(Hostname, Port, Retries - 1);
        Error ->
            Error
    end.

stop_ibmmq_server(Config) ->
    IBMmqCmd = filename:join([?config(data_dir, Config), "ibmmq_runner"]),
    Cmd = [IBMmqCmd, "stop"],
    ct:log("Running command ~p", [Cmd]),
    case rabbit_ct_helpers:exec(Cmd, []) of
        {ok, _} -> Config;
        Error   -> ct:pal("Error: ~tp", [Error]),
                   {skip, "Failed to stop IBM MQ"}
    end.
