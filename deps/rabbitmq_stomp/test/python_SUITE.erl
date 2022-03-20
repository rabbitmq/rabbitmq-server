%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(python_SUITE).
-compile(export_all).
-include_lib("common_test/include/ct.hrl").

all() ->
    [
        %% This must use a dedicated node as they mess with plugin configuration in incompatible ways
        {group, tls},
        {group, implicit_connect},
        {group, main}
    ].

groups() ->
    [
        {main, [], [
            main
        ]},
        {implicit_connect, [], [
            implicit_connect
        ]},
        {tls, [], [
            tls_connections
        ]}
    ].

init_per_group(_, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [
                                               {rmq_nodename_suffix, ?MODULE},
                                               {rmq_certspwd, "bunnychow"}
                                            ]),
    rabbit_ct_helpers:log_environment(),
    Config2 = rabbit_ct_helpers:run_setup_steps(
        Config1,
        rabbit_ct_broker_helpers:setup_steps()),
    DataDir = ?config(data_dir, Config2),
    PikaDir = filename:join([DataDir, "deps", "pika"]),
    StomppyDir = filename:join([DataDir, "deps", "stomppy"]),
    rabbit_ct_helpers:make(Config2, PikaDir, []),
    rabbit_ct_helpers:make(Config2, StomppyDir, []),
    Config2.

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
        rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Test, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Test).

end_per_testcase(Test, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Test).


main(Config) ->
    run(Config, filename:join("src", "main_runner.py")).

implicit_connect(Config) ->
    run(Config, filename:join("src", "implicit_connect_runner.py")).

tls_connections(Config) ->
    run(Config, filename:join("src", "tls_runner.py")).


run(Config, Test) ->
    DataDir = ?config(data_dir, Config),
    CertsDir = rabbit_ct_helpers:get_config(Config, rmq_certsdir),
    StompPort = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_stomp),
    StompPortTls = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_stomp_tls),
    AmqpPort = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    NodeName = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    PikaPath = filename:join([DataDir, "deps", "pika","pika"]),
    StomppyPath = filename:join([DataDir, "deps", "stomppy", "stomppy"]),
    PythonPath = case os:getenv("PYTHONPATH") of
        false -> PikaPath ++ ":" ++ StomppyPath;
        P -> PikaPath ++ ":" ++ StomppyPath ++ ":" ++ P
    end,
    os:putenv("PYTHONPATH", PythonPath),
    os:putenv("AMQP_PORT", integer_to_list(AmqpPort)),
    os:putenv("STOMP_PORT", integer_to_list(StompPort)),
    os:putenv("STOMP_PORT_TLS", integer_to_list(StompPortTls)),
    os:putenv("RABBITMQ_NODENAME", atom_to_list(NodeName)),
    os:putenv("SSL_CERTS_PATH", CertsDir),
    {ok, _} = rabbit_ct_helpers:exec([filename:join(DataDir, Test)]).


cur_dir() ->
    {ok, Src} = filelib:find_source(?MODULE),
    filename:dirname(Src).
