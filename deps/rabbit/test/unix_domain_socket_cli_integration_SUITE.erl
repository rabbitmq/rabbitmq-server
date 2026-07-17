%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(unix_domain_socket_cli_integration_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

all() ->
    [
        {group, tests}
    ].

groups() ->
    [
        {tests, [], [
            list_connections_over_uds
        ]}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(
                Config, [{rmq_nodename_suffix, ?MODULE},
                         {rmq_nodes_count, 1}]),
    Config2 = rabbit_ct_helpers:run_setup_steps(
                Config1,
                rabbit_ct_broker_helpers:setup_steps() ++
                    rabbit_ct_client_helpers:setup_steps()),
    SocketPath = uds_socket_path(),
    _ = file:delete(SocketPath),
    ok = rabbit_ct_broker_helpers:rpc(
           Config2, rabbit_networking, start_tcp_listener,
           [{local, SocketPath, 0}, 10]),
    rabbit_ct_helpers:set_config(Config2, {uds_socket_path, SocketPath}).

end_per_suite(Config) ->
    SocketPath = ?config(uds_socket_path, Config),
    _ = rabbit_ct_broker_helpers:rpc(
          Config, rabbit_networking, stop_tcp_listener,
          [{local, SocketPath, 0}]),
    _ = file:delete(SocketPath),
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
          rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Test Cases
%% -------------------------------------------------------------------

list_connections_over_uds(Config) ->
    SocketPath = ?config(uds_socket_path, Config),
    ConnParams = #amqp_params_network{host = {local, SocketPath}, port = 0},
    {ok, Conn} = amqp_connection:start(ConnParams),
    try
        SocketPathBin = list_to_binary(SocketPath),
        rabbit_ct_helpers:await_condition(
          fun () -> lists:member(SocketPathBin, list_connection_hosts(Config)) end,
          30000)
    after
        amqp_connection:close(Conn)
    end,
    ok.

%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------

list_connection_hosts(Config) ->
    {ok, Out} = rabbit_ct_broker_helpers:rabbitmqctl(
                  Config, 0,
                  ["list_connections", "--no-table-headers",
                   "host", "peer_host"]),
    [Host || Row <- re:split(Out, <<"\n">>, [trim]),
             binary:match(Row, <<"\t">>) =/= nomatch,
             [Host | _] <- [re:split(Row, <<"\t">>, [trim])]].

%% The OS caps a Unix domain socket's file path (the sun_path field of the
%% socket address) at 108 bytes on Linux, 104 on macOS/BSD, null terminator
%% included. CT's priv_dir overruns that, so use the system temp directory
%% (TMPDIR/TMP/TEMP, then /tmp), which is short enough on both.
uds_socket_path() ->
    Name = "rmq-uds-" ++ integer_to_list(erlang:unique_integer([positive])) ++
        ".sock",
    Limit = case os:type() of
                {unix, linux} -> 107;
                _             -> 103
            end,
    Dirs = [Dir || Dir <- [os:getenv("TMPDIR"), os:getenv("TMP"),
                           os:getenv("TEMP"), "/tmp"],
                   is_list(Dir)],
    Candidates = [P || Dir <- Dirs, P <- [filename:join(Dir, Name)],
                       length(P) =< Limit],
    case Candidates of
        [Path | _] -> Path;
        []         -> filename:join("/tmp", Name)
    end.
