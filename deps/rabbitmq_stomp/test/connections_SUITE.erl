%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(connections_SUITE).
-compile(export_all).

-import(rabbit_misc, [pget/2]).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_stomp_frame.hrl").
-define(DESTINATION, "/queue/bulk-test").

all() ->
    [
        messages_not_dropped_on_disconnect,
        direct_client_connections_are_not_leaked,
        stats_are_not_leaked,
        stats,
        heartbeat
    ].

merge_app_env(Config) ->
    rabbit_ct_helpers:merge_app_env(Config,
                                    {rabbit, [
                                              {collect_statistics, basic},
                                              {collect_statistics_interval, 100}
                                             ]}).

init_per_suite(Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_nodename_suffix, ?MODULE}]),
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config1,
      [ fun merge_app_env/1 ] ++
      rabbit_ct_broker_helpers:setup_steps()).


end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_broker_helpers:teardown_steps()).

-define(GARBAGE, <<"bdaf63dda9d78b075c748b740e7c3510ad203b07\nbdaf63dd">>).

count_connections(Config) ->
    StompPort = get_stomp_port(Config),
    %% The default port is 61613 but it's in the middle of the ephemeral
    %% ports range on many operating systems. Therefore, there is a
    %% chance this port is already in use. Let's use a port close to the
    %% AMQP default port.
    IPv4Count = try
        %% Count IPv4 connections. On some platforms, the IPv6 listener
        %% implicitely listens to IPv4 connections too so the IPv4
        %% listener doesn't exist. Thus this try/catch. This is the case
        %% with Linux where net.ipv6.bindv6only is disabled (default in
        %% most cases).
        rpc_count_connections(Config, {acceptor, {0,0,0,0}, StompPort})
    catch
        _:{badarg, _} -> 0;
        _:Other -> exit({foo, Other})
    end,
    IPv6Count = try
        %% Count IPv6 connections. We also use a try/catch block in case
        %% the host is not configured for IPv6.
        rpc_count_connections(Config, {acceptor, {0,0,0,0,0,0,0,0}, StompPort})
    catch
        _:{badarg, _} -> 0;
        _:Other1 -> exit({foo, Other1})
    end,
    IPv4Count + IPv6Count.

rpc_count_connections(Config, ConnSpec) ->
    rabbit_ct_broker_helpers:rpc(Config, 0,
                                 ranch_server, count_connections, [ConnSpec]).

direct_client_connections_are_not_leaked(Config) ->
    StompPort = get_stomp_port(Config),
    N = count_connections(Config),
    lists:foreach(fun (_) ->
                          {ok, Client = {Socket, _}} = rabbit_stomp_client:connect(StompPort),
                          %% send garbage which trips up the parser
                          gen_tcp:send(Socket, ?GARBAGE),
                          rabbit_stomp_client:send(
                           Client, "LOL", [{"", ""}])
                  end,
                  lists:seq(1, 100)),
    timer:sleep(5000),
    N = count_connections(Config),
    ok.

messages_not_dropped_on_disconnect(Config) ->
    StompPort = get_stomp_port(Config),
    N = count_connections(Config),
    {ok, Client} = rabbit_stomp_client:connect(StompPort),
    N1 = N + 1,
    N1 = count_connections(Config),
    [rabbit_stomp_client:send(
       Client, "SEND", [{"destination", ?DESTINATION}],
       [integer_to_list(Count)]) || Count <- lists:seq(1, 1000)],
    rabbit_stomp_client:disconnect(Client),
    QName = rabbit_misc:r(<<"/">>, queue, <<"bulk-test">>),
    timer:sleep(3000),
    N = count_connections(Config),
    {ok, Q} = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, lookup, [QName]),
    Messages = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, info, [Q, [messages]]),
    1000 = pget(messages, Messages),
    ok.

get_stomp_port(Config) ->
    rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_stomp).

stats_are_not_leaked(Config) ->
    StompPort = get_stomp_port(Config),
    N = rabbit_ct_broker_helpers:rpc(Config, 0, ets, info, [connection_metrics, size]),
    {ok, C} = gen_tcp:connect("localhost", StompPort, []),
    Bin = <<"GET / HTTP/1.1\r\nHost: www.rabbitmq.com\r\nUser-Agent: curl/7.43.0\r\nAccept: */*\n\n">>,
    gen_tcp:send(C, Bin),
    gen_tcp:close(C),
    timer:sleep(1000), %% Wait for stats to be emitted, which it does every 100ms
    N = rabbit_ct_broker_helpers:rpc(Config, 0, ets, info, [connection_metrics, size]),
    ok.

stats(Config) ->
    StompPort = get_stomp_port(Config),
    {ok, Client} = rabbit_stomp_client:connect(StompPort),
    timer:sleep(1000), %% Wait for stats to be emitted, which it does every 100ms
    %% Retrieve the connection Pid
    [Reader] = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_stomp, list, []),
    [{_, Pid}] = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_stomp_reader,
                                              info, [Reader, [connection]]),
    %% Verify the content of the metrics, garbage_collection must be present
    [{Pid, Props}] = rabbit_ct_broker_helpers:rpc(Config, 0, ets, lookup,
                                                  [connection_metrics, Pid]),
    true = proplists:is_defined(garbage_collection, Props),
    0 = proplists:get_value(timeout, Props),
    %% If the coarse entry is present, stats were successfully emitted
    [{Pid, _, _, _, _}] = rabbit_ct_broker_helpers:rpc(Config, 0, ets, lookup,
                                                       [connection_coarse_metrics, Pid]),
    rabbit_stomp_client:disconnect(Client),
    ok.

heartbeat(Config) ->
    StompPort = get_stomp_port(Config),
    {ok, Client} = rabbit_stomp_client:connect("1.2", "guest", "guest", StompPort,
                                               [{"heart-beat", "5000,7000"}]),
    timer:sleep(1000), %% Wait for stats to be emitted, which it does every 100ms
    %% Retrieve the connection Pid
    [Reader] = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_stomp, list, []),
    [{_, Pid}] = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_stomp_reader,
                                              info, [Reader, [connection]]),
    %% Verify the content of the heartbeat timeout
    [{Pid, Props}] = rabbit_ct_broker_helpers:rpc(Config, 0, ets, lookup,
                                                  [connection_metrics, Pid]),
    5 = proplists:get_value(timeout, Props),
    rabbit_stomp_client:disconnect(Client),
    ok.
