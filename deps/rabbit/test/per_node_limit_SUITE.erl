%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(per_node_limit_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
     {group, limit_tests}
    ].

groups() ->
    [
     {limit_tests, [], [
                        node_connection_limit,
                        vhost_limit,
                        channel_consumers_limit,
                        node_channel_limit
                       ]}
    ].

suite() ->
    [
      {timetrap, {minutes, 3}}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
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

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(vhost_limit = Testcase, Config) ->
    set_node_limit(Config, vhost_max, infinity),
    set_node_limit(Config, channel_max_per_node, infinity),
    set_node_limit(Config, consumer_max_per_channel, infinity),
    set_node_limit(Config, connection_max, infinity),
    [rabbit_ct_broker_helpers:delete_vhost(Config, integer_to_binary(I)) || I <- lists:seq(1,4)],
    rabbit_ct_helpers:testcase_finished(Config, Testcase);
end_per_testcase(Testcase, Config) ->
    set_node_limit(Config, vhost_max, infinity),
    set_node_limit(Config, channel_max_per_node, infinity),
    set_node_limit(Config, consumer_max_per_channel, infinity),
    set_node_limit(Config, connection_max, infinity),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Test cases
%% -------------------------------------------------------------------

node_connection_limit(Config) ->
    %% Set limit to 0, don't accept any connections
    set_node_limit(Config, connection_max, 0),
    {error, not_allowed} = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0),

    %% Set limit to 5, accept 5 connections
    Connections = open_connections_to_limit(Config, 5),
    %% But no more than 5
    {error, not_allowed} = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0),
    close_all_connections(Connections),

    set_node_limit(Config, connection_max, infinity),
    C = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0),
    true = is_pid(C),
    close_all_connections([C]),
    ok.

vhost_limit(Config) ->
    set_node_limit(Config, vhost_max, 0),
    {'EXIT',{vhost_limit_exceeded, _}} = rabbit_ct_broker_helpers:add_vhost(Config, <<"foo">>),

    set_node_limit(Config, vhost_max, 5),
    [ok = rabbit_ct_broker_helpers:add_vhost(Config, integer_to_binary(I)) || I <- lists:seq(1,4)],
    {'EXIT',{vhost_limit_exceeded, _}} = rabbit_ct_broker_helpers:add_vhost(Config, <<"5">>),
    [rabbit_ct_broker_helpers:delete_vhost(Config, integer_to_binary(I)) || I <- lists:seq(1,4)],

    set_node_limit(Config, vhost_max, infinity),
    [ok = rabbit_ct_broker_helpers:add_vhost(Config, integer_to_binary(I)) || I <- lists:seq(1,4)],
    ok = rabbit_ct_broker_helpers:add_vhost(Config, <<"5">>),
    [rabbit_ct_broker_helpers:delete_vhost(Config, integer_to_binary(I)) || I <- lists:seq(1,5)],
    ok.

node_channel_limit(Config) ->
    set_node_limit(Config, channel_max_per_node, 5),

    VHost = <<"foobar">>,
    User = <<"guest">>,
    ok = rabbit_ct_broker_helpers:add_vhost(Config, VHost),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, User, VHost),
    Conn1 = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0, VHost),
    Conn2 = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0, VHost),
    0 = count_channels_per_node(Config),

    lists:foreach(fun(N) when (N band 1) == 1 -> {ok, _} = open_channel(Conn1);
                     (_) -> {ok,_ } = open_channel(Conn2)
                  end, lists:seq(1, 5)),

    5 = count_channels_per_node(Config),
    %% In total 5 channels are open on this node, so a new one, regardless of
    %% connection, will not be allowed. It will terminate the connection with
    %% its channels too. So
    {error, not_allowed_crash} = open_channel(Conn2),
    3 = count_channels_per_node(Config),
    %% As the connection is dead, so are the 2 channels, so we should be able to
    %% create 2 more on Conn1
    {ok , _} = open_channel(Conn1),
    {ok , _} = open_channel(Conn1),
    %% But not a third
    {error, not_allowed_crash} = open_channel(Conn1),

    %% Now all connections are closed, so there should be 0 open connections
    0 = count_channels_per_node(Config),
    close_all_connections([Conn1, Conn2]),
    ok.

channel_consumers_limit(Config) ->
    set_node_limit(Config, consumer_max_per_channel, 1),

    VHost = <<"foobar">>,
    User = <<"guest">>,
    ok = rabbit_ct_broker_helpers:add_vhost(Config, VHost),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, User, VHost),
    Conn1 = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0, VHost),
    {ok, Ch} = open_channel(Conn1),
    Q = <<"Q">>, Tag = <<"Tag">>,

    {ok, _} = consume(Ch, Q, Tag),
    {error, not_allowed_crash} = consume(Ch, Q, Tag),  % Reusing Tag should fail

    close_all_connections([Conn1]),
    ok.

%% -------------------------------------------------------------------
%% Implementation
%% -------------------------------------------------------------------

open_connections_to_limit(Config, Limit) ->
    set_node_limit(Config, connection_max, Limit),
    Connections = [rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0) || _ <- lists:seq(1,Limit)],
    true = lists:all(fun(E) -> is_pid(E) end, Connections),
    Connections.

close_all_connections(Connections) ->
    [rabbit_ct_client_helpers:close_connection(C) || C <- Connections].

set_node_limit(Config, Type, Limit) ->
    rabbit_ct_broker_helpers:rpc(Config, 0,
                                 application,
                                 set_env, [rabbit, Type, Limit]).

consume(Ch, Q, Tag) ->
    #'queue.declare_ok'{queue = Q} = amqp_channel:call(Ch, #'queue.declare'{queue = Q}),
    try amqp_channel:call(Ch, #'basic.consume'{queue = Q, consumer_tag = Tag}) of
      #'basic.consume_ok'{} = OK -> {ok, OK};
      NotOk -> {error, NotOk}
    catch
      _:_Error -> {error, not_allowed_crash}
   end.

open_channel(Conn) when is_pid(Conn) ->
    try amqp_connection:open_channel(Conn) of
      {ok, Ch} -> {ok, Ch};
      {error, _} ->
            {error, not_allowed}
    catch
      _:_Error -> {error, not_allowed_crash}
   end.

count_channels_per_node(Config)  ->
    NodeConfig = rabbit_ct_broker_helpers:get_node_config(Config, 0),
    rabbit_ct_broker_helpers:rpc(Config, 0,
                                 rabbit_channel_tracking,
                                 channel_count_on_node,
                                 [?config(nodename, NodeConfig)]).
