%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(per_node_limit_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
     {group, parallel_tests}
    ].

groups() ->
    [
     {parallel_tests, [parallel], [
                                   node_connection_limit,
                                   vhost_limit
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
    [rabbit_ct_broker_helpers:delete_vhost(Config, integer_to_binary(I)) || I <- lists:seq(1,4)],
    rabbit_ct_helpers:testcase_finished(Config, Testcase);
end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Test cases
%% -------------------------------------------------------------------

node_connection_limit(Config) ->
    %% Set limit to 0, don't accept any connections
    set_node_limit(Config, 0),
    {error, not_allowed} = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0),

    %% Set limit to 5, accept 5 connections
    Connections = open_connections_to_limit(Config, 5),
    %% But no more than 5
    {error, not_allowed} = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0),
    close_all_connections(Connections),

    set_node_limit(Config, infinity),
    C = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0),
    true = is_pid(C),
    close_all_connections([C]),
    ok.

vhost_limit(Config) ->
    set_vhost_limit(Config, 0),
    {'EXIT',{vhost_limit_exceeded, _}} = rabbit_ct_broker_helpers:add_vhost(Config, <<"foo">>),

    set_vhost_limit(Config, 5),
    [ok = rabbit_ct_broker_helpers:add_vhost(Config, integer_to_binary(I)) || I <- lists:seq(1,4)],
    {'EXIT',{vhost_limit_exceeded, _}} = rabbit_ct_broker_helpers:add_vhost(Config, <<"5">>),
    [rabbit_ct_broker_helpers:delete_vhost(Config, integer_to_binary(I)) || I <- lists:seq(1,4)],

    set_vhost_limit(Config, infinity),
    [ok = rabbit_ct_broker_helpers:add_vhost(Config, integer_to_binary(I)) || I <- lists:seq(1,4)],
    ok = rabbit_ct_broker_helpers:add_vhost(Config, <<"5">>),
    [rabbit_ct_broker_helpers:delete_vhost(Config, integer_to_binary(I)) || I <- lists:seq(1,5)],
    ok.


%% -------------------------------------------------------------------
%% Implementation
%% -------------------------------------------------------------------

open_connections_to_limit(Config, Limit) ->
    set_node_limit(Config, Limit),
    Connections = [rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0) || _ <- lists:seq(1,Limit)],
    true = lists:all(fun(E) -> is_pid(E) end, Connections),
    Connections.

close_all_connections(Connections) ->
    [rabbit_ct_client_helpers:close_connection(C) || C <- Connections].

set_node_limit(Config, Limit) ->
    rabbit_ct_broker_helpers:rpc(Config, 0,
                                 application,
                                 set_env, [rabbit, connection_max, Limit]).

set_vhost_limit(Config, Limit) ->
    rabbit_ct_broker_helpers:rpc(Config, 0,
                                 application,
                                 set_env, [rabbit, vhost_max, Limit]).
