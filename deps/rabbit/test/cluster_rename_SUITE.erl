%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(cluster_rename_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

all() ->
    [
      {group, cluster_size_2},
      {group, cluster_size_3}
    ].

groups() ->
    [
      {cluster_size_2, [], [
          % XXX post_change_nodename,
          abortive_rename,
          rename_fail,
          rename_twice_fail
        ]},
      {cluster_size_3, [], [
          rename_cluster_one_by_one,
          rename_cluster_big_bang,
          partial_one_by_one,
          partial_big_bang
        ]}
    ].

suite() ->
    [
      %% If a test hangs, no need to wait for 30 minutes.
      {timetrap, {minutes, 15}}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(cluster_size_2, Config) ->
    rabbit_ct_helpers:set_config(Config, [
        {rmq_nodes_count, 2} %% Replaced with a list of node names later.
      ]);
init_per_group(cluster_size_3, Config) ->
    rabbit_ct_helpers:set_config(Config, [
        {rmq_nodes_count, 3} %% Replaced with a list of node names later.
      ]).

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    ClusterSize = ?config(rmq_nodes_count, Config),
    Nodenames = [
      list_to_atom(rabbit_misc:format("~s-~b", [Testcase, I]))
      || I <- lists:seq(1, ClusterSize)
    ],
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodes_count, Nodenames},
        {rmq_nodes_clustered, true}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_testcase(Testcase, Config) ->
    Config1 = case rabbit_ct_helpers:get_config(Config, save_config) of
        undefined -> Config;
        C         -> C
    end,
    Config2 = rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config2, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

%% Rolling rename of a cluster, each node should do a secondary rename.
rename_cluster_one_by_one(Config) ->
    [Node1, Node2, Node3] = rabbit_ct_broker_helpers:get_node_configs(
      Config, nodename),
    publish_all(Config,
      [{Node1, <<"1">>}, {Node2, <<"2">>}, {Node3, <<"3">>}]),

    Config1 = stop_rename_start(Config,  Node1, [Node1, jessica]),
    Config2 = stop_rename_start(Config1, Node2, [Node2, hazel]),
    Config3 = stop_rename_start(Config2, Node3, [Node3, flopsy]),

    [Jessica, Hazel, Flopsy] = rabbit_ct_broker_helpers:get_node_configs(
      Config3, nodename),
    consume_all(Config3,
      [{Jessica, <<"1">>}, {Hazel, <<"2">>}, {Flopsy, <<"3">>}]),
    {save_config, Config3}.

%% Big bang rename of a cluster, Node1 should do a primary rename.
rename_cluster_big_bang(Config) ->
    [Node1, Node2, Node3] = rabbit_ct_broker_helpers:get_node_configs(Config,
      nodename),
    publish_all(Config,
      [{Node1, <<"1">>}, {Node2, <<"2">>}, {Node3, <<"3">>}]),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Node3),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Node2),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Node1),

    Map = [Node1, jessica, Node2, hazel, Node3, flopsy],
    Config1 = rename_node(Config,  Node1, Map),
    Config2 = rename_node(Config1, Node2, Map),
    Config3 = rename_node(Config2, Node3, Map),

    [Jessica, Hazel, Flopsy] = rabbit_ct_broker_helpers:get_node_configs(
      Config3, nodename),
    ok = rabbit_ct_broker_helpers:start_node(Config3, Jessica),
    ok = rabbit_ct_broker_helpers:start_node(Config3, Hazel),
    ok = rabbit_ct_broker_helpers:start_node(Config3, Flopsy),

    consume_all(Config3,
      [{Jessica, <<"1">>}, {Hazel, <<"2">>}, {Flopsy, <<"3">>}]),
    {save_config, Config3}.

%% Here we test that Node1 copes with things being renamed around it.
partial_one_by_one(Config) ->
    [Node1, Node2, Node3] = rabbit_ct_broker_helpers:get_node_configs(Config,
      nodename),
    publish_all(Config,
      [{Node1, <<"1">>}, {Node2, <<"2">>}, {Node3, <<"3">>}]),

    Config1 = stop_rename_start(Config,  Node1, [Node1, jessica]),
    Config2 = stop_rename_start(Config1, Node2, [Node2, hazel]),

    [Jessica, Hazel, Node3] = rabbit_ct_broker_helpers:get_node_configs(
      Config2, nodename),
    consume_all(Config2,
      [{Jessica, <<"1">>}, {Hazel, <<"2">>}, {Node3, <<"3">>}]),
    {save_config, Config2}.

%% Here we test that Node1 copes with things being renamed around it.
partial_big_bang(Config) ->
    [Node1, Node2, Node3] = rabbit_ct_broker_helpers:get_node_configs(Config,
      nodename),
    publish_all(Config,
      [{Node1, <<"1">>}, {Node2, <<"2">>}, {Node3, <<"3">>}]),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Node3),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Node2),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Node1),

    Map = [Node2, hazel, Node3, flopsy],
    Config1 = rename_node(Config,  Node2, Map),
    Config2 = rename_node(Config1, Node3, Map),

    [Node1, Hazel, Flopsy] = rabbit_ct_broker_helpers:get_node_configs(Config2,
      nodename),
    ok = rabbit_ct_broker_helpers:start_node(Config2, Node1),
    ok = rabbit_ct_broker_helpers:start_node(Config2, Hazel),
    ok = rabbit_ct_broker_helpers:start_node(Config2, Flopsy),

    consume_all(Config2,
      [{Node1, <<"1">>}, {Hazel, <<"2">>}, {Flopsy, <<"3">>}]),
    {save_config, Config2}.

% XXX %% We should be able to specify the -n parameter on ctl with either
% XXX %% the before or after name for the local node (since in real cases
% XXX %% one might want to invoke the command before or after the hostname
% XXX %% has changed) - usually we test before so here we test after.
% XXX post_change_nodename([Node1, _Bigwig]) ->
% XXX     publish(Node1, <<"Node1">>),
% XXX
% XXX     Bugs1    = rabbit_test_configs:stop_node(Node1),
% XXX     Bugs2    = [{nodename, jessica} | proplists:delete(nodename, Bugs1)],
% XXX     Jessica0 = rename_node(Bugs2, jessica, [Node1, jessica]),
% XXX     Jessica  = rabbit_test_configs:start_node(Jessica0),
% XXX
% XXX     consume(Jessica, <<"Node1">>),
% XXX     stop_all([Jessica]),
% XXX     ok.

%% If we invoke rename but the node name does not actually change, we
%% should roll back.
abortive_rename(Config) ->
    Node1 = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    publish(Config, Node1,  <<"Node1">>),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Node1),
    _Config1 = rename_node(Config, Node1, [Node1, jessica]),
    ok = rabbit_ct_broker_helpers:start_node(Config, Node1),

    consume(Config, Node1, <<"Node1">>),
    ok.

%% And test some ways the command can fail.
rename_fail(Config) ->
    [Node1, Node2] = rabbit_ct_broker_helpers:get_node_configs(Config,
      nodename),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Node1),
    %% Rename from a node that does not exist
    ok = rename_node_fail(Config, Node1, [bugzilla, jessica]),
    %% Rename to a node which does
    ok = rename_node_fail(Config, Node1, [Node1, Node2]),
    %% Rename two nodes to the same thing
    ok = rename_node_fail(Config, Node1, [Node1, jessica, Node2, jessica]),
    %% Rename while impersonating a node not in the cluster
    Config1 = rabbit_ct_broker_helpers:set_node_config(Config, Node1,
      {nodename, 'rabbit@localhost'}),
    ok = rename_node_fail(Config1, Node1, [Node1, jessica]),
    ok.

rename_twice_fail(Config) ->
    Node1 = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Node1),
    Config1 = rename_node(Config, Node1, [Node1, indecisive]),
    ok = rename_node_fail(Config, Node1, [indecisive, jessica]),
    {save_config, Config1}.

%% ----------------------------------------------------------------------------

stop_rename_start(Config, Nodename, Map) ->
    ok = rabbit_ct_broker_helpers:stop_node(Config, Nodename),
    Config1 = rename_node(Config, Nodename, Map),
    ok = rabbit_ct_broker_helpers:start_node(Config1, Nodename),
    Config1.

rename_node(Config, Nodename, Map) ->
    {ok, Config1} = do_rename_node(Config, Nodename, Map),
    Config1.

rename_node_fail(Config, Nodename, Map) ->
    {error, _, _} = do_rename_node(Config, Nodename, Map),
    ok.

do_rename_node(Config, Nodename, Map) ->
    Map1 = [
      begin
          NStr = atom_to_list(N),
          case lists:member($@, NStr) of
              true  -> N;
              false -> rabbit_nodes:make({NStr, "localhost"})
          end
      end
      || N <- Map
    ],
    Ret = rabbit_ct_broker_helpers:rabbitmqctl(Config, Nodename,
      ["rename_cluster_node" | Map1], 120000),
    case Ret of
        {ok, _} ->
            Config1 = update_config_after_rename(Config, Map1),
            {ok, Config1};
        {error, _, _} = Error ->
            Error
    end.

update_config_after_rename(Config, [Old, New | Rest]) ->
    Config1 = rabbit_ct_broker_helpers:set_node_config(Config, Old,
      {nodename, New}),
    update_config_after_rename(Config1, Rest);
update_config_after_rename(Config, []) ->
    Config.

publish(Config, Node, Q) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:call(Ch, #'queue.declare'{queue = Q, durable = true}),
    amqp_channel:cast(Ch, #'basic.publish'{routing_key = Q},
                      #amqp_msg{props   = #'P_basic'{delivery_mode = 2},
                                payload = Q}),
    amqp_channel:wait_for_confirms(Ch),
    rabbit_ct_client_helpers:close_channels_and_connection(Config, Node).

consume(Config, Node, Q) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    amqp_channel:call(Ch, #'queue.declare'{queue = Q, durable = true}),
    {#'basic.get_ok'{}, #amqp_msg{payload = Q}} =
        amqp_channel:call(Ch, #'basic.get'{queue = Q}),
    rabbit_ct_client_helpers:close_channels_and_connection(Config, Node).


publish_all(Config, Nodes) ->
    [publish(Config, Node, Key) || {Node, Key} <- Nodes].

consume_all(Config, Nodes) ->
    [consume(Config, Node, Key) || {Node, Key} <- Nodes].

set_node(Nodename, Cfg) ->
    [{nodename, Nodename} | proplists:delete(nodename, Cfg)].
