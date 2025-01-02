%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(upgrade_preparation_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

all() ->
    [
      {group, clustered}
    ].

groups() ->
    [
     {clustered, [], [
         await_quorum_plus_one_qq,
         await_quorum_plus_one_stream,
         await_quorum_plus_one_stream_coordinator,
         await_quorum_plus_one_rabbitmq_metadata
     ]}
    ].


%% -------------------------------------------------------------------
%% Test Case
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [
                                            {rmq_nodes_count, 3},
                                            {rmq_nodename_suffix, Group}
                                           ]),
    rabbit_ct_helpers:run_steps(Config1,
                                rabbit_ct_broker_helpers:setup_steps() ++
                                rabbit_ct_client_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
              rabbit_ct_client_helpers:teardown_steps() ++
              rabbit_ct_broker_helpers:teardown_steps()).


init_per_testcase(Testcase, Config) when Testcase == await_quorum_plus_one_rabbitmq_metadata ->
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            {skip, "not mixed versions compatible"};
        _ ->
            rabbit_ct_helpers:testcase_started(Config, Testcase)
    end;
init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(TestCase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, TestCase).



%%
%% Test Cases
%%

-define(WAITING_INTERVAL, 10000).

await_quorum_plus_one_qq(Config) ->
    catch delete_queues(),
    [A, B, _C] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, A),
    declare(Ch, <<"qq.1">>, [{<<"x-queue-type">>, longstr, <<"quorum">>}]),
    timer:sleep(100),
    ?assert(await_quorum_plus_one(Config, 0)),

    ok = rabbit_ct_broker_helpers:stop_node(Config, B),
    ?assertNot(await_quorum_plus_one(Config, 0)),

    ok = rabbit_ct_broker_helpers:start_node(Config, B),
    ?assert(await_quorum_plus_one(Config, 0)).

await_quorum_plus_one_stream(Config) ->
    catch delete_queues(),
    [A, B, _C] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, A),
    declare(Ch, <<"st.1">>, [{<<"x-queue-type">>, longstr, <<"stream">>}]),
    timer:sleep(100),
    ?assert(await_quorum_plus_one(Config, 0)),

    ok = rabbit_ct_broker_helpers:stop_node(Config, B),
    ?assertNot(await_quorum_plus_one(Config, 0)),

    ok = rabbit_ct_broker_helpers:start_node(Config, B),
    ?assert(await_quorum_plus_one(Config, 0)).

await_quorum_plus_one_stream_coordinator(Config) ->
    catch delete_queues(),
    [A, B, _C] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, A),
    declare(Ch, <<"st.1">>, [{<<"x-queue-type">>, longstr, <<"stream">>}]),
    timer:sleep(100),
    ?assert(await_quorum_plus_one(Config, 0)),
    delete(Ch, <<"st.1">>),
    %% no queues/streams beyond this point

    ok = rabbit_ct_broker_helpers:stop_node(Config, B),
    %% this should fail because the coordinator has only 2 running nodes
    ?assertNot(await_quorum_plus_one(Config, 0)),

    ok = rabbit_ct_broker_helpers:start_node(Config, B),
    ?assert(await_quorum_plus_one(Config, 0)).

await_quorum_plus_one_rabbitmq_metadata(Config) ->
    Nodes = [A, B, _C] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    ok = rabbit_ct_broker_helpers:enable_feature_flag(Config, Nodes, khepri_db),
    ?assert(await_quorum_plus_one(Config, A)),

    ok = rabbit_ct_broker_helpers:stop_node(Config, B),
    %% this should fail because rabbitmq_metadata has only 2 running nodes
    ?assertNot(await_quorum_plus_one(Config, A)),

    ok = rabbit_ct_broker_helpers:start_node(Config, B),
    ?assert(await_quorum_plus_one(Config, A)).

%%
%% Implementation
%%

declare(Ch, Q) ->
    declare(Ch, Q, []).

declare(Ch, Q, Args) ->
    amqp_channel:call(Ch, #'queue.declare'{queue     = Q,
                                           durable   = true,
                                           auto_delete = false,
                                           arguments = Args}).

delete(Ch, Q) ->
    amqp_channel:call(Ch, #'queue.delete'{queue = Q}).

delete_queues() ->
    [rabbit_amqqueue:delete(Q, false, false, <<"tests">>) || Q <- rabbit_amqqueue:list()].

await_quorum_plus_one(Config, Node) ->
    await_quorum_plus_one(Config, Node, ?WAITING_INTERVAL).

await_quorum_plus_one(Config, Node, Timeout) ->
    rabbit_ct_broker_helpers:rpc(Config, Node,
        rabbit_upgrade_preparation, await_online_quorum_plus_one, [Timeout], Timeout + 500).

