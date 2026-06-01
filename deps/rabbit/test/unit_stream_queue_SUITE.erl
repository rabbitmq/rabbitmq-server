%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_stream_queue_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
                                minimum_quorum_uses_live_freshness
                               ]}
    ].

init_per_testcase(_, Config) ->
    ok = meck:new(rabbit_amqqueue, [unstick, no_link]),
    ok = meck:new(amqqueue, [unstick, no_link]),
    ok = meck:new(rabbit_stream_coordinator, [unstick, no_link]),
    ok = meck:new(osiris_writer, [unstick, no_link]),
    _ = ets:new(queue_metrics, [named_table, public, set]),
    put(replication_state_queries, 0),
    Config.

end_per_testcase(_, _Config) ->
    erase(replication_state_queries),
    catch ets:delete(queue_metrics),
    meck:unload().

minimum_quorum_uses_live_freshness(_Config) ->
    QName = rabbit_misc:r(<<"/">>, queue, <<"stream-1">>),
    StreamId = <<"stream-id">>,
    Queue = fake_queue,
    LeaderNode = node(),
    Replica1 = 'replica-1@tests',
    Replica2 = 'replica-2@tests',
    LeaderPid = spawn(fun() -> receive after infinity -> ok end end),

    ets:insert(queue_metrics,
               {QName, [{replica_freshness_status,
                         #{LeaderNode => {true, 0, 0},
                           Replica1 => {true, 0, 0},
                           Replica2 => {true, 0, 0}}}],
                false}),

    meck:expect(rabbit_amqqueue, list_local_stream_queues,
                fun() -> [Queue] end),
    meck:expect(amqqueue, get_type_state,
                fun(QueueArg) when QueueArg =:= Queue ->
                        #{name => StreamId}
                end),
    meck:expect(amqqueue, get_pid,
                fun(QueueArg) when QueueArg =:= Queue ->
                        LeaderPid
                end),
    meck:expect(rabbit_stream_coordinator, members,
                fun(StreamIdArg) when StreamIdArg =:= StreamId ->
                        {ok, #{LeaderNode => {running, leader},
                               Replica1 => {running, replica},
                               Replica2 => {running, replica}}}
                end),
    meck:expect(rabbit_stream_coordinator, is_replica_fresh,
                fun(LeaderTs, ReplicaTs) ->
                        LeaderTs =:= ReplicaTs
                end),
    meck:expect(osiris_writer, query_replication_state,
                fun(LeaderPidArg) when LeaderPidArg =:= LeaderPid ->
                        put(replication_state_queries, get(replication_state_queries) + 1),
                        #{LeaderNode => {10, 100},
                          Replica1 => {9, 95},
                          Replica2 => {10, 100}}
                end),

    try
        ?assertEqual([Queue], rabbit_stream_queue:list_with_minimum_quorum()),
        ?assertEqual(1, get(replication_state_queries)),
        ?assert(meck:validate(rabbit_amqqueue)),
        ?assert(meck:validate(amqqueue)),
        ?assert(meck:validate(rabbit_stream_coordinator)),
        ?assert(meck:validate(osiris_writer)),
        passed
    after
        exit(LeaderPid, kill)
    end.
