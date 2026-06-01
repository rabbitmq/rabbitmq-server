%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_osiris_metrics_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
                                replica_freshness_metrics_are_cached
                               ]}
    ].

init_per_testcase(_, Config) ->
    ok = meck:new(osiris_counters, [unstick, no_link]),
    ok = meck:new(rabbit_amqqueue, [unstick, no_link]),
    ok = meck:new(rabbit_stream_queue, [unstick, no_link]),
    ok = meck:new(rabbit_core_metrics, [unstick, no_link]),
    put(replica_freshness_calls, 0),
    Config.

end_per_testcase(_, _Config) ->
    erase(replica_freshness_calls),
    erase(last_queue_infos),
    meck:unload().

replica_freshness_metrics_are_cached(_Config) ->
    QName = rabbit_misc:r(<<"/">>, queue, <<"stream-1">>),
    FakeQueue = fake_queue,
    ReplicaFreshness = #{node() => {true, 0, 0}},

    meck:expect(osiris_counters, overview,
                fun() ->
                        #{{osiris_writer, QName} => #{offset => 10,
                                                     first_offset => 0}}
                end),
    meck:expect(rabbit_amqqueue, lookup,
                fun(Name) when Name =:= QName ->
                        {ok, FakeQueue}
                end),
    meck:expect(rabbit_stream_queue, info,
                fun(FakeQueueArg, [replica_freshness_status])
                      when FakeQueueArg =:= FakeQueue ->
                        put(replica_freshness_calls, get(replica_freshness_calls) + 1),
                        [{replica_freshness_status, ReplicaFreshness}];
                   (FakeQueueArg, Keys)
                      when FakeQueueArg =:= FakeQueue,
                           is_list(Keys) ->
                        ?assertNot(lists:member(replica_freshness_status, Keys)),
                        [{state, running}]
                end),
    meck:expect(rabbit_core_metrics, queue_stats,
                fun(_Name, _MessagesReady, _MessagesUnacknowledged, _Messages, _Reductions) ->
                        ok
                end),
    meck:expect(rabbit_core_metrics, queue_stats,
                fun(_Name, Infos) ->
                        put(last_queue_infos, Infos),
                        ok
                end),

    State0 = {state, 100000, #{}},
    {noreply, State1} = rabbit_osiris_metrics:handle_info(tick, State0),
    ?assertEqual(1, get(replica_freshness_calls)),
    ?assertEqual(ReplicaFreshness,
                 proplists:get_value(replica_freshness_status, get(last_queue_infos))),

    {noreply, _State2} = rabbit_osiris_metrics:handle_info(tick, State1),
    ?assertEqual(1, get(replica_freshness_calls)),
    ?assert(meck:validate(osiris_counters)),
    ?assert(meck:validate(rabbit_amqqueue)),
    ?assert(meck:validate(rabbit_stream_queue)),
    ?assert(meck:validate(rabbit_core_metrics)),
    passed.
