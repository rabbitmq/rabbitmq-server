%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(pending_count_SUITE).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit/include/mc.hrl").
-include("../include/rabbit_shovel.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, pending_count_tests}
    ].

groups() ->
    [
     {pending_count_tests, [], [
         amqp091_pending_count_empty_queue,
         amqp091_pending_count_with_messages,
         amqp091_pending_count_after_drain,
         amqp10_pending_count_empty_list,
         amqp10_pending_count_with_messages,
         amqp10_pending_count_after_clear,
         local_pending_count_empty_queue,
         local_pending_count_after_settle,
         behaviour_metrics_includes_pending,
         behaviour_pending_count_delegation
        ]}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    meck:unload(),
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

%% Test AMQP 0.9.1 pending_count functionality
amqp091_pending_count_empty_queue(_Config) ->
    %% Test that pending_count returns 0 when no messages are pending
    State = #{dest => #{}},
    ?assertEqual(0, rabbit_amqp091_shovel:pending_count(State)).

amqp091_pending_count_with_messages(_Config) ->
    %% Test that pending_count returns correct count when messages are pending
    PendingQueue = lqueue:from_list([{1, msg1}, {2, msg2}, {3, msg3}]),
    State = #{dest => #{pending => PendingQueue}},
    ?assertEqual(3, rabbit_amqp091_shovel:pending_count(State)).

amqp091_pending_count_after_drain(_Config) ->
    %% Test that pending_count returns 0 after messages are drained
    EmptyQueue = lqueue:new(),
    State = #{dest => #{pending => EmptyQueue}},
    ?assertEqual(0, rabbit_amqp091_shovel:pending_count(State)).

%% Test AMQP 1.0 pending_count functionality
amqp10_pending_count_empty_list(_Config) ->
    %% Test that pending_count returns 0 when no messages are pending
    State = #{dest => #{}},
    ?assertEqual(0, rabbit_amqp10_shovel:pending_count(State)).

amqp10_pending_count_with_messages(_Config) ->
    %% Test that pending_count returns correct count when messages are pending
    PendingList = [{1, msg1}, {2, msg2}],
    State = #{dest => #{pending => PendingList}},
    ?assertEqual(2, rabbit_amqp10_shovel:pending_count(State)).

amqp10_pending_count_after_clear(_Config) ->
    %% Test that pending_count returns 0 after pending list is cleared
    State = #{dest => #{pending => []}},
    ?assertEqual(0, rabbit_amqp10_shovel:pending_count(State)).

%% Test Local shovel pending_count functionality
local_pending_count_empty_queue(_Config) ->
    %% Test that pending_count returns 0 when unacked message queue is empty
    EmptyQueue = lqueue:new(),
    State = #{source => #{current => #{unacked_message_q => EmptyQueue}}},
    ?assertEqual(0, rabbit_local_shovel:pending_count(State)).


local_pending_count_after_settle(_Config) ->
    %% Test that pending_count returns 0 when state doesn't contain unacked queue
    State = #{source => #{current => #{}}},
    ?assertEqual(0, rabbit_local_shovel:pending_count(State)).

%% Test behaviour module integration
behaviour_metrics_includes_pending(_Config) ->
    %% Mock the destination module's pending_count and status functions
    meck:new(rabbit_amqp091_shovel, [passthrough]),
    meck:expect(rabbit_amqp091_shovel, pending_count, fun(_) -> 5 end),
    meck:expect(rabbit_amqp091_shovel, status, fun(_) -> running end),

    State = #{source => #{remaining => 10, remaining_unacked => 3},
              dest => #{module => rabbit_amqp091_shovel, forwarded => 7}},

    {_Status, Metrics} = rabbit_shovel_behaviour:status(State),

    ?assertMatch(#{remaining := 10,
                   remaining_unacked := 3,
                   pending := 5,
                   forwarded := 7}, Metrics),

    ?assert(meck:validate(rabbit_amqp091_shovel)).

behaviour_pending_count_delegation(_Config) ->
    %% Test that the behaviour module correctly delegates to the specific implementation
    meck:new(rabbit_amqp10_shovel, [passthrough]),
    meck:expect(rabbit_amqp10_shovel, pending_count, fun(_State) -> 3 end),
    meck:expect(rabbit_amqp10_shovel, status, fun(_State) -> running end),

    State = #{dest => #{module => rabbit_amqp10_shovel}},

    %% This would be called indirectly through status/1
    {_Status, Metrics} = rabbit_shovel_behaviour:status(#{source => #{},
                                                          dest => maps:get(dest, State)}),

    ?assertEqual(3, maps:get(pending, Metrics)),
    ?assert(meck:validate(rabbit_amqp10_shovel)).
