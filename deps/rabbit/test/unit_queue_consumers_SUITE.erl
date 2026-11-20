%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_queue_consumers_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
        is_same,
        get_consumer,
        get,
        list_consumers,
        list_consumers_reports_blocked,
        list_consumers_sac_active_overrides_blocked,
        list_consumers_sac_inactive_overrides_blocked,
        ack_id_reused_raw_tag_does_not_leak_across_deliveries,
        ack_id_never_minted_is_dropped_silently,
        ack_id_batch_with_unknown_id_does_not_poison_the_rest,
        expired_delivery_is_tombstoned_once_and_parks_its_consumer,
        acking_the_last_tombstone_unparks_the_consumer,
        cancel_after_expiry_does_not_leak_debt_to_a_resubscribed_tag,
        basic_cancel_after_expiry_does_not_leak_debt_to_a_resubscribed_tag,
        next_deadline_is_tightened_by_delivery_and_corrected_by_expire
    ].

is_same(_Config) ->
    ?assertEqual(
        true,
        rabbit_queue_consumers:is_same(
            self(), <<"1">>,
            consumer(self(), <<"1">>)
        )),
    ?assertEqual(
        false,
        rabbit_queue_consumers:is_same(
            self(), <<"1">>,
            consumer(self(), <<"2">>)
        )),
    Pid = spawn(?MODULE, function_for_process, []),
    Pid ! whatever,
    ?assertEqual(
        false,
        rabbit_queue_consumers:is_same(
            self(), <<"1">>,
            consumer(Pid, <<"1">>)
        )),
    ok.

get(_Config) ->
    Pid = spawn(?MODULE, function_for_process, []),
    Pid ! whatever,
    State = state(consumers([consumer(self(), <<"1">>), consumer(Pid, <<"2">>), consumer(self(), <<"3">>)])),
    {Pid, {consumer, <<"2">>, _, _, _, _, _}} =
        rabbit_queue_consumers:get(Pid, <<"2">>, State),
    ?assertEqual(
        undefined,
        rabbit_queue_consumers:get(self(), <<"2">>, State)
    ),
    ?assertEqual(
        undefined,
        rabbit_queue_consumers:get(Pid, <<"1">>, State)
    ),
    ok.

get_consumer(_Config) ->
    Pid = spawn(unit_queue_consumers_SUITE, function_for_process, []),
    Pid ! whatever,
    State = state(consumers([consumer(self(), <<"1">>), consumer(Pid, <<"2">>), consumer(self(), <<"3">>)])),
    {_Pid, {consumer, _, _, _, _, _, _}} =
        rabbit_queue_consumers:get_consumer(State),
    ?assertEqual(
        undefined,
        rabbit_queue_consumers:get_consumer(state(consumers([])))
    ),
    ok.

list_consumers(_Config) ->
    State = state(consumers([consumer(self(), <<"1">>), consumer(self(), <<"2">>), consumer(self(), <<"3">>)])),
    Consumer = rabbit_queue_consumers:get_consumer(State),
    {_Pid, ConsumerRecord} = Consumer,
    CTag = rabbit_queue_consumers:consumer_tag(ConsumerRecord),
    ConsumersWithSingleActive = rabbit_queue_consumers:all(State, Consumer, true),
    ?assertEqual(3, length(ConsumersWithSingleActive)),
    lists:foldl(fun({Pid, Tag, _, _, Active, ActivityStatus, _, _}, _Acc) ->
        ?assertEqual(self(), Pid),
        case Tag of
            CTag ->
                ?assert(Active),
                ?assertEqual(single_active, ActivityStatus);
            _ ->
                ?assertNot(Active),
                ?assertEqual(waiting, ActivityStatus)
        end
              end, [], ConsumersWithSingleActive),
    ConsumersNoSingleActive = rabbit_queue_consumers:all(State, none, false),
    ?assertEqual(3, length(ConsumersNoSingleActive)),
    lists:foldl(fun({Pid, _, _, _, Active, ActivityStatus, _, _}, _Acc) ->
                    ?assertEqual(self(), Pid),
                    ?assert(Active),
                    ?assertEqual(up, ActivityStatus)
                end, [], ConsumersNoSingleActive),
    ok.

list_consumers_reports_blocked(_Config) ->
    ChPid = self(),
    Consumer = consumer(ChPid, <<"blocked-tag">>),
    install_ch_record(ChPid, [Consumer]),
    try
        State = state(consumers([])),
        Result = rabbit_queue_consumers:all(State, none, false),
        ?assertEqual(1, length(Result)),
        [{Pid, Tag, _Ack, _Pref, Active, ActivityStatus, _Args, _User}] = Result,
        ?assertEqual(ChPid, Pid),
        ?assertEqual(<<"blocked-tag">>, Tag),
        ?assert(Active),
        ?assertEqual(blocked, ActivityStatus)
    after
        uninstall_ch_record(ChPid)
    end.

list_consumers_sac_active_overrides_blocked(_Config) ->
    ChPid = self(),
    Consumer = consumer(ChPid, <<"sac-tag">>),
    install_ch_record(ChPid, [Consumer]),
    try
        State = state(consumers([])),
        Result = rabbit_queue_consumers:all(State, Consumer, true),
        ?assertEqual(1, length(Result)),
        [{_Pid, _Tag, _Ack, _Pref, Active, ActivityStatus, _Args, _User}] = Result,
        ?assert(Active),
        ?assertEqual(single_active, ActivityStatus)
    after
        uninstall_ch_record(ChPid)
    end.

list_consumers_sac_inactive_overrides_blocked(_Config) ->
    ChPid = self(),
    HolderConsumer = consumer(ChPid, <<"sac-holder">>),
    OtherConsumer  = consumer(ChPid, <<"sac-waiting">>),
    install_ch_record(ChPid, [OtherConsumer]),
    try
        State = state(consumers([])),
        Result = rabbit_queue_consumers:all(State, HolderConsumer, true),
        ?assertEqual(1, length(Result)),
        [{_Pid, _Tag, _Ack, _Pref, Active, ActivityStatus, _Args, _User}] = Result,
        ?assertNot(Active),
        ?assertEqual(waiting, ActivityStatus)
    after
        uninstall_ch_record(ChPid)
    end.

%% #cr field order: ch_pid, monitor_ref, acktags, tombstones,
%% tombstoned_ctags, next_ack_id, next_deadline, consumer_count,
%% blocked_consumers, limiter, unsent_message_count, link_states.
-define(TEST_TIMEOUT, 600_000).

install_ch_record(ChPid, ConsumerEntries) ->
    BlockedQ = lists:foldl(fun (C, Acc) -> priority_queue:in(C, Acc) end,
                           priority_queue:new(), ConsumerEntries),
    CR = {cr, ChPid, erlang:make_ref(), #{}, #{}, #{}, 0, infinity,
          length(ConsumerEntries),
          BlockedQ, rabbit_limiter:client(ChPid), 0, #{}},
    put({ch, ChPid}, CR),
    ok.

uninstall_ch_record(ChPid) ->
    _ = erase({ch, ChPid}),
    ok.

%% Two deliveries can carry the same raw ack tag (raw_ack_tag()s may be
%% reused across delivery attempts, e.g. after a requeue). Acking the
%% ack_id() of one must not affect the other.
ack_id_reused_raw_tag_does_not_leak_across_deliveries(_Config) ->
    _ = rabbit_queue_consumers:new(),
    ChPid = self(),
    install_ch_record(ChPid, []),
    try
        QName = <<"test-queue">>,
        C1 = consumer(ChPid, <<"c1">>),
        C2 = consumer(ChPid, <<"c2">>),
        FetchFun = fun(_AckRequired) -> {{msg, false, 42}, unused} end,

        {delivered, [], unused, _} =
            rabbit_queue_consumers:deliver(
              FetchFun, QName, state(consumers([C1])), false, none),
        {cr, ChPid, _, AckTags1, _, _, 1, _, _, _, _, _, _} = erlang:get({ch, ChPid}),
        [{AckId0, {42, <<"c1">>, _}}] = maps:to_list(AckTags1),

        {delivered, [], unused, _} =
            rabbit_queue_consumers:deliver(
              FetchFun, QName, state(consumers([C2])), false, none),
        {cr, ChPid, _, AckTags2, _, _, 2, _, _, _, _, _, _} = erlang:get({ch, ChPid}),
        AckId1 = 1,
        ?assertNotEqual(AckId0, AckId1),
        ?assertEqual(2, maps:size(AckTags2)),
        ?assertMatch(#{AckId0 := {42, <<"c1">>, _}, AckId1 := {42, <<"c2">>, _}},
                     AckTags2),

        {[42], unchanged} =
            rabbit_queue_consumers:subtract_acks(
              ChPid, [AckId0], state(consumers([]))),
        {cr, ChPid, _, AckTags3, _, _, _, _, _, _, _, _, _} = erlang:get({ch, ChPid}),
        ?assertEqual(1, maps:size(AckTags3)),
        ?assertMatch(#{AckId1 := {42, <<"c2">>, _}}, AckTags3)
    after
        uninstall_ch_record(ChPid)
    end.

%% An ack_id() that was never minted for this channel resolves to nothing.
ack_id_never_minted_is_dropped_silently(_Config) ->
    _ = rabbit_queue_consumers:new(),
    ChPid = self(),
    install_ch_record(ChPid, []),
    try
        QName = <<"test-queue">>,
        C1 = consumer(ChPid, <<"c1">>),
        FetchFun = fun(_AckRequired) -> {{msg, false, 42}, unused} end,
        {delivered, [], unused, _} =
            rabbit_queue_consumers:deliver(
              FetchFun, QName, state(consumers([C1])), false, none),

        {[], unchanged} =
            rabbit_queue_consumers:subtract_acks(
              ChPid, [999], state(consumers([]))),
        {cr, ChPid, _, AckTags, _, _, _, _, _, _, _, _, _} = erlang:get({ch, ChPid}),
        ?assertEqual(1, maps:size(AckTags)),
        ?assertMatch([{_, {42, <<"c1">>, _}}], maps:to_list(AckTags))
    after
        uninstall_ch_record(ChPid)
    end.

%% A miss (tombstoned or unknown id) mid-batch must not poison later ids.
ack_id_batch_with_unknown_id_does_not_poison_the_rest(_Config) ->
    _ = rabbit_queue_consumers:new(),
    ChPid = self(),
    install_ch_record(ChPid, []),
    try
        QName = <<"test-queue">>,
        CDue = consumer(ChPid, <<"due">>, 0),
        CNever = consumer(ChPid, <<"never">>, ?TEST_TIMEOUT),
        FetchFunDue = fun(_AckRequired) -> {{msg1, false, 42}, unused} end,
        FetchFunNever = fun(_AckRequired) -> {{msg2, false, 43}, unused} end,

        {delivered, [], unused, _} =
            rabbit_queue_consumers:deliver(
              FetchFunDue, QName, state(consumers([CDue])), false, none),
        {delivered, [], unused, State2} =
            rabbit_queue_consumers:deliver(
              FetchFunNever, QName, state(consumers([CNever])), false, none),

        {cr, ChPid, _, AckTags0, _, _, _, _, _, _, _, _, _} = erlang:get({ch, ChPid}),
        [TombAckId, LiveAckId] = lists:sort(maps:keys(AckTags0)),

        Now = erlang:monotonic_time(millisecond),
        {[{ChPid, <<"due">>, [TombAckId], [42]}], _, State3} =
            rabbit_queue_consumers:expire_acks(Now, State2),

        ResolvedAckTags =
            case rabbit_queue_consumers:subtract_acks(
                   ChPid, [TombAckId, 999999, LiveAckId], State3) of
                {Resolved, unchanged} -> Resolved;
                {Resolved, unblocked, _, _} -> Resolved
            end,
        ?assertEqual([43], ResolvedAckTags),

        {cr, ChPid, _, AckTags1, Tombstones1, TombCTags1, _, _, _, _, _, _, _} =
            erlang:get({ch, ChPid}),
        ?assertEqual(0, maps:size(AckTags1)),
        ?assertEqual(0, maps:size(Tombstones1)),
        ?assertEqual(0, maps:size(TombCTags1))
    after
        uninstall_ch_record(ChPid)
    end.

%% A delivery whose deadline has passed is tombstoned (not deleted) exactly
%% once, its raw tag is surfaced for requeueing exactly once, and its
%% consumer is parked (moved out of the ready set) until the tombstone is
%% cleared.
expired_delivery_is_tombstoned_once_and_parks_its_consumer(_Config) ->
    _ = rabbit_queue_consumers:new(),
    ChPid = self(),
    install_ch_record(ChPid, []),
    try
        QName = <<"test-queue">>,
        C1 = consumer(ChPid, <<"c1">>, 0),
        FetchFun = fun(_AckRequired) -> {{msg, false, 42}, unused} end,
        State0 = state(consumers([C1])),
        {delivered, [], unused, State1} =
            rabbit_queue_consumers:deliver(FetchFun, QName, State0, false, none),

        Now = erlang:monotonic_time(millisecond),
        {Expired, NextDeadline, State2} =
            rabbit_queue_consumers:expire_acks(Now, State1),
        ?assertEqual([{ChPid, <<"c1">>, [0], [42]}], Expired),
        ?assertEqual(infinity, NextDeadline),
        %% Tombstoned, not live -- no longer "held".
        ?assertNot(rabbit_queue_consumers:holds_acks(ChPid, <<"c1">>)),

        %% Expiring again at the same (or a later) time must not resurface
        %% the same raw tag a second time.
        {Expired2, _, State3} = rabbit_queue_consumers:expire_acks(Now, State2),
        ?assertEqual([], Expired2),

        %% The consumer is no longer in the ready set.
        FetchFun2 = fun(_AckRequired) -> {{msg2, false, 43}, unused} end,
        ?assertMatch(
           {undelivered, _, _},
           rabbit_queue_consumers:deliver(FetchFun2, QName, State3, false, none))
    after
        uninstall_ch_record(ChPid)
    end.

%% Acking the ack_id() of the (only) tombstoned entry under a tag un-parks
%% its consumer.
acking_the_last_tombstone_unparks_the_consumer(_Config) ->
    _ = rabbit_queue_consumers:new(),
    ChPid = self(),
    install_ch_record(ChPid, []),
    try
        QName = <<"test-queue">>,
        C1 = consumer(ChPid, <<"c1">>, 0),
        FetchFun = fun(_AckRequired) -> {{msg, false, 42}, unused} end,
        State0 = state(consumers([C1])),
        {delivered, [], unused, State1} =
            rabbit_queue_consumers:deliver(FetchFun, QName, State0, false, none),
        Now = erlang:monotonic_time(millisecond),
        {[{ChPid, <<"c1">>, [0], [42]}], _, State2} =
            rabbit_queue_consumers:expire_acks(Now, State1),

        {cr, ChPid, _, AckTags, Tombstones, _, _, _, _, _, _, _, _} =
            erlang:get({ch, ChPid}),
        ?assertEqual(0, maps:size(AckTags)),
        [{AckId, <<"c1">>}] = maps:to_list(Tombstones),

        State3 = case rabbit_queue_consumers:subtract_acks(ChPid, [AckId], State2) of
                    {[], unchanged} -> State2;
                    {[], unblocked, _UnblockedConsumers, S} -> S
                 end,

        %% Un-parked: a fresh delivery to c1 succeeds again.
        FetchFun2 = fun(_AckRequired) -> {{msg2, false, 43}, unused} end,
        ?assertMatch(
           {delivered, [], unused, _},
           rabbit_queue_consumers:deliver(FetchFun2, QName, State3, false, none))
    after
        uninstall_ch_record(ChPid)
    end.

%% A consumer cancelled while it holds a tombstoned entry must not leave that
%% debt behind for a later consumer that resubscribes under the same tag. Since
%% acktags is the only ledger and remove/4 drops every entry under the
%% cancelled tag (tombstoned or not), resubscriptions start clean.
cancel_after_expiry_does_not_leak_debt_to_a_resubscribed_tag(_Config) ->
    _ = rabbit_queue_consumers:new(),
    ChPid = self(),
    install_ch_record(ChPid, []),
    try
        QName = <<"test-queue">>,
        C1 = consumer(ChPid, <<"c1">>, 0),
        FetchFun = fun(_AckRequired) -> {{msg, false, 42}, unused} end,
        State0 = state(consumers([C1])),
        {delivered, [], unused, State1} =
            rabbit_queue_consumers:deliver(FetchFun, QName, State0, false, none),
        Now = erlang:monotonic_time(millisecond),
        {[{ChPid, <<"c1">>, [0], [42]}], _, State2} =
            rabbit_queue_consumers:expire_acks(Now, State1),
        %% Only a tombstone remains under c1, nothing live.
        ?assertNot(rabbit_queue_consumers:holds_acks(ChPid, <<"c1">>)),

        %% The consumer is cancelled while its tombstone is still present.
        {RequeuedTags, _State3} =
            rabbit_queue_consumers:remove(ChPid, <<"c1">>, remove, State2),
        %% The tombstoned tag is already back in the backing queue, it
        %% must not be handed back for a second, duplicate requeue.
        ?assertEqual([], RequeuedTags),
        ?assertNot(rabbit_queue_consumers:holds_acks(ChPid, <<"c1">>)),
        %% Tombstone bookkeeping for c1 must be purged too.
        case erlang:get({ch, ChPid}) of
            undefined ->
                ok;
            {cr, ChPid, _, _, Tombstones2, TombCTags2, _, _, _, _, _, _, _} ->
                ?assertNot(maps:is_key(<<"c1">>, TombCTags2)),
                ?assertEqual([], [CT || CT <- maps:values(Tombstones2), CT =:= <<"c1">>])
        end,

        %% A new consumer resubscribes under the same tag and must not
        %% inherit any debt: a fresh delivery succeeds immediately.
        C1b = consumer(ChPid, <<"c1">>, 0),
        FetchFun2 = fun(_AckRequired) -> {{msg2, false, 44}, unused} end,
        ?assertMatch(
           {delivered, [], unused, _},
           rabbit_queue_consumers:deliver(
             FetchFun2, QName, state(consumers([C1b])), false, none))
    after
        uninstall_ch_record(ChPid)
    end.

basic_cancel_after_expiry_does_not_leak_debt_to_a_resubscribed_tag(_Config) ->
    _ = rabbit_queue_consumers:new(),
    ChPid = self(),
    install_ch_record(ChPid, []),
    try
        QName = <<"test-queue">>,
        C1 = consumer(ChPid, <<"c1">>, 0),
        FetchFun = fun(_AckRequired) -> {{msg, false, 42}, unused} end,
        State0 = state(consumers([C1])),
        {delivered, [], unused, State1} =
            rabbit_queue_consumers:deliver(FetchFun, QName, State0, false, none),
        Now = erlang:monotonic_time(millisecond),
        {[{ChPid, <<"c1">>, [0], [42]}], _, State2} =
            rabbit_queue_consumers:expire_acks(Now, State1),
        %% Only a tombstone remains under c1, nothing live.
        ?assertNot(rabbit_queue_consumers:holds_acks(ChPid, <<"c1">>)),

        %% The consumer sends a plain basic.cancel while its tombstone is
        %% present.
        {RequeuedTags, _State3} =
            rabbit_queue_consumers:remove(ChPid, <<"c1">>, cancel, State2),
        %% The tombstoned tag is already back in the backing queue, it
        %% must not be handed back for a second, duplicate requeue.
        ?assertEqual([], RequeuedTags),
        ?assertNot(rabbit_queue_consumers:holds_acks(ChPid, <<"c1">>)),
        %% Tombstone bookkeeping for c1 must be purged too, exactly as it
        %% would be for Reason = remove.
        case erlang:get({ch, ChPid}) of
            undefined ->
                ok;
            {cr, ChPid, _, _, Tombstones2, TombCTags2, _, _, _, _, _, _, _} ->
                ?assertNot(maps:is_key(<<"c1">>, TombCTags2)),
                ?assertEqual([], [CT || _ := CT <- Tombstones2, CT =:= <<"c1">>])
        end,

        %% A new consumer resubscribes under the same tag and must not
        %% inherit any debt. A fresh delivery succeeds immediately.
        C1b = consumer(ChPid, <<"c1">>, 0),
        FetchFun2 = fun(_AckRequired) -> {{msg2, false, 44}, unused} end,
        ?assertMatch(
           {delivered, [], unused, _},
           rabbit_queue_consumers:deliver(
             FetchFun2, QName, state(consumers([C1b])), false, none))
    after
        uninstall_ch_record(ChPid)
    end.

%% next_deadline/1 is a cache: deliver/5 tightens it with min/2, and
%% expire_acks/2 corrects it back to ground truth (the still-live entry's
%% deadline), never leaving it pinned to an entry that already expired.
next_deadline_is_tightened_by_delivery_and_corrected_by_expire(_Config) ->
    _ = rabbit_queue_consumers:new(),
    ChPid = self(),
    install_ch_record(ChPid, []),
    try
        QName = <<"test-queue">>,
        ?assertEqual(infinity, rabbit_queue_consumers:next_deadline(state(consumers([])))),

        CFar = consumer(ChPid, <<"far">>, 600_000),
        FetchFun1 = fun(_AckRequired) -> {{msg1, false, 1}, unused} end,
        {delivered, [], unused, State1} =
            rabbit_queue_consumers:deliver(
              FetchFun1, QName, state(consumers([CFar])), false, none),
        Hint1 = rabbit_queue_consumers:next_deadline(State1),
        ?assert(is_integer(Hint1)),

        %% Thread State1's hint through to the next delivery, the way
        %% #q.consumers is threaded through every queue operation in the SUT.
        {state, _, Use1, Hint1} = State1,
        CNear = consumer(ChPid, <<"near">>, 0),
        FetchFun2 = fun(_AckRequired) -> {{msg2, false, 2}, unused} end,
        {delivered, [], unused, State2} =
            rabbit_queue_consumers:deliver(
              FetchFun2, QName, {state, consumers([CNear]), Use1, Hint1}, false, none),
        Hint2 = rabbit_queue_consumers:next_deadline(State2),
        ?assert(Hint2 =< Hint1),

        Now = erlang:monotonic_time(millisecond),
        {Expired, _NextDeadline, State3} = rabbit_queue_consumers:expire_acks(Now, State2),
        ?assertMatch([{ChPid, <<"near">>, [_], [2]}], Expired),
        %% The far delivery is still outstanding: the corrected hint must
        %% reflect it, not stay pinned to the now-expired near one.
        ?assertEqual(Hint1, rabbit_queue_consumers:next_deadline(State3))
    after
        uninstall_ch_record(ChPid)
    end.

consumers([]) ->
    priority_queue:new();
consumers(Consumers) ->
    consumers(Consumers, priority_queue:new()).

consumers([H], Q) ->
    priority_queue:in(H, Q);
consumers([H | T], Q) ->
    consumers(T, priority_queue:in(H, Q)).


consumer(Pid, ConsumerTag) ->
    consumer(Pid, ConsumerTag, ?TEST_TIMEOUT).

consumer(Pid, ConsumerTag, Timeout) ->
    {Pid, {consumer, ConsumerTag, true, 1, [], <<"guest">>, Timeout}}.

state(Consumers) ->
    {state, Consumers, {active, erlang:monotonic_time(microsecond), 1.0}, infinity}.

function_for_process() ->
    receive
        _ -> ok
    end.
