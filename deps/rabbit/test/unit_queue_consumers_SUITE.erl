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
        ack_id_never_minted_is_dropped_silently
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
    {Pid, {consumer, <<"2">>, _, _, _, _}} =
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
    {_Pid, {consumer, _, _, _, _, _}} =
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

%% #cr field order: ch_pid, monitor_ref, acktags, next_ack_id,
%% consumer_count, blocked_consumers, limiter, unsent_message_count,
%% link_states.
install_ch_record(ChPid, ConsumerEntries) ->
    BlockedQ = lists:foldl(fun (C, Acc) -> priority_queue:in(C, Acc) end,
                           priority_queue:new(), ConsumerEntries),
    CR = {cr, ChPid, erlang:make_ref(), #{}, 0, length(ConsumerEntries),
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
        {cr, ChPid, _, AckTags1, 1, _, _, _, _, _} = erlang:get({ch, ChPid}),
        [{AckId0, {42, <<"c1">>}}] = maps:to_list(AckTags1),

        {delivered, [], unused, _} =
            rabbit_queue_consumers:deliver(
              FetchFun, QName, state(consumers([C2])), false, none),
        {cr, ChPid, _, AckTags2, 2, _, _, _, _, _} = erlang:get({ch, ChPid}),
        AckId1 = 1,
        ?assertNotEqual(AckId0, AckId1),
        ?assertEqual(#{AckId0 => {42, <<"c1">>}, AckId1 => {42, <<"c2">>}},
                     AckTags2),

        {[42], unchanged} =
            rabbit_queue_consumers:subtract_acks(
              ChPid, [AckId0], state(consumers([]))),
        {cr, ChPid, _, AckTags3, _, _, _, _, _, _} = erlang:get({ch, ChPid}),
        ?assertEqual(#{AckId1 => {42, <<"c2">>}}, AckTags3)
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
        {cr, ChPid, _, AckTags, _, _, _, _, _, _} = erlang:get({ch, ChPid}),
        ?assertMatch([{_, {42, <<"c1">>}}], maps:to_list(AckTags))
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
    {Pid, {consumer, ConsumerTag, true, 1, [], <<"guest">>}}.

state(Consumers) ->
    {state, Consumers, {}}.

function_for_process() ->
    receive
        _ -> ok
    end.
