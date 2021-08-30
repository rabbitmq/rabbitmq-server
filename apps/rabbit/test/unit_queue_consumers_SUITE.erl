%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2018-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(unit_queue_consumers_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
        is_same,
        get_consumer,
        get,
        list_consumers
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
