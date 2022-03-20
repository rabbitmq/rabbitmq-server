%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2018-2022 VMware, Inc. or its affiliates.  All rights reserved.

-module(rabbit_fifo_dlx_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit/src/rabbit_fifo.hrl").
-include_lib("rabbit/src/rabbit_fifo_dlx.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, tests}
    ].


groups() ->
    [
     {tests, [], [handler_undefined,
                  handler_at_most_once,
                  discard_dlx_consumer,
                  switch_strategies,
                  last_consumer_wins]}
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
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

handler_undefined(_Config) ->
    S = rabbit_fifo_dlx:init(),
    Handler = undefined,
    ?assertEqual({S, [{mod_call, rabbit_global_counters, messages_dead_lettered,
                       [because, rabbit_quorum_queue, disabled, 1]}]},
                 rabbit_fifo_dlx:discard([make_msg(1)], because, Handler, S)),
    ok.

handler_at_most_once(_Config) ->
    S = rabbit_fifo_dlx:init(),
    Handler = {at_most_once, {m, f, [a]}},
    {S, Effects} = rabbit_fifo_dlx:discard([make_msg(1),
                                            make_msg(2)], because, Handler, S),
    ?assertMatch([{log, [1, 2], _}], Effects),
    ok.

discard_dlx_consumer(_Config) ->
    Handler = at_least_once,
    S0 = rabbit_fifo_dlx:init(),
    ?assertEqual(#{num_discarded => 0,
                   num_discard_checked_out => 0,
                   discard_message_bytes => 0,
                   discard_checkout_message_bytes => 0}, rabbit_fifo_dlx:overview(S0)),

    %% message without dlx consumer
    {S1, [{mod_call, rabbit_global_counters, messages_dead_lettered,
           [because, rabbit_quorum_queue, at_least_once, 1]}]} =
        rabbit_fifo_dlx:discard([make_msg(1)], because, Handler, S0),
    {S2, []} = rabbit_fifo_dlx:checkout(Handler, S1),
    ?assertEqual(#{num_discarded => 1,
                   num_discard_checked_out => 0,
                   discard_message_bytes => 1,
                   discard_checkout_message_bytes => 0}, rabbit_fifo_dlx:overview(S2)),

    %% with dlx consumer
    Checkout = rabbit_fifo_dlx:make_checkout(self(), 2),
    {S3, []} = rabbit_fifo_dlx:apply(meta(2), Checkout, Handler, S2),
    {S4, DeliveryEffects0} = rabbit_fifo_dlx:checkout(Handler, S3),
    ?assertEqual(#{num_discarded => 0,
                   num_discard_checked_out => 1,
                   discard_message_bytes => 0,
                   discard_checkout_message_bytes => 1}, rabbit_fifo_dlx:overview(S4)),
    ?assertMatch([{log, [1], _}], DeliveryEffects0),

    %% more messages than dlx consumer's prefetch
    {S5, [_ModCallGlobalCounter]} = rabbit_fifo_dlx:discard([make_msg(3), make_msg(4)], because, Handler, S4),
    {S6, DeliveryEffects1} = rabbit_fifo_dlx:checkout(Handler, S5),
    ?assertEqual(#{num_discarded => 1,
                   num_discard_checked_out => 2,
                   discard_message_bytes => 1,
                   discard_checkout_message_bytes => 2}, rabbit_fifo_dlx:overview(S6)),
    ?assertMatch([{log, [3], _}], DeliveryEffects1),
    ?assertEqual({3, 3}, rabbit_fifo_dlx:stat(S6)),

    %% dlx consumer acks messages
    Settle = rabbit_fifo_dlx:make_settle([0,1]),
    {S7, [{mod_call, rabbit_global_counters, messages_dead_lettered_confirmed,
           [rabbit_quorum_queue, at_least_once, 2]}]} =
    rabbit_fifo_dlx:apply(meta(5), Settle, Handler, S6),
    {S8, DeliveryEffects2} = rabbit_fifo_dlx:checkout(Handler, S7),
    ?assertEqual(#{num_discarded => 0,
                   num_discard_checked_out => 1,
                   discard_message_bytes => 0,
                   discard_checkout_message_bytes => 1}, rabbit_fifo_dlx:overview(S8)),
    ?assertMatch([{log, [4], _}], DeliveryEffects2),
    ?assertEqual({1, 1}, rabbit_fifo_dlx:stat(S8)),
    ok.

switch_strategies(_Config) ->
    QRes = #resource{virtual_host = <<"/">>,
                     kind = queue,
                     name = <<"blah">>},
    application:set_env(rabbit, dead_letter_worker_consumer_prefetch, 1),
    application:set_env(rabbit, dead_letter_worker_publisher_confirm_timeout, 1000),
    {ok, _} = rabbit_fifo_dlx_sup:start_link(),
    S0 = rabbit_fifo_dlx:init(),

    Handler0 = undefined,
    Handler1 = at_least_once,
    %% Switching from undefined to at_least_once should start dlx consumer.
    {S1, Effects0} = rabbit_fifo_dlx:update_config(Handler0, Handler1, QRes, S0),
    ?assertEqual([{mod_call, rabbit_log, debug,
                   ["Switching dead_letter_handler from ~p to ~p for ~s",
                    [undefined, at_least_once, "queue 'blah' in vhost '/'"]]},
                  {aux, {dlx, setup}}],
                 Effects0),
    rabbit_fifo_dlx:handle_aux(leader, {dlx, setup}, fake_aux, QRes, Handler1, S1),
    [{_, WorkerPid, worker, _}] = supervisor:which_children(rabbit_fifo_dlx_sup),
    {S2, _} = rabbit_fifo_dlx:discard([make_msg(1)], because, Handler1, S1),
    Checkout = rabbit_fifo_dlx:make_checkout(WorkerPid, 1),
    {S3, _} = rabbit_fifo_dlx:apply(meta(2), Checkout, Handler1, S2),
    {S4, _} = rabbit_fifo_dlx:checkout(Handler1, S3),
    ?assertMatch(#{num_discard_checked_out := 1}, rabbit_fifo_dlx:overview(S4)),

    %% Switching from at_least_once to undefined should terminate dlx consumer.
    {S5, Effects} = rabbit_fifo_dlx:update_config(Handler1, Handler0, QRes, S4),
    ?assertEqual([{mod_call, rabbit_log, debug,
                   ["Switching dead_letter_handler from ~p to ~p for ~s",
                    [at_least_once, undefined, "queue 'blah' in vhost '/'"]]},
                  {mod_call, rabbit_log, info,
                   ["Deleted ~b dead-lettered messages (with total messages size of ~b bytes) in ~s",
                    [1, 1, "queue 'blah' in vhost '/'"]]}],
                 Effects),
    ?assertMatch([_, {active, 0}, _, _],
                 supervisor:count_children(rabbit_fifo_dlx_sup)),
    ?assertMatch(#{num_discarded := 0}, rabbit_fifo_dlx:overview(S5)),
    ok.

last_consumer_wins(_Config) ->
    S0 = rabbit_fifo_dlx:init(),
    Handler = at_least_once,
    Msgs = [make_msg(1), make_msg(2), make_msg(3), make_msg(4)],
    {S1, [{mod_call, rabbit_global_counters, messages_dead_lettered,
           [because, rabbit_quorum_queue, at_least_once, 4]}]} =
        rabbit_fifo_dlx:discard(Msgs, because, Handler, S0),
    Checkout = rabbit_fifo_dlx:make_checkout(self(), 10),
    {S2, []} = rabbit_fifo_dlx:apply(meta(5), Checkout, Handler, S1),
    {S3, DeliveryEffects0} = rabbit_fifo_dlx:checkout(Handler, S2),
    ?assertMatch([{log, [1, 2, 3, 4], _}], DeliveryEffects0),
    ?assertEqual(#{num_discarded => 0,
                   num_discard_checked_out => 4,
                   discard_message_bytes => 0,
                   discard_checkout_message_bytes => 4}, rabbit_fifo_dlx:overview(S3)),

    %% When another (or the same) consumer (re)subscribes,
    %% we expect this new consumer to be checked out and delivered all messages
    %% from the previous consumer.
    {S4, []} = rabbit_fifo_dlx:apply(meta(6), Checkout, Handler, S3),
    {S5, DeliveryEffects1} = rabbit_fifo_dlx:checkout(Handler, S4),
    ?assertMatch([{log, [1, 2, 3, 4], _}], DeliveryEffects1),
    ?assertEqual(#{num_discarded => 0,
                   num_discard_checked_out => 4,
                   discard_message_bytes => 0,
                   discard_checkout_message_bytes => 4}, rabbit_fifo_dlx:overview(S5)),
    ok.

make_msg(RaftIdx) ->
    ?MSG(RaftIdx, _Bytes = 1).

meta(Idx) ->
    #{index => Idx,
      term => 1,
      system_time => 0,
      from => {make_ref(), self()}}.
