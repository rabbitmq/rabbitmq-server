-module(rabbit_fifo_dlx_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

% -include_lib("common_test/include/ct.hrl").
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
                  purge,
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
    S0 = rabbit_fifo:init(init_config(undefined)),
    ?assertEqual({S0, [], true}, rabbit_fifo_dlx:discard([make_msg(1)], because, S0)),
    ok.

handler_at_most_once(_Config) ->
    S0 = rabbit_fifo:init(init_config({at_most_once, {m, f, [a]}})),
    {S0, Effects, true} = rabbit_fifo_dlx:discard([make_msg(1),
                                                   make_msg(2)], because, S0),
    ?assertMatch([{log, [1, 2], _}], Effects),
    ok.

discard_dlx_consumer(_Config) ->
    S0 = rabbit_fifo:init(init_config(at_least_once)),
    ?assertEqual(#{num_discarded => 0,
                   num_discard_checked_out => 0,
                   discard_message_bytes => 0,
                   discard_checkout_message_bytes => 0}, rabbit_fifo_dlx:overview(S0)),

    %% message without dlx consumer
    {S1, [], false} = rabbit_fifo_dlx:discard([make_msg(1)], because, S0),
    {S2, []} = rabbit_fifo_dlx:checkout(S1),
    ?assertEqual(#{num_discarded => 1,
                   num_discard_checked_out => 0,
                   discard_message_bytes => 1,
                   discard_checkout_message_bytes => 0}, rabbit_fifo_dlx:overview(S2)),

    %% with dlx consumer
    Checkout = rabbit_fifo_dlx:make_checkout(self(), 2),
    {S3, ok, DeliveryEffects0} = rabbit_fifo_dlx:apply(meta(2), Checkout, S2),
    ?assertEqual(#{num_discarded => 0,
                   num_discard_checked_out => 1,
                   discard_message_bytes => 0,
                   discard_checkout_message_bytes => 1}, rabbit_fifo_dlx:overview(S3)),
    ?assertMatch([{log, [1], _}], DeliveryEffects0),

    %% more messages than dlx consumer's prefetch
    {S4, [], false} = rabbit_fifo_dlx:discard([make_msg(3), make_msg(4)], because, S3),
    {S5, DeliveryEffects1} = rabbit_fifo_dlx:checkout(S4),
    ?assertEqual(#{num_discarded => 1,
                   num_discard_checked_out => 2,
                   discard_message_bytes => 1,
                   discard_checkout_message_bytes => 2}, rabbit_fifo_dlx:overview(S5)),
    ?assertMatch([{log, [3], _}], DeliveryEffects1),
    ?assertEqual({3, 3}, rabbit_fifo_dlx:stat(S5)),

    %% dlx consumer acks messages
    Settle = rabbit_fifo_dlx:make_settle([0,1]),
    {S6, ok, DeliveryEffects2} = rabbit_fifo_dlx:apply(meta(5), Settle, S5),
    ?assertEqual(#{num_discarded => 0,
                   num_discard_checked_out => 1,
                   discard_message_bytes => 0,
                   discard_checkout_message_bytes => 1}, rabbit_fifo_dlx:overview(S6)),
    ?assertMatch([{log, [4], _}], DeliveryEffects2),
    ?assertEqual({1, 1}, rabbit_fifo_dlx:stat(S6)),
    ok.

purge(_Config) ->
    S0 = rabbit_fifo:init(init_config(at_least_once)),
    Checkout = rabbit_fifo_dlx:make_checkout(self(), 1),
    {S1, ok, _} = rabbit_fifo_dlx:apply(meta(1), Checkout, S0),
    Msgs = [make_msg(2), make_msg(3)],
    {S2, _, _} = rabbit_fifo_dlx:discard(Msgs, because, S1),
    {S3, _} = rabbit_fifo_dlx:checkout(S2),
    ?assertMatch(#{num_discarded := 1,
                   num_discard_checked_out := 1}, rabbit_fifo_dlx:overview(S3)),

    {S4, PurgedMsgs} = rabbit_fifo_dlx:purge(S3),
    ?assertEqual(Msgs, PurgedMsgs),
    ?assertEqual(#{num_discarded => 0,
                   num_discard_checked_out => 0,
                   discard_message_bytes => 0,
                   discard_checkout_message_bytes => 0}, rabbit_fifo_dlx:overview(S4)),
    ok.

switch_strategies(_Config) ->
    {ok, _} = rabbit_fifo_dlx_sup:start_link(),
    S0 = rabbit_fifo:init(init_config(undefined)),

    %% Switching from undefined to at_least_once should start dlx consumer.
    {S1, Effects} = rabbit_fifo_dlx:update_config(
                      #{dead_letter_handler => at_least_once}, S0),
    ?assertEqual([{aux, {dlx, setup}}], Effects),
    rabbit_fifo_dlx:handle_aux(leader, setup, fake_aux, S1),
    [{_, WorkerPid, worker, _}] = supervisor:which_children(rabbit_fifo_dlx_sup),
    {S2, _, _} = rabbit_fifo_dlx:discard([make_msg(1)], because, S1),
    Checkout = rabbit_fifo_dlx:make_checkout(WorkerPid, 1),
    {S3, ok, _} = rabbit_fifo_dlx:apply(meta(2), Checkout, S2),
    ?assertMatch(#{num_discard_checked_out := 1}, rabbit_fifo_dlx:overview(S3)),

    %% Switching from at_least_once to undefined should terminate dlx consumer.
    {S4, []} = rabbit_fifo_dlx:update_config(
                 #{dead_letter_handler => undefined}, S3),
    ?assertMatch([_, {active, 0}, _, _],
                 supervisor:count_children(rabbit_fifo_dlx_sup)),
    ?assertMatch(#{num_discarded := 0}, rabbit_fifo_dlx:overview(S4)),
    ok.

last_consumer_wins(_Config) ->
    S0 = rabbit_fifo:init(init_config(at_least_once)),
    Msgs = [make_msg(1), make_msg(2), make_msg(3), make_msg(4)],
    {S1, [], false} = rabbit_fifo_dlx:discard(Msgs, because, S0),
    Checkout = rabbit_fifo_dlx:make_checkout(self(), 5),
    {S2, ok, DeliveryEffects0} = rabbit_fifo_dlx:apply(meta(5), Checkout, S1),
    ?assertMatch([{log, [1, 2, 3, 4], _}], DeliveryEffects0),
    ?assertEqual(#{num_discarded => 0,
                   num_discard_checked_out => 4,
                   discard_message_bytes => 0,
                   discard_checkout_message_bytes => 4}, rabbit_fifo_dlx:overview(S2)),

    %% When another (or the same) consumer (re)subscribes,
    %% we expect this new consumer to be checked out and delivered all messages
    %% from the previous consumer.
    {S3, ok, DeliveryEffects1} = rabbit_fifo_dlx:apply(meta(6), Checkout, S2),
    ?assertMatch([{log, [1, 2, 3, 4], _}], DeliveryEffects1),
    ?assertEqual(#{num_discarded => 0,
                   num_discard_checked_out => 4,
                   discard_message_bytes => 0,
                   discard_checkout_message_bytes => 4}, rabbit_fifo_dlx:overview(S3)),
    ok.

make_msg(RaftIdx) ->
    ?INDEX_MSG(RaftIdx, ?DISK_MSG(1)).

meta(Idx) ->
    #{index => Idx,
      term => 1,
      system_time => 0,
      from => {make_ref(), self()}}.

init_config(Handler) ->
    #{name => ?MODULE,
      queue_resource => #resource{virtual_host = <<"/">>,
                                  kind = queue,
                                  name = <<"blah">>},
      release_cursor_interval => 1,
      dead_letter_handler => Handler}.
