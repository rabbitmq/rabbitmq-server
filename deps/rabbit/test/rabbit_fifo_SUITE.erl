-module(rabbit_fifo_SUITE).

%% rabbit_fifo unit tests suite

-compile(nowarn_export_all).
-compile(export_all).

-compile({no_auto_import, [apply/3]}).
-export([
         ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include_lib("rabbit/src/rabbit_fifo.hrl").
-include_lib("rabbit/src/rabbit_fifo_dlx.hrl").

% -define(PROTOMOD, rabbit_framing_amqp_0_9_1).
%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, tests},
     {group, machine_version_conversion}
    ].


%% replicate eunit like test resultion
all_tests() ->
    [F || {F, 1} <- ?MODULE:module_info(functions),
          re:run(atom_to_list(F), "_test$") /= nomatch].

groups() ->
    [
     {tests, [shuffle], all_tests()},
     {machine_version_conversion, [shuffle],
      [convert_v2_to_v3,
       convert_v3_to_v4]}
    ].

init_per_group(tests, Config) ->
    [{machine_version, rabbit_fifo:version()} | Config];
init_per_group(machine_version_conversion, Config) ->
    Config.

init_per_testcase(_Testcase, Config) ->
    FF = ?config(machine_version, Config) == rabbit_fifo:version(),
    ok = meck:new(rabbit_feature_flags, [passthrough]),
    meck:expect(rabbit_feature_flags, is_enabled, fun (_) -> FF end),
    Config.

end_per_group(_, Config) ->
    Config.

end_per_testcase(_Group, _Config) ->
    meck:unload(),
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

-define(ASSERT_EFF(EfxPat, Effects),
        ?ASSERT_EFF(EfxPat, true, Effects)).

-define(ASSERT_EFF(EfxPat, Guard, Effects),
        ?assert(lists:any(fun (EfxPat) when Guard -> true;
                              (_) -> false
                          end, Effects),
                lists:flatten(io_lib:format("Expected to find effect matching "
                                            "pattern '~s' in effect list '~0p'",
                                            [??EfxPat, Effects])))).

-define(ASSERT_NO_EFF(EfxPat, Effects),
        ?assert(not lists:any(fun (EfxPat) -> true;
                                  (_) -> false
                              end, Effects))).

-define(ASSERT_NO_EFF(EfxPat, Guard, Effects),
        ?assert(not lists:any(fun (EfxPat) when Guard -> true;
                                  (_) -> false
                              end, Effects))).

-define(assertNoEffect(EfxPat, Effects),
        ?assert(not lists:any(fun (EfxPat) -> true;
                                  (_) -> false
                              end, Effects))).

-define(ASSERT(Guard, Fun),
        {assert, fun (S) -> ?assertMatch(Guard, S), _ = Fun(S) end}).
-define(ASSERT(Guard),
        ?ASSERT(Guard, fun (_) -> true end)).

test_init(Name) ->
    init(#{name => Name,
           queue_resource => rabbit_misc:r("/", queue, atom_to_binary(Name)),
           release_cursor_interval => 0}).

-define(FUNCTION_NAME_B, atom_to_binary(?FUNCTION_NAME)).
-define(LINE_B, integer_to_binary(?LINE)).

enq_enq_checkout_compat_test(C) ->
    enq_enq_checkout_test(C, {auto, 2, simple_prefetch}).

enq_enq_checkout_v4_test(C) ->
    enq_enq_checkout_test(C, {auto, {simple_prefetch, 2}}).

enq_enq_checkout_test(Config, Spec) ->
    Cid = {?FUNCTION_NAME_B, self()},
    {State1, _} = enq(Config, 1, 1, first, test_init(?FUNCTION_NAME)),
    {State2, _} = enq(Config, 2, 2, second, State1),
    ?assertEqual(2, rabbit_fifo:query_messages_total(State2)),
    {State3, #{key := CKey,
               next_msg_id := NextMsgId}, Effects} =
        checkout(Config, ?LINE, Cid, Spec, State2),
    ?ASSERT_EFF({monitor, _, _}, Effects),
    ?ASSERT_EFF({log_ext, [1, 2], _Fun, _Local}, Effects),

    {State4, _} = settle(Config, CKey, ?LINE,
                         [NextMsgId, NextMsgId+1], State3),
    ?assertMatch(#{num_messages := 0,
                   num_ready_messages := 0,
                   num_checked_out := 0,
                   num_consumers := 1},
                 rabbit_fifo:overview(State4)),
    ok.

credit_enq_enq_checkout_settled_credit_v1_test(Config) ->
    Cid = {?FUNCTION_NAME, self()},
    {State1, _} = enq(Config, 1, 1, first, test_init(test)),
    {State2, _} = enq(Config, 2, 2, second, State1),
    {State3, #{key := CKey,
               next_msg_id := NextMsgId}, Effects3} =
        checkout(Config, ?LINE, Cid, {auto, 0, credited}, State2),
    ?ASSERT_EFF({monitor, _, _}, Effects3),
    {State4, Effects4} = credit(Config, CKey, ?LINE, 1, 0, false, State3),
    ?ASSERT_EFF({log_ext, [1], _Fun, _Local}, Effects4),
    %% settle the delivery this should _not_ result in further messages being
    %% delivered
    {State5, SettledEffects} = settle(Config, CKey, ?LINE, NextMsgId, State4),
    ?assertEqual(false, lists:any(fun ({log_ext, _, _, _}) ->
                                          true;
                                      (_) ->
                                          false
                                  end, SettledEffects)),
    %% granting credit (3) should deliver the second msg if the receivers
    %% delivery count is (1)
    {State6, CreditEffects} = credit(Config, CKey, ?LINE, 1, 1, false, State5),
    ?ASSERT_EFF({log_ext, [2], _, _}, CreditEffects),
    {_State, FinalEffects} = enq(Config, 6, 3, third, State6),
    ?assertEqual(false, lists:any(fun ({log_ext, _, _, _}) ->
                                          true;
                                      (_) -> false
                                  end, FinalEffects)),
    ok.

credit_enq_enq_checkout_settled_credit_v2_test(Config) ->
    InitDelCnt = 16#ff_ff_ff_ff,
    Ctag = ?FUNCTION_NAME,
    Cid = {Ctag, self()},
    {State1, _} = enq(Config, 1, 1, first, test_init(test)),
    {State2, _} = enq(Config, 2, 2, second, State1),
    {State3, #{key := CKey,
               next_msg_id := NextMsgId}, Effects3} =
        checkout(Config, ?LINE, Cid, {auto, {credited, InitDelCnt}}, State2),
    ?ASSERT_EFF({monitor, _, _}, Effects3),
    {State4, Effects4} = credit(Config, CKey, ?LINE, 1, InitDelCnt, false, State3),
    ?ASSERT_EFF({log_ext, [1], _Plan, _Local}, Effects4),
    %% Settling the delivery should not grant new credit.
    {State5, SettledEffects} = settle(Config, CKey, 4, NextMsgId, State4),
    ?assertEqual(false, lists:any(fun ({log_ext, _, _, _}) ->
                                          true;
                                      (_) ->
                                          false
                                  end, SettledEffects)),
    {State6, CreditEffects} = credit(Config, CKey, ?LINE, 1, 0, false, State5),
    ?ASSERT_EFF({log_ext, [2], _, _}, CreditEffects),
    %% The credit_reply should be sent **after** the delivery.
    ?assertEqual({send_msg, self(),
                  {credit_reply, Ctag, _DeliveryCount = 1, _Credit = 0, _Available = 0, _Drain = false},
                  ?DELIVERY_SEND_MSG_OPTS},
                 lists:last(CreditEffects)),
    {_State, FinalEffects} = enq(Config, 6, 3, third, State6),
    ?assertEqual(false, lists:any(fun ({log_ext, _, _, _}) ->
                                          true;
                                      (_) -> false
                                  end, FinalEffects)).

credit_with_drained_v1_test(Config) ->
    Ctag = ?FUNCTION_NAME_B,
    Cid = {Ctag, self()},
    State0 = test_init(test),
    %% checkout with a single credit
    {State1, #{key := CKey}, _} = checkout(Config, ?LINE, Cid, {auto, 0, credited}, State0),
    ?assertMatch(#rabbit_fifo{consumers = #{CKey := #consumer{credit = 0,
                                                             delivery_count = 0}}},
                 State1),
    {State2, _Effects2} = credit(Config, CKey, ?LINE, 1, 0, false, State1),
    ?assertMatch(#rabbit_fifo{consumers = #{CKey := #consumer{credit = 1,
                                                             delivery_count = 0}}},
                 State2),
    {State, Result, _} =
        apply(meta(Config, ?LINE), rabbit_fifo:make_credit(Cid, 5, 0, true), State2),
    ?assertMatch(#rabbit_fifo{consumers = #{CKey := #consumer{credit = 0,
                                                             delivery_count = 5}}},
                 State),
    ?assertEqual({multi, [{send_credit_reply, 0},
                          {send_drained, {Ctag, 5}}]},
                 Result),
    ok.

credit_with_drained_v2_test(Config) ->
    Ctag = ?FUNCTION_NAME,
    Cid = {Ctag, self()},
    State0 = test_init(test),
    %% checkout with a single credit
    {State1, #{key := CKey}, _} = checkout(Config, ?LINE, Cid, {auto, {credited, 0}}, State0),
    ?assertMatch(#rabbit_fifo{consumers = #{CKey := #consumer{credit = 0,
                                                             delivery_count = 0}}},
                 State1),
    {State2, _Effects2} = credit(Config, CKey, ?LINE, 1, 0, false, State1),
    ?assertMatch(#rabbit_fifo{consumers = #{CKey := #consumer{credit = 1,
                                                             delivery_count = 0}}},
                 State2),
    {State, _, Effects} =
        apply(meta(Config, ?LINE), rabbit_fifo:make_credit(CKey, 5, 0, true), State2),
    ?assertMatch(#rabbit_fifo{consumers = #{CKey := #consumer{credit = 0,
                                                             delivery_count = 5}}},
                 State),
    ?assertEqual([{send_msg, self(),
                   {credit_reply, Ctag, _DeliveryCount = 5,
                    _Credit = 0, _Available = 0, _Drain = true},
                   ?DELIVERY_SEND_MSG_OPTS}],
                 Effects).

credit_and_drain_v1_test(Config) ->
    Ctag = ?FUNCTION_NAME,
    Cid = {Ctag, self()},
    {State1, _} = enq(Config, 1, 1, first, test_init(test)),
    {State2, _} = enq(Config, 2, 2, second, State1),
    %% checkout without any initial credit (like AMQP 1.0 would)
    {State3, _, CheckEffs} =
        apply(meta(Config, 3), make_checkout(Cid, {auto, 0, credited}, #{}),
              State2),

    ?ASSERT_NO_EFF({log_ext, _, _, _}, CheckEffs),
    {State4, {multi, [{send_credit_reply, 0},
                      {send_drained, {Ctag, 2}}]},
    Effects} = apply(meta(Config, 4), rabbit_fifo:make_credit(Cid, 4, 0, true), State3),
    ?assertMatch(#rabbit_fifo{consumers = #{Cid := #consumer{credit = 0,
                                                             delivery_count = 4}}},
                 State4),

    ?ASSERT_EFF({log_ext, [1, 2], _, _}, Effects),
    {_State5, EnqEffs} = enq(Config, 5, 2, third, State4),
    ?ASSERT_NO_EFF({log_ext, _, _, _}, EnqEffs),
    ok.

credit_and_drain_v2_test(Config) ->
    Ctag = ?FUNCTION_NAME_B,
    Cid = {Ctag, self()},
    {State1, _} = enq(Config, 1, 1, first, test_init(test)),
    {State2, _} = enq(Config, 2, 2, second, State1),
    {State3, #{key := CKey}, CheckEffs} = checkout(Config, ?LINE, Cid,
                                                   {auto, {credited, 16#ff_ff_ff_ff - 1}},
                                                   State2),
    ?ASSERT_NO_EFF({log_ext, _, _, _}, CheckEffs),

    {State4, Effects} = credit(Config, CKey, ?LINE, 4, 16#ff_ff_ff_ff - 1,
                               true, State3),
    ?assertMatch(#rabbit_fifo{consumers = #{CKey := #consumer{credit = 0,
                                                              delivery_count = 2}}},
                 State4),
    ?ASSERT_EFF({log_ext, [1, 2], _, _}, Effects),
    %% The credit_reply should be sent **after** the deliveries.
    ?assertEqual({send_msg, self(),
                  {credit_reply, Ctag, _DeliveryCount = 2, _Credit = 0,
                   _Available = 0, _Drain = true},
                  ?DELIVERY_SEND_MSG_OPTS},
                 lists:last(Effects)),

    {_State5, EnqEffs} = enq(Config, 5, 2, third, State4),
    ?ASSERT_NO_EFF({log_ext, _, _, _}, EnqEffs),
    ok.

credit_and_drain_single_active_consumer_v2_test(Config) ->
    State0 = init(#{name => ?FUNCTION_NAME,
                    queue_resource => rabbit_misc:r(
                                        "/", queue, atom_to_binary(?FUNCTION_NAME)),
                    release_cursor_interval => 0,
                    single_active_consumer_on => true}),
    Self = self(),

    % Send 1 message.
    {State1, _} = enq(Config, 1, 1, first, State0),

    % Add 2 consumers.
    Ctag1 = <<"ctag1">>,
    Ctag2 = <<"ctag2">>,
    C1 = {Ctag1, Self},
    C2 = {Ctag2, Self},
    CK1 = ?LINE,
    CK2 = ?LINE,
    Entries = [
               {CK1, make_checkout(C1, {auto, {credited, 16#ff_ff_ff_ff}}, #{})},
               {CK2, make_checkout(C2, {auto, {credited, 16#ff_ff_ff_ff}}, #{})}
              ],
    {State2, _} = run_log(Config, State1, Entries),

    % The 1st registered consumer is the active one, the 2nd consumer is waiting.
    ?assertMatch(#{single_active_consumer_id := C1,
                   single_active_num_waiting_consumers := 1},
                 rabbit_fifo:overview(State2)),

    % Drain the inactive consumer.
    {State3, Effects0} = credit(Config, CK2, ?LINE, 5000, 16#ff_ff_ff_ff, true, State2),
    % The inactive consumer should not receive any message.
    % Hence, no log effect should be returned.
    % Since we sent drain=true, we expect the sending queue to consume all link credit
    % advancing the delivery-count.
    ?assertEqual({send_msg, Self,
                  {credit_reply, Ctag2, _DeliveryCount = 4999, _Credit = 0,
                   _Available = 0, _Drain = true},
                  ?DELIVERY_SEND_MSG_OPTS},
                 Effects0),

    % Drain the active consumer.
    {_State4, Effects1} = credit(Config, CK1, ?LINE, 1000, 16#ff_ff_ff_ff, true, State3),
    ?assertMatch([
                  {log_ext, [1], _Fun, _Local},
                  {send_msg, Self,
                   {credit_reply, Ctag1, _DeliveryCount = 999, _Credit = 0,
                    _Available = 0, _Drain = true},
                   ?DELIVERY_SEND_MSG_OPTS}
                 ],
                 Effects1).

enq_enq_deq_test(C) ->
    Cid = {?FUNCTION_NAME_B, self()},
    {State1, _} = enq(C, 1, 1, first, test_init(test)),
    {State2, _} = enq(C, 2, 2, second, State1),
    % get returns a reply value
    {_State3, _,
     [{log, [1], _Fun},
      {monitor, _, _}]} =
        apply(meta(C, 3), make_checkout(Cid, {dequeue, unsettled}, #{}),
              State2),
    ok.

enq_enq_deq_deq_settle_test(Config) ->
    Cid = {?FUNCTION_NAME_B, self()},
    {State1, _} = enq(Config, 1, 1, first, test_init(test)),
    {State2, _} = enq(Config, 2, 2, second, State1),
    % get returns a reply value
    {State3, '$ra_no_reply',
     [{log, [1], _},
      {monitor, _, _}]} =
        apply(meta(Config, 3), make_checkout(Cid, {dequeue, unsettled}, #{}),
              State2),
    {State4, {dequeue, empty}} =
        apply(meta(Config, 4), make_checkout(Cid, {dequeue, unsettled}, #{}),
              State3),

    {State, _} = settle(Config, Cid, ?LINE, 0, State4),

    ?assertMatch(#{num_consumers := 0}, rabbit_fifo:overview(State)),
    ok.

enq_enq_checkout_get_settled_test(Config) ->
    Cid = {?FUNCTION_NAME, self()},
    {State1, _} = enq(Config, 1, 1, first, test_init(test)),
    % get returns a reply value
    {State2, _, Effs} =
        apply(meta(Config, 3), make_checkout(Cid, {dequeue, settled}, #{}),
              State1),
    ?ASSERT_EFF({log, [1], _}, Effs),
    ?assertEqual(0, rabbit_fifo:query_messages_total(State2)),
    ok.

checkout_get_empty_test(Config) ->
    Cid = {?FUNCTION_NAME, self()},
    State0 = test_init(test),
    {State, {dequeue, empty}, _} = checkout(Config, ?LINE, Cid,
                                            {dequeue, unsettled}, State0),
    ?assertMatch(#{num_consumers := 0}, rabbit_fifo:overview(State)),
    ok.

untracked_enq_deq_test(Config) ->
    Cid = {?FUNCTION_NAME, self()},
    State0 = test_init(test),
    {State1, _, _} = apply(meta(Config, 1),
                           rabbit_fifo:make_enqueue(undefined, undefined, first),
                           State0),
    {_State2, _, Effs} =
        apply(meta(Config, 3), make_checkout(Cid, {dequeue, settled}, #{}), State1),
    ?ASSERT_EFF({log, [1], _}, Effs),
    ok.

enq_expire_deq_test(C) ->
    Conf = #{name => ?FUNCTION_NAME,
             queue_resource => rabbit_misc:r(<<"/">>, queue, <<"test">>),
             msg_ttl => 0},
    S0 = rabbit_fifo:init(Conf),
    Msg = #basic_message{content = #content{properties = #'P_basic'{},
                                            payload_fragments_rev = []}},
    {S1, ok, _} = apply(meta(C, 1, 100, {notify, 1, self()}),
                        rabbit_fifo:make_enqueue(self(), 1, Msg), S0),
    Cid = {?FUNCTION_NAME, self()},
    {_S2, {dequeue, empty}, Effs} =
        apply(meta(C, 2, 101), make_checkout(Cid, {dequeue, unsettled}, #{}), S1),
    ?ASSERT_EFF({mod_call, rabbit_global_counters, messages_dead_lettered,
                 [expired, rabbit_quorum_queue, disabled, 1]}, Effs),
    ok.

enq_expire_enq_deq_test(Config) ->
    S0 = test_init(test),
    %% Msg1 and Msg2 get enqueued in the same millisecond,
    %% but only Msg1 expires immediately.
    Msg1 = mc_amqpl:from_basic_message(
             #basic_message{routing_keys = [<<"">>],
                            exchange_name = #resource{name = <<"x">>,
                                                      kind = exchange,
                                                      virtual_host = <<"v">>},
                            content = #content{properties = #'P_basic'{
                                                               expiration = <<"0">>},
                                               payload_fragments_rev = [<<"msg1">>]}}),
    Enq1 = rabbit_fifo:make_enqueue(self(), 1, Msg1),
    Idx1 = ?LINE,
    {S1, ok, _} = apply(meta(Config, Idx1, 100, {notify, 1, self()}), Enq1, S0),
    Msg2 = #basic_message{content = #content{properties = #'P_basic'{},
                                             % class_id = 60,
                                             % protocol = ?PROTOMOD,
                                             payload_fragments_rev = [<<"msg2">>]}},
    Enq2 = rabbit_fifo:make_enqueue(self(), 2, Msg2),
    Idx2 = ?LINE,
    {S2, ok, _} = apply(meta(Config, Idx2, 100, {notify, 2, self()}), Enq2, S1),
    Cid = {?FUNCTION_NAME, self()},
    {_S3, _, Effs} =
        apply(meta(Config, ?LINE, 101), make_checkout(Cid, {dequeue, unsettled}, #{}), S2),
    {log, [Idx2], Fun} = get_log_eff(Effs),
    [{reply, _From,
      {wrap_reply, {dequeue, {_MsgId, _HeaderMsg}, ReadyMsgCount}}}] = Fun([Enq2]),
    ?assertEqual(0, ReadyMsgCount).

enq_expire_deq_enq_enq_deq_deq_test(Config) ->
    S0 = test_init(test),
    Msg1 = #basic_message{content =
                          #content{properties = #'P_basic'{expiration = <<"0">>},
                                   payload_fragments_rev = [<<"msg1">>]}},
    {S1, ok, _} = apply(meta(Config, 1, 100, {notify, 1, self()}),
                        rabbit_fifo:make_enqueue(self(), 1, Msg1), S0),
    {S2, {dequeue, empty}, _} = apply(meta(Config, 2, 101),
                                      make_checkout({c1, self()},
                                                    {dequeue, unsettled}, #{}), S1),
    {S3, _} = enq(Config, 3, 2, msg2, S2),
    {S4, _} = enq(Config, 4, 3, msg3, S3),
    {S5, '$ra_no_reply',
     [{log, [3], _},
      {monitor, _, _}]} =
        apply(meta(Config, 5), make_checkout({c2, self()}, {dequeue, unsettled}, #{}), S4),
    {_S6, '$ra_no_reply',
     [{log, [4], _},
      {monitor, _, _}]} =
        apply(meta(Config, 6), make_checkout({c3, self()}, {dequeue, unsettled}, #{}), S5),
    ok.

checkout_enq_settle_test(Config) ->
    Cid = {?FUNCTION_NAME_B, self()},
    {State1, #{key := CKey,
               next_msg_id := NextMsgId},
     [{monitor, _, _} | _]} = checkout(Config, ?LINE, Cid, 1, test_init(test)),
    {State2, Effects0} = enq(Config, 2, 1,  first, State1),
    ?ASSERT_EFF({send_msg, _, {delivery, _, [{0, {_, first}}]}, _}, Effects0),
    {State3, _} = enq(Config, 3, 2, second, State2),
    {_, _Effects} = settle(Config, CKey, 4, NextMsgId, State3),
    ok.

duplicate_enqueue_test(Config) ->
    Cid = {?FUNCTION_NAME_B, self()},
    MsgSeq = 1,
    {State1, [ {monitor, _, _} | _]} = check_n(Config, Cid, 5, 5, test_init(test)),
    {State2, Effects2} = enq(Config, 2, MsgSeq, first, State1),
    ?ASSERT_EFF({send_msg, _, {delivery, _, [{_, {_, first}}]}, _}, Effects2),
    {_State3, Effects3} = enq(Config, 3, MsgSeq, first, State2),
    ?ASSERT_NO_EFF({log_ext, [_], _, _}, Effects3),
    ok.

return_test(Config) ->
    Cid = {<<"cid">>, self()},
    Cid2 = {<<"cid2">>, self()},
    {State0, _} = enq(Config, 1, 1, msg, test_init(test)),
    {State1, #{key := C1Key,
               next_msg_id := MsgId}, _} = checkout(Config, ?LINE, Cid, 1, State0),
    {State2, #{key := C2Key}, _} = checkout(Config, ?LINE, Cid2, 1, State1),
    {State3, _, _} = apply(meta(Config, 4),
                           rabbit_fifo:make_return(C1Key, [MsgId]), State2),
    ?assertMatch(#{C1Key := #consumer{checked_out = C1}}
                   when map_size(C1) == 0, State3#rabbit_fifo.consumers),
    ?assertMatch(#{C2Key := #consumer{checked_out = C2}}
                   when map_size(C2) == 1, State3#rabbit_fifo.consumers),
    ok.

return_multiple_test(Config) ->
    Cid = {<<"cid">>, self()},
    {State0, _} = enq(Config, 1, 1, first, test_init(?FUNCTION_NAME)),
    {State1, _} = enq(Config, 2, 2, second, State0),
    {State2, _} = enq(Config, 3, 3, third, State1),

    {State3,
     #{key := CKey,
       next_msg_id := NextMsgId},
     Effects0} = checkout(Config, ?LINE, Cid, 3, State2),
    ?ASSERT_EFF({log_ext, [1, 2, 3], _Fun, _Local}, Effects0),

    {_, _, Effects1} = apply(meta(Config, ?LINE),
                             rabbit_fifo:make_return(
                               CKey,
                               %% Return messages in following order: 3, 1, 2
                               [NextMsgId + 2, NextMsgId, NextMsgId + 1]),
                             State3),
    %% We expect messages to be re-delivered in the same order in which we previously returned.
    ?ASSERT_EFF({log_ext, [3, 1, 2], _Fun, _Local}, Effects1),
    ok.

return_dequeue_delivery_limit_test(C) ->
    Init = init(#{name => test,
                  queue_resource => rabbit_misc:r("/", queue,
                                                  atom_to_binary(test, utf8)),
                  max_in_memory_length => 0,
                  release_cursor_interval => 0,
                  delivery_limit => 1}),
    {State0, _} = enq(C, 1, 1, msg, Init),

    Cid = {<<"cid">>, self()},
    Cid2 = {<<"cid2">>, self()},

    Msg = rabbit_fifo:make_enqueue(self(), 1, msg),
    {State1, {MsgId1, _}} = deq(C, 2, Cid, unsettled, Msg, State0),
    {State2, _, _} = apply(meta(C, 4), rabbit_fifo:make_return(Cid, [MsgId1]),
                           State1),

    {State3, {MsgId2, _}} = deq(C, 2, Cid2, unsettled, Msg, State2),
    {State4, _, _} = apply(meta(C, 4), rabbit_fifo:make_return(Cid2, [MsgId2]),
                           State3),
    ?assertMatch(#{num_messages := 0}, rabbit_fifo:overview(State4)),
    ok.

return_non_existent_test(Config) ->
    Cid = {<<"cid">>, self()},
    {State0, _} = enq(Config, 1, 1, second, test_init(test)),
    % return non-existent, check it doesn't crash
    {_State2, _} = apply(meta(Config, 3), rabbit_fifo:make_return(Cid, [99]), State0),
    ok.

return_checked_out_test(Config) ->
    Cid = {<<"cid">>, self()},
    {State0, _} = enq(Config, 1, 1, first, test_init(test)),
    {State1, #{key := CKey,
               next_msg_id := MsgId}, Effects1} =
        checkout(Config, ?LINE, Cid, 1, State0),
    ?ASSERT_EFF({log_ext, [1], _Fun, _Local}, Effects1),
    % returning immediately checks out the same message again
    {_State, ok, Effects2} =
        apply(meta(Config, 3), rabbit_fifo:make_return(CKey, [MsgId]), State1),
    ?ASSERT_EFF({log_ext, [1], _Fun, _Local}, Effects2),
    ok.

return_checked_out_limit_test(Config) ->
    Cid = {<<"cid">>, self()},
    Init = init(#{name => test,
                  queue_resource => rabbit_misc:r("/", queue,
                                                  atom_to_binary(test, utf8)),
                  release_cursor_interval => 0,
                  max_in_memory_length => 0,
                  delivery_limit => 1}),
    Msg1 = rabbit_fifo:make_enqueue(self(), 1, first),
    {State0, _} = enq(Config, 1, 1, Msg1, Init),
    {State1, #{key := CKey,
               next_msg_id := MsgId}, Effects1} =
        checkout(Config, ?LINE, Cid, 1, State0),
    ?ASSERT_EFF({log_ext, [1], _Fun, _Local}, Effects1),
    % returning immediately checks out the same message again
    {State2, ok, Effects2} =
        apply(meta(Config, 3), rabbit_fifo:make_return(CKey, [MsgId]), State1),
    ?ASSERT_EFF({log_ext, [1], _Fun, _Local}, Effects2),

    {#rabbit_fifo{} = State, ok, _} =
        apply(meta(Config, 4), rabbit_fifo:make_return(Cid, [MsgId + 1]), State2),
    ?assertEqual(0, rabbit_fifo:query_messages_total(State)),
    ok.

return_auto_checked_out_test(Config) ->
    Cid = {<<"cid">>, self()},
    {State00, _} = enq(Config, 1, 1, first, test_init(test)),
    {State0, _} = enq(Config, 2, 2, second, State00),
    % it first active then inactive as the consumer took on but cannot take
    % any more
    {State1, #{key := CKey,
               next_msg_id := MsgId},
     [_Monitor, {log_ext, [1], _Fun1, _} ]} = checkout(Config, ?LINE, Cid, 1, State0),
    % return should include another delivery
    {State2, _, Effects} = apply(meta(Config, 3),
                                  rabbit_fifo:make_return(CKey, [MsgId]), State1),
    [{log_ext, [1], _Fun2, _} | _] = Effects,

    MsgId2 = MsgId+1,
    [{_MsgId2, {_, #{acquired_count := 1}}}]
        = rabbit_fifo:get_checked_out(CKey, MsgId2, MsgId2, State2),

    %% a down does not increment the return_count
    {State3, _, _} = apply(meta(Config, ?LINE), {down, self(), noproc}, State2),

    {State4, #{key := CKey2,
                next_msg_id := MsgId3},
     [_, {log_ext, [1], _Fun3, _} ]} = checkout(Config, ?LINE, Cid, 1, State3),

    [{_, {_, #{delivery_count := 1,
               acquired_count := 2}}}]
        = rabbit_fifo:get_checked_out(CKey2, MsgId3, MsgId3, State4),
    ok.

requeue_test(Config) ->
    Cid = {<<"cid">>, test_util:fake_pid(n1@banana)},
    Msg1 = rabbit_fifo:make_enqueue(self(), 1, first),
    {State0, _} = enq(Config, 1, 1, first, test_init(test)),
    % it first active then inactive as the consumer took on but cannot take
    % any more
    {State1, #{key := CKey,
               next_msg_id := MsgId},
     [_Monitor, {log_ext, [1], _Fun, _}]} = checkout(Config, ?LINE, Cid, 1, State0),

    [{MsgId, {H1, _}}] = rabbit_fifo:get_checked_out(CKey, MsgId, MsgId, State1),
    ct:pal("query consumers ~p", [rabbit_fifo:query_consumers(State1)]),

    [{append, Requeue, _}] = rabbit_fifo:make_requeue(CKey, {notify, 1, self()},
                                                      [{MsgId, 1, H1, Msg1}], []),
    {State2, _, Effects} = apply(meta(Config, 3), Requeue, State1),
    [{log_ext, [_], _Fun2, _} | _] = Effects,

    %%
    NextMsgId = MsgId + 1,
    [{_MsgId2, {_RaftIdx, #{acquired_count := 1}}}] =
        rabbit_fifo:get_checked_out(CKey, NextMsgId , NextMsgId, State2),
    ok.

cancelled_checkout_empty_queue_test(Config) ->
    Cid = {?FUNCTION_NAME_B, self()},
    {State1, #{key := _CKey,
               next_msg_id := _NextMsgId}, _} =
        checkout(Config, ?LINE, Cid, 1, test_init(test)),%% prefetch of 1
    % cancelled checkout should clear out service_queue also, else we'd get a
    % build up of these
    {State2, _, _Effects} = apply(meta(Config, 3),
                                  make_checkout(Cid, cancel, #{}), State1),
    ?assertEqual(0, map_size(State2#rabbit_fifo.consumers)),
    ?assertEqual(0, priority_queue:len(State2#rabbit_fifo.service_queue)),
    ok.

cancelled_checkout_out_test(Config) ->
    Cid = {<<"cid">>, self()},
    {State00, _} = enq(Config, 1, 1, first, test_init(test)),
    {State0, _} = enq(Config, 2, 2, second, State00),
    {State1, #{key := CKey,
               next_msg_id := NextMsgId}, _} =
        checkout(Config, ?LINE, Cid, 1, State0),%% prefetch of 1
    % cancelled checkout should not return pending messages to queue
    {State2, _, _} = apply(meta(Config, 4),
                           rabbit_fifo:make_checkout(Cid, cancel, #{}), State1),
    ?assertEqual(1, rabbit_fifo_q:len(State2#rabbit_fifo.messages)),
    ?assertEqual(0, lqueue:len(State2#rabbit_fifo.returns)),
    ?assertEqual(0, priority_queue:len(State2#rabbit_fifo.service_queue)),

    {State3, {dequeue, empty}} =
        apply(meta(Config, 5), make_checkout(Cid, {dequeue, settled}, #{}), State2),
    %% settle
    {State4, ok, _} =
        apply(meta(Config, 6), rabbit_fifo:make_settle(CKey, [NextMsgId]), State3),

    {_State, _, [{log, [2], _Fun} | _]} =
        apply(meta(Config, 7), make_checkout(Cid, {dequeue, settled}, #{}), State4),
    ok.

down_with_noproc_consumer_returns_unsettled_test(Config) ->
    Cid = {?FUNCTION_NAME_B, self()},
    {State0, _} = enq(Config, 1, 1, second, test_init(test)),
    {State1, #{key := CKey},
     [{monitor, process, Pid} | _]} = checkout(Config, ?LINE, Cid, 1, State0),
    {State2, _, _} = apply(meta(Config, 3), {down, Pid, noproc}, State1),
    {_State, #{key := CKey2}, Effects} = checkout(Config, ?LINE, Cid, 1, State2),
    ?assertNotEqual(CKey, CKey2),
    ?ASSERT_EFF({monitor, process, _}, Effects),
    ok.

removed_consumer_returns_unsettled_test(Config) ->
    Cid = {?FUNCTION_NAME_B, self()},
    {State0, _} = enq(Config, 1, 1, second, test_init(test)),
    {State1, #{key := CKey},
     [{monitor, process, _Pid} | _]} = checkout(Config, ?LINE, Cid, 1, State0),
    Remove = rabbit_fifo:make_checkout(Cid, remove, #{}),
    {State2, _, _} = apply(meta(Config, 3), Remove, State1),
    {_State, #{key := CKey2}, Effects} = checkout(Config, ?LINE, Cid, 1, State2),
    ?assertNotEqual(CKey, CKey2),
    ?ASSERT_EFF({monitor, process, _}, Effects),
    ok.

down_with_noconnection_marks_suspect_and_node_is_monitored_test(Config) ->
    Pid = spawn(fun() -> ok end),
    Cid = {?FUNCTION_NAME_B, Pid},
    Self = self(),
    Node = node(Pid),
    {State0, Effects0} = enq(Config, 1, 1, second, test_init(test)),
    ?ASSERT_EFF({monitor, process, P}, P =:= Self, Effects0),
    {State1, #{key := CKey}, Effects1} = checkout(Config, ?LINE, Cid, 1, State0),
    #consumer{credit = 0} = maps:get(CKey, State1#rabbit_fifo.consumers),
    ?ASSERT_EFF({monitor, process, P}, P =:= Pid, Effects1),
    % monitor both enqueuer and consumer
    % because we received a noconnection we now need to monitor the node
    {State2a, _, _} = apply(meta(Config, 3), {down, Pid, noconnection}, State1),
    #consumer{credit = 1,
              checked_out = Ch,
              status = suspected_down} = maps:get(CKey, State2a#rabbit_fifo.consumers),
    ?assertEqual(#{}, Ch),
    %% validate consumer has credit
    {State2, _, Effects2} = apply(meta(Config, 3), {down, Self, noconnection}, State2a),
    ?ASSERT_EFF({monitor, node, _}, Effects2),
    ?assertNoEffect({demonitor, process, _}, Effects2),
    % when the node comes up we need to retry the process monitors for the
    % disconnected processes
    {State3, _, Effects3} = apply(meta(Config, 3), {nodeup, Node}, State2),
    #consumer{status = up} = maps:get(CKey, State3#rabbit_fifo.consumers),
    % try to re-monitor the suspect processes
    ?ASSERT_EFF({monitor, process, P}, P =:= Pid, Effects3),
    ?ASSERT_EFF({monitor, process, P}, P =:= Self, Effects3),
    ok.

down_with_noconnection_returns_unack_test(Config) ->
    Pid = spawn(fun() -> ok end),
    Cid = {?FUNCTION_NAME_B, Pid},
    Msg = rabbit_fifo:make_enqueue(self(), 1, second),
    {State0, _} = enq(Config, 1, 1, second, test_init(test)),
    ?assertEqual(1, rabbit_fifo_q:len(State0#rabbit_fifo.messages)),
    ?assertEqual(0, lqueue:len(State0#rabbit_fifo.returns)),
    {State1, {_, _}} = deq(Config, 2, Cid, unsettled, Msg, State0),
    ?assertEqual(0, rabbit_fifo_q:len(State1#rabbit_fifo.messages)),
    ?assertEqual(0, lqueue:len(State1#rabbit_fifo.returns)),
    {State2a, _, _} = apply(meta(Config, 3), {down, Pid, noconnection}, State1),
    ?assertEqual(0, rabbit_fifo_q:len(State2a#rabbit_fifo.messages)),
    ?assertEqual(1, lqueue:len(State2a#rabbit_fifo.returns)),
    ?assertMatch(#consumer{checked_out = Ch,
                           status = suspected_down}
                   when map_size(Ch) == 0,
                        maps:get(Cid, State2a#rabbit_fifo.consumers)),
    ok.

down_with_noproc_enqueuer_is_cleaned_up_test(Config) ->
    State00 = test_init(test),
    Pid = spawn(fun() -> ok end),
    {State0, _, Effects0} = apply(meta(Config, 1, ?LINE, {notify, 1, Pid}),
                                  rabbit_fifo:make_enqueue(Pid, 1, first), State00),
    ?ASSERT_EFF({monitor, process, _}, Effects0),
    {State1, _, _} = apply(meta(Config, 3), {down, Pid, noproc}, State0),
    % ensure there are no enqueuers
    ?assert(0 =:= maps:size(State1#rabbit_fifo.enqueuers)),
    ok.

discarded_message_without_dead_letter_handler_is_removed_test(Config) ->
    Cid = {?FUNCTION_NAME_B, self()},
    {State0, _} = enq(Config, 1, 1, first, test_init(test)),
    {State1, #{key := CKey,
               next_msg_id := MsgId}, Effects1} =
        checkout(Config, ?LINE, Cid, 10, State0),
    ?ASSERT_EFF({log_ext, [1], _Fun, _}, Effects1),
    {_State2, _, Effects2} = apply(meta(Config, 1),
                                   rabbit_fifo:make_discard(CKey, [MsgId]), State1),
    ?ASSERT_NO_EFF({log_ext, [1], _Fun, _}, Effects2),
    ok.

discarded_message_with_dead_letter_handler_emits_log_effect_test(Config) ->
    Cid = {?FUNCTION_NAME_B, self()},
    State00 = init(#{name => test,
                     queue_resource => rabbit_misc:r(<<"/">>, queue, <<"test">>),
                     max_in_memory_length => 0,
                     dead_letter_handler =>
                     {at_most_once, {somemod, somefun, [somearg]}}}),

    Mc = mk_mc(<<"first">>),
    Msg1 = rabbit_fifo:make_enqueue(self(), 1, Mc),
    {State0, _} = enq(Config, 1, 1, Mc, State00),
    {State1, #{key := CKey,
               next_msg_id := MsgId}, Effects1} =
        checkout(Config, ?LINE, Cid, 10, State0),
    ?ASSERT_EFF({log_ext, [1], _, _}, Effects1),
    {_State2, _, Effects2} = apply(meta(Config, 1),
                                   rabbit_fifo:make_discard(CKey, [MsgId]), State1),
    % assert mod call effect with appended reason and message
    % dlx still uses log effects
    {value, {log, [1], Fun}} = lists:search(fun (E) -> element(1, E) == log end,
                                            Effects2),
    [{mod_call, somemod, somefun, [somearg, rejected, [McOut]]}] = Fun([Msg1]),

    ?assertEqual(undefined, mc:get_annotation(acquired_count, McOut)),
    ?assertEqual(1, mc:get_annotation(delivery_count, McOut)),

    ok.

discard_after_cancel_test(Config) ->
    Cid = {?FUNCTION_NAME_B, self()},
    {State0, _} = enq(Config, 1, 1, first, test_init(test)),
    {State1, #{key := _CKey,
               next_msg_id := MsgId}, _Effects1} =
        checkout(Config, ?LINE, Cid, 10, State0),
    {State2, _, _} = apply(meta(Config, ?LINE),
                           rabbit_fifo:make_checkout(Cid, cancel, #{}), State1),
    {State, _, _} = apply(meta(Config, ?LINE),
                          rabbit_fifo:make_discard(Cid, [MsgId]), State2),
    ct:pal("State ~p", [State]),
    ok.

enqueued_msg_with_delivery_count_test(Config) ->
    State00 = init(#{name => test,
                     queue_resource => rabbit_misc:r(<<"/">>, queue, <<"test">>),
                     max_in_memory_length => 0,
                     dead_letter_handler =>
                     {at_most_once, {somemod, somefun, [somearg]}}}),
    Mc = mc:set_annotation(delivery_count, 2, mk_mc(<<"first">>)),
    {#rabbit_fifo{messages = Msgs}, _} = enq(Config, 1, 1, Mc, State00),
    ?assertMatch(?MSG(_, #{delivery_count := 2}), rabbit_fifo_q:get(Msgs)),
    ok.

get_log_eff(Effs) ->
    {value, Log} = lists:search(fun (E) -> element(1, E) == log end, Effs),
    Log.

get_log_ext_eff(Effs) ->
    {value, Log} = lists:search(fun (E) -> element(1, E) == log_ext end, Effs),
    Log.

mixed_send_msg_and_log_effects_are_correctly_ordered_test(Config) ->
    Cid = {cid(?FUNCTION_NAME), self()},
    State00 = init(#{name => test,
                     queue_resource => rabbit_misc:r(<<"/">>, queue, <<"test">>),
                     max_in_memory_length =>1,
                     dead_letter_handler =>
                     {at_most_once,
                      {somemod, somefun, [somearg]}}}),
    %% enqueue two messages
    {State0, _} = enq(Config, 1, 1, first, State00),
    {State1, _} = enq(Config, 2, 2, snd, State0),

    {State2, _, Effects1} = checkout(Config, ?LINE, Cid, 10, State1),
    {log_ext, [1, 2], _Fun, _} = get_log_ext_eff(Effects1),

    [{0,{_, 0}},{1,{_, 0}}] = rabbit_fifo:get_checked_out(Cid, 0, 1, State2),
    %% in this case we expect no send_msg effect as any in memory messages
    %% should be weaved into the send_msg effect emitted by the log effect
    %% later. hence this is all we can assert on
    %% as we need to send message is in the correct order to the consuming
    %% channel or the channel may think a message has been lost in transit
    ?ASSERT_NO_EFF({send_msg, _, _, _}, Effects1),
    ok.

tick_test(Config) ->
    Cid = {<<"c">>, self()},
    Cid2 = {<<"c2">>, self()},

    Msg1 = rabbit_fifo:make_enqueue(self(), 1, <<"fst">>),
    Msg2 = rabbit_fifo:make_enqueue(self(), 2, <<"snd">>),
    {S0, _} = enq(Config, 1, 1, <<"fst">>, test_init(?FUNCTION_NAME)),
    {S1, _} = enq(Config, 2, 2, <<"snd">>, S0),
    {S2, {MsgId, _}} = deq(Config, 3, Cid, unsettled, Msg1, S1),
    {S3, {_, _}} = deq(Config, 4, Cid2, unsettled, Msg2, S2),
    {S4, _, _} = apply(meta(Config, 5), rabbit_fifo:make_return(Cid, [MsgId]), S3),

    [{aux, {handle_tick,
            [#resource{},
             #{config := #{name := ?FUNCTION_NAME},
               num_consumers := 1,
               num_checked_out := 1,
               num_ready_messages := 1,
               num_messages := 2,
               enqueue_message_bytes := 3,
               checkout_message_bytes := 3,
               num_discarded := _Discards},
             [_Node]
            ]}}] = rabbit_fifo:tick(1, S4),
    ok.


delivery_query_returns_deliveries_test(Config) ->
    Tag = atom_to_binary(?FUNCTION_NAME, utf8),
    Cid = {Tag, self()},
    CKey = ?LINE,
    Entries = [
                {CKey, make_checkout(Cid, {auto, {simple_prefetch, 5}}, #{})},
                {?LINE, rabbit_fifo:make_enqueue(self(), 1, one)},
                {?LINE, rabbit_fifo:make_enqueue(self(), 2, two)},
                {?LINE, rabbit_fifo:make_enqueue(self(), 3, tre)},
                {?LINE, rabbit_fifo:make_enqueue(self(), 4, for)}
              ],
    {State, _Effects} = run_log(Config, test_init(help), Entries),
    % 3 deliveries are returned
    [{0, {_, _}}] = rabbit_fifo:get_checked_out(CKey, 0, 0, State),
    [_, _, _] = rabbit_fifo:get_checked_out(Cid, 1, 3, State),
    ok.

duplicate_delivery_test(Config) ->
    {State0, _} = enq(Config, 1, 1, first, test_init(test)),
    {#rabbit_fifo{messages = Messages} = State, _} =
        enq(Config, 2, 1, first, State0),
    ?assertEqual(1, rabbit_fifo:query_messages_total(State)),
    ?assertEqual(1, rabbit_fifo_q:len(Messages)),
    ok.

state_enter_monitors_and_notifications_test(Config) ->
    Oth = spawn(fun () -> ok end),
    {State0, _} = enq(Config, 1, 1, first, test_init(test)),
    Cid = {<<"adf">>, self()},
    OthCid = {<<"oth">>, Oth},
    {State1, _, _} = checkout(Config, ?LINE, Cid, 1, State0),
    {State, _, _} = checkout(Config, ?LINE, OthCid, 1, State1),
    Self = self(),
    Effects = rabbit_fifo:state_enter(leader, State),

    %% monitor all enqueuers and consumers
    [{monitor, process, Self},
     {monitor, process, Oth}] =
        lists:filter(fun ({monitor, process, _}) -> true;
                         (_) -> false
                     end, Effects),
    [{send_msg, Self, leader_change, ra_event},
     {send_msg, Oth, leader_change, ra_event}] =
        lists:filter(fun ({send_msg, _, leader_change, ra_event}) -> true;
                         (_) -> false
                     end, Effects),
    ?ASSERT_EFF({monitor, process, _}, Effects),
    ok.

purge_test(Config) ->
    Cid = {<<"purge_test">>, self()},
    {State1, _} = enq(Config, 1, 1, first, test_init(test)),
    {State2, {purge, 1}, _} = apply(meta(Config, 2), rabbit_fifo:make_purge(), State1),
    {State3, _} = enq(Config, 3, 2, second, State2),
    % get returns a reply value
    {_State4, _, Effs} =
        apply(meta(Config, 4), make_checkout(Cid, {dequeue, unsettled}, #{}), State3),
    ?ASSERT_EFF({log, [3], _}, Effs),
    ok.

purge_with_checkout_test(Config) ->
    Cid = {<<"purge_test">>, self()},
    {State0, #{key := CKey}, _} = checkout(Config, ?LINE, Cid, 1,
                                           test_init(?FUNCTION_NAME)),
    {State1, _} = enq(Config, 2, 1, <<"first">>, State0),
    {State2, _} = enq(Config, 3, 2, <<"second">>, State1),
    %% assert message bytes are non zero
    ?assert(State2#rabbit_fifo.msg_bytes_checkout > 0),
    ?assert(State2#rabbit_fifo.msg_bytes_enqueue > 0),
    {State3, {purge, 1}, _} = apply(meta(Config, 2), rabbit_fifo:make_purge(), State2),
    ?assert(State2#rabbit_fifo.msg_bytes_checkout > 0),
    ?assertEqual(0, State3#rabbit_fifo.msg_bytes_enqueue),
    ?assertEqual(1, rabbit_fifo:query_messages_total(State3)),
    #consumer{checked_out = Checked} = maps:get(CKey, State3#rabbit_fifo.consumers),
    ?assertEqual(1, maps:size(Checked)),
    ok.

down_noproc_returns_checked_out_in_order_test(Config) ->
    S0 = test_init(?FUNCTION_NAME),
    %% enqueue 100
    S1 = lists:foldl(fun (Num, FS0) ->
                         {FS, _} = enq(Config, Num, Num, Num, FS0),
                         FS
                     end, S0, lists:seq(1, 100)),
    ?assertEqual(100, rabbit_fifo_q:len(S1#rabbit_fifo.messages)),
    Cid = {<<"cid">>, self()},
    {S2, #{key := CKey}, _} = checkout(Config, ?LINE, Cid, 1000, S1),
    #consumer{checked_out = Checked} = maps:get(CKey, S2#rabbit_fifo.consumers),
    ?assertEqual(100, maps:size(Checked)),
    %% simulate down
    {S, _, _} = apply(meta(Config, 102), {down, self(), noproc}, S2),
    Returns = lqueue:to_list(S#rabbit_fifo.returns),
    ?assertEqual(100, length(Returns)),
    ?assertEqual(0, maps:size(S#rabbit_fifo.consumers)),
    %% validate returns are in order
    ?assertEqual(lists:sort(Returns), Returns),
    ok.

down_noconnection_returns_checked_out_test(Config) ->
    S0 = test_init(?FUNCTION_NAME),
    NumMsgs = 20,
    S1 = lists:foldl(fun (Num, FS0) ->
                         {FS, _} = enq(Config, Num, Num, Num, FS0),
                         FS
                     end, S0, lists:seq(1, NumMsgs)),
    ?assertEqual(NumMsgs, rabbit_fifo_q:len(S1#rabbit_fifo.messages)),
    Cid = {<<"cid">>, self()},
    {S2, #{key := CKey}, _} = checkout(Config, ?LINE, Cid, 1000, S1),
    #consumer{checked_out = Checked} = maps:get(CKey, S2#rabbit_fifo.consumers),
    ?assertEqual(NumMsgs, maps:size(Checked)),
    %% simulate down
    {S, _, _} = apply(meta(Config, 102), {down, self(), noconnection}, S2),
    Returns = lqueue:to_list(S#rabbit_fifo.returns),
    ?assertEqual(NumMsgs, length(Returns)),
    ?assertMatch(#consumer{checked_out = Ch}
                   when map_size(Ch) == 0,
                        maps:get(CKey, S#rabbit_fifo.consumers)),
    %% validate returns are in order
    ?assertEqual(lists:sort(Returns), Returns),
    ok.

single_active_consumer_basic_get_test(Config) ->
    Cid = {?FUNCTION_NAME, self()},
    State0 = init(#{name => ?FUNCTION_NAME,
                    queue_resource => rabbit_misc:r("/", queue,
                        atom_to_binary(?FUNCTION_NAME, utf8)),
                    release_cursor_interval => 0,
                    single_active_consumer_on => true}),
    ?assertEqual(single_active, State0#rabbit_fifo.cfg#cfg.consumer_strategy),
    ?assertEqual(0, map_size(State0#rabbit_fifo.consumers)),
    {State1, _} = enq(Config, 1, 1, first, State0),
    {_State, {error, {unsupported, single_active_consumer}}} =
        apply(meta(Config, 2), make_checkout(Cid, {dequeue, unsettled}, #{}),
              State1),

    ok.

single_active_consumer_revive_test(Config) ->
    S0 = init(#{name => ?FUNCTION_NAME,
                queue_resource => rabbit_misc:r("/", queue, ?FUNCTION_NAME_B),
                single_active_consumer_on => true}),
    Cid1 = {<<"one">>, self()},
    Cid2 = {<<"two">>, self()},
    {S1, #{key := CKey1}, _} = checkout(Config, ?LINE, Cid1, 1, S0),
    {S2, #{key := _CKey2}, _} = checkout(Config, ?LINE, Cid2, 1, S1),
    {S3, _} = enq(Config, 3, 1, first, S2),
    %% cancel the active consumer whilst it has a message pending
    {S4, _, _} = rabbit_fifo:apply(meta(Config, ?LINE),
                                   make_checkout(Cid1, cancel, #{}), S3),
    %% the revived consumer should have the original key
    {S5, #{key := CKey1}, _} = checkout(Config, ?LINE, Cid1, 1, S4),

    ?assertEqual(1, rabbit_fifo:query_messages_checked_out(S5)),
    ?assertEqual(1, rabbit_fifo:query_messages_total(S5)),
    Consumers = S5#rabbit_fifo.consumers,
    ?assertEqual(2, map_size(Consumers)),
    Up = maps:filter(fun (_, #consumer{status = Status}) ->
                             Status == up
                     end, Consumers),
    ?assertEqual(1, map_size(Up)),

    %% settle message and ensure it is handled correctly
    {S6, _} = settle(Config, CKey1, 6, 0, S5),
    ?assertEqual(0, rabbit_fifo:query_messages_checked_out(S6)),
    ?assertEqual(0, rabbit_fifo:query_messages_total(S6)),

    %% requeue message and check that is handled
    {S6b, _} = return(Config, CKey1, 6, 0, S5),
    ?assertEqual(1, rabbit_fifo:query_messages_checked_out(S6b)),
    ?assertEqual(1, rabbit_fifo:query_messages_total(S6b)),
    %%
    %% TOOD: test this but without the fallback consumer
    %%
    %%
    %%
    %% MULTI checkout should not result in multiple waiting
    ok.

single_active_consumer_revive_2_test(C) ->
    S0 = init(#{name => ?FUNCTION_NAME,
                queue_resource => rabbit_misc:r("/", queue, ?FUNCTION_NAME_B),
                single_active_consumer_on => true}),
    Cid1 = {<<"one">>, self()},
    {S1, #{key := CKey}, _} = checkout(C, ?LINE, Cid1, 1, S0),
    {S2, _} = enq(C, 3, 1, first, S1),
    %% cancel the active consumer whilst it has a message pending
    {S3, _, _} = rabbit_fifo:apply(meta(C, 4), make_checkout(Cid1, cancel, #{}), S2),
    {S4, #{key := CKey}, _} = checkout(C, ?LINE, Cid1, 5, S3),
    ?assertEqual(1, rabbit_fifo:query_consumer_count(S4)),
    ?assertEqual(0, length(rabbit_fifo:query_waiting_consumers(S4))),
    ?assertEqual(1, rabbit_fifo:query_messages_total(S4)),
    ?assertEqual(1, rabbit_fifo:query_messages_checked_out(S4)),
    ok.

single_active_consumer_test(Config) ->
    State0 = init(#{name => ?FUNCTION_NAME,
                    queue_resource => rabbit_misc:r("/", queue,
                        atom_to_binary(?FUNCTION_NAME, utf8)),
                    release_cursor_interval => 0,
                    single_active_consumer_on => true}),
    ?assertEqual(single_active, State0#rabbit_fifo.cfg#cfg.consumer_strategy),
    ?assertEqual(0, map_size(State0#rabbit_fifo.consumers)),

    % adding some consumers
    C1 = {<<"ctag1">>, self()},
    C2 = {<<"ctag2">>, self()},
    C3 = {<<"ctag3">>, self()},
    C4 = {<<"ctag4">>, self()},
    CK1 = ?LINE,
    CK2 = ?LINE,
    CK3 = ?LINE,
    CK4 = ?LINE,
    Entries = [
               {CK1, make_checkout(C1, {once, {simple_prefetch, 1}}, #{})},
               {CK2, make_checkout(C2, {once, {simple_prefetch, 1}}, #{})},
               {CK3, make_checkout(C3, {once, {simple_prefetch, 1}}, #{})},
               {CK4, make_checkout(C4, {once, {simple_prefetch, 1}}, #{})}
              ],
    {State1, _} = run_log(Config, State0, Entries),

    % the first registered consumer is the active one, the others are waiting
    ?assertEqual(1, map_size(State1#rabbit_fifo.consumers)),
    ?assertMatch(#{CK1 := _}, State1#rabbit_fifo.consumers),

    ?assertMatch(#{single_active_consumer_id := C1,
                   single_active_num_waiting_consumers := 3},
                 rabbit_fifo:overview(State1)),
    ?assertEqual(3, length(rabbit_fifo:query_waiting_consumers(State1))),
    ?assertNotEqual(false, lists:keyfind(CK2, 1, rabbit_fifo:query_waiting_consumers(State1))),
    ?assertNotEqual(false, lists:keyfind(CK3, 1, rabbit_fifo:query_waiting_consumers(State1))),
    ?assertNotEqual(false, lists:keyfind(CK4, 1, rabbit_fifo:query_waiting_consumers(State1))),

    % cancelling a waiting consumer
    {State2, _, Effects1} = apply(meta(Config, ?LINE),
                                  make_checkout(C3, cancel, #{}),
                                  State1),
    % the active consumer should still be in place
    ?assertEqual(1, map_size(State2#rabbit_fifo.consumers)),
    ?assertMatch(#{CK1 := _}, State2#rabbit_fifo.consumers),
    % the cancelled consumer has been removed from waiting consumers
    ?assertMatch(#{single_active_consumer_id := C1,
                   single_active_num_waiting_consumers := 2},
                 rabbit_fifo:overview(State2)),
    ?assertEqual(2, length(rabbit_fifo:query_waiting_consumers(State2))),
    ?assertNotEqual(false, lists:keyfind(CK2, 1, rabbit_fifo:query_waiting_consumers(State2))),
    ?assertNotEqual(false, lists:keyfind(CK4, 1, rabbit_fifo:query_waiting_consumers(State2))),
    % there are some effects to unregister the consumer
    ?ASSERT_EFF({mod_call, rabbit_quorum_queue,
                 cancel_consumer_handler, [_, Con]}, Con == C3, Effects1),

    % cancelling the active consumer
    {State3, _, Effects2} = apply(meta(Config, ?LINE),
                                  make_checkout(C1, cancel, #{}),
                                  State2),
    % the second registered consumer is now the active one
    ?assertEqual(1, map_size(State3#rabbit_fifo.consumers)),
    ?assertMatch(#{CK2 := _}, State3#rabbit_fifo.consumers),
    % the new active consumer is no longer in the waiting list
    ?assertEqual(1, length(rabbit_fifo:query_waiting_consumers(State3))),
    ?assertNotEqual(false, lists:keyfind(CK4, 1,
                                         rabbit_fifo:query_waiting_consumers(State3))),
    %% should have a cancel consumer handler mod_call effect and
    %% an active new consumer effect
    ?ASSERT_EFF({mod_call, rabbit_quorum_queue,
                 cancel_consumer_handler, [_, Con]}, Con == C1, Effects2),
    ?ASSERT_EFF({mod_call, rabbit_quorum_queue,
                 update_consumer_handler, _}, Effects2),

    % cancelling the active consumer
    {State4, _, Effects3} = apply(meta(Config, ?LINE),
                                  make_checkout(C2, cancel, #{}),
                                  State3),
    % the last waiting consumer became the active one
    ?assertEqual(1, map_size(State4#rabbit_fifo.consumers)),
    ?assertMatch(#{CK4 := _}, State4#rabbit_fifo.consumers),
    % the waiting consumer list is now empty
    ?assertEqual(0, length(rabbit_fifo:query_waiting_consumers(State4))),
    % there are some effects to unregister the consumer and
    % to update the new active one (metrics)
    ?ASSERT_EFF({mod_call, rabbit_quorum_queue,
                 cancel_consumer_handler, [_, Con]}, Con == C2, Effects3),
    ?ASSERT_EFF({mod_call, rabbit_quorum_queue,
                 update_consumer_handler, _}, Effects3),

    % cancelling the last consumer
    {State5, _, Effects4} = apply(meta(Config, ?LINE),
                                  make_checkout(C4, cancel, #{}),
                                  State4),
    % no active consumer anymore
    ?assertEqual(0, map_size(State5#rabbit_fifo.consumers)),
    % still nothing in the waiting list
    ?assertEqual(0, length(rabbit_fifo:query_waiting_consumers(State5))),
    % there is an effect to unregister the consumer + queue inactive effect
    ?ASSERT_EFF({mod_call, rabbit_quorum_queue,
                 cancel_consumer_handler, _}, Effects4),

    ok.

single_active_consumer_cancel_consumer_when_channel_is_down_test(Config) ->
    State0 = init(#{name => ?FUNCTION_NAME,
                    queue_resource => rabbit_misc:r("/", queue, ?FUNCTION_NAME_B),
                    single_active_consumer_on => true}),

    Pid1 = spawn(fun() -> ok end),
    Pid2 = spawn(fun() -> ok end),
    Pid3 = spawn(fun() -> ok end),
    C1 = {<<"ctag1">>, Pid1},
    C2 = {<<"ctag2">>, Pid2},
    C3 = {<<"ctag3">>, Pid2},
    C4 = {<<"ctag4">>, Pid3},
    CK1 = ?LINE,
    CK2 = ?LINE,
    CK3 = ?LINE,
    CK4 = ?LINE,
    Entries = [
               {CK1, make_checkout(C1, {auto, {simple_prefetch, 1}}, #{})},
               {CK2, make_checkout(C2, {auto, {simple_prefetch, 1}}, #{})},
               {CK3, make_checkout(C3, {auto, {simple_prefetch, 1}}, #{})},
               {CK4, make_checkout(C4, {auto, {simple_prefetch, 1}}, #{})},
               ?ASSERT(#rabbit_fifo{consumers = #{CK1 := #consumer{status = up}}}),
               % the channel of the active consumer goes down
               {?LINE, {down, Pid1, noproc}}
              ],
    {State2, Effects} = run_log(Config, State0, Entries),

    % {State2, _, Effects} = apply(meta(Config, 2), {down, Pid1, noproc}, State1),
    % fell back to another consumer
    ?assertEqual(1, map_size(State2#rabbit_fifo.consumers)),
    % there are still waiting consumers
    ?assertEqual(2, length(rabbit_fifo:query_waiting_consumers(State2))),
    % effects to unregister the consumer and
    % to update the new active one (metrics) are there
    ?ASSERT_EFF({mod_call, rabbit_quorum_queue,
                 cancel_consumer_handler, [_, Con]}, Con == C1, Effects),
    ?ASSERT_EFF({mod_call, rabbit_quorum_queue,
                 update_consumer_handler, _}, Effects),

    ct:pal("STate2 ~p", [State2]),
    % the channel of the active consumer and a waiting consumer goes down
    {State3, _, Effects2} = apply(meta(Config, ?LINE), {down, Pid2, noproc}, State2),
    ct:pal("STate3 ~p", [State3]),
    ct:pal("Effects2 ~p", [Effects2]),
    % fell back to another consumer
    ?assertEqual(1, map_size(State3#rabbit_fifo.consumers)),
    % no more waiting consumer
    ?assertEqual(0, length(rabbit_fifo:query_waiting_consumers(State3))),
    % effects to cancel both consumers of this channel + effect to update the new active one (metrics)
    ?ASSERT_EFF({mod_call, rabbit_quorum_queue,
                 cancel_consumer_handler, [_, Con]}, Con == C2, Effects2),
    ?ASSERT_EFF({mod_call, rabbit_quorum_queue,
                 cancel_consumer_handler, [_, Con]}, Con == C3, Effects2),
    ?ASSERT_EFF({mod_call, rabbit_quorum_queue,
                 update_consumer_handler, _}, Effects2),

    % the last channel goes down
    {State4, _, Effects3} = apply(meta(Config, ?LINE),
                                  {down, Pid3, doesnotmatter}, State3),
    % no more consumers
    ?assertEqual(0, map_size(State4#rabbit_fifo.consumers)),
    ?assertEqual(0, length(rabbit_fifo:query_waiting_consumers(State4))),
    % there is an effect to unregister the consumer + queue inactive effect
    ?ASSERT_EFF({mod_call, rabbit_quorum_queue,
                 cancel_consumer_handler, [_, Con]}, Con == C4, Effects3),

    ok.

single_active_returns_messages_on_noconnection_test(Config) ->
    R = rabbit_misc:r("/", queue, ?FUNCTION_NAME_B),
    State0 = init(#{name => ?FUNCTION_NAME,
                    queue_resource => R,
                    release_cursor_interval => 0,
                    single_active_consumer_on => true}),
    % adding some consumers
    {CK1, {_, DownPid} = C1} = {?LINE, {?LINE_B, test_util:fake_pid(n1)}},
    Entries = [
               {CK1, make_checkout(C1, {auto, {simple_prefetch, 1}}, #{})}
              ],
    {State1, _} = run_log(Config, State0, Entries),
    {State2, _} = enq(Config, 4, 1, msg1, State1),
    % simulate node goes down
    {State3, _, _} = apply(meta(Config, ?LINE), {down, DownPid, noconnection}, State2),
    ct:pal("state3 ~p", [State3]),
    %% assert the consumer is up
    ?assertMatch([_], lqueue:to_list(State3#rabbit_fifo.returns)),
    ?assertMatch([{_, #consumer{checked_out = Checked,
                                credit = 1}}]
                 when map_size(Checked) == 0,
                      rabbit_fifo:query_waiting_consumers(State3)),

    ok.

single_active_consumer_replaces_consumer_when_down_noconnection_test(Config) ->
    R = rabbit_misc:r("/", queue, atom_to_binary(?FUNCTION_NAME, utf8)),
    State0 = init(#{name => ?FUNCTION_NAME,
                    queue_resource => R,
                    release_cursor_interval => 0,
                    single_active_consumer_on => true}),

    {CK1, {_, DownPid} = C1} = {?LINE, {?LINE_B, test_util:fake_pid(n1)}},
    {CK2, C2} = {?LINE, {?LINE_B, test_util:fake_pid(n2)}},
    {CK3, C3} = {?LINE, {?LINE_B, test_util:fake_pid(n3)}},
    Entries = [
               {CK1, make_checkout(C1, {auto, {simple_prefetch, 1}}, #{})},
               {CK2, make_checkout(C2, {auto, {simple_prefetch, 1}}, #{})},
               {CK3, make_checkout(C3, {auto, {simple_prefetch, 1}}, #{})},
               {?LINE, rabbit_fifo:make_enqueue(self(), 1, msg)}
              ],
    {State1, _} = run_log(Config, State0, Entries),
    %% assert the consumer is up
    ?assertMatch(#{CK1 := #consumer{status = up}},
                 State1#rabbit_fifo.consumers),

    % simulate node goes down
    {State2, _, _} = apply(meta(Config, ?LINE),
                           {down, DownPid, noconnection}, State1),

    %% assert a new consumer is in place and it is up
    ?assertMatch([{CK2, #consumer{status = up,
                                  checked_out = Ch}}]
                   when map_size(Ch) == 1,
                        maps:to_list(State2#rabbit_fifo.consumers)),

    %% the disconnected consumer has been returned to waiting
    ?assert(lists:any(fun ({Con, _}) -> Con =:= CK1 end,
                      rabbit_fifo:query_waiting_consumers(State2))),
    ?assertEqual(2, length(rabbit_fifo:query_waiting_consumers(State2))),

    % simulate node comes back up
    {State3, _, _} = apply(meta(Config, 2), {nodeup, node(DownPid)}, State2),

    %% the consumer is still active and the same as before
    ?assertMatch([{CK2, #consumer{status = up}}],
                 maps:to_list(State3#rabbit_fifo.consumers)),
    % the waiting consumers should be un-suspected
    ?assertEqual(2, length(rabbit_fifo:query_waiting_consumers(State3))),
    lists:foreach(fun({_, #consumer{status = Status}}) ->
                      ?assert(Status /= suspected_down)
                  end, rabbit_fifo:query_waiting_consumers(State3)),
    ok.

single_active_consumer_all_disconnected_test(Config) ->
    R = rabbit_misc:r("/", queue, atom_to_binary(?FUNCTION_NAME, utf8)),
    State0 = init(#{name => ?FUNCTION_NAME,
                    queue_resource => R,
                    release_cursor_interval => 0,
                    single_active_consumer_on => true}),

    {CK1, {_, C1Pid} = C1} = {?LINE, {?LINE_B, test_util:fake_pid(n1)}},
    {CK2, {_, C2Pid} = C2} = {?LINE, {?LINE_B, test_util:fake_pid(n2)}},
    Entries =
    [
     {CK1, make_checkout(C1, {auto, {simple_prefetch, 1}}, #{})},
     {CK2, make_checkout(C2, {auto, {simple_prefetch, 1}}, #{})},
     ?ASSERT(#rabbit_fifo{consumers = #{CK1 := #consumer{status = up}}}),
     {?LINE, {down, C1Pid, noconnection}},
     ?ASSERT(#rabbit_fifo{consumers = #{CK2 := #consumer{status = up}}}),
     {?LINE, {down, C2Pid, noconnection}},
     ?ASSERT(#rabbit_fifo{consumers = C} when map_size(C) == 0),
     {?LINE, {nodeup, node(C2Pid)}},
     ?ASSERT(#rabbit_fifo{consumers = #{CK2 := #consumer{status = up,
                                                         credit = 1}}})
    ],
    {_State1, _} = run_log(Config, State0, Entries),
    ok.

single_active_consumer_state_enter_leader_include_waiting_consumers_test(Config) ->
    State0 = init(#{name => ?FUNCTION_NAME,
                    queue_resource => rabbit_misc:r("/", queue, ?FUNCTION_NAME_B),
                    release_cursor_interval => 0,
                    single_active_consumer_on => true}),

    Pid1 = spawn(fun() -> ok end),
    Pid2 = spawn(fun() -> ok end),
    Pid3 = spawn(fun() -> ok end),
    C1 = {<<"ctag1">>, Pid1},
    C2 = {<<"ctag2">>, Pid2},
    C3 = {<<"ctag3">>, Pid2},
    C4 = {<<"ctag4">>, Pid3},
    CK1 = ?LINE,
    CK2 = ?LINE,
    CK3 = ?LINE,
    CK4 = ?LINE,
    Entries = [
               {CK1, make_checkout(C1, {auto, {simple_prefetch, 1}}, #{})},
               {CK2, make_checkout(C2, {auto, {simple_prefetch, 1}}, #{})},
               {CK3, make_checkout(C3, {auto, {simple_prefetch, 1}}, #{})},
               {CK4, make_checkout(C4, {auto, {simple_prefetch, 1}}, #{})}
              ],
    {State1, _} = run_log(Config, State0, Entries),
    Effects = rabbit_fifo:state_enter(leader, State1),
    %% 2 effects for each consumer process (channel process), 1 effect for the node,
    ?assertEqual(2 * 3 + 1 + 1 + 1, length(Effects)).

single_active_consumer_state_enter_eol_include_waiting_consumers_test(Config) ->
    Resource = rabbit_misc:r("/", queue, ?FUNCTION_NAME_B),
    State0 = init(#{name => ?FUNCTION_NAME,
                    queue_resource => Resource,
                    release_cursor_interval => 0,
                    single_active_consumer_on => true}),

    Pid1 = spawn(fun() -> ok end),
    Pid2 = spawn(fun() -> ok end),
    Pid3 = spawn(fun() -> ok end),
    {CK1, C1} = {?LINE, {?LINE_B, Pid1}},
    {CK2, C2} = {?LINE, {?LINE_B, Pid2}},
    {CK3, C3} = {?LINE, {?LINE_B, Pid2}},
    {CK4, C4} = {?LINE, {?LINE_B, Pid3}},
    Entries = [
               {CK1, make_checkout(C1, {auto, {simple_prefetch, 1}}, #{})},
               {CK2, make_checkout(C2, {auto, {simple_prefetch, 1}}, #{})},
               {CK3, make_checkout(C3, {auto, {simple_prefetch, 1}}, #{})},
               {CK4, make_checkout(C4, {auto, {simple_prefetch, 1}}, #{})}
              ],
    {State1, _} = run_log(Config, State0, Entries),
    Effects = rabbit_fifo:state_enter(eol, State1),
    %% 1 effect for each consumer process (channel process),
    %% 1 effect for eol to handle rabbit_fifo_usage entries
    ?assertEqual(4, length(Effects)),
    ok.

query_consumers_test(Config) ->
    State0 = init(#{name => ?FUNCTION_NAME,
                    queue_resource => rabbit_misc:r("/", queue,
                        atom_to_binary(?FUNCTION_NAME, utf8)),
                    release_cursor_interval => 0,
                    single_active_consumer_on => false}),

    % % adding some consumers
    {CK1, C1} = {?LINE, {?LINE_B, self()}},
    {CK2, C2} = {?LINE, {?LINE_B, self()}},
    {CK3, C3} = {?LINE, {?LINE_B, self()}},
    {CK4, C4} = {?LINE, {?LINE_B, self()}},
    Entries = [
               {CK1, make_checkout(C1, {auto, {simple_prefetch, 1}}, #{})},
               {CK2, make_checkout(C2, {auto, {simple_prefetch, 1}}, #{})},
               {CK3, make_checkout(C3, {auto, {simple_prefetch, 1}}, #{})},
               {CK4, make_checkout(C4, {auto, {simple_prefetch, 1}}, #{})}
              ],
    {State1, _} = run_log(Config, State0, Entries),
    Consumers0 = State1#rabbit_fifo.consumers,
    Consumer = maps:get(CK2, Consumers0),
    Consumers1 = maps:put(CK2, Consumer#consumer{status = suspected_down},
                          Consumers0),
    State2 = State1#rabbit_fifo{consumers = Consumers1},

    ?assertEqual(3, rabbit_fifo:query_consumer_count(State2)),
    Consumers2 = rabbit_fifo:query_consumers(State2),
    ?assertEqual(4, maps:size(Consumers2)),
    maps:fold(fun(Key, {Pid, _Tag, _, _, Active, ActivityStatus, _, _}, _Acc) ->
                      ?assertEqual(self(), Pid),
                      case Key of
                          CK2 ->
                              ?assertNot(Active),
                              ?assertEqual(suspected_down, ActivityStatus);
                          _ ->
                              ?assert(Active),
                              ?assertEqual(up, ActivityStatus)
                      end
              end, [], Consumers2),
    ok.

query_consumers_when_single_active_consumer_is_on_test(Config) ->
    State0 = init(#{name => ?FUNCTION_NAME,
                    queue_resource => rabbit_misc:r("/", queue, ?FUNCTION_NAME_B),
                    release_cursor_interval => 0,
                    single_active_consumer_on => true}),

    % % adding some consumers
    {CK1, C1} = {?LINE, {?LINE_B, self()}},
    {CK2, C2} = {?LINE, {?LINE_B, self()}},
    {CK3, C3} = {?LINE, {?LINE_B, self()}},
    {CK4, C4} = {?LINE, {?LINE_B, self()}},
    Entries = [
               {CK1, make_checkout(C1, {auto, {simple_prefetch, 1}}, #{})},
               {CK2, make_checkout(C2, {auto, {simple_prefetch, 1}}, #{})},
               {CK3, make_checkout(C3, {auto, {simple_prefetch, 1}}, #{})},
               {CK4, make_checkout(C4, {auto, {simple_prefetch, 1}}, #{})}
              ],
    {State1, _} = run_log(Config, State0, Entries),
    ?assertEqual(4, rabbit_fifo:query_consumer_count(State1)),
    Consumers = rabbit_fifo:query_consumers(State1),
    ?assertEqual(4, maps:size(Consumers)),
    maps:fold(fun(Key, {Pid, _Tag, _, _, Active, ActivityStatus, _, _}, _Acc) ->
                  ?assertEqual(self(), Pid),
                  case Key of
                     CK1 ->
                         ?assert(Active),
                         ?assertEqual(single_active, ActivityStatus);
                     _ ->
                         ?assertNot(Active),
                         ?assertEqual(waiting, ActivityStatus)
                  end
              end, [], Consumers),
    ok.

active_flag_updated_when_consumer_suspected_unsuspected_test(Config) ->
    State0 = init(#{name => ?FUNCTION_NAME,
                    queue_resource => rabbit_misc:r("/", queue,
                                                    ?FUNCTION_NAME_B),
                    release_cursor_interval => 0,
                    single_active_consumer_on => false}),

    DummyFunction = fun() -> ok  end,
    Pid1 = spawn(DummyFunction),
    Pid2 = spawn(DummyFunction),
    Pid3 = spawn(DummyFunction),

    % adding some consumers
    {CK1, C1} = {?LINE, {?LINE_B, Pid1}},
    {CK2, C2} = {?LINE, {?LINE_B, Pid2}},
    {CK3, C3} = {?LINE, {?LINE_B, Pid2}},
    {CK4, C4} = {?LINE, {?LINE_B, Pid3}},
    Entries = [
               {CK1, make_checkout(C1, {auto, {simple_prefetch, 1}}, #{})},
               {CK2, make_checkout(C2, {auto, {simple_prefetch, 1}}, #{})},
               {CK3, make_checkout(C3, {auto, {simple_prefetch, 1}}, #{})},
               {CK4, make_checkout(C4, {auto, {simple_prefetch, 1}}, #{})}
              ],
    {State1, _} = run_log(Config, State0, Entries),
    {State2, _, Effects2} = apply(meta(Config, 3),
                                    {down, Pid1, noconnection}, State1),
    % 1 effect to update the metrics of each consumer
    % (they belong to the same node),
    % 1 more effect to monitor the node,
    % 1 more decorators effect
    ?assertEqual(4 + 1, length(Effects2)),

    {_, _, Effects3} = apply(meta(Config, 4), {nodeup, node(self())}, State2),
    % for each consumer: 1 effect to update the metrics,
    % 1 effect to monitor the consumer PID, 1 more decorators effect
    ?assertEqual(4 + 4, length(Effects3)),
    ok.

active_flag_not_updated_when_consumer_suspected_unsuspected_and_single_active_consumer_is_on_test(Config) ->
    State0 = init(#{name => ?FUNCTION_NAME,
                    queue_resource => rabbit_misc:r("/", queue, ?FUNCTION_NAME_B),
                    release_cursor_interval => 0,
                    single_active_consumer_on => true}),

    DummyFunction = fun() -> ok  end,
    Pid1 = spawn(DummyFunction),
    Pid2 = spawn(DummyFunction),
    Pid3 = spawn(DummyFunction),

    % adding some consumers
    {CK1, C1} = {?LINE, {?LINE_B, Pid1}},
    {CK2, C2} = {?LINE, {?LINE_B, Pid2}},
    {CK3, C3} = {?LINE, {?LINE_B, Pid2}},
    {CK4, C4} = {?LINE, {?LINE_B, Pid3}},
    Entries = [
               {CK1, make_checkout(C1, {auto, {simple_prefetch, 1}}, #{})},
               {CK2, make_checkout(C2, {auto, {simple_prefetch, 1}}, #{})},
               {CK3, make_checkout(C3, {auto, {simple_prefetch, 1}}, #{})},
               {CK4, make_checkout(C4, {auto, {simple_prefetch, 1}}, #{})}
              ],
    {State1, _} = run_log(Config, State0, Entries),
    {State2, _, Effects2} = apply(meta(Config, 2), {down, Pid1, noconnection}, State1),
    % one monitor and one consumer status update (deactivated)
    ?assertEqual(2, length(Effects2)),

    {_, _, Effects3} = apply(meta(Config, 3), {nodeup, node(self())}, State2),
    % for each consumer: 1 effect to monitor the consumer PID
    ?assertEqual(5, length(Effects3)),
    ok.

single_active_cancelled_with_unacked_test(Config) ->
    State0 = init(#{name => ?FUNCTION_NAME,
                    queue_resource => rabbit_misc:r("/", queue, ?FUNCTION_NAME_B),
                    release_cursor_interval => 0,
                    single_active_consumer_on => true}),

    % % adding some consumers
    {CK1, C1} = {?LINE, {?LINE_B, self()}},
    {CK2, C2} = {?LINE, {?LINE_B, self()}},
    Entries = [
               {CK1, make_checkout(C1, {auto, {simple_prefetch, 1}}, #{})},
               {CK2, make_checkout(C2, {auto, {simple_prefetch, 1}}, #{})}
              ],
    {State1, _} = run_log(Config, State0, Entries),

    %% enqueue 2 messages
    {State2, _Effects2} = enq(Config, 3, 1, msg1, State1),
    {State3, _Effects3} = enq(Config, 4, 2, msg2, State2),
    %% one should be checked ou to C1
    %% cancel C1
    {State4, _, _} = apply(meta(Config, ?LINE),
                           make_checkout(C1, cancel, #{}),
                           State3),
    %% C2 should be the active consumer
    ?assertMatch(#{CK2 := #consumer{status = up,
                                    checked_out = #{0 := _}}},
                 State4#rabbit_fifo.consumers),
    %% C1 should be a cancelled consumer
    ?assertMatch(#{CK1 := #consumer{status = cancelled,
                                    cfg = #consumer_cfg{lifetime = once},
                                    checked_out = #{0 := _}}},
                 State4#rabbit_fifo.consumers),
    ?assertMatch([], rabbit_fifo:query_waiting_consumers(State4)),

    %% Ack both messages
    {State5, _Effects5} = settle(Config, CK1, ?LINE, 0, State4),
    %% C1 should now be cancelled
    {State6, _Effects6} = settle(Config, CK2, ?LINE, 0, State5),

    %% C2 should remain
    ?assertMatch(#{CK2 := #consumer{status = up}},
                 State6#rabbit_fifo.consumers),
    %% C1 should be gone
    ?assertNotMatch(#{CK1 := _},
                    State6#rabbit_fifo.consumers),
    ?assertMatch([], rabbit_fifo:query_waiting_consumers(State6)),
    ok.

single_active_with_credited_v1_test(Config) ->
    State0 = init(#{name => ?FUNCTION_NAME,
                    queue_resource => rabbit_misc:r("/", queue, ?FUNCTION_NAME_B),
                    release_cursor_interval => 0,
                    single_active_consumer_on => true}),

    % % adding some consumers
    {CK1, C1} = {?LINE, {?LINE_B, self()}},
    {CK2, C2} = {?LINE, {?LINE_B, self()}},
    Entries = [
               {CK1, make_checkout(C1, {auto, {simple_prefetch, 1}}, #{})},
               {CK2, make_checkout(C2, {auto, {simple_prefetch, 1}}, #{})}
              ],
    {State1, _} = run_log(Config, State0, Entries),

    %% add some credit
    C1Cred = rabbit_fifo:make_credit(CK1, 5, 0, false),
    {State2, _, _Effects2} = apply(meta(Config, ?LINE), C1Cred, State1),
    C2Cred = rabbit_fifo:make_credit(CK2, 4, 0, false),
    {State3, _} = apply(meta(Config, ?LINE), C2Cred, State2),
    %% both consumers should have credit
    ?assertMatch(#{CK1 := #consumer{credit = 5}},
                 State3#rabbit_fifo.consumers),
    ?assertMatch([{CK2, #consumer{credit = 4}}],
                 rabbit_fifo:query_waiting_consumers(State3)),
    ok.

single_active_with_credited_v2_test(Config) ->
    State0 = init(#{name => ?FUNCTION_NAME,
                    queue_resource => rabbit_misc:r("/", queue,
                                                    ?FUNCTION_NAME_B),
                    release_cursor_interval => 0,
                    single_active_consumer_on => true}),
    C1 = {<<"ctag1">>, self()},
    {State1, {ok, #{key := CKey1}}, _} =
        apply(meta(Config, 1),
              make_checkout(C1, {auto, {credited, 0}}, #{}), State0),
    C2 = {<<"ctag2">>, self()},
    {State2, {ok, #{key := CKey2}}, _} =
        apply(meta(Config, 2),
              make_checkout(C2, {auto, {credited, 0}}, #{}), State1),
    %% add some credit
    C1Cred = rabbit_fifo:make_credit(CKey1, 5, 0, false),
    {State3, ok, Effects1} = apply(meta(Config, 3), C1Cred, State2),
    ?assertEqual([{send_msg, self(),
                   {credit_reply, <<"ctag1">>, _DeliveryCount = 0, _Credit = 5,
                    _Available = 0, _Drain = false},
                   ?DELIVERY_SEND_MSG_OPTS}],
                 Effects1),

    C2Cred = rabbit_fifo:make_credit(CKey2, 4, 0, false),
    {State, ok, Effects2} = apply(meta(Config, 4), C2Cred, State3),
    ?assertEqual({send_msg, self(),
                  {credit_reply, <<"ctag2">>, _DeliveryCount = 0, _Credit = 4,
                   _Available = 0, _Drain = false},
                  ?DELIVERY_SEND_MSG_OPTS},
                 Effects2),

    %% both consumers should have credit
    ?assertMatch(#{CKey1 := #consumer{credit = 5}},
                 State#rabbit_fifo.consumers),
    ?assertMatch([{CKey2, #consumer{credit = 4}}],
                 rabbit_fifo:query_waiting_consumers(State)),
    ok.

single_active_settle_after_cancel_test(Config) ->
    S0 = init(#{name => ?FUNCTION_NAME,
                queue_resource => rabbit_misc:r("/", queue, ?FUNCTION_NAME_B),
                single_active_consumer_on => true}),

    Pid1 = test_util:fake_pid(node()),
    % % adding some consumers
    E1Idx = ?LINE,
    {CK1, C1} = {?LINE, {?LINE_B, self()}},
    {CK2, C2} = {?LINE, {?LINE_B, self()}},
    Entries =
    [
     {E1Idx , rabbit_fifo:make_enqueue(Pid1, 1, msg1)},
     %% add a consumer
     {CK1, make_checkout(C1, {auto, {simple_prefetch, 1}}, #{})},
     ?ASSERT(#rabbit_fifo{consumers = #{CK1 := #consumer{next_msg_id = 1,
                                                         status = up,
                                                         checked_out = Ch}}}
               when map_size(Ch) == 1),
     %% add another consumer
     {CK2, make_checkout(C2, {auto, {simple_prefetch, 1}}, #{})},
     ?ASSERT(#rabbit_fifo{consumers = #{CK1 := #consumer{status = up}},
                          waiting_consumers = [{CK2, _}]}),

     %% cancel C1
     {?LINE, make_checkout(C1, cancel, #{})},
     ?ASSERT(#rabbit_fifo{consumers = #{CK1 := #consumer{status = cancelled},
                                        CK2 := #consumer{status = up}},
                          waiting_consumers = []}),
     %% settle the message, C1 one should be completely removed
     {?LINE, rabbit_fifo:make_settle(CK1, [0])},
     ?ASSERT(#rabbit_fifo{consumers = #{CK2 := #consumer{status = up}} = C,
                          waiting_consumers = []}
               when map_size(C) == 1)

    ],
    {_S1, _} = run_log(Config, S0, Entries, fun single_active_invariant/1),

    ok.

single_active_consumer_priority_test(Config) ->
    S0 = init(#{name => ?FUNCTION_NAME,
                queue_resource => rabbit_misc:r("/", queue, ?FUNCTION_NAME_B),
                single_active_consumer_on => true}),

    Pid1 = test_util:fake_pid(node()),
    % % adding some consumers
    {CK1, C1} = {?LINE, {?LINE_B, self()}},
    {CK2, C2} = {?LINE, {?LINE_B, self()}},
    E1Idx = ?LINE,
    {CK3, C3} = {?LINE, {?LINE_B, self()}},
    Entries =
    [
     %% add a consumer
     {CK1, make_checkout(C1, {auto, {simple_prefetch, 1}}, #{priority => 1})},
     ?ASSERT(#rabbit_fifo{consumers = #{CK1 := #consumer{status = up}},
                          waiting_consumers = []}),

     %% add a consumer with a higher priority, assert it becomes active
     {CK2, make_checkout(C2, {auto, {simple_prefetch, 1}}, #{priority => 2})},
     ?ASSERT(#rabbit_fifo{consumers = #{CK2 := #consumer{status = up}},
                          waiting_consumers = [{CK1, _}]}),
     %% enqueue a message
     {E1Idx , rabbit_fifo:make_enqueue(Pid1, 1, msg1)},
     ?ASSERT(#rabbit_fifo{consumers = #{CK2 := #consumer{next_msg_id = 1,
                                                         status = up,
                                                         checked_out = Ch}}}
               when map_size(Ch) == 1),

     %% add en even higher consumer, but the current active has a message pending
     %% so can't be immedately replaced
     {CK3, make_checkout(C3, {auto, {simple_prefetch, 1}}, #{priority => 3})},
     ?ASSERT(#rabbit_fifo{consumers = #{CK2 := #consumer{status = quiescing}},
                          waiting_consumers = [_, _]}),
     %% settle the message, the higher priority should become the active,
     %% completing the replacement
     {?LINE, rabbit_fifo:make_settle(CK2, [0])},
     ?ASSERT(#rabbit_fifo{consumers = #{CK3 := #consumer{status = up,
                                                         checked_out = Ch}},
                          waiting_consumers = [_, _]}
               when map_size(Ch) == 0)

    ],
    {#rabbit_fifo{ cfg = #cfg{resource = Resource}}, StateMachineEvents} = run_log(Config, S0, Entries, fun single_active_invariant/1),
    ModCalls = [ S || S = {mod_call, rabbit_quorum_queue, update_consumer_handler, _} <- StateMachineEvents ],
    
    %% C1 should be added as single_active    
    assert_update_consumer_handler_state_transition(C1, Resource, true, single_active, lists:nth(1, ModCalls)),
    %% C1 should transition to waiting because ...
    assert_update_consumer_handler_state_transition(C1, Resource, false, waiting, lists:nth(2, ModCalls)),
    %% C2 should become single_active
    assert_update_consumer_handler_state_transition(C2, Resource, true, single_active, lists:nth(3, ModCalls)),
    %% C2 should transition as waiting because ...
    assert_update_consumer_handler_state_transition(C2, Resource, false, waiting, lists:nth(4, ModCalls)),
    %% C3 is added as single_active
    assert_update_consumer_handler_state_transition(C3, Resource, true, single_active, lists:nth(5, ModCalls)),
    
    ok.

assert_update_consumer_handler_state_transition(ConsumerId, Resource, IsActive, UpdatedState, ModCall) ->
    {mod_call,rabbit_quorum_queue,update_consumer_handler,
        [Resource,
        ConsumerId,
        _,_,_,IsActive,UpdatedState,[]]} = ModCall.

single_active_consumer_priority_cancel_active_test(Config) ->
    S0 = init(#{name => ?FUNCTION_NAME,
                queue_resource => rabbit_misc:r("/", queue, ?FUNCTION_NAME_B),
                single_active_consumer_on => true}),

    % % adding some consumers
    {CK1, C1} = {?LINE, {?LINE_B, self()}},
    {CK2, C2} = {?LINE, {?LINE_B, self()}},
    {CK3, C3} = {?LINE, {?LINE_B, self()}},
    Entries =
    [
     %% add a consumer
     {CK1, make_checkout(C1, {auto, {simple_prefetch, 1}}, #{priority => 2})},
     ?ASSERT(#rabbit_fifo{consumers = #{CK1 := #consumer{status = up}},
                          waiting_consumers = []}),

     %% add two consumers each with a lower priority
     {CK2, make_checkout(C2, {auto, {simple_prefetch, 1}}, #{priority => 1})},
     {CK3, make_checkout(C3, {auto, {simple_prefetch, 1}}, #{priority => 0})},
     ?ASSERT(#rabbit_fifo{consumers = #{CK1 := #consumer{status = up}},
                          waiting_consumers = [_, _]}),

     {?LINE, make_checkout(C1, cancel, #{})},
     ?ASSERT(#rabbit_fifo{consumers = #{CK2 := #consumer{status = up}},
                          waiting_consumers = [{CK3, _}]})
    ],
    {_S1, _} = run_log(Config, S0, Entries, fun single_active_invariant/1),

    ok.

single_active_consumer_update_priority_test(Config) ->
    S0 = init(#{name => ?FUNCTION_NAME,
                queue_resource => rabbit_misc:r("/", queue, ?FUNCTION_NAME_B),
                single_active_consumer_on => true}),

    Pid1 = test_util:fake_pid(node()),
    % % adding some consumers
    {CK1, C1} = {?LINE, {?LINE_B, self()}},
    {CK2, C2} = {?LINE, {?LINE_B, self()}},
    Entries =
    [
     %% add a consumer
     {CK1, make_checkout(C1, {auto, {simple_prefetch, 1}}, #{priority => 2})},
     ?ASSERT(#rabbit_fifo{consumers = #{CK1 := #consumer{status = up}},
                          waiting_consumers = []}),
     %% add abother consumer with lower priority
     {CK2, make_checkout(C2, {auto, {simple_prefetch, 1}}, #{priority => 1})},
     %% update the current active consumer to lower priority
     {?LINE, make_checkout(C1, {auto, {simple_prefetch, 1}}, #{priority => 0})},
     ?ASSERT(#rabbit_fifo{consumers = #{CK2 := #consumer{status = up}},
                          waiting_consumers = [_]}),
     %% back to original priority
     {?LINE, make_checkout(C1, {auto, {simple_prefetch, 1}}, #{priority => 2})},
     ?ASSERT(#rabbit_fifo{consumers = #{CK1 := #consumer{status = up}},
                          waiting_consumers = [_]}),
     {?LINE , rabbit_fifo:make_enqueue(Pid1, 1, msg1)},
     ?ASSERT(#rabbit_fifo{consumers = #{CK1 := #consumer{checked_out = Ch}},
                          waiting_consumers = [{CK2, _}]}
               when map_size(Ch) == 1),
     %% update priority for C2
     {?LINE, make_checkout(C2, {auto, {simple_prefetch, 1}}, #{priority => 3})},
     ?ASSERT(#rabbit_fifo{consumers = #{CK1 := #consumer{checked_out = Ch}},
                          waiting_consumers = [{CK2, _}]}
               when map_size(Ch) == 1),
     %% settle should cause the existing active to be replaced
     {?LINE, rabbit_fifo:make_settle(CK1, [0])},
     ?ASSERT(#rabbit_fifo{consumers = #{CK2 := #consumer{status = up}},
                          waiting_consumers = [{CK1, _}]})
    ],
    {_S1, _} = run_log(Config, S0, Entries, fun single_active_invariant/1),
    ok.

single_active_consumer_quiescing_resumes_after_cancel_test(Config) ->
    S0 = init(#{name => ?FUNCTION_NAME,
                queue_resource => rabbit_misc:r("/", queue, ?FUNCTION_NAME_B),
                single_active_consumer_on => true}),

    Pid1 = test_util:fake_pid(node()),
    % % adding some consumers
    {CK1, C1} = {?LINE, {?LINE_B, self()}},
    {CK2, C2} = {?LINE, {?LINE_B, self()}},
    Entries =
    [
     %% add a consumer
     {CK1, make_checkout(C1, {auto, {simple_prefetch, 1}}, #{priority => 1})},
     ?ASSERT(#rabbit_fifo{consumers = #{CK1 := #consumer{status = up}},
                          waiting_consumers = []}),

     %% enqueue a message
     {?LINE , rabbit_fifo:make_enqueue(Pid1, 1, msg1)},

     %% add a consumer with a higher priority, current is quiescing
     {CK2, make_checkout(C2, {auto, {simple_prefetch, 1}}, #{priority => 2})},
     ?ASSERT(#rabbit_fifo{consumers = #{CK1 := #consumer{status = quiescing}},
                          waiting_consumers = [{CK2, _}]}),

     %% C2 cancels
     {?LINE, make_checkout(C2, cancel, #{})},
     ?ASSERT(#rabbit_fifo{consumers = #{CK1 := #consumer{status = quiescing,
                                                         checked_out = Ch}},
                         waiting_consumers = []}
               when map_size(Ch) == 1),

     %% settle
     {?LINE, rabbit_fifo:make_settle(CK1, [0])},
     ?ASSERT(#rabbit_fifo{consumers = #{CK1 := #consumer{status = up,
                                                         credit = 1}},
                          waiting_consumers = []})
    ],

    {_S1, _} = run_log(Config, S0, Entries, fun single_active_invariant/1),

    ok.

single_active_consumer_higher_waiting_disconnected_test(Config) ->
    S0 = init(#{name => ?FUNCTION_NAME,
                queue_resource => rabbit_misc:r("/", queue, ?FUNCTION_NAME_B),
                single_active_consumer_on => true}),

    Pid1 = test_util:fake_pid(node()),
    C1Pid = test_util:fake_pid(n1@banana),
    C2Pid = test_util:fake_pid(n2@banana),
    % % adding some consumers
    {CK1, C1} = {?LINE, {?LINE_B, C1Pid}},
    {CK2, C2} = {?LINE, {?LINE_B, C2Pid}},
    Entries =
    [
     %% add a consumer
     {CK1, make_checkout(C1, {auto, {simple_prefetch, 1}}, #{priority => 1})},
     ?ASSERT(#rabbit_fifo{consumers = #{CK1 := #consumer{status = up}},
                          waiting_consumers = []}),

     %% enqueue a message
     {?LINE , rabbit_fifo:make_enqueue(Pid1, 1, msg1)},

     %% add a consumer with a higher priority, current is quiescing
     {CK2, make_checkout(C2, {auto, {simple_prefetch, 1}}, #{priority => 2})},
     ?ASSERT(#rabbit_fifo{consumers = #{CK1 := #consumer{status = quiescing}},
                          waiting_consumers = [{CK2, _}]}),
     %% C2 is disconnected,
     {?LINE, {down, C2Pid, noconnection}},
     ?ASSERT(
        #rabbit_fifo{consumers = #{CK1 := #consumer{status = quiescing}},
                     waiting_consumers = [{CK2, #consumer{status = suspected_down}}]}),
     %% settle
     {?LINE, rabbit_fifo:make_settle(CK1, [0])},
     %% C1 should be reactivated
     ?ASSERT(#rabbit_fifo{consumers = #{CK1 := #consumer{status = up,
                                                         credit = 1}},
                          waiting_consumers = [_]}),
     %% C2 comes back up and takes over
     {?LINE, {nodeup, n2@banana}},
     ?ASSERT(
        #rabbit_fifo{consumers = #{CK2 := #consumer{status = up}},
                     waiting_consumers = [{CK1, #consumer{status = up}}]})
    ],
    {_S1, _} = run_log(Config, S0, Entries, fun single_active_invariant/1),

    ok.

single_active_consumer_higher_waiting_return_test(Config) ->
    S0 = init(#{name => ?FUNCTION_NAME,
                queue_resource => rabbit_misc:r("/", queue, ?FUNCTION_NAME_B),
                single_active_consumer_on => true}),

    Pid1 = test_util:fake_pid(node()),
    C1Pid = test_util:fake_pid(n1@banana),
    C2Pid = test_util:fake_pid(n2@banana),
    % % adding some consumers
    {CK1, C1} = {?LINE, {?LINE_B, C1Pid}},
    {CK2, C2} = {?LINE, {?LINE_B, C2Pid}},
    Entries =
    [
     %% add a consumer
     {CK1, make_checkout(C1, {auto, {simple_prefetch, 1}}, #{priority => 1})},
     ?ASSERT(#rabbit_fifo{consumers = #{CK1 := #consumer{status = up}},
                          waiting_consumers = []}),

     %% enqueue a message
     {?LINE , rabbit_fifo:make_enqueue(Pid1, 1, msg1)},

     %% add a consumer with a higher priority, current is quiescing
     {CK2, make_checkout(C2, {auto, {simple_prefetch, 1}}, #{priority => 2})},
     ?ASSERT(#rabbit_fifo{consumers = #{CK1 := #consumer{status = quiescing}},
                          waiting_consumers = [{CK2, _}]}),
     %% C1 returns message
     {?LINE, rabbit_fifo:make_return(CK1, [0])},
     %% C2 should activated
     ?ASSERT(#rabbit_fifo{consumers = #{CK2 := #consumer{status = up,
                                                         checked_out = Ch,
                                                         credit = 0}},
                          waiting_consumers = [_]} when map_size(Ch) == 1)
    ],
    {_S1, _} = run_log(Config, S0, Entries, fun single_active_invariant/1),

    ok.

single_active_consumer_higher_waiting_requeue_test(Config) ->
    S0 = init(#{name => ?FUNCTION_NAME,
                queue_resource => rabbit_misc:r("/", queue, ?FUNCTION_NAME_B),
                single_active_consumer_on => true}),

    Pid1 = test_util:fake_pid(node()),
    C1Pid = test_util:fake_pid(n1@banana),
    C2Pid = test_util:fake_pid(n2@banana),
    % % adding some consumers
    {CK1, C1} = {?LINE, {?LINE_B, C1Pid}},
    EnqIdx = ?LINE,
    RequeueIdx = ?LINE,
    {CK2, C2} = {?LINE, {?LINE_B, C2Pid}},
    Entries =
    [
     %% add a consumer
     {CK1, make_checkout(C1, {auto, {simple_prefetch, 1}}, #{priority => 1})},
     ?ASSERT(#rabbit_fifo{consumers = #{CK1 := #consumer{status = up}},
                          waiting_consumers = []}),

     %% enqueue a message
     {EnqIdx , rabbit_fifo:make_enqueue(Pid1, 1, msg1)},

     %% add a consumer with a higher priority, current is quiescing
     {CK2, make_checkout(C2, {auto, {simple_prefetch, 1}}, #{priority => 2})},
     ?ASSERT(#rabbit_fifo{consumers = #{CK1 := #consumer{status = quiescing}},
                          waiting_consumers = [{CK2, _}]}),
     %% C1 returns message
     % {?LINE, rabbit_fifo:make_requeue(CK1, [0])},
     {RequeueIdx , element(2, hd(rabbit_fifo:make_requeue(CK1, {notify, 1, self()},
                                               [{0, EnqIdx, 0, msg1}], [])))},
     %% C2 should activated
     ?ASSERT(#rabbit_fifo{consumers = #{CK2 := #consumer{status = up,
                                                         checked_out = Ch,
                                                         credit = 0}},
                          waiting_consumers = [_]} when map_size(Ch) == 1)
    ],
    {_S1, _} = run_log(Config, S0, Entries, fun single_active_invariant/1),

    ok.
single_active_consumer_quiescing_disconnected_test(Config) ->
    S0 = init(#{name => ?FUNCTION_NAME,
                queue_resource => rabbit_misc:r("/", queue, ?FUNCTION_NAME_B),
                single_active_consumer_on => true}),

    Pid1 = test_util:fake_pid(node()),
    C1Pid = test_util:fake_pid(n1@banana),
    C2Pid = test_util:fake_pid(n2@banana),
    % % adding some consumers
    {CK1, C1} = {?LINE, {?LINE_B, C1Pid}},
    {CK2, C2} = {?LINE, {?LINE_B, C2Pid}},
    Entries =
    [
     %% add a consumer
     {CK1, make_checkout(C1, {auto, {simple_prefetch, 1}}, #{priority => 1})},
     ?ASSERT(#rabbit_fifo{consumers = #{CK1 := #consumer{status = up}},
                          waiting_consumers = []}),

     %% enqueue a message
     {?LINE , rabbit_fifo:make_enqueue(Pid1, 1, msg1)},

     %% add a consumer with a higher priority, current is quiescing
     {CK2, make_checkout(C2, {auto, {simple_prefetch, 1}}, #{priority => 2})},
     ?ASSERT(#rabbit_fifo{consumers = #{CK1 := #consumer{status = quiescing}},
                          waiting_consumers = [{CK2, _}]}),
     %% C1 is disconnected,
     {?LINE, {down, C1Pid, noconnection}},
     ?ASSERT(
        #rabbit_fifo{consumers = #{CK2 := #consumer{status = up,
                                                    checked_out = Ch2}},
                     waiting_consumers =
                         [{CK1, #consumer{status = suspected_down,
                                          checked_out = Ch1}}]}
          when map_size(Ch2) == 1 andalso
               map_size(Ch1) == 0),
     %% C1 settles which will be ignored
     {?LINE, rabbit_fifo:make_settle(CK1, [0])},
     ?ASSERT(
        #rabbit_fifo{consumers = #{CK2 := #consumer{status = up,
                                                    checked_out = Ch2}},
                     waiting_consumers =
                         [{CK1, #consumer{status = suspected_down,
                                          checked_out = Ch1}}]}
          when map_size(Ch2) == 1 andalso
               map_size(Ch1) == 0),
     % %% C1 comes back up
     {?LINE, {nodeup, n1@banana}},
     ?ASSERT(
        #rabbit_fifo{consumers = #{CK2 := #consumer{status = up}},
                     waiting_consumers = [{CK1, #consumer{status = up}}]})
    ],
    {_S1, _} = run_log(Config, S0, Entries, fun single_active_invariant/1),

    ok.

single_active_consumer_quiescing_receives_no_further_messages_test(Config) ->
    S0 = init(#{name => ?FUNCTION_NAME,
                queue_resource => rabbit_misc:r("/", queue, ?FUNCTION_NAME_B),
                single_active_consumer_on => true}),

    Pid1 = test_util:fake_pid(node()),
    C1Pid = test_util:fake_pid(n1@banana),
    C2Pid = test_util:fake_pid(n2@banana),
    % % adding some consumers
    {CK1, C1} = {?LINE, {?LINE_B, C1Pid}},
    {CK2, C2} = {?LINE, {?LINE_B, C2Pid}},
    Entries =
    [
     %% add a consumer, with plenty of prefetch
     {CK1, make_checkout(C1, {auto, {simple_prefetch, 10}}, #{priority => 1})},
     ?ASSERT(#rabbit_fifo{consumers = #{CK1 := #consumer{status = up}},
                          waiting_consumers = []}),

     %% enqueue a message
     {?LINE, rabbit_fifo:make_enqueue(Pid1, 1, msg1)},

     %% add a consumer with a higher priority, current is quiescing
     {CK2, make_checkout(C2, {auto, {simple_prefetch, 10}}, #{priority => 2})},
     ?ASSERT(#rabbit_fifo{consumers = #{CK1 := #consumer{status = quiescing,
                                                         checked_out = Ch}},
                          waiting_consumers = [{CK2, _}]}
               when map_size(Ch) == 1),

     %% enqueue another message
     {?LINE, rabbit_fifo:make_enqueue(Pid1, 2, msg2)},
     %% message should not be assinged to quiescing consumer
     ?ASSERT(#rabbit_fifo{consumers = #{CK1 := #consumer{status = quiescing,
                                                         checked_out = Ch}},
                          waiting_consumers = [{CK2, _}]}
               when map_size(Ch) == 1)

    ],

    {_S1, _} = run_log(Config, S0, Entries, fun single_active_invariant/1),
    ok.

single_active_consumer_credited_favour_with_credit_test(Config) ->
    S0 = init(#{name => ?FUNCTION_NAME,
                queue_resource => rabbit_misc:r("/", queue, ?FUNCTION_NAME_B),
                single_active_consumer_on => true}),

    C1Pid = test_util:fake_pid(n1@banana),
    C2Pid = test_util:fake_pid(n2@banana),
    C3Pid = test_util:fake_pid(n3@banana),
    % % adding some consumers
    {CK1, C1} = {?LINE, {?LINE_B, C1Pid}},
    {CK2, C2} = {?LINE, {?LINE_B, C2Pid}},
    {CK3, C3} = {?LINE, {?LINE_B, C3Pid}},
    Entries =
    [
     %% add a consumer
     {CK1, make_checkout(C1, {auto, {credited, 0}}, #{priority => 3})},
     {CK2, make_checkout(C2, {auto, {credited, 0}}, #{priority => 1})},
     {CK3, make_checkout(C3, {auto, {credited, 0}}, #{priority => 1})},
     %% waiting are sorted by arrival order
     ?ASSERT(#rabbit_fifo{consumers = #{CK1 := #consumer{status = up}},
                          waiting_consumers = [{CK2, _}, {CK3, _}]}),

     %% give credit to C3
     {?LINE , rabbit_fifo:make_credit(CK3, 1, 0, false)},
     ?ASSERT(#rabbit_fifo{consumers = #{CK1 := #consumer{status = up}},
                          waiting_consumers = [{CK3, _}, {CK2, _}]}),
     %% cancel the current active consumer
     {CK1, make_checkout(C1, cancel, #{})},
     %% C3 should become active due having credits
     ?ASSERT(#rabbit_fifo{consumers = #{CK3 := #consumer{status = up,
                                                         credit = 1}},
                          waiting_consumers = [{CK2, _}]})
    ],

    {_S1, _} = run_log(Config, S0, Entries, fun single_active_invariant/1),
    ok.



register_enqueuer_test(Config) ->
    State0 = init(#{name => ?FUNCTION_NAME,
                    queue_resource => rabbit_misc:r("/", queue, ?FUNCTION_NAME_B),
                    max_length => 2,
                    max_in_memory_length => 0,
                    overflow_strategy => reject_publish}),
    %% simply registering should be ok when we're below limit
    Pid1 = test_util:fake_pid(node()),
    {State1, ok, [_]} = apply(meta(Config, 1, ?LINE, {notify, 1, Pid1}),
                              make_register_enqueuer(Pid1), State0),

    {State2, ok, _} = apply(meta(Config, 2, ?LINE, {notify, 2, Pid1}),
                            rabbit_fifo:make_enqueue(Pid1, 1, one), State1),
    %% register another enqueuer shoudl be ok
    Pid2 = test_util:fake_pid(node()),
    {State3, ok, [_]} = apply(meta(Config, 3, ?LINE, {notify, 3, Pid2}),
                              make_register_enqueuer(Pid2), State2),

    {State4, ok, _} = apply(meta(Config, 4, ?LINE, {notify, 4, Pid1}),
                            rabbit_fifo:make_enqueue(Pid1, 2, two), State3),
    {State5, ok, Efx} = apply(meta(Config, 5, ?LINE, {notify, 4, Pid1}),
                              rabbit_fifo:make_enqueue(Pid1, 3, three), State4),
    %% validate all registered enqueuers are notified of overflow state
    ?ASSERT_EFF({send_msg, P, {queue_status, reject_publish}, [ra_event]},
                P == Pid1, Efx),
    ?ASSERT_EFF({send_msg, P, {queue_status, reject_publish}, [ra_event]},
                P == Pid2, Efx),

    %% this time, registry should return reject_publish
    {State6, reject_publish, [_]} =
        apply(meta(Config, 6), make_register_enqueuer(
                                 test_util:fake_pid(node())), State5),
    ?assertMatch(#{num_enqueuers := 3}, rabbit_fifo:overview(State6)),

    Pid3 = test_util:fake_pid(node()),
    %% remove two messages this should make the queue fall below the 0.8 limit
    {State7, _, Efx7} =
        apply(meta(Config, 7),
              rabbit_fifo:make_checkout({<<"a">>, Pid3}, {dequeue, settled}, #{}),
              State6),
    ?ASSERT_EFF({log, [_], _}, Efx7),
    {State8, _, Efx8} =
        apply(meta(Config, 8),
              rabbit_fifo:make_checkout({<<"a">>, Pid3}, {dequeue, settled}, #{}),
              State7),
    ?ASSERT_EFF({log, [_], _}, Efx8),
    %% validate all registered enqueuers are notified of overflow state
    ?ASSERT_EFF({send_msg, P, {queue_status, go}, [ra_event]}, P == Pid1, Efx8),
    ?ASSERT_EFF({send_msg, P, {queue_status, go}, [ra_event]}, P == Pid2, Efx8),
    {_State9, _, Efx9} =
        apply(meta(Config, 9),
              rabbit_fifo:make_checkout({<<"a">>, Pid3}, {dequeue, settled}, #{}),
              State8),
    ?ASSERT_EFF({log, [_], _}, Efx9),
    ?ASSERT_NO_EFF({send_msg, P, go, [ra_event]}, P == Pid1, Efx9),
    ?ASSERT_NO_EFF({send_msg, P, go, [ra_event]}, P == Pid2, Efx9),
    ok.

reject_publish_purge_test(Config) ->
    State0 = init(#{name => ?FUNCTION_NAME,
                    queue_resource => rabbit_misc:r("/", queue, ?FUNCTION_NAME_B),
                    max_length => 2,
                    max_in_memory_length => 0,
                    overflow_strategy => reject_publish}),
    %% simply registering should be ok when we're below limit
    Pid1 = test_util:fake_pid(node()),
    {State1, ok, [_]} = apply(meta(Config, 1), make_register_enqueuer(Pid1), State0),
    {State2, ok, _} = apply(meta(Config, 2, ?LINE, {notify, 2, Pid1}),
                            rabbit_fifo:make_enqueue(Pid1, 1, one), State1),
    {State3, ok, _} = apply(meta(Config, 3, ?LINE, {notify, 2, Pid1}),
                            rabbit_fifo:make_enqueue(Pid1, 2, two), State2),
    {State4, ok, Efx} = apply(meta(Config, 4, ?LINE, {notify, 2, Pid1}),
                              rabbit_fifo:make_enqueue(Pid1, 3, three), State3),
    % ct:pal("Efx ~tp", [Efx]),
    ?ASSERT_EFF({send_msg, P, {queue_status, reject_publish}, [ra_event]}, P == Pid1, Efx),
    {_State5, {purge, 3}, Efx1} = apply(meta(Config, 5), rabbit_fifo:make_purge(), State4),
    ?ASSERT_EFF({send_msg, P, {queue_status, go}, [ra_event]}, P == Pid1, Efx1),
    ok.

reject_publish_applied_after_limit_test(Config) ->
    QName = rabbit_misc:r("/", queue, ?FUNCTION_NAME_B),
    InitConf = #{name => ?FUNCTION_NAME,
                 max_in_memory_length => 0,
                 queue_resource => QName
                },
    State0 = init(InitConf),
    %% simply registering should be ok when we're below limit
    Pid1 = test_util:fake_pid(node()),
    {State1, ok, [_]} = apply(meta(Config, 1, ?LINE, {notify, 1, Pid1}),
                              make_register_enqueuer(Pid1), State0),
    {State2, ok, _} = apply(meta(Config, 2, ?LINE, {notify, 1, Pid1}),
                            rabbit_fifo:make_enqueue(Pid1, 1, one), State1),
    {State3, ok, _} = apply(meta(Config, 3, ?LINE, {notify, 1, Pid1}),
                            rabbit_fifo:make_enqueue(Pid1, 2, two), State2),
    {State4, ok, Efx} = apply(meta(Config, 4, ?LINE, {notify, 1, Pid1}),
                              rabbit_fifo:make_enqueue(Pid1, 3, three), State3),
    ?ASSERT_NO_EFF({send_msg, P, {queue_status, reject_publish}, [ra_event]},
                   P == Pid1, Efx),
    %% apply new config
    Conf = #{name => ?FUNCTION_NAME,
             queue_resource => QName,
             max_length => 2,
             overflow_strategy => reject_publish,
             dead_letter_handler => undefined
            },
    {State5, ok, Efx1} = apply(meta(Config, 5), rabbit_fifo:make_update_config(Conf), State4),
    ?ASSERT_EFF({send_msg, P, {queue_status, reject_publish}, [ra_event]},
                P == Pid1, Efx1),
    Pid2 = test_util:fake_pid(node()),
    {_State6, reject_publish, _} =
        apply(meta(Config, 1), make_register_enqueuer(Pid2), State5),
    ok.

update_config_delivery_limit_test(Config) ->
    QName = rabbit_misc:r("/", queue, ?FUNCTION_NAME_B),
    InitConf = #{name => ?FUNCTION_NAME,
                 queue_resource => QName,
                 delivery_limit => 20
                },
    State0 = init(InitConf),
    ?assertMatch(#{config := #{delivery_limit := 20}},
                 rabbit_fifo:overview(State0)),

    %% A delivery limit of -1 (or any negative value) turns the delivery_limit
    %% off
    Conf = #{name => ?FUNCTION_NAME,
             queue_resource => QName,
             delivery_limit => -1,
             dead_letter_handler => undefined
            },
    {State1, ok, _} = apply(meta(Config, ?LINE),
                            rabbit_fifo:make_update_config(Conf), State0),

    ?assertMatch(#{config := #{delivery_limit := undefined}},
                 rabbit_fifo:overview(State1)),

    ok.

update_config_max_length_test(Config) ->
    QName = rabbit_misc:r("/", queue, ?FUNCTION_NAME_B),
    InitConf = #{name => ?FUNCTION_NAME,
                 queue_resource => QName,
                 delivery_limit => 20
                },
    State0 = init(InitConf),
    ?assertMatch(#{config := #{delivery_limit := 20}},
                 rabbit_fifo:overview(State0)),

    State1 = lists:foldl(fun (Num, FS0) ->
                         {FS, _} = enq(Config, Num, Num, Num, FS0),
                         FS
                     end, State0, lists:seq(1, 100)),
    Conf = #{name => ?FUNCTION_NAME,
             queue_resource => QName,
             max_length => 2,
             dead_letter_handler => undefined},
    %% assert only one global counter effect is generated rather than 1 per
    %% dropped message
    {State, ok, Effects} = apply(meta(Config, ?LINE),
                                 rabbit_fifo:make_update_config(Conf), State1),
    ?assertMatch([{mod_call, rabbit_global_counters, messages_dead_lettered,
                   [maxlen, rabbit_quorum_queue,disabled, 98]}], Effects),
    ?assertMatch(#{config := #{max_length := 2},
                   num_ready_messages := 2}, rabbit_fifo:overview(State)),
    ok.

purge_nodes_test(Config) ->
    Node = purged@node,
    ThisNode = node(),
    EnqPid = test_util:fake_pid(Node),
    EnqPid2 = test_util:fake_pid(node()),
    ConPid = test_util:fake_pid(Node),
    Cid = {<<"tag">>, ConPid},

    State0 = init(#{name => ?FUNCTION_NAME,
                    queue_resource => rabbit_misc:r("/", queue, ?FUNCTION_NAME_B),
                    single_active_consumer_on => false}),
    {State1, _, _} = apply(meta(Config, 1, ?LINE, {notify, 1, EnqPid}),
                           rabbit_fifo:make_enqueue(EnqPid, 1, msg1),
                           State0),
    {State2, _, _} = apply(meta(Config, 2, ?LINE, {notify, 2, EnqPid2}),
                           rabbit_fifo:make_enqueue(EnqPid2, 1, msg2),
                           State1),
    {State3, _} = check(Config, Cid, 3, 1000, State2),
    {State4, _, _} = apply(meta(Config, ?LINE),
                           {down, EnqPid, noconnection},
                           State3),
    ?assertMatch([{aux, {handle_tick,
                         [#resource{}, _Metrics,
                          [ThisNode, Node]]}}],
                 rabbit_fifo:tick(1, State4)),
    %% assert there are both enqueuers and consumers
    {State, _, _} = apply(meta(Config, ?LINE),
                          rabbit_fifo:make_purge_nodes([Node]),
                          State4),

    %% assert there are no enqueuers nor consumers
    ?assertMatch(#rabbit_fifo{enqueuers = Enqs}
                   when map_size(Enqs) == 1, State),
    ?assertMatch(#rabbit_fifo{consumers = Cons}
                   when map_size(Cons) == 0, State),
    ?assertMatch([{aux, {handle_tick, [#resource{}, _Metrics, [ThisNode]]}}],
                 rabbit_fifo:tick(1, State)),
    ok.

meta(Config, Idx) ->
    meta(Config, Idx, 0).

meta(Config, Idx, Timestamp) ->
    meta(Config, Idx, Timestamp, no_reply).

meta(Config, Idx, Timestamp, ReplyMode) ->
    #{machine_version => ?config(machine_version, Config),
      index => Idx,
      term => 1,
      system_time => Timestamp,
      reply_mode => ReplyMode,
      from => {make_ref(), self()}}.

enq(Config, Idx, MsgSeq, Msg, State) ->
    strip_reply(
      apply(meta(Config, Idx, 0, {notify, MsgSeq, self()}),
            rabbit_fifo:make_enqueue(self(), MsgSeq, Msg),
            State)).

deq(Config, Idx, Cid, Settlement, Msg, State0) ->
    {State, _, Effs} =
        apply(meta(Config, Idx),
              rabbit_fifo:make_checkout(Cid, {dequeue, Settlement}, #{}),
              State0),
        {value, {log, [_Idx], Fun}} = lists:search(fun(E) ->
                                                           element(1, E) == log
                                                   end, Effs),
    [{reply, _From,
      {wrap_reply, {dequeue, {MsgId, _}, _}}}] = Fun([Msg]),

    {State, {MsgId, Msg}}.

check_n(Config, Cid, Idx, N, State) ->
    strip_reply(
      apply(meta(Config, Idx),
            rabbit_fifo:make_checkout(Cid, {auto, N, simple_prefetch}, #{}),
            State)).

check(Config, Cid, Idx, State) ->
    strip_reply(
      apply(meta(Config, Idx),
            rabbit_fifo:make_checkout(Cid, {once, 1, simple_prefetch}, #{}),
            State)).

check_auto(Config, Cid, Idx, State) ->
    strip_reply(
      apply(meta(Config, Idx),
            rabbit_fifo:make_checkout(Cid, {auto, 1, simple_prefetch}, #{}),
            State)).

check(Config, Cid, Idx, Num, State) ->
    strip_reply(
      apply(meta(Config, Idx),
            rabbit_fifo:make_checkout(Cid, {auto, Num, simple_prefetch}, #{}),
            State)).

checkout(Config, Idx, Cid, Credit, State)
  when is_integer(Credit) ->
    checkout(Config, Idx, Cid, {auto, {simple_prefetch, Credit}}, State);
checkout(Config, Idx, Cid, Spec, State) ->
    checkout_reply(
      apply(meta(Config, Idx),
            rabbit_fifo:make_checkout(Cid, Spec, #{}),
            State)).

settle(Config, Cid, Idx, MsgId, State) when is_integer(MsgId) ->
    settle(Config, Cid, Idx, [MsgId], State);
settle(Config, Cid, Idx, MsgIds, State) when is_list(MsgIds) ->
    strip_reply(apply(meta(Config, Idx),
                      rabbit_fifo:make_settle(Cid, MsgIds), State)).

return(Config, Cid, Idx, MsgId, State) ->
    strip_reply(apply(meta(Config, Idx), rabbit_fifo:make_return(Cid, [MsgId]), State)).

credit(Config, Cid, Idx, Credit, DelCnt, Drain, State) ->
    strip_reply(apply(meta(Config, Idx), rabbit_fifo:make_credit(Cid, Credit, DelCnt, Drain),
                      State)).

strip_reply({State, _, Effects}) ->
    {State, Effects}.

checkout_reply({State, {ok, CInfo}, Effects}) when is_map(CInfo) ->
    {State, CInfo, Effects};
checkout_reply(Oth) ->
    Oth.

run_log(Config, InitState, Entries) ->
    run_log(rabbit_fifo, Config, InitState, Entries, fun (_) -> true end).

run_log(Config, InitState, Entries, Invariant) ->
    run_log(rabbit_fifo, Config, InitState, Entries, Invariant).

run_log(Module, Config, InitState, Entries, Invariant) ->
    lists:foldl(
      fun ({assert, Fun}, {Acc0, Efx0}) ->
              _ = Fun(Acc0),
              {Acc0, Efx0};
          ({Idx, E}, {Acc0, Efx0}) ->
              case Module:apply(meta(Config, Idx, Idx, {notify, Idx, self()}),
                                E, Acc0) of
                  {Acc, _, Efx} when is_list(Efx) ->
                      ?assert(Invariant(Acc)),
                      {Acc, Efx0 ++ Efx};
                  {Acc, _, Efx}  ->
                      ?assert(Invariant(Acc)),
                      {Acc, Efx0 ++ [Efx]};
                  {Acc, _}  ->
                      ?assert(Invariant(Acc)),
                      {Acc, Efx0}
              end
      end, {InitState, []}, Entries).


%% AUX Tests

aux_test(_) ->
    _ = ra_machine_ets:start_link(),
    Aux0 = init_aux(aux_test),
    LastApplied = 0,
    State0 = #{machine_state =>
               init(#{name => ?FUNCTION_NAME,
                      queue_resource => rabbit_misc:r("/", queue, ?FUNCTION_NAME_B),
                      single_active_consumer_on => false}),
               log => mock_log,
               last_applied => LastApplied},
    ok = meck:new(ra_log, []),
    meck:expect(ra_log, last_index_term, fun (_) -> {0, 0} end),
    {no_reply, Aux, State} = handle_aux(leader, cast, active, Aux0, State0),
    {no_reply, _Aux, _,
     [{release_cursor, LastApplied}]} = handle_aux(leader, cast, tick, Aux, State),
    [X] = ets:lookup(rabbit_fifo_usage, aux_test),
    meck:unload(),
    ?assert(X > 0.0),
    ok.

handle_aux_tick_test(Config) ->
    _ = ra_machine_ets:start_link(),
    Aux0 = init_aux(aux_test),
    LastApplied = 1,
    MacState0 = init(#{name => ?FUNCTION_NAME,
                       queue_resource => rabbit_misc:r("/", queue, ?FUNCTION_NAME_B),
                       single_active_consumer_on => false}),
    State0 = #{machine_state => MacState0,
               log => mock_log,
               last_applied => LastApplied},
    {MacState1, _} = enq(Config, 1, 1, first, MacState0),
    State1 = State0#{machine_state => MacState1},
    meck:expect(ra_log, last_index_term, fun (_) -> {1, 0} end),
    ?assertEqual(1, rabbit_fifo:smallest_raft_index(MacState1)),
    %% the release cursor should be 1 lower than the smallest raft index
    {no_reply, _, _,
     [{release_cursor, 0}]} = handle_aux(leader, cast, tick, Aux0, State1),
    timer:sleep(10),

    persistent_term:put(quorum_queue_checkpoint_config, {1, 0, 1}),
    {no_reply, _, _,
     [{checkpoint, 1, _},
      {release_cursor, 0}]} = handle_aux(follower, cast, force_checkpoint, Aux0, State1),
    ok.


%% machine version conversion test

machine_version_test(C) ->
    V0 = rabbit_fifo_v0,
    S0 = V0:init(#{name => ?FUNCTION_NAME,
                   queue_resource => rabbit_misc:r(<<"/">>, queue, <<"test">>)}),
    Idx = 1,
    {#rabbit_fifo{}, ok, _} = apply(meta(C, Idx), {machine_version, 0, 2}, S0),

    Cid = {atom_to_binary(?FUNCTION_NAME, utf8), self()},
    Entries = [
               {1, rabbit_fifo_v0:make_enqueue(self(), 1, banana)},
               {2, rabbit_fifo_v0:make_enqueue(self(), 2, apple)},
               {3, rabbit_fifo_v0:make_checkout(Cid, {auto, 1, unsettled}, #{})}
              ],
    {S1, _Effects} = rabbit_fifo_v0_SUITE:run_log(S0, Entries),
    Self = self(),
    {#rabbit_fifo{enqueuers = #{Self := #enqueuer{}},
                  consumers = #{Cid := #consumer{cfg = #consumer_cfg{priority = 0}}},
                  service_queue = S,
                  messages = Msgs}, ok,
     [_|_]} = apply(meta(C, Idx), {machine_version, 0, 2}, S1),
    %% validate message conversion to lqueue
    ?assertEqual(1, lqueue:len(Msgs)),
    ?assert(priority_queue:is_queue(S)),
    ok.

machine_version_waiting_consumer_test(C) ->
    V0 = rabbit_fifo_v0,
    S0 = V0:init(#{name => ?FUNCTION_NAME,
                   queue_resource => rabbit_misc:r(<<"/">>, queue, <<"test">>)}),
    Idx = 1,
    {#rabbit_fifo{}, ok, _} = apply(meta(C, Idx), {machine_version, 0, 2}, S0),

    Cid = {atom_to_binary(?FUNCTION_NAME, utf8), self()},
    Entries = [
               {1, rabbit_fifo_v0:make_enqueue(self(), 1, banana)},
               {2, rabbit_fifo_v0:make_enqueue(self(), 2, apple)},
               {3, rabbit_fifo_v0:make_checkout(Cid, {auto, 5, unsettled}, #{})}
              ],
    {S1, _Effects} = rabbit_fifo_v0_SUITE:run_log(S0, Entries),
    Self = self(),
    {#rabbit_fifo{enqueuers = #{Self := #enqueuer{}},
                  consumers = #{Cid := #consumer{cfg = #consumer_cfg{priority = 0}}},
                  service_queue = S,
                  messages = Msgs}, ok, _} = apply(meta(C, Idx),
                                                    {machine_version, 0, 2}, S1),
    %% validate message conversion to lqueue
    ?assertEqual(0, lqueue:len(Msgs)),
    ?assert(priority_queue:is_queue(S)),
    ?assertEqual(1, priority_queue:len(S)),
    ok.

convert_v2_to_v3(Config) ->
    ConfigV2 = [{machine_version, 2} | Config],
    ConfigV3 = [{machine_version, 3} | Config],

    Cid1 = {ctag1, self()},
    Cid2 = {ctag2, self()},
    MaxCredits = 20,
    Entries = [{1, make_checkout(Cid1, {auto, 10, credited}, #{})},
               {2, make_checkout(Cid2, {auto, MaxCredits, simple_prefetch},
                                 #{prefetch => MaxCredits})}],

    %% run log in v2
    {State, _} = run_log(ConfigV2, test_init(?FUNCTION_NAME), Entries),

    %% convert from v2 to v3
    {#rabbit_fifo{consumers = Consumers}, ok, _} =
        apply(meta(ConfigV3, 3), {machine_version, 2, 3}, State),

    ?assertEqual(2, maps:size(Consumers)),
    ?assertMatch(#consumer{cfg = #consumer_cfg{credit_mode = {simple_prefetch, MaxCredits}}},
                 maps:get(Cid2, Consumers)),
    ok.

convert_v3_to_v4(Config) ->
    ConfigV3 = [{machine_version, 3} | Config],
    ConfigV4 = [{machine_version, 4} | Config],

    EPid = test_util:fake_pid(node()),
    Pid1 = test_util:fake_pid(node()),
    Cid1 = {ctag1, Pid1},
    Cid2 = {ctag2, self()},
    MaxCredits = 2,
    Entries = [
               {1, rabbit_fifo_v3:make_enqueue(EPid, 1, banana)},
               {2, rabbit_fifo_v3:make_enqueue(EPid, 2, apple)},
               {3, rabbit_fifo_v3:make_enqueue(EPid, 3, orange)},
               {4, make_checkout(Cid1, {auto, 10, credited}, #{})},
               {5, make_checkout(Cid2, {auto, MaxCredits, simple_prefetch},
                                 #{prefetch => MaxCredits})},
               {6, {down, Pid1, error}}],

    %% run log in v3
    Name = ?FUNCTION_NAME,
    Init = rabbit_fifo_v3:init(
             #{name => Name,
               queue_resource => rabbit_misc:r("/", queue, atom_to_binary(Name)),
               release_cursor_interval => 0}),
    {State, _} = run_log(rabbit_fifo_v3, ConfigV3, Init, Entries,
                         fun (_) -> true end),

    %% convert from v3 to v4
    {#rabbit_fifo{consumers = Consumers,
                  returns = Returns}, ok, _} =
        apply(meta(ConfigV4, ?LINE), {machine_version, 3, 4}, State),

    ?assertEqual(1, maps:size(Consumers)),
    ?assertMatch(#consumer{cfg = #consumer_cfg{credit_mode =
                                               {simple_prefetch, MaxCredits}}},
                 maps:get(Cid2, Consumers)),
    ?assertNot(is_map_key(Cid1, Consumers)),
    %% assert delivery_count is copied to acquired_count
    #consumer{checked_out = Ch2} = maps:get(Cid2, Consumers),
    ?assertMatch(#{0 := ?MSG(_, #{delivery_count := 1,
                                  acquired_count := 1}),
                   1 := ?MSG(_, #{delivery_count := 1,
                                  acquired_count := 1})}, Ch2),

    ?assertMatch(?MSG(_, #{delivery_count := 1,
                           acquired_count := 1}), lqueue:get(Returns)),

    ok.

queue_ttl_test(C) ->
    QName = rabbit_misc:r(<<"/">>, queue, <<"test">>),
    Conf = #{name => ?FUNCTION_NAME,
             queue_resource => QName,
             created => 1000,
             expires => 1000},
    S0 = rabbit_fifo:init(Conf),
    Now = 1500,
    [{aux, {handle_tick, [_, _, _]}}] = rabbit_fifo:tick(Now, S0),
    %% this should delete the queue
    [{mod_call, rabbit_quorum_queue, spawn_deleter, [QName]}]
        = rabbit_fifo:tick(Now + 1000, S0),
    %% adding a consumer should not ever trigger deletion
    Cid = {<<"cid1">>, self()},
    {S1, _} = check_auto(C, Cid, 1, S0),
    [{aux, {handle_tick, [_, _, _]}}] = rabbit_fifo:tick(Now, S1),
    [{aux, {handle_tick, [_, _, _]}}] = rabbit_fifo:tick(Now + 1000, S1),
    %% cancelling the consumer should then
    {S2, _, _} = apply(meta(C, 2, Now),
                       make_checkout(Cid, cancel, #{}), S1),
    %% last_active should have been reset when consumer was cancelled
    %% last_active = 2500
    [{aux, {handle_tick, [_, _, _]}}] = rabbit_fifo:tick(Now + 1000, S2),
    %% but now it should be deleted
    [{mod_call, rabbit_quorum_queue, spawn_deleter, [QName]}]
        = rabbit_fifo:tick(Now + 2500, S2),

    %% Same for downs
    {S2D, _, _} = apply(meta(C, 2, Now),
                        {down, self(), noconnection}, S1),
    %% last_active should have been reset when consumer was cancelled
    %% last_active = 2500
    [{aux, {handle_tick, [_, _, _]}}] = rabbit_fifo:tick(Now + 1000, S2D),
    %% but now it should be deleted
    [{mod_call, rabbit_quorum_queue, spawn_deleter, [QName]}]
        = rabbit_fifo:tick(Now + 2500, S2D),

    %% dequeue should set last applied
    {S1Deq, {dequeue, empty}, _} =
        apply(meta(C, 2, Now),
              make_checkout(Cid, {dequeue, unsettled}, #{}),
              S0),

    [{aux, {handle_tick, [_, _, _]}}] = rabbit_fifo:tick(Now + 1000, S1Deq),
    %% but now it should be deleted
    [{mod_call, rabbit_quorum_queue, spawn_deleter, [QName]}]
        = rabbit_fifo:tick(Now + 2500, S1Deq),
    %% Enqueue message,
    Msg = rabbit_fifo:make_enqueue(self(), 1, msg1),
    {E1, _, _} = apply(meta(C, 2, Now, {notify, 2, self()}), Msg, S0),
    Deq = {<<"deq1">>, self()},
    {E2, _, Effs2} =
        apply(meta(C, 3, Now),
              make_checkout(Deq, {dequeue, unsettled}, #{}),
              E1),

    {log, [2], Fun2} = get_log_eff(Effs2),
    [{reply, _From,
      {wrap_reply, {dequeue, {MsgId, _}, _}}}] = Fun2([Msg]),
    {E3, _, _} = apply(meta(C, 3, Now + 1000),
                       rabbit_fifo:make_settle(Deq, [MsgId]), E2),
    [{aux, {handle_tick, [_, _, _]}}] = rabbit_fifo:tick(Now + 1500, E3),
    %% but now it should be deleted
    [{mod_call, rabbit_quorum_queue, spawn_deleter, [QName]}]
        = rabbit_fifo:tick(Now + 3000, E3),
    ok.

queue_ttl_with_single_active_consumer_test(Config) ->
    QName = rabbit_misc:r(<<"/">>, queue, <<"test">>),
    Conf = #{name => ?FUNCTION_NAME,
             queue_resource => QName,
             created => 1000,
             expires => 1000,
             single_active_consumer_on => true},
    S0 = rabbit_fifo:init(Conf),
    Now = 1500,
    [{aux, {handle_tick, [_, _, _]}}] = rabbit_fifo:tick(Now, S0),
    %% this should delete the queue
    [{mod_call, rabbit_quorum_queue, spawn_deleter, [QName]}]
        = rabbit_fifo:tick(Now + 1000, S0),
    %% adding a consumer should not ever trigger deletion
    Cid = {<<"cid1">>, self()},
    {S1, _, _} = checkout(Config, ?LINE, Cid, 1, S0),
    [{aux, {handle_tick, [_, _, _]}}] = rabbit_fifo:tick(Now, S1),
    [{aux, {handle_tick, [_, _, _]}}] = rabbit_fifo:tick(Now + 1000, S1),
    %% cancelling the consumer should then
    {S2, _, _} = apply(meta(Config, ?LINE, Now),
                       make_checkout(Cid, cancel, #{}), S1),
    %% last_active should have been reset when consumer was cancelled
    %% last_active = 2500
    [{aux, {handle_tick, [_, _, _]}}] = rabbit_fifo:tick(Now + 1000, S2),
    %% but now it should be deleted
    [{mod_call, rabbit_quorum_queue, spawn_deleter, [QName]}]
        = rabbit_fifo:tick(Now + 2500, S2),
    %% Same for downs
    {S2D, _, _} = apply(meta(Config, ?LINE, Now),
                        {down, self(), noconnection}, S1),
    %% last_active should have been reset when consumer was cancelled
    %% last_active = 2500
    [{aux, {handle_tick, [_, _, _]}}] = rabbit_fifo:tick(Now + 1000, S2D),
    %% but now it should be deleted
    [{mod_call, rabbit_quorum_queue, spawn_deleter, [QName]}]
        = rabbit_fifo:tick(Now + 2500, S2D),
    ok.

query_peek_test(Config) ->
    State0 = test_init(test),
    ?assertEqual({error, no_message_at_pos}, rabbit_fifo:query_peek(1, State0)),
    {State1, _} = enq(Config, 1, 1, first, State0),
    {State2, _} = enq(Config, 2, 2, second, State1),
    ?assertMatch({ok, [1 | _]}, rabbit_fifo:query_peek(1, State1)),
    ?assertEqual({error, no_message_at_pos}, rabbit_fifo:query_peek(2, State1)),
    ?assertMatch({ok, [1 | _]}, rabbit_fifo:query_peek(1, State2)),
    ?assertMatch({ok, [2 | _]}, rabbit_fifo:query_peek(2, State2)),
    ?assertEqual({error, no_message_at_pos}, rabbit_fifo:query_peek(3, State2)),
    ok.

checkout_priority_test(Config) ->
    Cid = {<<"checkout_priority_test">>, self()},
    Pid = spawn(fun () -> ok end),
    Cid2 = {<<"checkout_priority_test2">>, Pid},
    Args = [{<<"x-priority">>, long, 1}],
    {S1, _, _} =
        apply(meta(Config, ?LINE),
              make_checkout(Cid, {auto, {simple_prefetch, 2}},
                            #{args => Args}),
              test_init(test)),
    {S2, _, _} =
        apply(meta(Config, ?LINE),
              make_checkout(Cid2, {auto, {simple_prefetch, 2}},
                            #{args => []}),
              S1),
    {S3, E3} = enq(Config, ?LINE, 1, first, S2),
    ?ASSERT_EFF({send_msg, P, {delivery, _, _}, _}, P == self(), E3),
    {S4, E4} = enq(Config, ?LINE, 2, second, S3),
    ?ASSERT_EFF({send_msg, P, {delivery, _, _}, _}, P == self(), E4),
    {_S5, E5} = enq(Config, ?LINE, 3, third, S4),
    ?ASSERT_EFF({send_msg, P, {delivery, _, _}, _}, P == Pid, E5),
    ok.

header_test(_) ->
    H0 = Size = 5,
    ?assertEqual(Size, rabbit_fifo:get_header(size, H0)),
    ?assertEqual(undefined, rabbit_fifo:get_header(expiry, H0)),
    ?assertEqual(undefined, rabbit_fifo:get_header(delivery_count, H0)),

    H1 = rabbit_fifo:update_header(delivery_count, fun(C) -> C+1 end, 1, H0),
    ?assertEqual(#{size => Size,
                   delivery_count => 1}, H1),
    ?assertEqual(Size, rabbit_fifo:get_header(size, H1)),
    ?assertEqual(undefined, rabbit_fifo:get_header(expiry, H1)),
    ?assertEqual(1, rabbit_fifo:get_header(delivery_count, H1)),

    Expiry = 1000,
    H2 = rabbit_fifo:update_header(expiry, fun(Ts) -> Ts end, Expiry, H0),
    ?assertEqual([Size | Expiry], H2),
    ?assertEqual(Size, rabbit_fifo:get_header(size, H2)),
    ?assertEqual(Expiry, rabbit_fifo:get_header(expiry, H2)),
    ?assertEqual(undefined, rabbit_fifo:get_header(delivery_count, H2)),

    H3 = rabbit_fifo:update_header(delivery_count, fun(C) -> C+1 end, 1, H2),
    ?assertEqual(#{size => Size,
                   expiry => Expiry,
                   delivery_count => 1}, H3),
    ?assertEqual(Size, rabbit_fifo:get_header(size, H3)),
    ?assertEqual(Expiry, rabbit_fifo:get_header(expiry, H3)),
    ?assertEqual(1, rabbit_fifo:get_header(delivery_count, H3)),

    H4 = rabbit_fifo:update_header(delivery_count, fun(C) -> C+1 end, 1, H3),
    ?assertEqual(#{size => Size,
                   expiry => Expiry,
                   delivery_count => 2}, H4),
    ?assertEqual(2, rabbit_fifo:get_header(delivery_count, H4)),

    H5 = rabbit_fifo:update_header(expiry, fun(Ts) -> Ts end, Expiry, H1),
    ?assertEqual(#{size => Size,
                   expiry => Expiry,
                   delivery_count => 1}, H5),
    ?assertEqual(Size, rabbit_fifo:get_header(size, H5)),
    ?assertEqual(Expiry, rabbit_fifo:get_header(expiry, H5)),
    ?assertEqual(1, rabbit_fifo:get_header(delivery_count, H5)),
    ?assertEqual(undefined, rabbit_fifo:get_header(blah, H5)),
    ok.

chunk_disk_msgs_test(_Config) ->
    %% NB: this does test an internal function
    %% input to this function is a reversed list of MSGs
    Input = [{I, ?MSG(I, 1000)} || I <- lists:seq(200, 1, -1)],
    Chunks = rabbit_fifo:chunk_disk_msgs(Input, 0, [[]]),
    ?assertMatch([_, _], Chunks),
    [Chunk1, Chunk2] = Chunks,
    ?assertMatch([{1, ?MSG(1, 1000)} | _], Chunk1),
    %% the chunks are worked out in backwards order, hence the first chunk
    %% will be a "remainder" chunk
    ?assertMatch([{73, ?MSG(73, 1000)} | _], Chunk2),
    ?assertEqual(128, length(Chunk2)),
    ?assertEqual(72, length(Chunk1)),

    TwoBigMsgs = [{124, ?MSG(124, 200_000)},
                  {123, ?MSG(123, 200_000)}],
    ?assertMatch([[{123, ?MSG(123, 200_000)}],
                  [{124, ?MSG(124, 200_000)}]],
                 rabbit_fifo:chunk_disk_msgs(TwoBigMsgs, 0, [[]])),
    ok.

checkout_metadata_test(Config) ->
    Cid = {<<"cid">>, self()},
    {State00, _} = enq(Config, 1, 1, first, test_init(test)),
    {State0, _} = enq(Config, 2, 2, second, State00),
    %% NB: the consumer meta data is taken _before_ it runs a checkout
    %% so in this case num_checked_out will be 0
    {State1, #{next_msg_id := 0,
               num_checked_out := 0}, _} =
        checkout(Config, ?LINE, Cid, 1, State0),
    {State2, _, _} = apply(meta(Config, ?LINE),
                           make_checkout(Cid, cancel, #{}), State1),
    {_State3, #{next_msg_id := 1,
                num_checked_out := 1}, _} =
        checkout(Config, ?LINE, Cid, 1, State2),
    ok.

modify_test(Config) ->
    S0 = init(#{name => ?FUNCTION_NAME,
                dead_letter_handler => at_least_once,
                queue_resource =>
                    rabbit_misc:r("/", queue, ?FUNCTION_NAME_B)}),

    Pid1 = test_util:fake_pid(node()),
    % % adding some consumers
    E1Idx = ?LINE,
    {CK1, C1} = {?LINE, {?LINE_B, self()}},
    Entries =
    [
     {E1Idx , rabbit_fifo:make_enqueue(Pid1, 1, msg1)},
     %% add a consumer
     {CK1, make_checkout(C1, {auto, {simple_prefetch, 1}}, #{})},
     ?ASSERT(#rabbit_fifo{consumers = #{CK1 := #consumer{next_msg_id = 1,
                                                         checked_out = Ch}}}
               when map_size(Ch) == 1),
     %% delivery_failed = false, undeliverable_here = false|true
     %% this is the same as a requeue,
     %% this should not increment the delivery count
     {?LINE, rabbit_fifo:make_modify(CK1, [0], false, false,
                                     #{<<"x-opt-blah">> => <<"blah1">>})},
     ?ASSERT(#rabbit_fifo{consumers = #{CK1 := #consumer{next_msg_id = 2,
                                                         checked_out = Ch}}}
               when map_size(Ch) == 1,
               fun (#rabbit_fifo{consumers =
                                 #{CK1 := #consumer{checked_out = Ch}}}) ->
                       ?assertMatch(
                          ?MSG(_, #{acquired_count := 1,
                                    anns := #{<<"x-opt-blah">> := <<"blah1">>}} = H)
                            when not is_map_key(delivery_count, H),
                          maps:get(1, Ch))
               end),
     %% delivery_failed = true does increment delivery_count
     {?LINE, rabbit_fifo:make_modify(CK1, [1], true, false,
                                     #{<<"x-opt-blah">> => <<"blah2">>})},
     ?ASSERT(#rabbit_fifo{consumers = #{CK1 := #consumer{next_msg_id = 3,
                                                         checked_out = Ch}}}
               when map_size(Ch) == 1,
               fun (#rabbit_fifo{consumers =
                                 #{CK1 := #consumer{checked_out = Ch}}}) ->
                       ?assertMatch(
                          ?MSG(_, #{delivery_count := 1,
                                    acquired_count := 2,
                                    anns := #{<<"x-opt-blah">> := <<"blah2">>}}),
                          maps:get(2, Ch))
               end),
     %% delivery_failed = true and undeliverable_here = true is the same as discard
     {?LINE, rabbit_fifo:make_modify(CK1, [2], true, true,
                                     #{<<"x-opt-blah">> => <<"blah3">>})},
     ?ASSERT(#rabbit_fifo{consumers = #{CK1 := #consumer{next_msg_id = 3,
                                                         checked_out = Ch}}}
               when map_size(Ch) == 0,
               fun (#rabbit_fifo{dlx = #rabbit_fifo_dlx{discards = Discards}}) ->
                       ?assertMatch([[_|
                                      ?MSG(_, #{delivery_count := 2,
                                                acquired_count := 3,
                                                anns := #{<<"x-opt-blah">> := <<"blah3">>}})]],
                                    lqueue:to_list(Discards))
               end)
    ],
    {_S1, _} = run_log(Config, S0, Entries, fun single_active_invariant/1),

    ok.

%% Utility
%%

init(Conf) -> rabbit_fifo:init(Conf).
make_register_enqueuer(Pid) -> rabbit_fifo:make_register_enqueuer(Pid).
apply(Meta, Entry, State) -> rabbit_fifo:apply(Meta, Entry, State).
init_aux(Conf) -> rabbit_fifo:init_aux(Conf).
handle_aux(S, T, C, A, A2) -> rabbit_fifo:handle_aux(S, T, C, A, A2).
make_checkout(C, S, M) -> rabbit_fifo:make_checkout(C, S, M).

cid(A) when is_atom(A) ->
    atom_to_binary(A, utf8).

single_active_invariant( #rabbit_fifo{consumers = Cons}) ->
    1 >= map_size(maps:filter(fun (_, #consumer{status = S}) ->
                                      S == up
                              end, Cons)).

mk_mc(Body) ->
    mc_amqpl:from_basic_message(
      #basic_message{routing_keys = [<<"">>],
                     exchange_name = #resource{name = <<"x">>,
                                               kind = exchange,
                                               virtual_host = <<"v">>},
                     content = #content{properties = #'P_basic'{},
                                        payload_fragments_rev = [Body]}}).
