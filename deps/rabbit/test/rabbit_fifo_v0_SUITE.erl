-module(rabbit_fifo_v0_SUITE).

%% rabbit_fifo unit tests suite

-compile(export_all).

-compile({no_auto_import, [apply/3]}).
-export([
         ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include("src/rabbit_fifo_v0.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, tests}
    ].


%% replicate eunit like test resultion
all_tests() ->
    [F || {F, _} <- ?MODULE:module_info(functions),
          re:run(atom_to_list(F), "_test$") /= nomatch].

groups() ->
    [
     {tests, [], all_tests()}
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

-define(ASSERT_EFF(EfxPat, Effects),
        ?ASSERT_EFF(EfxPat, true, Effects)).

-define(ASSERT_EFF(EfxPat, Guard, Effects),
    ?assert(lists:any(fun (EfxPat) when Guard -> true;
                          (_) -> false
                      end, Effects))).

-define(ASSERT_NO_EFF(EfxPat, Effects),
    ?assert(not lists:any(fun (EfxPat) -> true;
                          (_) -> false
                      end, Effects))).

-define(assertNoEffect(EfxPat, Effects),
    ?assert(not lists:any(fun (EfxPat) -> true;
                          (_) -> false
                      end, Effects))).

test_init(Name) ->
    init(#{name => Name,
           queue_resource => rabbit_misc:r("/", queue,
                                           atom_to_binary(Name, utf8)),
           release_cursor_interval => 0}).

enq_enq_checkout_test(_) ->
    Cid = {<<"enq_enq_checkout_test">>, self()},
    {State1, _} = enq(1, 1, first, test_init(test)),
    {State2, _} = enq(2, 2, second, State1),
    {_State3, _, Effects} =
        apply(meta(3),
              rabbit_fifo_v0:make_checkout(Cid, {once, 2, simple_prefetch}, #{}),
              State2),
    ?ASSERT_EFF({monitor, _, _}, Effects),
    ?ASSERT_EFF({send_msg, _, {delivery, _, _}, _}, Effects),
    ok.

credit_enq_enq_checkout_settled_credit_test(_) ->
    Cid = {?FUNCTION_NAME, self()},
    {State1, _} = enq(1, 1, first, test_init(test)),
    {State2, _} = enq(2, 2, second, State1),
    {State3, _, Effects} =
        apply(meta(3), rabbit_fifo_v0:make_checkout(Cid, {auto, 1, credited}, #{}), State2),
    ?ASSERT_EFF({monitor, _, _}, Effects),
    Deliveries = lists:filter(fun ({send_msg, _, {delivery, _, _}, _}) -> true;
                                  (_) -> false
                              end, Effects),
    ?assertEqual(1, length(Deliveries)),
    %% settle the delivery this should _not_ result in further messages being
    %% delivered
    {State4, SettledEffects} = settle(Cid, 4, 1, State3),
    ?assertEqual(false, lists:any(fun ({send_msg, _, {delivery, _, _}, _}) ->
                                          true;
                                      (_) -> false
                                  end, SettledEffects)),
    %% granting credit (3) should deliver the second msg if the receivers
    %% delivery count is (1)
    {State5, CreditEffects} = credit(Cid, 5, 1, 1, false, State4),
    % ?debugFmt("CreditEffects  ~p ~n~p", [CreditEffects, State4]),
    ?ASSERT_EFF({send_msg, _, {delivery, _, _}, _}, CreditEffects),
    {_State6, FinalEffects} = enq(6, 3, third, State5),
    ?assertEqual(false, lists:any(fun ({send_msg, _, {delivery, _, _}, _}) ->
                                          true;
                                      (_) -> false
                                  end, FinalEffects)),
    ok.

credit_with_drained_test(_) ->
    Cid = {?FUNCTION_NAME, self()},
    State0 = test_init(test),
    %% checkout with a single credit
    {State1, _, _} =
        apply(meta(1), rabbit_fifo_v0:make_checkout(Cid, {auto, 1, credited},#{}),
              State0),
    ?assertMatch(#?STATE{consumers = #{Cid := #consumer{credit = 1,
                                                       delivery_count = 0}}},
                 State1),
    {State, Result, _} =
         apply(meta(3), rabbit_fifo_v0:make_credit(Cid, 0, 5, true), State1),
    ?assertMatch(#?STATE{consumers = #{Cid := #consumer{credit = 0,
                                                       delivery_count = 5}}},
                 State),
    ?assertEqual({multi, [{send_credit_reply, 0},
                          {send_drained, {?FUNCTION_NAME, 5}}]},
                           Result),
    ok.

credit_and_drain_test(_) ->
    Cid = {?FUNCTION_NAME, self()},
    {State1, _} = enq(1, 1, first, test_init(test)),
    {State2, _} = enq(2, 2, second, State1),
    %% checkout without any initial credit (like AMQP 1.0 would)
    {State3, _, CheckEffs} =
        apply(meta(3), rabbit_fifo_v0:make_checkout(Cid, {auto, 0, credited}, #{}),
              State2),

    ?ASSERT_NO_EFF({send_msg, _, {delivery, _, _}}, CheckEffs),
    {State4, {multi, [{send_credit_reply, 0},
                      {send_drained, {?FUNCTION_NAME, 2}}]},
    Effects} = apply(meta(4), rabbit_fifo_v0:make_credit(Cid, 4, 0, true), State3),
    ?assertMatch(#?STATE{consumers = #{Cid := #consumer{credit = 0,
                                                       delivery_count = 4}}},
                 State4),

    ?ASSERT_EFF({send_msg, _, {delivery, _, [{_, {_, first}},
                                             {_, {_, second}}]}, _}, Effects),
    {_State5, EnqEffs} = enq(5, 2, third, State4),
    ?ASSERT_NO_EFF({send_msg, _, {delivery, _, _}}, EnqEffs),
    ok.



enq_enq_deq_test(_) ->
    Cid = {?FUNCTION_NAME, self()},
    {State1, _} = enq(1, 1, first, test_init(test)),
    {State2, _} = enq(2, 2, second, State1),
    % get returns a reply value
    NumReady = 1,
    {_State3, {dequeue, {0, {_, first}}, NumReady}, [{monitor, _, _}]} =
        apply(meta(3), rabbit_fifo_v0:make_checkout(Cid, {dequeue, unsettled}, #{}),
              State2),
    ok.

enq_enq_deq_deq_settle_test(_) ->
    Cid = {?FUNCTION_NAME, self()},
    {State1, _} = enq(1, 1, first, test_init(test)),
    {State2, _} = enq(2, 2, second, State1),
    % get returns a reply value
    {State3, {dequeue, {0, {_, first}}, 1}, [{monitor, _, _}]} =
        apply(meta(3), rabbit_fifo_v0:make_checkout(Cid, {dequeue, unsettled}, #{}),
              State2),
    {_State4, {dequeue, empty}} =
        apply(meta(4), rabbit_fifo_v0:make_checkout(Cid, {dequeue, unsettled}, #{}),
              State3),
    ok.

enq_enq_checkout_get_settled_test(_) ->
    Cid = {?FUNCTION_NAME, self()},
    {State1, _} = enq(1, 1, first, test_init(test)),
    % get returns a reply value
    {_State2, {dequeue, {0, {_, first}}, _}, _Effs} =
        apply(meta(3), rabbit_fifo_v0:make_checkout(Cid, {dequeue, settled}, #{}),
              State1),
    ok.

checkout_get_empty_test(_) ->
    Cid = {?FUNCTION_NAME, self()},
    State = test_init(test),
    {_State2, {dequeue, empty}} =
        apply(meta(1), rabbit_fifo_v0:make_checkout(Cid, {dequeue, unsettled}, #{}), State),
    ok.

untracked_enq_deq_test(_) ->
    Cid = {?FUNCTION_NAME, self()},
    State0 = test_init(test),
    {State1, _, _} = apply(meta(1),
                           rabbit_fifo_v0:make_enqueue(undefined, undefined, first),
                           State0),
    {_State2, {dequeue, {0, {_, first}}, _}, _} =
        apply(meta(3), rabbit_fifo_v0:make_checkout(Cid, {dequeue, settled}, #{}), State1),
    ok.

release_cursor_test(_) ->
    Cid = {?FUNCTION_NAME, self()},
    {State1, _} = enq(1, 1, first,  test_init(test)),
    {State2, _} = enq(2, 2, second, State1),
    {State3, _} = check(Cid, 3, 10, State2),
    % no release cursor effect at this point
    {State4, _} = settle(Cid, 4, 1, State3),
    {_Final, Effects1} = settle(Cid, 5, 0, State4),
    % empty queue forwards release cursor all the way
    ?ASSERT_EFF({release_cursor, 5, _}, Effects1),
    ok.

checkout_enq_settle_test(_) ->
    Cid = {?FUNCTION_NAME, self()},
    {State1, [{monitor, _, _} | _]} = check(Cid, 1, test_init(test)),
    {State2, Effects0} = enq(2, 1,  first, State1),
    ?ASSERT_EFF({send_msg, _,
                 {delivery, ?FUNCTION_NAME,
                  [{0, {_, first}}]}, _},
                Effects0),
    {State3, [_Inactive]} = enq(3, 2, second, State2),
    {_, _Effects} = settle(Cid, 4, 0, State3),
    % the release cursor is the smallest raft index that does not
    % contribute to the state of the application
    % ?ASSERT_EFF({release_cursor, 2, _}, Effects),
    ok.

out_of_order_enqueue_test(_) ->
    Cid = {?FUNCTION_NAME, self()},
    {State1, [{monitor, _, _} | _]} = check_n(Cid, 5, 5, test_init(test)),
    {State2, Effects2} = enq(2, 1, first, State1),
    ?ASSERT_EFF({send_msg, _, {delivery, _, [{_, {_, first}}]}, _}, Effects2),
    % assert monitor was set up
    ?ASSERT_EFF({monitor, _, _}, Effects2),
    % enqueue seq num 3 and 4 before 2
    {State3, Effects3} = enq(3, 3, third, State2),
    ?assertNoEffect({send_msg, _, {delivery, _, _}, _}, Effects3),
    {State4, Effects4} = enq(4, 4, fourth, State3),
    % assert no further deliveries where made
    ?assertNoEffect({send_msg, _, {delivery, _, _}, _}, Effects4),
    {_State5, Effects5} = enq(5, 2, second, State4),
    % assert two deliveries were now made
    ?ASSERT_EFF({send_msg, _, {delivery, _, [{_, {_, second}},
                                               {_, {_, third}},
                                               {_, {_, fourth}}]}, _},
                Effects5),
    ok.

out_of_order_first_enqueue_test(_) ->
    Cid = {?FUNCTION_NAME, self()},
    {State1, _} = check_n(Cid, 5, 5, test_init(test)),
    {_State2, Effects2} = enq(2, 10, first, State1),
    ?ASSERT_EFF({monitor, process, _}, Effects2),
    ?assertNoEffect({send_msg, _, {delivery, _, [{_, {_, first}}]}, _},
                    Effects2),
    ok.

duplicate_enqueue_test(_) ->
    Cid = {<<"duplicate_enqueue_test">>, self()},
    {State1, [{monitor, _, _} | _]} = check_n(Cid, 5, 5, test_init(test)),
    {State2, Effects2} = enq(2, 1, first, State1),
    ?ASSERT_EFF({send_msg, _, {delivery, _, [{_, {_, first}}]}, _}, Effects2),
    {_State3, Effects3} = enq(3, 1, first, State2),
    ?assertNoEffect({send_msg, _, {delivery, _, [{_, {_, first}}]}, _}, Effects3),
    ok.

return_test(_) ->
    Cid = {<<"cid">>, self()},
    Cid2 = {<<"cid2">>, self()},
    {State0, _} = enq(1, 1, msg, test_init(test)),
    {State1, _} = check_auto(Cid, 2, State0),
    {State2, _} = check_auto(Cid2, 3, State1),
    {State3, _, _} = apply(meta(4), rabbit_fifo_v0:make_return(Cid, [0]), State2),
    ?assertMatch(#{Cid := #consumer{checked_out = C}} when map_size(C) == 0,
                                                           State3#?STATE.consumers),
    ?assertMatch(#{Cid2 := #consumer{checked_out = C2}} when map_size(C2) == 1,
                                                           State3#?STATE.consumers),
    ok.

return_dequeue_delivery_limit_test(_) ->
    Init = init(#{name => test,
                  queue_resource => rabbit_misc:r("/", queue,
                                                  atom_to_binary(test, utf8)),
                  release_cursor_interval => 0,
                  delivery_limit => 1}),
    {State0, _} = enq(1, 1, msg, Init),

    Cid = {<<"cid">>, self()},
    Cid2 = {<<"cid2">>, self()},

    {State1, {MsgId1, _}} = deq(2, Cid, unsettled, State0),
    {State2, _, _} = apply(meta(4), rabbit_fifo_v0:make_return(Cid, [MsgId1]),
                           State1),

    {State3, {MsgId2, _}} = deq(2, Cid2, unsettled, State2),
    {State4, _, _} = apply(meta(4), rabbit_fifo_v0:make_return(Cid2, [MsgId2]),
                           State3),
    ?assertMatch(#{num_messages := 0}, rabbit_fifo_v0:overview(State4)),
    ok.

return_non_existent_test(_) ->
    Cid = {<<"cid">>, self()},
    {State0, [_, _Inactive]} = enq(1, 1, second, test_init(test)),
    % return non-existent
    {_State2, _} = apply(meta(3), rabbit_fifo_v0:make_return(Cid, [99]), State0),
    ok.

return_checked_out_test(_) ->
    Cid = {<<"cid">>, self()},
    {State0, [_, _]} = enq(1, 1, first, test_init(test)),
    {State1, [_Monitor,
              {send_msg, _, {delivery, _, [{MsgId, _}]}, _},
              {aux, active} | _ ]} = check_auto(Cid, 2, State0),
    % returning immediately checks out the same message again
    {_, ok, [{send_msg, _, {delivery, _, [{_, _}]}, _},
             {aux, active}]} =
        apply(meta(3), rabbit_fifo_v0:make_return(Cid, [MsgId]), State1),
    ok.

return_checked_out_limit_test(_) ->
    Cid = {<<"cid">>, self()},
    Init = init(#{name => test,
                  queue_resource => rabbit_misc:r("/", queue,
                                                  atom_to_binary(test, utf8)),
                  release_cursor_interval => 0,
                  delivery_limit => 1}),
    {State0, [_, _]} = enq(1, 1, first, Init),
    {State1, [_Monitor,
              {send_msg, _, {delivery, _, [{MsgId, _}]}, _},
              {aux, active} | _ ]} = check_auto(Cid, 2, State0),
    % returning immediately checks out the same message again
    {State2, ok, [{send_msg, _, {delivery, _, [{MsgId2, _}]}, _},
                  {aux, active}]} =
        apply(meta(3), rabbit_fifo_v0:make_return(Cid, [MsgId]), State1),
    {#?STATE{ra_indexes = RaIdxs}, ok, [_ReleaseEff]} =
        apply(meta(4), rabbit_fifo_v0:make_return(Cid, [MsgId2]), State2),
    ?assertEqual(0, rabbit_fifo_index:size(RaIdxs)),
    ok.

return_auto_checked_out_test(_) ->
    Cid = {<<"cid">>, self()},
    {State00, [_, _]} = enq(1, 1, first, test_init(test)),
    {State0, [_]} = enq(2, 2, second, State00),
    % it first active then inactive as the consumer took on but cannot take
    % any more
    {State1, [_Monitor,
              {send_msg, _, {delivery, _, [{MsgId, _}]}, _},
              {aux, active},
              {aux, inactive}
             ]} = check_auto(Cid, 2, State0),
    % return should include another delivery
    {_State2, _, Effects} = apply(meta(3), rabbit_fifo_v0:make_return(Cid, [MsgId]), State1),
    ?ASSERT_EFF({send_msg, _,
                 {delivery, _, [{_, {#{delivery_count := 1}, first}}]}, _},
                Effects),
    ok.

cancelled_checkout_out_test(_) ->
    Cid = {<<"cid">>, self()},
    {State00, [_, _]} = enq(1, 1, first, test_init(test)),
    {State0, [_]} = enq(2, 2, second, State00),
    {State1, _} = check_auto(Cid, 2, State0),
    % cancelled checkout should not return pending messages to queue
    {State2, _, _} = apply(meta(3), rabbit_fifo_v0:make_checkout(Cid, cancel, #{}), State1),
    ?assertEqual(1, maps:size(State2#?STATE.messages)),
    ?assertEqual(0, lqueue:len(State2#?STATE.returns)),

    {State3, {dequeue, empty}} =
        apply(meta(3), rabbit_fifo_v0:make_checkout(Cid, {dequeue, settled}, #{}), State2),
    %% settle
    {State4, ok, _} =
        apply(meta(4), rabbit_fifo_v0:make_settle(Cid, [0]), State3),

    {_State, {dequeue, {_, {_, second}}, _}, _} =
        apply(meta(5), rabbit_fifo_v0:make_checkout(Cid, {dequeue, settled}, #{}), State4),
    ok.

down_with_noproc_consumer_returns_unsettled_test(_) ->
    Cid = {<<"down_consumer_returns_unsettled_test">>, self()},
    {State0, [_, _]} = enq(1, 1, second, test_init(test)),
    {State1, [{monitor, process, Pid} | _]} = check(Cid, 2, State0),
    {State2, _, _} = apply(meta(3), {down, Pid, noproc}, State1),
    {_State, Effects} = check(Cid, 4, State2),
    ?ASSERT_EFF({monitor, process, _}, Effects),
    ok.

down_with_noconnection_marks_suspect_and_node_is_monitored_test(_) ->
    Pid = spawn(fun() -> ok end),
    Cid = {<<"down_with_noconnect">>, Pid},
    Self = self(),
    Node = node(Pid),
    {State0, Effects0} = enq(1, 1, second, test_init(test)),
    ?ASSERT_EFF({monitor, process, P}, P =:= Self, Effects0),
    {State1, Effects1} = check_auto(Cid, 2, State0),
    #consumer{credit = 0} = maps:get(Cid, State1#?STATE.consumers),
    ?ASSERT_EFF({monitor, process, P}, P =:= Pid, Effects1),
    % monitor both enqueuer and consumer
    % because we received a noconnection we now need to monitor the node
    {State2a, _, _} = apply(meta(3), {down, Pid, noconnection}, State1),
    #consumer{credit = 1,
              checked_out = Ch,
              status = suspected_down} = maps:get(Cid, State2a#?STATE.consumers),
    ?assertEqual(#{}, Ch),
    %% validate consumer has credit
    {State2, _, Effects2} = apply(meta(3), {down, Self, noconnection}, State2a),
    ?ASSERT_EFF({monitor, node, _}, Effects2),
    ?assertNoEffect({demonitor, process, _}, Effects2),
    % when the node comes up we need to retry the process monitors for the
    % disconnected processes
    {State3, _, Effects3} = apply(meta(3), {nodeup, Node}, State2),
    #consumer{status = up} = maps:get(Cid, State3#?STATE.consumers),
    % try to re-monitor the suspect processes
    ?ASSERT_EFF({monitor, process, P}, P =:= Pid, Effects3),
    ?ASSERT_EFF({monitor, process, P}, P =:= Self, Effects3),
    ok.

down_with_noconnection_returns_unack_test(_) ->
    Pid = spawn(fun() -> ok end),
    Cid = {<<"down_with_noconnect">>, Pid},
    {State0, _} = enq(1, 1, second, test_init(test)),
    ?assertEqual(1, maps:size(State0#?STATE.messages)),
    ?assertEqual(0, lqueue:len(State0#?STATE.returns)),
    {State1, {_, _}} = deq(2, Cid, unsettled, State0),
    ?assertEqual(0, maps:size(State1#?STATE.messages)),
    ?assertEqual(0, lqueue:len(State1#?STATE.returns)),
    {State2a, _, _} = apply(meta(3), {down, Pid, noconnection}, State1),
    ?assertEqual(0, maps:size(State2a#?STATE.messages)),
    ?assertEqual(1, lqueue:len(State2a#?STATE.returns)),
    ?assertMatch(#consumer{checked_out = Ch,
                           status = suspected_down}
                   when map_size(Ch) == 0,
                        maps:get(Cid, State2a#?STATE.consumers)),
    ok.

down_with_noproc_enqueuer_is_cleaned_up_test(_) ->
    State00 = test_init(test),
    Pid = spawn(fun() -> ok end),
    {State0, _, Effects0} = apply(meta(1), rabbit_fifo_v0:make_enqueue(Pid, 1, first), State00),
    ?ASSERT_EFF({monitor, process, _}, Effects0),
    {State1, _, _} = apply(meta(3), {down, Pid, noproc}, State0),
    % ensure there are no enqueuers
    ?assert(0 =:= maps:size(State1#?STATE.enqueuers)),
    ok.

discarded_message_without_dead_letter_handler_is_removed_test(_) ->
    Cid = {<<"completed_consumer_yields_demonitor_effect_test">>, self()},
    {State0, [_, _]} = enq(1, 1, first, test_init(test)),
    {State1, Effects1} = check_n(Cid, 2, 10, State0),
    ?ASSERT_EFF({send_msg, _,
                 {delivery, _, [{0, {_, first}}]}, _},
                Effects1),
    {_State2, _, Effects2} = apply(meta(1),
                                   rabbit_fifo_v0:make_discard(Cid, [0]), State1),
    ?assertNoEffect({send_msg, _,
                     {delivery, _, [{0, {_, first}}]}, _},
                    Effects2),
    ok.

discarded_message_with_dead_letter_handler_emits_log_effect_test(_) ->
    Cid = {<<"completed_consumer_yields_demonitor_effect_test">>, self()},
    State00 = init(#{name => test,
                     queue_resource => rabbit_misc:r(<<"/">>, queue, <<"test">>),
                     dead_letter_handler =>
                     {somemod, somefun, [somearg]}}),
    {State0, [_, _]} = enq(1, 1, first, State00),
    {State1, Effects1} = check_n(Cid, 2, 10, State0),
    ?ASSERT_EFF({send_msg, _,
                 {delivery, _, [{0, {_, first}}]}, _},
                Effects1),
    {_State2, _, Effects2} = apply(meta(1), rabbit_fifo_v0:make_discard(Cid, [0]), State1),
    % assert mod call effect with appended reason and message
    ?ASSERT_EFF({log, _RaftIdxs, _}, Effects2),
    ok.

tick_test(_) ->
    Cid = {<<"c">>, self()},
    Cid2 = {<<"c2">>, self()},
    {S0, _} = enq(1, 1, <<"fst">>, test_init(?FUNCTION_NAME)),
    {S1, _} = enq(2, 2, <<"snd">>, S0),
    {S2, {MsgId, _}} = deq(3, Cid, unsettled, S1),
    {S3, {_, _}} = deq(4, Cid2, unsettled, S2),
    {S4, _, _} = apply(meta(5), rabbit_fifo_v0:make_return(Cid, [MsgId]), S3),

    [{mod_call, rabbit_quorum_queue, handle_tick,
      [#resource{},
       {?FUNCTION_NAME, 1, 1, 2, 1, 3, 3},
       [_Node]
      ]}] = rabbit_fifo_v0:tick(1, S4),
    ok.


delivery_query_returns_deliveries_test(_) ->
    Tag = atom_to_binary(?FUNCTION_NAME, utf8),
    Cid = {Tag, self()},
    Commands = [
                rabbit_fifo_v0:make_checkout(Cid, {auto, 5, simple_prefetch}, #{}),
                rabbit_fifo_v0:make_enqueue(self(), 1, one),
                rabbit_fifo_v0:make_enqueue(self(), 2, two),
                rabbit_fifo_v0:make_enqueue(self(), 3, tre),
                rabbit_fifo_v0:make_enqueue(self(), 4, for)
              ],
    Indexes = lists:seq(1, length(Commands)),
    Entries = lists:zip(Indexes, Commands),
    {State, _Effects} = run_log(test_init(help), Entries),
    % 3 deliveries are returned
    [{0, {_, one}}] = rabbit_fifo_v0:get_checked_out(Cid, 0, 0, State),
    [_, _, _] = rabbit_fifo_v0:get_checked_out(Cid, 1, 3, State),
    ok.

pending_enqueue_is_enqueued_on_down_test(_) ->
    Cid = {<<"cid">>, self()},
    Pid = self(),
    {State0, _} = enq(1, 2, first, test_init(test)),
    {State1, _, _} = apply(meta(2), {down, Pid, noproc}, State0),
    {_State2, {dequeue, {0, {_, first}}, 0}, _} =
        apply(meta(3), rabbit_fifo_v0:make_checkout(Cid, {dequeue, settled}, #{}), State1),
    ok.

duplicate_delivery_test(_) ->
    {State0, _} = enq(1, 1, first, test_init(test)),
    {#?STATE{ra_indexes = RaIdxs,
            messages = Messages}, _} = enq(2, 1, first, State0),
    ?assertEqual(1, rabbit_fifo_index:size(RaIdxs)),
    ?assertEqual(1, maps:size(Messages)),
    ok.

state_enter_file_handle_leader_reservation_test(_) ->
    S0 = init(#{name => the_name,
                queue_resource => rabbit_misc:r(<<"/">>, queue, <<"test">>),
                become_leader_handler => {m, f, [a]}}),

    Resource = {resource, <<"/">>, queue, <<"test">>},
    Effects = rabbit_fifo_v0:state_enter(leader, S0),
    ?assertEqual([
        {mod_call, m, f, [a, the_name]},
        {mod_call, rabbit_quorum_queue, file_handle_leader_reservation, [Resource]}
      ], Effects),
    ok.

state_enter_file_handle_other_reservation_test(_) ->
    S0 = init(#{name => the_name,
                queue_resource => rabbit_misc:r(<<"/">>, queue, <<"test">>)}),
    Effects = rabbit_fifo_v0:state_enter(other, S0),
    ?assertEqual([
        {mod_call, rabbit_quorum_queue, file_handle_other_reservation, []}
      ],
      Effects),
    ok.

state_enter_monitors_and_notifications_test(_) ->
    Oth = spawn(fun () -> ok end),
    {State0, _} = enq(1, 1, first, test_init(test)),
    Cid = {<<"adf">>, self()},
    OthCid = {<<"oth">>, Oth},
    {State1, _} = check(Cid, 2, State0),
    {State, _} = check(OthCid, 3, State1),
    Self = self(),
    Effects = rabbit_fifo_v0:state_enter(leader, State),

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

purge_test(_) ->
    Cid = {<<"purge_test">>, self()},
    {State1, _} = enq(1, 1, first, test_init(test)),
    {State2, {purge, 1}, _} = apply(meta(2), rabbit_fifo_v0:make_purge(), State1),
    {State3, _} = enq(3, 2, second, State2),
    % get returns a reply value
    {_State4, {dequeue, {0, {_, second}}, _}, [{monitor, _, _}]} =
        apply(meta(4), rabbit_fifo_v0:make_checkout(Cid, {dequeue, unsettled}, #{}), State3),
    ok.

purge_with_checkout_test(_) ->
    Cid = {<<"purge_test">>, self()},
    {State0, _} = check_auto(Cid, 1, test_init(?FUNCTION_NAME)),
    {State1, _} = enq(2, 1, <<"first">>, State0),
    {State2, _} = enq(3, 2, <<"second">>, State1),
    %% assert message bytes are non zero
    ?assert(State2#?STATE.msg_bytes_checkout > 0),
    ?assert(State2#?STATE.msg_bytes_enqueue > 0),
    {State3, {purge, 1}, _} = apply(meta(2), rabbit_fifo_v0:make_purge(), State2),
    ?assert(State2#?STATE.msg_bytes_checkout > 0),
    ?assertEqual(0, State3#?STATE.msg_bytes_enqueue),
    ?assertEqual(1, rabbit_fifo_index:size(State3#?STATE.ra_indexes)),
    #consumer{checked_out = Checked} = maps:get(Cid, State3#?STATE.consumers),
    ?assertEqual(1, maps:size(Checked)),
    ok.

down_noproc_returns_checked_out_in_order_test(_) ->
    S0 = test_init(?FUNCTION_NAME),
    %% enqueue 100
    S1 = lists:foldl(fun (Num, FS0) ->
                         {FS, _} = enq(Num, Num, Num, FS0),
                         FS
                     end, S0, lists:seq(1, 100)),
    ?assertEqual(100, maps:size(S1#?STATE.messages)),
    Cid = {<<"cid">>, self()},
    {S2, _} = check(Cid, 101, 1000, S1),
    #consumer{checked_out = Checked} = maps:get(Cid, S2#?STATE.consumers),
    ?assertEqual(100, maps:size(Checked)),
    %% simulate down
    {S, _, _} = apply(meta(102), {down, self(), noproc}, S2),
    Returns = lqueue:to_list(S#?STATE.returns),
    ?assertEqual(100, length(Returns)),
    ?assertEqual(0, maps:size(S#?STATE.consumers)),
    %% validate returns are in order
    ?assertEqual(lists:sort(Returns), Returns),
    ok.

down_noconnection_returns_checked_out_test(_) ->
    S0 = test_init(?FUNCTION_NAME),
    NumMsgs = 20,
    S1 = lists:foldl(fun (Num, FS0) ->
                         {FS, _} = enq(Num, Num, Num, FS0),
                         FS
                     end, S0, lists:seq(1, NumMsgs)),
    ?assertEqual(NumMsgs, maps:size(S1#?STATE.messages)),
    Cid = {<<"cid">>, self()},
    {S2, _} = check(Cid, 101, 1000, S1),
    #consumer{checked_out = Checked} = maps:get(Cid, S2#?STATE.consumers),
    ?assertEqual(NumMsgs, maps:size(Checked)),
    %% simulate down
    {S, _, _} = apply(meta(102), {down, self(), noconnection}, S2),
    Returns = lqueue:to_list(S#?STATE.returns),
    ?assertEqual(NumMsgs, length(Returns)),
    ?assertMatch(#consumer{checked_out = Ch}
                   when map_size(Ch) == 0,
                        maps:get(Cid, S#?STATE.consumers)),
    %% validate returns are in order
    ?assertEqual(lists:sort(Returns), Returns),
    ok.

single_active_consumer_basic_get_test(_) ->
    Cid = {?FUNCTION_NAME, self()},
    State0 = init(#{name => ?FUNCTION_NAME,
                    queue_resource => rabbit_misc:r("/", queue,
                        atom_to_binary(?FUNCTION_NAME, utf8)),
                    release_cursor_interval => 0,
                    single_active_consumer_on => true}),
    ?assertEqual(single_active, State0#?STATE.cfg#cfg.consumer_strategy),
    ?assertEqual(0, map_size(State0#?STATE.consumers)),
    {State1, _} = enq(1, 1, first, State0),
    {_State, {error, unsupported}} =
        apply(meta(2), rabbit_fifo_v0:make_checkout(Cid, {dequeue, unsettled}, #{}),
              State1),
    ok.

single_active_consumer_test(_) ->
    State0 = init(#{name => ?FUNCTION_NAME,
                    queue_resource => rabbit_misc:r("/", queue,
                        atom_to_binary(?FUNCTION_NAME, utf8)),
                    release_cursor_interval => 0,
                    single_active_consumer_on => true}),
    ?assertEqual(single_active, State0#?STATE.cfg#cfg.consumer_strategy),
    ?assertEqual(0, map_size(State0#?STATE.consumers)),

    % adding some consumers
    AddConsumer = fun(CTag, State) ->
                          {NewState, _, _} = apply(
                                               meta(1),
                                               make_checkout({CTag, self()},
                                                             {once, 1, simple_prefetch},
                                                             #{}),
                                               State),
                          NewState
                  end,
    State1 = lists:foldl(AddConsumer, State0,
                         [<<"ctag1">>, <<"ctag2">>, <<"ctag3">>, <<"ctag4">>]),
    C1 = {<<"ctag1">>, self()},
    C2 = {<<"ctag2">>, self()},
    C3 = {<<"ctag3">>, self()},
    C4 = {<<"ctag4">>, self()},

    % the first registered consumer is the active one, the others are waiting
    ?assertEqual(1, map_size(State1#?STATE.consumers)),
    ?assertMatch(#{C1 := _}, State1#?STATE.consumers),
    ?assertEqual(3, length(State1#?STATE.waiting_consumers)),
    ?assertNotEqual(false, lists:keyfind(C2, 1, State1#?STATE.waiting_consumers)),
    ?assertNotEqual(false, lists:keyfind(C3, 1, State1#?STATE.waiting_consumers)),
    ?assertNotEqual(false, lists:keyfind(C4, 1, State1#?STATE.waiting_consumers)),

    % cancelling a waiting consumer
    {State2, _, Effects1} = apply(meta(2),
                                  make_checkout(C3, cancel, #{}),
                                  State1),
    % the active consumer should still be in place
    ?assertEqual(1, map_size(State2#?STATE.consumers)),
    ?assertMatch(#{C1 := _}, State2#?STATE.consumers),
    % the cancelled consumer has been removed from waiting consumers
    ?assertEqual(2, length(State2#?STATE.waiting_consumers)),
    ?assertNotEqual(false, lists:keyfind(C2, 1, State2#?STATE.waiting_consumers)),
    ?assertNotEqual(false, lists:keyfind(C4, 1, State2#?STATE.waiting_consumers)),
    % there are some effects to unregister the consumer
    ?ASSERT_EFF({mod_call, rabbit_quorum_queue,
                 cancel_consumer_handler, [_, C]}, C == C3, Effects1),

    % cancelling the active consumer
    {State3, _, Effects2} = apply(meta(3),
                                  make_checkout(C1, cancel, #{}),
                                  State2),
    % the second registered consumer is now the active one
    ?assertEqual(1, map_size(State3#?STATE.consumers)),
    ?assertMatch(#{C2 := _}, State3#?STATE.consumers),
    % the new active consumer is no longer in the waiting list
    ?assertEqual(1, length(State3#?STATE.waiting_consumers)),
    ?assertNotEqual(false, lists:keyfind(C4, 1,
                                         State3#?STATE.waiting_consumers)),
    %% should have a cancel consumer handler mod_call effect and
    %% an active new consumer effect
    ?ASSERT_EFF({mod_call, rabbit_quorum_queue,
                 cancel_consumer_handler, [_, C]}, C == C1, Effects2),
    ?ASSERT_EFF({mod_call, rabbit_quorum_queue,
                 update_consumer_handler, _}, Effects2),

    % cancelling the active consumer
    {State4, _, Effects3} = apply(meta(4),
                                  make_checkout(C2, cancel, #{}),
                                  State3),
    % the last waiting consumer became the active one
    ?assertEqual(1, map_size(State4#?STATE.consumers)),
    ?assertMatch(#{C4 := _}, State4#?STATE.consumers),
    % the waiting consumer list is now empty
    ?assertEqual(0, length(State4#?STATE.waiting_consumers)),
    % there are some effects to unregister the consumer and
    % to update the new active one (metrics)
    ?ASSERT_EFF({mod_call, rabbit_quorum_queue,
                 cancel_consumer_handler, [_, C]}, C == C2, Effects3),
    ?ASSERT_EFF({mod_call, rabbit_quorum_queue,
                 update_consumer_handler, _}, Effects3),

    % cancelling the last consumer
    {State5, _, Effects4} = apply(meta(5),
                                  make_checkout(C4, cancel, #{}),
                                  State4),
    % no active consumer anymore
    ?assertEqual(0, map_size(State5#?STATE.consumers)),
    % still nothing in the waiting list
    ?assertEqual(0, length(State5#?STATE.waiting_consumers)),
    % there is an effect to unregister the consumer + queue inactive effect
    ?ASSERT_EFF({mod_call, rabbit_quorum_queue,
                 cancel_consumer_handler, _}, Effects4),

    ok.

single_active_consumer_cancel_consumer_when_channel_is_down_test(_) ->
    State0 = init(#{name => ?FUNCTION_NAME,
        queue_resource => rabbit_misc:r("/", queue,
            atom_to_binary(?FUNCTION_NAME, utf8)),
        release_cursor_interval => 0,
        single_active_consumer_on => true}),

    DummyFunction = fun() -> ok  end,
    Pid1 = spawn(DummyFunction),
    Pid2 = spawn(DummyFunction),
    Pid3 = spawn(DummyFunction),

    [C1, C2, C3, C4] = Consumers =
        [{<<"ctag1">>, Pid1}, {<<"ctag2">>, Pid2},
         {<<"ctag3">>, Pid2}, {<<"ctag4">>, Pid3}],
    % adding some consumers
    AddConsumer = fun({CTag, ChannelId}, State) ->
        {NewState, _, _} = apply(
            #{index => 1},
            make_checkout({CTag, ChannelId}, {once, 1, simple_prefetch}, #{}),
            State),
        NewState
                  end,
    State1 = lists:foldl(AddConsumer, State0, Consumers),

    % the channel of the active consumer goes down
    {State2, _, Effects} = apply(#{index => 2}, {down, Pid1, noproc}, State1),
    % fell back to another consumer
    ?assertEqual(1, map_size(State2#?STATE.consumers)),
    % there are still waiting consumers
    ?assertEqual(2, length(State2#?STATE.waiting_consumers)),
    % effects to unregister the consumer and
    % to update the new active one (metrics) are there
    ?ASSERT_EFF({mod_call, rabbit_quorum_queue,
                 cancel_consumer_handler, [_, C]}, C == C1, Effects),
    ?ASSERT_EFF({mod_call, rabbit_quorum_queue,
                 update_consumer_handler, _}, Effects),

    % the channel of the active consumer and a waiting consumer goes down
    {State3, _, Effects2} = apply(#{index => 3}, {down, Pid2, noproc}, State2),
    % fell back to another consumer
    ?assertEqual(1, map_size(State3#?STATE.consumers)),
    % no more waiting consumer
    ?assertEqual(0, length(State3#?STATE.waiting_consumers)),
    % effects to cancel both consumers of this channel + effect to update the new active one (metrics)
    ?ASSERT_EFF({mod_call, rabbit_quorum_queue,
                 cancel_consumer_handler, [_, C]}, C == C2, Effects2),
    ?ASSERT_EFF({mod_call, rabbit_quorum_queue,
                 cancel_consumer_handler, [_, C]}, C == C3, Effects2),
    ?ASSERT_EFF({mod_call, rabbit_quorum_queue,
                 update_consumer_handler, _}, Effects2),

    % the last channel goes down
    {State4, _, Effects3} = apply(#{index => 4}, {down, Pid3, doesnotmatter}, State3),
    % no more consumers
    ?assertEqual(0, map_size(State4#?STATE.consumers)),
    ?assertEqual(0, length(State4#?STATE.waiting_consumers)),
    % there is an effect to unregister the consumer + queue inactive effect
    ?ASSERT_EFF({mod_call, rabbit_quorum_queue,
                 cancel_consumer_handler, [_, C]}, C == C4, Effects3),

    ok.

single_active_returns_messages_on_noconnection_test(_) ->
    R = rabbit_misc:r("/", queue, atom_to_binary(?FUNCTION_NAME, utf8)),
    State0 = init(#{name => ?FUNCTION_NAME,
                    queue_resource => R,
                    release_cursor_interval => 0,
                    single_active_consumer_on => true}),
    Meta = #{index => 1},
    Nodes = [n1],
    ConsumerIds = [{_, DownPid}] =
        [begin
             B = atom_to_binary(N, utf8),
             {<<"ctag_", B/binary>>,
              test_util:fake_pid(N)}
         end || N <- Nodes],
    % adding some consumers
    State1 = lists:foldl(
               fun(CId, Acc0) ->
                       {Acc, _, _} =
                           apply(Meta,
                                 make_checkout(CId,
                                               {once, 1, simple_prefetch}, #{}),
                                 Acc0),
                       Acc
               end, State0, ConsumerIds),
    {State2, _} = enq(4, 1, msg1, State1),
    % simulate node goes down
    {State3, _, _} = apply(meta(5), {down, DownPid, noconnection}, State2),
    %% assert the consumer is up
    ?assertMatch([_], lqueue:to_list(State3#?STATE.returns)),
    ?assertMatch([{_, #consumer{checked_out = Checked}}]
                 when map_size(Checked) == 0,
                      State3#?STATE.waiting_consumers),

    ok.

single_active_consumer_replaces_consumer_when_down_noconnection_test(_) ->
    R = rabbit_misc:r("/", queue, atom_to_binary(?FUNCTION_NAME, utf8)),
    State0 = init(#{name => ?FUNCTION_NAME,
                    queue_resource => R,
                    release_cursor_interval => 0,
                    single_active_consumer_on => true}),
    Meta = #{index => 1},
    Nodes = [n1, n2, node()],
    ConsumerIds = [C1 = {_, DownPid}, C2, _C3] =
    [begin
         B = atom_to_binary(N, utf8),
         {<<"ctag_", B/binary>>,
          test_util:fake_pid(N)}
     end || N <- Nodes],
    % adding some consumers
    State1a = lists:foldl(
                fun(CId, Acc0) ->
                        {Acc, _, _} =
                        apply(Meta,
                              make_checkout(CId,
                                            {once, 1, simple_prefetch}, #{}),
                              Acc0),
                        Acc
                end, State0, ConsumerIds),

    %% assert the consumer is up
    ?assertMatch(#{C1 := #consumer{status = up}},
                 State1a#?STATE.consumers),

    {State1, _} = enq(10, 1, msg, State1a),

    % simulate node goes down
    {State2, _, _} = apply(meta(5), {down, DownPid, noconnection}, State1),

    %% assert a new consumer is in place and it is up
    ?assertMatch([{C2, #consumer{status = up,
                                 checked_out = Ch}}]
                   when map_size(Ch) == 1,
                        maps:to_list(State2#?STATE.consumers)),

    %% the disconnected consumer has been returned to waiting
    ?assert(lists:any(fun ({C,_}) -> C =:= C1 end,
                      State2#?STATE.waiting_consumers)),
    ?assertEqual(2, length(State2#?STATE.waiting_consumers)),

    % simulate node comes back up
    {State3, _, _} = apply(#{index => 2}, {nodeup, node(DownPid)}, State2),

    %% the consumer is still active and the same as before
    ?assertMatch([{C2, #consumer{status = up}}],
                 maps:to_list(State3#?STATE.consumers)),
    % the waiting consumers should be un-suspected
    ?assertEqual(2, length(State3#?STATE.waiting_consumers)),
    lists:foreach(fun({_, #consumer{status = Status}}) ->
                      ?assert(Status /= suspected_down)
                  end, State3#?STATE.waiting_consumers),
    ok.

single_active_consumer_all_disconnected_test(_) ->
    R = rabbit_misc:r("/", queue, atom_to_binary(?FUNCTION_NAME, utf8)),
    State0 = init(#{name => ?FUNCTION_NAME,
                    queue_resource => R,
                    release_cursor_interval => 0,
                    single_active_consumer_on => true}),
    Meta = #{index => 1},
    Nodes = [n1, n2],
    ConsumerIds = [C1 = {_, C1Pid}, C2 = {_, C2Pid}] =
    [begin
         B = atom_to_binary(N, utf8),
         {<<"ctag_", B/binary>>,
          test_util:fake_pid(N)}
     end || N <- Nodes],
    % adding some consumers
    State1 = lists:foldl(
               fun(CId, Acc0) ->
                       {Acc, _, _} =
                       apply(Meta,
                             make_checkout(CId,
                                           {once, 1, simple_prefetch}, #{}),
                             Acc0),
                       Acc
               end, State0, ConsumerIds),

    %% assert the consumer is up
    ?assertMatch(#{C1 := #consumer{status = up}}, State1#?STATE.consumers),

    % simulate node goes down
    {State2, _, _} = apply(meta(5), {down, C1Pid, noconnection}, State1),
    %% assert the consumer fails over to the consumer on n2
    ?assertMatch(#{C2 := #consumer{status = up}}, State2#?STATE.consumers),
    {State3, _, _} = apply(meta(6), {down, C2Pid, noconnection}, State2),
    %% assert these no active consumer after both nodes are maked as down
    ?assertMatch([], maps:to_list(State3#?STATE.consumers)),
    %% n2 comes back
    {State4, _, _} = apply(meta(7), {nodeup, node(C2Pid)}, State3),
    %% ensure n2 is the active consumer as this node as been registered
    %% as up again
    ?assertMatch([{{<<"ctag_n2">>, _}, #consumer{status = up,
                                                 credit = 1}}],
                 maps:to_list(State4#?STATE.consumers)),
    ok.

single_active_consumer_state_enter_leader_include_waiting_consumers_test(_) ->
    State0 = init(#{name => ?FUNCTION_NAME,
                    queue_resource =>
                    rabbit_misc:r("/", queue,
                                  atom_to_binary(?FUNCTION_NAME, utf8)),
                    release_cursor_interval => 0,
                    single_active_consumer_on => true}),

    DummyFunction = fun() -> ok  end,
    Pid1 = spawn(DummyFunction),
    Pid2 = spawn(DummyFunction),
    Pid3 = spawn(DummyFunction),

    Meta = #{index => 1},
    % adding some consumers
    AddConsumer = fun({CTag, ChannelId}, State) ->
        {NewState, _, _} = apply(
            Meta,
            make_checkout({CTag, ChannelId},
                          {once, 1, simple_prefetch}, #{}),
            State),
        NewState
                  end,
    State1 = lists:foldl(AddConsumer, State0,
        [{<<"ctag1">>, Pid1}, {<<"ctag2">>, Pid2}, {<<"ctag3">>, Pid2}, {<<"ctag4">>, Pid3}]),

    Effects = rabbit_fifo_v0:state_enter(leader, State1),
    %% 2 effects for each consumer process (channel process), 1 effect for the node,
    %% 1 effect for file handle reservation
    ?assertEqual(2 * 3 + 1 + 1, length(Effects)).

single_active_consumer_state_enter_eol_include_waiting_consumers_test(_) ->
    Resource = rabbit_misc:r("/", queue, atom_to_binary(?FUNCTION_NAME, utf8)),
    State0 = init(#{name => ?FUNCTION_NAME,
                    queue_resource => Resource,
                    release_cursor_interval => 0,
                    single_active_consumer_on => true}),

    DummyFunction = fun() -> ok  end,
    Pid1 = spawn(DummyFunction),
    Pid2 = spawn(DummyFunction),
    Pid3 = spawn(DummyFunction),

    Meta = #{index => 1},
    % adding some consumers
    AddConsumer = fun({CTag, ChannelId}, State) ->
        {NewState, _, _} = apply(
            Meta,
            make_checkout({CTag, ChannelId},
                          {once, 1, simple_prefetch}, #{}),
            State),
        NewState
                  end,
    State1 = lists:foldl(AddConsumer, State0,
                         [{<<"ctag1">>, Pid1}, {<<"ctag2">>, Pid2},
                          {<<"ctag3">>, Pid2}, {<<"ctag4">>, Pid3}]),

    Effects = rabbit_fifo_v0:state_enter(eol, State1),
    %% 1 effect for each consumer process (channel process),
    %% 1 effect for file handle reservation
    ?assertEqual(4, length(Effects)).

query_consumers_test(_) ->
    State0 = init(#{name => ?FUNCTION_NAME,
                    queue_resource => rabbit_misc:r("/", queue,
                        atom_to_binary(?FUNCTION_NAME, utf8)),
                    release_cursor_interval => 0,
                    single_active_consumer_on => false}),

    % adding some consumers
    AddConsumer = fun(CTag, State) ->
        {NewState, _, _} = apply(
            #{index => 1},
            make_checkout({CTag, self()},
                          {once, 1, simple_prefetch}, #{}),
            State),
        NewState
                  end,
    State1 = lists:foldl(AddConsumer, State0, [<<"ctag1">>, <<"ctag2">>, <<"ctag3">>, <<"ctag4">>]),
    Consumers0 = State1#?STATE.consumers,
    Consumer = maps:get({<<"ctag2">>, self()}, Consumers0),
    Consumers1 = maps:put({<<"ctag2">>, self()},
                          Consumer#consumer{status = suspected_down}, Consumers0),
    State2 = State1#?STATE{consumers = Consumers1},

    ?assertEqual(4, rabbit_fifo_v0:query_consumer_count(State2)),
    Consumers2 = rabbit_fifo_v0:query_consumers(State2),
    ?assertEqual(4, maps:size(Consumers2)),
    maps:fold(fun(_Key, {Pid, Tag, _, _, Active, ActivityStatus, _, _}, _Acc) ->
        ?assertEqual(self(), Pid),
        case Tag of
            <<"ctag2">> ->
                ?assertNot(Active),
                ?assertEqual(suspected_down, ActivityStatus);
            _ ->
                ?assert(Active),
                ?assertEqual(up, ActivityStatus)
        end
              end, [], Consumers2).

query_consumers_when_single_active_consumer_is_on_test(_) ->
    State0 = init(#{name => ?FUNCTION_NAME,
                    queue_resource => rabbit_misc:r("/", queue,
                        atom_to_binary(?FUNCTION_NAME, utf8)),
                    release_cursor_interval => 0,
                    single_active_consumer_on => true}),
    Meta = #{index => 1},
    % adding some consumers
    AddConsumer = fun(CTag, State) ->
                    {NewState, _, _} = apply(
                        Meta,
                        make_checkout({CTag, self()},
                                      {once, 1, simple_prefetch}, #{}),
                        State),
                    NewState
                  end,
    State1 = lists:foldl(AddConsumer, State0, [<<"ctag1">>, <<"ctag2">>, <<"ctag3">>, <<"ctag4">>]),

    ?assertEqual(4, rabbit_fifo_v0:query_consumer_count(State1)),
    Consumers = rabbit_fifo_v0:query_consumers(State1),
    ?assertEqual(4, maps:size(Consumers)),
    maps:fold(fun(_Key, {Pid, Tag, _, _, Active, ActivityStatus, _, _}, _Acc) ->
                  ?assertEqual(self(), Pid),
                  case Tag of
                     <<"ctag1">> ->
                         ?assert(Active),
                         ?assertEqual(single_active, ActivityStatus);
                     _ ->
                         ?assertNot(Active),
                         ?assertEqual(waiting, ActivityStatus)
                  end
              end, [], Consumers).

active_flag_updated_when_consumer_suspected_unsuspected_test(_) ->
    State0 = init(#{name => ?FUNCTION_NAME,
        queue_resource => rabbit_misc:r("/", queue,
            atom_to_binary(?FUNCTION_NAME, utf8)),
        release_cursor_interval => 0,
        single_active_consumer_on => false}),

    DummyFunction = fun() -> ok  end,
    Pid1 = spawn(DummyFunction),
    Pid2 = spawn(DummyFunction),
    Pid3 = spawn(DummyFunction),

    % adding some consumers
    AddConsumer = fun({CTag, ChannelId}, State) ->
                          {NewState, _, _} =
                          apply(
                            #{index => 1},
                            rabbit_fifo_v0:make_checkout({CTag, ChannelId},
                                                      {once, 1, simple_prefetch},
                                                      #{}),
                            State),
                          NewState
                  end,
    State1 = lists:foldl(AddConsumer, State0,
        [{<<"ctag1">>, Pid1}, {<<"ctag2">>, Pid2}, {<<"ctag3">>, Pid2}, {<<"ctag4">>, Pid3}]),

    {State2, _, Effects2} = apply(#{index => 3}, {down, Pid1, noconnection}, State1),
    % 1 effect to update the metrics of each consumer (they belong to the same node), 1 more effect to monitor the node
    ?assertEqual(4 + 1, length(Effects2)),

    {_, _, Effects3} = apply(#{index => 4}, {nodeup, node(self())}, State2),
    % for each consumer: 1 effect to update the metrics, 1 effect to monitor the consumer PID
    ?assertEqual(4 + 4, length(Effects3)).

active_flag_not_updated_when_consumer_suspected_unsuspected_and_single_active_consumer_is_on_test(_) ->
    State0 = init(#{name => ?FUNCTION_NAME,
                    queue_resource => rabbit_misc:r("/", queue,
                                                    atom_to_binary(?FUNCTION_NAME, utf8)),
                    release_cursor_interval => 0,
                    single_active_consumer_on => true}),

    DummyFunction = fun() -> ok  end,
    Pid1 = spawn(DummyFunction),
    Pid2 = spawn(DummyFunction),
    Pid3 = spawn(DummyFunction),

    % adding some consumers
    AddConsumer = fun({CTag, ChannelId}, State) ->
                          {NewState, _, _} = apply(
                                               #{index => 1},
                                               make_checkout({CTag, ChannelId},
                                                             {once, 1, simple_prefetch}, #{}),
            State),
        NewState
                  end,
    State1 = lists:foldl(AddConsumer, State0,
                         [{<<"ctag1">>, Pid1}, {<<"ctag2">>, Pid2},
                          {<<"ctag3">>, Pid2}, {<<"ctag4">>, Pid3}]),

    {State2, _, Effects2} = apply(#{index => 2}, {down, Pid1, noconnection}, State1),
    % one monitor and one consumer status update (deactivated)
    ?assertEqual(3, length(Effects2)),

    {_, _, Effects3} = apply(#{index => 3}, {nodeup, node(self())}, State2),
    % for each consumer: 1 effect to monitor the consumer PID
    ?assertEqual(5, length(Effects3)).

single_active_cancelled_with_unacked_test(_) ->
    State0 = init(#{name => ?FUNCTION_NAME,
                    queue_resource => rabbit_misc:r("/", queue,
                        atom_to_binary(?FUNCTION_NAME, utf8)),
                    release_cursor_interval => 0,
                    single_active_consumer_on => true}),

    C1 = {<<"ctag1">>, self()},
    C2 = {<<"ctag2">>, self()},
    % adding some consumers
    AddConsumer = fun(C, S0) ->
                          {S, _, _} = apply(
                                        meta(1),
                                        make_checkout(C,
                                                      {auto, 1, simple_prefetch},
                                                      #{}),
                                        S0),
                          S
                  end,
    State1 = lists:foldl(AddConsumer, State0, [C1, C2]),

    %% enqueue 2 messages
    {State2, _Effects2} = enq(3, 1, msg1, State1),
    {State3, _Effects3} = enq(4, 2, msg2, State2),
    %% one should be checked ou to C1
    %% cancel C1
    {State4, _, _} = apply(meta(5),
                           make_checkout(C1, cancel, #{}),
                           State3),
    %% C2 should be the active consumer
    ?assertMatch(#{C2 := #consumer{status = up,
                                   checked_out = #{0 := _}}},
                 State4#?STATE.consumers),
    %% C1 should be a cancelled consumer
    ?assertMatch(#{C1 := #consumer{status = cancelled,
                                   lifetime = once,
                                   checked_out = #{0 := _}}},
                 State4#?STATE.consumers),
    ?assertMatch([], State4#?STATE.waiting_consumers),

    %% Ack both messages
    {State5, _Effects5} = settle(C1, 1, 0, State4),
    %% C1 should now be cancelled
    {State6, _Effects6} = settle(C2, 2, 0, State5),

    %% C2 should remain
    ?assertMatch(#{C2 := #consumer{status = up}},
                 State6#?STATE.consumers),
    %% C1 should be gone
    ?assertNotMatch(#{C1 := _},
                    State6#?STATE.consumers),
    ?assertMatch([], State6#?STATE.waiting_consumers),
    ok.

single_active_with_credited_test(_) ->
    State0 = init(#{name => ?FUNCTION_NAME,
                    queue_resource => rabbit_misc:r("/", queue,
                        atom_to_binary(?FUNCTION_NAME, utf8)),
                    release_cursor_interval => 0,
                    single_active_consumer_on => true}),

    C1 = {<<"ctag1">>, self()},
    C2 = {<<"ctag2">>, self()},
    % adding some consumers
    AddConsumer = fun(C, S0) ->
                          {S, _, _} = apply(
                                        meta(1),
                                        make_checkout(C,
                                                      {auto, 0, credited},
                                                      #{}),
                                        S0),
                          S
                  end,
    State1 = lists:foldl(AddConsumer, State0, [C1, C2]),

    %% add some credit
    C1Cred = rabbit_fifo_v0:make_credit(C1, 5, 0, false),
    {State2, _, _Effects2} = apply(meta(3), C1Cred, State1),
    C2Cred = rabbit_fifo_v0:make_credit(C2, 4, 0, false),
    {State3, _} = apply(meta(4), C2Cred, State2),
    %% both consumers should have credit
    ?assertMatch(#{C1 := #consumer{credit = 5}},
                 State3#?STATE.consumers),
    ?assertMatch([{C2, #consumer{credit = 4}}],
                 State3#?STATE.waiting_consumers),
    ok.

purge_nodes_test(_) ->
    Node = purged@node,
    ThisNode = node(),
    EnqPid = test_util:fake_pid(Node),
    EnqPid2 = test_util:fake_pid(node()),
    ConPid = test_util:fake_pid(Node),
    Cid = {<<"tag">>, ConPid},
    % WaitingPid = test_util:fake_pid(Node),

    State0 = init(#{name => ?FUNCTION_NAME,
                    queue_resource => rabbit_misc:r("/", queue,
                        atom_to_binary(?FUNCTION_NAME, utf8)),
                    single_active_consumer_on => false}),
    {State1, _, _} = apply(meta(1),
                           rabbit_fifo_v0:make_enqueue(EnqPid, 1, msg1),
                           State0),
    {State2, _, _} = apply(meta(2),
                           rabbit_fifo_v0:make_enqueue(EnqPid2, 1, msg2),
                           State1),
    {State3, _} = check(Cid, 3, 1000, State2),
    {State4, _, _} = apply(meta(4),
                           {down, EnqPid, noconnection},
                           State3),
    ?assertMatch(
       [{mod_call, rabbit_quorum_queue, handle_tick,
         [#resource{}, _Metrics,
          [ThisNode, Node]
         ]}] , rabbit_fifo_v0:tick(1, State4)),
    %% assert there are both enqueuers and consumers
    {State, _, _} = apply(meta(5),
                          rabbit_fifo_v0:make_purge_nodes([Node]),
                          State4),

    %% assert there are no enqueuers nor consumers
    ?assertMatch(#?STATE{enqueuers = Enqs} when map_size(Enqs) == 1, State),
    ?assertMatch(#?STATE{consumers = Cons} when map_size(Cons) == 0, State),
    ?assertMatch(
       [{mod_call, rabbit_quorum_queue, handle_tick,
         [#resource{}, _Metrics,
          [ThisNode]
         ]}] , rabbit_fifo_v0:tick(1, State)),
    ok.

meta(Idx) ->
    #{index => Idx, term => 1,
      from => {make_ref(), self()}}.

enq(Idx, MsgSeq, Msg, State) ->
    strip_reply(
        apply(meta(Idx), rabbit_fifo_v0:make_enqueue(self(), MsgSeq, Msg), State)).

deq(Idx, Cid, Settlement, State0) ->
    {State, {dequeue, {MsgId, Msg}, _}, _} =
        apply(meta(Idx),
              rabbit_fifo_v0:make_checkout(Cid, {dequeue, Settlement}, #{}),
              State0),
    {State, {MsgId, Msg}}.

check_n(Cid, Idx, N, State) ->
    strip_reply(
      apply(meta(Idx),
            rabbit_fifo_v0:make_checkout(Cid, {auto, N, simple_prefetch}, #{}),
            State)).

check(Cid, Idx, State) ->
    strip_reply(
      apply(meta(Idx),
            rabbit_fifo_v0:make_checkout(Cid, {once, 1, simple_prefetch}, #{}),
            State)).

check_auto(Cid, Idx, State) ->
    strip_reply(
      apply(meta(Idx),
            rabbit_fifo_v0:make_checkout(Cid, {auto, 1, simple_prefetch}, #{}),
            State)).

check(Cid, Idx, Num, State) ->
    strip_reply(
      apply(meta(Idx),
            rabbit_fifo_v0:make_checkout(Cid, {auto, Num, simple_prefetch}, #{}),
            State)).

settle(Cid, Idx, MsgId, State) ->
    strip_reply(apply(meta(Idx), rabbit_fifo_v0:make_settle(Cid, [MsgId]), State)).

credit(Cid, Idx, Credit, DelCnt, Drain, State) ->
    strip_reply(apply(meta(Idx), rabbit_fifo_v0:make_credit(Cid, Credit, DelCnt, Drain),
                      State)).

strip_reply({State, _, Effects}) ->
    {State, Effects}.

run_log(InitState, Entries) ->
    lists:foldl(fun ({Idx, E}, {Acc0, Efx0}) ->
                        case apply(meta(Idx), E, Acc0) of
                            {Acc, _, Efx} when is_list(Efx) ->
                                {Acc, Efx0 ++ Efx};
                            {Acc, _, Efx}  ->
                                {Acc, Efx0 ++ [Efx]};
                            {Acc, _}  ->
                                {Acc, Efx0}
                        end
                end, {InitState, []}, Entries).


%% AUX Tests

aux_test(_) ->
    _ = ra_machine_ets:start_link(),
    Aux0 = init_aux(aux_test),
    MacState = init(#{name => aux_test,
                      queue_resource =>
                      rabbit_misc:r(<<"/">>, queue, <<"test">>)}),
    ok = meck:new(ra_log, []),
    Log = mock_log,
    meck:expect(ra_log, last_index_term, fun (_) -> {0, 0} end),
    {no_reply, Aux, mock_log} = handle_aux(leader, cast, active, Aux0,
                                            Log, MacState),
    {no_reply, _Aux, mock_log} = handle_aux(leader, cast, tick, Aux,
                                             Log, MacState),
    [X] = ets:lookup(rabbit_fifo_usage, aux_test),
    meck:unload(),
    ?assert(X > 0.0),
    ok.

%% Utility

init(Conf) -> rabbit_fifo_v0:init(Conf).
apply(Meta, Entry, State) -> rabbit_fifo_v0:apply(Meta, Entry, State).
init_aux(Conf) -> rabbit_fifo_v0:init_aux(Conf).
handle_aux(S, T, C, A, L, M) -> rabbit_fifo_v0:handle_aux(S, T, C, A, L, M).
make_checkout(C, S, M) -> rabbit_fifo_v0:make_checkout(C, S, M).
