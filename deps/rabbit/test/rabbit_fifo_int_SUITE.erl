-module(rabbit_fifo_int_SUITE).

%% rabbit_fifo and rabbit_fifo_client integration suite

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-define(RA_EVENT_TIMEOUT, 5000).
-define(RA_SYSTEM, quorum_queues).

all() ->
    [
     {group, tests}
    ].

all_tests() ->
    [
     basics,
     return,
     rabbit_fifo_returns_correlation,
     resends_lost_command,
     returns_after_down,
     resends_after_lost_applied,
     handles_reject_notification,
     two_quick_enqueues,
     detects_lost_delivery,
     dequeue,
     discard,
     cancel_checkout,
     lost_delivery,
     credit_api_v1,
     credit_api_v2,
     untracked_enqueue,
     flow,
     test_queries,
     duplicate_delivery,
     usage
    ].

groups() ->
    [
     {tests, [shuffle], all_tests()}
    ].

init_per_group(_, Config) ->
    PrivDir = ?config(priv_dir, Config),
    _ = application:load(ra),
    ok = application:set_env(ra, data_dir, PrivDir),
    application:ensure_all_started(ra),
    application:ensure_all_started(lg),
    SysCfg = ra_system:default_config(),
    ra_system:start(SysCfg#{name => ?RA_SYSTEM}),
    Config.

end_per_group(_, Config) ->
    _ = application:stop(ra),
    Config.

init_per_testcase(TestCase, Config) ->
    meck:new(rabbit_quorum_queue, [passthrough]),
    meck:expect(rabbit_quorum_queue, handle_tick, fun (_, _, _) -> ok end),
    meck:expect(rabbit_quorum_queue, file_handle_leader_reservation, fun (_) -> ok end),
    meck:expect(rabbit_quorum_queue, file_handle_other_reservation, fun () -> ok end),
    meck:expect(rabbit_quorum_queue, cancel_consumer_handler, fun (_, _) -> ok end),
    ra_server_sup_sup:remove_all(?RA_SYSTEM),
    ServerName2 = list_to_atom(atom_to_list(TestCase) ++ "2"),
    ServerName3 = list_to_atom(atom_to_list(TestCase) ++ "3"),
    ClusterName = rabbit_misc:r("/", queue, atom_to_binary(TestCase, utf8)),
    [
     {cluster_name, ClusterName},
     {uid, atom_to_binary(TestCase, utf8)},
     {node_id, {TestCase, node()}},
     {uid2, atom_to_binary(ServerName2, utf8)},
     {node_id2, {ServerName2, node()}},
     {uid3, atom_to_binary(ServerName3, utf8)},
     {node_id3, {ServerName3, node()}}
     | Config].

end_per_testcase(_, Config) ->
    meck:unload(),
    Config.

basics(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    UId = ?config(uid, Config),
    ConsumerTag = UId,
    ok = start_cluster(ClusterName, [ServerId]),
    FState0 = rabbit_fifo_client:init([ServerId]),
    {ok, FState1} = rabbit_fifo_client:checkout(ConsumerTag, 1, simple_prefetch,
                                                #{}, FState0),

    rabbit_quorum_queue:wal_force_roll_over(node()),
    % create segment the segment will trigger a snapshot
    timer:sleep(1000),

    {ok, FState2, []} = rabbit_fifo_client:enqueue(ClusterName, one, FState1),

    DeliverFun = fun DeliverFun(S0, F) ->
                         receive
                             {ra_event, From, Evt} ->
                                 ct:pal("ra_event ~p", [Evt]),
                                 case rabbit_fifo_client:handle_ra_event(ClusterName, From, Evt, S0) of
                                     {ok, S1,
                                      [{deliver, C, true,
                                        [{_Qname, _QRef, MsgId, _SomBool, _Msg}]}]} ->
                                         {S, _A} = rabbit_fifo_client:F(C, [MsgId], S1),
                                         %% settle applied event
                                         process_ra_event(ClusterName, S, ?RA_EVENT_TIMEOUT);
                                     {ok, S, _} ->
                                         DeliverFun(S, F)
                                 end
                         after 5000 ->
                                   flush(),
                                   exit(await_delivery_timeout)
                         end
                 end,

    FState5 = DeliverFun(FState2, settle),

    _ = rabbit_quorum_queue:stop_server(ServerId),
    _ = rabbit_quorum_queue:restart_server(ServerId),

    %% wait for leader change to notice server is up again
    FState5b =
    receive
        {ra_event, From, Evt} ->
            ct:pal("ra_event ~p", [Evt]),
            {ok, F6, _} = rabbit_fifo_client:handle_ra_event(ClusterName, From, Evt, FState5),
            F6
    after 5000 ->
              exit(leader_change_timeout)
    end,

    {ok, FState6, []} = rabbit_fifo_client:enqueue(ClusterName, two, FState5b),
    _FState8 = DeliverFun(FState6, return),

    rabbit_quorum_queue:stop_server(ServerId),
    ok.

return(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterName, [ServerId]),

    F00 = rabbit_fifo_client:init([ServerId]),
    {ok, F0, []} = rabbit_fifo_client:enqueue(ClusterName, 1, msg1, F00),
    {ok, F1, []} = rabbit_fifo_client:enqueue(ClusterName, 2, msg2, F0),
    {_, _, F2} = process_ra_events(receive_ra_events(2, 0), ClusterName, F1),
    {ok, _, {_, _, MsgId, _, _}, F} = rabbit_fifo_client:dequeue(ClusterName, <<"tag">>, unsettled, F2),
    _F2 = rabbit_fifo_client:return(<<"tag">>, [MsgId], F),

    rabbit_quorum_queue:stop_server(ServerId),
    ok.

rabbit_fifo_returns_correlation(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterName, [ServerId]),
    F0 = rabbit_fifo_client:init([ServerId]),
    {ok, F1, []} = rabbit_fifo_client:enqueue(ClusterName, corr1, msg1, F0),
    receive
        {ra_event, Frm, E} ->
            case rabbit_fifo_client:handle_ra_event(ClusterName, Frm, E, F1) of
                {ok, _F2, [{settled, _, _}]} ->
                    ok;
                Del ->
                    exit({unexpected, Del})
            end
    after 2000 ->
              exit(await_msg_timeout)
    end,
    rabbit_quorum_queue:stop_server(ServerId),
    ok.

duplicate_delivery(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterName, [ServerId]),
    F0 = rabbit_fifo_client:init([ServerId]),
    {ok, F1} = rabbit_fifo_client:checkout(<<"tag">>, 10, simple_prefetch, #{}, F0),
    {ok, F2, []} = rabbit_fifo_client:enqueue(ClusterName, corr1, msg1, F1),
    Fun = fun Loop(S0) ->
            receive
                {ra_event, Frm, E} = Evt ->
                    case rabbit_fifo_client:handle_ra_event(ClusterName, Frm, E, S0) of
                        {ok, S1, [{settled, _, _}]} ->
                            Loop(S1);
                        {ok, S1, _} ->
                            %% repeat event delivery
                            self() ! Evt,
                            %% check that then next received delivery doesn't
                            %% repeat or crash
                            receive
                                {ra_event, F, E1} ->
                                    case rabbit_fifo_client:handle_ra_event(ClusterName,
                                           F, E1, S1) of
                                        {ok, S2, _} ->
                                            S2
                                    end
                            end
                    end
            after 2000 ->
                      exit(await_msg_timeout)
            end
        end,
    Fun(F2),
    rabbit_quorum_queue:stop_server(ServerId),
    ok.

usage(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterName, [ServerId]),
    F0 = rabbit_fifo_client:init([ServerId]),
    {ok, F1} = rabbit_fifo_client:checkout(<<"tag">>, 10, simple_prefetch, #{}, F0),
    {ok, F2, []} = rabbit_fifo_client:enqueue(ClusterName, corr1, msg1, F1),
    {ok, F3, []} = rabbit_fifo_client:enqueue(ClusterName, corr2, msg2, F2),
    {_, _, _} = process_ra_events(receive_ra_events(2, 2), ClusterName, F3),
    % force tick and usage stats emission
    ServerId ! tick_timeout,
    timer:sleep(50),
    Use = rabbit_fifo:usage(element(1, ServerId)),
    rabbit_quorum_queue:stop_server(ServerId),
    ?assert(Use > 0.0),
    ok.

resends_lost_command(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterName, [ServerId]),

    ok = meck:new(ra, [passthrough]),

    F0 = rabbit_fifo_client:init([ServerId]),
    {ok, F1, []} = rabbit_fifo_client:enqueue(ClusterName, msg1, F0),
    % lose the enqueue
    meck:expect(ra, pipeline_command, fun (_, _, _) -> ok end),
    {ok, F2, []} = rabbit_fifo_client:enqueue(ClusterName, msg2, F1),
    meck:unload(ra),
    {ok, F3, []} = rabbit_fifo_client:enqueue(ClusterName, msg3, F2),
    {_, _, F4} = process_ra_events(receive_ra_events(2, 0), ClusterName, F3),
    {ok, _, {_, _, _, _, msg1}, F5} = rabbit_fifo_client:dequeue(ClusterName, <<"tag">>, settled, F4),
    {ok, _, {_, _, _, _, msg2}, F6} = rabbit_fifo_client:dequeue(ClusterName, <<"tag">>, settled, F5),
    {ok, _, {_, _, _, _, msg3}, _F7} = rabbit_fifo_client:dequeue(ClusterName, <<"tag">>, settled, F6),
    rabbit_quorum_queue:stop_server(ServerId),
    ok.

two_quick_enqueues(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterName, [ServerId]),

    F0 = rabbit_fifo_client:init([ServerId]),
    F1 = element(2, rabbit_fifo_client:enqueue(ClusterName, msg1, F0)),
    {ok, F2, []} = rabbit_fifo_client:enqueue(ClusterName, msg2, F1),
    _ = process_ra_events(receive_ra_events(2, 0), ClusterName, F2),
    rabbit_quorum_queue:stop_server(ServerId),
    ok.

detects_lost_delivery(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterName, [ServerId]),

    F000 = rabbit_fifo_client:init([ServerId]),
    {ok, F00, []} = rabbit_fifo_client:enqueue(ClusterName, msg1, F000),
    {_, _, F0} = process_ra_events(receive_ra_events(1, 0), ClusterName, F00),
    {ok, F1} = rabbit_fifo_client:checkout(<<"tag">>, 10, simple_prefetch, #{}, F0),
    {ok, F2, []} = rabbit_fifo_client:enqueue(ClusterName, msg2, F1),
    {ok, F3, []} = rabbit_fifo_client:enqueue(ClusterName, msg3, F2),
    % lose first delivery
    receive
        {ra_event, _, {machine, {delivery, _, [{_, {_, msg1}}]}}} ->
            ok
    after 5000 ->
              exit(await_delivery_timeout)
    end,

    % assert three deliveries were received
    {[_, _, _], _, _} = process_ra_events(receive_ra_events(2, 2), ClusterName, F3),
    rabbit_quorum_queue:stop_server(ServerId),
    ok.

returns_after_down(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterName, [ServerId]),

    F0 = rabbit_fifo_client:init([ServerId]),
    {ok, F1, []} = rabbit_fifo_client:enqueue(ClusterName, msg1, F0),
    {_, _, F2} = process_ra_events(receive_ra_events(1, 0), ClusterName, F1),
    % start a consumer in a separate processes
    % that exits after checkout
    Self = self(),
    _Pid = spawn(fun () ->
                         F = rabbit_fifo_client:init([ServerId]),
                         {ok, _} = rabbit_fifo_client:checkout(<<"tag">>, 10,
                                                               simple_prefetch,
                                                               #{}, F),
                         Self ! checkout_done
                 end),
    receive checkout_done -> ok after 1000 -> exit(checkout_done_timeout) end,
    timer:sleep(1000),
    % message should be available for dequeue
    {ok, _, {_, _, _, _, msg1}, _} = rabbit_fifo_client:dequeue(ClusterName, <<"tag">>, settled, F2),
    rabbit_quorum_queue:stop_server(ServerId),
    ok.

resends_after_lost_applied(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterName, [ServerId]),

    F0 = rabbit_fifo_client:init([ServerId]),
    {ok, F1, []} = rabbit_fifo_client:enqueue(ClusterName, msg1, F0),
    {_, _, F2} = process_ra_events(receive_ra_events(1, 0), ClusterName, F1),
    {ok, F3, []} = rabbit_fifo_client:enqueue(ClusterName, msg2, F2),
    % lose an applied event
    receive
        {ra_event, _, {applied, _}} ->
            ok
    after 500 ->
              exit(await_ra_event_timeout)
    end,
    % send another message
    {ok, F4, []} = rabbit_fifo_client:enqueue(ClusterName, msg3, F3),
    {_, _, F5} = process_ra_events(receive_ra_events(1, 0), ClusterName, F4),
    {ok, _, {_, _, _, _, msg1}, F6} = rabbit_fifo_client:dequeue(ClusterName, <<"tag">>, settled, F5),
    {ok, _, {_, _, _, _, msg2}, F7} = rabbit_fifo_client:dequeue(ClusterName, <<"tag">>, settled, F6),
    {ok, _, {_, _, _, _, msg3}, _F8} = rabbit_fifo_client:dequeue(ClusterName, <<"tag">>, settled, F7),
    rabbit_quorum_queue:stop_server(ServerId),
    ok.

handles_reject_notification(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId1 = ?config(node_id, Config),
    ServerId2 = ?config(node_id2, Config),
    UId1 = ?config(uid, Config),
    CId = {UId1, self()},

    ok = start_cluster(ClusterName, [ServerId1, ServerId2]),
    _ = ra:process_command(ServerId1,
                           rabbit_fifo:make_checkout(
                             CId,
                             {auto, 10, simple_prefetch},
                             #{})),
    % reverse order - should try the first node in the list first
    F0 = rabbit_fifo_client:init([ServerId2, ServerId1]),
    {ok, F1, []} = rabbit_fifo_client:enqueue(ClusterName, one, F0),

    timer:sleep(500),

    % the applied notification
    _F2 = process_ra_events(receive_ra_events(1, 0), ClusterName, F1),
    rabbit_quorum_queue:stop_server(ServerId1),
    rabbit_quorum_queue:stop_server(ServerId2),
    ok.

discard(Config) ->
    PrivDir = ?config(priv_dir, Config),
    ServerId = ?config(node_id, Config),
    UId = ?config(uid, Config),
    ClusterName = ?config(cluster_name, Config),
    Conf = #{cluster_name => ClusterName#resource.name,
             id => ServerId,
             uid => UId,
             log_init_args => #{data_dir => PrivDir, uid => UId},
             initial_member => [],
             machine => {module, rabbit_fifo,
                         #{queue_resource => discard,
                           dead_letter_handler =>
                           {at_most_once, {?MODULE, dead_letter_handler, [self()]}}}}},
    _ = rabbit_quorum_queue:start_server(Conf),
    ok = ra:trigger_election(ServerId),
    _ = ra:members(ServerId),

    F0 = rabbit_fifo_client:init([ServerId]),
    {ok, F1} = rabbit_fifo_client:checkout(<<"tag">>, 10,
                                           simple_prefetch, #{}, F0),
    {ok, F2, []} = rabbit_fifo_client:enqueue(ClusterName, msg1, F1),
    F3 = discard_next_delivery(ClusterName, F2, 5000),
    {empty, _F4} = rabbit_fifo_client:dequeue(ClusterName, <<"tag1">>, settled, F3),
    receive
        {dead_letter, Reason, Letters} ->
            [msg1] = Letters,
            rejected = Reason,
            ok
    after 500 ->
              flush(),
              exit(dead_letter_timeout)
    end,
    rabbit_quorum_queue:stop_server(ServerId),
    ok.

cancel_checkout(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterName, [ServerId]),
    F0 = rabbit_fifo_client:init([ServerId], 4),
    {ok, F1, []} = rabbit_fifo_client:enqueue(ClusterName, m1, F0),
    {ok, F2} = rabbit_fifo_client:checkout(<<"tag">>, 10, simple_prefetch, #{}, F1),
    {_, _, F3} = process_ra_events(receive_ra_events(1, 1), ClusterName, F2, [], [], fun (_, S) -> S end),
    {ok, F4} = rabbit_fifo_client:cancel_checkout(<<"tag">>, F3),
    {F5, _} = rabbit_fifo_client:return(<<"tag">>, [0], F4),
    {ok, _, {_, _, _, _, m1}, F5} = rabbit_fifo_client:dequeue(ClusterName, <<"d1">>, settled, F5),
    ok.

lost_delivery(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterName, [ServerId]),
    F0 = rabbit_fifo_client:init([ServerId], 4),
    {ok, F1, []} = rabbit_fifo_client:enqueue(ClusterName, m1, F0),
    {_, _, F2} = process_ra_events(
                   receive_ra_events(1, 0), ClusterName, F1, [], [], fun (_, S) -> S end),
    {ok, F3} = rabbit_fifo_client:checkout(<<"tag">>, 10, simple_prefetch, #{}, F2),
    %% drop a delivery, simulating e.g. a full distribution buffer
    receive
        {ra_event, _, Evt} ->
            ct:pal("dropping event ~tp", [Evt]),
            ok
    after 500 ->
              exit(await_ra_event_timeout)
    end,
    % send another message
    {ok, F4, []} = rabbit_fifo_client:enqueue(ClusterName, m2, F3),
    %% this hsould trigger the fifo client to fetch any missing messages
    %% from the server
    {_, _, _F5} = process_ra_events(
                    receive_ra_events(1, 1), ClusterName, F4, [], [],
                    fun ({deliver, _, _, Dels}, S) ->
                            [{_, _, _, _, M1},
                             {_, _, _, _, M2}] = Dels,
                            ?assertEqual(m1, M1),
                            ?assertEqual(m2, M2),
                            S
                    end),
    ok.

credit_api_v1(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterName, [ServerId]),
    F0 = rabbit_fifo_client:init([ServerId], 4),
    {ok, F1, []} = rabbit_fifo_client:enqueue(ClusterName, m1, F0),
    {ok, F2, []} = rabbit_fifo_client:enqueue(ClusterName, m2, F1),
    {_, _, F3} = process_ra_events(receive_ra_events(2, 0), ClusterName, F2),
    %% checkout with 0 prefetch
    CTag = <<"my-tag">>,
    {ok, F4} = rabbit_fifo_client:checkout(CTag, 0, credited, #{}, F3),
    %% assert no deliveries
    {_, _, F5} = process_ra_events(receive_ra_events(), ClusterName, F4, [], [],
                                   fun
                                       (D, _) -> error({unexpected_delivery, D})
                                   end),
    %% provide some credit
    {F6, []} = rabbit_fifo_client:credit_v1(CTag, 1, false, F5),
    {[{_, _, _, _, m1}], [{send_credit_reply, 1}], F7} =
    process_ra_events(receive_ra_events(1, 1), ClusterName, F6),

    %% credit and drain
    Drain = true,
    {F8, []} = rabbit_fifo_client:credit_v1(CTag, 4, Drain, F7),
    AvailableAfterCheckout = 0,
    {[{_, _, _, _, m2}],
     [{send_credit_reply, AvailableAfterCheckout},
      {credit_reply_v1, CTag, _CreditAfterCheckout = 3,
       AvailableAfterCheckout, Drain}],
     F9} = process_ra_events(receive_ra_events(2, 1), ClusterName, F8),
    flush(),

    %% enqueue another message - at this point the consumer credit should be
    %% all used up due to the drain
    {ok, F10, []} = rabbit_fifo_client:enqueue(ClusterName, m3, F9),
    %% assert no deliveries
    {_, _, F11} = process_ra_events(receive_ra_events(), ClusterName, F10, [], [],
                                    fun
                                        (D, _) -> error({unexpected_delivery, D})
                                    end),
    %% credit again and receive the last message
    {F12, []} = rabbit_fifo_client:credit_v1(CTag, 10, false, F11),
    {[{_, _, _, _, m3}], _, _} = process_ra_events(receive_ra_events(1, 1), ClusterName, F12),
    ok.

credit_api_v2(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterName, [ServerId]),
    F0 = rabbit_fifo_client:init([ServerId], 4),
    %% Enqueue 2 messages.
    {ok, F1, []} = rabbit_fifo_client:enqueue(ClusterName, m1, F0),
    {ok, F2, []} = rabbit_fifo_client:enqueue(ClusterName, m2, F1),
    {_, _, F3} = process_ra_events(receive_ra_events(2, 0), ClusterName, F2),
    CTag = <<"my-tag">>,
    DC0 = 16#ff_ff_ff_ff,
    DC1 = 0, %% = DC0 + 1 using 32 bit serial number arithmetic
    {ok, F4} = rabbit_fifo_client:checkout(
                 %% initial_delivery_count in consumer meta means credit API v2.
                 CTag, 0, credited, #{initial_delivery_count => DC0}, F3),
    %% assert no deliveries
    {_, _, F5} = process_ra_events(receive_ra_events(), ClusterName, F4, [], [],
                                   fun
                                       (D, _) -> error({unexpected_delivery, D})
                                   end),
    %% Grant 1 credit.
    {F6, []} = rabbit_fifo_client:credit(CTag, DC0, 1, false, _Echo0 = true, F5),
    %% We expect exactly 1 message due to 1 credit being granted.
    {[{_, _, _, _, m1}],
     %% We expect a credit_reply action due to echo=true
     [{credit_reply, CTag, DC1, _Credit0 = 0, _Available0 = 1, _Drain0 = false}],
     F7} = process_ra_events(receive_ra_events(), ClusterName, F6),

    %% Again, grant 1 credit.
    %% However, because we still use the initial delivery count DC0, rabbit_fifo
    %% wont' send us a new message since it already sent us m1 for that old delivery-count.
    %% In other words, this credit top up simulates in-flight deliveries.
    {F8, []} = rabbit_fifo_client:credit(CTag, DC0, 1, false, _Echo1 = true, F7),
    {_NoMessages = [],
     %% We still expect a credit_reply action due to echo=true
     [{credit_reply, CTag, DC1, _Credit1 = 0, _Available1 = 1, _Drain1 = false}],
     F9} = process_ra_events(receive_ra_events(), ClusterName, F8),

    %% Grant 4 credits and drain.
    {F10, []} = rabbit_fifo_client:credit(CTag, DC1, 4, true, _Echo2 = false, F9),
    %% rabbit_fifo should advance the delivery-count as much as possible
    %% consuming all credits due to drain=true and insufficient messages in the queue.
    DC2 = DC1 + 4,
    %% We expect to receive m2 which is the only message in the queue.
    {[{_, _, _, _, m2}],
     %% Even though echo=false, we still expect a credit_reply action due
     %% drain=true and insufficient messages in the queue.
     [{credit_reply, CTag, DC2, _Credit2 = 0, _Available2 = 0, _Drain2 = true}],
     F11} = process_ra_events(receive_ra_events(), ClusterName, F10),
    flush(),

    %% Enqueue another message.
    %% At this point the consumer credit should be all used up due to the drain.
    {ok, F12, []} = rabbit_fifo_client:enqueue(ClusterName, m3, F11),
    %% assert no deliveries
    {_, _, F13} = process_ra_events(receive_ra_events(), ClusterName, F12, [], [],
                                    fun
                                        (D, _) -> error({unexpected_delivery, D})
                                    end),

    %% Grant 10 credits and receive the last message.
    {F14, []} = rabbit_fifo_client:credit(CTag, DC2, 10, false, _Echo = false, F13),
    ?assertMatch(
       {[{_, _, _, _, m3}],
        %% Due to echo=false, we don't expect a credit_reply action.
        _NoCreditReplyAction = [],
        _F15}, process_ra_events(receive_ra_events(), ClusterName, F14)).

untracked_enqueue(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterName, [ServerId]),

    ok = rabbit_fifo_client:untracked_enqueue([ServerId], msg1),
    timer:sleep(100),
    F0 = rabbit_fifo_client:init([ServerId]),
    {ok, _, {_, _, _, _, msg1}, _F5} = rabbit_fifo_client:dequeue(ClusterName, <<"tag">>, settled, F0),
    rabbit_quorum_queue:stop_server(ServerId),
    ok.


flow(Config) ->
    ClusterName = ?config(cluster_name, Config),
    {Name, _Node} = ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterName, [ServerId]),
    F0 = rabbit_fifo_client:init([ServerId], 3),
    {ok, F1, []} = rabbit_fifo_client:enqueue(ClusterName, m1, F0),
    {ok, F2, []} = rabbit_fifo_client:enqueue(ClusterName, m2, F1),
    {ok, F3, []} = rabbit_fifo_client:enqueue(ClusterName, m3, F2),
    {ok, F4, [{block, Name}]} = rabbit_fifo_client:enqueue(ClusterName, m4, F3),
    {_, Actions, F5} = process_ra_events(receive_ra_events(4, 0), ClusterName, F4),
    true = lists:member({unblock, Name}, Actions),
    {ok, _, []} = rabbit_fifo_client:enqueue(ClusterName, m5, F5),
    rabbit_quorum_queue:stop_server(ServerId),
    ok.

test_queries(Config) ->
    % ok = logger:set_primary_config(level, all),
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterName, [ServerId]),
    Self = self(),
    P = spawn(fun () ->
                  F0 = rabbit_fifo_client:init([ServerId], 4),
                  {ok, F1, []} = rabbit_fifo_client:enqueue(ClusterName, m1, F0),
                  {ok, F2, []} = rabbit_fifo_client:enqueue(ClusterName, m2, F1),
                  process_ra_events(receive_ra_events(2, 0), ClusterName, F2),
                  Self ! ready,
                  receive stop -> ok end
          end),
    receive
        ready -> ok
    after 5000 ->
              exit(ready_timeout)
    end,
    F0 = rabbit_fifo_client:init([ServerId], 4),
    {ok, _} = rabbit_fifo_client:checkout(<<"tag">>, 1, simple_prefetch, #{}, F0),
    {ok, {_, Ready}, _} = ra:local_query(ServerId,
                                         fun rabbit_fifo:query_messages_ready/1),
    ?assertEqual(1, Ready),
    {ok, {_, Checked}, _} = ra:local_query(ServerId,
                                           fun rabbit_fifo:query_messages_checked_out/1),
    ?assertEqual(1, Checked),
    {ok, {_, Processes}, _} = ra:local_query(ServerId,
                                             fun rabbit_fifo:query_processes/1),
    ?assertEqual(2, length(Processes)),
    P ! stop,
    rabbit_quorum_queue:stop_server(ServerId),
    ok.

dead_letter_handler(Pid, Reason, Msgs) ->
    Pid ! {dead_letter, Reason, Msgs}.

dequeue(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    UId = ?config(uid, Config),
    Tag = UId,
    ok = start_cluster(ClusterName, [ServerId]),
    F1 = rabbit_fifo_client:init([ServerId]),
    {empty, F1b} = rabbit_fifo_client:dequeue(ClusterName, Tag, settled, F1),
    {ok, F2_, []} = rabbit_fifo_client:enqueue(ClusterName, msg1, F1b),
    {_, _, F2} = process_ra_events(receive_ra_events(1, 0), ClusterName, F2_),

    % {ok, {{0, {_, msg1}}, _}, F3} = rabbit_fifo_client:dequeue(ClusterName, Tag, settled, F2),
    {ok, _, {_, _, 0, _, msg1}, F3} = rabbit_fifo_client:dequeue(ClusterName, Tag, settled, F2),
    {ok, F4_, []} = rabbit_fifo_client:enqueue(ClusterName, msg2, F3),
    {_, _, F4} = process_ra_events(receive_ra_events(1, 0), ClusterName, F4_),
    {ok, _, {_, _, MsgId, _, msg2}, F5} = rabbit_fifo_client:dequeue(ClusterName, Tag, unsettled, F4),
    {_F6, _A} = rabbit_fifo_client:settle(Tag, [MsgId], F5),
    rabbit_quorum_queue:stop_server(ServerId),
    ok.

conf(ClusterName, UId, ServerId, _, Peers) ->
    #{cluster_name => ClusterName,
      id => ServerId,
      uid => UId,
      log_init_args => #{uid => UId},
      initial_members => Peers,
      machine => {module, rabbit_fifo, #{}}}.

process_ra_event(ClusterName, State, Wait) ->
    receive
        {ra_event, From, Evt} ->
            ct:pal("Ra_event ~p", [Evt]),
            {ok, S, _Actions} =
            rabbit_fifo_client:handle_ra_event(ClusterName, From, Evt, State),
            S
    after Wait ->
              flush(),
              exit(ra_event_timeout)
    end.

receive_ra_events(Applied, Deliveries) ->
    receive_ra_events(Applied, Deliveries, []).

receive_ra_events(Applied, Deliveries, Acc)
  when Applied =< 0 andalso Deliveries =< 0 ->
    %% what if we get more events? Testcases should check what they're!
    lists:reverse(Acc);
receive_ra_events(Applied, Deliveries, Acc) ->
    receive
        {ra_event, _, {applied, Seqs}} = Evt ->
            receive_ra_events(Applied - length(Seqs), Deliveries, [Evt | Acc]);
        {ra_event, _, {machine, {delivery, _, MsgIds}}} = Evt ->
            receive_ra_events(Applied, Deliveries - length(MsgIds), [Evt | Acc]);
        {ra_event, _, _} = Evt ->
            receive_ra_events(Applied, Deliveries, [Evt | Acc])
    after 5000 ->
            exit({missing_events, Applied, Deliveries, Acc})
    end.

%% Flusing the mailbox to later check that deliveries hasn't been received
receive_ra_events() ->
    receive_ra_events([]).

receive_ra_events(Acc) ->
    receive
        {ra_event, _, _} = Evt ->
            receive_ra_events([Evt | Acc])
    after 500 ->
            Acc
    end.

process_ra_events(Events, ClusterName, State) ->
    DeliveryFun = fun ({deliver, _, Tag, Msgs}, S) ->
                          MsgIds = [element(1, M) || M <- Msgs],
                          {S0, _} = rabbit_fifo_client:settle(Tag, MsgIds, S),
                          S0
                  end,
    process_ra_events(Events, ClusterName, State, [], [], DeliveryFun).

process_ra_events([], _ClusterName, State0, Acc, Actions0, _DeliveryFun) ->
    {Acc, Actions0, State0};
process_ra_events([{ra_event, From, Evt} | Events], ClusterName, State0, Acc,
                  Actions0, DeliveryFun) ->
    case rabbit_fifo_client:handle_ra_event(ClusterName, From, Evt, State0) of
        {ok, State1, Actions1} ->
            {Msgs, Actions, State} =
                lists:foldl(
                  fun ({deliver, _, _, Msgs} = Del, {M, A, S}) ->
                          {M ++ Msgs, A, DeliveryFun(Del, S)};
                      (Ac, {M, A, S}) ->
                          {M, A ++ [Ac], S}
                  end, {Acc, [], State1}, Actions1),
            process_ra_events(Events, ClusterName, State, Msgs, Actions0 ++ Actions, DeliveryFun);
        eol ->
            eol
    end.

discard_next_delivery(ClusterName, State0, Wait) ->
    receive
        {ra_event, _, {machine, {delivery, _, _}}} = Evt ->
            element(3, process_ra_events([Evt], ClusterName, State0, [], [],
                                         fun ({deliver, Tag, _, Msgs}, S) ->
                                                 MsgIds = [element(3, M) || M <- Msgs],
                                                 {S0, _} = rabbit_fifo_client:discard(Tag, MsgIds, S),
                                                 S0
                                         end))
    after Wait ->
            State0
    end.

start_cluster(ClusterName, ServerIds, RaFifoConfig) ->
    {ok, Started, _} = ra:start_cluster(?RA_SYSTEM,
                                        ClusterName#resource.name,
                                        {module, rabbit_fifo, RaFifoConfig},
                                        ServerIds),
    ?assertEqual(length(Started), length(ServerIds)),
    ok.

start_cluster(ClusterName, ServerIds) ->
    start_cluster(ClusterName, ServerIds, #{name => some_name,
                                            queue_resource => ClusterName}).

flush() ->
    receive
        Msg ->
            ct:pal("flushed: ~w~n", [Msg]),
            flush()
    after 10 ->
              ok
    end.
