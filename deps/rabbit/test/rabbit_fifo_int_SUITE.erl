-module(rabbit_fifo_int_SUITE).

%% rabbit_fifo and rabbit_fifo_client integration suite

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
     credit,
     untracked_enqueue,
     flow,
     test_queries,
     duplicate_delivery,
     usage
    ].

groups() ->
    [
     {tests, [], all_tests()}
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
    meck:expect(rabbit_quorum_queue, cancel_consumer_handler,
                fun (_, _) -> ok end),
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
    CustomerTag = UId,
    ok = start_cluster(ClusterName, [ServerId]),
    FState0 = rabbit_fifo_client:init(ClusterName, [ServerId]),
    {ok, FState1} = rabbit_fifo_client:checkout(CustomerTag, 1, simple_prefetch,
                                                #{}, FState0),

    rabbit_quorum_queue:wal_force_roll_over(node()),
    % create segment the segment will trigger a snapshot
    timer:sleep(1000),

    {ok, FState2} = rabbit_fifo_client:enqueue(one, FState1),
    % process ra events
    FState3 = process_ra_event(FState2, ?RA_EVENT_TIMEOUT),

    FState5 = receive
                  {ra_event, From, Evt} ->
                      case rabbit_fifo_client:handle_ra_event(From, Evt, FState3) of
                          {ok, FState4,
                           [{deliver, C, true,
                             [{_Qname, _QRef, MsgId, _SomBool, _Msg}]}]} ->
                              {S, _A} = rabbit_fifo_client:settle(C, [MsgId], FState4),
                              S
                      end
              after 5000 ->
                        exit(await_msg_timeout)
              end,

    % process settle applied notification
    FState5b = process_ra_event(FState5, ?RA_EVENT_TIMEOUT),
    _ = rabbit_quorum_queue:stop_server(ServerId),
    _ = rabbit_quorum_queue:restart_server(ServerId),

    %% wait for leader change to notice server is up again
    receive
        {ra_event, _, {machine, leader_change}} -> ok
    after 5000 ->
              exit(leader_change_timeout)
    end,

    {ok, FState6} = rabbit_fifo_client:enqueue(two, FState5b),
    % process applied event
    FState6b = process_ra_event(FState6, ?RA_EVENT_TIMEOUT),

    receive
        {ra_event, Frm, E} ->
            case rabbit_fifo_client:handle_ra_event(Frm, E, FState6b) of
                {ok, FState7, [{deliver, Ctag, true,
                                [{_, _, Mid, _, two}]}]} ->
                    {_, _} = rabbit_fifo_client:return(Ctag, [Mid], FState7),
                    ok
            end
    after 5000 ->
              exit(await_msg_timeout)
    end,
    rabbit_quorum_queue:stop_server(ServerId),
    ok.

return(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterName, [ServerId]),

    F00 = rabbit_fifo_client:init(ClusterName, [ServerId]),
    {ok, F0} = rabbit_fifo_client:enqueue(1, msg1, F00),
    {ok, F1} = rabbit_fifo_client:enqueue(2, msg2, F0),
    {_, _, F2} = process_ra_events(receive_ra_events(2, 0), F1),
    {ok, _, {_, _, MsgId, _, _}, F} = rabbit_fifo_client:dequeue(<<"tag">>, unsettled, F2),
    _F2 = rabbit_fifo_client:return(<<"tag">>, [MsgId], F),

    rabbit_quorum_queue:stop_server(ServerId),
    ok.

rabbit_fifo_returns_correlation(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterName, [ServerId]),
    F0 = rabbit_fifo_client:init(ClusterName, [ServerId]),
    {ok, F1} = rabbit_fifo_client:enqueue(corr1, msg1, F0),
    receive
        {ra_event, Frm, E} ->
            case rabbit_fifo_client:handle_ra_event(Frm, E, F1) of
                {ok, _F2, [{settled, _, _}]} ->
                    ok;
                Del ->
                    exit({unexpected, Del})
            end
    after 5000 ->
              exit(await_msg_timeout)
    end,
    rabbit_quorum_queue:stop_server(ServerId),
    ok.

duplicate_delivery(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterName, [ServerId]),
    F0 = rabbit_fifo_client:init(ClusterName, [ServerId]),
    {ok, F1} = rabbit_fifo_client:checkout(<<"tag">>, 10, simple_prefetch, #{}, F0),
    {ok, F2} = rabbit_fifo_client:enqueue(corr1, msg1, F1),
    Fun = fun Loop(S0) ->
            receive
                {ra_event, Frm, E} = Evt ->
                    case rabbit_fifo_client:handle_ra_event(Frm, E, S0) of
                        {ok, S1, [{settled, _, _}]} ->
                            Loop(S1);
                        {ok, S1, _} ->
                            %% repeat event delivery
                            self() ! Evt,
                            %% check that then next received delivery doesn't
                            %% repeat or crash
                            receive
                                {ra_event, F, E1} ->
                                    case rabbit_fifo_client:handle_ra_event(
                                           F, E1, S1) of
                                        {ok, S2, _} ->
                                            S2
                                    end
                            end
                    end
            after 5000 ->
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
    F0 = rabbit_fifo_client:init(ClusterName, [ServerId]),
    {ok, F1} = rabbit_fifo_client:checkout(<<"tag">>, 10, simple_prefetch, #{}, F0),
    {ok, F2} = rabbit_fifo_client:enqueue(corr1, msg1, F1),
    {ok, F3} = rabbit_fifo_client:enqueue(corr2, msg2, F2),
    {_, _, _} = process_ra_events(receive_ra_events(2, 2), F3),
    % force tick and usage stats emission
    ServerId ! tick_timeout,
    timer:sleep(100),
    Use = rabbit_fifo:usage(element(1, ServerId)),
    rabbit_quorum_queue:stop_server(ServerId),
    ?assert(Use > 0.0),
    ok.

resends_lost_command(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterName, [ServerId]),

    ok = meck:new(ra, [passthrough]),

    F0 = rabbit_fifo_client:init(ClusterName, [ServerId]),
    {ok, F1} = rabbit_fifo_client:enqueue(msg1, F0),
    % lose the enqueue
    meck:expect(ra, pipeline_command, fun (_, _, _) -> ok end),
    {ok, F2} = rabbit_fifo_client:enqueue(msg2, F1),
    meck:unload(ra),
    {ok, F3} = rabbit_fifo_client:enqueue(msg3, F2),
    {_, _, F4} = process_ra_events(receive_ra_events(2, 0), F3),
    {ok, _, {_, _, _, _, msg1}, F5} = rabbit_fifo_client:dequeue(<<"tag">>, settled, F4),
    {ok, _, {_, _, _, _, msg2}, F6} = rabbit_fifo_client:dequeue(<<"tag">>, settled, F5),
    {ok, _, {_, _, _, _, msg3}, _F7} = rabbit_fifo_client:dequeue(<<"tag">>, settled, F6),
    rabbit_quorum_queue:stop_server(ServerId),
    ok.

two_quick_enqueues(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterName, [ServerId]),

    F0 = rabbit_fifo_client:init(ClusterName, [ServerId]),
    F1 = element(2, rabbit_fifo_client:enqueue(msg1, F0)),
    {ok, F2} = rabbit_fifo_client:enqueue(msg2, F1),
    _ = process_ra_events(receive_ra_events(2, 0), F2),
    rabbit_quorum_queue:stop_server(ServerId),
    ok.

detects_lost_delivery(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterName, [ServerId]),

    F000 = rabbit_fifo_client:init(ClusterName, [ServerId]),
    {ok, F00} = rabbit_fifo_client:enqueue(msg1, F000),
    {_, _, F0} = process_ra_events(receive_ra_events(1, 0), F00),
    {ok, F1} = rabbit_fifo_client:checkout(<<"tag">>, 10, simple_prefetch, #{}, F0),
    {ok, F2} = rabbit_fifo_client:enqueue(msg2, F1),
    {ok, F3} = rabbit_fifo_client:enqueue(msg3, F2),
    % lose first delivery
    receive
        {ra_event, _, {machine, {delivery, _, [{_, {_, msg1}}]}}} ->
            ok
    after 5000 ->
              exit(await_delivery_timeout)
    end,

    % assert three deliveries were received
    {[_, _, _], _, _} = process_ra_events(receive_ra_events(2, 2), F3),
    rabbit_quorum_queue:stop_server(ServerId),
    ok.

returns_after_down(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterName, [ServerId]),

    F0 = rabbit_fifo_client:init(ClusterName, [ServerId]),
    {ok, F1} = rabbit_fifo_client:enqueue(msg1, F0),
    {_, _, F2} = process_ra_events(receive_ra_events(1, 0), F1),
    % start a customer in a separate processes
    % that exits after checkout
    Self = self(),
    _Pid = spawn(fun () ->
                         F = rabbit_fifo_client:init(ClusterName, [ServerId]),
                         {ok, _} = rabbit_fifo_client:checkout(<<"tag">>, 10,
                                                               simple_prefetch,
                                                               #{}, F),
                         Self ! checkout_done
                 end),
    receive checkout_done -> ok after 1000 -> exit(checkout_done_timeout) end,
    timer:sleep(2000),
    % message should be available for dequeue
    {ok, _, {_, _, _, _, msg1}, _} = rabbit_fifo_client:dequeue(<<"tag">>, settled, F2),
    rabbit_quorum_queue:stop_server(ServerId),
    ok.

resends_after_lost_applied(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterName, [ServerId]),

    F0 = rabbit_fifo_client:init(ClusterName, [ServerId]),
    {ok, F1} = rabbit_fifo_client:enqueue(msg1, F0),
    {_, _, F2} = process_ra_events(receive_ra_events(1, 0), F1),
    {ok, F3} = rabbit_fifo_client:enqueue(msg2, F2),
    % lose an applied event
    receive
        {ra_event, _, {applied, _}} ->
            ok
    after 2000 ->
              exit(await_ra_event_timeout)
    end,
    % send another message
    {ok, F4} = rabbit_fifo_client:enqueue(msg3, F3),
    {_, _, F5} = process_ra_events(receive_ra_events(1, 0), F4),
    {ok, _, {_, _, _, _, msg1}, F6} = rabbit_fifo_client:dequeue(<<"tag">>, settled, F5),
    {ok, _, {_, _, _, _, msg2}, F7} = rabbit_fifo_client:dequeue(<<"tag">>, settled, F6),
    {ok, _, {_, _, _, _, msg3}, _F8} = rabbit_fifo_client:dequeue(<<"tag">>, settled, F7),
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
    F0 = rabbit_fifo_client:init(ClusterName, [ServerId2, ServerId1]),
    {ok, F1} = rabbit_fifo_client:enqueue(one, F0),

    timer:sleep(1000),

    % the applied notification
    _F2 = process_ra_events(receive_ra_events(1, 0), F1),
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
                           {?MODULE, dead_letter_handler, [self()]}}}},
    _ = rabbit_quorum_queue:start_server(Conf),
    ok = ra:trigger_election(ServerId),
    _ = ra:members(ServerId),

    F0 = rabbit_fifo_client:init(ClusterName, [ServerId]),
    {ok, F1} = rabbit_fifo_client:checkout(<<"tag">>, 10,
                                           simple_prefetch, #{}, F0),
    {ok, F2} = rabbit_fifo_client:enqueue(msg1, F1),
    F3 = discard_next_delivery(F2, 5000),
    {empty, _F4} = rabbit_fifo_client:dequeue(<<"tag1">>, settled, F3),
    receive
        {dead_letter, Letters} ->
            [{_, msg1}] = Letters,
            ok
    after 1000 ->
              flush(),
              exit(dead_letter_timeout)
    end,
    rabbit_quorum_queue:stop_server(ServerId),
    ok.

cancel_checkout(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterName, [ServerId]),
    F0 = rabbit_fifo_client:init(ClusterName, [ServerId], 4),
    {ok, F1} = rabbit_fifo_client:enqueue(m1, F0),
    {ok, F2} = rabbit_fifo_client:checkout(<<"tag">>, 10, simple_prefetch, #{}, F1),
    {_, _, F3} = process_ra_events(receive_ra_events(1, 1), F2, [], [], fun (_, S) -> S end),
    {ok, F4} = rabbit_fifo_client:cancel_checkout(<<"tag">>, F3),
    {F5, _} = rabbit_fifo_client:return(<<"tag">>, [0], F4),
    {ok, _, {_, _, _, _, m1}, F5} = rabbit_fifo_client:dequeue(<<"d1">>, settled, F5),
    ok.

credit(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterName, [ServerId]),
    F0 = rabbit_fifo_client:init(ClusterName, [ServerId], 4),
    {ok, F1} = rabbit_fifo_client:enqueue(m1, F0),
    {ok, F2} = rabbit_fifo_client:enqueue(m2, F1),
    {_, _, F3} = process_ra_events(receive_ra_events(2, 0), F2),
    %% checkout with 0 prefetch
    {ok, F4} = rabbit_fifo_client:checkout(<<"tag">>, 0, credited, #{}, F3),
    %% assert no deliveries
    {_, _, F5} = process_ra_events(receive_ra_events(), F4, [], [],
                                   fun
                                       (D, _) -> error({unexpected_delivery, D})
                                   end),
    %% provide some credit
    {F6, []} = rabbit_fifo_client:credit(<<"tag">>, 1, false, F5),
    {[{_, _, _, _, m1}], [{send_credit_reply, _}], F7} =
        process_ra_events(receive_ra_events(1, 1), F6),

    %% credit and drain
    {F8, []} = rabbit_fifo_client:credit(<<"tag">>, 4, true, F7),
    {[{_, _, _, _, m2}], [{send_credit_reply, _}, {send_drained, _}], F9} =
        process_ra_events(receive_ra_events(1, 1), F8),
    flush(),

    %% enqueue another message - at this point the consumer credit should be
    %% all used up due to the drain
    {ok, F10} = rabbit_fifo_client:enqueue(m3, F9),
    %% assert no deliveries
    {_, _, F11} = process_ra_events(receive_ra_events(), F10, [], [],
                                    fun
                                        (D, _) -> error({unexpected_delivery, D})
                                    end),
    %% credit again and receive the last message
    {F12, []} = rabbit_fifo_client:credit(<<"tag">>, 10, false, F11),
    {[{_, _, _, _, m3}], _, _} = process_ra_events(receive_ra_events(1, 1), F12),
    ok.

untracked_enqueue(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterName, [ServerId]),

    ok = rabbit_fifo_client:untracked_enqueue([ServerId], msg1),
    timer:sleep(500),
    F0 = rabbit_fifo_client:init(ClusterName, [ServerId]),
    {ok, _, {_, _, _, _, msg1}, _F5} = rabbit_fifo_client:dequeue(<<"tag">>, settled, F0),
    rabbit_quorum_queue:stop_server(ServerId),
    ok.


flow(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterName, [ServerId]),
    F0 = rabbit_fifo_client:init(ClusterName, [ServerId], 3),
    {ok, F1} = rabbit_fifo_client:enqueue(m1, F0),
    {ok, F2} = rabbit_fifo_client:enqueue(m2, F1),
    {ok, F3} = rabbit_fifo_client:enqueue(m3, F2),
    {slow, F4} = rabbit_fifo_client:enqueue(m4, F3),
    {_, _, F5} = process_ra_events(receive_ra_events(4, 0), F4),
    {ok, _} = rabbit_fifo_client:enqueue(m5, F5),
    rabbit_quorum_queue:stop_server(ServerId),
    ok.

test_queries(Config) ->
    % ok = logger:set_primary_config(level, all),
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterName, [ServerId]),
    Self = self(),
    P = spawn(fun () ->
                  F0 = rabbit_fifo_client:init(ClusterName, [ServerId], 4),
                  {ok, F1} = rabbit_fifo_client:enqueue(m1, F0),
                  {ok, F2} = rabbit_fifo_client:enqueue(m2, F1),
                  process_ra_events(receive_ra_events(2, 0), F2),
                  Self ! ready,
                  receive stop -> ok end
          end),
    receive
        ready -> ok
    after 5000 ->
              exit(ready_timeout)
    end,
    F0 = rabbit_fifo_client:init(ClusterName, [ServerId], 4),
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

dead_letter_handler(Pid, Msgs) ->
    Pid ! {dead_letter, Msgs}.

dequeue(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    UId = ?config(uid, Config),
    Tag = UId,
    ok = start_cluster(ClusterName, [ServerId]),
    F1 = rabbit_fifo_client:init(ClusterName, [ServerId]),
    {empty, F1b} = rabbit_fifo_client:dequeue(Tag, settled, F1),
    {ok, F2_} = rabbit_fifo_client:enqueue(msg1, F1b),
    {_, _, F2} = process_ra_events(receive_ra_events(1, 0), F2_),

    % {ok, {{0, {_, msg1}}, _}, F3} = rabbit_fifo_client:dequeue(Tag, settled, F2),
    {ok, _, {_, _, 0, _, msg1}, F3} = rabbit_fifo_client:dequeue(Tag, settled, F2),
    {ok, F4_} = rabbit_fifo_client:enqueue(msg2, F3),
    {_, _, F4} = process_ra_events(receive_ra_events(1, 0), F4_),
    {ok, _, {_, _, MsgId, _, msg2}, F5} = rabbit_fifo_client:dequeue(Tag, unsettled, F4),
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

process_ra_event(State, Wait) ->
    receive
        {ra_event, From, Evt} ->
            {ok, S, _Actions} =
                rabbit_fifo_client:handle_ra_event(From, Evt, State),
            S
    after Wait ->
              exit(ra_event_timeout)
    end.

receive_ra_events(Applied, Deliveries) ->
    receive_ra_events(Applied, Deliveries, []).

receive_ra_events(Applied, Deliveries, Acc) when Applied =< 0, Deliveries =< 0->
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
    after 1000 ->
            Acc
    end.

process_ra_events(Events, State) ->
    DeliveryFun = fun ({deliver, _, Tag, Msgs}, S) ->
                          MsgIds = [element(1, M) || M <- Msgs],
                          {S0, _} = rabbit_fifo_client:settle(Tag, MsgIds, S),
                          S0
                  end,
    process_ra_events(Events, State, [], [], DeliveryFun).

process_ra_events([], State0, Acc, Actions0, _DeliveryFun) ->
    {Acc, Actions0, State0};
process_ra_events([{ra_event, From, Evt} | Events], State0, Acc, Actions0, DeliveryFun) ->
    case rabbit_fifo_client:handle_ra_event(From, Evt, State0) of
        {ok, State1, Actions1} ->
            {Msgs, Actions, State} =
                lists:foldl(
                  fun ({deliver, _, _, Msgs} = Del, {M, A, S}) ->
                          {M ++ Msgs, A, DeliveryFun(Del, S)};
                      (Ac, {M, A, S}) ->
                          {M, A ++ [Ac], S}
                  end, {Acc, [], State1}, Actions1),
            process_ra_events(Events, State, Msgs, Actions0 ++ Actions, DeliveryFun);
        eol ->
            eol
    end.

discard_next_delivery(State0, Wait) ->
    receive
        {ra_event, _, {machine, {delivery, _, _}}} = Evt ->
            element(3, process_ra_events([Evt], State0, [], [],
                                         fun ({deliver, Tag, _, Msgs}, S) ->
                                                 MsgIds = [element(3, M) || M <- Msgs],
                                                 {S0, _} = rabbit_fifo_client:discard(Tag, MsgIds, S),
                                                 S0
                                         end))
    after Wait ->
            State0
    end.

return_next_delivery(State0, Wait) ->
    receive
        {ra_event, _, {machine, {delivery, _, _}}} = Evt ->
            element(3, process_ra_events([Evt], State0, [], [],
                                         fun ({deliver, Tag, _, Msgs}, S) ->
                                                 MsgIds = [element(3, M) || M <- Msgs],
                                                 {S0, _} = rabbit_fifo_client:return(Tag, MsgIds, S),
                                                 S0
                                         end))
    after Wait ->
              State0
    end.

validate_process_down(Name, 0) ->
    exit({process_not_down, Name});
validate_process_down(Name, Num) ->
    case whereis(Name) of
        undefined ->
            ok;
        _ ->
            timer:sleep(100),
            validate_process_down(Name, Num-1)
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
