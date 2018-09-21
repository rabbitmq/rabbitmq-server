-module(rabbit_fifo_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

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
    Config.

end_per_group(_, Config) ->
    _ = application:stop(ra),
    Config.

init_per_testcase(TestCase, Config) ->
    ra_server_sup:remove_all(),
    ServerName2 = list_to_atom(atom_to_list(TestCase) ++ "2"),
    ServerName3 = list_to_atom(atom_to_list(TestCase) ++ "3"),
    [
     {cluster_id, TestCase},
     {uid, atom_to_binary(TestCase, utf8)},
     {node_id, {TestCase, node()}},
     {uid2, atom_to_binary(ServerName2, utf8)},
     {node_id2, {ServerName2, node()}},
     {uid3, atom_to_binary(ServerName3, utf8)},
     {node_id3, {ServerName3, node()}}
     | Config].

basics(Config) ->
    ClusterId = ?config(cluster_id, Config),
    ServerId = ?config(node_id, Config),
    UId = ?config(uid, Config),
    CustomerTag = UId,
    ok = start_cluster(ClusterId, [ServerId]),
    FState0 = rabbit_fifo_client:init(ClusterId, [ServerId]),
    {ok, FState1} = rabbit_fifo_client:checkout(CustomerTag, 1, FState0),

    ra_log_wal:force_roll_over(ra_log_wal),
    % create segment the segment will trigger a snapshot
    timer:sleep(1000),

    {ok, FState2} = rabbit_fifo_client:enqueue(one, FState1),
    % process ra events
    FState3 = process_ra_event(FState2, 250),

    FState5 = receive
                  {ra_event, From, Evt} ->
                      case rabbit_fifo_client:handle_ra_event(From, Evt, FState3) of
                          {internal, _AcceptedSeqs, _FState4} ->
                              exit(unexpected_internal_event);
                          {{delivery, C, [{MsgId, _Msg}]}, FState4} ->
                              {ok, S} = rabbit_fifo_client:settle(C, [MsgId],
                                                              FState4),
                              S
                      end
              after 5000 ->
                        exit(await_msg_timeout)
              end,

    % process settle applied notificaiton
    FState5b = process_ra_event(FState5, 250),
    _ = ra:stop_server(ServerId),
    _ = ra:restart_server(ServerId),

    % give time to become leader
    timer:sleep(500),
    {ok, FState6} = rabbit_fifo_client:enqueue(two, FState5b),
    % process applied event
    FState6b = process_ra_event(FState6, 250),

    receive
        {ra_event, Frm, E} ->
            case rabbit_fifo_client:handle_ra_event(Frm, E, FState6b) of
                {internal, _, _FState7} ->
                    ct:pal("unexpected event ~p~n", [E]),
                    exit({unexpected_internal_event, E});
                {{delivery, Ctag, [{Mid, {_, two}}]}, FState7} ->
                    {ok, _S} = rabbit_fifo_client:return(Ctag, [Mid], FState7),
                    ok
            end
    after 2000 ->
              exit(await_msg_timeout)
    end,
    ra:stop_server(ServerId),
    ok.

return(Config) ->
    ClusterId = ?config(cluster_id, Config),
    ServerId = ?config(node_id, Config),
    ServerId2 = ?config(node_id2, Config),
    ok = start_cluster(ClusterId, [ServerId, ServerId2]),

    F00 = rabbit_fifo_client:init(ClusterId, [ServerId, ServerId2]),
    {ok, F0} = rabbit_fifo_client:enqueue(1, msg1, F00),
    {ok, F1} = rabbit_fifo_client:enqueue(2, msg2, F0),
    {_, F2} = process_ra_events(F1, 100),
    {ok, {MsgId, _}, F} = rabbit_fifo_client:dequeue(<<"tag">>, unsettled, F2),
    {ok, _F2} = rabbit_fifo_client:return(<<"tag">>, [MsgId], F),

    ra:stop_server(ServerId),
    ok.

rabbit_fifo_returns_correlation(Config) ->
    ClusterId = ?config(cluster_id, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterId, [ServerId]),
    F0 = rabbit_fifo_client:init(ClusterId, [ServerId]),
    {ok, F1} = rabbit_fifo_client:enqueue(corr1, msg1, F0),
    receive
        {ra_event, Frm, E} ->
            case rabbit_fifo_client:handle_ra_event(Frm, E, F1) of
                {internal, [corr1], _F2} ->
                    ok;
                {Del, _} ->
                    exit({unexpected, Del})
            end
    after 2000 ->
              exit(await_msg_timeout)
    end,
    ra:stop_server(ServerId),
    ok.

duplicate_delivery(Config) ->
    ClusterId = ?config(cluster_id, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterId, [ServerId]),
    F0 = rabbit_fifo_client:init(ClusterId, [ServerId]),
    {ok, F1} = rabbit_fifo_client:checkout(<<"tag">>, 10, F0),
    {ok, F2} = rabbit_fifo_client:enqueue(corr1, msg1, F1),
    Fun = fun Loop(S0) ->
            receive
                {ra_event, Frm, E} = Evt ->
                    case rabbit_fifo_client:handle_ra_event(Frm, E, S0) of
                        {internal, [corr1], S1} ->
                            Loop(S1);
                        {_Del, S1} ->
                            %% repeat event delivery
                            self() ! Evt,
                            %% check that then next received delivery doesn't
                            %% repeat or crash
                            receive
                                {ra_event, F, E1} ->
                                    case rabbit_fifo_client:handle_ra_event(F, E1, S1) of
                                        {internal, [], S2} ->
                                            S2
                                    end
                            end
                    end
            after 2000 ->
                      exit(await_msg_timeout)
            end
        end,
    Fun(F2),
    ra:stop_server(ServerId),
    ok.

usage(Config) ->
    ClusterId = ?config(cluster_id, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterId, [ServerId]),
    F0 = rabbit_fifo_client:init(ClusterId, [ServerId]),
    {ok, F1} = rabbit_fifo_client:checkout(<<"tag">>, 10, F0),
    {ok, F2} = rabbit_fifo_client:enqueue(corr1, msg1, F1),
    {ok, F3} = rabbit_fifo_client:enqueue(corr2, msg2, F2),
    {_, _} = process_ra_events(F3, 50),
    % force tick and usage stats emission
    ServerId ! tick_timeout,
    timer:sleep(50),
    % ct:pal("ets ~w ~w ~w", [ets:tab2list(rabbit_fifo_usage), ServerId, UId]),
    Use = rabbit_fifo:usage(element(1, ServerId)),
    ct:pal("Use ~w~n", [Use]),
    ra:stop_server(ServerId),
    ?assert(Use > 0.0),
    ok.

resends_lost_command(Config) ->
    ClusterId = ?config(cluster_id, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterId, [ServerId]),

    ok = meck:new(ra, [passthrough]),

    F0 = rabbit_fifo_client:init(ClusterId, [ServerId]),
    {ok, F1} = rabbit_fifo_client:enqueue(msg1, F0),
    % lose the enqueue
    meck:expect(ra, send_and_notify, fun (_, _, _) -> ok end),
    {ok, F2} = rabbit_fifo_client:enqueue(msg2, F1),
    meck:unload(ra),
    {ok, F3} = rabbit_fifo_client:enqueue(msg3, F2),
    {_, F4} = process_ra_events(F3, 500),
    {ok, {_, {_, msg1}}, F5} = rabbit_fifo_client:dequeue(<<"tag">>, settled, F4),
    {ok, {_, {_, msg2}}, F6} = rabbit_fifo_client:dequeue(<<"tag">>, settled, F5),
    {ok, {_, {_, msg3}}, _F7} = rabbit_fifo_client:dequeue(<<"tag">>, settled, F6),
    ra:stop_server(ServerId),
    ok.

two_quick_enqueues(Config) ->
    ClusterId = ?config(cluster_id, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterId, [ServerId]),

    F0 = rabbit_fifo_client:init(ClusterId, [ServerId]),
    F1 = element(2, rabbit_fifo_client:enqueue(msg1, F0)),
    {ok, F2} = rabbit_fifo_client:enqueue(msg2, F1),
    _ = process_ra_events(F2, 500),
    ra:stop_server(ServerId),
    ok.

detects_lost_delivery(Config) ->
    ClusterId = ?config(cluster_id, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterId, [ServerId]),

    F000 = rabbit_fifo_client:init(ClusterId, [ServerId]),
    {ok, F00} = rabbit_fifo_client:enqueue(msg1, F000),
    {_, F0} = process_ra_events(F00, 100),
    {ok, F1} = rabbit_fifo_client:checkout(<<"tag">>, 10, F0),
    {ok, F2} = rabbit_fifo_client:enqueue(msg2, F1),
    {ok, F3} = rabbit_fifo_client:enqueue(msg3, F2),
    % lose first delivery
    receive
        {ra_event, _, {machine, {delivery, _, [{_, {_, msg1}}]}}} ->
            ok
    after 500 ->
              exit(await_delivery_timeout)
    end,

    % assert three deliveries were received
    {[_, _, _], _} = process_ra_events(F3, 500),
    ra:stop_server(ServerId),
    ok.

returns_after_down(Config) ->
    ClusterId = ?config(cluster_id, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterId, [ServerId]),

    F0 = rabbit_fifo_client:init(ClusterId, [ServerId]),
    {ok, F1} = rabbit_fifo_client:enqueue(msg1, F0),
    {_, F2} = process_ra_events(F1, 500),
    % start a customer in a separate processes
    % that exits after checkout
    Self = self(),
    _Pid = spawn(fun () ->
                         F = rabbit_fifo_client:init(ClusterId, [ServerId]),
                         {ok, _} = rabbit_fifo_client:checkout(<<"tag">>, 10, F),
                         Self ! checkout_done
                 end),
    receive checkout_done -> ok after 1000 -> exit(checkout_done_timeout) end,
    % message should be available for dequeue
    {ok, {_, {_, msg1}}, _} = rabbit_fifo_client:dequeue(<<"tag">>, settled, F2),
    ra:stop_server(ServerId),
    ok.

resends_after_lost_applied(Config) ->
    ClusterId = ?config(cluster_id, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterId, [ServerId]),

    F0 = rabbit_fifo_client:init(ClusterId, [ServerId]),
    {_, F1} = process_ra_events(element(2, rabbit_fifo_client:enqueue(msg1, F0)),
                           500),
    {ok, F2} = rabbit_fifo_client:enqueue(msg2, F1),
    % lose an applied event
    receive
        {ra_event, _, {applied, _}} ->
            ok
    after 500 ->
              exit(await_ra_event_timeout)
    end,
    % send another message
    {ok, F3} = rabbit_fifo_client:enqueue(msg3, F2),
    {_, F4} = process_ra_events(F3, 500),
    {ok, {_, {_, msg1}}, F5} = rabbit_fifo_client:dequeue(<<"tag">>, settled, F4),
    {ok, {_, {_, msg2}}, F6} = rabbit_fifo_client:dequeue(<<"tag">>, settled, F5),
    {ok, {_, {_, msg3}}, _F7} = rabbit_fifo_client:dequeue(<<"tag">>, settled, F6),
    ra:stop_server(ServerId),
    ok.

handles_reject_notification(Config) ->
    ClusterId = ?config(cluster_id, Config),
    ServerId1 = ?config(node_id, Config),
    ServerId2 = ?config(node_id2, Config),
    UId1 = ?config(uid, Config),
    CId = {UId1, self()},

    ok = start_cluster(ClusterId, [ServerId1, ServerId2]),
    _ = ra:send_and_await_consensus(ServerId1, {checkout, {auto, 10}, CId}),
    % reverse order - should try the first node in the list first
    F0 = rabbit_fifo_client:init(ClusterId, [ServerId2, ServerId1]),
    {ok, F1} = rabbit_fifo_client:enqueue(one, F0),

    timer:sleep(500),

    % the applied notification
    _F2 = process_ra_event(F1, 250),
    ra:stop_server(ServerId1),
    ra:stop_server(ServerId2),
    ok.

discard(Config) ->
    PrivDir = ?config(priv_dir, Config),
    ServerId = ?config(node_id, Config),
    UId = ?config(uid, Config),
    ClusterId = ?config(cluster_id, Config),
    Conf = #{cluster_id => ClusterId,
             id => ServerId,
             uid => UId,
             log_init_args => #{data_dir => PrivDir, uid => UId},
             initial_member => [],
             machine => {module, rabbit_fifo,
                         #{dead_letter_handler =>
                           {?MODULE, dead_letter_handler, [self()]}}}},
    _ = ra:start_server(Conf),
    ok = ra:trigger_election(ServerId),
    _ = ra:members(ServerId),

    F0 = rabbit_fifo_client:init(ClusterId, [ServerId]),
    {ok, F1} = rabbit_fifo_client:checkout(<<"tag">>, 10, F0),
    {ok, F2} = rabbit_fifo_client:enqueue(msg1, F1),
    F3 = discard_next_delivery(F2, 500),
    {ok, empty, _F4} = rabbit_fifo_client:dequeue(<<"tag1">>, settled, F3),
    receive
        {dead_letter, Letters} ->
            ct:pal("dead letters ~p~n", [Letters]),
            [{_, msg1}] = Letters,
            ok
    after 500 ->
              exit(dead_letter_timeout)
    end,
    ra:stop_server(ServerId),
    ok.

cancel_checkout(Config) ->
    ClusterId = ?config(cluster_id, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterId, [ServerId]),
    F0 = rabbit_fifo_client:init(ClusterId, [ServerId], 4),
    {ok, F1} = rabbit_fifo_client:enqueue(m1, F0),
    {ok, F2} = rabbit_fifo_client:checkout(<<"tag">>, 10, F1),
    {_, F3} = process_ra_events0(F2, [], 250, fun (_, S) -> S end),
    {ok, F4} = rabbit_fifo_client:cancel_checkout(<<"tag">>, F3),
    {ok, {_, {_, m1}}, _} = rabbit_fifo_client:dequeue(<<"d1">>, settled, F4),
    ok.

untracked_enqueue(Config) ->
    ClusterId = ?config(cluster_id, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterId, [ServerId]),

    ok = rabbit_fifo_client:untracked_enqueue(ClusterId, [ServerId], msg1),
    timer:sleep(100),
    F0 = rabbit_fifo_client:init(ClusterId, [ServerId]),
    {ok, {_, {_, msg1}}, _} = rabbit_fifo_client:dequeue(<<"tag">>, settled, F0),
    ra:stop_server(ServerId),
    ok.


flow(Config) ->
    ClusterId = ?config(cluster_id, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterId, [ServerId]),
    F0 = rabbit_fifo_client:init(ClusterId, [ServerId], 3),
    {ok, F1} = rabbit_fifo_client:enqueue(m1, F0),
    {ok, F2} = rabbit_fifo_client:enqueue(m2, F1),
    {ok, F3} = rabbit_fifo_client:enqueue(m3, F2),
    {slow, F4} = rabbit_fifo_client:enqueue(m4, F3),
    {_, F5} = process_ra_events(F4, 500),
    {ok, _} = rabbit_fifo_client:enqueue(m5, F5),
    ra:stop_server(ServerId),
    ok.

test_queries(Config) ->
    ClusterId = ?config(cluster_id, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterId, [ServerId]),
    P = spawn(fun () ->
                  F0 = rabbit_fifo_client:init(ClusterId, [ServerId], 4),
                  {ok, F1} = rabbit_fifo_client:enqueue(m1, F0),
                  {ok, F2} = rabbit_fifo_client:enqueue(m2, F1),
                  process_ra_events(F2, 100),
                  receive stop ->  ok end
          end),
    F0 = rabbit_fifo_client:init(ClusterId, [ServerId], 4),
    {ok, _} = rabbit_fifo_client:checkout(<<"tag">>, 1, F0),
    {ok, {_, Ready}, _} = ra:local_query(ServerId,
                                             fun rabbit_fifo:query_messages_ready/1),
    ?assertEqual(1, maps:size(Ready)),
    ct:pal("Ready ~w~n", [Ready]),
    {ok, {_, Checked}, _} = ra:local_query(ServerId,
                                               fun rabbit_fifo:query_messages_checked_out/1),
    ?assertEqual(1, maps:size(Checked)),
    ct:pal("Checked ~w~n", [Checked]),
    {ok, {_, Processes}, _} = ra:local_query(ServerId,
                                                 fun rabbit_fifo:query_processes/1),
    ct:pal("Processes ~w~n", [Processes]),
    ?assertEqual(2, length(Processes)),
    P !  stop,
    ra:stop_server(ServerId),
    ok.

dead_letter_handler(Pid, Msgs) ->
    Pid ! {dead_letter, Msgs}.

dequeue(Config) ->
    ClusterId = ?config(priv_dir, Config),
    ServerId = ?config(node_id, Config),
    UId = ?config(uid, Config),
    Tag = UId,
    ok = start_cluster(ClusterId, [ServerId]),
    F1 = rabbit_fifo_client:init(ClusterId, [ServerId]),
    {ok, empty, F1b} = rabbit_fifo_client:dequeue(Tag, settled, F1),
    {ok, F2_} = rabbit_fifo_client:enqueue(msg1, F1b),
    {_, F2} = process_ra_events(F2_, 100),

    {ok, {0, {_, msg1}}, F3} = rabbit_fifo_client:dequeue(Tag, settled, F2),
    {ok, F4_} = rabbit_fifo_client:enqueue(msg2, F3),
    {_, F4} = process_ra_events(F4_, 100),
    {ok, {MsgId, {_, msg2}}, F5} = rabbit_fifo_client:dequeue(Tag, unsettled, F4),
    {ok, _F6} = rabbit_fifo_client:settle(Tag, [MsgId], F5),
    ra:stop_server(ServerId),
    ok.

enq_deq_n(N, F0) ->
    enq_deq_n(N, F0, []).

enq_deq_n(0, F0, Acc) ->
    {_, F} = process_ra_events(F0, 100),
    {F, Acc};
enq_deq_n(N, F, Acc) ->
    {ok, F1} = rabbit_fifo_client:enqueue(N, F),
    {_, F2} = process_ra_events(F1, 10),
    {ok, {_, {_, Deq}}, F3} = rabbit_fifo_client:dequeue(term_to_binary(N), settled, F2),

    {_, F4} = process_ra_events(F3, 5),
    enq_deq_n(N-1, F4, [Deq | Acc]).

conf(ClusterId, UId, ServerId, _, Peers) ->
    #{cluster_id => ClusterId,
      id => ServerId,
      uid => UId,
      log_init_args => #{uid => UId},
      initial_members => Peers,
      machine => {module, rabbit_fifo, #{}}}.

process_ra_event(State, Wait) ->
    receive
        {ra_event, From, Evt} ->
            % ct:pal("processed ra event ~p~n", [Evt]),
            {internal, _, S} = rabbit_fifo_client:handle_ra_event(From, Evt, State),
            S
    after Wait ->
              exit(ra_event_timeout)
    end.

process_ra_events(State0, Wait) ->
    process_ra_events(State0, [], Wait).

process_ra_events(State, Acc, Wait) ->
    DeliveryFun = fun ({delivery, Tag, Msgs}, S) ->
                          MsgIds = [element(1, M) || M <- Msgs],
                          {ok, S2} = rabbit_fifo_client:settle(Tag, MsgIds, S),
                          S2
                  end,
    process_ra_events0(State, Acc, Wait, DeliveryFun).

process_ra_events0(State0, Acc, Wait, DeliveryFun) ->
    receive
        {ra_event, From, Evt} ->
            % ct:pal("ra event ~w~n", [Evt]),
            case rabbit_fifo_client:handle_ra_event(From, Evt, State0) of
                {internal, _, State} ->
                    process_ra_events0(State, Acc, Wait, DeliveryFun);
                {{delivery, _Tag, Msgs} = Del, State1} ->
                    State = DeliveryFun(Del, State1),
                    process_ra_events0(State, Acc ++ Msgs, Wait, DeliveryFun);
                eol ->
                    eol
            end
    after Wait ->
              {Acc, State0}
    end.

discard_next_delivery(State0, Wait) ->
    receive
        {ra_event, From, Evt} ->
            case rabbit_fifo_client:handle_ra_event(From, Evt, State0) of
                {internal, _, State} ->
                    discard_next_delivery(State, Wait);
                {{delivery, Tag, Msgs}, State1} ->
                    MsgIds = [element(1, M) || M <- Msgs],
                    ct:pal("discarding ~p", [Msgs]),
                    {ok, State} = rabbit_fifo_client:discard(Tag, MsgIds,
                                                         State1),
                    State
            end
    after Wait ->
              State0
    end.

return_next_delivery(State0, Wait) ->
    receive
        {ra_event, From, Evt} ->
            case rabbit_fifo_client:handle_ra_event(From, Evt, State0) of
                {internal, _, State} ->
                    return_next_delivery(State, Wait);
                {{delivery, Tag, Msgs}, State1} ->
                    MsgIds = [element(1, M) || M <- Msgs],
                    ct:pal("returning ~p", [Msgs]),
                    {ok, State} = rabbit_fifo_client:return(Tag, MsgIds,
                                                        State1),
                    State
            end
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

start_cluster(ClusterId, ServerIds, RaFifoConfig) ->
    {ok, Started, _} = ra:start_cluster(ClusterId,
                                        {module, rabbit_fifo, RaFifoConfig},
                                        ServerIds),
    ?assertEqual(length(Started), length(ServerIds)),
    ok.

start_cluster(ClusterId, ServerIds) ->
    start_cluster(ClusterId, ServerIds, #{}).
