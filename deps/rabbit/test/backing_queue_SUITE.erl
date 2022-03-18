%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(backing_queue_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include("amqqueue.hrl").

-compile(export_all).

-define(PERSISTENT_MSG_STORE, msg_store_persistent).
-define(TRANSIENT_MSG_STORE,  msg_store_transient).

-define(TIMEOUT, 30000).
-define(VHOST, <<"/">>).

-define(VARIABLE_QUEUE_TESTCASES, [
    variable_queue_dynamic_duration_change,
    variable_queue_partial_segments_delta_thing,
    variable_queue_all_the_bits_not_covered_elsewhere_A,
    variable_queue_all_the_bits_not_covered_elsewhere_B,
    variable_queue_drop,
    variable_queue_fold_msg_on_disk,
    variable_queue_dropfetchwhile,
    variable_queue_dropwhile_varying_ram_duration,
    variable_queue_dropwhile_restart,
    variable_queue_dropwhile_sync_restart,
    variable_queue_fetchwhile_varying_ram_duration,
    variable_queue_ack_limiting,
    variable_queue_purge,
    variable_queue_requeue,
    variable_queue_requeue_ram_beta,
    variable_queue_fold,
    variable_queue_batch_publish,
    variable_queue_batch_publish_delivered
  ]).

-define(BACKING_QUEUE_TESTCASES, [
    bq_queue_index,
    bq_queue_index_props,
    {variable_queue_default, [parallel], ?VARIABLE_QUEUE_TESTCASES},
    {variable_queue_lazy, [parallel], ?VARIABLE_QUEUE_TESTCASES ++
                                      [variable_queue_mode_change]},
    bq_variable_queue_delete_msg_store_files_callback,
    bq_queue_recover
  ]).

all() ->
    [
      {group, backing_queue_tests}
    ].

groups() ->
    [
     {backing_queue_tests, [], [
          msg_store,
          {backing_queue_embed_limit_0, [], ?BACKING_QUEUE_TESTCASES},
          {backing_queue_embed_limit_1024, [], ?BACKING_QUEUE_TESTCASES}
        ]}
    ].

group(backing_queue_tests) ->
    [
      %% Several tests based on lazy queues may take more than 30 minutes.
      {timetrap, {hours, 1}}
    ];
group(_) ->
    [].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group, Config) ->
    case lists:member({group, Group}, all()) of
        true ->
            ClusterSize = 2,
            Config1 = rabbit_ct_helpers:set_config(Config, [
                {rmq_nodename_suffix, Group},
                {rmq_nodes_count, ClusterSize}
              ]),
            rabbit_ct_helpers:run_steps(Config1,
              rabbit_ct_broker_helpers:setup_steps() ++
              rabbit_ct_client_helpers:setup_steps() ++ [
                fun(C) -> init_per_group1(Group, C) end,
                fun setup_file_handle_cache/1
              ]);
        false ->
            rabbit_ct_helpers:run_steps(Config, [
                fun(C) -> init_per_group1(Group, C) end
              ])
    end.

init_per_group1(backing_queue_tests, Config) ->
    Module = rabbit_ct_broker_helpers:rpc(Config, 0,
      application, get_env, [rabbit, backing_queue_module]),
    case Module of
        {ok, rabbit_priority_queue} ->
            rabbit_ct_broker_helpers:rpc(Config, 0,
              ?MODULE, setup_backing_queue_test_group, [Config]);
        _ ->
            {skip, rabbit_misc:format(
               "Backing queue module not supported by this test group: ~p~n",
               [Module])}
    end;
init_per_group1(backing_queue_embed_limit_0, Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
      application, set_env, [rabbit, queue_index_embed_msgs_below, 0]),
    Config;
init_per_group1(backing_queue_embed_limit_1024, Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
      application, set_env, [rabbit, queue_index_embed_msgs_below, 1024]),
    Config;
init_per_group1(variable_queue_default, Config) ->
    rabbit_ct_helpers:set_config(Config, {variable_queue_type, default});
init_per_group1(variable_queue_lazy, Config) ->
    rabbit_ct_helpers:set_config(Config, {variable_queue_type, lazy});
init_per_group1(from_cluster_node1, Config) ->
    rabbit_ct_helpers:set_config(Config, {test_direction, {0, 1}});
init_per_group1(from_cluster_node2, Config) ->
    rabbit_ct_helpers:set_config(Config, {test_direction, {1, 0}});
init_per_group1(_, Config) ->
    Config.

setup_file_handle_cache(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, setup_file_handle_cache1, []),
    Config.

setup_file_handle_cache1() ->
    %% FIXME: Why are we doing this?
    application:set_env(rabbit, file_handles_high_watermark, 10),
    ok = file_handle_cache:set_limit(10),
    ok.

end_per_group(Group, Config) ->
    case lists:member({group, Group}, all()) of
        true ->
            rabbit_ct_helpers:run_steps(Config,
              [fun(C) -> end_per_group1(Group, C) end] ++
              rabbit_ct_client_helpers:teardown_steps() ++
              rabbit_ct_broker_helpers:teardown_steps());
        false ->
            Config
    end.

end_per_group1(backing_queue_tests, Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, teardown_backing_queue_test_group, [Config]);
end_per_group1(Group, Config)
when   Group =:= backing_queue_embed_limit_0
orelse Group =:= backing_queue_embed_limit_1024 ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
      application, set_env, [rabbit, queue_index_embed_msgs_below,
        ?config(rmq_queue_index_embed_msgs_below, Config)]),
    Config;
end_per_group1(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) when Testcase == variable_queue_requeue;
                                         Testcase == variable_queue_fold ->
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, application, set_env,
           [rabbit, queue_explicit_gc_run_operation_threshold, 0]),
    rabbit_ct_helpers:testcase_started(Config, Testcase);
init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) when Testcase == variable_queue_requeue;
                                        Testcase == variable_queue_fold ->
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, application, set_env,
           [rabbit, queue_explicit_gc_run_operation_threshold, 1000]),
    rabbit_ct_helpers:testcase_finished(Config, Testcase);
end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Message store.
%% -------------------------------------------------------------------

msg_store(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, msg_store1, [Config]).

msg_store1(_Config) ->
    restart_msg_store_empty(),
    MsgIds = [msg_id_bin(M) || M <- lists:seq(1,100)],
    {MsgIds1stHalf, MsgIds2ndHalf} = lists:split(length(MsgIds) div 2, MsgIds),
    Ref = rabbit_guid:gen(),
    {Cap, MSCState} = msg_store_client_init_capture(
                        ?PERSISTENT_MSG_STORE, Ref),
    Ref2 = rabbit_guid:gen(),
    {Cap2, MSC2State} = msg_store_client_init_capture(
                          ?PERSISTENT_MSG_STORE, Ref2),
    %% check we don't contain any of the msgs we're about to publish
    false = msg_store_contains(false, MsgIds, MSCState),
    %% test confirm logic
    passed = test_msg_store_confirms([hd(MsgIds)], Cap, MSCState),
    %% check we don't contain any of the msgs we're about to publish
    false = msg_store_contains(false, MsgIds, MSCState),
    %% publish the first half
    ok = msg_store_write(MsgIds1stHalf, MSCState),
    %% sync on the first half
    ok = on_disk_await(Cap, MsgIds1stHalf),
    %% publish the second half
    ok = msg_store_write(MsgIds2ndHalf, MSCState),
    %% check they're all in there
    true = msg_store_contains(true, MsgIds, MSCState),
    %% publish the latter half twice so we hit the caching and ref
    %% count code. We need to do this through a 2nd client since a
    %% single client is not supposed to write the same message more
    %% than once without first removing it.
    ok = msg_store_write(MsgIds2ndHalf, MSC2State),
    %% check they're still all in there
    true = msg_store_contains(true, MsgIds, MSCState),
    %% sync on the 2nd half
    ok = on_disk_await(Cap2, MsgIds2ndHalf),
    %% cleanup
    ok = on_disk_stop(Cap2),
    ok = rabbit_msg_store:client_delete_and_terminate(MSC2State),
    ok = on_disk_stop(Cap),
    %% read them all
    MSCState1 = msg_store_read(MsgIds, MSCState),
    %% read them all again - this will hit the cache, not disk
    MSCState2 = msg_store_read(MsgIds, MSCState1),
    %% remove them all
    ok = msg_store_remove(MsgIds, MSCState2),
    %% check first half doesn't exist
    false = msg_store_contains(false, MsgIds1stHalf, MSCState2),
    %% check second half does exist
    true = msg_store_contains(true, MsgIds2ndHalf, MSCState2),
    %% read the second half again
    MSCState3 = msg_store_read(MsgIds2ndHalf, MSCState2),
    %% read the second half again, just for fun (aka code coverage)
    MSCState4 = msg_store_read(MsgIds2ndHalf, MSCState3),
    ok = rabbit_msg_store:client_terminate(MSCState4),
    %% stop and restart, preserving every other msg in 2nd half
    ok = rabbit_variable_queue:stop_msg_store(?VHOST),
    ok = rabbit_variable_queue:start_msg_store(?VHOST,
           [], {fun ([]) -> finished;
                    ([MsgId|MsgIdsTail])
                      when length(MsgIdsTail) rem 2 == 0 ->
                        {MsgId, 1, MsgIdsTail};
                    ([MsgId|MsgIdsTail]) ->
                        {MsgId, 0, MsgIdsTail}
                end, MsgIds2ndHalf}),
    MSCState5 = msg_store_client_init(?PERSISTENT_MSG_STORE, Ref),
    %% check we have the right msgs left
    lists:foldl(
      fun (MsgId, Bool) ->
              not(Bool = rabbit_msg_store:contains(MsgId, MSCState5))
      end, false, MsgIds2ndHalf),
    ok = rabbit_msg_store:client_terminate(MSCState5),
    %% restart empty
    restart_msg_store_empty(),
    MSCState6 = msg_store_client_init(?PERSISTENT_MSG_STORE, Ref),
    %% check we don't contain any of the msgs
    false = msg_store_contains(false, MsgIds, MSCState6),
    %% publish the first half again
    ok = msg_store_write(MsgIds1stHalf, MSCState6),
    %% this should force some sort of sync internally otherwise misread
    ok = rabbit_msg_store:client_terminate(
           msg_store_read(MsgIds1stHalf, MSCState6)),
    MSCState7 = msg_store_client_init(?PERSISTENT_MSG_STORE, Ref),
    ok = msg_store_remove(MsgIds1stHalf, MSCState7),
    ok = rabbit_msg_store:client_terminate(MSCState7),
    %% restart empty
    restart_msg_store_empty(), %% now safe to reuse msg_ids
    %% push a lot of msgs in... at least 100 files worth
    {ok, FileSize} = application:get_env(rabbit, msg_store_file_size_limit),
    PayloadSizeBits = 65536,
    BigCount = trunc(100 * FileSize / (PayloadSizeBits div 8)),
    MsgIdsBig = [msg_id_bin(X) || X <- lists:seq(1, BigCount)],
    Payload = << 0:PayloadSizeBits >>,
    ok = with_msg_store_client(
           ?PERSISTENT_MSG_STORE, Ref,
           fun (MSCStateM) ->
                   [ok = rabbit_msg_store:write(MsgId, Payload, MSCStateM) ||
                       MsgId <- MsgIdsBig],
                   MSCStateM
           end),
    %% now read them to ensure we hit the fast client-side reading
    ok = foreach_with_msg_store_client(
           ?PERSISTENT_MSG_STORE, Ref,
           fun (MsgId, MSCStateM) ->
                   {{ok, Payload}, MSCStateN} = rabbit_msg_store:read(
                                                  MsgId, MSCStateM),
                   MSCStateN
           end, MsgIdsBig),
    %% .., then 3s by 1...
    ok = msg_store_remove(?PERSISTENT_MSG_STORE, Ref,
                          [msg_id_bin(X) || X <- lists:seq(BigCount, 1, -3)]),
    %% .., then remove 3s by 2, from the young end first. This hits
    %% GC (under 50% good data left, but no empty files. Must GC).
    ok = msg_store_remove(?PERSISTENT_MSG_STORE, Ref,
                          [msg_id_bin(X) || X <- lists:seq(BigCount-1, 1, -3)]),
    %% .., then remove 3s by 3, from the young end first. This hits
    %% GC...
    ok = msg_store_remove(?PERSISTENT_MSG_STORE, Ref,
                          [msg_id_bin(X) || X <- lists:seq(BigCount-2, 1, -3)]),
    %% ensure empty
    ok = with_msg_store_client(
           ?PERSISTENT_MSG_STORE, Ref,
           fun (MSCStateM) ->
                   false = msg_store_contains(false, MsgIdsBig, MSCStateM),
                   MSCStateM
           end),
    %%
    passed = test_msg_store_client_delete_and_terminate(),
    %% restart empty
    restart_msg_store_empty(),
    passed.

restart_msg_store_empty() ->
    ok = rabbit_variable_queue:stop_msg_store(?VHOST),
    ok = rabbit_variable_queue:start_msg_store(?VHOST,
           undefined, {fun (ok) -> finished end, ok}).

msg_id_bin(X) ->
    erlang:md5(term_to_binary(X)).

on_disk_capture() ->
    receive
        {await, MsgIds, Pid} -> on_disk_capture([], MsgIds, Pid);
        stop                 -> done
    end.

on_disk_capture([_|_], _Awaiting, Pid) ->
    Pid ! {self(), surplus};
on_disk_capture(OnDisk, Awaiting, Pid) ->
    receive
        {on_disk, MsgIdsS} ->
            MsgIds = gb_sets:to_list(MsgIdsS),
            on_disk_capture(OnDisk ++ (MsgIds -- Awaiting), Awaiting -- MsgIds,
                            Pid);
        stop ->
            done
    after (case Awaiting of [] -> 200; _ -> ?TIMEOUT end) ->
            case Awaiting of
                [] -> Pid ! {self(), arrived}, on_disk_capture();
                _  -> Pid ! {self(), timeout}
            end
    end.

on_disk_await(Pid, MsgIds) when is_list(MsgIds) ->
    Pid ! {await, MsgIds, self()},
    receive
        {Pid, arrived} -> ok;
        {Pid, Error}   -> Error
    end.

on_disk_stop(Pid) ->
    MRef = erlang:monitor(process, Pid),
    Pid ! stop,
    receive {'DOWN', MRef, process, Pid, _Reason} ->
            ok
    end.

msg_store_client_init_capture(MsgStore, Ref) ->
    Pid = spawn(fun on_disk_capture/0),
    {Pid, rabbit_vhost_msg_store:client_init(?VHOST, MsgStore, Ref,
                                             fun (MsgIds, _ActionTaken) ->
                                                 Pid ! {on_disk, MsgIds}
                                             end, undefined)}.

msg_store_contains(Atom, MsgIds, MSCState) ->
    Atom = lists:foldl(
             fun (MsgId, Atom1) when Atom1 =:= Atom ->
                     rabbit_msg_store:contains(MsgId, MSCState) end,
             Atom, MsgIds).

msg_store_read(MsgIds, MSCState) ->
    lists:foldl(fun (MsgId, MSCStateM) ->
                        {{ok, MsgId}, MSCStateN} = rabbit_msg_store:read(
                                                     MsgId, MSCStateM),
                        MSCStateN
                end, MSCState, MsgIds).

msg_store_write(MsgIds, MSCState) ->
    ok = lists:foldl(fun (MsgId, ok) ->
                             rabbit_msg_store:write(MsgId, MsgId, MSCState)
                     end, ok, MsgIds).

msg_store_write_flow(MsgIds, MSCState) ->
    ok = lists:foldl(fun (MsgId, ok) ->
                             rabbit_msg_store:write_flow(MsgId, MsgId, MSCState)
                     end, ok, MsgIds).

msg_store_remove(MsgIds, MSCState) ->
    rabbit_msg_store:remove(MsgIds, MSCState).

msg_store_remove(MsgStore, Ref, MsgIds) ->
    with_msg_store_client(MsgStore, Ref,
                          fun (MSCStateM) ->
                                  ok = msg_store_remove(MsgIds, MSCStateM),
                                  MSCStateM
                          end).

with_msg_store_client(MsgStore, Ref, Fun) ->
    rabbit_msg_store:client_terminate(
      Fun(msg_store_client_init(MsgStore, Ref))).

foreach_with_msg_store_client(MsgStore, Ref, Fun, L) ->
    rabbit_msg_store:client_terminate(
      lists:foldl(fun (MsgId, MSCState) -> Fun(MsgId, MSCState) end,
                  msg_store_client_init(MsgStore, Ref), L)).

test_msg_store_confirms(MsgIds, Cap, MSCState) ->
    %% write -> confirmed
    ok = msg_store_write(MsgIds, MSCState),
    ok = on_disk_await(Cap, MsgIds),
    %% remove -> _
    ok = msg_store_remove(MsgIds, MSCState),
    ok = on_disk_await(Cap, []),
    %% write, remove -> confirmed
    ok = msg_store_write(MsgIds, MSCState),
    ok = msg_store_remove(MsgIds, MSCState),
    ok = on_disk_await(Cap, MsgIds),
    %% write, remove, write -> confirmed, confirmed
    ok = msg_store_write(MsgIds, MSCState),
    ok = msg_store_remove(MsgIds, MSCState),
    ok = msg_store_write(MsgIds, MSCState),
    ok = on_disk_await(Cap, MsgIds ++ MsgIds),
    %% remove, write -> confirmed
    ok = msg_store_remove(MsgIds, MSCState),
    ok = msg_store_write(MsgIds, MSCState),
    ok = on_disk_await(Cap, MsgIds),
    %% remove, write, remove -> confirmed
    ok = msg_store_remove(MsgIds, MSCState),
    ok = msg_store_write(MsgIds, MSCState),
    ok = msg_store_remove(MsgIds, MSCState),
    ok = on_disk_await(Cap, MsgIds),
    %% confirmation on timer-based sync
    passed = test_msg_store_confirm_timer(),
    passed.

test_msg_store_confirm_timer() ->
    Ref = rabbit_guid:gen(),
    MsgId  = msg_id_bin(1),
    Self = self(),
    MSCState = rabbit_vhost_msg_store:client_init(
        ?VHOST,
        ?PERSISTENT_MSG_STORE,
        Ref,
        fun (MsgIds, _ActionTaken) ->
            case gb_sets:is_member(MsgId, MsgIds) of
                true  -> Self ! on_disk;
                false -> ok
            end
        end, undefined),
    ok = msg_store_write([MsgId], MSCState),
    ok = msg_store_keep_busy_until_confirm([msg_id_bin(2)], MSCState, false),
    ok = msg_store_remove([MsgId], MSCState),
    ok = rabbit_msg_store:client_delete_and_terminate(MSCState),
    passed.

msg_store_keep_busy_until_confirm(MsgIds, MSCState, Blocked) ->
    After = case Blocked of
                false -> 0;
                true  -> ?MAX_WAIT
            end,
    Recurse = fun () -> msg_store_keep_busy_until_confirm(
                          MsgIds, MSCState, credit_flow:blocked()) end,
    receive
        on_disk            -> ok;
        {bump_credit, Msg} -> credit_flow:handle_bump_msg(Msg),
                              Recurse()
    after After ->
            ok = msg_store_write_flow(MsgIds, MSCState),
            ok = msg_store_remove(MsgIds, MSCState),
            Recurse()
    end.

test_msg_store_client_delete_and_terminate() ->
    restart_msg_store_empty(),
    MsgIds = [msg_id_bin(M) || M <- lists:seq(1, 10)],
    Ref = rabbit_guid:gen(),
    MSCState = msg_store_client_init(?PERSISTENT_MSG_STORE, Ref),
    ok = msg_store_write(MsgIds, MSCState),
    %% test the 'dying client' fast path for writes
    ok = rabbit_msg_store:client_delete_and_terminate(MSCState),
    passed.

%% -------------------------------------------------------------------
%% Backing queue.
%% -------------------------------------------------------------------

setup_backing_queue_test_group(Config) ->
    {ok, FileSizeLimit} =
        application:get_env(rabbit, msg_store_file_size_limit),
    application:set_env(rabbit, msg_store_file_size_limit, 512),
    {ok, MaxJournal} =
        application:get_env(rabbit, queue_index_max_journal_entries),
    application:set_env(rabbit, queue_index_max_journal_entries, 128),
    application:set_env(rabbit, msg_store_file_size_limit,
                        FileSizeLimit),
    {ok, Bytes} =
        application:get_env(rabbit, queue_index_embed_msgs_below),
    rabbit_ct_helpers:set_config(Config, [
        {rmq_queue_index_max_journal_entries, MaxJournal},
        {rmq_queue_index_embed_msgs_below, Bytes}
      ]).

teardown_backing_queue_test_group(Config) ->
    %% FIXME: Undo all the setup function did.
    application:set_env(rabbit, queue_index_max_journal_entries,
                        ?config(rmq_queue_index_max_journal_entries, Config)),
    %% We will have restarted the message store, and thus changed
    %% the order of the children of rabbit_sup. This will cause
    %% problems if there are subsequent failures - see bug 24262.
    ok = restart_app(),
    Config.

bq_queue_index(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, bq_queue_index1, [Config]).

bq_queue_index1(_Config) ->
    init_queue_index(),
    SegmentSize = rabbit_queue_index:next_segment_boundary(0),
    TwoSegs = SegmentSize + SegmentSize,
    MostOfASegment = trunc(SegmentSize*0.75),
    SeqIdsA = lists:seq(0, MostOfASegment-1),
    SeqIdsB = lists:seq(MostOfASegment, 2*MostOfASegment),
    SeqIdsC = lists:seq(0, trunc(SegmentSize/2)),
    SeqIdsD = lists:seq(0, SegmentSize*4),

    with_empty_test_queue(
      fun (Qi0, QName) ->
              {0, 0, Qi1} = rabbit_queue_index:bounds(Qi0),
              {Qi2, SeqIdsMsgIdsA} = queue_index_publish(SeqIdsA, false, Qi1),
              {0, SegmentSize, Qi3} = rabbit_queue_index:bounds(Qi2),
              {ReadA, Qi4} = rabbit_queue_index:read(0, SegmentSize, Qi3),
              ok = verify_read_with_published(false, false, ReadA,
                                              lists:reverse(SeqIdsMsgIdsA)),
              %% should get length back as 0, as all the msgs were transient
              {0, 0, Qi6} = restart_test_queue(Qi4, QName),
              {0, 0, Qi7} = rabbit_queue_index:bounds(Qi6),
              {Qi8, SeqIdsMsgIdsB} = queue_index_publish(SeqIdsB, true, Qi7),
              {0, TwoSegs, Qi9} = rabbit_queue_index:bounds(Qi8),
              {ReadB, Qi10} = rabbit_queue_index:read(0, SegmentSize, Qi9),
              ok = verify_read_with_published(false, true, ReadB,
                                              lists:reverse(SeqIdsMsgIdsB)),
              %% should get length back as MostOfASegment
              LenB = length(SeqIdsB),
              BytesB = LenB * 10,
              {LenB, BytesB, Qi12} = restart_test_queue(Qi10, QName),
              {0, TwoSegs, Qi13} = rabbit_queue_index:bounds(Qi12),
              Qi14 = rabbit_queue_index:deliver(SeqIdsB, Qi13),
              {ReadC, Qi15} = rabbit_queue_index:read(0, SegmentSize, Qi14),
              ok = verify_read_with_published(true, true, ReadC,
                                              lists:reverse(SeqIdsMsgIdsB)),
              Qi16 = rabbit_queue_index:ack(SeqIdsB, Qi15),
              Qi17 = rabbit_queue_index:flush(Qi16),
              %% Everything will have gone now because #pubs == #acks
              {0, 0, Qi18} = rabbit_queue_index:bounds(Qi17),
              %% should get length back as 0 because all persistent
              %% msgs have been acked
              {0, 0, Qi19} = restart_test_queue(Qi18, QName),
              Qi19
      end),

    %% These next bits are just to hit the auto deletion of segment files.
    %% First, partials:
    %% a) partial pub+del+ack, then move to new segment
    with_empty_test_queue(
      fun (Qi0, _QName) ->
              {Qi1, _SeqIdsMsgIdsC} = queue_index_publish(SeqIdsC,
                                                          false, Qi0),
              Qi2 = rabbit_queue_index:deliver(SeqIdsC, Qi1),
              Qi3 = rabbit_queue_index:ack(SeqIdsC, Qi2),
              Qi4 = rabbit_queue_index:flush(Qi3),
              {Qi5, _SeqIdsMsgIdsC1} = queue_index_publish([SegmentSize],
                                                           false, Qi4),
              Qi5
      end),

    %% b) partial pub+del, then move to new segment, then ack all in old segment
    with_empty_test_queue(
      fun (Qi0, _QName) ->
              {Qi1, _SeqIdsMsgIdsC2} = queue_index_publish(SeqIdsC,
                                                           false, Qi0),
              Qi2 = rabbit_queue_index:deliver(SeqIdsC, Qi1),
              {Qi3, _SeqIdsMsgIdsC3} = queue_index_publish([SegmentSize],
                                                           false, Qi2),
              Qi4 = rabbit_queue_index:ack(SeqIdsC, Qi3),
              rabbit_queue_index:flush(Qi4)
      end),

    %% c) just fill up several segments of all pubs, then +dels, then +acks
    with_empty_test_queue(
      fun (Qi0, _QName) ->
              {Qi1, _SeqIdsMsgIdsD} = queue_index_publish(SeqIdsD,
                                                          false, Qi0),
              Qi2 = rabbit_queue_index:deliver(SeqIdsD, Qi1),
              Qi3 = rabbit_queue_index:ack(SeqIdsD, Qi2),
              rabbit_queue_index:flush(Qi3)
      end),

    %% d) get messages in all states to a segment, then flush, then do
    %% the same again, don't flush and read. This will hit all
    %% possibilities in combining the segment with the journal.
    with_empty_test_queue(
      fun (Qi0, _QName) ->
              {Qi1, [Seven,Five,Four|_]} = queue_index_publish([0,1,2,4,5,7],
                                                               false, Qi0),
              Qi2 = rabbit_queue_index:deliver([0,1,4], Qi1),
              Qi3 = rabbit_queue_index:ack([0], Qi2),
              Qi4 = rabbit_queue_index:flush(Qi3),
              {Qi5, [Eight,Six|_]} = queue_index_publish([3,6,8], false, Qi4),
              Qi6 = rabbit_queue_index:deliver([2,3,5,6], Qi5),
              Qi7 = rabbit_queue_index:ack([1,2,3], Qi6),
              {[], Qi8} = rabbit_queue_index:read(0, 4, Qi7),
              {ReadD, Qi9} = rabbit_queue_index:read(4, 7, Qi8),
              ok = verify_read_with_published(true, false, ReadD,
                                              [Four, Five, Six]),
              {ReadE, Qi10} = rabbit_queue_index:read(7, 9, Qi9),
              ok = verify_read_with_published(false, false, ReadE,
                                              [Seven, Eight]),
              Qi10
      end),

    %% e) as for (d), but use terminate instead of read, which will
    %% exercise journal_minus_segment, not segment_plus_journal.
    with_empty_test_queue(
      fun (Qi0, QName) ->
              {Qi1, _SeqIdsMsgIdsE} = queue_index_publish([0,1,2,4,5,7],
                                                          true, Qi0),
              Qi2 = rabbit_queue_index:deliver([0,1,4], Qi1),
              Qi3 = rabbit_queue_index:ack([0], Qi2),
              {5, 50, Qi4} = restart_test_queue(Qi3, QName),
              {Qi5, _SeqIdsMsgIdsF} = queue_index_publish([3,6,8], true, Qi4),
              Qi6 = rabbit_queue_index:deliver([2,3,5,6], Qi5),
              Qi7 = rabbit_queue_index:ack([1,2,3], Qi6),
              {5, 50, Qi8} = restart_test_queue(Qi7, QName),
              Qi8
      end),

    ok = rabbit_variable_queue:stop(?VHOST),
    {ok, _} = rabbit_variable_queue:start(?VHOST, []),

    passed.

bq_queue_index_props(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, bq_queue_index_props1, [Config]).

bq_queue_index_props1(_Config) ->
    with_empty_test_queue(
      fun(Qi0, _QName) ->
              MsgId = rabbit_guid:gen(),
              Props = #message_properties{expiry=12345, size = 10},
              Qi1 = rabbit_queue_index:publish(
                      MsgId, 1, Props, true, infinity, Qi0),
              {[{MsgId, 1, Props, _, _}], Qi2} =
                  rabbit_queue_index:read(1, 2, Qi1),
              Qi2
      end),

    ok = rabbit_variable_queue:stop(?VHOST),
    {ok, _} = rabbit_variable_queue:start(?VHOST, []),

    passed.

bq_variable_queue_delete_msg_store_files_callback(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, bq_variable_queue_delete_msg_store_files_callback1, [Config]).

bq_variable_queue_delete_msg_store_files_callback1(Config) ->
    ok = restart_msg_store_empty(),
    QName0 = queue_name(Config, <<"bq_variable_queue_delete_msg_store_files_callback-q">>),
    {new, Q} = rabbit_amqqueue:declare(QName0, true, false, [], none, <<"acting-user">>),
    QName = amqqueue:get_name(Q),
    QPid = amqqueue:get_pid(Q),
    Payload = <<0:8388608>>, %% 1MB
    Count = 30,
    QTState = publish_and_confirm(Q, Payload, Count),

    rabbit_amqqueue:set_ram_duration_target(QPid, 0),

    {ok, Limiter} = rabbit_limiter:start_link(no_id),

    CountMinusOne = Count - 1,
    {ok, CountMinusOne, {QName, QPid, _AckTag, false, _Msg}, _} =
        rabbit_amqqueue:basic_get(Q, true, Limiter,
                                  <<"bq_variable_queue_delete_msg_store_files_callback1">>,
                                  QTState),
    {ok, CountMinusOne} = rabbit_amqqueue:purge(Q),

    %% give the queue a second to receive the close_fds callback msg
    timer:sleep(1000),

    rabbit_amqqueue:delete(Q, false, false, <<"acting-user">>),
    passed.

bq_queue_recover(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, bq_queue_recover1, [Config]).

bq_queue_recover1(Config) ->
    init_queue_index(),
    Count = 2 * rabbit_queue_index:next_segment_boundary(0),
    QName0 = queue_name(Config, <<"bq_queue_recover-q">>),
    {new, Q} = rabbit_amqqueue:declare(QName0, true, false, [], none, <<"acting-user">>),
    QName = amqqueue:get_name(Q),
    QPid = amqqueue:get_pid(Q),
    QT = publish_and_confirm(Q, <<>>, Count),
    SupPid = get_queue_sup_pid(Q),
    true = is_pid(SupPid),
    exit(SupPid, kill),
    exit(QPid, kill),
    MRef = erlang:monitor(process, QPid),
    receive {'DOWN', MRef, process, QPid, _Info} -> ok
    after 10000 -> exit(timeout_waiting_for_queue_death)
    end,
    rabbit_amqqueue:stop(?VHOST),
    {Recovered, []} = rabbit_amqqueue:recover(?VHOST),
    rabbit_amqqueue:start(Recovered),
    {ok, Limiter} = rabbit_limiter:start_link(no_id),
    rabbit_amqqueue:with_or_die(
      QName,
      fun (Q1) when ?is_amqqueue(Q1) ->
              QPid1 = amqqueue:get_pid(Q1),
              CountMinusOne = Count - 1,
              {ok, CountMinusOne, {QName, QPid1, _AckTag, true, _Msg}, _} =
                  rabbit_amqqueue:basic_get(Q1, false, Limiter,
                                            <<"bq_queue_recover1">>, QT),
              exit(QPid1, shutdown),
              VQ1 = variable_queue_init(Q, true),
              {{_Msg1, true, _AckTag1}, VQ2} =
                  rabbit_variable_queue:fetch(true, VQ1),
              CountMinusOne = rabbit_variable_queue:len(VQ2),
              _VQ3 = rabbit_variable_queue:delete_and_terminate(shutdown, VQ2),
              ok = rabbit_amqqueue:internal_delete(QName, <<"acting-user">>)
      end),
    passed.

%% Return the PID of the given queue's supervisor.
get_queue_sup_pid(Q) when ?is_amqqueue(Q) ->
    QName = amqqueue:get_name(Q),
    QPid = amqqueue:get_pid(Q),
    VHost = QName#resource.virtual_host,
    {ok, AmqSup} = rabbit_amqqueue_sup_sup:find_for_vhost(VHost, node(QPid)),
    Sups = supervisor:which_children(AmqSup),
    get_queue_sup_pid(Sups, QPid).

get_queue_sup_pid([{_, SupPid, _, _} | Rest], QueuePid) ->
    WorkerPids = [Pid || {_, Pid, _, _} <- supervisor:which_children(SupPid)],
    case lists:member(QueuePid, WorkerPids) of
        true  -> SupPid;
        false -> get_queue_sup_pid(Rest, QueuePid)
    end;
get_queue_sup_pid([], _QueuePid) ->
    undefined.

variable_queue_dynamic_duration_change(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, variable_queue_dynamic_duration_change1, [Config]).

variable_queue_dynamic_duration_change1(Config) ->
    with_fresh_variable_queue(
      fun variable_queue_dynamic_duration_change2/2,
      ?config(variable_queue_type, Config)).

variable_queue_dynamic_duration_change2(VQ0, _QName) ->
    SegmentSize = rabbit_queue_index:next_segment_boundary(0),

    %% start by sending in a couple of segments worth
    Len = 2*SegmentSize,
    VQ1 = variable_queue_publish(false, Len, VQ0),
    %% squeeze and relax queue
    Churn = Len div 32,
    VQ2 = publish_fetch_and_ack(Churn, Len, VQ1),

    {Duration, VQ3} = rabbit_variable_queue:ram_duration(VQ2),
    VQ7 = lists:foldl(
            fun (Duration1, VQ4) ->
                    {_Duration, VQ5} = rabbit_variable_queue:ram_duration(VQ4),
                    VQ6 = variable_queue_set_ram_duration_target(
                            Duration1, VQ5),
                    publish_fetch_and_ack(Churn, Len, VQ6)
            end, VQ3, [Duration / 4, 0, Duration / 4, infinity]),

    %% drain
    {VQ8, AckTags} = variable_queue_fetch(Len, false, false, Len, VQ7),
    {_Guids, VQ9} = rabbit_variable_queue:ack(AckTags, VQ8),
    {empty, VQ10} = rabbit_variable_queue:fetch(true, VQ9),

    VQ10.

variable_queue_partial_segments_delta_thing(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, variable_queue_partial_segments_delta_thing1, [Config]).

variable_queue_partial_segments_delta_thing1(Config) ->
    with_fresh_variable_queue(
      fun variable_queue_partial_segments_delta_thing2/2,
      ?config(variable_queue_type, Config)).

variable_queue_partial_segments_delta_thing2(VQ0, _QName) ->
    SegmentSize = rabbit_queue_index:next_segment_boundary(0),
    HalfSegment = SegmentSize div 2,
    OneAndAHalfSegment = SegmentSize + HalfSegment,
    VQ1 = variable_queue_publish(true, OneAndAHalfSegment, VQ0),
    {_Duration, VQ2} = rabbit_variable_queue:ram_duration(VQ1),
    VQ3 = check_variable_queue_status(
            variable_queue_set_ram_duration_target(0, VQ2),
            %% one segment in q3, and half a segment in delta
            [{delta, {delta, SegmentSize, HalfSegment, OneAndAHalfSegment}},
             {q3, SegmentSize},
             {len, SegmentSize + HalfSegment}]),
    VQ4 = variable_queue_set_ram_duration_target(infinity, VQ3),
    VQ5 = check_variable_queue_status(
            variable_queue_publish(true, 1, VQ4),
            %% one alpha, but it's in the same segment as the deltas
            [{q1, 1},
             {delta, {delta, SegmentSize, HalfSegment, OneAndAHalfSegment}},
             {q3, SegmentSize},
             {len, SegmentSize + HalfSegment + 1}]),
    {VQ6, AckTags} = variable_queue_fetch(SegmentSize, true, false,
                                          SegmentSize + HalfSegment + 1, VQ5),
    VQ7 = check_variable_queue_status(
            VQ6,
            %% the half segment should now be in q3
            [{q1, 1},
             {delta, {delta, undefined, 0, undefined}},
             {q3, HalfSegment},
             {len, HalfSegment + 1}]),
    {VQ8, AckTags1} = variable_queue_fetch(HalfSegment + 1, true, false,
                                           HalfSegment + 1, VQ7),
    {_Guids, VQ9} = rabbit_variable_queue:ack(AckTags ++ AckTags1, VQ8),
    %% should be empty now
    {empty, VQ10} = rabbit_variable_queue:fetch(true, VQ9),
    VQ10.

variable_queue_all_the_bits_not_covered_elsewhere_A(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, variable_queue_all_the_bits_not_covered_elsewhere_A1, [Config]).

variable_queue_all_the_bits_not_covered_elsewhere_A1(Config) ->
    with_fresh_variable_queue(
      fun variable_queue_all_the_bits_not_covered_elsewhere_A2/2,
      ?config(variable_queue_type, Config)).

variable_queue_all_the_bits_not_covered_elsewhere_A2(VQ0, QName) ->
    Count = 2 * rabbit_queue_index:next_segment_boundary(0),
    VQ1 = variable_queue_publish(true, Count, VQ0),
    VQ2 = variable_queue_publish(false, Count, VQ1),
    VQ3 = variable_queue_set_ram_duration_target(0, VQ2),
    {VQ4, _AckTags}  = variable_queue_fetch(Count, true, false,
                                            Count + Count, VQ3),
    {VQ5, _AckTags1} = variable_queue_fetch(Count, false, false,
                                            Count, VQ4),
    _VQ6 = rabbit_variable_queue:terminate(shutdown, VQ5),
    VQ7 = variable_queue_init(test_amqqueue(QName, true), true),
    {{_Msg1, true, _AckTag1}, VQ8} = rabbit_variable_queue:fetch(true, VQ7),
    Count1 = rabbit_variable_queue:len(VQ8),
    VQ9 = variable_queue_publish(false, 1, VQ8),
    VQ10 = variable_queue_set_ram_duration_target(0, VQ9),
    {VQ11, _AckTags2} = variable_queue_fetch(Count1, true, true, Count, VQ10),
    {VQ12, _AckTags3} = variable_queue_fetch(1, false, false, 1, VQ11),
    VQ12.

variable_queue_all_the_bits_not_covered_elsewhere_B(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, variable_queue_all_the_bits_not_covered_elsewhere_B1, [Config]).

variable_queue_all_the_bits_not_covered_elsewhere_B1(Config) ->
    with_fresh_variable_queue(
      fun variable_queue_all_the_bits_not_covered_elsewhere_B2/2,
      ?config(variable_queue_type, Config)).

variable_queue_all_the_bits_not_covered_elsewhere_B2(VQ0, QName) ->
    VQ1 = variable_queue_set_ram_duration_target(0, VQ0),
    VQ2 = variable_queue_publish(false, 4, VQ1),
    {VQ3, AckTags} = variable_queue_fetch(2, false, false, 4, VQ2),
    {_Guids, VQ4} =
        rabbit_variable_queue:requeue(AckTags, VQ3),
    VQ5 = rabbit_variable_queue:timeout(VQ4),
    _VQ6 = rabbit_variable_queue:terminate(shutdown, VQ5),
    VQ7 = variable_queue_init(test_amqqueue(QName, true), true),
    {empty, VQ8} = rabbit_variable_queue:fetch(false, VQ7),
    VQ8.

variable_queue_drop(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, variable_queue_drop1, [Config]).

variable_queue_drop1(Config) ->
    with_fresh_variable_queue(
      fun variable_queue_drop2/2,
      ?config(variable_queue_type, Config)).

variable_queue_drop2(VQ0, _QName) ->
    %% start by sending a messages
    VQ1 = variable_queue_publish(false, 1, VQ0),
    %% drop message with AckRequired = true
    {{MsgId, AckTag}, VQ2} = rabbit_variable_queue:drop(true, VQ1),
    true = rabbit_variable_queue:is_empty(VQ2),
    true = AckTag =/= undefinded,
    %% drop again -> empty
    {empty, VQ3} = rabbit_variable_queue:drop(false, VQ2),
    %% requeue
    {[MsgId], VQ4} = rabbit_variable_queue:requeue([AckTag], VQ3),
    %% drop message with AckRequired = false
    {{MsgId, undefined}, VQ5} = rabbit_variable_queue:drop(false, VQ4),
    true = rabbit_variable_queue:is_empty(VQ5),
    VQ5.

variable_queue_fold_msg_on_disk(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, variable_queue_fold_msg_on_disk1, [Config]).

variable_queue_fold_msg_on_disk1(Config) ->
    with_fresh_variable_queue(
      fun variable_queue_fold_msg_on_disk2/2,
      ?config(variable_queue_type, Config)).

variable_queue_fold_msg_on_disk2(VQ0, _QName) ->
    VQ1 = variable_queue_publish(true, 1, VQ0),
    {VQ2, AckTags} = variable_queue_fetch(1, true, false, 1, VQ1),
    {ok, VQ3} = rabbit_variable_queue:ackfold(fun (_M, _A, ok) -> ok end,
                                              ok, VQ2, AckTags),
    VQ3.

variable_queue_dropfetchwhile(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, variable_queue_dropfetchwhile1, [Config]).

variable_queue_dropfetchwhile1(Config) ->
    with_fresh_variable_queue(
      fun variable_queue_dropfetchwhile2/2,
      ?config(variable_queue_type, Config)).

variable_queue_dropfetchwhile2(VQ0, _QName) ->
    Count = 10,

    %% add messages with sequential expiry
    VQ1 = variable_queue_publish(
            false, 1, Count,
            fun (N, Props) -> Props#message_properties{expiry = N} end,
            fun erlang:term_to_binary/1, VQ0),

    %% fetch the first 5 messages
    {#message_properties{expiry = 6}, {Msgs, AckTags}, VQ2} =
        rabbit_variable_queue:fetchwhile(
          fun (#message_properties{expiry = Expiry}) -> Expiry =< 5 end,
          fun (Msg, AckTag, {MsgAcc, AckAcc}) ->
                  {[Msg | MsgAcc], [AckTag | AckAcc]}
          end, {[], []}, VQ1),
    true = lists:seq(1, 5) == [msg2int(M) || M <- lists:reverse(Msgs)],

    %% requeue them
    {_MsgIds, VQ3} = rabbit_variable_queue:requeue(AckTags, VQ2),

    %% drop the first 5 messages
    {#message_properties{expiry = 6}, VQ4} =
        rabbit_variable_queue:dropwhile(
          fun (#message_properties {expiry = Expiry}) -> Expiry =< 5 end, VQ3),

    %% fetch 5
    VQ5 = lists:foldl(fun (N, VQN) ->
                              {{Msg, _, _}, VQM} =
                                  rabbit_variable_queue:fetch(false, VQN),
                              true = msg2int(Msg) == N,
                              VQM
                      end, VQ4, lists:seq(6, Count)),

    %% should be empty now
    true = rabbit_variable_queue:is_empty(VQ5),

    VQ5.

variable_queue_dropwhile_restart(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, variable_queue_dropwhile_restart1, [Config]).

variable_queue_dropwhile_restart1(Config) ->
    with_fresh_variable_queue(
      fun variable_queue_dropwhile_restart2/2,
      ?config(variable_queue_type, Config)).

variable_queue_dropwhile_restart2(VQ0, QName) ->
    Count = 10000,

    %% add messages with sequential expiry
    VQ1 = variable_queue_publish(
            true, 1, Count,
            fun (N, Props) -> Props#message_properties{expiry = N} end,
            fun erlang:term_to_binary/1, VQ0),

    %% drop the first 5 messages
    {#message_properties{expiry = 6}, VQ2} =
        rabbit_variable_queue:dropwhile(
          fun (#message_properties {expiry = Expiry}) -> Expiry =< 5 end, VQ1),

    _VQ3 = rabbit_variable_queue:terminate(shutdown, VQ2),
    Terms = variable_queue_read_terms(QName),
    VQ4 = variable_queue_init(test_amqqueue(QName, true), Terms),

    %% fetch 5
    VQ5 = lists:foldl(fun (_, VQN) ->
                              {{_, _, _}, VQM} =
                                  rabbit_variable_queue:fetch(false, VQN),
                              VQM
                      end, VQ4, lists:seq(6, Count)),

    %% should be empty now
    true = rabbit_variable_queue:is_empty(VQ5),

    VQ5.

variable_queue_dropwhile_sync_restart(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, variable_queue_dropwhile_sync_restart1, [Config]).

variable_queue_dropwhile_sync_restart1(Config) ->
    with_fresh_variable_queue(
      fun variable_queue_dropwhile_sync_restart2/2,
      ?config(variable_queue_type, Config)).

variable_queue_dropwhile_sync_restart2(VQ0, QName) ->
    Count = 10000,

    %% add messages with sequential expiry
    VQ1 = variable_queue_publish(
            true, 1, Count,
            fun (N, Props) -> Props#message_properties{expiry = N} end,
            fun erlang:term_to_binary/1, VQ0),

    %% drop the first 5 messages
    {#message_properties{expiry = 6}, VQ2} =
        rabbit_variable_queue:dropwhile(
          fun (#message_properties {expiry = Expiry}) -> Expiry =< 5 end, VQ1),

    %% Queue index sync.
    VQ2b = rabbit_variable_queue:handle_pre_hibernate(VQ2),

    _VQ3 = rabbit_variable_queue:terminate(shutdown, VQ2b),
    Terms = variable_queue_read_terms(QName),
    VQ4 = variable_queue_init(test_amqqueue(QName, true), Terms),

    %% fetch 5
    VQ5 = lists:foldl(fun (_, VQN) ->
                              {{_, _, _}, VQM} =
                                  rabbit_variable_queue:fetch(false, VQN),
                              VQM
                      end, VQ4, lists:seq(6, Count)),

    %% should be empty now
    true = rabbit_variable_queue:is_empty(VQ5),

    VQ5.

variable_queue_dropwhile_varying_ram_duration(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, variable_queue_dropwhile_varying_ram_duration1, [Config]).

variable_queue_dropwhile_varying_ram_duration1(Config) ->
    with_fresh_variable_queue(
      fun variable_queue_dropwhile_varying_ram_duration2/2,
      ?config(variable_queue_type, Config)).

variable_queue_dropwhile_varying_ram_duration2(VQ0, _QName) ->
    test_dropfetchwhile_varying_ram_duration(
      fun (VQ1) ->
              {_, VQ2} = rabbit_variable_queue:dropwhile(
                           fun (_) -> false end, VQ1),
              VQ2
      end, VQ0).

variable_queue_fetchwhile_varying_ram_duration(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, variable_queue_fetchwhile_varying_ram_duration1, [Config]).

variable_queue_fetchwhile_varying_ram_duration1(Config) ->
    with_fresh_variable_queue(
      fun variable_queue_fetchwhile_varying_ram_duration2/2,
      ?config(variable_queue_type, Config)).

variable_queue_fetchwhile_varying_ram_duration2(VQ0, _QName) ->
    test_dropfetchwhile_varying_ram_duration(
      fun (VQ1) ->
              {_, ok, VQ2} = rabbit_variable_queue:fetchwhile(
                               fun (_) -> false end,
                               fun (_, _, A) -> A end,
                               ok, VQ1),
              VQ2
      end, VQ0).

test_dropfetchwhile_varying_ram_duration(Fun, VQ0) ->
    VQ1 = variable_queue_publish(false, 1, VQ0),
    VQ2 = variable_queue_set_ram_duration_target(0, VQ1),
    VQ3 = Fun(VQ2),
    VQ4 = variable_queue_set_ram_duration_target(infinity, VQ3),
    VQ5 = variable_queue_publish(false, 1, VQ4),
    VQ6 = Fun(VQ5),
    VQ6.

variable_queue_ack_limiting(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, variable_queue_ack_limiting1, [Config]).

variable_queue_ack_limiting1(Config) ->
    with_fresh_variable_queue(
      fun variable_queue_ack_limiting2/2,
      ?config(variable_queue_type, Config)).

variable_queue_ack_limiting2(VQ0, _Config) ->
    %% start by sending in a bunch of messages
    Len = 1024,
    VQ1 = variable_queue_publish(false, Len, VQ0),

    %% squeeze and relax queue
    Churn = Len div 32,
    VQ2 = publish_fetch_and_ack(Churn, Len, VQ1),

    %% update stats for duration
    {_Duration, VQ3} = rabbit_variable_queue:ram_duration(VQ2),

    %% fetch half the messages
    {VQ4, _AckTags} = variable_queue_fetch(Len div 2, false, false, Len, VQ3),

    VQ5 = check_variable_queue_status(
            VQ4, [{len,                         Len div 2},
                  {messages_unacknowledged_ram, Len div 2},
                  {messages_ready_ram,          Len div 2},
                  {messages_ram,                Len}]),

    %% ensure all acks go to disk on 0 duration target
    VQ6 = check_variable_queue_status(
            variable_queue_set_ram_duration_target(0, VQ5),
            [{len,                         Len div 2},
             {target_ram_count,            0},
             {messages_unacknowledged_ram, 0},
             {messages_ready_ram,          0},
             {messages_ram,                0}]),

    VQ6.

variable_queue_purge(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, variable_queue_purge1, [Config]).

variable_queue_purge1(Config) ->
    with_fresh_variable_queue(
      fun variable_queue_purge2/2,
      ?config(variable_queue_type, Config)).

variable_queue_purge2(VQ0, _Config) ->
    LenDepth = fun (VQ) ->
                       {rabbit_variable_queue:len(VQ),
                        rabbit_variable_queue:depth(VQ)}
               end,
    VQ1         = variable_queue_publish(false, 10, VQ0),
    {VQ2, Acks} = variable_queue_fetch(6, false, false, 10, VQ1),
    {4, VQ3}    = rabbit_variable_queue:purge(VQ2),
    {0, 6}      = LenDepth(VQ3),
    {_, VQ4}    = rabbit_variable_queue:requeue(lists:sublist(Acks, 2), VQ3),
    {2, 6}      = LenDepth(VQ4),
    VQ5         = rabbit_variable_queue:purge_acks(VQ4),
    {2, 2}      = LenDepth(VQ5),
    VQ5.

variable_queue_requeue(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, variable_queue_requeue1, [Config]).

variable_queue_requeue1(Config) ->
    with_fresh_variable_queue(
      fun variable_queue_requeue2/2,
      ?config(variable_queue_type, Config)).

variable_queue_requeue2(VQ0, _Config) ->
    {_PendingMsgs, RequeuedMsgs, FreshMsgs, VQ1} =
        variable_queue_with_holes(VQ0),
    Msgs =
        lists:zip(RequeuedMsgs,
                  lists:duplicate(length(RequeuedMsgs), true)) ++
        lists:zip(FreshMsgs,
                  lists:duplicate(length(FreshMsgs), false)),
    VQ2 = lists:foldl(fun ({I, Requeued}, VQa) ->
                              {{M, MRequeued, _}, VQb} =
                                  rabbit_variable_queue:fetch(true, VQa),
                              Requeued = MRequeued, %% assertion
                              I = msg2int(M),       %% assertion
                              VQb
                      end, VQ1, Msgs),
    {empty, VQ3} = rabbit_variable_queue:fetch(true, VQ2),
    VQ3.

%% requeue from ram_pending_ack into q3, move to delta and then empty queue
variable_queue_requeue_ram_beta(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, variable_queue_requeue_ram_beta1, [Config]).

variable_queue_requeue_ram_beta1(Config) ->
    with_fresh_variable_queue(
      fun variable_queue_requeue_ram_beta2/2,
      ?config(variable_queue_type, Config)).

variable_queue_requeue_ram_beta2(VQ0, _Config) ->
    Count = rabbit_queue_index:next_segment_boundary(0)*2 + 2,
    VQ1 = variable_queue_publish(false, Count, VQ0),
    {VQ2, AcksR} = variable_queue_fetch(Count, false, false, Count, VQ1),
    {Back, Front} = lists:split(Count div 2, AcksR),
    {_, VQ3} = rabbit_variable_queue:requeue(erlang:tl(Back), VQ2),
    VQ4 = variable_queue_set_ram_duration_target(0, VQ3),
    {_, VQ5} = rabbit_variable_queue:requeue([erlang:hd(Back)], VQ4),
    VQ6 = requeue_one_by_one(Front, VQ5),
    {VQ7, AcksAll} = variable_queue_fetch(Count, false, true, Count, VQ6),
    {_, VQ8} = rabbit_variable_queue:ack(AcksAll, VQ7),
    VQ8.

variable_queue_fold(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, variable_queue_fold1, [Config]).

variable_queue_fold1(Config) ->
    with_fresh_variable_queue(
      fun variable_queue_fold2/2,
      ?config(variable_queue_type, Config)).

variable_queue_fold2(VQ0, _Config) ->
    {PendingMsgs, RequeuedMsgs, FreshMsgs, VQ1} =
        variable_queue_with_holes(VQ0),
    Count = rabbit_variable_queue:depth(VQ1),
    Msgs = lists:sort(PendingMsgs ++ RequeuedMsgs ++ FreshMsgs),
    lists:foldl(fun (Cut, VQ2) ->
                        test_variable_queue_fold(Cut, Msgs, PendingMsgs, VQ2)
                end, VQ1, [0, 1, 2, Count div 2,
                           Count - 1, Count, Count + 1, Count * 2]).

test_variable_queue_fold(Cut, Msgs, PendingMsgs, VQ0) ->
    {Acc, VQ1} = rabbit_variable_queue:fold(
                   fun (M, _, Pending, A) ->
                           MInt = msg2int(M),
                           Pending = lists:member(MInt, PendingMsgs), %% assert
                           case MInt =< Cut of
                               true  -> {cont, [MInt | A]};
                               false -> {stop, A}
                           end
                   end, [], VQ0),
    Expected = lists:takewhile(fun (I) -> I =< Cut end, Msgs),
    Expected = lists:reverse(Acc), %% assertion
    VQ1.

variable_queue_batch_publish(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, variable_queue_batch_publish1, [Config]).

variable_queue_batch_publish1(Config) ->
    with_fresh_variable_queue(
      fun variable_queue_batch_publish2/2,
      ?config(variable_queue_type, Config)).

variable_queue_batch_publish2(VQ, _Config) ->
    Count = 10,
    VQ1 = variable_queue_batch_publish(true, Count, VQ),
    Count = rabbit_variable_queue:len(VQ1),
    VQ1.

variable_queue_batch_publish_delivered(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, variable_queue_batch_publish_delivered1, [Config]).

variable_queue_batch_publish_delivered1(Config) ->
    with_fresh_variable_queue(
      fun variable_queue_batch_publish_delivered2/2,
      ?config(variable_queue_type, Config)).

variable_queue_batch_publish_delivered2(VQ, _Config) ->
    Count = 10,
    VQ1 = variable_queue_batch_publish_delivered(true, Count, VQ),
    Count = rabbit_variable_queue:depth(VQ1),
    VQ1.

%% same as test_variable_queue_requeue_ram_beta but randomly changing
%% the queue mode after every step.
variable_queue_mode_change(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, variable_queue_mode_change1, [Config]).

variable_queue_mode_change1(Config) ->
    with_fresh_variable_queue(
      fun variable_queue_mode_change2/2,
      ?config(variable_queue_type, Config)).

variable_queue_mode_change2(VQ0, _Config) ->
    Count = rabbit_queue_index:next_segment_boundary(0)*2 + 2,
    VQ1 = variable_queue_publish(false, Count, VQ0),
    VQ2 = maybe_switch_queue_mode(VQ1),
    {VQ3, AcksR} = variable_queue_fetch(Count, false, false, Count, VQ2),
    VQ4 = maybe_switch_queue_mode(VQ3),
    {Back, Front} = lists:split(Count div 2, AcksR),
    {_, VQ5} = rabbit_variable_queue:requeue(erlang:tl(Back), VQ4),
    VQ6 = maybe_switch_queue_mode(VQ5),
    VQ7 = variable_queue_set_ram_duration_target(0, VQ6),
    VQ8 = maybe_switch_queue_mode(VQ7),
    {_, VQ9} = rabbit_variable_queue:requeue([erlang:hd(Back)], VQ8),
    VQ10 = maybe_switch_queue_mode(VQ9),
    VQ11 = requeue_one_by_one(Front, VQ10),
    VQ12 = maybe_switch_queue_mode(VQ11),
    {VQ13, AcksAll} = variable_queue_fetch(Count, false, true, Count, VQ12),
    VQ14 = maybe_switch_queue_mode(VQ13),
    {_, VQ15} = rabbit_variable_queue:ack(AcksAll, VQ14),
    VQ16 = maybe_switch_queue_mode(VQ15),
    VQ16.

maybe_switch_queue_mode(VQ) ->
    Mode = random_queue_mode(),
    set_queue_mode(Mode, VQ).

random_queue_mode() ->
    Modes = [lazy, default],
    lists:nth(rand:uniform(length(Modes)), Modes).

pub_res({_, VQS}) ->
    VQS;
pub_res(VQS) ->
    VQS.

make_publish(IsPersistent, PayloadFun, PropFun, N) ->
    {rabbit_basic:message(
       rabbit_misc:r(<<>>, exchange, <<>>),
       <<>>, #'P_basic'{delivery_mode = case IsPersistent of
                                            true  -> 2;
                                            false -> 1
                                        end},
       PayloadFun(N)),
     PropFun(N, #message_properties{size = 10}),
     false}.

make_publish_delivered(IsPersistent, PayloadFun, PropFun, N) ->
    {rabbit_basic:message(
       rabbit_misc:r(<<>>, exchange, <<>>),
       <<>>, #'P_basic'{delivery_mode = case IsPersistent of
                                            true  -> 2;
                                            false -> 1
                                        end},
       PayloadFun(N)),
     PropFun(N, #message_properties{size = 10})}.

queue_name(Config, Name) ->
    Name1 = iolist_to_binary(rabbit_ct_helpers:config_to_testcase_name(Config, Name)),
    queue_name(Name1).

queue_name(Name) ->
    rabbit_misc:r(<<"/">>, queue, Name).

test_queue() ->
    queue_name(rabbit_guid:gen()).

init_test_queue(QName) ->
    PRef = rabbit_guid:gen(),
    PersistentClient = msg_store_client_init(?PERSISTENT_MSG_STORE, PRef),
    Res = rabbit_queue_index:recover(
            QName, [], false,
            fun (MsgId) ->
                    rabbit_msg_store:contains(MsgId, PersistentClient)
            end,
            fun nop/1, fun nop/1),
    ok = rabbit_msg_store:client_delete_and_terminate(PersistentClient),
    Res.

restart_test_queue(Qi, QName) ->
    _ = rabbit_queue_index:terminate(?VHOST, [], Qi),
    ok = rabbit_variable_queue:stop(?VHOST),
    {ok, _} = rabbit_variable_queue:start(?VHOST, [QName]),
    init_test_queue(QName).

empty_test_queue(QName) ->
    ok = rabbit_variable_queue:stop(?VHOST),
    {ok, _} = rabbit_variable_queue:start(?VHOST, []),
    {0, 0, Qi} = init_test_queue(QName),
    _ = rabbit_queue_index:delete_and_terminate(Qi),
    ok.

unin_empty_test_queue(QName) ->
    {0, 0, Qi} = init_test_queue(QName),
    _ = rabbit_queue_index:delete_and_terminate(Qi),
    ok.

with_empty_test_queue(Fun) ->
    QName = test_queue(),
    ok = empty_test_queue(QName),
    {0, 0, Qi} = init_test_queue(QName),
    rabbit_queue_index:delete_and_terminate(Fun(Qi, QName)).

init_queue_index() ->
    %% We must set the segment entry count in the process dictionary
    %% for tests that call the queue index directly to have a correct
    %% value.
    put(segment_entry_count, 2048),
    ok.

restart_app() ->
    rabbit:stop(),
    rabbit:start().

queue_index_publish(SeqIds, Persistent, Qi) ->
    Ref = rabbit_guid:gen(),
    MsgStore = case Persistent of
                   true  -> ?PERSISTENT_MSG_STORE;
                   false -> ?TRANSIENT_MSG_STORE
               end,
    MSCState = msg_store_client_init(MsgStore, Ref),
    {A, B = [{_SeqId, LastMsgIdWritten} | _]} =
        lists:foldl(
          fun (SeqId, {QiN, SeqIdsMsgIdsAcc}) ->
                  MsgId = rabbit_guid:gen(),
                  QiM = rabbit_queue_index:publish(
                          MsgId, SeqId, #message_properties{size = 10},
                          Persistent, infinity, QiN),
                  ok = rabbit_msg_store:write(MsgId, MsgId, MSCState),
                  {QiM, [{SeqId, MsgId} | SeqIdsMsgIdsAcc]}
          end, {Qi, []}, SeqIds),
    %% do this just to force all of the publishes through to the msg_store:
    true = rabbit_msg_store:contains(LastMsgIdWritten, MSCState),
    ok = rabbit_msg_store:client_delete_and_terminate(MSCState),
    {A, B}.

verify_read_with_published(_Delivered, _Persistent, [], _) ->
    ok;
verify_read_with_published(Delivered, Persistent,
                           [{MsgId, SeqId, _Props, Persistent, Delivered}|Read],
                           [{SeqId, MsgId}|Published]) ->
    verify_read_with_published(Delivered, Persistent, Read, Published);
verify_read_with_published(_Delivered, _Persistent, _Read, _Published) ->
    ko.

nop(_) -> ok.
nop(_, _) -> ok.

msg_store_client_init(MsgStore, Ref) ->
    rabbit_vhost_msg_store:client_init(?VHOST, MsgStore, Ref,  undefined, undefined).

variable_queue_init(Q, Recover) ->
    rabbit_variable_queue:init(
      Q, case Recover of
             true  -> non_clean_shutdown;
             false -> new;
             Terms -> Terms
         end, fun nop/2, fun nop/2, fun nop/1, fun nop/1).

variable_queue_read_terms(QName) ->
    #resource { kind = queue,
                virtual_host = VHost,
                name = Name } = QName,
    <<Num:128>> = erlang:md5(<<"queue", VHost/binary, Name/binary>>),
    DirName = rabbit_misc:format("~.36B", [Num]),
    {ok, Terms} = rabbit_recovery_terms:read(VHost, DirName),
    Terms.

publish_and_confirm(Q, Payload, Count) ->
    Seqs = lists:seq(1, Count),
    QTState0 = rabbit_queue_type:new(Q, rabbit_queue_type:init()),
    QTState =
    lists:foldl(
      fun (Seq, Acc0) ->
              Msg = rabbit_basic:message(rabbit_misc:r(<<>>, exchange, <<>>),
                                         <<>>, #'P_basic'{delivery_mode = 2},
                                         Payload),
              Delivery = #delivery{mandatory = false, sender = self(),
                                   confirm = true, message = Msg, msg_seq_no = Seq,
                                   flow = noflow},
              {ok, Acc, _Actions} = rabbit_queue_type:deliver([Q], Delivery, Acc0),
              Acc
      end, QTState0, Seqs),
    wait_for_confirms(gb_sets:from_list(Seqs)),
    QTState.

wait_for_confirms(Unconfirmed) ->
    case gb_sets:is_empty(Unconfirmed) of
        true  -> ok;
        false ->
            receive
                {'$gen_cast',
                 {queue_event, _QName, {confirm, Confirmed, _}}} ->
                    wait_for_confirms(
                      rabbit_misc:gb_sets_difference(
                        Unconfirmed, gb_sets:from_list(Confirmed)));
                {'$gen_cast', {confirm, Confirmed, _}} ->
                    wait_for_confirms(
                      rabbit_misc:gb_sets_difference(
                        Unconfirmed, gb_sets:from_list(Confirmed)))
            after ?TIMEOUT ->
                      flush(),
                      exit(timeout_waiting_for_confirm)
            end
    end.

with_fresh_variable_queue(Fun, Mode) ->
    Ref = make_ref(),
    Me = self(),
    %% Run in a separate process since rabbit_msg_store will send
    %% bump_credit messages and we want to ignore them
    spawn_link(fun() ->
                       QName = test_queue(),
                       ok = unin_empty_test_queue(QName),
                       VQ = variable_queue_init(test_amqqueue(QName, true), false),
                       S0 = variable_queue_status(VQ),
                       assert_props(S0, [{q1, 0}, {q2, 0},
                                         {delta,
                                          {delta, undefined, 0, undefined}},
                                         {q3, 0}, {q4, 0},
                                         {len, 0}]),
                       VQ1 = set_queue_mode(Mode, VQ),
                       try
                           _ = rabbit_variable_queue:delete_and_terminate(
                                 shutdown, Fun(VQ1, QName)),
                           Me ! Ref
                       catch
                           Type:Error:Stacktrace ->
                               Me ! {Ref, Type, Error, Stacktrace}
                       end
               end),
    receive
        Ref                    -> ok;
        {Ref, Type, Error, ST} -> exit({Type, Error, ST})
    end,
    passed.

set_queue_mode(Mode, VQ) ->
    VQ1 = rabbit_variable_queue:set_queue_mode(Mode, VQ),
    S1 = variable_queue_status(VQ1),
    assert_props(S1, [{mode, Mode}]),
    VQ1.

variable_queue_publish(IsPersistent, Count, VQ) ->
    variable_queue_publish(IsPersistent, Count, fun (_N, P) -> P end, VQ).

variable_queue_publish(IsPersistent, Count, PropFun, VQ) ->
    variable_queue_publish(IsPersistent, 1, Count, PropFun,
                           fun (_N) -> <<>> end, VQ).

variable_queue_publish(IsPersistent, Start, Count, PropFun, PayloadFun, VQ) ->
    variable_queue_wait_for_shuffling_end(
      lists:foldl(
        fun (N, VQN) ->

                rabbit_variable_queue:publish(
                  rabbit_basic:message(
                    rabbit_misc:r(<<>>, exchange, <<>>),
                    <<>>, #'P_basic'{delivery_mode = case IsPersistent of
                                                         true  -> 2;
                                                         false -> 1
                                                     end},
                    PayloadFun(N)),
                  PropFun(N, #message_properties{size = 10}),
                  false, self(), noflow, VQN)
        end, VQ, lists:seq(Start, Start + Count - 1))).

variable_queue_batch_publish(IsPersistent, Count, VQ) ->
    variable_queue_batch_publish(IsPersistent, Count, fun (_N, P) -> P end, VQ).

variable_queue_batch_publish(IsPersistent, Count, PropFun, VQ) ->
    variable_queue_batch_publish(IsPersistent, 1, Count, PropFun,
                                 fun (_N) -> <<>> end, VQ).

variable_queue_batch_publish(IsPersistent, Start, Count, PropFun, PayloadFun, VQ) ->
    variable_queue_batch_publish0(IsPersistent, Start, Count, PropFun,
                                  PayloadFun, fun make_publish/4,
                                  fun rabbit_variable_queue:batch_publish/4,
                                  VQ).

variable_queue_batch_publish_delivered(IsPersistent, Count, VQ) ->
    variable_queue_batch_publish_delivered(IsPersistent, Count, fun (_N, P) -> P end, VQ).

variable_queue_batch_publish_delivered(IsPersistent, Count, PropFun, VQ) ->
    variable_queue_batch_publish_delivered(IsPersistent, 1, Count, PropFun,
                                           fun (_N) -> <<>> end, VQ).

variable_queue_batch_publish_delivered(IsPersistent, Start, Count, PropFun, PayloadFun, VQ) ->
    variable_queue_batch_publish0(IsPersistent, Start, Count, PropFun,
                                  PayloadFun, fun make_publish_delivered/4,
                                  fun rabbit_variable_queue:batch_publish_delivered/4,
                                  VQ).

variable_queue_batch_publish0(IsPersistent, Start, Count, PropFun, PayloadFun,
                              MakePubFun, PubFun, VQ) ->
    Publishes =
        [MakePubFun(IsPersistent, PayloadFun, PropFun, N)
         || N <- lists:seq(Start, Start + Count - 1)],
    Res = PubFun(Publishes, self(), noflow, VQ),
    VQ1 = pub_res(Res),
    variable_queue_wait_for_shuffling_end(VQ1).

variable_queue_fetch(Count, IsPersistent, IsDelivered, Len, VQ) ->
    lists:foldl(fun (N, {VQN, AckTagsAcc}) ->
                        Rem = Len - N,
                        {{#basic_message { is_persistent = IsPersistent },
                          IsDelivered, AckTagN}, VQM} =
                            rabbit_variable_queue:fetch(true, VQN),
                        Rem = rabbit_variable_queue:len(VQM),
                        {VQM, [AckTagN | AckTagsAcc]}
                end, {VQ, []}, lists:seq(1, Count)).

test_amqqueue(QName, Durable) ->
    rabbit_amqqueue:pseudo_queue(QName, self(), Durable).

assert_prop(List, Prop, Value) ->
    case proplists:get_value(Prop, List)of
        Value -> ok;
        _     -> {exit, Prop, exp, Value, List}
    end.

assert_props(List, PropVals) ->
    [assert_prop(List, Prop, Value) || {Prop, Value} <- PropVals].

variable_queue_set_ram_duration_target(Duration, VQ) ->
    variable_queue_wait_for_shuffling_end(
      rabbit_variable_queue:set_ram_duration_target(Duration, VQ)).

publish_fetch_and_ack(0, _Len, VQ0) ->
    VQ0;
publish_fetch_and_ack(N, Len, VQ0) ->
    VQ1 = variable_queue_publish(false, 1, VQ0),
    {{_Msg, false, AckTag}, VQ2} = rabbit_variable_queue:fetch(true, VQ1),
    Len = rabbit_variable_queue:len(VQ2),
    {_Guids, VQ3} = rabbit_variable_queue:ack([AckTag], VQ2),
    publish_fetch_and_ack(N-1, Len, VQ3).

variable_queue_status(VQ) ->
    Keys = rabbit_backing_queue:info_keys() -- [backing_queue_status],
    [{K, rabbit_variable_queue:info(K, VQ)} || K <- Keys] ++
        rabbit_variable_queue:info(backing_queue_status, VQ).

variable_queue_wait_for_shuffling_end(VQ) ->
    case credit_flow:blocked() of
        false -> VQ;
        true  ->
            receive
                {bump_credit, Msg} ->
                    credit_flow:handle_bump_msg(Msg),
                    variable_queue_wait_for_shuffling_end(
                      rabbit_variable_queue:resume(VQ))
            end
    end.

msg2int(#basic_message{content = #content{ payload_fragments_rev = P}}) ->
    binary_to_term(list_to_binary(lists:reverse(P))).

ack_subset(AckSeqs, Interval, Rem) ->
    lists:filter(fun ({_Ack, N}) -> (N + Rem) rem Interval == 0 end, AckSeqs).

requeue_one_by_one(Acks, VQ) ->
    lists:foldl(fun (AckTag, VQN) ->
                        {_MsgId, VQM} = rabbit_variable_queue:requeue(
                                          [AckTag], VQN),
                        VQM
                end, VQ, Acks).

%% Create a vq with messages in q1, delta, and q3, and holes (in the
%% form of pending acks) in the latter two.
variable_queue_with_holes(VQ0) ->
    Interval = 2048, %% should match vq:IO_BATCH_SIZE
    Count = rabbit_queue_index:next_segment_boundary(0)*2 + 2 * Interval,
    Seq = lists:seq(1, Count),
    VQ1 = variable_queue_publish(
            false, 1, Count,
            fun (_, P) -> P end, fun erlang:term_to_binary/1, VQ0),
    VQ2 = variable_queue_set_ram_duration_target(0, VQ1),
    {VQ3, AcksR} = variable_queue_fetch(Count, false, false, Count, VQ2),
    Acks = lists:reverse(AcksR),
    AckSeqs = lists:zip(Acks, Seq),
    [{Subset1, _Seq1}, {Subset2, _Seq2}, {Subset3, Seq3}] =
        [lists:unzip(ack_subset(AckSeqs, Interval, I)) || I <- [0, 1, 2]],
    %% we requeue in three phases in order to exercise requeuing logic
    %% in various vq states
    {_MsgIds, VQ4} = rabbit_variable_queue:requeue(
                       Acks -- (Subset1 ++ Subset2 ++ Subset3), VQ3),
    VQ5 = requeue_one_by_one(Subset1, VQ4),
    %% by now we have some messages (and holes) in delta
    VQ6 = requeue_one_by_one(Subset2, VQ5),
    VQ7 = variable_queue_set_ram_duration_target(infinity, VQ6),
    %% add the q1 tail
    VQ8 = variable_queue_publish(
            true, Count + 1, Interval,
            fun (_, P) -> P end, fun erlang:term_to_binary/1, VQ7),
    %% assertions
    Status = variable_queue_status(VQ8),

    vq_with_holes_assertions(VQ8, proplists:get_value(mode, Status)),
    Depth = Count + Interval,
    Depth = rabbit_variable_queue:depth(VQ8),
    Len = Depth - length(Subset3),
    Len = rabbit_variable_queue:len(VQ8),

    {Seq3, Seq -- Seq3, lists:seq(Count + 1, Count + Interval), VQ8}.

vq_with_holes_assertions(VQ, default) ->
    [false =
         case V of
             {delta, _, 0, _} -> true;
             0                -> true;
             _                -> false
         end || {K, V} <- variable_queue_status(VQ),
                lists:member(K, [q1, delta, q3])];
vq_with_holes_assertions(VQ, lazy) ->
    [false =
         case V of
             {delta, _, 0, _} -> true;
             _                -> false
         end || {K, V} <- variable_queue_status(VQ),
                lists:member(K, [delta])].

check_variable_queue_status(VQ0, Props) ->
    VQ1 = variable_queue_wait_for_shuffling_end(VQ0),
    S = variable_queue_status(VQ1),
    assert_props(S, Props),
    VQ1.

flush() ->
    receive
        Any ->
            ct:pal("flush ~p", [Any]),
            flush()
    after 0 ->
              ok
    end.
