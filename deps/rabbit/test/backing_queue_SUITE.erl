%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(backing_queue_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include("amqqueue.hrl").

-compile(nowarn_export_all).
-compile(export_all).

-define(PERSISTENT_MSG_STORE, msg_store_persistent).
-define(TRANSIENT_MSG_STORE,  msg_store_transient).

-define(TIMEOUT, 30000).
-define(VHOST, <<"/">>).

-define(VARIABLE_QUEUE_TESTCASES, [
    variable_queue_partial_segments_q_tail_thing,
    variable_queue_all_the_bits_not_covered_elsewhere_A,
    variable_queue_all_the_bits_not_covered_elsewhere_B,
    variable_queue_drop,
    variable_queue_fold_msg_on_disk,
    variable_queue_dropfetchwhile,
    variable_queue_dropwhile_restart,
    variable_queue_dropwhile_sync_restart,
    variable_queue_restart_large_seq_id,
    variable_queue_ack_limiting,
    variable_queue_purge,
    variable_queue_requeue,
    variable_queue_requeue_ram_beta
  ]).

-define(BACKING_QUEUE_TESTCASES, [
    bq_queue_index,
    bq_queue_index_props,
    {variable_queue, [parallel], ?VARIABLE_QUEUE_TESTCASES},
    bq_variable_queue_delete_msg_store_files_callback,
    bq_queue_recover
  ]).

all() ->
    [
      {group, backing_queue_tests}
    ].

groups() ->
    Common = [
        {backing_queue_embed_limit_0, [], ?BACKING_QUEUE_TESTCASES},
        {backing_queue_embed_limit_1024, [], ?BACKING_QUEUE_TESTCASES}
    ],
    V2Only = [
        v2_delete_segment_file_completely_acked,
        v2_delete_segment_file_partially_acked,
        v2_delete_segment_file_partially_acked_with_holes,
        v2_reset_state_no_slash_accumulation
    ],
    [
     {backing_queue_tests, [], [
          msg_store,
          msg_store_read_many_fanout,
          msg_store_compaction_v2,
          msg_store_compaction_v2_exact_fit,
          msg_store_compaction_v2_scannable_before_truncate,
          msg_store_compaction_v2_packed_run_atomic,
          msg_store_compaction_v2_packed_run_with_trailing_hole,
          msg_store_v1_compat,
          msg_store_v1_current_file_emptied_before_crash,
          msg_store_recovers_torn_current_file,
          msg_store_recovers_from_corrupted_file,
          msg_store_recovers_from_corrupted_file_no_fd_leak,
          msg_store_read_error_includes_msg_id,
          msg_store_recovers_from_corrupted_non_current_file,
          msg_store_v2_scan_failure_crashes_recovery,
          msg_store_v1_scan_failure_crashes_recovery,
          msg_store_file_scan,
          msg_store_file_scan_v2,
          msg_store_gc_stuck_suspended,
          msg_store_gc_stuck_mid_callback,
          {backing_queue_v2, [], Common ++ V2Only}
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
            ClusterSize = 1,
            %% msg_store_v1_scan_failure_crashes_recovery and
            %% msg_store_v2_scan_failure_crashes_recovery each
            %% deliberately crash message store startup once, to prove
            %% a scan failure is not silently mistaken for corruption:
            %% expect those gen_server terminations rather than failing
            %% the whole group over them.
            Config1 = rabbit_ct_helpers:set_config(Config, [
                {rmq_nodename_suffix, Group},
                {rmq_nodes_count, ClusterSize},
                {ignored_crashes, ["eio"]}
              ]),
            rabbit_ct_helpers:run_steps(Config1,
              rabbit_ct_broker_helpers:setup_steps() ++
              rabbit_ct_client_helpers:setup_steps() ++ [
                fun(C) -> init_per_group1(Group, C) end
              ]);
        false ->
            rabbit_ct_helpers:run_steps(Config, [
                fun(C) -> init_per_group1(Group, C) end
              ])
    end.

init_per_group1(backing_queue_tests, Config) ->
    %% @todo Is that test still relevant?
    Module = rabbit_ct_broker_helpers:rpc(Config, 0,
      application, get_env, [rabbit, backing_queue_module]),
    case Module of
        {ok, rabbit_priority_queue} ->
            rabbit_ct_broker_helpers:rpc(Config, 0,
              ?MODULE, setup_backing_queue_test_group, [Config]);
        _ ->
            {skip, rabbit_misc:format(
               "Backing queue module not supported by this test group: ~tp~n",
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
%% @todo These groups are no longer used?
init_per_group1(from_cluster_node1, Config) ->
    rabbit_ct_helpers:set_config(Config, {test_direction, {0, 1}});
init_per_group1(from_cluster_node2, Config) ->
    rabbit_ct_helpers:set_config(Config, {test_direction, {1, 0}});
init_per_group1(_, Config) ->
    Config.

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

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Message store.
%% -------------------------------------------------------------------

msg_store(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, msg_store1, [Config]).

msg_store1(_Config) ->
    %% We simulate the SeqId (used as a message ref for the flying optimisation)
    %% using the process dictionary.
    GenRefFun = fun(Key) -> V = case get(Key) of undefined -> 0; V0 -> V0 end, put(Key, V + 1), V end,
    GenRef = fun() -> GenRefFun(msc) end,
    restart_msg_store_empty(),
    MsgIds = [{GenRef(), msg_id_bin(M)} || M <- lists:seq(1,100)],
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
    passed = test_msg_store_confirms([hd(MsgIds)], Cap, GenRef, MSCState),
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
    ok = msg_store_write([{GenRefFun(msc2), MsgId} || {_, MsgId} <- MsgIds2ndHalf], MSC2State),
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
    {ok, _} = msg_store_remove(MsgIds, MSCState2),
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
                    ([{_, MsgId}|MsgIdsTail])
                      when length(MsgIdsTail) rem 2 == 0 ->
                        {MsgId, 1, MsgIdsTail};
                    ([{_, MsgId}|MsgIdsTail]) ->
                        {MsgId, 0, MsgIdsTail}
                end, MsgIds2ndHalf}),
    MSCState5 = msg_store_client_init(?PERSISTENT_MSG_STORE, Ref),
    %% check we have the right msgs left
    lists:foldl(
      fun ({_, MsgId}, Bool) ->
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
    {ok, _} = msg_store_remove(MsgIds1stHalf, MSCState7),
    ok = rabbit_msg_store:client_terminate(MSCState7),
    %% restart empty
    restart_msg_store_empty(), %% now safe to reuse msg_ids
    %% push a lot of msgs in... at least 100 files worth
    {ok, FileSize} = application:get_env(rabbit, msg_store_file_size_limit),
    PayloadSizeBits = 65536,
    BigCount = trunc(100 * FileSize / (PayloadSizeBits div 8)),
    MsgIdsBig = [{GenRef(), msg_id_bin(X)} || X <- lists:seq(1, BigCount)],
    Payload = << 0:PayloadSizeBits >>,
    ok = with_msg_store_client(
           ?PERSISTENT_MSG_STORE, Ref,
           fun (MSCStateM) ->
                   [ok = rabbit_msg_store:write(SeqId, MsgId, Payload, MSCStateM) ||
                       {SeqId, MsgId} <- MsgIdsBig],
                   MSCStateM
           end),
    %% now read them to ensure we hit the fast client-side reading
    ok = foreach_with_msg_store_client(
           ?PERSISTENT_MSG_STORE, Ref,
           fun ({_, MsgId}, MSCStateM) ->
                   {{ok, Payload}, MSCStateN} = rabbit_msg_store:read(
                                                  MsgId, MSCStateM),
                   MSCStateN
           end, MsgIdsBig),
    %% We remove every other other message first, then do it again a second
    %% time with another set of messages and then a third time. We start
    %% with younger messages on purpose. So we split the list in three
    %% lists keeping the message reference.
    Part = fun
        PartFun([], _, Acc) ->
            Acc;
        PartFun([E|Tail], N, Acc) ->
            Pos = 1 + (N rem 3),
            AccL = element(Pos, Acc),
            PartFun(Tail, N + 1, setelement(Pos, Acc, [E|AccL]))
    end,
    {One, Two, Three} = Part(MsgIdsBig, 0, {[], [], []}),
    ok = msg_store_remove(?PERSISTENT_MSG_STORE, Ref, One),
    %% This is likely to hit GC (under 50% good data left in files, but no empty files).
    ok = msg_store_remove(?PERSISTENT_MSG_STORE, Ref, Two),
    %% Files are empty now and will get removed.
    ok = msg_store_remove(?PERSISTENT_MSG_STORE, Ref, Three),
    %% ensure empty
    ok = with_msg_store_client(
           ?PERSISTENT_MSG_STORE, Ref,
           fun (MSCStateM) ->
                   false = msg_store_contains(false, MsgIdsBig, MSCStateM),
                   MSCStateM
           end),
    %%
    passed = test_msg_store_client_delete_and_terminate(fun() -> GenRefFun(msc_cdat) end),
    %% restart empty
    restart_msg_store_empty(),
    passed.

msg_store_read_many_fanout(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, msg_store_read_many_fanout1, [Config]).

msg_store_read_many_fanout1(_Config) ->
    GenRefFun = fun(Key) -> V = case get(Key) of undefined -> 0; V0 -> V0 end, put(Key, V + 1), V end,
    GenRef = fun() -> GenRefFun(msc) end,
    %% We will fill the first message store file with random messages
    %% + 1 fanout message (written once for now). We will then write
    %% two messages from our queue, then the fanout message (to +1
    %% from our queue), and two more messages. We expect all messages
    %% from our queue to be in the current write file, except the
    %% fanout message. We then try to read the messages.
    restart_msg_store_empty(),
    CRef1 = rabbit_guid:gen(),
    CRef2 = rabbit_guid:gen(),
    {ok, FileSize} = application:get_env(rabbit, msg_store_file_size_limit),
    PayloadSizeBits = 65536,
    Payload = <<0:PayloadSizeBits>>,
    %% @todo -7 because -1 and -hd, fix better.
    NumRandomMsgs = (FileSize div (PayloadSizeBits div 8)) - 1,
    RandomMsgIds = [{GenRef(), msg_id_bin(X)} || X <- lists:seq(1, NumRandomMsgs)],
    FanoutMsgId = {GenRef(), msg_id_bin(NumRandomMsgs + 1)},
    [Q1, Q2, Q3, Q4] = [{GenRef(), msg_id_bin(X)} || X <- lists:seq(NumRandomMsgs + 2, NumRandomMsgs + 5)],
    QueueMsgIds0 = [Q1, Q2] ++ [FanoutMsgId] ++ [Q3, Q4],
    QueueMsgIds = [{GenRef(), M} || {_, M} <- QueueMsgIds0],
    BasicMsgFun = fun(MsgId) ->
        Ex = rabbit_misc:r(<<>>, exchange, <<>>),
        BasicMsg = rabbit_basic:message(Ex, <<>>,
                                        #'P_basic'{delivery_mode = 2},
                                        Payload),
        {ok, Msg0} = mc_amqpl:message(Ex, <<>>, BasicMsg#basic_message.content),
        mc:set_annotation(id, MsgId, Msg0)
    end,
    ok = with_msg_store_client(
           ?PERSISTENT_MSG_STORE, CRef1,
           fun (MSCStateM) ->
                   [begin
                       Msg = BasicMsgFun(MsgId),
                       ok = rabbit_msg_store:write(SeqId, MsgId, Msg, MSCStateM)
                   end || {SeqId, MsgId} <- [FanoutMsgId] ++ RandomMsgIds],
                   MSCStateM
           end),
    ok = with_msg_store_client(
           ?PERSISTENT_MSG_STORE, CRef2,
           fun (MSCStateM) ->
                   [begin
                       Msg = BasicMsgFun(MsgId),
                       ok = rabbit_msg_store:write(SeqId, MsgId, Msg, MSCStateM)
                   end || {SeqId, MsgId} <- QueueMsgIds],
                   MSCStateM
           end),
    ok = with_msg_store_client(
           ?PERSISTENT_MSG_STORE, CRef2,
           fun (MSCStateM) ->
                QueueOnlyMsgIds = [M || {_, M} <- QueueMsgIds],
                   {#{}, MSCStateN} = rabbit_msg_store:read_many(
                                          QueueOnlyMsgIds, MSCStateM),
                   MSCStateN
           end),
    passed.

%% Compaction of a v2 (.sqs) segment file is the one path that isn't
%% adequately covered by scanning hand-built files (msg_store_file_scan_v2):
%% do_compact_file_v2/3 plans the whole move up front (plan_compact_file_v2/4)
%% and then writes it (write_compact_file_v2/2), and when a moved message is
%% smaller than the hole it lands in (the common case with mixed message
%% sizes), whatever remains of that hole must be re-marked as a HOLE or
%% SMALL_HOLE record rather than left as arbitrary leftover bytes. A
%% structural scan of the compacted file, not just reads through the index,
%% is what actually proves this: reads alone would succeed even if
%% compaction never touched the surviving messages' bytes at all.
msg_store_compaction_v2(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, msg_store_compaction_v2_1, [Config]).

%% A single hand-picked size/removal distribution wouldn't tell us much
%% about shapes it doesn't happen to produce, so this runs several:
%% many small messages packed several-to-a-hole, few survivors spread
%% across large holes, most of the file left untouched with only
%% scattered removals, and the original mixed-size scattered pattern.
msg_store_compaction_v2_1(_Config) ->
    lists:foreach(fun({NumMsgs, SizeFun, KeepFun1, KeepFun2}) ->
        ok = msg_store_compaction_v2_scenario(NumMsgs, SizeFun, KeepFun1, KeepFun2)
    end, [
        {60, fun(N) -> 50 + (N * 37 rem 400) end,
             fun(N) -> N rem 7 =:= 0 end, fun(N) -> N rem 3 =:= 0 end},
        {80, fun(_) -> 20 end,
             fun(N) -> N rem 5 =:= 0 end, fun(N) -> N rem 2 =:= 0 end},
        {50, fun(N) -> 30 + (N * 91 rem 900) end,
             fun(N) -> N rem 10 =/= 0 end, fun(N) -> N rem 6 =:= 0 end},
        {50, fun(N) -> 40 + (N * 53 rem 300) end,
             fun(N) -> N rem 11 =:= 0 end, fun(N) -> N rem 2 =:= 0 end}
    ]),
    passed.

msg_store_compaction_v2_scenario(NumMsgs, SizeFun, KeepFun1, KeepFun2) ->
    restart_msg_store_empty(),
    Ref = rabbit_guid:gen(),
    %% Writes must be confirmed on disk before we measure file sizes below:
    %% with no confirm callback the message store never arms its periodic
    %% write-buffer flush (update_pending_confirms/3 short-circuits), so
    %% small messages can sit unflushed in memory indefinitely.
    {Cap, MSCState0} = msg_store_client_init_capture(?PERSISTENT_MSG_STORE, Ref),
    MsgIds = [{SeqId, msg_id_bin(N), SizeFun(N)} ||
                 {SeqId, N} <- lists:enumerate(0, lists:seq(1, NumMsgs))],
    MSCState = lists:foldl(fun({SeqId, MsgId, BodySize}, MSCStateM) ->
        ok = rabbit_msg_store:write(SeqId, MsgId, crypto:strong_rand_bytes(BodySize), MSCStateM),
        MSCStateM
    end, MSCState0, MsgIds),
    ok = on_disk_await(Cap, [{SeqId, MsgId} || {SeqId, MsgId, _} <- MsgIds]),

    StorePid = rabbit_vhost_msg_store:vhost_store_pid(?VHOST, ?PERSISTENT_MSG_STORE),
    GCPid = rabbit_msg_store:gc_pid(StorePid),
    Path = msg_store_file_path(?VHOST, ?PERSISTENT_MSG_STORE, "0.sqs"),

    %% Keep a scattered subset and remove the rest. A single burst-remove
    %% like this lands all of file 0's candidacy in one gc_candidate
    %% timer window, which maybe_gc/2's anti-thrash check then
    %% permanently defers (nothing removes from file 0 again to
    %% re-trigger reconsideration), so drive compaction directly instead
    %% of waiting on the timer, the same way msg_store_gc_stuck_mid_callback
    %% does elsewhere in this suite.
    {KeepMsgs, RemoveMsgs} = lists:partition(
        fun({_, _, N}) -> KeepFun1(N) end,
        [{SeqId, MsgId, N} || {N, {SeqId, MsgId, _BodySize}} <- lists:enumerate(MsgIds)]),
    KeepMsgIds = [MsgId || {_, MsgId, _} <- KeepMsgs],
    {ok, _} = rabbit_msg_store:remove([{SeqId, MsgId} || {SeqId, MsgId, _} <- RemoveMsgs], MSCState),
    timer:sleep(200),
    SizeBefore = filelib:file_size(Path),
    ok = rabbit_msg_store_gc:compact(GCPid, 0),
    timer:sleep(500),
    true = filelib:file_size(Path) < SizeBefore,
    ok = assert_compacted_file_intact(Path, KeepMsgIds, MSCState),

    %% Compact a second time after removing more of the survivors: this
    %% exercises scan_and_vacuum_message_file re-scanning a file that
    %% already contains HOLE/SMALL_HOLE records written by the *previous*
    %% compaction, not just holes derived directly from the original
    %% (pre-compaction) layout.
    {KeepMsgs2, RemoveMsgs2} = lists:partition(
        fun({_, _, N}) -> KeepFun2(N) end,
        [{SeqId, MsgId, N} || {N, {SeqId, MsgId}} <- lists:enumerate(
            [{SeqId, MsgId} || {SeqId, MsgId, _} <- KeepMsgs])]),
    KeepMsgIds2 = [MsgId || {_, MsgId, _} <- KeepMsgs2],
    {ok, _} = rabbit_msg_store:remove([{SeqId, MsgId} || {SeqId, MsgId, _} <- RemoveMsgs2], MSCState),
    timer:sleep(200),
    ok = rabbit_msg_store_gc:compact(GCPid, 0),
    timer:sleep(500),
    ok = assert_compacted_file_intact(Path, KeepMsgIds2, MSCState),

    %% Removing every remaining survivor empties the v2 file entirely,
    %% which goes through delete_file's v2 dispatch (file_format/2,
    %% filenum_to_name/2) and the file disappears, exactly as
    %% msg_store_v1_compat exercises for the v1 dispatch. Drive it
    %% directly via rabbit_msg_store_gc:delete/2, the same reason
    %% compaction above is driven directly rather than via the timer:
    %% this file's candidacy already went through several manually
    %% triggered rounds, and maybe_gc/2's anti-thrash check defers
    %% automatic reconsideration once that's happened.
    {ok, _} = rabbit_msg_store:remove([{SeqId, MsgId} || {SeqId, MsgId, _} <- KeepMsgs2], MSCState),
    timer:sleep(200),
    ok = rabbit_msg_store_gc:delete(GCPid, 0),
    timer:sleep(500),
    {ok, Files} = file:list_dir(filename:dirname(Path)),
    false = lists:member(filename:basename(Path), Files),

    ok = rabbit_msg_store:client_terminate(MSCState),
    ok = on_disk_stop(Cap),
    restart_msg_store_empty(),
    ok.

%% do_compact_file_v2 calls hole_bytes(0) (an empty binary; no write at
%% all beyond the moved message itself) whenever a moved message exactly
%% fills the hole it lands in. The randomised scenarios above are likely
%% to hit this by chance given enough different sizes, but nothing
%% asserts it directly, so this constructs it deterministically instead:
%% two equally-sized messages, one removed to open a hole, the other
%% pulled from the end of the file to exactly fill it, with a third,
%% untouched message positioned right after to prove there is no gap at
%% all between the moved message and whatever follows it.
msg_store_compaction_v2_exact_fit(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, msg_store_compaction_v2_exact_fit1, [Config]).

msg_store_compaction_v2_exact_fit1(_Config) ->
    restart_msg_store_empty(),
    Ref = rabbit_guid:gen(),
    {Cap, MSCState0} = msg_store_client_init_capture(?PERSISTENT_MSG_STORE, Ref),
    MsgIdKeep1 = msg_id_bin(exact_fit_keep1),
    MsgIdRemoved = msg_id_bin(exact_fit_removed),
    MsgIdKeep2 = msg_id_bin(exact_fit_keep2),
    MsgIdMoved = msg_id_bin(exact_fit_moved),
    %% MsgIdRemoved and MsgIdMoved have identical body sizes, so their
    %% on-disk record sizes match exactly: moving MsgIdMoved into the
    %% hole left by removing MsgIdRemoved leaves nothing over.
    SameSize = 50,
    Writes = [
        {1, MsgIdKeep1,   100},
        {2, MsgIdRemoved, SameSize},
        {3, MsgIdKeep2,   100},
        {4, MsgIdMoved,   SameSize}
    ],
    MSCState = lists:foldl(fun({SeqId, MsgId, BodySize}, MSCStateM) ->
        ok = rabbit_msg_store:write(SeqId, MsgId, crypto:strong_rand_bytes(BodySize), MSCStateM),
        MSCStateM
    end, MSCState0, Writes),
    ok = on_disk_await(Cap, [{SeqId, MsgId} || {SeqId, MsgId, _} <- Writes]),

    StorePid = rabbit_vhost_msg_store:vhost_store_pid(?VHOST, ?PERSISTENT_MSG_STORE),
    GCPid = rabbit_msg_store:gc_pid(StorePid),
    Path = msg_store_file_path(?VHOST, ?PERSISTENT_MSG_STORE, "0.sqs"),

    {ok, _} = rabbit_msg_store:remove([{2, MsgIdRemoved}], MSCState),
    timer:sleep(200),
    ok = rabbit_msg_store_gc:compact(GCPid, 0),
    timer:sleep(500),

    lists:foreach(fun(MsgId) ->
        {{ok, _}, _} = rabbit_msg_store:read(MsgId, MSCState)
    end, [MsgIdKeep1, MsgIdKeep2, MsgIdMoved]),
    {ok, ScannedEntries} = rabbit_msg_store:scan_file_for_valid_messages(Path),
    Entries = maps:from_list([{MsgId, {TotalSize, Offset}} ||
                                  {MsgId, TotalSize, Offset} <- ScannedEntries]),
    3 = maps:size(Entries),
    {MovedSize, MovedOffset} = maps:get(MsgIdMoved, Entries),
    {_, Keep2Offset} = maps:get(MsgIdKeep2, Entries),
    %% No gap at all between the moved message and the one right after
    %% it: proof the leftover was genuinely zero bytes, not merely a
    %% small hole that happened not to get noticed otherwise.
    Keep2Offset = MovedOffset + MovedSize,

    ok = rabbit_msg_store:client_terminate(MSCState),
    ok = on_disk_stop(Cap),
    restart_msg_store_empty(),
    passed.

%% Physically truncating a compacted file down to its planned
%% TruncateSize is a separate step, deferred by rabbit_msg_store_gc
%% until there are no readers (and droppable entirely if a delete
%% supersedes it). Until that truncation actually happens, the file
%% is still physically at its old size, and a scan (from another
%% compaction pass, a delete, or dirty recovery after a crash in that
%% window) must not trip over whatever is physically sitting between
%% TruncateSize and the old end of file. This constructs a plan whose
%% last move leaves real leftover space behind it (as opposed to
%% msg_store_compaction_v2_exact_fit, which deliberately leaves none),
%% mocks the truncate away entirely, and scans the file while it is
%% still at its original, untruncated size.
%%
%% The leftover space is deliberately *not* the last moved message's
%% own source copy: at the point this write happens the index still
%% points there (index_update_fields/3 only runs later, once every
%% write has landed and the file has been synced), so overwriting it
%% would make a concurrent read, or a crash before the index update
%% lands, see a hole where it still expects that message. Only the
%% torn remainder of whatever the move partially overwrote -- here,
%% the tail of MsgIdDead's removed record -- gets marked; the last
%% moved message's own now-redundant source copy is left as a plain,
%% untouched, still-valid (if stale) record instead, exactly like
%% every earlier moved message's source copy already is. This checks
%% that too, directly.
msg_store_compaction_v2_scannable_before_truncate(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, msg_store_compaction_v2_scannable_before_truncate1, [Config]).

msg_store_compaction_v2_scannable_before_truncate1(_Config) ->
    restart_msg_store_empty(),
    Ref = rabbit_guid:gen(),
    {Cap, MSCState0} = msg_store_client_init_capture(?PERSISTENT_MSG_STORE, Ref),
    MsgIdDead = msg_id_bin(scannable_before_truncate_dead),
    MsgIdLive = msg_id_bin(scannable_before_truncate_live),
    %% Records are 21 bytes of v2 framing plus the term_to_binary/1
    %% envelope (6 bytes) plus the body: MsgIdDead is 27 + 12 = 39
    %% bytes (offset 64-103), MsgIdLive is 27 + 2 = 29 bytes (offset
    %% 103-132). MsgIdLive is the only survivor and the only message
    %% pulled from the end of the file, so it moves into the hole
    %% MsgIdDead's removal leaves (39 bytes, starting at 64) but only
    %% fills 29 of it: TruncateSize is 64 + 29 = 93. The 10-byte
    %% remainder between TruncateSize and MsgIdLive's own original
    %% offset (103) is the torn tail of MsgIdDead's old record; the
    %% 29 bytes from 103 to the original 132-byte end of file are
    %% MsgIdLive's own source copy, left alone.
    Writes = [
        {1, MsgIdDead, 12},
        {2, MsgIdLive, 2}
    ],
    MSCState = lists:foldl(fun({SeqId, MsgId, BodySize}, MSCStateM) ->
        ok = rabbit_msg_store:write(SeqId, MsgId, crypto:strong_rand_bytes(BodySize), MSCStateM),
        MSCStateM
    end, MSCState0, Writes),
    ok = on_disk_await(Cap, [{SeqId, MsgId} || {SeqId, MsgId, _} <- Writes]),

    StorePid = rabbit_vhost_msg_store:vhost_store_pid(?VHOST, ?PERSISTENT_MSG_STORE),
    GCPid = rabbit_msg_store:gc_pid(StorePid),
    Path = msg_store_file_path(?VHOST, ?PERSISTENT_MSG_STORE, "0.sqs"),
    132 = filelib:file_size(Path),

    {ok, _} = rabbit_msg_store:remove([{1, MsgIdDead}], MSCState),
    timer:sleep(200),

    %% Prevent the physical truncation entirely, so the scan below is
    %% guaranteed to see the file exactly as compaction left it, not
    %% whatever it happens to look like in a narrow timing window.
    ok = meck:new(rabbit_msg_store_gc, [no_link, passthrough]),
    ok = meck:expect(rabbit_msg_store_gc, truncate, fun(_, _, _, _) -> ok end),
    ok = rabbit_msg_store_gc:compact(GCPid, 0),
    timer:sleep(500),
    ok = meck:unload(rabbit_msg_store_gc),

    %% Still at the original size: truncation genuinely never happened.
    132 = filelib:file_size(Path),

    {{ok, _}, _} = rabbit_msg_store:read(MsgIdLive, MSCState),
    {ok, ScannedEntries} = rabbit_msg_store:scan_file_for_valid_messages(Path),
    [{MsgIdLive, 29, 64}] = ScannedEntries,

    %% MsgIdLive's own source copy, at its original offset (103), must
    %% still be a real, untouched REC_MESSAGE record (type byte 3),
    %% not zeroed out as part of the trailing hole: the index still
    %% pointed there when this write happened.
    {ok, ReadFd} = file:open(Path, [read, binary, raw]),
    {ok, <<3:8, _/binary>>} = file:pread(ReadFd, 103, 1),
    ok = file:close(ReadFd),

    ok = rabbit_msg_store:client_terminate(MSCState),
    ok = on_disk_stop(Cap),
    restart_msg_store_empty(),
    passed.

%% write_compact_file_v2 must not leave a moved message's trailing
%% space unmarked when what immediately follows is *another* moved
%% message that hasn't been written yet: until that write happens,
%% the space is still whatever was there before compaction, so a
%% crash between the two writes would leave the second message's
%% target offset holding stale bytes, and the next scan wouldn't
%% just lose the interrupted message: it would treat the stale bytes
%% as corruption and discard everything after them too, including
%% anything that compaction never touched. The fix packs a whole run
%% of messages with no gaps between them into a single pwrite, so
%% the run lands atomically. This constructs a hole exactly big
%% enough for two messages pulled from the end of the file, with a
%% third message positioned right after the hole, and checks that
%% compaction issues exactly one pwrite for the pair rather than one
%% per message.
msg_store_compaction_v2_packed_run_atomic(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, msg_store_compaction_v2_packed_run_atomic1, [Config]).

msg_store_compaction_v2_packed_run_atomic1(_Config) ->
    restart_msg_store_empty(),
    Ref = rabbit_guid:gen(),
    {Cap, MSCState0} = msg_store_client_init_capture(?PERSISTENT_MSG_STORE, Ref),
    MsgIdKeep1 = msg_id_bin(packed_run_keep1),
    MsgIdRemoved = msg_id_bin(packed_run_removed),
    MsgIdKeep2 = msg_id_bin(packed_run_keep2),
    MsgIdMovedPenultimate = msg_id_bin(packed_run_moved_penultimate),
    MsgIdMovedLast = msg_id_bin(packed_run_moved_last),
    %% MsgIdRemoved's record is exactly as big as MsgIdMovedPenultimate's
    %% and MsgIdMovedLast's combined, so both fit into the hole its
    %% removal leaves with nothing left over between them. Record sizes
    %% are 27 bytes plus the body size given here: 21 bytes of v2
    %% REC_MESSAGE framing (type, size, msg id) plus the 6-byte
    %% external term format header term_to_binary/1 adds to wrap the
    %% body as a binary term.
    Writes = [
        {1, MsgIdKeep1,            100},
        {2, MsgIdRemoved,          56},
        {3, MsgIdKeep2,            100},
        {4, MsgIdMovedPenultimate, 9},
        {5, MsgIdMovedLast,        20}
    ],
    MSCState = lists:foldl(fun({SeqId, MsgId, BodySize}, MSCStateM) ->
        ok = rabbit_msg_store:write(SeqId, MsgId, crypto:strong_rand_bytes(BodySize), MSCStateM),
        MSCStateM
    end, MSCState0, Writes),
    ok = on_disk_await(Cap, [{SeqId, MsgId} || {SeqId, MsgId, _} <- Writes]),

    StorePid = rabbit_vhost_msg_store:vhost_store_pid(?VHOST, ?PERSISTENT_MSG_STORE),
    GCPid = rabbit_msg_store:gc_pid(StorePid),
    Path = msg_store_file_path(?VHOST, ?PERSISTENT_MSG_STORE, "0.sqs"),

    {ok, _} = rabbit_msg_store:remove([{2, MsgIdRemoved}], MSCState),
    timer:sleep(200),

    %% Capture every file:pwrite/3 call made anywhere while compaction
    %% runs: the GC process does the actual writing, not this one, so
    %% this has to be a real (if narrowly scoped and brief) mock,
    %% rather than something local to this process.
    TestPid = self(),
    ok = meck:new(file, [unstick, passthrough, no_link]),
    ok = meck:expect(file, pwrite, fun(Fd, Offset, Data) ->
        TestPid ! {pwrite, Offset, iolist_size(Data)},
        meck:passthrough([Fd, Offset, Data])
    end),
    ok = rabbit_msg_store_gc:compact(GCPid, 0),
    timer:sleep(500),
    ok = meck:unload(file),
    PwriteCalls = flush_pwrite_calls(),

    %% One pwrite for both moved messages together, at the offset the
    %% first of them (the one pulled from furthest into the file)
    %% lands at, sized for both records combined with no hole between
    %% them: not one pwrite per message, which would also produce a
    %% call at the second message's own offset and size (238, 36).
    true = lists:member({191, 83}, PwriteCalls),
    false = lists:member({238, 36}, PwriteCalls),

    lists:foreach(fun(MsgId) ->
        {{ok, _}, _} = rabbit_msg_store:read(MsgId, MSCState)
    end, [MsgIdKeep1, MsgIdKeep2, MsgIdMovedPenultimate, MsgIdMovedLast]),
    {ok, ScannedEntries} = rabbit_msg_store:scan_file_for_valid_messages(Path),
    Entries = maps:from_list([{MsgId, {TotalSize, Offset}} ||
                                  {MsgId, TotalSize, Offset} <- ScannedEntries]),
    4 = maps:size(Entries),
    {_, 64}  = maps:get(MsgIdKeep1, Entries),
    {_, 191} = maps:get(MsgIdMovedLast, Entries),
    {_, 238} = maps:get(MsgIdMovedPenultimate, Entries),
    {_, 274} = maps:get(MsgIdKeep2, Entries),
    401 = filelib:file_size(Path),

    ok = rabbit_msg_store:client_terminate(MSCState),
    ok = on_disk_stop(Cap),
    restart_msg_store_empty(),
    passed.

%% msg_store_compaction_v2_packed_run_atomic's packed run exactly
%% fills the hole it lands in, so its group's trailing hole is always
%% zero-sized (no bytes actually written for it). This constructs a
%% packed run that only partially fills its hole, so the group's one
%% pwrite must also carry a real, non-empty trailing hole record.
msg_store_compaction_v2_packed_run_with_trailing_hole(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, msg_store_compaction_v2_packed_run_with_trailing_hole1, [Config]).

msg_store_compaction_v2_packed_run_with_trailing_hole1(_Config) ->
    restart_msg_store_empty(),
    Ref = rabbit_guid:gen(),
    {Cap, MSCState0} = msg_store_client_init_capture(?PERSISTENT_MSG_STORE, Ref),
    MsgIdKeep1 = msg_id_bin(packed_hole_keep1),
    MsgIdRemoved = msg_id_bin(packed_hole_removed),
    MsgIdKeep2 = msg_id_bin(packed_hole_keep2),
    MsgIdMovedPenultimate = msg_id_bin(packed_hole_moved_penultimate),
    MsgIdMovedLast = msg_id_bin(packed_hole_moved_last),
    %% MsgIdRemoved's record (27 + 73 = 100 bytes) is bigger than
    %% MsgIdMovedPenultimate's and MsgIdMovedLast's combined (32 + 37 =
    %% 69 bytes), so both fit into the hole its removal leaves with
    %% 31 bytes still left over afterwards.
    Writes = [
        {1, MsgIdKeep1,            100},
        {2, MsgIdRemoved,          73},
        {3, MsgIdKeep2,            100},
        {4, MsgIdMovedPenultimate, 5},
        {5, MsgIdMovedLast,        10}
    ],
    MSCState = lists:foldl(fun({SeqId, MsgId, BodySize}, MSCStateM) ->
        ok = rabbit_msg_store:write(SeqId, MsgId, crypto:strong_rand_bytes(BodySize), MSCStateM),
        MSCStateM
    end, MSCState0, Writes),
    ok = on_disk_await(Cap, [{SeqId, MsgId} || {SeqId, MsgId, _} <- Writes]),

    StorePid = rabbit_vhost_msg_store:vhost_store_pid(?VHOST, ?PERSISTENT_MSG_STORE),
    GCPid = rabbit_msg_store:gc_pid(StorePid),
    Path = msg_store_file_path(?VHOST, ?PERSISTENT_MSG_STORE, "0.sqs"),

    {ok, _} = rabbit_msg_store:remove([{2, MsgIdRemoved}], MSCState),
    timer:sleep(200),

    TestPid = self(),
    ok = meck:new(file, [unstick, passthrough, no_link]),
    ok = meck:expect(file, pwrite, fun(Fd, Offset, Data) ->
        TestPid ! {pwrite, Offset, iolist_size(Data)},
        meck:passthrough([Fd, Offset, Data])
    end),
    ok = rabbit_msg_store_gc:compact(GCPid, 0),
    timer:sleep(500),
    ok = meck:unload(file),
    PwriteCalls = flush_pwrite_calls(),

    %% One pwrite covering both moved messages plus the 31-byte hole
    %% left over after them (37 + 32 + 31 = 100, the exact size of the
    %% hole MsgIdRemoved's removal left): not one pwrite per message,
    %% each with its own trailing hole (the second of which, sized
    %% against the 31-byte leftover alone, would be {228, 32 + 31}).
    true = lists:member({191, 100}, PwriteCalls),
    false = lists:member({228, 63}, PwriteCalls),

    lists:foreach(fun(MsgId) ->
        {{ok, _}, _} = rabbit_msg_store:read(MsgId, MSCState)
    end, [MsgIdKeep1, MsgIdKeep2, MsgIdMovedPenultimate, MsgIdMovedLast]),
    {ok, ScannedEntries} = rabbit_msg_store:scan_file_for_valid_messages(Path),
    Entries = maps:from_list([{MsgId, {TotalSize, Offset}} ||
                                  {MsgId, TotalSize, Offset} <- ScannedEntries]),
    4 = maps:size(Entries),
    {_, 64}  = maps:get(MsgIdKeep1, Entries),
    {_, 191} = maps:get(MsgIdMovedLast, Entries),
    {_, 228} = maps:get(MsgIdMovedPenultimate, Entries),
    {_, 291} = maps:get(MsgIdKeep2, Entries),
    418 = filelib:file_size(Path),

    ok = rabbit_msg_store:client_terminate(MSCState),
    ok = on_disk_stop(Cap),
    restart_msg_store_empty(),
    passed.

flush_pwrite_calls() ->
    receive
        {pwrite, Offset, Size} -> [{Offset, Size} | flush_pwrite_calls()]
    after 0 ->
        []
    end.

%% A raw scan may also find messages that compaction left in place
%% without touching (complete, still-valid records that are simply no
%% longer referenced by the index): that's expected, so we only check
%% that every message we kept is among the ones found, not that
%% nothing else is. A real message store scan reconciles against the
%% index and would remove such stale entries.
assert_compacted_file_intact(Path, KeepMsgIds, MSCState) ->
    lists:foreach(fun(MsgId) ->
        {{ok, _}, _} = rabbit_msg_store:read(MsgId, MSCState)
    end, KeepMsgIds),
    {ok, ScannedEntries} = rabbit_msg_store:scan_file_for_valid_messages(Path),
    ScannedMsgIds = sets:from_list([MsgId || {MsgId, _TotalSize, _Offset} <- ScannedEntries], [{version, 2}]),
    true = sets:is_subset(sets:from_list(KeepMsgIds, [{version, 2}]), ScannedMsgIds),
    ok.

msg_store_file_path(VHost, MsgStore, FileName) ->
    filename:join([rabbit_vhost:msg_store_dir_path(VHost), atom_to_list(MsgStore), FileName]).

%% None of the other msg_store_* tests ever produce or touch a v1 (.rdq)
%% file: restart_msg_store_empty/0 always boots a store with no files at
%% all, and a store with no v1 files writes v2 from the very first byte.
%% msg_store_file_scan only covers the standalone scanner against
%% hand-built files, never the live client read path, the v1-to-v2
%% rollover on boot, or GC dispatching to the v1 code paths. This test
%% hand-places a legacy v1 segment file to cover all of that.
msg_store_v1_compat(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, msg_store_v1_compat1, [Config]).

msg_store_v1_compat1(_Config) ->
    ok = rabbit_variable_queue:stop_msg_store(?VHOST),
    Dir = filename:join([rabbit_vhost:msg_store_dir_path(?VHOST), atom_to_list(?PERSISTENT_MSG_STORE)]),
    ok = rabbit_file:recursive_delete([Dir]),
    ok = filelib:ensure_dir(filename:join(Dir, "nothing")),

    %% Hand-write a legacy v1 segment file (0.rdq) with two messages of
    %% deliberately different sizes (so that removing the larger one
    %% leaves a hole the smaller one can be moved into during
    %% compaction), with no clean.dot present, so the next boot must go
    %% through dirty recovery and discover it purely from what's on disk
    %% -- mirroring a store that predates the v2 format.
    MsgId1 = msg_id_bin(v1_legacy_1),
    LegacyMsg1 = {legacy, "first-v1-message", crypto:strong_rand_bytes(500)},
    MsgId2 = msg_id_bin(v1_legacy_2),
    LegacyMsg2 = {legacy, "second-v1-message"},
    V1Record = fun(MsgId, Msg) ->
        Bin = term_to_binary(Msg),
        Size = byte_size(MsgId) + byte_size(Bin),
        [<<Size:64>>, MsgId, Bin, <<255>>]
    end,
    ok = file:write_file(filename:join(Dir, "0.rdq"),
        [V1Record(MsgId1, LegacyMsg1), V1Record(MsgId2, LegacyMsg2)]),

    %% No matching client refs => dirty recovery. The ref-count generator
    %% mimics the shape rabbit_classic_queue_index_v2:queue_index_walker/1
    %% actually reports: a list of msg ids per batch.
    Ref = rabbit_guid:gen(),
    Gen = fun
        ([])  -> finished;
        (Ids) -> {Ids, []}
    end,
    ok = rabbit_variable_queue:start_msg_store(?VHOST, [Ref], {Gen, [MsgId1, MsgId2]}),
    false = rabbit_vhost_msg_store:successfully_recovered_state(?VHOST, ?PERSISTENT_MSG_STORE),

    {ok, Files1} = file:list_dir(Dir),
    true = lists:member("0.rdq", Files1),
    true = lists:any(fun(F) -> filename:extension(F) =:= ".sqs" end, Files1),

    %% Both legacy messages must be readable through the live client API
    %% (reader_open/reader_pread_parse_v1), not just the standalone scanner.
    MSCState0 = msg_store_client_init(?PERSISTENT_MSG_STORE, Ref),
    {{ok, LegacyMsg1}, MSCState1} = rabbit_msg_store:read(MsgId1, MSCState0),
    {{ok, LegacyMsg2}, MSCState2} = rabbit_msg_store:read(MsgId2, MSCState1),

    %% A newly written message must roll into a fresh v2 file: v2 records
    %% must never be appended into the v1 file (open_current_file/5).
    NewMsgId = msg_id_bin(new_v2_msg),
    NewMsg = {payload, <<"a new v2 message">>},
    ok = rabbit_msg_store:write(make_ref(), NewMsgId, NewMsg, MSCState2),
    {{ok, NewMsg}, MSCState3} = rabbit_msg_store:read(NewMsgId, MSCState2),
    ok = rabbit_msg_store:client_terminate(MSCState3),

    %% Restart cleanly: last_v1_file is now persisted in clean.dot rather
    %% than recomputed from disk, and everything must still read correctly.
    ok = rabbit_variable_queue:stop_msg_store(?VHOST),
    ok = rabbit_variable_queue:start_msg_store(?VHOST, [Ref], {fun([]) -> finished end, []}),
    true = rabbit_vhost_msg_store:successfully_recovered_state(?VHOST, ?PERSISTENT_MSG_STORE),
    MSCState4 = msg_store_client_init(?PERSISTENT_MSG_STORE, Ref),
    {{ok, LegacyMsg1}, MSCState5} = rabbit_msg_store:read(MsgId1, MSCState4),
    {{ok, LegacyMsg2}, MSCState6} = rabbit_msg_store:read(MsgId2, MSCState5),
    {{ok, NewMsg}, MSCState7} = rabbit_msg_store:read(NewMsgId, MSCState6),

    %% Removing the larger legacy message leaves the v1 file with live
    %% data (the smaller one), so it goes through compact_file and
    %% scan_and_vacuum_message_file dispatching to their v1 code paths,
    %% not straight to deletion.
    StorePid = rabbit_vhost_msg_store:vhost_store_pid(?VHOST, ?PERSISTENT_MSG_STORE),
    GCPid = rabbit_msg_store:gc_pid(StorePid),
    Path = msg_store_file_path(?VHOST, ?PERSISTENT_MSG_STORE, "0.rdq"),
    SizeBeforeCompact = filelib:file_size(Path),
    %% count_msg_refs registers refs via ets:update_counter/4, whose
    %% Default already carries ref_count=1; on a fresh key the Increment
    %% is added on top of that, so a message recovered this way starts
    %% at ref_count=2 and needs two removes to be fully dereferenced.
    {ok, _} = rabbit_msg_store:remove([{make_ref(), MsgId1}], MSCState7),
    {ok, _} = rabbit_msg_store:remove([{make_ref(), MsgId1}], MSCState7),
    timer:sleep(200),
    ok = rabbit_msg_store_gc:compact(GCPid, 0),
    timer:sleep(500),
    true = filelib:is_regular(Path),
    true = filelib:file_size(Path) < SizeBeforeCompact,
    %% Unlike v2, v1 compaction zero-fills every removed message's bytes
    %% up front (blank_holes_in_file_v1), so a raw scan of the compacted
    %% file must find exactly the survivor, not just have it be readable
    %% through the index: this is what actually proves do_compact_file_v1
    %% moved MsgId2's bytes rather than leaving them untouched by luck.
    {ok, [{MsgId2, _, _}]} = rabbit_msg_store:scan_file_for_valid_messages(Path),
    {{ok, LegacyMsg2}, MSCState8} = rabbit_msg_store:read(MsgId2, MSCState7),

    %% Removing the last legacy message empties the v1 file, which goes
    %% through delete_file's v1 dispatch and disappears entirely.
    {ok, _} = rabbit_msg_store:remove([{make_ref(), MsgId2}], MSCState8),
    {ok, _} = rabbit_msg_store:remove([{make_ref(), MsgId2}], MSCState8),
    timer:sleep(500),
    {ok, Files2} = file:list_dir(Dir),
    false = lists:member("0.rdq", Files2),

    ok = rabbit_msg_store:client_terminate(MSCState8),
    restart_msg_store_empty(),
    passed.

%% delete_file_if_empty/2 always exempts current_file, and build_index's
%% dirty-recovery fold runs it over every file while the about-to-be-
%% rolled-away-from v1 file is still current, so it's skipped there
%% too. If that v1 file holds no valid messages, nothing revisits it
%% once open_current_file/5 decides to roll to a fresh v2 file instead
%% -- unless init/1 explicitly checks it right after making that
%% decision, which is what this covers.
msg_store_v1_current_file_emptied_before_crash(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, msg_store_v1_current_file_emptied_before_crash1, [Config]).

msg_store_v1_current_file_emptied_before_crash1(_Config) ->
    ok = rabbit_variable_queue:stop_msg_store(?VHOST),
    Dir = filename:join([rabbit_vhost:msg_store_dir_path(?VHOST), atom_to_list(?PERSISTENT_MSG_STORE)]),
    ok = rabbit_file:recursive_delete([Dir]),
    ok = filelib:ensure_dir(filename:join(Dir, "nothing")),

    %% Hand-write a legacy v1 segment file (0.rdq) holding one message,
    %% with no clean.dot present, so the next boot must go through
    %% dirty recovery and discover it purely from what's on disk --
    %% mirroring a store that predates the v2 format.
    MsgId = msg_id_bin(v1_current_file_emptied),
    LegacyMsg = {legacy, "already fully consumed before the crash"},
    Bin = term_to_binary(LegacyMsg),
    Size = byte_size(MsgId) + byte_size(Bin),
    ok = file:write_file(filename:join(Dir, "0.rdq"),
        [<<Size:64>>, MsgId, Bin, <<255>>]),

    %% The ref-count generator reports no messages at all needed by any
    %% queue: MsgId was already fully acked by every queue that had it
    %% before the crash, so this file genuinely holds no live messages
    %% by the time dirty recovery runs, even though it's still the
    %% highest-numbered (and so, provisionally, "current") file on disk.
    Ref = rabbit_guid:gen(),
    ok = rabbit_variable_queue:start_msg_store(?VHOST, [Ref], {fun([]) -> finished end, []}),
    false = rabbit_vhost_msg_store:successfully_recovered_state(?VHOST, ?PERSISTENT_MSG_STORE),
    timer:sleep(500),

    %% The empty v1 file must be gone, not left behind forever: nothing
    %% after boot ever points into it, and nothing after boot will ever
    %% reconsider deleting it since it's no longer current.
    {ok, Files} = file:list_dir(Dir),
    false = lists:member("0.rdq", Files),
    true = lists:any(fun(F) -> filename:extension(F) =:= ".sqs" end, Files),

    %% The store is still fully usable: a new message goes into a fresh
    %% v2 file and reads back correctly.
    MSCState0 = msg_store_client_init(?PERSISTENT_MSG_STORE, Ref),
    NewMsgId = msg_id_bin(v1_current_file_emptied_new),
    NewMsg = {payload, <<"written after the empty v1 file was reclaimed">>},
    ok = rabbit_msg_store:write(make_ref(), NewMsgId, NewMsg, MSCState0),
    {{ok, NewMsg}, MSCState1} = rabbit_msg_store:read(NewMsgId, MSCState0),

    ok = rabbit_msg_store:client_terminate(MSCState1),
    restart_msg_store_empty(),
    passed.

%% open_current_file/5 has three branches: roll a v1 current file to a
%% fresh v2 one (covered by msg_store_v1_compat), recover a v2 current
%% file that does hold valid messages (covered by every test that
%% restarts a store with data, e.g. msg_store_v1_compat's second
%% restart), and reopen a v2 current file that recovery determined
%% holds no valid messages, truncating away anything past its header.
%% That last one is reachable in an ordinary way: a crash partway
%% through appending a message leaves exactly this shape (a header,
%% then a torn write that scanning treats as a truncated last write,
%% not as a real message). This covers it directly.
msg_store_recovers_torn_current_file(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, msg_store_recovers_torn_current_file1, [Config]).

msg_store_recovers_torn_current_file1(_Config) ->
    restart_msg_store_empty(),
    ok = rabbit_variable_queue:stop_msg_store(?VHOST),
    Path = msg_store_file_path(?VHOST, ?PERSISTENT_MSG_STORE, "0.sqs"),
    Dir = filename:join([rabbit_vhost:msg_store_dir_path(?VHOST), atom_to_list(?PERSISTENT_MSG_STORE)]),

    %% A fresh store's current file is 0.sqs with nothing but its
    %% 64-byte header. Simulate a crash partway through appending a
    %% message: a header claiming far more data than actually follows,
    %% which scanning treats as a torn last write, not as real data.
    {ok, Fd} = file:open(Path, [read, write, binary, raw]),
    {ok, _} = file:position(Fd, eof),
    ok = file:write(Fd, <<3:8, 999999:32, "torn append">>),
    ok = file:close(Fd),
    ok = file:delete(filename:join(Dir, "clean.dot")),

    %% No matching client refs => dirty recovery, forcing a full rescan
    %% from disk rather than trusting anything persisted.
    Ref = rabbit_guid:gen(),
    ok = rabbit_variable_queue:start_msg_store(?VHOST, [Ref], {fun([]) -> finished end, []}),
    false = rabbit_vhost_msg_store:successfully_recovered_state(?VHOST, ?PERSISTENT_MSG_STORE),

    %% The stale tail must be gone immediately: truncation happens as
    %% part of startup (open_current_file/5), not lazily on first write.
    64 = filelib:file_size(Path),

    %% A new message must land right after the header, not after the
    %% discarded tail, and must read back correctly. Writes must be
    %% confirmed on disk before the raw scan below, or the message may
    %% still be sitting unflushed in memory (see msg_store_compaction_v2).
    {Cap, MSCState0} = msg_store_client_init_capture(?PERSISTENT_MSG_STORE, Ref),
    MsgId = msg_id_bin(after_torn_recovery),
    Msg = {payload, <<"written after recovering a torn current file">>},
    SeqId = make_ref(),
    ok = rabbit_msg_store:write(SeqId, MsgId, Msg, MSCState0),
    ok = on_disk_await(Cap, [{SeqId, MsgId}]),
    {{ok, Msg}, MSCState1} = rabbit_msg_store:read(MsgId, MSCState0),
    {ok, [{MsgId, _TotalSize, 64}]} = rabbit_msg_store:scan_file_for_valid_messages(Path),

    ok = rabbit_msg_store:client_terminate(MSCState1),
    ok = on_disk_stop(Cap),
    restart_msg_store_empty(),
    passed.

%% Unlike a torn last write (msg_store_recovers_torn_current_file,
%% tolerated silently, no data lost), a genuine hard corruption error
%% must not crash the whole store during dirty recovery either: we
%% cannot safely resync past it (that is exactly the byte-guessing v2
%% was designed to avoid), so scan_file_recovering_corruption truncates
%% the file at the point of the error instead, keeping everything
%% scanned successfully before it.
msg_store_recovers_from_corrupted_file(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, msg_store_recovers_from_corrupted_file1, [Config]).

msg_store_recovers_from_corrupted_file1(_Config) ->
    restart_msg_store_empty(),
    Ref0 = rabbit_guid:gen(),
    {Cap0, MSCState0} = msg_store_client_init_capture(?PERSISTENT_MSG_STORE, Ref0),
    MsgId1 = msg_id_bin(corrupt_test_1),
    MsgId2 = msg_id_bin(corrupt_test_2),
    Msg1 = {payload, <<"kept before the corruption, message 1">>},
    Msg2 = {payload, <<"kept before the corruption, message 2">>},
    ok = rabbit_msg_store:write(1, MsgId1, Msg1, MSCState0),
    ok = rabbit_msg_store:write(2, MsgId2, Msg2, MSCState0),
    ok = on_disk_await(Cap0, [{1, MsgId1}, {2, MsgId2}]),
    ok = rabbit_msg_store:client_terminate(MSCState0),
    ok = on_disk_stop(Cap0),

    ok = rabbit_variable_queue:stop_msg_store(?VHOST),
    Path = msg_store_file_path(?VHOST, ?PERSISTENT_MSG_STORE, "0.sqs"),
    Dir = filename:join([rabbit_vhost:msg_store_dir_path(?VHOST), atom_to_list(?PERSISTENT_MSG_STORE)]),
    GoodSize = filelib:file_size(Path),

    %% Append a record with an unrecognised type byte: definite
    %% corruption, not a torn write (a torn write cannot fully flush a
    %% valid type byte with a bogus value; see the comment above
    %% scan_v2_data's catch-all clause).
    {ok, Fd} = file:open(Path, [read, write, binary, raw]),
    {ok, _} = file:position(Fd, eof),
    ok = file:write(Fd, <<250:8, "this is not a valid v2 record">>),
    ok = file:close(Fd),
    ok = file:delete(filename:join(Dir, "clean.dot")),

    %% No matching client refs => dirty recovery, forcing a full rescan
    %% from disk rather than trusting anything persisted. The ref-count
    %% generator must declare MsgId1/MsgId2 up front (mirroring what
    %% rabbit_classic_queue_index_v2:queue_index_walker/1 reports for a
    %% real queue) so build_index_worker's index_lookup has a
    %% file = undefined entry to resolve them against; otherwise
    %% neither one is ever considered valid, corruption or not.
    Ref = rabbit_guid:gen(),
    Gen = fun
        ([])  -> finished;
        (Ids) -> {Ids, []}
    end,
    ok = rabbit_variable_queue:start_msg_store(?VHOST, [Ref], {Gen, [MsgId1, MsgId2]}),
    false = rabbit_vhost_msg_store:successfully_recovered_state(?VHOST, ?PERSISTENT_MSG_STORE),

    %% The corrupted tail is gone: the file is truncated back to
    %% exactly where the good data ended, not dropped entirely.
    GoodSize = filelib:file_size(Path),

    {Cap, MSCState1} = msg_store_client_init_capture(?PERSISTENT_MSG_STORE, Ref),
    {{ok, Msg1}, MSCState2} = rabbit_msg_store:read(MsgId1, MSCState1),
    {{ok, Msg2}, MSCState3} = rabbit_msg_store:read(MsgId2, MSCState2),

    %% A new message must land right after the truncation point, not
    %% after the discarded corruption, and read back correctly.
    MsgId3 = msg_id_bin(corrupt_test_3),
    Msg3 = {payload, <<"written after truncating the corruption">>},
    ok = rabbit_msg_store:write(3, MsgId3, Msg3, MSCState3),
    ok = on_disk_await(Cap, [{3, MsgId3}]),
    {{ok, Msg3}, MSCState4} = rabbit_msg_store:read(MsgId3, MSCState3),
    {ok, ScannedEntries} = rabbit_msg_store:scan_file_for_valid_messages(Path),
    ScannedOffsets = maps:from_list([{MsgId, Offset} || {MsgId, _TotalSize, Offset} <- ScannedEntries]),
    3 = maps:size(ScannedOffsets),
    GoodSize = maps:get(MsgId3, ScannedOffsets),

    ok = rabbit_msg_store:client_terminate(MSCState4),
    ok = on_disk_stop(Cap),
    restart_msg_store_empty(),
    passed.

%% scan_v2_file_for_valid_messages/2 did not close its fd on the
%% exception path, only on success. scan_file_recovering_corruption/2
%% retries a failed scan of the same file in a loop, so every retry
%% before the one that finally succeeds leaked one fd. This forces
%% exactly one such retry (the same corruption as
%% msg_store_recovers_from_corrupted_file) and checks that every
%% file:open/2 call against the target file is matched by a
%% file:close/1 call once the store has fully stopped (which also
%% closes the current file's own long-lived write handle, so the
%% counts only balance if nothing else was left open).
msg_store_recovers_from_corrupted_file_no_fd_leak(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, msg_store_recovers_from_corrupted_file_no_fd_leak1, [Config]).

msg_store_recovers_from_corrupted_file_no_fd_leak1(_Config) ->
    restart_msg_store_empty(),
    Ref0 = rabbit_guid:gen(),
    {Cap0, MSCState0} = msg_store_client_init_capture(?PERSISTENT_MSG_STORE, Ref0),
    MsgId1 = msg_id_bin(corrupt_no_leak_1),
    Msg1 = {payload, <<"kept before the corruption">>},
    ok = rabbit_msg_store:write(1, MsgId1, Msg1, MSCState0),
    ok = on_disk_await(Cap0, [{1, MsgId1}]),
    ok = rabbit_msg_store:client_terminate(MSCState0),
    ok = on_disk_stop(Cap0),

    ok = rabbit_variable_queue:stop_msg_store(?VHOST),
    Path = msg_store_file_path(?VHOST, ?PERSISTENT_MSG_STORE, "0.sqs"),
    Dir = filename:join([rabbit_vhost:msg_store_dir_path(?VHOST), atom_to_list(?PERSISTENT_MSG_STORE)]),

    %% Same corruption as msg_store_recovers_from_corrupted_file: an
    %% unrecognised type byte, definite corruption rather than a torn
    %% write, forcing scan_file_recovering_corruption/2 to truncate
    %% and retry exactly once.
    {ok, CorruptFd} = file:open(Path, [read, write, binary, raw]),
    {ok, _} = file:position(CorruptFd, eof),
    ok = file:write(CorruptFd, <<250:8, "this is not a valid v2 record">>),
    ok = file:close(CorruptFd),
    ok = file:delete(filename:join(Dir, "clean.dot")),

    Counters = counters:new(2, []),
    OpenFds = ets:new(open_fds, [public, set]),
    ok = meck:new(file, [unstick, passthrough, no_link]),
    ok = meck:expect(file, open, fun(OpenPath, Modes) ->
        Result = meck:passthrough([OpenPath, Modes]),
        case {OpenPath, Result} of
            {Path, {ok, Fd}} ->
                counters:add(Counters, 1, 1),
                ets:insert(OpenFds, {Fd}),
                Result;
            _ ->
                Result
        end
    end),
    ok = meck:expect(file, close, fun(Fd) ->
        case ets:take(OpenFds, Fd) of
            [_] -> counters:add(Counters, 2, 1);
            []  -> ok
        end,
        meck:passthrough([Fd])
    end),

    %% Unloaded in the `after` clause below regardless of outcome: a
    %% failure while file is mocked must not leave the mock (and the
    %% OpenFds table it depends on) installed for every later test in
    %% the group.
    try
        Ref = rabbit_guid:gen(),
        Gen = fun
            ([])  -> finished;
            (Ids) -> {Ids, []}
        end,
        ok = rabbit_variable_queue:start_msg_store(?VHOST, [Ref], {Gen, [MsgId1]}),
        false = rabbit_vhost_msg_store:successfully_recovered_state(?VHOST, ?PERSISTENT_MSG_STORE),

        MSCState1 = msg_store_client_init(?PERSISTENT_MSG_STORE, Ref),
        {{ok, Msg1}, MSCState2} = rabbit_msg_store:read(MsgId1, MSCState1),
        ok = rabbit_msg_store:client_terminate(MSCState2),

        %% Stop the store so the current file's own long-lived write
        %% handle (opened separately by open_current_file/5, and only
        %% ever closed on shutdown) is closed too, leaving only
        %% genuinely leaked handles open.
        ok = rabbit_variable_queue:stop_msg_store(?VHOST)
    after
        ok = meck:unload(file)
    end,

    OpenCount = counters:get(Counters, 1),
    CloseCount = counters:get(Counters, 2),
    %% At least the failed attempt and the successful retry: proves
    %% the corruption genuinely forced more than one open of this file.
    true = OpenCount >= 2,
    OpenCount = CloseCount,

    %% The store was already stopped above; restart_msg_store_empty/0
    %% would stop it a second time and fail, so start it back up directly.
    ok = rabbit_variable_queue:start_msg_store(?VHOST,
           undefined, {fun (ok) -> finished end, ok}),
    passed.

%% reader_pread_parse_v1/2 and reader_pread_parse_v2/2 attach the file
%% number to a corrupted record's read error, but used to leave out the
%% message's own id -- even though the id is already parsed out of the
%% record before the corruption (a bad body, or a size that doesn't add
%% up) is even detected. This confirms the id is included now, so a
%% corrupted record can be located directly instead of requiring a full
%% scan of the file to find which message is at fault.
msg_store_read_error_includes_msg_id(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, msg_store_read_error_includes_msg_id1, [Config]).

msg_store_read_error_includes_msg_id1(_Config) ->
    restart_msg_store_empty(),
    Ref = rabbit_guid:gen(),
    {Cap, MSCState0} = msg_store_client_init_capture(?PERSISTENT_MSG_STORE, Ref),
    MsgId = msg_id_bin(read_error_includes_msg_id),
    Msg = {payload, <<"corrupted on purpose after being written">>},
    ok = rabbit_msg_store:write(1, MsgId, Msg, MSCState0),
    ok = on_disk_await(Cap, [{1, MsgId}]),
    ok = rabbit_msg_store:client_terminate(MSCState0),
    ok = on_disk_stop(Cap),

    %% Restart cleanly (matching client refs) so the freshly-recreated
    %% cur_file_cache_ets can't serve the read below from memory -- the
    %% corruption must actually be read back from disk to be caught.
    ok = rabbit_variable_queue:stop_msg_store(?VHOST),
    ok = rabbit_variable_queue:start_msg_store(?VHOST, [Ref], {fun([]) -> finished end, []}),
    true = rabbit_vhost_msg_store:successfully_recovered_state(?VHOST, ?PERSISTENT_MSG_STORE),

    %% Corrupt just the message body in place, leaving the record's own
    %% type/size/msg_id header (the first 21 bytes of the record, right
    %% after the 64-byte file header) untouched, so the parser reads out
    %% MsgId before it gets to -- and fails to decode -- the body.
    Path = msg_store_file_path(?VHOST, ?PERSISTENT_MSG_STORE, "0.sqs"),
    BodySize = byte_size(term_to_binary(Msg)),
    Garbage = binary:copy(<<0>>, BodySize),
    {ok, Fd} = file:open(Path, [read, write, binary, raw]),
    ok = file:pwrite(Fd, 64 + 21, Garbage),
    ok = file:close(Fd),

    MSCState1 = msg_store_client_init(?PERSISTENT_MSG_STORE, Ref),
    ReadMsgId = try
        rabbit_msg_store:read(MsgId, MSCState1),
        unexpected_success
    catch
        error:{rabbit_msg_store_v2_read, invalid_message_body, _File, Id} ->
            Id
    end,
    MsgId = ReadMsgId,

    ok = rabbit_msg_store:client_terminate(MSCState1),
    restart_msg_store_empty(),
    passed.

%% msg_store_recovers_from_corrupted_file above only ever corrupts the
%% current (last) segment file, which gets a second, independent
%% truncation pass via open_current_file/5 (writer_recover/3) on top
%% of scan_file_recovering_corruption's own truncate. A file that has
%% already been rolled past -- no longer current -- relies solely on
%% scan_file_recovering_corruption: this covers that path by writing
%% enough to roll over to a second file, then corrupting the first
%% one, which by then is no longer being written to.
msg_store_recovers_from_corrupted_non_current_file(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, msg_store_recovers_from_corrupted_non_current_file1, [Config]).

msg_store_recovers_from_corrupted_non_current_file1(_Config) ->
    {ok, DefaultFileSizeLimit} = application:get_env(rabbit, msg_store_file_size_limit),
    %% A tiny limit forces a rollover after a couple of small messages
    %% instead of needing megabytes of writes to exercise the same
    %% path. Restored in the `after` clause below regardless of
    %% outcome, so a failure here can't leak this override into every
    %% later test in the group.
    ok = application:set_env(rabbit, msg_store_file_size_limit, 200),
    try
        msg_store_recovers_from_corrupted_non_current_file2()
    after
        ok = application:set_env(rabbit, msg_store_file_size_limit, DefaultFileSizeLimit)
    end.

msg_store_recovers_from_corrupted_non_current_file2() ->
    restart_msg_store_empty(),
    Ref0 = rabbit_guid:gen(),
    {Cap0, MSCState0} = msg_store_client_init_capture(?PERSISTENT_MSG_STORE, Ref0),
    MsgId1 = msg_id_bin(corrupt_non_current_1),
    MsgId2 = msg_id_bin(corrupt_non_current_2),
    MsgId3 = msg_id_bin(corrupt_non_current_3),
    Msg1 = {payload, <<"file 0, kept before the corruption, message 1">>},
    Msg2 = {payload, <<"file 0, kept before the corruption, message 2">>},
    Msg3 = {payload, <<"file 1, the current file, untouched by any of this">>},
    ok = rabbit_msg_store:write(1, MsgId1, Msg1, MSCState0),
    ok = rabbit_msg_store:write(2, MsgId2, Msg2, MSCState0),
    ok = on_disk_await(Cap0, [{1, MsgId1}, {2, MsgId2}]),
    %% MsgId2's write pushes file 0's offset past the 200-byte limit
    %% (each of these two records is 83 bytes; 64 + 83 + 83 = 230), so
    %% file 0 is already closed and file 1 already the current file by
    %% the time this third message is written into it.
    ok = rabbit_msg_store:write(3, MsgId3, Msg3, MSCState0),
    ok = on_disk_await(Cap0, [{3, MsgId3}]),
    ok = rabbit_msg_store:client_terminate(MSCState0),
    ok = on_disk_stop(Cap0),

    ok = rabbit_variable_queue:stop_msg_store(?VHOST),
    Path0 = msg_store_file_path(?VHOST, ?PERSISTENT_MSG_STORE, "0.sqs"),
    Path1 = msg_store_file_path(?VHOST, ?PERSISTENT_MSG_STORE, "1.sqs"),
    Dir = filename:join([rabbit_vhost:msg_store_dir_path(?VHOST), atom_to_list(?PERSISTENT_MSG_STORE)]),
    true = filelib:is_regular(Path0),
    true = filelib:is_regular(Path1),
    GoodSize0 = filelib:file_size(Path0),
    GoodSize1 = filelib:file_size(Path1),

    %% Corrupt file 0 only, exactly as msg_store_recovers_from_corrupted_file
    %% does for the current file: an unrecognised type byte, definite
    %% corruption rather than a torn write.
    {ok, Fd} = file:open(Path0, [read, write, binary, raw]),
    {ok, _} = file:position(Fd, eof),
    ok = file:write(Fd, <<250:8, "this is not a valid v2 record">>),
    ok = file:close(Fd),
    ok = file:delete(filename:join(Dir, "clean.dot")),

    Ref = rabbit_guid:gen(),
    Gen = fun
        ([])  -> finished;
        (Ids) -> {Ids, []}
    end,
    ok = rabbit_variable_queue:start_msg_store(?VHOST, [Ref], {Gen, [MsgId1, MsgId2, MsgId3]}),
    false = rabbit_vhost_msg_store:successfully_recovered_state(?VHOST, ?PERSISTENT_MSG_STORE),

    %% File 0's corrupted tail is truncated away; file 1, never
    %% corrupted and never the target of open_current_file/5's own
    %% truncate (that only applies to whichever file is current now,
    %% file 1, and it does hold valid data), is untouched.
    GoodSize0 = filelib:file_size(Path0),
    GoodSize1 = filelib:file_size(Path1),

    {Cap, MSCState1} = msg_store_client_init_capture(?PERSISTENT_MSG_STORE, Ref),
    {{ok, Msg1}, MSCState2} = rabbit_msg_store:read(MsgId1, MSCState1),
    {{ok, Msg2}, MSCState3} = rabbit_msg_store:read(MsgId2, MSCState2),
    {{ok, Msg3}, MSCState4} = rabbit_msg_store:read(MsgId3, MSCState3),

    %% The store is still fully usable afterwards.
    MsgId4 = msg_id_bin(corrupt_non_current_4),
    Msg4 = {payload, <<"written after recovering file 0">>},
    ok = rabbit_msg_store:write(4, MsgId4, Msg4, MSCState4),
    ok = on_disk_await(Cap, [{4, MsgId4}]),
    {{ok, Msg4}, MSCState5} = rabbit_msg_store:read(MsgId4, MSCState4),

    ok = rabbit_msg_store:client_terminate(MSCState5),
    ok = on_disk_stop(Cap),
    restart_msg_store_empty(),
    passed.

%% scan_v2_file_for_valid_messages/2 wraps *any* error raised while
%% scanning, not just the tagged rabbit_msg_store_v2_scan errors it
%% raises itself: an I/O error surfacing through file:read/2, for
%% example, falls through the scanner's case clauses and comes out as
%% a case_clause error instead. scan_file_recovering_corruption/2 must
%% not mistake this for corruption it can safely truncate around: Fun
%% has already updated the index for every message reached before the
%% error, so guessing wrong would leave those entries pointing at a
%% file a wrong guess then truncates out from under them. It must
%% crash instead, the same as v1 does for the equivalent situation
%% (see msg_store_v1_scan_failure_crashes_recovery).
msg_store_v2_scan_failure_crashes_recovery(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, msg_store_v2_scan_failure_crashes_recovery1, [Config]).

msg_store_v2_scan_failure_crashes_recovery1(_Config) ->
    restart_msg_store_empty(),
    Ref0 = rabbit_guid:gen(),
    {Cap0, MSCState0} = msg_store_client_init_capture(?PERSISTENT_MSG_STORE, Ref0),
    MsgId1 = msg_id_bin(v2_scan_failure_1),
    MsgId2 = msg_id_bin(v2_scan_failure_2),
    Msg1 = {payload, <<"untouched by the injected failure, message 1">>},
    Msg2 = {payload, <<"untouched by the injected failure, message 2">>},
    ok = rabbit_msg_store:write(1, MsgId1, Msg1, MSCState0),
    ok = rabbit_msg_store:write(2, MsgId2, Msg2, MSCState0),
    ok = on_disk_await(Cap0, [{1, MsgId1}, {2, MsgId2}]),
    ok = rabbit_msg_store:client_terminate(MSCState0),
    ok = on_disk_stop(Cap0),

    ok = rabbit_variable_queue:stop_msg_store(?VHOST),
    Path = msg_store_file_path(?VHOST, ?PERSISTENT_MSG_STORE, "0.sqs"),
    Dir = filename:join([rabbit_vhost:msg_store_dir_path(?VHOST), atom_to_list(?PERSISTENT_MSG_STORE)]),
    OriginalSize = filelib:file_size(Path),
    ok = file:delete(filename:join(Dir, "clean.dot")),

    %% Fail exactly the first 4MB-sized file:read/2 call anywhere on
    %% the node: the v2 scanner always reads in ?SCAN_BLOCK_SIZE (4MB)
    %% chunks, and dirty recovery's very first scan is the first thing
    %% to run one after clean.dot is gone, so this lands on the target
    %% file's first read, before it has parsed anything.
    Counter = counters:new(1, []),
    ok = meck:new(file, [unstick, passthrough, no_link]),
    ok = meck:expect(file, read, fun(Fd, Length) ->
        case Length =:= 4194304 andalso counters:get(Counter, 1) =:= 0 of
            true ->
                counters:add(Counter, 1, 1),
                {error, eio};
            false ->
                meck:passthrough([Fd, Length])
        end
    end),

    %% No matching client refs => dirty recovery, forcing a full rescan
    %% from disk. The ref-count generator declares MsgId1/MsgId2 up
    %% front, mirroring msg_store_recovers_from_corrupted_file1.
    Ref = rabbit_guid:gen(),
    Gen = fun
        ([])  -> finished;
        (Ids) -> {Ids, []}
    end,
    Outcome = try
        rabbit_variable_queue:start_msg_store(?VHOST, [Ref], {Gen, [MsgId1, MsgId2]}),
        recovered
    catch
        Class:Reason -> {crashed, Class, Reason}
    end,
    ok = meck:unload(file),

    {crashed, _, CrashReason} = Outcome,
    %% Pin the crash to the scanner, not just to "something went
    %% wrong": a mis-targeted fault injection should not be able to
    %% satisfy this test by crashing recovery some other way.
    true = string:find(io_lib:format("~0p", [CrashReason]), "rabbit_msg_store_v2_scan_error") =/= nomatch,
    %% The file must be untouched: recovery crashed before it ever got
    %% the chance to (mis)truncate anything.
    OriginalSize = filelib:file_size(Path),

    %% start_msg_store/3 starts the transient store first, which
    %% succeeded, then the persistent one, whose supervisor:start_child/2
    %% call failed and so left no child spec behind to clean up: only
    %% the transient store, which is still running, needs stopping
    %% before starting fresh (see msg_store_v1_scan_failure_crashes_recovery).
    ok = rabbit_vhost_msg_store:stop(?VHOST, ?TRANSIENT_MSG_STORE),

    %% Starting it again, without the injected fault, must still
    %% recover both messages normally.
    ok = rabbit_variable_queue:start_msg_store(?VHOST, [Ref], {Gen, [MsgId1, MsgId2]}),
    false = rabbit_vhost_msg_store:successfully_recovered_state(?VHOST, ?PERSISTENT_MSG_STORE),

    %% The store is fully usable: both original messages and a new one
    %% written afterwards all read back correctly.
    {Cap, MSCState1} = msg_store_client_init_capture(?PERSISTENT_MSG_STORE, Ref),
    {{ok, Msg1}, MSCState1a} = rabbit_msg_store:read(MsgId1, MSCState1),
    {{ok, Msg2}, MSCState1b} = rabbit_msg_store:read(MsgId2, MSCState1a),
    MsgId3 = msg_id_bin(v2_scan_failure_3),
    Msg3 = {payload, <<"written after the unexpected error">>},
    ok = rabbit_msg_store:write(3, MsgId3, Msg3, MSCState1b),
    ok = on_disk_await(Cap, [{3, MsgId3}]),
    {{ok, Msg3}, MSCState2} = rabbit_msg_store:read(MsgId3, MSCState1b),
    %% MsgId3 lands right after the two original messages, not after a
    %% truncation: nothing was ever discarded.
    {ok, ScannedEntries} = rabbit_msg_store:scan_file_for_valid_messages(Path),
    ScannedOffsets = maps:from_list([{MsgId, Offset} || {MsgId, _TotalSize, Offset} <- ScannedEntries]),
    3 = maps:size(ScannedOffsets),
    OriginalSize = maps:get(MsgId3, ScannedOffsets),

    ok = rabbit_msg_store:client_terminate(MSCState2),
    ok = on_disk_stop(Cap),
    restart_msg_store_empty(),
    passed.

%% v1 has no tagged corruption errors of its own (see the comment
%% above scan_file_recovering_corruption/2): corruption there is
%% handled silently, by byte-shifting forward, never by raising. So
%% build_index_worker scans a v1 file directly, unwrapped, and a scan
%% failure there -- which can now only mean an unanticipated error
%% (an I/O failure, as injected below, or a bug in Fun), not on-disk
%% corruption -- must crash recovery loudly instead of being mistaken
%% for corruption and silently discarding the whole file.
msg_store_v1_scan_failure_crashes_recovery(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, msg_store_v1_scan_failure_crashes_recovery1, [Config]).

msg_store_v1_scan_failure_crashes_recovery1(_Config) ->
    ok = rabbit_variable_queue:stop_msg_store(?VHOST),
    Dir = filename:join([rabbit_vhost:msg_store_dir_path(?VHOST), atom_to_list(?PERSISTENT_MSG_STORE)]),
    ok = rabbit_file:recursive_delete([Dir]),
    ok = filelib:ensure_dir(filename:join(Dir, "nothing")),

    %% Hand-write a legacy v1 segment file, same as msg_store_v1_compat,
    %% with no clean.dot present, forcing dirty recovery to scan it.
    MsgId = msg_id_bin(v1_scan_failure),
    LegacyMsg = {legacy, "a v1 message untouched by the injected failure"},
    Bin = term_to_binary(LegacyMsg),
    V1Record = [<<(byte_size(MsgId) + byte_size(Bin)):64>>, MsgId, Bin, <<255>>],
    Path = filename:join(Dir, "0.rdq"),
    ok = file:write_file(Path, V1Record),
    OriginalSize = filelib:file_size(Path),

    %% Fail exactly the first 4MB-sized file:read/2 call anywhere on
    %% the node: v1 scanning, like v2, always reads in ?SCAN_BLOCK_SIZE
    %% (4MB) chunks, and dirty recovery's very first scan is the first
    %% thing to run one after clean.dot is gone.
    Counter = counters:new(1, []),
    ok = meck:new(file, [unstick, passthrough, no_link]),
    ok = meck:expect(file, read, fun(Fd, Length) ->
        case Length =:= 4194304 andalso counters:get(Counter, 1) =:= 0 of
            true ->
                counters:add(Counter, 1, 1),
                {error, eio};
            false ->
                meck:passthrough([Fd, Length])
        end
    end),

    Ref = rabbit_guid:gen(),
    Gen = fun
        ([])  -> finished;
        (Ids) -> {Ids, []}
    end,
    Outcome = try
        rabbit_variable_queue:start_msg_store(?VHOST, [Ref], {Gen, [MsgId]}),
        recovered
    catch
        Class:Reason -> {crashed, Class, Reason}
    end,
    ok = meck:unload(file),

    {crashed, _, _} = Outcome,
    %% The file must be untouched: recovery crashed before it ever got
    %% the chance to (mis)truncate anything.
    OriginalSize = filelib:file_size(Path),

    %% start_msg_store/3 starts the transient store first, which
    %% succeeded, then the persistent one, whose supervisor:start_child/2
    %% call failed and so left no child spec behind to clean up: only
    %% the transient store, which is still running, needs stopping
    %% before starting fresh.
    ok = rabbit_vhost_msg_store:stop(?VHOST, ?TRANSIENT_MSG_STORE),

    %% Starting it again, without the injected fault, must still
    %% recover the message normally.
    ok = rabbit_variable_queue:start_msg_store(?VHOST, [Ref], {Gen, [MsgId]}),
    false = rabbit_vhost_msg_store:successfully_recovered_state(?VHOST, ?PERSISTENT_MSG_STORE),
    MSCState0 = msg_store_client_init(?PERSISTENT_MSG_STORE, Ref),
    {{ok, LegacyMsg}, MSCState1} = rabbit_msg_store:read(MsgId, MSCState0),
    ok = rabbit_msg_store:client_terminate(MSCState1),

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
            MsgIds = sets:to_list(MsgIdsS),
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

on_disk_await(Pid, MsgIds0) when is_list(MsgIds0) ->
    {_, MsgIds} = lists:unzip(MsgIds0),
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
                                             end)}.

msg_store_contains(Atom, MsgIds, MSCState) ->
    Atom = lists:foldl(
             fun ({_, MsgId}, Atom1) when Atom1 =:= Atom ->
                     rabbit_msg_store:contains(MsgId, MSCState) end,
             Atom, MsgIds).

msg_store_read(MsgIds, MSCState) ->
    lists:foldl(fun ({_, MsgId}, MSCStateM) ->
                        {{ok, MsgId}, MSCStateN} = rabbit_msg_store:read(
                                                     MsgId, MSCStateM),
                        MSCStateN
                end, MSCState, MsgIds).

msg_store_write(MsgIds, MSCState) ->
    ok = lists:foldl(fun ({SeqId, MsgId}, ok) ->
                             rabbit_msg_store:write(SeqId, MsgId, MsgId, MSCState)
                     end, ok, MsgIds).

msg_store_write_flow(MsgIds, MSCState) ->
    ok = lists:foldl(fun ({SeqId, MsgId}, ok) ->
                             rabbit_msg_store:write_flow(SeqId, MsgId, MsgId, MSCState)
                     end, ok, MsgIds).

msg_store_remove(MsgIds, MSCState) ->
    rabbit_msg_store:remove(MsgIds, MSCState).

msg_store_remove(MsgStore, Ref, MsgIds) ->
    with_msg_store_client(MsgStore, Ref,
                          fun (MSCStateM) ->
                                  {ok, _} = msg_store_remove(MsgIds, MSCStateM),
                                  MSCStateM
                          end).

with_msg_store_client(MsgStore, Ref, Fun) ->
    rabbit_msg_store:client_terminate(
      Fun(msg_store_client_init(MsgStore, Ref))).

foreach_with_msg_store_client(MsgStore, Ref, Fun, L) ->
    rabbit_msg_store:client_terminate(
      lists:foldl(fun (MsgId, MSCState) -> Fun(MsgId, MSCState) end,
                  msg_store_client_init(MsgStore, Ref), L)).

test_msg_store_confirms(MsgIds, Cap, GenRef, MSCState) ->
    %% write -> confirmed
    MsgIds1 = [{GenRef(), MsgId} || {_, MsgId} <- MsgIds],
    ok = msg_store_write(MsgIds1, MSCState),
    ok = on_disk_await(Cap, MsgIds1),
    %% remove -> _
    {ok, []} = msg_store_remove(MsgIds1, MSCState),
    ok = on_disk_await(Cap, []),
    %% write, remove -> confirmed
    MsgIds2 = [{GenRef(), MsgId} || {_, MsgId} <- MsgIds],
    ok = msg_store_write(MsgIds2, MSCState),
    {ok, ConfirmedMsgIds2} = msg_store_remove(MsgIds2, MSCState),
    ok = on_disk_await(Cap, lists:filter(fun({_, MsgId}) -> not lists:member(MsgId, ConfirmedMsgIds2) end, MsgIds2)),
    %% write, remove, write -> confirmed, confirmed
    MsgIds3 = [{GenRef(), MsgId} || {_, MsgId} <- MsgIds],
    ok = msg_store_write(MsgIds3, MSCState),
    {ok, ConfirmedMsgIds3} = msg_store_remove(MsgIds3, MSCState),
    MsgIds4 = [{GenRef(), MsgId} || {_, MsgId} <- MsgIds],
    ok = msg_store_write(MsgIds4, MSCState),
    ok = on_disk_await(Cap, lists:filter(fun({_, MsgId}) -> not lists:member(MsgId, ConfirmedMsgIds3) end, MsgIds3) ++ MsgIds4),
    %% remove, write -> confirmed
    {ok, []} = msg_store_remove(MsgIds4, MSCState),
    MsgIds5 = [{GenRef(), MsgId} || {_, MsgId} <- MsgIds],
    ok = msg_store_write(MsgIds5, MSCState),
    ok = on_disk_await(Cap, MsgIds5),
    %% remove, write, remove -> confirmed
    {ok, []} = msg_store_remove(MsgIds5, MSCState),
    MsgIds6 = [{GenRef(), MsgId} || {_, MsgId} <- MsgIds],
    ok = msg_store_write(MsgIds6, MSCState),
    {ok, ConfirmedMsgIds6} = msg_store_remove(MsgIds6, MSCState),
    ok = on_disk_await(Cap, lists:filter(fun({_, MsgId}) -> not lists:member(MsgId, ConfirmedMsgIds6) end, MsgIds6)),
    %% confirmation on timer-based sync
    passed = test_msg_store_confirm_timer(GenRef),
    passed.

test_msg_store_confirm_timer(GenRef) ->
    Ref = rabbit_guid:gen(),
    MsgId  = msg_id_bin(1),
    Self = self(),
    MSCState = rabbit_vhost_msg_store:client_init(
        ?VHOST,
        ?PERSISTENT_MSG_STORE,
        Ref,
        fun (MsgIds, _ActionTaken) ->
            case sets:is_element(MsgId, MsgIds) of
                true  -> Self ! on_disk;
                false -> ok
            end
        end),
    MsgIdsChecked = [{GenRef(), MsgId}],
    ok = msg_store_write(MsgIdsChecked, MSCState),
    ok = msg_store_keep_busy_until_confirm([msg_id_bin(2)], GenRef, MSCState, false),
    {ok, _} = msg_store_remove(MsgIdsChecked, MSCState),
    ok = rabbit_msg_store:client_delete_and_terminate(MSCState),
    passed.

msg_store_keep_busy_until_confirm(MsgIds, GenRef, MSCState, Blocked) ->
    After = case Blocked of
                false -> 0;
                true  -> ?MAX_WAIT
            end,
    Recurse = fun () -> msg_store_keep_busy_until_confirm(
                          MsgIds, GenRef, MSCState, credit_flow:blocked()) end,
    receive
        on_disk            -> ok;
        {bump_credit, Msg} -> credit_flow:handle_bump_msg(Msg),
                              Recurse()
    after After ->
            MsgIds1 = [{GenRef(), MsgId} || MsgId <- MsgIds],
            ok = msg_store_write_flow(MsgIds1, MSCState),
            {ok, _} = msg_store_remove(MsgIds1, MSCState),
            Recurse()
    end.

test_msg_store_client_delete_and_terminate(GenRef) ->
    restart_msg_store_empty(),
    MsgIds = [{GenRef(), msg_id_bin(M)} || M <- lists:seq(1, 10)],
    Ref = rabbit_guid:gen(),
    MSCState = msg_store_client_init(?PERSISTENT_MSG_STORE, Ref),
    ok = msg_store_write(MsgIds, MSCState),
    %% test the 'dying client' fast path for writes
    ok = rabbit_msg_store:client_delete_and_terminate(MSCState),
    passed.

%% -------------------------------------------------------------------
%% Message store file scanning.
%% -------------------------------------------------------------------

%% While it is possible although very unlikely that this test case
%% produces false positives, all failures of this test case should
%% be investigated thoroughly as they test an algorithm that is
%% central to the reliability of the data in the shared message store.
%% Failing files can be found in the CT private data.
msg_store_file_scan(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, msg_store_file_scan1, [Config]).

msg_store_file_scan1(Config) ->
    Scan = fun (Blocks) ->
        Expected = gen_result(Blocks),
        Path = gen_msg_file(Config, Blocks),
        Result = rabbit_msg_store:scan_file_for_valid_messages(Path),
        ok = file:delete(Path),
        case Result of
            Expected -> ok;
            _ -> {expected, Expected, got, Result}
        end
    end,
    %% Empty files.
    ok = Scan([]),
    ok = Scan([{pad, 1024}]),
    ok = Scan([{pad, 1024 * 1024}]),
    %% One-message files.
    ok = Scan([{msg, gen_id(), <<0>>}]),
    ok = Scan([{msg, gen_id(), <<255>>}]),
    ok = Scan([{msg, gen_id(), gen_msg()}]),
    ok = Scan([{pad, 1024}, {msg, gen_id(), gen_msg()}]),
    ok = Scan([{pad, 1024 * 1024}, {msg, gen_id(), gen_msg()}]),
    ok = Scan([{msg, gen_id(), gen_msg()}, {pad, 1024}]),
    ok = Scan([{msg, gen_id(), gen_msg()}, {pad, 1024 * 1024}]),
    %% Multiple messages.
    ok = Scan([{msg, gen_id(), gen_msg()} || _ <- lists:seq(1, 2)]),
    ok = Scan([{msg, gen_id(), gen_msg()} || _ <- lists:seq(1, 5)]),
    ok = Scan([{msg, gen_id(), gen_msg()} || _ <- lists:seq(1, 20)]),
    ok = Scan([{msg, gen_id(), gen_msg()} || _ <- lists:seq(1, 100)]),
    %% Multiple messages with padding.
    ok = Scan([
        {pad, 1024},
        {msg, gen_id(), gen_msg()},
        {msg, gen_id(), gen_msg()}
    ]),
    ok = Scan([
        {msg, gen_id(), gen_msg()},
        {pad, 1024},
        {msg, gen_id(), gen_msg()}
    ]),
    ok = Scan([
        {msg, gen_id(), gen_msg()},
        {msg, gen_id(), gen_msg()},
        {pad, 1024}
    ]),
    ok = Scan([
        {pad, 1024},
        {msg, gen_id(), gen_msg()},
        {pad, 1024},
        {msg, gen_id(), gen_msg()}
    ]),
    ok = Scan([
        {msg, gen_id(), gen_msg()},
        {pad, 1024},
        {msg, gen_id(), gen_msg()},
        {pad, 1024}
    ]),
    ok = Scan([
        {pad, 1024},
        {msg, gen_id(), gen_msg()},
        {msg, gen_id(), gen_msg()},
        {pad, 1024}
    ]),
    ok = Scan([
        {pad, 1024},
        {msg, gen_id(), gen_msg()},
        {pad, 1024},
        {msg, gen_id(), gen_msg()},
        {pad, 1024}
    ]),
    OneOf = fun(A, B) ->
        case rand:uniform() of
            F when F < +0.5 -> A;
            _ -> B
        end
    end,
    ok = Scan([OneOf({msg, gen_id(), gen_msg()}, {pad, 1024}) || _ <- lists:seq(1, 2)]),
    ok = Scan([OneOf({msg, gen_id(), gen_msg()}, {pad, 1024}) || _ <- lists:seq(1, 5)]),
    ok = Scan([OneOf({msg, gen_id(), gen_msg()}, {pad, 1024}) || _ <- lists:seq(1, 20)]),
    ok = Scan([OneOf({msg, gen_id(), gen_msg()}, {pad, 1024}) || _ <- lists:seq(1, 100)]),
    %% Duplicate messages.
    Msg = {msg, gen_id(), gen_msg()},
    ok = Scan([Msg, Msg]),
    ok = Scan([Msg, Msg, Msg, Msg, Msg]),
    ok = Scan([Msg, {pad, 1024}, Msg]),
    ok = Scan([Msg]
        ++ [OneOf({msg, gen_id(), gen_msg()}, {pad, 1024}) || _ <- lists:seq(1, 100)]
        ++ [Msg]),
    %% Truncated start of message.
    ok = Scan([{bin, <<21:56, "deadbeefdeadbeef", "hello", 255>>}]),
    ok = Scan([{bin, <<21:48, "deadbeefdeadbeef", "hello", 255>>}]),
    ok = Scan([{bin, <<21:40, "deadbeefdeadbeef", "hello", 255>>}]),
    ok = Scan([{bin, <<21:32, "deadbeefdeadbeef", "hello", 255>>}]),
    ok = Scan([{bin, <<21:24, "deadbeefdeadbeef", "hello", 255>>}]),
    ok = Scan([{bin, <<21:16, "deadbeefdeadbeef", "hello", 255>>}]),
    ok = Scan([{bin, <<21:8, "deadbeefdeadbeef", "hello", 255>>}]),
    ok = Scan([{bin, <<"deadbeefdeadbeef", "hello", 255>>}]),
    ok = Scan([{bin, <<"beefdeadbeef", "hello", 255>>}]),
    ok = Scan([{bin, <<"deadbeef", "hello", 255>>}]),
    ok = Scan([{bin, <<"beef", "hello", 255>>}]),
    ok = Scan([{bin, <<"hello", 255>>}]),
    ok = Scan([{bin, <<255>>}]),
    %% Truncated end of message (unlikely).
    ok = Scan([{bin, <<255>>}]),
    ok = Scan([{bin, <<255, 255>>}]),
    ok = Scan([{bin, <<255, 255, 255>>}]),
    ok = Scan([{bin, <<255, 255, 255, 255>>}]),
    ok = Scan([{bin, <<255, 255, 255, 255, 255>>}]),
    ok = Scan([{bin, <<255, 255, 255, 255, 255, 255>>}]),
    ok = Scan([{bin, <<255, 255, 255, 255, 255, 255, 255>>}]),
    ok = Scan([{bin, <<255, 255, 255, 255, 255, 255, 255, 255>>}]),
    ok = Scan([{bin, <<15:64, "deadbeefdeadbee">>}]),
    ok = Scan([{bin, <<16:64, "deadbeefdeadbeef">>}]),
    ok = Scan([{bin, <<17:64, "deadbeefdeadbeef", 0>>}]),
    ok = Scan([{bin, <<17:64, "deadbeefdeadbeef", 255>>}]),
    ok = Scan([{bin, <<17:64, "deadbeefdeadbeef", 255, 254>>}]),
    %% Messages with no content.
    ok = Scan([{bin, <<0:64, "deadbeefdeadbeef", 255>>}]),
    ok = Scan([{msg, gen_id(), <<>>}]),
    %% Tricky messages.
    %%
    %% These only get properly detected when the index is populated.
    %% In this test case we simulate the index with a fun.
    TrickyScan = fun (Blocks, Expected, Fun) ->
        Path = gen_msg_file(Config, Blocks),
        Result = rabbit_msg_store:scan_file_for_valid_messages(Path, Fun),
        case Result of
            Expected -> ok;
            _ -> {expected, Expected, got, Result}
        end
    end,
    ok = TrickyScan(
        [{bin, <<0, 0:48, 17, 17, "idididididididid", 255, 0:4352/unit:8, 255>>}],
        {ok, [{<<"idididididididid">>, 4378, 1}]},
        fun(Obj = {<<"idididididididid">>, 4378, 1}) -> {valid, Obj}; (_) -> invalid end),
    %% Off-by-nine regression testing. The file scanning could miss
    %% some messages if previous data looked like a message but its
    %% size went past the end of the file.
    lists:foreach(fun(N) ->
        ok = Scan([
            {bin, <<(4194304 + N):64, 0:(4194304 - 8 - 25 - 10)/unit:8>>},
            {msg, gen_id(), <<>>},
            %% Padding ensures there's no 255 at the end of the size indicated by 'bin'.
            {pad, 10}
        ])
    end, lists:seq(-9, -1)),
    %% All good!!
    passed.

%% Same idea as msg_store_file_scan/1, but for the v2 (.sqs) shared
%% store format: typed HOLE/SMALL_HOLE/MESSAGE records instead of
%% zero-filled gaps, and no length-ambiguous scanning (format is
%% inferred from the .sqs extension).
msg_store_file_scan_v2(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, msg_store_file_scan_v2_1, [Config]).

msg_store_file_scan_v2_1(Config) ->
    Scan = fun (Blocks) ->
        Expected = gen_result_v2(Blocks),
        Path = gen_msg_file_v2(Config, Blocks),
        Result = rabbit_msg_store:scan_file_for_valid_messages(Path),
        ok = file:delete(Path),
        case Result of
            Expected -> ok;
            _ -> {expected, Expected, got, Result}
        end
    end,
    %% Same as Scan/1, but for the corruption checks that must make the
    %% scan crash instead of silently accepting or skipping bad data.
    %% Only the reason atom is asserted on, not the exact offset. Every
    %% such crash is wrapped with the file path (rabbit_msg_store_v2_scan_error)
    %% by scan_v2_file_for_valid_messages/2, so that outer shape is
    %% unwrapped here before matching the actual reason.
    ScanError = fun (Blocks) ->
        Path = gen_msg_file_v2(Config, Blocks),
        Result = try
            rabbit_msg_store:scan_file_for_valid_messages(Path),
            unexpected_success
        catch
            error:{rabbit_msg_store_v2_scan_error, Path, {rabbit_msg_store_v2_scan, _Offset, Reason}} ->
                Reason;
            error:{rabbit_msg_store_v2_scan_error, Path, {rabbit_msg_store_v2_scan, _Offset, Reason, _}} ->
                Reason
        end,
        ok = file:delete(Path),
        Result
    end,
    %% Empty files (nothing but the header).
    ok = Scan([]),
    ok = Scan([{hole, 1024}]),
    ok = Scan([{hole, 1024 * 1024}]),
    %% One-message files, with and without surrounding holes.
    ok = Scan([{msg, gen_id(), gen_msg()}]),
    ok = Scan([{msg, gen_id(), term_to_binary(<<>>)}]),
    ok = Scan([{small_hole, 3}, {msg, gen_id(), gen_msg()}]),
    ok = Scan([{hole, 1024}, {msg, gen_id(), gen_msg()}]),
    ok = Scan([{msg, gen_id(), gen_msg()}, {small_hole, 2}]),
    ok = Scan([{msg, gen_id(), gen_msg()}, {hole, 1024}]),
    %% Multiple messages, with and without holes in between.
    ok = Scan([{msg, gen_id(), gen_msg()} || _ <- lists:seq(1, 20)]),
    ok = Scan([
        {hole, 1024},
        {msg, gen_id(), gen_msg()},
        {small_hole, 1},
        {msg, gen_id(), gen_msg()},
        {hole, 1024 * 1024},
        {msg, gen_id(), gen_msg()}
    ]),
    %% Duplicate messages.
    Msg = {msg, gen_id(), gen_msg()},
    ok = Scan([Msg, Msg]),
    ok = Scan([Msg, {hole, 1024}, Msg]),
    %% A hole larger than the scan block size (exercises the seek-past path).
    ok = Scan([{hole, 8 * 1024 * 1024}, {msg, gen_id(), gen_msg()}]),
    %% Runs of SMALL_HOLE are bounded to at most 4 bytes by construction:
    %% 4 in a row is fine, 5 is corruption.
    ok = Scan([{small_hole, 4}, {msg, gen_id(), gen_msg()}]),
    small_hole_run_too_long = ScanError([{bin, binary:copy(<<1:8>>, 5)}]),
    %% The same run of 5 SMALL_HOLE bytes must still be caught when the
    %% scanner's 4MB (4194304-byte) read buffer happens to end partway
    %% through it. A filler message with a total on-disk record size of
    %% 4194301 bytes ends the run 3 bytes before the buffer boundary
    %% (64-byte header + 4194301 = 4194365; the buffer covers up to
    %% 64 + 4194304 = 4194368), splitting the run into a 3-byte tail of
    %% one buffer and a 2-byte head of the next, neither 5 bytes long
    %% on its own. The record's body is 4194301 - 21 (record header) -
    %% 6 (term_to_binary/1's own binary-term overhead) = 4194274 bytes.
    small_hole_run_too_long = ScanError([
        {msg, gen_id(), term_to_binary(crypto:strong_rand_bytes(4194274))},
        {bin, binary:copy(<<1:8>>, 5)},
        {msg, gen_id(), gen_msg()}
    ]),
    %% A hole is never immediately followed by another hole, in either
    %% combination, whether both are fully buffered or the first is only
    %% skipped via the seek-past path (too large to have been buffered).
    hole_after_hole = ScanError([{small_hole, 1}, {hole, 10}]),
    hole_after_hole = ScanError([{hole, 10}, {hole, 10}]),
    hole_after_hole = ScanError([{hole, 10}, {small_hole, 2}]),
    hole_after_hole = ScanError([{hole, 8 * 1024 * 1024}, {hole, 10}]),
    %% A message body that doesn't decode via binary_to_term/1 is
    %% corruption, regardless of how plausible its Type/Size/MsgId looked.
    invalid_message_body = ScanError([{msg, gen_id(), <<"not a real term, just raw junk bytes">>}]),
    %% A HOLE/MESSAGE Size field below the type's own minimum record
    %% length is corruption, not a torn write (see the comment above the
    %% Size < 5 / Size < 21 clauses: a torn write cannot fully flush a
    %% valid header with a bogus size).
    invalid_hole_size = ScanError([{bin, <<2:8, 3:32>>}]),
    invalid_message_size = ScanError([{bin, <<3:8, 10:32>>}]),
    %% Torn records at EOF.
    ok = Scan([
        {msg, gen_id(), gen_msg()},
        {bin, <<3:8, 999999:32, (gen_id())/binary, "not enough bytes">>}
    ]),
    ok = Scan([
        {msg, gen_id(), gen_msg()},
        {bin, <<2:8, 999999:32>>}
    ]),
    %% Unrecognised record types, including the reserved 0 byte.
    lists:foreach(fun(Type) ->
        Path = gen_msg_file_v2(Config, [{bin, <<Type:8, "garbage">>}]),
        ok = try
            rabbit_msg_store:scan_file_for_valid_messages(Path),
            {unexpected_success, Type}
        catch
            error:{rabbit_msg_store_v2_scan_error, Path,
                   {rabbit_msg_store_v2_scan, 64, unrecognised_record_type, Type}} -> ok
        end,
        ok = file:delete(Path)
    end, [0, 4, 5, 200, 255]),
    passed.

gen_msg_file_v2(Config, Blocks) ->
    PrivDir = ?config(priv_dir, Config),
    TmpFile = integer_to_list(erlang:unique_integer([positive])) ++ ".sqs",
    Path = filename:join(PrivDir, TmpFile),
    %% The header content itself is never validated on the read path;
    %% any 64 bytes will do here.
    Header = <<0:64/unit:8>>,
    ok = file:write_file(Path, [Header | [case Block of
        {bin, Bin} ->
            Bin;
        {hole, Size} ->
            %% Inner bytes of a HOLE are never interpreted by the scanner.
            [<<2:8, Size:32>>, <<0:(Size - 5)/unit:8>>];
        {small_hole, Size} when Size >= 1, Size =< 4 ->
            binary:copy(<<1:8>>, Size);
        {msg, MsgId, Msg} ->
            Size = 21 + byte_size(Msg),
            [<<3:8, Size:32>>, MsgId, Msg]
    end || Block <- Blocks]]),
    Path.

gen_result_v2(Blocks) ->
    {ok, gen_result_v2(Blocks, 64, [])}.

gen_result_v2([], _, Acc) ->
    Acc;
gen_result_v2([{bin, Bin}|Tail], Offset, Acc) ->
    gen_result_v2(Tail, Offset + byte_size(Bin), Acc);
gen_result_v2([{hole, Size}|Tail], Offset, Acc) ->
    gen_result_v2(Tail, Offset + Size, Acc);
gen_result_v2([{small_hole, Size}|Tail], Offset, Acc) ->
    gen_result_v2(Tail, Offset + Size, Acc);
gen_result_v2([{msg, MsgId, Msg}|Tail], Offset, Acc) ->
    Size = 21 + byte_size(Msg),
    %% Only the first MsgId found (lowest offset) is returned when
    %% duplicates exist.
    case lists:keymember(MsgId, 1, Acc) of
        false ->
            gen_result_v2(Tail, Offset + Size, [{MsgId, Size, Offset}|Acc]);
        true ->
            gen_result_v2(Tail, Offset + Size, Acc)
    end.

gen_id() ->
    rand:bytes(16).

%% Test that when the GC process is unresponsive during shutdown,
%% the msg_store recovers cleanly because terminate sends the GC an
%% exit signal and proceeds to write recovery files.
msg_store_gc_stuck_suspended(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, msg_store_gc_stuck_suspended1, [Config]).

msg_store_gc_stuck_suspended1(_Config) ->
    GenRef = fun() -> make_ref() end,
    restart_msg_store_empty(),

    %% Write some messages so the store has data to recover.
    Ref = rabbit_guid:gen(),
    MSCState = msg_store_client_init(?PERSISTENT_MSG_STORE, Ref),
    MsgIds = [{GenRef(), msg_id_bin(M)} || M <- lists:seq(1, 50)],
    ok = msg_store_write(MsgIds, MSCState),
    ok = rabbit_msg_store:client_terminate(MSCState),

    %% Get the msg_store pid and its GC pid.
    StorePid = rabbit_vhost_msg_store:vhost_store_pid(
                   ?VHOST, ?PERSISTENT_MSG_STORE),
    GCPid = rabbit_msg_store:gc_pid(StorePid),
    true = is_process_alive(GCPid),

    %% Suspend the GC process so it cannot process messages.
    ok = sys:suspend(GCPid),

    %% Stop the transient store cleanly first.
    rabbit_vhost_msg_store:stop(?VHOST, ?TRANSIENT_MSG_STORE),

    %% Terminate the persistent store via the supervisor. The terminate
    %% callback sends the GC an exit signal. The GC does not trap exits
    %% so it terminates immediately, and terminate proceeds to write
    %% recovery files.
    {ok, VHostSup} = rabbit_vhost_sup_sup:get_vhost_sup(?VHOST),
    ok = supervisor:terminate_child(VHostSup, ?PERSISTENT_MSG_STORE),

    %% Delete the child specs so we can restart.
    ok = supervisor:delete_child(VHostSup, ?PERSISTENT_MSG_STORE),

    %% Restart the msg_store and check recovery state.
    ok = rabbit_variable_queue:start_msg_store(
             ?VHOST, [Ref], {fun ([]) -> finished end, []}),

    %% The store should report a clean recovery because the fix
    %% terminates the unresponsive GC and proceeds to write recovery files.
    true = rabbit_vhost_msg_store:successfully_recovered_state(
                ?VHOST, ?PERSISTENT_MSG_STORE),

    %% Verify all messages survived the unclean GC shutdown.
    MSCState2 = msg_store_client_init(?PERSISTENT_MSG_STORE, Ref),
    true = msg_store_contains(true, MsgIds, MSCState2),
    ok = rabbit_msg_store:client_terminate(MSCState2),

    %% Clean up.
    restart_msg_store_empty(),
    passed.

%% Test that when the GC process is blocked mid-callback (simulating disk I/O),
%% the msg_store recovers cleanly because terminate sends the GC an exit
%% signal and proceeds to write recovery files.
msg_store_gc_stuck_mid_callback(Config) ->
    rabbit_ct_broker_helpers:setup_meck(Config),
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, msg_store_gc_stuck_mid_callback1, [Config]).

msg_store_gc_stuck_mid_callback1(_Config) ->
    GenRef = fun() -> make_ref() end,
    restart_msg_store_empty(),

    %% Write some messages so the store has data to recover.
    Ref = rabbit_guid:gen(),
    MSCState = msg_store_client_init(?PERSISTENT_MSG_STORE, Ref),
    MsgIds = [{GenRef(), msg_id_bin(M)} || M <- lists:seq(1, 50)],
    ok = msg_store_write(MsgIds, MSCState),
    ok = rabbit_msg_store:client_terminate(MSCState),

    %% Get the msg_store pid and its GC pid.
    StorePid = rabbit_vhost_msg_store:vhost_store_pid(
                   ?VHOST, ?PERSISTENT_MSG_STORE),
    GCPid = rabbit_msg_store:gc_pid(StorePid),
    true = is_process_alive(GCPid),

    %% Mock compact_file to signal the test process on entry, then block
    %% indefinitely, simulating a GC process stuck on disk I/O mid-callback.
    TestPid = self(),
    ok = meck:new(rabbit_msg_store, [no_link, passthrough]),
    ok = meck:expect(rabbit_msg_store, compact_file,
                     fun(_, _) ->
                         TestPid ! gc_in_callback,
                         %% Block forever with no CPU usage, simulating a
                         %% process stuck waiting on disk I/O that never
                         %% completes. The GC will be terminated by stop_gc/1.
                         receive after infinity -> ok end
                     end),

    %% Send a compact cast directly to the GC. It will enter the mocked
    %% compact_file, signal us, then block inside the handle_cast callback.
    rabbit_msg_store_gc:compact(GCPid, 0),

    %% Wait for the GC to confirm it has entered the blocking callback.
    receive
        gc_in_callback -> ok
    after 5000 ->
        error(gc_did_not_enter_callback)
    end,

    %% Stop the transient store cleanly first.
    rabbit_vhost_msg_store:stop(?VHOST, ?TRANSIENT_MSG_STORE),

    %% Terminate the persistent store via the supervisor. The GC is blocked
    %% mid-callback but the exit signal sent by terminate preempts the
    %% callback because the GC does not trap exits, so terminate proceeds
    %% to write recovery files.
    {ok, VHostSup} = rabbit_vhost_sup_sup:get_vhost_sup(?VHOST),
    ok = supervisor:terminate_child(VHostSup, ?PERSISTENT_MSG_STORE),

    ok = meck:unload(rabbit_msg_store),

    %% Delete the child spec so we can restart.
    ok = supervisor:delete_child(VHostSup, ?PERSISTENT_MSG_STORE),

    %% Restart the msg_store and check recovery state.
    ok = rabbit_variable_queue:start_msg_store(
             ?VHOST, [Ref], {fun ([]) -> finished end, []}),

    %% The store should report a clean recovery because the fix terminates
    %% the unresponsive GC and proceeds to write recovery files.
    true = rabbit_vhost_msg_store:successfully_recovered_state(
                ?VHOST, ?PERSISTENT_MSG_STORE),

    %% Verify all messages survived the unclean GC shutdown.
    MSCState2 = msg_store_client_init(?PERSISTENT_MSG_STORE, Ref),
    true = msg_store_contains(true, MsgIds, MSCState2),
    ok = rabbit_msg_store:client_terminate(MSCState2),

    %% Clean up.
    restart_msg_store_empty(),
    passed.

gen_msg() ->
    gen_msg(1024 * 1024).

gen_msg(MaxSize) ->
    Bytes = rand:bytes(rand:uniform(MaxSize)),
    %% We remove 255 to avoid false positives. In a running
    %% rabbit node we will not get false positives because
    %% we also check messages against the index.
    Bytes1 = << <<case B of 255 -> 254; _ -> B end>> || <<B>> <= Bytes >>,
    %% A real message body is always term_to_binary/1-encoded (see
    %% write_message/3): the v2 scanner now validates that, so a body
    %% that isn't a real encoded term would be rejected as corruption.
    term_to_binary(Bytes1).

gen_msg_file(Config, Blocks) ->
    PrivDir = ?config(priv_dir, Config),
    TmpFile = integer_to_list(erlang:unique_integer([positive])),
    Path = filename:join(PrivDir, TmpFile),
    ok = file:write_file(Path, [case Block of
        {bin, Bin} ->
            Bin;
        {pad, Size} ->
            %% Empty space between messages is expected to be zeroes.
            <<0:Size/unit:8>>;
        {msg, MsgId, Msg} ->
            Size = 16 + byte_size(Msg),
            [<<Size:64>>, MsgId, Msg, <<255>>]
    end || Block <- Blocks]),
    Path.

gen_result(Blocks) ->
    Messages = gen_result(Blocks, 0, []),
    {ok, Messages}.

gen_result([], _, Acc) ->
    Acc;
gen_result([{bin, Bin}|Tail], Offset, Acc) ->
    gen_result(Tail, Offset + byte_size(Bin), Acc);
gen_result([{pad, Size}|Tail], Offset, Acc) ->
    gen_result(Tail, Offset + Size, Acc);
gen_result([{msg, MsgId, Msg}|Tail], Offset, Acc) ->
    Size = 9 + 16 + byte_size(Msg),
    %% Only the first MsgId found is returned when duplicates exist.
    case lists:keymember(MsgId, 1, Acc) of
        false ->
            gen_result(Tail, Offset + Size, [{MsgId, Size, Offset}|Acc]);
        true ->
            gen_result(Tail, Offset + Size, Acc)
    end.

%% -------------------------------------------------------------------
%% Backing queue.
%% -------------------------------------------------------------------

setup_backing_queue_test_group(Config) ->
    {ok, Bytes} =
        application:get_env(rabbit, queue_index_embed_msgs_below),
    rabbit_ct_helpers:set_config(Config, [
        {rmq_queue_index_embed_msgs_below, Bytes}
      ]).

teardown_backing_queue_test_group(Config) ->
    %% We will have restarted the message store, and thus changed
    %% the order of the children of rabbit_sup. This will cause
    %% problems if there are subsequent failures - see bug 24262.
    ok = restart_app(),
    Config.

bq_queue_index(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, bq_queue_index1, [Config]).

index_mod() ->
    rabbit_classic_queue_index_v2.

segment_entry_count() ->
    persistent_term:get(classic_queue_index_v2_segment_entry_count, 4096).

bq_queue_index1(_Config) ->
    IndexMod = index_mod(),
    SegmentSize = segment_entry_count(),
    TwoSegs = SegmentSize + SegmentSize,
    MostOfASegment = trunc(SegmentSize*0.75),
    SeqIdsA = lists:seq(0, MostOfASegment-1),
    NextSeqIdA = MostOfASegment,
    SeqIdsB = lists:seq(MostOfASegment, 2*MostOfASegment),
    NextSeqIdB = 2 * MostOfASegment + 1,
    SeqIdsC = lists:seq(0, trunc(SegmentSize/2)),
    SeqIdsD = lists:seq(0, SegmentSize*4),

    VerifyReadWithPublishedFun = fun verify_read_with_published_v2/3,

    with_empty_test_queue(
      fun (Qi0, QName) ->
              {0, 0, Qi1} = IndexMod:bounds(Qi0, undefined),
              {Qi2, SeqIdsMsgIdsA} = queue_index_publish(SeqIdsA, false, Qi1),
              {0, SegmentSize, Qi3} = IndexMod:bounds(Qi2, NextSeqIdA),
              {ReadA, Qi4} = IndexMod:read(0, SegmentSize, Qi3),
              ok = VerifyReadWithPublishedFun(false, ReadA,
                                              lists:reverse(SeqIdsMsgIdsA)),
              %% should get length back as 0, as all the msgs were transient
              {0, 0, Qi6} = restart_test_queue(Qi4, QName),
              {NextSeqIdA, NextSeqIdA, Qi7} = IndexMod:bounds(Qi6, NextSeqIdA),
              {Qi8, SeqIdsMsgIdsB} = queue_index_publish(SeqIdsB, true, Qi7),
              {0, TwoSegs, Qi9} = IndexMod:bounds(Qi8, NextSeqIdB),
              {ReadB, Qi10} = IndexMod:read(0, SegmentSize, Qi9),
              ok = VerifyReadWithPublishedFun(true, ReadB,
                                              lists:reverse(SeqIdsMsgIdsB)),
              %% should get length back as MostOfASegment
              LenB = length(SeqIdsB),
              BytesB = LenB * 10,
              {LenB, BytesB, Qi12} = restart_test_queue(Qi10, QName),
              {0, TwoSegs, Qi13} = IndexMod:bounds(Qi12, NextSeqIdB),
              Qi15 = case IndexMod of
                  rabbit_queue_index ->
                      Qi14 = IndexMod:deliver(SeqIdsB, Qi13),
                      {ReadC, Qi14b} = IndexMod:read(0, SegmentSize, Qi14),
                      ok = VerifyReadWithPublishedFun(true, ReadC,
                                                      lists:reverse(SeqIdsMsgIdsB)),
                      Qi14b;
                  _ ->
                      Qi13
              end,
              {_DeletedSegments, Qi16} = IndexMod:ack(SeqIdsB, Qi15),
              {_Confirms, Qi17} = IndexMod:sync(Qi16),
              %% Everything will have gone now because #pubs == #acks
              {NextSeqIdB, NextSeqIdB, Qi18} = IndexMod:bounds(Qi17, NextSeqIdB),
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
              Qi2 = case IndexMod of
                  rabbit_queue_index -> IndexMod:deliver(SeqIdsC, Qi1);
                  _ -> Qi1
              end,
              {_DeletedSegments, Qi3} = IndexMod:ack(SeqIdsC, Qi2),
              {_Confirms, Qi4} = IndexMod:sync(Qi3),
              {Qi5, _SeqIdsMsgIdsC1} = queue_index_publish([SegmentSize],
                                                           false, Qi4),
              Qi5
      end),

    %% b) partial pub+del, then move to new segment, then ack all in old segment
    with_empty_test_queue(
      fun (Qi0, _QName) ->
              {Qi1, _SeqIdsMsgIdsC2} = queue_index_publish(SeqIdsC,
                                                           false, Qi0),
              Qi2 = case IndexMod of
                  rabbit_queue_index -> IndexMod:deliver(SeqIdsC, Qi1);
                  _ -> Qi1
              end,
              {Qi3, _SeqIdsMsgIdsC3} = queue_index_publish([SegmentSize],
                                                           false, Qi2),
              {_DeletedSegments, Qi4} = IndexMod:ack(SeqIdsC, Qi3),
              {_Confirms, Qi5} = IndexMod:sync(Qi4),
              Qi5
      end),

    %% c) just fill up several segments of all pubs, then +acks
    with_empty_test_queue(
      fun (Qi0, _QName) ->
              {Qi1, _SeqIdsMsgIdsD} = queue_index_publish(SeqIdsD,
                                                          false, Qi0),
              Qi2 = case IndexMod of
                  rabbit_queue_index -> IndexMod:deliver(SeqIdsD, Qi1);
                  _ -> Qi1
              end,
              {_DeletedSegments, Qi3} = IndexMod:ack(SeqIdsD, Qi2),
              {_Confirms, Qi4} = IndexMod:sync(Qi3),
              Qi4
      end),

    %% d) get messages in all states to a segment, then flush, then do
    %% the same again, don't flush and read.
    with_empty_test_queue(
      fun (Qi0, _QName) ->
              {Qi1, [Seven,Five,Four|_]} = queue_index_publish([0,1,2,4,5,7],
                                                               false, Qi0),
              Qi2 = case IndexMod of
                  rabbit_queue_index -> IndexMod:deliver([0,1,4], Qi1);
                  _ -> Qi1
              end,
              {_DeletedSegments3, Qi3} = IndexMod:ack([0], Qi2),
              {_Confirms, Qi4} = IndexMod:sync(Qi3),
              {Qi5, [Eight,Six|_]} = queue_index_publish([3,6,8], false, Qi4),
              Qi6 = case IndexMod of
                  rabbit_queue_index -> IndexMod:deliver([2,3,5,6], Qi5);
                  _ -> Qi5
              end,
              {_DeletedSegments7, Qi7} = IndexMod:ack([1,2,3], Qi6),
              {[], Qi8} = IndexMod:read(0, 4, Qi7),
              {ReadD, Qi9} = IndexMod:read(4, 7, Qi8),
              ok = VerifyReadWithPublishedFun(false, ReadD,
                                              [Four, Five, Six]),
              {ReadE, Qi10} = IndexMod:read(7, 9, Qi9),
              ok = VerifyReadWithPublishedFun(false, ReadE,
                                              [Seven, Eight]),
              Qi10
      end),

    %% e) as for (d), but use terminate instead of read.
    with_empty_test_queue(
      fun (Qi0, QName) ->
              {Qi1, _SeqIdsMsgIdsE} = queue_index_publish([0,1,2,4,5,7],
                                                          true, Qi0),
              Qi2 = case IndexMod of
                  rabbit_queue_index -> IndexMod:deliver([0,1,4], Qi1);
                  _ -> Qi1
              end,
              {_DeletedSegments3, Qi3} = IndexMod:ack([0], Qi2),
              {5, 50, Qi4} = restart_test_queue(Qi3, QName),
              {Qi5, _SeqIdsMsgIdsF} = queue_index_publish([3,6,8], true, Qi4),
              Qi6 = case IndexMod of
                  rabbit_queue_index -> IndexMod:deliver([2,3,5,6], Qi5);
                  _ -> Qi5
              end,
              {_DeletedSegments7, Qi7} = IndexMod:ack([1,2,3], Qi6),
              {5, 50, Qi8} = restart_test_queue(Qi7, QName),
              Qi8
      end),

    ok = rabbit_variable_queue:stop(?VHOST),
    {ok, _} = rabbit_variable_queue:start(?VHOST, []),

    passed.

%% The v2 index does not store the MsgId unless required.
%% We therefore do not check it.
verify_read_with_published_v2(_Persistent, [], _) ->
    ok;
verify_read_with_published_v2(Persistent,
                           [{_MsgId1, SeqId, _Location, _Props, Persistent}|Read],
                           [{SeqId, _MsgId2}|Published]) ->
    verify_read_with_published_v2(Persistent, Read, Published);
verify_read_with_published_v2(_Persistent, _Read, _Published) ->
    ko.

bq_queue_index_props(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, bq_queue_index_props1, [Config]).

bq_queue_index_props1(_Config) ->
    IndexMod = index_mod(),

    with_empty_test_queue(
      fun(Qi0, _QName) ->
              MsgId = rabbit_guid:gen(),
              Props = #message_properties{expiry=12345, size = 10},
              Qi1 = IndexMod:publish(
                      MsgId, 0, memory, Props, true, true, Qi0),
              {[{MsgId, 0, _, Props, _}], Qi2} =
                  IndexMod:read(0, 1, Qi1),
              Qi2
      end),

    ok = rabbit_variable_queue:stop(?VHOST),
    {ok, _} = rabbit_variable_queue:start(?VHOST, []),

    passed.

v2_delete_segment_file_completely_acked(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, v2_delete_segment_file_completely_acked1, [Config]).

v2_delete_segment_file_completely_acked1(_Config) ->
    IndexMod = rabbit_classic_queue_index_v2,
    SegmentSize = segment_entry_count(),
    SeqIds = lists:seq(0, SegmentSize - 1),

    with_empty_test_queue(
      fun (Qi0, _QName) ->
              %% Publish a full segment file.
              {Qi1, SeqIdsMsgIds} = queue_index_publish(SeqIds, true, Qi0),
              SegmentSize = length(SeqIdsMsgIds),
              {0, SegmentSize, Qi2} = IndexMod:bounds(Qi1, undefined),
              %% Confirm that the file exists on disk.
              Path = IndexMod:segment_file(0, Qi2),
              true = filelib:is_file(Path),
              %% Ack the full segment file.
              {[0], Qi3} = IndexMod:ack(SeqIds, Qi2),
              %% Confirm that the file was deleted.
              false = filelib:is_file(Path),
              Qi3
      end),

    passed.

v2_delete_segment_file_partially_acked(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, v2_delete_segment_file_partially_acked1, [Config]).

v2_delete_segment_file_partially_acked1(_Config) ->
    IndexMod = rabbit_classic_queue_index_v2,
    SegmentSize = segment_entry_count(),
    SeqIds = lists:seq(0, SegmentSize div 2),
    SeqIdsLen = length(SeqIds),

    with_empty_test_queue(
      fun (Qi0, _QName) ->
              %% Publish a partial segment file.
              {Qi1, SeqIdsMsgIds} = queue_index_publish(SeqIds, true, Qi0),
              SeqIdsLen = length(SeqIdsMsgIds),
              {0, SegmentSize, Qi2} = IndexMod:bounds(Qi1, undefined),
              %% Confirm that the file exists on disk.
              Path = IndexMod:segment_file(0, Qi2),
              true = filelib:is_file(Path),
              %% Ack the partial segment file.
              {[0], Qi3} = IndexMod:ack(SeqIds, Qi2),
              %% Confirm that the file was deleted.
              false = filelib:is_file(Path),
              Qi3
      end),

    passed.

v2_delete_segment_file_partially_acked_with_holes(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, v2_delete_segment_file_partially_acked_with_holes1, [Config]).

v2_delete_segment_file_partially_acked_with_holes1(_Config) ->
    IndexMod = rabbit_classic_queue_index_v2,
    SegmentSize = segment_entry_count(),
    SeqIdsA = lists:seq(0, SegmentSize div 2),
    SeqIdsB = lists:seq(11 + SegmentSize div 2, SegmentSize - 1),
    SeqIdsLen = length(SeqIdsA) + length(SeqIdsB),

    with_empty_test_queue(
      fun (Qi0, _QName) ->
              %% Publish a partial segment file with holes.
              {Qi1, SeqIdsMsgIdsA} = queue_index_publish(SeqIdsA, true, Qi0),
              {Qi2, SeqIdsMsgIdsB} = queue_index_publish(SeqIdsB, true, Qi1),
              SeqIdsLen = length(SeqIdsMsgIdsA) + length(SeqIdsMsgIdsB),
              {0, SegmentSize, Qi3} = IndexMod:bounds(Qi2, undefined),
              %% Confirm that the file exists on disk.
              Path = IndexMod:segment_file(0, Qi3),
              true = filelib:is_file(Path),
              %% Ack the partial segment file with holes.
              {[], Qi4} = IndexMod:ack(SeqIdsA, Qi3),
              {[0], Qi5} = IndexMod:ack(SeqIdsB, Qi4),
              %% Confirm that the file was deleted.
              false = filelib:is_file(Path),
              Qi5
      end),

    passed.

v2_reset_state_no_slash_accumulation(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, v2_reset_state_no_slash_accumulation1, [Config]).

v2_reset_state_no_slash_accumulation1(_Config) ->
    IndexMod = rabbit_classic_queue_index_v2,
    with_empty_test_queue(
      fun (Qi0, _QName) ->
              %% Each reset_state call used to append an extra "/" to the dir
              %% path, eventually causing enametoolong after thousands of purges.
              Qi1 = lists:foldl(fun (_, Qi) -> IndexMod:reset_state(Qi) end,
                                Qi0, lists:seq(1, 100)),
              %% segment_file/2 uses the dir binary directly; "//" in the result
              %% indicates slash accumulation.
              SegPath = IndexMod:segment_file(0, Qi1),
              case binary:match(SegPath, <<"//">>) of
                  nomatch -> ok;
                  _ -> ct:fail("Slash accumulation in queue index dir: ~tp", [SegPath])
              end,
              Qi1
      end),
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
    Count = 2 * segment_entry_count(),
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
    after ?TIMEOUT -> exit(timeout_waiting_for_queue_death)
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
              ok = rabbit_amqqueue:internal_delete(Q1, <<"acting-user">>)
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

variable_queue_partial_segments_q_tail_thing(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, variable_queue_partial_segments_q_tail_thing1, []).

variable_queue_partial_segments_q_tail_thing1() ->
    with_fresh_variable_queue(fun variable_queue_partial_segments_q_tail_thing2/2).

variable_queue_partial_segments_q_tail_thing2(VQ0, _QName) ->
    SegmentSize = segment_entry_count(),
    HalfSegment = SegmentSize div 2,
    OneAndAHalfSegment = SegmentSize + HalfSegment,
    VQ1 = variable_queue_publish(true, OneAndAHalfSegment, VQ0),
    VQ2 = rabbit_variable_queue:update_rates(VQ1),
    VQ3 = check_variable_queue_status(
            VQ2,
            %% We only have one message in memory because the amount in memory
            %% depends on the consume rate, which is nil in this test.
            [{q_head, 1},
             {q_tail, {q_tail, 1, OneAndAHalfSegment - 1, OneAndAHalfSegment}},
             {len, OneAndAHalfSegment}]),
    VQ5 = check_variable_queue_status(
            variable_queue_publish(true, 1, VQ3),
            %% one alpha, but it's in the same segment as the q_tail
            %% @todo That's wrong now! v1/v2
            [{q_head, 1},
             {q_tail, {q_tail, 1, OneAndAHalfSegment, OneAndAHalfSegment + 1}},
             {len, OneAndAHalfSegment + 1}]),
    {VQ6, AckTags} = variable_queue_fetch(SegmentSize, true, false,
                                          SegmentSize + HalfSegment + 1, VQ5),
    VQ7 = check_variable_queue_status(
            VQ6,
            %% The length is the only predictible stat we have since
            %% the contents of q_head and q_tail depend on the rate.
            [{len, HalfSegment + 1}]),
    {VQ8, AckTags1} = variable_queue_fetch(HalfSegment + 1, true, false,
                                           HalfSegment + 1, VQ7),
    {_Guids, VQ9} = rabbit_variable_queue:ack(AckTags ++ AckTags1, VQ8),
    %% should be empty now
    {empty, VQ10} = rabbit_variable_queue:fetch(true, VQ9),
    VQ10.

variable_queue_all_the_bits_not_covered_elsewhere_A(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, variable_queue_all_the_bits_not_covered_elsewhere_A1, []).

variable_queue_all_the_bits_not_covered_elsewhere_A1() ->
    with_fresh_variable_queue(fun variable_queue_all_the_bits_not_covered_elsewhere_A2/2).

variable_queue_all_the_bits_not_covered_elsewhere_A2(VQ0, QName) ->
    Count = 2 * segment_entry_count(),
    VQ1 = variable_queue_publish(true, Count, VQ0),
    VQ2 = variable_queue_publish(false, Count, VQ1),
    {VQ4, _AckTags}  = variable_queue_fetch(Count, true, false,
                                            Count + Count, VQ2),
    {VQ5, _AckTags1} = variable_queue_fetch(Count, false, false,
                                            Count, VQ4),
    _VQ6 = rabbit_variable_queue:terminate(shutdown, VQ5),
    VQ7 = variable_queue_init(test_amqqueue(QName, true), true),
    {{_Msg1, true, _AckTag1}, VQ8} = rabbit_variable_queue:fetch(true, VQ7),
    Count1 = rabbit_variable_queue:len(VQ8),
    VQ9 = variable_queue_publish(false, 1, VQ8),
    {VQ11, _AckTags2} = variable_queue_fetch(Count1, true, true, Count, VQ9),
    {VQ12, _AckTags3} = variable_queue_fetch(1, false, false, 1, VQ11),
    VQ12.

variable_queue_all_the_bits_not_covered_elsewhere_B(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, variable_queue_all_the_bits_not_covered_elsewhere_B1, []).

variable_queue_all_the_bits_not_covered_elsewhere_B1() ->
    with_fresh_variable_queue(fun variable_queue_all_the_bits_not_covered_elsewhere_B2/2).

variable_queue_all_the_bits_not_covered_elsewhere_B2(VQ1, QName) ->
    VQ2 = variable_queue_publish(false, 4, VQ1),
    {VQ3, AckTags} = variable_queue_fetch(2, false, false, 4, VQ2),
    {_Guids, VQ4} =
        rabbit_variable_queue:requeue(AckTags, true, VQ3),
    VQ5 = rabbit_variable_queue:timeout(VQ4),
    _VQ6 = rabbit_variable_queue:terminate(shutdown, VQ5),
    VQ7 = variable_queue_init(test_amqqueue(QName, true), true),
    {empty, VQ8} = rabbit_variable_queue:fetch(false, VQ7),
    VQ8.

variable_queue_drop(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, variable_queue_drop1, []).

variable_queue_drop1() ->
    with_fresh_variable_queue(fun variable_queue_drop2/2).

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
    {[MsgId], VQ4} = rabbit_variable_queue:requeue([AckTag], true, VQ3),
    %% drop message with AckRequired = false
    {{MsgId, undefined}, VQ5} = rabbit_variable_queue:drop(false, VQ4),
    true = rabbit_variable_queue:is_empty(VQ5),
    VQ5.

variable_queue_fold_msg_on_disk(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, variable_queue_fold_msg_on_disk1, []).

variable_queue_fold_msg_on_disk1() ->
    with_fresh_variable_queue(fun variable_queue_fold_msg_on_disk2/2).

variable_queue_fold_msg_on_disk2(VQ0, _QName) ->
    VQ1 = variable_queue_publish(true, 1, VQ0),
    {VQ2, AckTags} = variable_queue_fetch(1, true, false, 1, VQ1),
    {ok, VQ3} = rabbit_variable_queue:ackfold(fun (_M, _A, ok) -> ok end,
                                              ok, VQ2, AckTags, true),
    VQ3.

variable_queue_dropfetchwhile(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, variable_queue_dropfetchwhile1, []).

variable_queue_dropfetchwhile1() ->
    with_fresh_variable_queue(fun variable_queue_dropfetchwhile2/2).

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
    {_MsgIds, VQ3} = rabbit_variable_queue:requeue(AckTags, true, VQ2),

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
      ?MODULE, variable_queue_dropwhile_restart1, []).

variable_queue_dropwhile_restart1() ->
    with_fresh_variable_queue(fun variable_queue_dropwhile_restart2/2).

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
      ?MODULE, variable_queue_dropwhile_sync_restart1, []).

variable_queue_dropwhile_sync_restart1() ->
    with_fresh_variable_queue(fun variable_queue_dropwhile_sync_restart2/2).

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

variable_queue_restart_large_seq_id(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, variable_queue_restart_large_seq_id1, []).

variable_queue_restart_large_seq_id1() ->
    with_fresh_variable_queue(fun variable_queue_restart_large_seq_id2/2).

variable_queue_restart_large_seq_id2(VQ0, QName) ->
    Count = 1,

    %% publish and consume a message
    VQ1 = publish_fetch_and_ack(Count, 0, VQ0),
    %% should be empty now
    true = rabbit_variable_queue:is_empty(VQ1),

    _VQ2 = rabbit_variable_queue:terminate(shutdown, VQ1),
    Terms = variable_queue_read_terms(QName),
    Count = proplists:get_value(next_seq_id, Terms),

    %% set a very high next_seq_id as if 100 billion messages have been
    %% published and consumed
    Terms2 = lists:keyreplace(next_seq_id, 1, Terms, {next_seq_id, 100_000_000_000}),

    {TInit, VQ3} =
        timer:tc(
          fun() -> variable_queue_init(test_amqqueue(QName, true), Terms2) end,
          millisecond),
    %% even with a very high next_seq_id start of an empty queue
    %% should be quick (few milliseconds, but let's give it 500ms, to
    %% avoid flaking on slow servers)
    {true, _} = {TInit < 500, TInit},

    %% should be empty now
    true = rabbit_variable_queue:is_empty(VQ3),

    VQ3.

variable_queue_ack_limiting(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, variable_queue_ack_limiting1, []).

variable_queue_ack_limiting1() ->
    with_fresh_variable_queue(fun variable_queue_ack_limiting2/2).

variable_queue_ack_limiting2(VQ0, _Config) ->
    %% start by sending in a bunch of messages
    Len = 1024,
    VQ1 = variable_queue_publish(false, Len, VQ0),

    %% squeeze and relax queue
    Churn = Len div 32,
    VQ2 = publish_fetch_and_ack(Churn, Len, VQ1),

    %% update stats
    VQ3 = rabbit_variable_queue:update_rates(VQ2),

    %% fetch half the messages
    {VQ4, _AckTags} = variable_queue_fetch(Len div 2, false, false, Len, VQ3),

    %% We only check the length anymore because
    %% that's the only predictable stats we got.
    VQ5 = check_variable_queue_status(VQ4, [{len, Len div 2}]),

    VQ5.

variable_queue_purge(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, variable_queue_purge1, []).

variable_queue_purge1() ->
    with_fresh_variable_queue(fun variable_queue_purge2/2).

variable_queue_purge2(VQ0, _Config) ->
    LenDepth = fun (VQ) ->
                       {rabbit_variable_queue:len(VQ),
                        rabbit_variable_queue:depth(VQ)}
               end,
    VQ1         = variable_queue_publish(false, 10, VQ0),
    {VQ2, Acks} = variable_queue_fetch(6, false, false, 10, VQ1),
    {4, VQ3}    = rabbit_variable_queue:purge(VQ2),
    {0, 6}      = LenDepth(VQ3),
    {_, VQ4}    = rabbit_variable_queue:requeue(lists:sublist(Acks, 2), true, VQ3),
    {2, 6}      = LenDepth(VQ4),
    VQ5         = rabbit_variable_queue:purge_acks(VQ4),
    {2, 2}      = LenDepth(VQ5),
    VQ5.

variable_queue_requeue(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, variable_queue_requeue1, []).

variable_queue_requeue1() ->
    with_fresh_variable_queue(fun variable_queue_requeue2/2).

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

%% requeue from ram_pending_ack into q_head, move to q_tail and then empty queue
variable_queue_requeue_ram_beta(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, variable_queue_requeue_ram_beta1, []).

variable_queue_requeue_ram_beta1() ->
    with_fresh_variable_queue(fun variable_queue_requeue_ram_beta2/2).

variable_queue_requeue_ram_beta2(VQ0, _Config) ->
    Count = 2 + 2 * segment_entry_count(),
    VQ1 = variable_queue_publish(false, Count, VQ0),
    {VQ2, AcksR} = variable_queue_fetch(Count, false, false, Count, VQ1),
    {Back, Front} = lists:split(Count div 2, AcksR),
    {_, VQ3} = rabbit_variable_queue:requeue(erlang:tl(Back), true, VQ2),
    {_, VQ5} = rabbit_variable_queue:requeue([erlang:hd(Back)], true, VQ3),
    VQ6 = requeue_one_by_one(Front, VQ5),
    {VQ7, AcksAll} = variable_queue_fetch(Count, false, true, Count, VQ6),
    {_, VQ8} = rabbit_variable_queue:ack(AcksAll, VQ7),
    VQ8.

pub_res({_, VQS}) ->
    VQS;
pub_res(VQS) ->
    VQS.

make_publish(IsPersistent, PayloadFun, PropFun, N) ->
    {message(IsPersistent, PayloadFun, N),
     PropFun(N, #message_properties{size = 10}),
     false}.

make_publish_delivered(IsPersistent, PayloadFun, PropFun, N) ->
    {message(IsPersistent, PayloadFun, N),
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
    IndexMod = index_mod(),
    Res = IndexMod:recover(
            QName, [], false,
            fun (MsgId) ->
                    rabbit_msg_store:contains(MsgId, PersistentClient)
            end),
    ok = rabbit_msg_store:client_delete_and_terminate(PersistentClient),
    Res.

restart_test_queue(Qi, QName) ->
    IndexMod = index_mod(),
    _ = IndexMod:terminate(?VHOST, [], Qi),
    ok = rabbit_variable_queue:stop(?VHOST),
    {ok, _} = rabbit_variable_queue:start(?VHOST, [QName]),
    init_test_queue(QName).

empty_test_queue(QName) ->
    ok = rabbit_variable_queue:stop(?VHOST),
    {ok, _} = rabbit_variable_queue:start(?VHOST, []),
    {0, 0, Qi} = init_test_queue(QName),
    IndexMod = index_mod(),
    _ = IndexMod:delete_and_terminate(Qi),
    ok.

unin_empty_test_queue(QName) ->
    {0, 0, Qi} = init_test_queue(QName),
    IndexMod = index_mod(),
    _ = IndexMod:delete_and_terminate(Qi),
    ok.

with_empty_test_queue(Fun) ->
    QName = test_queue(),
    ok = empty_test_queue(QName),
    {0, 0, Qi} = init_test_queue(QName),
    IndexMod = index_mod(),
    IndexMod:delete_and_terminate(Fun(Qi, QName)).

restart_app() ->
    rabbit:stop(),
    rabbit:start().

queue_index_publish(SeqIds, Persistent, Qi) ->
    IndexMod = index_mod(),
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
                  QiM = IndexMod:publish(
                          MsgId, SeqId, rabbit_msg_store,
                          #message_properties{size = 10},
                          Persistent, true, QiN),
                  ok = rabbit_msg_store:write(SeqId, MsgId, MsgId, MSCState),
                  {QiM, [{SeqId, MsgId} | SeqIdsMsgIdsAcc]}
          end, {Qi, []}, SeqIds),
    %% do this just to force all of the publishes through to the msg_store:
    true = rabbit_msg_store:contains(LastMsgIdWritten, MSCState),
    ok = rabbit_msg_store:client_delete_and_terminate(MSCState),
    {A, B}.

msg_store_client_init(MsgStore, Ref) ->
    rabbit_vhost_msg_store:client_init(?VHOST, MsgStore, Ref,  undefined).

variable_queue_init(Q, Recover) ->
    rabbit_variable_queue:init(
      Q, case Recover of
             true  -> non_clean_shutdown;
             false -> new;
             Terms -> Terms
         end, fun(_, _) -> ok end).

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
              BMsg = rabbit_basic:message(rabbit_misc:r(<<>>, exchange, <<>>),
                                         <<>>, #'P_basic'{delivery_mode = 2},
                                         Payload),
              Content = BMsg#basic_message.content,
              Ex = BMsg#basic_message.exchange_name,
              {ok, Msg} = mc_amqpl:message(Ex, <<>>, Content),
              Options = #{correlation => Seq},
              {ok, Acc, _Actions} = rabbit_queue_type:deliver([Q], Msg,
                                                              Options, Acc0),
              Acc
      end, QTState0, Seqs),
    wait_for_confirms(sets:from_list(Seqs, [{version, 2}])),
    QTState.

wait_for_confirms(Unconfirmed) ->
    case sets:is_empty(Unconfirmed) of
        true  -> ok;
        false ->
            receive
                {'$gen_cast', {queue_event, _QName, {confirm, Confirmed, _}}} ->
                    wait_for_confirms(
                      sets:subtract(
                        Unconfirmed, sets:from_list(Confirmed, [{version, 2}])))
            after ?TIMEOUT ->
                      flush(),
                      exit(timeout_waiting_for_confirm)
            end
    end.

with_fresh_variable_queue(Fun) ->
    Ref = make_ref(),
    Me = self(),
    %% Run in a separate process since rabbit_msg_store will send
    %% bump_credit messages and we want to ignore them
    spawn_link(fun() ->
                       QName = test_queue(),
                       ok = unin_empty_test_queue(QName),
                       VQ = variable_queue_init(test_amqqueue(QName, true), false),
                       S0 = variable_queue_status(VQ),
                       assert_props(S0, [{q_head, 0},
                                         {q_tail, {q_tail, undefined, 0, undefined}},
                                         {len, 0}]),
                       try
                           _ = rabbit_variable_queue:delete_and_terminate(
                                 shutdown, Fun(VQ, QName)),
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

variable_queue_publish(IsPersistent, Count, VQ) ->
    variable_queue_publish(IsPersistent, Count, fun (_N, P) -> P end, VQ).

variable_queue_publish(IsPersistent, Count, PropFun, VQ) ->
    variable_queue_publish(IsPersistent, 1, Count, PropFun,
                           fun (_N) -> <<>> end, VQ).

variable_queue_publish(IsPersistent, Start, Count, PropFun, PayloadFun, VQ) ->
    variable_queue_wait_for_shuffling_end(
      lists:foldl(
        fun (N, VQN) ->
                Msg = message(IsPersistent, PayloadFun, N),
                rabbit_variable_queue:publish(
                  Msg,
                  PropFun(N, #message_properties{size = 10}),
                  false, self(), VQN)
        end, VQ, lists:seq(Start, Start + Count - 1))).

variable_queue_fetch(Count, IsPersistent, IsDelivered, Len, VQ) ->
    lists:foldl(fun (N, {VQN, AckTagsAcc}) ->
                        Rem = Len - N,
                        {{Msg, IsDelivered, AckTagN}, VQM} =
                            rabbit_variable_queue:fetch(true, VQN),
                        IsPersistent = mc:is_persistent(Msg),
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
    Res = [assert_prop(List, Prop, Value) || {Prop, Value} <- PropVals],
    case lists:usort(Res) of
        [ok] -> ok;
        Error -> error(Error -- [ok])
    end.

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
    binary_to_term(list_to_binary(lists:reverse(P)));
msg2int(Msg) ->
    #content{payload_fragments_rev = P} = mc:protocol_state(Msg),
    binary_to_term(list_to_binary(lists:reverse(P))).

ack_subset(AckSeqs, Interval, Rem) ->
    lists:filter(fun ({_Ack, N}) -> (N + Rem) rem Interval == 0 end, AckSeqs).

requeue_one_by_one(Acks, VQ) ->
    lists:foldl(fun (AckTag, VQN) ->
                        {_MsgId, VQM} = rabbit_variable_queue:requeue(
                                          [AckTag], true, VQN),
                        VQM
                end, VQ, Acks).

%% Historical test case that exercised the many different
%% internal queues. Kept for completeness.
variable_queue_with_holes(VQ0) ->
    Interval = 2048, %% should match vq:IO_BATCH_SIZE
    Count = 2 * Interval + 2 * segment_entry_count(),
    Seq = lists:seq(1, Count),
    VQ1 = variable_queue_publish(
            false, 1, Count,
            fun (_, P) -> P end, fun erlang:term_to_binary/1, VQ0),
    {VQ3, AcksR} = variable_queue_fetch(Count, false, false, Count, VQ1),
    Acks = lists:reverse(AcksR),
    AckSeqs = lists:zip(Acks, Seq),
    [{Subset1, _Seq1}, {Subset2, _Seq2}, {Subset3, Seq3}] =
        [lists:unzip(ack_subset(AckSeqs, Interval, I)) || I <- [0, 1, 2]],
    %% we requeue in three phases in order to exercise requeuing logic
    %% in various vq states
    {_MsgIds, VQ4} = rabbit_variable_queue:requeue(
                       Acks -- (Subset1 ++ Subset2 ++ Subset3), true, VQ3),
    VQ5 = requeue_one_by_one(Subset1, VQ4),
    %% by now we have some messages (and holes) in q_tail
    VQ6 = requeue_one_by_one(Subset2, VQ5),
    %% add the q1 tail
    VQ8 = variable_queue_publish(
            true, Count + 1, Interval,
            fun (_, P) -> P end, fun erlang:term_to_binary/1, VQ6),
    %% assertions
    vq_with_holes_assertions(VQ8),
    Depth = Count + Interval,
    Depth = rabbit_variable_queue:depth(VQ8),
    Len = Depth - length(Subset3),
    Len = rabbit_variable_queue:len(VQ8),

    {Seq3, Seq -- Seq3, lists:seq(Count + 1, Count + Interval), VQ8}.

vq_with_holes_assertions(VQ) ->
    [false =
         case V of
             {q_tail, _, 0, _} -> true;
             0                 -> true;
             _                 -> false
         end || {K, V} <- variable_queue_status(VQ),
                lists:member(K, [q_head, q_tail])].

check_variable_queue_status(VQ0, Props) ->
    VQ1 = variable_queue_wait_for_shuffling_end(VQ0),
    S = variable_queue_status(VQ1),
    assert_props(S, Props),
    VQ1.

flush() ->
    receive
        Any ->
            ct:pal("flush ~tp", [Any]),
            flush()
    after 0 ->
              ok
    end.

message(IsPersistent, PayloadFun, N) ->
    #basic_message{content = Content,
                   exchange_name = Ex,
                   id = Id} =
        rabbit_basic:message(rabbit_misc:r(<<>>, exchange, <<>>),
                             <<>>, #'P_basic'{delivery_mode = case IsPersistent of
                                                                  true  -> 2;
                                                                  false -> 1
                                                              end},
                             PayloadFun(N)),
        {ok, Msg} = mc_amqpl:message(Ex, <<>>, Content, #{id => Id}),
        Msg.
