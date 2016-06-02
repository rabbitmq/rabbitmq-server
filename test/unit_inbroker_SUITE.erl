%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2011-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(unit_inbroker_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

-define(PERSISTENT_MSG_STORE, msg_store_persistent_vhost).
-define(TRANSIENT_MSG_STORE,  msg_store_transient_vhost).

-define(TIMEOUT_LIST_OPS_PASS, 5000).
-define(TIMEOUT, 30000).

-define(CLEANUP_QUEUE_NAME, <<"cleanup-queue">>).

-define(VARIABLE_QUEUE_TESTCASES, [
    variable_queue_dynamic_duration_change,
    variable_queue_partial_segments_delta_thing,
    variable_queue_all_the_bits_not_covered_elsewhere_A,
    variable_queue_all_the_bits_not_covered_elsewhere_B,
    variable_queue_drop,
    variable_queue_fold_msg_on_disk,
    variable_queue_dropfetchwhile,
    variable_queue_dropwhile_varying_ram_duration,
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
    {variable_queue_default, [], ?VARIABLE_QUEUE_TESTCASES},
    {variable_queue_lazy, [], ?VARIABLE_QUEUE_TESTCASES ++
                              [variable_queue_mode_change]},
    bq_variable_queue_delete_msg_store_files_callback,
    bq_queue_recover
  ]).

-define(CLUSTER_TESTCASES, [
    delegates_async,
    delegates_sync,
    queue_cleanup,
    declare_on_dead_queue,
    refresh_events
  ]).

all() ->
    [
      {group, parallel_tests},
      {group, non_parallel_tests},
      {group, backing_queue_tests},
      {group, cluster_tests},

      {group, disconnect_detected_during_alarm},
      {group, list_consumers_sanity_check},
      {group, list_queues_online_and_offline}
    ].

groups() ->
    [
      {parallel_tests, [parallel], [
          amqp_connection_refusal,
          configurable_server_properties,
          confirms,
          credit_flow_settings,
          dynamic_mirroring,
          gen_server2_with_state,
          list_operations_timeout_pass,
          mcall,
          {password_hashing, [], [
              password_hashing,
              change_password
            ]},
          {policy_validation, [parallel, {repeat, 20}], [
              ha_policy_validation,
              policy_validation,
              policy_opts_validation,
              queue_master_location_policy_validation,
              queue_modes_policy_validation,
              vhost_removed_while_updating_policy
            ]},
          runtime_parameters,
          set_disk_free_limit_command,
          topic_matching,
          user_management
        ]},
      {non_parallel_tests, [], [
          app_management, %% Restart RabbitMQ.
          channel_statistics, %% Expect specific statistics.
          disk_monitor, %% Replace rabbit_misc module.
          file_handle_cache, %% Change FHC limit.
          head_message_timestamp_statistics, %% Expect specific statistics.
          log_management, %% Check log files.
          log_management_during_startup, %% Check log files.
          memory_high_watermark, %% Trigger alarm.
          externally_rotated_logs_are_automatically_reopened, %% Check log files.
          server_status %% Trigger alarm.
        ]},
      {backing_queue_tests, [], [
          msg_store,
          {backing_queue_embed_limit_0, [], ?BACKING_QUEUE_TESTCASES},
          {backing_queue_embed_limit_1024, [], ?BACKING_QUEUE_TESTCASES}
        ]},
      {cluster_tests, [], [
          {from_cluster_node1, [], ?CLUSTER_TESTCASES},
          {from_cluster_node2, [], ?CLUSTER_TESTCASES}
        ]},

      %% Test previously executed with the multi-node target.
      {disconnect_detected_during_alarm, [], [
          disconnect_detected_during_alarm %% Trigger alarm.
        ]},
      {list_consumers_sanity_check, [], [
          list_consumers_sanity_check
        ]},
      {list_queues_online_and_offline, [], [
          list_queues_online_and_offline %% Stop node B.
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
            ClusterSize = case Group of
                disconnect_detected_during_alarm -> 1;
                list_consumers_sanity_check      -> 1;
                _                                -> 2
            end,
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

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Application management.
%% -------------------------------------------------------------------

app_management(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, app_management1, [Config]).

app_management1(_Config) ->
    control_action(wait, [os:getenv("RABBITMQ_PID_FILE")]),
    %% Starting, stopping and diagnostics.  Note that we don't try
    %% 'report' when the rabbit app is stopped and that we enable
    %% tracing for the duration of this function.
    ok = control_action(trace_on, []),
    ok = control_action(stop_app, []),
    ok = control_action(stop_app, []),
    ok = control_action(status, []),
    ok = control_action(cluster_status, []),
    ok = control_action(environment, []),
    ok = control_action(start_app, []),
    ok = control_action(start_app, []),
    ok = control_action(status, []),
    ok = control_action(report, []),
    ok = control_action(cluster_status, []),
    ok = control_action(environment, []),
    ok = control_action(trace_off, []),
    passed.

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
    ok = rabbit_variable_queue:stop_msg_store(),
    ok = rabbit_variable_queue:start_msg_store(
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
    ok = rabbit_variable_queue:stop_msg_store(),
    ok = rabbit_variable_queue:start_msg_store(
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
    {Pid, rabbit_msg_store:client_init(
            MsgStore, Ref, fun (MsgIds, _ActionTaken) ->
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
    MSCState = rabbit_msg_store:client_init(
                 ?PERSISTENT_MSG_STORE, Ref,
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
    SegmentSize = rabbit_queue_index:next_segment_boundary(0),
    TwoSegs = SegmentSize + SegmentSize,
    MostOfASegment = trunc(SegmentSize*0.75),
    SeqIdsA = lists:seq(0, MostOfASegment-1),
    SeqIdsB = lists:seq(MostOfASegment, 2*MostOfASegment),
    SeqIdsC = lists:seq(0, trunc(SegmentSize/2)),
    SeqIdsD = lists:seq(0, SegmentSize*4),

    with_empty_test_queue(
      fun (Qi0) ->
              {0, 0, Qi1} = rabbit_queue_index:bounds(Qi0),
              {Qi2, SeqIdsMsgIdsA} = queue_index_publish(SeqIdsA, false, Qi1),
              {0, SegmentSize, Qi3} = rabbit_queue_index:bounds(Qi2),
              {ReadA, Qi4} = rabbit_queue_index:read(0, SegmentSize, Qi3),
              ok = verify_read_with_published(false, false, ReadA,
                                              lists:reverse(SeqIdsMsgIdsA)),
              %% should get length back as 0, as all the msgs were transient
              {0, 0, Qi6} = restart_test_queue(Qi4),
              {0, 0, Qi7} = rabbit_queue_index:bounds(Qi6),
              {Qi8, SeqIdsMsgIdsB} = queue_index_publish(SeqIdsB, true, Qi7),
              {0, TwoSegs, Qi9} = rabbit_queue_index:bounds(Qi8),
              {ReadB, Qi10} = rabbit_queue_index:read(0, SegmentSize, Qi9),
              ok = verify_read_with_published(false, true, ReadB,
                                              lists:reverse(SeqIdsMsgIdsB)),
              %% should get length back as MostOfASegment
              LenB = length(SeqIdsB),
              BytesB = LenB * 10,
              {LenB, BytesB, Qi12} = restart_test_queue(Qi10),
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
              {0, 0, Qi19} = restart_test_queue(Qi18),
              Qi19
      end),

    %% These next bits are just to hit the auto deletion of segment files.
    %% First, partials:
    %% a) partial pub+del+ack, then move to new segment
    with_empty_test_queue(
      fun (Qi0) ->
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
      fun (Qi0) ->
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
      fun (Qi0) ->
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
      fun (Qi0) ->
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
      fun (Qi0) ->
              {Qi1, _SeqIdsMsgIdsE} = queue_index_publish([0,1,2,4,5,7],
                                                          true, Qi0),
              Qi2 = rabbit_queue_index:deliver([0,1,4], Qi1),
              Qi3 = rabbit_queue_index:ack([0], Qi2),
              {5, 50, Qi4} = restart_test_queue(Qi3),
              {Qi5, _SeqIdsMsgIdsF} = queue_index_publish([3,6,8], true, Qi4),
              Qi6 = rabbit_queue_index:deliver([2,3,5,6], Qi5),
              Qi7 = rabbit_queue_index:ack([1,2,3], Qi6),
              {5, 50, Qi8} = restart_test_queue(Qi7),
              Qi8
      end),

    ok = rabbit_variable_queue:stop(),
    {ok, _} = rabbit_variable_queue:start([]),

    passed.

bq_queue_index_props(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, bq_queue_index_props1, [Config]).

bq_queue_index_props1(_Config) ->
    with_empty_test_queue(
      fun(Qi0) ->
              MsgId = rabbit_guid:gen(),
              Props = #message_properties{expiry=12345, size = 10},
              Qi1 = rabbit_queue_index:publish(
                      MsgId, 1, Props, true, infinity, Qi0),
              {[{MsgId, 1, Props, _, _}], Qi2} =
                  rabbit_queue_index:read(1, 2, Qi1),
              Qi2
      end),

    ok = rabbit_variable_queue:stop(),
    {ok, _} = rabbit_variable_queue:start([]),

    passed.

bq_variable_queue_delete_msg_store_files_callback(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, bq_variable_queue_delete_msg_store_files_callback1, [Config]).

bq_variable_queue_delete_msg_store_files_callback1(Config) ->
    ok = restart_msg_store_empty(),
    {new, #amqqueue { pid = QPid, name = QName } = Q} =
      rabbit_amqqueue:declare(
        queue_name(Config,
          <<"bq_variable_queue_delete_msg_store_files_callback-q">>),
        true, false, [], none),
    Payload = <<0:8388608>>, %% 1MB
    Count = 30,
    publish_and_confirm(Q, Payload, Count),

    rabbit_amqqueue:set_ram_duration_target(QPid, 0),

    {ok, Limiter} = rabbit_limiter:start_link(no_id),

    CountMinusOne = Count - 1,
    {ok, CountMinusOne, {QName, QPid, _AckTag, false, _Msg}} =
        rabbit_amqqueue:basic_get(Q, self(), true, Limiter),
    {ok, CountMinusOne} = rabbit_amqqueue:purge(Q),

    %% give the queue a second to receive the close_fds callback msg
    timer:sleep(1000),

    rabbit_amqqueue:delete(Q, false, false),
    passed.

bq_queue_recover(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, bq_queue_recover1, [Config]).

bq_queue_recover1(Config) ->
    Count = 2 * rabbit_queue_index:next_segment_boundary(0),
    {new, #amqqueue { pid = QPid, name = QName } = Q} =
        rabbit_amqqueue:declare(queue_name(Config, <<"bq_queue_recover-q">>),
                                true, false, [], none),
    publish_and_confirm(Q, <<>>, Count),

    SupPid = rabbit_ct_broker_helpers:get_queue_sup_pid(QPid),
    true = is_pid(SupPid),
    exit(SupPid, kill),
    exit(QPid, kill),
    MRef = erlang:monitor(process, QPid),
    receive {'DOWN', MRef, process, QPid, _Info} -> ok
    after 10000 -> exit(timeout_waiting_for_queue_death)
    end,
    rabbit_amqqueue:stop(),
    rabbit_amqqueue:start(rabbit_amqqueue:recover()),
    {ok, Limiter} = rabbit_limiter:start_link(no_id),
    rabbit_amqqueue:with_or_die(
      QName,
      fun (Q1 = #amqqueue { pid = QPid1 }) ->
              CountMinusOne = Count - 1,
              {ok, CountMinusOne, {QName, QPid1, _AckTag, true, _Msg}} =
                  rabbit_amqqueue:basic_get(Q1, self(), false, Limiter),
              exit(QPid1, shutdown),
              VQ1 = variable_queue_init(Q, true),
              {{_Msg1, true, _AckTag1}, VQ2} =
                  rabbit_variable_queue:fetch(true, VQ1),
              CountMinusOne = rabbit_variable_queue:len(VQ2),
              _VQ3 = rabbit_variable_queue:delete_and_terminate(shutdown, VQ2),
              ok = rabbit_amqqueue:internal_delete(QName)
      end),
    passed.

variable_queue_dynamic_duration_change(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, variable_queue_dynamic_duration_change1, [Config]).

variable_queue_dynamic_duration_change1(Config) ->
    with_fresh_variable_queue(
      fun variable_queue_dynamic_duration_change2/1,
      ?config(variable_queue_type, Config)).

variable_queue_dynamic_duration_change2(VQ0) ->
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
      fun variable_queue_partial_segments_delta_thing2/1,
      ?config(variable_queue_type, Config)).

variable_queue_partial_segments_delta_thing2(VQ0) ->
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
      fun variable_queue_all_the_bits_not_covered_elsewhere_A2/1,
      ?config(variable_queue_type, Config)).

variable_queue_all_the_bits_not_covered_elsewhere_A2(VQ0) ->
    Count = 2 * rabbit_queue_index:next_segment_boundary(0),
    VQ1 = variable_queue_publish(true, Count, VQ0),
    VQ2 = variable_queue_publish(false, Count, VQ1),
    VQ3 = variable_queue_set_ram_duration_target(0, VQ2),
    {VQ4, _AckTags}  = variable_queue_fetch(Count, true, false,
                                            Count + Count, VQ3),
    {VQ5, _AckTags1} = variable_queue_fetch(Count, false, false,
                                            Count, VQ4),
    _VQ6 = rabbit_variable_queue:terminate(shutdown, VQ5),
    VQ7 = variable_queue_init(test_amqqueue(true), true),
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
      fun variable_queue_all_the_bits_not_covered_elsewhere_B2/1,
      ?config(variable_queue_type, Config)).

variable_queue_all_the_bits_not_covered_elsewhere_B2(VQ0) ->
    VQ1 = variable_queue_set_ram_duration_target(0, VQ0),
    VQ2 = variable_queue_publish(false, 4, VQ1),
    {VQ3, AckTags} = variable_queue_fetch(2, false, false, 4, VQ2),
    {_Guids, VQ4} =
        rabbit_variable_queue:requeue(AckTags, VQ3),
    VQ5 = rabbit_variable_queue:timeout(VQ4),
    _VQ6 = rabbit_variable_queue:terminate(shutdown, VQ5),
    VQ7 = variable_queue_init(test_amqqueue(true), true),
    {empty, VQ8} = rabbit_variable_queue:fetch(false, VQ7),
    VQ8.

variable_queue_drop(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, variable_queue_drop1, [Config]).

variable_queue_drop1(Config) ->
    with_fresh_variable_queue(
      fun variable_queue_drop2/1,
      ?config(variable_queue_type, Config)).

variable_queue_drop2(VQ0) ->
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
      fun variable_queue_fold_msg_on_disk2/1,
      ?config(variable_queue_type, Config)).

variable_queue_fold_msg_on_disk2(VQ0) ->
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
      fun variable_queue_dropfetchwhile2/1,
      ?config(variable_queue_type, Config)).

variable_queue_dropfetchwhile2(VQ0) ->
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

variable_queue_dropwhile_varying_ram_duration(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, variable_queue_dropwhile_varying_ram_duration1, [Config]).

variable_queue_dropwhile_varying_ram_duration1(Config) ->
    with_fresh_variable_queue(
      fun variable_queue_dropwhile_varying_ram_duration2/1,
      ?config(variable_queue_type, Config)).

variable_queue_dropwhile_varying_ram_duration2(VQ0) ->
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
      fun variable_queue_fetchwhile_varying_ram_duration2/1,
      ?config(variable_queue_type, Config)).

variable_queue_fetchwhile_varying_ram_duration2(VQ0) ->
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
      fun variable_queue_ack_limiting2/1,
      ?config(variable_queue_type, Config)).

variable_queue_ack_limiting2(VQ0) ->
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
      fun variable_queue_purge2/1,
      ?config(variable_queue_type, Config)).

variable_queue_purge2(VQ0) ->
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
      fun variable_queue_requeue2/1,
      ?config(variable_queue_type, Config)).

variable_queue_requeue2(VQ0) ->
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
      fun variable_queue_requeue_ram_beta2/1,
      ?config(variable_queue_type, Config)).

variable_queue_requeue_ram_beta2(VQ0) ->
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
      fun variable_queue_fold2/1,
      ?config(variable_queue_type, Config)).

variable_queue_fold2(VQ0) ->
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
      fun variable_queue_batch_publish2/1,
      ?config(variable_queue_type, Config)).

variable_queue_batch_publish2(VQ) ->
    Count = 10,
    VQ1 = variable_queue_batch_publish(true, Count, VQ),
    Count = rabbit_variable_queue:len(VQ1),
    VQ1.

variable_queue_batch_publish_delivered(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, variable_queue_batch_publish_delivered1, [Config]).

variable_queue_batch_publish_delivered1(Config) ->
    with_fresh_variable_queue(
      fun variable_queue_batch_publish_delivered2/1,
      ?config(variable_queue_type, Config)).

variable_queue_batch_publish_delivered2(VQ) ->
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
      fun variable_queue_mode_change2/1,
      ?config(variable_queue_type, Config)).

variable_queue_mode_change2(VQ0) ->
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
    Name1 = rabbit_ct_helpers:config_to_testcase_name(Config, Name),
    queue_name(Name1).

queue_name(Name) ->
    rabbit_misc:r(<<"/">>, queue, Name).

test_queue() ->
    queue_name(<<"test">>).

init_test_queue() ->
    TestQueue = test_queue(),
    PRef = rabbit_guid:gen(),
    PersistentClient = msg_store_client_init(?PERSISTENT_MSG_STORE, PRef),
    Res = rabbit_queue_index:recover(
            TestQueue, [], false,
            fun (MsgId) ->
                    rabbit_msg_store:contains(MsgId, PersistentClient)
            end,
            fun nop/1, fun nop/1),
    ok = rabbit_msg_store:client_delete_and_terminate(PersistentClient),
    Res.

restart_test_queue(Qi) ->
    _ = rabbit_queue_index:terminate([], Qi),
    ok = rabbit_variable_queue:stop(),
    {ok, _} = rabbit_variable_queue:start([test_queue()]),
    init_test_queue().

empty_test_queue() ->
    ok = rabbit_variable_queue:stop(),
    {ok, _} = rabbit_variable_queue:start([]),
    {0, 0, Qi} = init_test_queue(),
    _ = rabbit_queue_index:delete_and_terminate(Qi),
    ok.

with_empty_test_queue(Fun) ->
    ok = empty_test_queue(),
    {0, 0, Qi} = init_test_queue(),
    rabbit_queue_index:delete_and_terminate(Fun(Qi)).

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
    rabbit_msg_store:client_init(MsgStore, Ref, undefined, undefined).

variable_queue_init(Q, Recover) ->
    rabbit_variable_queue:init(
      Q, case Recover of
             true  -> non_clean_shutdown;
             false -> new
         end, fun nop/2, fun nop/2, fun nop/1, fun nop/1).

publish_and_confirm(Q, Payload, Count) ->
    Seqs = lists:seq(1, Count),
    [begin
         Msg = rabbit_basic:message(rabbit_misc:r(<<>>, exchange, <<>>),
                                    <<>>, #'P_basic'{delivery_mode = 2},
                                    Payload),
         Delivery = #delivery{mandatory = false, sender = self(),
                              confirm = true, message = Msg, msg_seq_no = Seq,
                              flow = noflow},
         _QPids = rabbit_amqqueue:deliver([Q], Delivery)
     end || Seq <- Seqs],
    wait_for_confirms(gb_sets:from_list(Seqs)).

wait_for_confirms(Unconfirmed) ->
    case gb_sets:is_empty(Unconfirmed) of
        true  -> ok;
        false -> receive {'$gen_cast', {confirm, Confirmed, _}} ->
                         wait_for_confirms(
                           rabbit_misc:gb_sets_difference(
                             Unconfirmed, gb_sets:from_list(Confirmed)))
                 after ?TIMEOUT -> exit(timeout_waiting_for_confirm)
                 end
    end.

with_fresh_variable_queue(Fun, Mode) ->
    Ref = make_ref(),
    Me = self(),
    %% Run in a separate process since rabbit_msg_store will send
    %% bump_credit messages and we want to ignore them
    spawn_link(fun() ->
                       ok = empty_test_queue(),
                       VQ = variable_queue_init(test_amqqueue(true), false),
                       S0 = variable_queue_status(VQ),
                       assert_props(S0, [{q1, 0}, {q2, 0},
                                         {delta,
                                          {delta, undefined, 0, undefined}},
                                         {q3, 0}, {q4, 0},
                                         {len, 0}]),
                       VQ1 = set_queue_mode(Mode, VQ),
                       try
                           _ = rabbit_variable_queue:delete_and_terminate(
                                 shutdown, Fun(VQ1)),
                           Me ! Ref
                       catch
                           Type:Error ->
                               Me ! {Ref, Type, Error, erlang:get_stacktrace()}
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

test_amqqueue(Durable) ->
    (rabbit_amqqueue:pseudo_queue(test_queue(), self()))
        #amqqueue { durable = Durable }.

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
        true  -> receive
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
    VQ1 = variable_queue_set_ram_duration_target(0, VQ0),
    VQ2 = variable_queue_publish(
            false, 1, Count,
            fun (_, P) -> P end, fun erlang:term_to_binary/1, VQ1),
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

%% ---------------------------------------------------------------------------
%% Credit flow.
%% ---------------------------------------------------------------------------

credit_flow_settings(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, credit_flow_settings1, [Config]).

credit_flow_settings1(_Config) ->
    %% default values
    passed = test_proc(200, 100),

    application:set_env(rabbit, credit_flow_default_credit, {100, 300}),
    passed = test_proc(100, 300),

    application:unset_env(rabbit, credit_flow_default_credit),

    % back to defaults
    passed = test_proc(200, 100),
    passed.

test_proc(InitialCredit, MoreCreditAfter) ->
    Pid = spawn(fun dummy/0),
    Pid ! {credit, self()},
    {InitialCredit, MoreCreditAfter} =
        receive
            {credit, Val} -> Val
        end,
    passed.

dummy() ->
    credit_flow:send(self()),
    receive
        {credit, From} ->
            From ! {credit, get(credit_flow_default_credit)};
        _      ->
            dummy()
    end.

%% -------------------------------------------------------------------
%% dynamic_mirroring.
%% -------------------------------------------------------------------

dynamic_mirroring(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, dynamic_mirroring1, [Config]).

dynamic_mirroring1(_Config) ->
    %% Just unit tests of the node selection logic, see multi node
    %% tests for the rest...
    Test = fun ({NewM, NewSs, ExtraSs}, Policy, Params,
                {MNode, SNodes, SSNodes}, All) ->
                   {ok, M} = rabbit_mirror_queue_misc:module(Policy),
                   {NewM, NewSs0} = M:suggested_queue_nodes(
                                      Params, MNode, SNodes, SSNodes, All),
                   NewSs1 = lists:sort(NewSs0),
                   case dm_list_match(NewSs, NewSs1, ExtraSs) of
                       ok    -> ok;
                       error -> exit({no_match, NewSs, NewSs1, ExtraSs})
                   end
           end,

    Test({a,[b,c],0},<<"all">>,'_',{a,[],   []},   [a,b,c]),
    Test({a,[b,c],0},<<"all">>,'_',{a,[b,c],[b,c]},[a,b,c]),
    Test({a,[b,c],0},<<"all">>,'_',{a,[d],  [d]},  [a,b,c]),

    N = fun (Atoms) -> [list_to_binary(atom_to_list(A)) || A <- Atoms] end,

    %% Add a node
    Test({a,[b,c],0},<<"nodes">>,N([a,b,c]),{a,[b],[b]},[a,b,c,d]),
    Test({b,[a,c],0},<<"nodes">>,N([a,b,c]),{b,[a],[a]},[a,b,c,d]),
    %% Add two nodes and drop one
    Test({a,[b,c],0},<<"nodes">>,N([a,b,c]),{a,[d],[d]},[a,b,c,d]),
    %% Don't try to include nodes that are not running
    Test({a,[b],  0},<<"nodes">>,N([a,b,f]),{a,[b],[b]},[a,b,c,d]),
    %% If we can't find any of the nodes listed then just keep the master
    Test({a,[],   0},<<"nodes">>,N([f,g,h]),{a,[b],[b]},[a,b,c,d]),
    %% And once that's happened, still keep the master even when not listed,
    %% if nothing is synced
    Test({a,[b,c],0},<<"nodes">>,N([b,c]),  {a,[], []}, [a,b,c,d]),
    Test({a,[b,c],0},<<"nodes">>,N([b,c]),  {a,[b],[]}, [a,b,c,d]),
    %% But if something is synced we can lose the master - but make
    %% sure we pick the new master from the nodes which are synced!
    Test({b,[c],  0},<<"nodes">>,N([b,c]),  {a,[b],[b]},[a,b,c,d]),
    Test({b,[c],  0},<<"nodes">>,N([c,b]),  {a,[b],[b]},[a,b,c,d]),

    Test({a,[],   1},<<"exactly">>,2,{a,[],   []},   [a,b,c,d]),
    Test({a,[],   2},<<"exactly">>,3,{a,[],   []},   [a,b,c,d]),
    Test({a,[c],  0},<<"exactly">>,2,{a,[c],  [c]},  [a,b,c,d]),
    Test({a,[c],  1},<<"exactly">>,3,{a,[c],  [c]},  [a,b,c,d]),
    Test({a,[c],  0},<<"exactly">>,2,{a,[c,d],[c,d]},[a,b,c,d]),
    Test({a,[c,d],0},<<"exactly">>,3,{a,[c,d],[c,d]},[a,b,c,d]),

    passed.

%% Does the first list match the second where the second is required
%% to have exactly Extra superfluous items?
dm_list_match([],     [],      0)     -> ok;
dm_list_match(_,      [],     _Extra) -> error;
dm_list_match([H|T1], [H |T2], Extra) -> dm_list_match(T1, T2, Extra);
dm_list_match(L1,     [_H|T2], Extra) -> dm_list_match(L1, T2, Extra - 1).

%% ---------------------------------------------------------------------------
%% file_handle_cache.
%% ---------------------------------------------------------------------------

file_handle_cache(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, file_handle_cache1, [Config]).

file_handle_cache1(_Config) ->
    %% test copying when there is just one spare handle
    Limit = file_handle_cache:get_limit(),
    ok = file_handle_cache:set_limit(5), %% 1 or 2 sockets, 2 msg_stores
    TmpDir = filename:join(rabbit_mnesia:dir(), "tmp"),
    ok = filelib:ensure_dir(filename:join(TmpDir, "nothing")),
    [Src1, Dst1, Src2, Dst2] = Files =
        [filename:join(TmpDir, Str) || Str <- ["file1", "file2", "file3", "file4"]],
    Content = <<"foo">>,
    CopyFun = fun (Src, Dst) ->
                      {ok, Hdl} = prim_file:open(Src, [binary, write]),
                      ok = prim_file:write(Hdl, Content),
                      ok = prim_file:sync(Hdl),
                      prim_file:close(Hdl),

                      {ok, SrcHdl} = file_handle_cache:open(Src, [read], []),
                      {ok, DstHdl} = file_handle_cache:open(Dst, [write], []),
                      Size = size(Content),
                      {ok, Size} = file_handle_cache:copy(SrcHdl, DstHdl, Size),
                      ok = file_handle_cache:delete(SrcHdl),
                      ok = file_handle_cache:delete(DstHdl)
              end,
    Pid = spawn(fun () -> {ok, Hdl} = file_handle_cache:open(
                                        filename:join(TmpDir, "file5"),
                                        [write], []),
                          receive {next, Pid1} -> Pid1 ! {next, self()} end,
                          file_handle_cache:delete(Hdl),
                          %% This will block and never return, so we
                          %% exercise the fhc tidying up the pending
                          %% queue on the death of a process.
                          ok = CopyFun(Src1, Dst1)
                end),
    ok = CopyFun(Src1, Dst1),
    ok = file_handle_cache:set_limit(2),
    Pid ! {next, self()},
    receive {next, Pid} -> ok end,
    timer:sleep(100),
    Pid1 = spawn(fun () -> CopyFun(Src2, Dst2) end),
    timer:sleep(100),
    erlang:monitor(process, Pid),
    erlang:monitor(process, Pid1),
    exit(Pid, kill),
    exit(Pid1, kill),
    receive {'DOWN', _MRef, process, Pid, _Reason} -> ok end,
    receive {'DOWN', _MRef1, process, Pid1, _Reason1} -> ok end,
    [file:delete(File) || File <- Files],
    ok = file_handle_cache:set_limit(Limit),
    passed.

%% -------------------------------------------------------------------
%% Log management.
%% -------------------------------------------------------------------

log_management(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, log_management1, [Config]).

log_management1(_Config) ->
    [LogFile] = rabbit:log_locations(),
    Suffix = ".0",

    ok = test_logs_working([LogFile]),

    %% prepare basic logs
    file:delete(LogFile ++ Suffix),
    ok = test_logs_working([LogFile]),

    %% simple log rotation
    ok = control_action(rotate_logs, []),
    %% FIXME: rabbit:rotate_logs/0 is asynchronous due to a limitation
    %% in Lager. Therefore, we have no choice but to wait an arbitrary
    %% amount of time.
    timer:sleep(2000),
    [true, true] = non_empty_files([LogFile ++ Suffix, LogFile]),
    ok = test_logs_working([LogFile]),

    %% log rotation on empty files
    ok = clean_logs([LogFile], Suffix),
    ok = control_action(rotate_logs, []),
    timer:sleep(2000),
    [{error, enoent}, true] = non_empty_files([LogFile ++ Suffix, LogFile]),

    %% logs with suffix are not writable
    ok = control_action(rotate_logs, []),
    timer:sleep(2000),
    ok = make_files_non_writable([LogFile ++ Suffix]),
    ok = control_action(rotate_logs, []),
    timer:sleep(2000),
    ok = test_logs_working([LogFile]),

    %% rotate when original log files are not writable
    ok = make_files_non_writable([LogFile]),
    ok = control_action(rotate_logs, []),
    timer:sleep(2000),

    %% logging directed to tty (first, remove handlers)
    ok = control_action(stop_app, []),
    ok = clean_logs([LogFile], Suffix),
    ok = application:set_env(rabbit, lager_handler, tty),
    application:unset_env(lager, handlers),
    application:unset_env(lager, extra_sinks),
    ok = control_action(start_app, []),
    timer:sleep(200),
    rabbit_log:info("test info"),
    [{error, enoent}] = empty_files([LogFile]),

    %% rotate logs when logging is turned off
    ok = control_action(stop_app, []),
    ok = clean_logs([LogFile], Suffix),
    ok = application:set_env(rabbit, lager_handler, false),
    application:unset_env(lager, handlers),
    application:unset_env(lager, extra_sinks),
    ok = control_action(start_app, []),
    timer:sleep(200),
    rabbit_log:error("test error"),
    timer:sleep(200),
    [{error, enoent}] = empty_files([LogFile]),

    %% cleanup
    ok = control_action(stop_app, []),
    ok = clean_logs([LogFile], Suffix),
    ok = application:set_env(rabbit, lager_handler, LogFile),
    application:unset_env(lager, handlers),
    application:unset_env(lager, extra_sinks),
    ok = control_action(start_app, []),
    ok = test_logs_working([LogFile]),
    passed.

log_management_during_startup(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, log_management_during_startup1, [Config]).

log_management_during_startup1(_Config) ->
    [LogFile] = rabbit:log_locations(),
    Suffix = ".0",

    %% start application with simple tty logging
    ok = control_action(stop_app, []),
    ok = clean_logs([LogFile], Suffix),
    ok = application:set_env(rabbit, lager_handler, tty),
    application:unset_env(lager, handlers),
    application:unset_env(lager, extra_sinks),
    ok = control_action(start_app, []),

    %% start application with logging to non-existing directory
    NonExistent = "/tmp/non-existent/test.log",
    delete_file(NonExistent),
    delete_file(filename:dirname(NonExistent)),
    ok = control_action(stop_app, []),
    ok = application:set_env(rabbit, lager_handler, NonExistent),
    application:unset_env(lager, handlers),
    application:unset_env(lager, extra_sinks),
    ok = control_action(start_app, []),

    %% start application with logging to directory with no
    %% write permissions
    ok = control_action(stop_app, []),
    NoPermission1 = "/var/empty/test.log",
    delete_file(NoPermission1),
    delete_file(filename:dirname(NoPermission1)),
    ok = control_action(stop_app, []),
    ok = application:set_env(rabbit, lager_handler, NoPermission1),
    application:unset_env(lager, handlers),
    application:unset_env(lager, extra_sinks),
    ok = case control_action(start_app, []) of
             ok -> exit({got_success_but_expected_failure,
                         log_rotation_no_write_permission_dir_test});
             {badrpc,
              {'EXIT', {error, {cannot_log_to_file, _, Reason1}}}}
              when Reason1 =:= enoent orelse Reason1 =:= eacces -> ok;
             {badrpc,
              {'EXIT',
               {error, {cannot_log_to_file, _,
                        {cannot_create_parent_dirs, _, Reason1}}}}}
               when Reason1 =:= eperm orelse
                    Reason1 =:= eacces orelse
                    Reason1 =:= enoent-> ok
         end,

    %% start application with logging to a subdirectory which
    %% parent directory has no write permissions
    NoPermission2 = "/var/empty/non-existent/test.log",
    delete_file(NoPermission2),
    delete_file(filename:dirname(NoPermission2)),
    case control_action(stop_app, []) of
        ok                         -> ok;
        {error, lager_not_running} -> ok
    end,
    ok = application:set_env(rabbit, lager_handler, NoPermission2),
    application:unset_env(lager, handlers),
    application:unset_env(lager, extra_sinks),
    ok = case control_action(start_app, []) of
             ok -> exit({got_success_but_expected_failure,
                         log_rotatation_parent_dirs_test});
             {badrpc,
              {'EXIT', {error, {cannot_log_to_file, _, Reason2}}}}
              when Reason2 =:= enoent orelse Reason2 =:= eacces -> ok;
             {badrpc,
              {'EXIT',
               {error, {cannot_log_to_file, _,
                        {cannot_create_parent_dirs, _, Reason2}}}}}
               when Reason2 =:= eperm orelse
                    Reason2 =:= eacces orelse
                    Reason2 =:= enoent-> ok
         end,

    %% cleanup
    ok = application:set_env(rabbit, lager_handler, LogFile),
    application:unset_env(lager, handlers),
    application:unset_env(lager, extra_sinks),
    ok = control_action(start_app, []),
    passed.

externally_rotated_logs_are_automatically_reopened(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, externally_rotated_logs_are_automatically_reopened1, [Config]).

externally_rotated_logs_are_automatically_reopened1(_Config) ->
    [LogFile] = rabbit:log_locations(),

    %% Make sure log file is opened
    ok = test_logs_working([LogFile]),

    %% Move it away - i.e. external log rotation happened
    file:rename(LogFile, [LogFile, ".rotation_test"]),

    %% New files should be created - test_logs_working/1 will check that
    %% LogFile is not empty after doing some logging. And it's exactly
    %% what we need to check here.
    ok = test_logs_working([LogFile]),
    passed.

empty_or_nonexist_files(Files) ->
    [case file:read_file_info(File) of
         {ok, FInfo}     -> FInfo#file_info.size == 0;
         {error, enoent} -> true;
         Error           -> Error
     end || File <- Files].

empty_files(Files) ->
    [case file:read_file_info(File) of
         {ok, FInfo} -> FInfo#file_info.size == 0;
         Error       -> Error
     end || File <- Files].

non_empty_files(Files) ->
    [case EmptyFile of
         {error, Reason} -> {error, Reason};
         _               -> not(EmptyFile)
     end || EmptyFile <- empty_files(Files)].

test_logs_working(LogFiles) ->
    ok = rabbit_log:error("Log a test message"),
    %% give the error loggers some time to catch up
    timer:sleep(200),
    lists:all(fun(LogFile) -> [true] =:= non_empty_files([LogFile]) end, LogFiles),
    ok.

set_permissions(Path, Mode) ->
    case file:read_file_info(Path) of
        {ok, FInfo} -> file:write_file_info(
                         Path,
                         FInfo#file_info{mode=Mode});
        Error       -> Error
    end.

clean_logs(Files, Suffix) ->
    [begin
         ok = delete_file(File),
         ok = delete_file([File, Suffix])
     end || File <- Files],
    ok.

assert_ram_node() ->
    case rabbit_mnesia:node_type() of
        disc -> exit('not_ram_node');
        ram  -> ok
    end.

assert_disc_node() ->
    case rabbit_mnesia:node_type() of
        disc -> ok;
        ram  -> exit('not_disc_node')
    end.

delete_file(File) ->
    case file:delete(File) of
        ok              -> ok;
        {error, enoent} -> ok;
        Error           -> Error
    end.

make_files_non_writable(Files) ->
    [ok = file:write_file_info(File, #file_info{mode=8#444}) ||
        File <- Files],
    ok.

add_log_handlers(Handlers) ->
    [ok = error_logger:add_report_handler(Handler, Args) ||
        {Handler, Args} <- Handlers],
    ok.

%% sasl_report_file_h returns [] during terminate
%% see: https://github.com/erlang/otp/blob/maint/lib/stdlib/src/error_logger_file_h.erl#L98
%%
%% error_logger_file_h returns ok since OTP 18.1
%% see: https://github.com/erlang/otp/blob/maint/lib/stdlib/src/error_logger_file_h.erl#L98
delete_log_handlers(Handlers) ->
    [ok_or_empty_list(error_logger:delete_report_handler(Handler))
     || Handler <- Handlers],
    ok.

ok_or_empty_list([]) ->
    [];
ok_or_empty_list(ok) ->
    ok.

%% ---------------------------------------------------------------------------
%% Password hashing.
%% ---------------------------------------------------------------------------

password_hashing(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, password_hashing1, [Config]).

password_hashing1(_Config) ->
    rabbit_password_hashing_sha256 = rabbit_password:hashing_mod(),
    application:set_env(rabbit, password_hashing_module,
                        rabbit_password_hashing_md5),
    rabbit_password_hashing_md5    = rabbit_password:hashing_mod(),
    application:set_env(rabbit, password_hashing_module,
                        rabbit_password_hashing_sha256),
    rabbit_password_hashing_sha256 = rabbit_password:hashing_mod(),

    rabbit_password_hashing_sha256 =
        rabbit_password:hashing_mod(rabbit_password_hashing_sha256),
    rabbit_password_hashing_md5    =
        rabbit_password:hashing_mod(rabbit_password_hashing_md5),
    rabbit_password_hashing_md5    =
        rabbit_password:hashing_mod(undefined),

    rabbit_password_hashing_md5    =
        rabbit_auth_backend_internal:hashing_module_for_user(
          #internal_user{}),
    rabbit_password_hashing_md5    =
        rabbit_auth_backend_internal:hashing_module_for_user(
          #internal_user{
             hashing_algorithm = undefined
            }),
    rabbit_password_hashing_md5    =
        rabbit_auth_backend_internal:hashing_module_for_user(
          #internal_user{
             hashing_algorithm = rabbit_password_hashing_md5
            }),

    rabbit_password_hashing_sha256 =
        rabbit_auth_backend_internal:hashing_module_for_user(
          #internal_user{
             hashing_algorithm = rabbit_password_hashing_sha256
            }),

    passed.

change_password(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, change_password1, [Config]).

change_password1(_Config) ->
    UserName = <<"test_user">>,
    Password = <<"test_password">>,
    case rabbit_auth_backend_internal:lookup_user(UserName) of
        {ok, _} -> rabbit_auth_backend_internal:delete_user(UserName);
        _       -> ok
    end,
    ok = application:set_env(rabbit, password_hashing_module,
                             rabbit_password_hashing_md5),
    ok = rabbit_auth_backend_internal:add_user(UserName, Password),
    {ok, #auth_user{username = UserName}} =
        rabbit_auth_backend_internal:user_login_authentication(
            UserName, [{password, Password}]),
    ok = application:set_env(rabbit, password_hashing_module,
                             rabbit_password_hashing_sha256),
    {ok, #auth_user{username = UserName}} =
        rabbit_auth_backend_internal:user_login_authentication(
            UserName, [{password, Password}]),

    NewPassword = <<"test_password1">>,
    ok = rabbit_auth_backend_internal:change_password(UserName, NewPassword),
    {ok, #auth_user{username = UserName}} =
        rabbit_auth_backend_internal:user_login_authentication(
            UserName, [{password, NewPassword}]),

    {refused, _, [UserName]} =
        rabbit_auth_backend_internal:user_login_authentication(
            UserName, [{password, Password}]),
    passed.

%% -------------------------------------------------------------------
%% rabbitmqctl.
%% -------------------------------------------------------------------

list_operations_timeout_pass(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, list_operations_timeout_pass1, [Config]).

list_operations_timeout_pass1(Config) ->
    %% create a few things so there is some useful information to list
    {_Writer1, Limiter1, Ch1} = rabbit_ct_broker_helpers:test_channel(),
    {_Writer2, Limiter2, Ch2} = rabbit_ct_broker_helpers:test_channel(),

    [Q, Q2] = [Queue || Name <- [<<"list_operations_timeout_pass-q1">>,
                                 <<"list_operations_timeout_pass-q2">>],
                        {new, Queue = #amqqueue{}} <-
                            [rabbit_amqqueue:declare(
                               rabbit_misc:r(<<"/">>, queue, Name),
                               false, false, [], none)]],

    ok = rabbit_amqqueue:basic_consume(
           Q, true, Ch1, Limiter1, false, 0, <<"ctag1">>, true, [],
           undefined),
    ok = rabbit_amqqueue:basic_consume(
           Q2, true, Ch2, Limiter2, false, 0, <<"ctag2">>, true, [],
           undefined),

    %% list users
    ok = control_action(add_user,
      ["list_operations_timeout_pass-user",
       "list_operations_timeout_pass-password"]),
    {error, {user_already_exists, _}} =
        control_action(add_user,
          ["list_operations_timeout_pass-user",
           "list_operations_timeout_pass-password"]),
    ok = control_action_t(list_users, [], ?TIMEOUT_LIST_OPS_PASS),

    %% list parameters
    ok = dummy_runtime_parameters:register(),
    ok = control_action(set_parameter, ["test", "good", "123"]),
    ok = control_action_t(list_parameters, [], ?TIMEOUT_LIST_OPS_PASS),
    ok = control_action(clear_parameter, ["test", "good"]),
    dummy_runtime_parameters:unregister(),

    %% list vhosts
    ok = control_action(add_vhost, ["/list_operations_timeout_pass-vhost"]),
    {error, {vhost_already_exists, _}} =
        control_action(add_vhost, ["/list_operations_timeout_pass-vhost"]),
    ok = control_action_t(list_vhosts, [], ?TIMEOUT_LIST_OPS_PASS),

    %% list permissions
    ok = control_action(set_permissions,
      ["list_operations_timeout_pass-user", ".*", ".*", ".*"],
      [{"-p", "/list_operations_timeout_pass-vhost"}]),
    ok = control_action_t(list_permissions, [],
      [{"-p", "/list_operations_timeout_pass-vhost"}],
      ?TIMEOUT_LIST_OPS_PASS),

    %% list user permissions
    ok = control_action_t(list_user_permissions,
      ["list_operations_timeout_pass-user"],
      ?TIMEOUT_LIST_OPS_PASS),

    %% list policies
    ok = control_action_opts(
      ["set_policy", "list_operations_timeout_pass-policy", ".*",
       "{\"ha-mode\":\"all\"}"]),
    ok = control_action_t(list_policies, [], ?TIMEOUT_LIST_OPS_PASS),
    ok = control_action(clear_policy, ["list_operations_timeout_pass-policy"]),

    %% list queues
    ok = info_action_t(list_queues,
      rabbit_amqqueue:info_keys(), false,
      ?TIMEOUT_LIST_OPS_PASS),

    %% list exchanges
    ok = info_action_t(list_exchanges,
      rabbit_exchange:info_keys(), true,
      ?TIMEOUT_LIST_OPS_PASS),

    %% list bindings
    ok = info_action_t(list_bindings,
      rabbit_binding:info_keys(), true,
      ?TIMEOUT_LIST_OPS_PASS),

    %% list connections
    H = ?config(rmq_hostname, Config),
    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    {ok, C1} = gen_tcp:connect(H, P, [binary, {active, false}]),
    gen_tcp:send(C1, <<"AMQP", 0, 0, 9, 1>>),
    {ok, <<1,0,0>>} = gen_tcp:recv(C1, 3, 100),

    {ok, C2} = gen_tcp:connect(H, P, [binary, {active, false}]),
    gen_tcp:send(C2, <<"AMQP", 0, 0, 9, 1>>),
    {ok, <<1,0,0>>} = gen_tcp:recv(C2, 3, 100),

    ok = info_action_t(
      list_connections, rabbit_networking:connection_info_keys(), false,
      ?TIMEOUT_LIST_OPS_PASS),

    %% list consumers
    ok = info_action_t(
      list_consumers, rabbit_amqqueue:consumer_info_keys(), false,
      ?TIMEOUT_LIST_OPS_PASS),

    %% list channels
    ok = info_action_t(
      list_channels, rabbit_channel:info_keys(), false,
      ?TIMEOUT_LIST_OPS_PASS),

    %% do some cleaning up
    ok = control_action(delete_user, ["list_operations_timeout_pass-user"]),
    {error, {no_such_user, _}} =
        control_action(delete_user, ["list_operations_timeout_pass-user"]),

    ok = control_action(delete_vhost, ["/list_operations_timeout_pass-vhost"]),
    {error, {no_such_vhost, _}} =
        control_action(delete_vhost, ["/list_operations_timeout_pass-vhost"]),

    %% close_connection
    Conns = rabbit_ct_broker_helpers:get_connection_pids([C1, C2]),
    [ok, ok] = [ok = control_action(
        close_connection, [rabbit_misc:pid_to_string(ConnPid), "go away"])
     || ConnPid <- Conns],

    %% cleanup queues
    [{ok, _} = rabbit_amqqueue:delete(QR, false, false) || QR <- [Q, Q2]],

    [begin
         unlink(Chan),
         ok = rabbit_channel:shutdown(Chan)
     end || Chan <- [Ch1, Ch2]],
    passed.

user_management(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, user_management1, [Config]).

user_management1(_Config) ->

    %% lots if stuff that should fail
    {error, {no_such_user, _}} =
        control_action(delete_user,
          ["user_management-user"]),
    {error, {no_such_user, _}} =
        control_action(change_password,
          ["user_management-user", "user_management-password"]),
    {error, {no_such_vhost, _}} =
        control_action(delete_vhost,
          ["/user_management-vhost"]),
    {error, {no_such_user, _}} =
        control_action(set_permissions,
          ["user_management-user", ".*", ".*", ".*"]),
    {error, {no_such_user, _}} =
        control_action(clear_permissions,
          ["user_management-user"]),
    {error, {no_such_user, _}} =
        control_action(list_user_permissions,
          ["user_management-user"]),
    {error, {no_such_vhost, _}} =
        control_action(list_permissions, [],
          [{"-p", "/user_management-vhost"}]),
    {error, {invalid_regexp, _, _}} =
        control_action(set_permissions,
          ["guest", "+foo", ".*", ".*"]),
    {error, {no_such_user, _}} =
        control_action(set_user_tags,
          ["user_management-user", "bar"]),

    %% user creation
    ok = control_action(add_user,
      ["user_management-user", "user_management-password"]),
    {error, {user_already_exists, _}} =
        control_action(add_user,
          ["user_management-user", "user_management-password"]),
    ok = control_action(clear_password,
      ["user_management-user"]),
    ok = control_action(change_password,
      ["user_management-user", "user_management-newpassword"]),

    TestTags = fun (Tags) ->
                       Args = ["user_management-user" | [atom_to_list(T) || T <- Tags]],
                       ok = control_action(set_user_tags, Args),
                       {ok, #internal_user{tags = Tags}} =
                           rabbit_auth_backend_internal:lookup_user(
                             <<"user_management-user">>),
                       ok = control_action(list_users, [])
               end,
    TestTags([foo, bar, baz]),
    TestTags([administrator]),
    TestTags([]),

    %% user authentication
    ok = control_action(authenticate_user,
      ["user_management-user", "user_management-newpassword"]),
    {refused, _User, _Format, _Params} =
        control_action(authenticate_user,
          ["user_management-user", "user_management-password"]),

    %% vhost creation
    ok = control_action(add_vhost,
      ["/user_management-vhost"]),
    {error, {vhost_already_exists, _}} =
        control_action(add_vhost,
          ["/user_management-vhost"]),
    ok = control_action(list_vhosts, []),

    %% user/vhost mapping
    ok = control_action(set_permissions,
      ["user_management-user", ".*", ".*", ".*"],
      [{"-p", "/user_management-vhost"}]),
    ok = control_action(set_permissions,
      ["user_management-user", ".*", ".*", ".*"],
      [{"-p", "/user_management-vhost"}]),
    ok = control_action(set_permissions,
      ["user_management-user", ".*", ".*", ".*"],
      [{"-p", "/user_management-vhost"}]),
    ok = control_action(list_permissions, [],
      [{"-p", "/user_management-vhost"}]),
    ok = control_action(list_permissions, [],
      [{"-p", "/user_management-vhost"}]),
    ok = control_action(list_user_permissions,
      ["user_management-user"]),

    %% user/vhost unmapping
    ok = control_action(clear_permissions,
      ["user_management-user"], [{"-p", "/user_management-vhost"}]),
    ok = control_action(clear_permissions,
      ["user_management-user"], [{"-p", "/user_management-vhost"}]),

    %% vhost deletion
    ok = control_action(delete_vhost,
      ["/user_management-vhost"]),
    {error, {no_such_vhost, _}} =
        control_action(delete_vhost,
          ["/user_management-vhost"]),

    %% deleting a populated vhost
    ok = control_action(add_vhost,
      ["/user_management-vhost"]),
    ok = control_action(set_permissions,
      ["user_management-user", ".*", ".*", ".*"],
      [{"-p", "/user_management-vhost"}]),
    {new, _} = rabbit_amqqueue:declare(
                 rabbit_misc:r(<<"/user_management-vhost">>, queue,
                               <<"user_management-vhost-queue">>),
                 true, false, [], none),
    ok = control_action(delete_vhost,
      ["/user_management-vhost"]),

    %% user deletion
    ok = control_action(delete_user,
      ["user_management-user"]),
    {error, {no_such_user, _}} =
        control_action(delete_user,
          ["user_management-user"]),

    passed.

runtime_parameters(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, runtime_parameters1, [Config]).

runtime_parameters1(_Config) ->
    dummy_runtime_parameters:register(),
    Good = fun(L) -> ok                = control_action(set_parameter, L) end,
    Bad  = fun(L) -> {error_string, _} = control_action(set_parameter, L) end,

    %% Acceptable for bijection
    Good(["test", "good", "\"ignore\""]),
    Good(["test", "good", "123"]),
    Good(["test", "good", "true"]),
    Good(["test", "good", "false"]),
    Good(["test", "good", "null"]),
    Good(["test", "good", "{\"key\": \"value\"}"]),

    %% Invalid json
    Bad(["test", "good", "atom"]),
    Bad(["test", "good", "{\"foo\": \"bar\""]),
    Bad(["test", "good", "{foo: \"bar\"}"]),

    %% Test actual validation hook
    Good(["test", "maybe", "\"good\""]),
    Bad(["test", "maybe", "\"bad\""]),
    Good(["test", "admin", "\"ignore\""]), %% ctl means 'user' -> none

    ok = control_action(list_parameters, []),

    ok = control_action(clear_parameter, ["test", "good"]),
    ok = control_action(clear_parameter, ["test", "maybe"]),
    ok = control_action(clear_parameter, ["test", "admin"]),
    {error_string, _} =
        control_action(clear_parameter, ["test", "neverexisted"]),

    %% We can delete for a component that no longer exists
    Good(["test", "good", "\"ignore\""]),
    dummy_runtime_parameters:unregister(),
    ok = control_action(clear_parameter, ["test", "good"]),
    passed.

policy_validation(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, policy_validation1, [Config]).

policy_validation1(_Config) ->
    PolicyName = "runtime_parameters-policy",
    dummy_runtime_parameters:register_policy_validator(),
    SetPol = fun (Key, Val) ->
                     control_action_opts(
                       ["set_policy", PolicyName, ".*",
                        rabbit_misc:format("{\"~s\":~p}", [Key, Val])])
             end,
    OK     = fun (Key, Val) ->
                 ok = SetPol(Key, Val),
                 true = does_policy_exist(PolicyName,
                   [{definition, [{list_to_binary(Key), Val}]}])
             end,

    OK("testeven", []),
    OK("testeven", [1, 2]),
    OK("testeven", [1, 2, 3, 4]),
    OK("testpos",  [2, 5, 5678]),

    {error_string, _} = SetPol("testpos",  [-1, 0, 1]),
    {error_string, _} = SetPol("testeven", [ 1, 2, 3]),

    ok = control_action(clear_policy, [PolicyName]),
    dummy_runtime_parameters:unregister_policy_validator(),
    passed.

policy_opts_validation(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, policy_opts_validation1, [Config]).

policy_opts_validation1(_Config) ->
    PolicyName = "policy_opts_validation-policy",
    Set  = fun (Extra) -> control_action_opts(
                            ["set_policy", PolicyName,
                             ".*", "{\"ha-mode\":\"all\"}"
                             | Extra]) end,
    OK   = fun (Extra, Props) ->
               ok = Set(Extra),
               true = does_policy_exist(PolicyName, Props)
           end,
    Fail = fun (Extra) ->
            case Set(Extra) of
                {error_string, _} -> ok;
                no_command when Extra =:= ["--priority"] -> ok;
                no_command when Extra =:= ["--apply-to"] -> ok;
                {'EXIT',
                 {function_clause,
                  [{rabbit_control_main,action, _, _} | _]}}
                when Extra =:= ["--offline"] -> ok
          end
    end,

    OK  ([], [{priority, 0}, {'apply-to', <<"all">>}]),

    OK  (["--priority", "0"], [{priority, 0}]),
    OK  (["--priority", "3"], [{priority, 3}]),
    Fail(["--priority", "banana"]),
    Fail(["--priority"]),

    OK  (["--apply-to", "all"],    [{'apply-to', <<"all">>}]),
    OK  (["--apply-to", "queues"], [{'apply-to', <<"queues">>}]),
    Fail(["--apply-to", "bananas"]),
    Fail(["--apply-to"]),

    OK  (["--priority", "3",      "--apply-to", "queues"], [{priority, 3}, {'apply-to', <<"queues">>}]),
    Fail(["--priority", "banana", "--apply-to", "queues"]),
    Fail(["--priority", "3",      "--apply-to", "bananas"]),

    Fail(["--offline"]),

    ok = control_action(clear_policy, [PolicyName]),
    passed.

ha_policy_validation(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, ha_policy_validation1, [Config]).

ha_policy_validation1(_Config) ->
    PolicyName = "ha_policy_validation-policy",
    Set  = fun (JSON) -> control_action_opts(
                           ["set_policy", PolicyName,
                            ".*", JSON]) end,
    OK   = fun (JSON, Def) ->
               ok = Set(JSON),
               true = does_policy_exist(PolicyName, [{definition, Def}])
           end,
    Fail = fun (JSON) -> {error_string, _} = Set(JSON) end,

    OK  ("{\"ha-mode\":\"all\"}", [{<<"ha-mode">>, <<"all">>}]),
    Fail("{\"ha-mode\":\"made_up\"}"),

    Fail("{\"ha-mode\":\"nodes\"}"),
    Fail("{\"ha-mode\":\"nodes\",\"ha-params\":2}"),
    Fail("{\"ha-mode\":\"nodes\",\"ha-params\":[\"a\",2]}"),
    OK  ("{\"ha-mode\":\"nodes\",\"ha-params\":[\"a\",\"b\"]}",
      [{<<"ha-mode">>, <<"nodes">>}, {<<"ha-params">>, [<<"a">>, <<"b">>]}]),
    Fail("{\"ha-params\":[\"a\",\"b\"]}"),

    Fail("{\"ha-mode\":\"exactly\"}"),
    Fail("{\"ha-mode\":\"exactly\",\"ha-params\":[\"a\",\"b\"]}"),
    OK  ("{\"ha-mode\":\"exactly\",\"ha-params\":2}",
      [{<<"ha-mode">>, <<"exactly">>}, {<<"ha-params">>, 2}]),
    Fail("{\"ha-params\":2}"),

    OK  ("{\"ha-mode\":\"all\",\"ha-sync-mode\":\"manual\"}",
      [{<<"ha-mode">>, <<"all">>}, {<<"ha-sync-mode">>, <<"manual">>}]),
    OK  ("{\"ha-mode\":\"all\",\"ha-sync-mode\":\"automatic\"}",
      [{<<"ha-mode">>, <<"all">>}, {<<"ha-sync-mode">>, <<"automatic">>}]),
    Fail("{\"ha-mode\":\"all\",\"ha-sync-mode\":\"made_up\"}"),
    Fail("{\"ha-sync-mode\":\"manual\"}"),
    Fail("{\"ha-sync-mode\":\"automatic\"}"),

    ok = control_action(clear_policy, [PolicyName]),
    passed.

queue_master_location_policy_validation(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, queue_master_location_policy_validation1, [Config]).

queue_master_location_policy_validation1(_Config) ->
    PolicyName = "queue_master_location_policy_validation-policy",
    Set  = fun (JSON) ->
                   control_action_opts(
                     ["set_policy", PolicyName, ".*", JSON])
           end,
    OK   = fun (JSON, Def) ->
               ok = Set(JSON),
               true = does_policy_exist(PolicyName, [{definition, Def}])
           end,
    Fail = fun (JSON) -> {error_string, _} = Set(JSON) end,

    OK  ("{\"queue-master-locator\":\"min-masters\"}",
      [{<<"queue-master-locator">>, <<"min-masters">>}]),
    OK  ("{\"queue-master-locator\":\"client-local\"}",
      [{<<"queue-master-locator">>, <<"client-local">>}]),
    OK  ("{\"queue-master-locator\":\"random\"}",
      [{<<"queue-master-locator">>, <<"random">>}]),
    Fail("{\"queue-master-locator\":\"made_up\"}"),

    ok = control_action(clear_policy, [PolicyName]),
    passed.

queue_modes_policy_validation(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, queue_modes_policy_validation1, [Config]).

queue_modes_policy_validation1(_Config) ->
    PolicyName = "queue_modes_policy_validation-policy",
    Set  = fun (JSON) ->
                   control_action_opts(
                     ["set_policy", PolicyName, ".*", JSON])
           end,
    OK   = fun (JSON, Def) ->
               ok = Set(JSON),
               true = does_policy_exist(PolicyName, [{definition, Def}])
           end,
    Fail = fun (JSON) -> {error_string, _} = Set(JSON) end,

    OK  ("{\"queue-mode\":\"lazy\"}",
      [{<<"queue-mode">>, <<"lazy">>}]),
    OK  ("{\"queue-mode\":\"default\"}",
      [{<<"queue-mode">>, <<"default">>}]),
    Fail("{\"queue-mode\":\"wrong\"}"),

    ok = control_action(clear_policy, [PolicyName]),
    passed.

vhost_removed_while_updating_policy(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, vhost_removed_while_updating_policy1, [Config]).

vhost_removed_while_updating_policy1(_Config) ->
    VHost = "/vhost_removed_while_updating_policy-vhost",
    PolicyName = "vhost_removed_while_updating_policy-policy",

    ok = control_action(add_vhost, [VHost]),
    ok = control_action_opts(
      ["set_policy", "-p", VHost, PolicyName, ".*", "{\"ha-mode\":\"all\"}"]),
    true = does_policy_exist(PolicyName, []),

    %% Removing the vhost triggers the deletion of the policy. Once
    %% the policy and the vhost are actually removed, RabbitMQ calls
    %% update_policies() which lists policies on the given vhost. This
    %% obviously fails because the vhost is gone, but the call should
    %% still succeed.
    ok = control_action(delete_vhost, [VHost]),
    false = does_policy_exist(PolicyName, []),

    passed.

does_policy_exist(PolicyName, Props) ->
    PolicyNameBin = list_to_binary(PolicyName),
    Policies = lists:filter(
      fun(Policy) ->
          lists:member({name, PolicyNameBin}, Policy)
      end, rabbit_policy:list()),
    case Policies of
        [Policy] -> check_policy_props(Policy, Props);
        []       -> false;
        _        -> false
    end.

check_policy_props(Policy, [Prop | Rest]) ->
    case lists:member(Prop, Policy) of
        true  -> check_policy_props(Policy, Rest);
        false -> false
    end;
check_policy_props(_Policy, []) ->
    true.

server_status(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, server_status1, [Config]).

server_status1(Config) ->
    %% create a few things so there is some useful information to list
    {_Writer, Limiter, Ch} = rabbit_ct_broker_helpers:test_channel(),
    [Q, Q2] = [Queue || {Name, Owner} <- [{<<"server_status-q1">>, none},
                                          {<<"server_status-q2">>, self()}],
                        {new, Queue = #amqqueue{}} <-
                            [rabbit_amqqueue:declare(
                               rabbit_misc:r(<<"/">>, queue, Name),
                               false, false, [], Owner)]],
    ok = rabbit_amqqueue:basic_consume(
           Q, true, Ch, Limiter, false, 0, <<"ctag">>, true, [], undefined),

    %% list queues
    ok = info_action(list_queues,
      rabbit_amqqueue:info_keys(), true),

    %% as we have no way to collect output of
    %% info_action/3 call, the only way we
    %% can test individual queueinfoitems is by directly calling
    %% rabbit_amqqueue:info/2
    [{exclusive, false}] = rabbit_amqqueue:info(Q, [exclusive]),
    [{exclusive, true}] = rabbit_amqqueue:info(Q2, [exclusive]),

    %% list exchanges
    ok = info_action(list_exchanges,
      rabbit_exchange:info_keys(), true),

    %% list bindings
    ok = info_action(list_bindings,
      rabbit_binding:info_keys(), true),
    %% misc binding listing APIs
    [_|_] = rabbit_binding:list_for_source(
              rabbit_misc:r(<<"/">>, exchange, <<"">>)),
    [_] = rabbit_binding:list_for_destination(
            rabbit_misc:r(<<"/">>, queue, <<"server_status-q1">>)),
    [_] = rabbit_binding:list_for_source_and_destination(
            rabbit_misc:r(<<"/">>, exchange, <<"">>),
            rabbit_misc:r(<<"/">>, queue, <<"server_status-q1">>)),

    %% list connections
    H = ?config(rmq_hostname, Config),
    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    {ok, C} = gen_tcp:connect(H, P, []),
    gen_tcp:send(C, <<"AMQP", 0, 0, 9, 1>>),
    timer:sleep(100),
    ok = info_action(list_connections,
      rabbit_networking:connection_info_keys(), false),
    %% close_connection
    [ConnPid] = rabbit_ct_broker_helpers:get_connection_pids([C]),
    ok = control_action(close_connection,
      [rabbit_misc:pid_to_string(ConnPid), "go away"]),

    %% list channels
    ok = info_action(list_channels, rabbit_channel:info_keys(), false),

    %% list consumers
    ok = control_action(list_consumers, []),

    %% set vm memory high watermark
    HWM = vm_memory_monitor:get_vm_memory_high_watermark(),
    ok = control_action(set_vm_memory_high_watermark, ["1"]),
    ok = control_action(set_vm_memory_high_watermark, ["1.0"]),
    %% this will trigger an alarm
    ok = control_action(set_vm_memory_high_watermark, ["0.0"]),
    %% reset
    ok = control_action(set_vm_memory_high_watermark, [float_to_list(HWM)]),

    %% eval
    {error_string, _} = control_action(eval, ["\""]),
    {error_string, _} = control_action(eval, ["a("]),
    ok = control_action(eval, ["a."]),

    %% cleanup
    [{ok, _} = rabbit_amqqueue:delete(QR, false, false) || QR <- [Q, Q2]],

    unlink(Ch),
    ok = rabbit_channel:shutdown(Ch),

    passed.

amqp_connection_refusal(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, amqp_connection_refusal1, [Config]).

amqp_connection_refusal1(Config) ->
    H = ?config(rmq_hostname, Config),
    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    [passed = test_amqp_connection_refusal(H, P, V) ||
        V <- [<<"AMQP",9,9,9,9>>, <<"AMQP",0,1,0,0>>, <<"XXXX",0,0,9,1>>]],
    passed.

test_amqp_connection_refusal(H, P, Header) ->
    {ok, C} = gen_tcp:connect(H, P, [binary, {active, false}]),
    ok = gen_tcp:send(C, Header),
    {ok, <<"AMQP",0,0,9,1>>} = gen_tcp:recv(C, 8, 100),
    ok = gen_tcp:close(C),
    passed.

list_consumers_sanity_check(Config) ->
    A = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Chan = rabbit_ct_client_helpers:open_channel(Config, A),
    %% this queue is not cleaned up because the entire node is
    %% reset between tests
    QName = <<"list_consumers_q">>,
    #'queue.declare_ok'{} = amqp_channel:call(Chan, #'queue.declare'{queue = QName}),

    %% No consumers even if we have some queues
    [] = rabbitmqctl_list_consumers(Config, A),

    %% Several consumers on single channel should be correctly reported
    #'basic.consume_ok'{consumer_tag = CTag1} = amqp_channel:call(Chan, #'basic.consume'{queue = QName}),
    #'basic.consume_ok'{consumer_tag = CTag2} = amqp_channel:call(Chan, #'basic.consume'{queue = QName}),
    true = (lists:sort([CTag1, CTag2]) =:=
            lists:sort(rabbitmqctl_list_consumers(Config, A))),

    %% `rabbitmqctl report` shares some code with `list_consumers`, so
    %% check that it also reports both channels
    {ok, ReportStdOut} = rabbit_ct_broker_helpers:rabbitmqctl(Config, A,
      ["list_consumers"]),
    ReportLines = re:split(ReportStdOut, <<"\n">>, [trim]),
    ReportCTags = [lists:nth(3, re:split(Row, <<"\t">>)) || <<"list_consumers_q", _/binary>> = Row <- ReportLines],
    true = (lists:sort([CTag1, CTag2]) =:=
            lists:sort(ReportCTags)).

rabbitmqctl_list_consumers(Config, Node) ->
    {ok, StdOut} = rabbit_ct_broker_helpers:rabbitmqctl(Config, Node,
      ["list_consumers"]),
    [<<"Listing consumers", _/binary>> | ConsumerRows] = re:split(StdOut, <<"\n">>, [trim]),
    CTags = [ lists:nth(3, re:split(Row, <<"\t">>)) || Row <- ConsumerRows ],
    CTags.

list_queues_online_and_offline(Config) ->
    [A, B] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    ACh = rabbit_ct_client_helpers:open_channel(Config, A),
    %% Node B will be stopped
    BCh = rabbit_ct_client_helpers:open_channel(Config, B),
    #'queue.declare_ok'{} = amqp_channel:call(ACh, #'queue.declare'{queue = <<"q_a_1">>, durable = true}),
    #'queue.declare_ok'{} = amqp_channel:call(ACh, #'queue.declare'{queue = <<"q_a_2">>, durable = true}),
    #'queue.declare_ok'{} = amqp_channel:call(BCh, #'queue.declare'{queue = <<"q_b_1">>, durable = true}),
    #'queue.declare_ok'{} = amqp_channel:call(BCh, #'queue.declare'{queue = <<"q_b_2">>, durable = true}),

    rabbit_ct_broker_helpers:rabbitmqctl(Config, B, ["stop"]),

    GotUp = lists:sort(rabbit_ct_broker_helpers:rabbitmqctl_list(Config, A,
        ["list_queues", "--online", "name"])),
    ExpectUp = [[<<"q_a_1">>], [<<"q_a_2">>]],
    ExpectUp = GotUp,

    GotDown = lists:sort(rabbit_ct_broker_helpers:rabbitmqctl_list(Config, A,
        ["list_queues", "--offline", "name"])),
    ExpectDown = [[<<"q_b_1">>], [<<"q_b_2">>]],
    ExpectDown = GotDown,

    GotAll = lists:sort(rabbit_ct_broker_helpers:rabbitmqctl_list(Config, A,
        ["list_queues", "name"])),
    ExpectAll = ExpectUp ++ ExpectDown,
    ExpectAll = GotAll,

    ok.

%% -------------------------------------------------------------------
%% Statistics.
%% -------------------------------------------------------------------

channel_statistics(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, channel_statistics1, [Config]).

channel_statistics1(_Config) ->
    application:set_env(rabbit, collect_statistics, fine),

    %% ATM this just tests the queue / exchange stats in channels. That's
    %% by far the most complex code though.

    %% Set up a channel and queue
    {_Writer, Ch} = test_spawn(),
    rabbit_channel:do(Ch, #'queue.declare'{}),
    QName = receive #'queue.declare_ok'{queue = Q0} -> Q0
            after ?TIMEOUT -> throw(failed_to_receive_queue_declare_ok)
            end,
    QRes = rabbit_misc:r(<<"/">>, queue, QName),
    X = rabbit_misc:r(<<"/">>, exchange, <<"">>),

    dummy_event_receiver:start(self(), [node()], [channel_stats]),

    %% Check stats empty
    Event = test_ch_statistics_receive_event(Ch, fun (_) -> true end),
    [] = proplists:get_value(channel_queue_stats, Event),
    [] = proplists:get_value(channel_exchange_stats, Event),
    [] = proplists:get_value(channel_queue_exchange_stats, Event),

    %% Publish and get a message
    rabbit_channel:do(Ch, #'basic.publish'{exchange = <<"">>,
                                           routing_key = QName},
                      rabbit_basic:build_content(#'P_basic'{}, <<"">>)),
    rabbit_channel:do(Ch, #'basic.get'{queue = QName}),

    %% Check the stats reflect that
    Event2 = test_ch_statistics_receive_event(
               Ch,
               fun (E) ->
                       length(proplists:get_value(
                                channel_queue_exchange_stats, E)) > 0
               end),
    [{QRes, [{get,1}]}] = proplists:get_value(channel_queue_stats,    Event2),
    [{X,[{publish,1}]}] = proplists:get_value(channel_exchange_stats, Event2),
    [{{QRes,X},[{publish,1}]}] =
        proplists:get_value(channel_queue_exchange_stats, Event2),

    %% Check the stats remove stuff on queue deletion
    rabbit_channel:do(Ch, #'queue.delete'{queue = QName}),
    Event3 = test_ch_statistics_receive_event(
               Ch,
               fun (E) ->
                       length(proplists:get_value(
                                channel_queue_exchange_stats, E)) == 0
               end),

    [] = proplists:get_value(channel_queue_stats, Event3),
    [{X,[{publish,1}]}] = proplists:get_value(channel_exchange_stats, Event3),
    [] = proplists:get_value(channel_queue_exchange_stats, Event3),

    rabbit_channel:shutdown(Ch),
    dummy_event_receiver:stop(),
    passed.

test_ch_statistics_receive_event(Ch, Matcher) ->
    rabbit_channel:flush(Ch),
    Ch ! emit_stats,
    test_ch_statistics_receive_event1(Ch, Matcher).

test_ch_statistics_receive_event1(Ch, Matcher) ->
    receive #event{type = channel_stats, props = Props} ->
            case Matcher(Props) of
                true -> Props;
                _    -> test_ch_statistics_receive_event1(Ch, Matcher)
            end
    after ?TIMEOUT -> throw(failed_to_receive_event)
    end.

head_message_timestamp_statistics(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, head_message_timestamp1, [Config]).

head_message_timestamp1(_Config) ->
    %% Can't find a way to receive the ack here so can't test pending acks status

    application:set_env(rabbit, collect_statistics, fine),

    %% Set up a channel and queue
    {_Writer, Ch} = test_spawn(),
    rabbit_channel:do(Ch, #'queue.declare'{}),
    QName = receive #'queue.declare_ok'{queue = Q0} -> Q0
            after ?TIMEOUT -> throw(failed_to_receive_queue_declare_ok)
            end,
    QRes = rabbit_misc:r(<<"/">>, queue, QName),

    {ok, Q1} = rabbit_amqqueue:lookup(QRes),
    QPid = Q1#amqqueue.pid,

    %% Set up event receiver for queue
    dummy_event_receiver:start(self(), [node()], [queue_stats]),

    %% Check timestamp is empty when queue is empty
    Event1 = test_queue_statistics_receive_event(QPid, fun (E) -> proplists:get_value(name, E) == QRes end),
    '' = proplists:get_value(head_message_timestamp, Event1),

    %% Publish two messages and check timestamp is that of first message
    rabbit_channel:do(Ch, #'basic.publish'{exchange = <<"">>,
                                           routing_key = QName},
                      rabbit_basic:build_content(#'P_basic'{timestamp = 1}, <<"">>)),
    rabbit_channel:do(Ch, #'basic.publish'{exchange = <<"">>,
                                           routing_key = QName},
                      rabbit_basic:build_content(#'P_basic'{timestamp = 2}, <<"">>)),
    Event2 = test_queue_statistics_receive_event(QPid, fun (E) -> proplists:get_value(name, E) == QRes end),
    1 = proplists:get_value(head_message_timestamp, Event2),

    %% Get first message and check timestamp is that of second message
    rabbit_channel:do(Ch, #'basic.get'{queue = QName, no_ack = true}),
    Event3 = test_queue_statistics_receive_event(QPid, fun (E) -> proplists:get_value(name, E) == QRes end),
    2 = proplists:get_value(head_message_timestamp, Event3),

    %% Get second message and check timestamp is empty again
    rabbit_channel:do(Ch, #'basic.get'{queue = QName, no_ack = true}),
    Event4 = test_queue_statistics_receive_event(QPid, fun (E) -> proplists:get_value(name, E) == QRes end),
    '' = proplists:get_value(head_message_timestamp, Event4),

    %% Teardown
    rabbit_channel:do(Ch, #'queue.delete'{queue = QName}),
    rabbit_channel:shutdown(Ch),
    dummy_event_receiver:stop(),

    passed.

test_queue_statistics_receive_event(Q, Matcher) ->
    %% Q ! emit_stats,
    test_queue_statistics_receive_event1(Q, Matcher).

test_queue_statistics_receive_event1(Q, Matcher) ->
    receive #event{type = queue_stats, props = Props} ->
            case Matcher(Props) of
                true -> Props;
                _    -> test_queue_statistics_receive_event1(Q, Matcher)
            end
    after ?TIMEOUT -> throw(failed_to_receive_event)
    end.

test_spawn() ->
    {Writer, _Limiter, Ch} = rabbit_ct_broker_helpers:test_channel(),
    ok = rabbit_channel:do(Ch, #'channel.open'{}),
    receive #'channel.open_ok'{} -> ok
    after ?TIMEOUT -> throw(failed_to_receive_channel_open_ok)
    end,
    {Writer, Ch}.

test_spawn(Node) ->
    rpc:call(Node, ?MODULE, test_spawn_remote, []).

%% Spawn an arbitrary long lived process, so we don't end up linking
%% the channel to the short-lived process (RPC, here) spun up by the
%% RPC server.
test_spawn_remote() ->
    RPC = self(),
    spawn(fun () ->
                  {Writer, Ch} = test_spawn(),
                  RPC ! {Writer, Ch},
                  link(Ch),
                  receive
                      _ -> ok
                  end
          end),
    receive Res -> Res
    after ?TIMEOUT  -> throw(failed_to_receive_result)
    end.

%% -------------------------------------------------------------------
%% Topic matching.
%% -------------------------------------------------------------------

topic_matching(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, topic_matching1, [Config]).

topic_matching1(_Config) ->
    XName = #resource{virtual_host = <<"/">>,
                      kind = exchange,
                      name = <<"topic_matching-exchange">>},
    X0 = #exchange{name = XName, type = topic, durable = false,
                   auto_delete = false, arguments = []},
    X = rabbit_exchange_decorator:set(X0),
    %% create
    rabbit_exchange_type_topic:validate(X),
    exchange_op_callback(X, create, []),

    %% add some bindings
    Bindings = [#binding{source = XName,
                         key = list_to_binary(Key),
                         destination = #resource{virtual_host = <<"/">>,
                                                 kind = queue,
                                                 name = list_to_binary(Q)},
                         args = Args} ||
                   {Key, Q, Args} <- [{"a.b.c",         "t1",  []},
                                      {"a.*.c",         "t2",  []},
                                      {"a.#.b",         "t3",  []},
                                      {"a.b.b.c",       "t4",  []},
                                      {"#",             "t5",  []},
                                      {"#.#",           "t6",  []},
                                      {"#.b",           "t7",  []},
                                      {"*.*",           "t8",  []},
                                      {"a.*",           "t9",  []},
                                      {"*.b.c",         "t10", []},
                                      {"a.#",           "t11", []},
                                      {"a.#.#",         "t12", []},
                                      {"b.b.c",         "t13", []},
                                      {"a.b.b",         "t14", []},
                                      {"a.b",           "t15", []},
                                      {"b.c",           "t16", []},
                                      {"",              "t17", []},
                                      {"*.*.*",         "t18", []},
                                      {"vodka.martini", "t19", []},
                                      {"a.b.c",         "t20", []},
                                      {"*.#",           "t21", []},
                                      {"#.*.#",         "t22", []},
                                      {"*.#.#",         "t23", []},
                                      {"#.#.#",         "t24", []},
                                      {"*",             "t25", []},
                                      {"#.b.#",         "t26", []},
                                      {"args-test",     "t27",
                                       [{<<"foo">>, longstr, <<"bar">>}]},
                                      {"args-test",     "t27", %% Note aliasing
                                       [{<<"foo">>, longstr, <<"baz">>}]}]],
    lists:foreach(fun (B) -> exchange_op_callback(X, add_binding, [B]) end,
                  Bindings),

    %% test some matches
    test_topic_expect_match(
      X, [{"a.b.c",               ["t1", "t2", "t5", "t6", "t10", "t11", "t12",
                                   "t18", "t20", "t21", "t22", "t23", "t24",
                                   "t26"]},
          {"a.b",                 ["t3", "t5", "t6", "t7", "t8", "t9", "t11",
                                   "t12", "t15", "t21", "t22", "t23", "t24",
                                   "t26"]},
          {"a.b.b",               ["t3", "t5", "t6", "t7", "t11", "t12", "t14",
                                   "t18", "t21", "t22", "t23", "t24", "t26"]},
          {"",                    ["t5", "t6", "t17", "t24"]},
          {"b.c.c",               ["t5", "t6", "t18", "t21", "t22", "t23",
                                   "t24", "t26"]},
          {"a.a.a.a.a",           ["t5", "t6", "t11", "t12", "t21", "t22",
                                   "t23", "t24"]},
          {"vodka.gin",           ["t5", "t6", "t8", "t21", "t22", "t23",
                                   "t24"]},
          {"vodka.martini",       ["t5", "t6", "t8", "t19", "t21", "t22", "t23",
                                   "t24"]},
          {"b.b.c",               ["t5", "t6", "t10", "t13", "t18", "t21",
                                   "t22", "t23", "t24", "t26"]},
          {"nothing.here.at.all", ["t5", "t6", "t21", "t22", "t23", "t24"]},
          {"oneword",             ["t5", "t6", "t21", "t22", "t23", "t24",
                                   "t25"]},
          {"args-test",           ["t5", "t6", "t21", "t22", "t23", "t24",
                                   "t25", "t27"]}]),
    %% remove some bindings
    RemovedBindings = [lists:nth(1, Bindings), lists:nth(5, Bindings),
                       lists:nth(11, Bindings), lists:nth(19, Bindings),
                       lists:nth(21, Bindings), lists:nth(28, Bindings)],
    exchange_op_callback(X, remove_bindings, [RemovedBindings]),
    RemainingBindings = ordsets:to_list(
                          ordsets:subtract(ordsets:from_list(Bindings),
                                           ordsets:from_list(RemovedBindings))),

    %% test some matches
    test_topic_expect_match(
      X,
      [{"a.b.c",               ["t2", "t6", "t10", "t12", "t18", "t20", "t22",
                                "t23", "t24", "t26"]},
       {"a.b",                 ["t3", "t6", "t7", "t8", "t9", "t12", "t15",
                                "t22", "t23", "t24", "t26"]},
       {"a.b.b",               ["t3", "t6", "t7", "t12", "t14", "t18", "t22",
                                "t23", "t24", "t26"]},
       {"",                    ["t6", "t17", "t24"]},
       {"b.c.c",               ["t6", "t18", "t22", "t23", "t24", "t26"]},
       {"a.a.a.a.a",           ["t6", "t12", "t22", "t23", "t24"]},
       {"vodka.gin",           ["t6", "t8", "t22", "t23", "t24"]},
       {"vodka.martini",       ["t6", "t8", "t22", "t23", "t24"]},
       {"b.b.c",               ["t6", "t10", "t13", "t18", "t22", "t23",
                                "t24", "t26"]},
       {"nothing.here.at.all", ["t6", "t22", "t23", "t24"]},
       {"oneword",             ["t6", "t22", "t23", "t24", "t25"]},
       {"args-test",           ["t6", "t22", "t23", "t24", "t25", "t27"]}]),

    %% remove the entire exchange
    exchange_op_callback(X, delete, [RemainingBindings]),
    %% none should match now
    test_topic_expect_match(X, [{"a.b.c", []}, {"b.b.c", []}, {"", []}]),
    passed.

exchange_op_callback(X, Fun, Args) ->
    rabbit_misc:execute_mnesia_transaction(
      fun () -> rabbit_exchange:callback(X, Fun, transaction, [X] ++ Args) end),
    rabbit_exchange:callback(X, Fun, none, [X] ++ Args).

test_topic_expect_match(X, List) ->
    lists:foreach(
      fun ({Key, Expected}) ->
              BinKey = list_to_binary(Key),
              Message = rabbit_basic:message(X#exchange.name, BinKey,
                                             #'P_basic'{}, <<>>),
              Res = rabbit_exchange_type_topic:route(
                      X, #delivery{mandatory = false,
                                   sender    = self(),
                                   message   = Message}),
              ExpectedRes = lists:map(
                              fun (Q) -> #resource{virtual_host = <<"/">>,
                                                   kind = queue,
                                                   name = list_to_binary(Q)}
                              end, Expected),
              true = (lists:usort(ExpectedRes) =:= lists:usort(Res))
      end, List).

%% ---------------------------------------------------------------------------
%% Unordered tests (originally from rabbit_tests.erl).
%% ---------------------------------------------------------------------------

confirms(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, confirms1, [Config]).

confirms1(_Config) ->
    {_Writer, Ch} = test_spawn(),
    DeclareBindDurableQueue =
        fun() ->
                rabbit_channel:do(Ch, #'queue.declare'{durable = true}),
                receive #'queue.declare_ok'{queue = Q0} ->
                        rabbit_channel:do(Ch, #'queue.bind'{
                                                 queue = Q0,
                                                 exchange = <<"amq.direct">>,
                                                 routing_key = "confirms-magic" }),
                        receive #'queue.bind_ok'{} -> Q0
                        after ?TIMEOUT -> throw(failed_to_bind_queue)
                        end
                after ?TIMEOUT -> throw(failed_to_declare_queue)
                end
        end,
    %% Declare and bind two queues
    QName1 = DeclareBindDurableQueue(),
    QName2 = DeclareBindDurableQueue(),
    %% Get the first one's pid (we'll crash it later)
    {ok, Q1} = rabbit_amqqueue:lookup(rabbit_misc:r(<<"/">>, queue, QName1)),
    QPid1 = Q1#amqqueue.pid,
    %% Enable confirms
    rabbit_channel:do(Ch, #'confirm.select'{}),
    receive
        #'confirm.select_ok'{} -> ok
    after ?TIMEOUT -> throw(failed_to_enable_confirms)
    end,
    %% Publish a message
    rabbit_channel:do(Ch, #'basic.publish'{exchange = <<"amq.direct">>,
                                           routing_key = "confirms-magic"
                                          },
                      rabbit_basic:build_content(
                        #'P_basic'{delivery_mode = 2}, <<"">>)),
    %% We must not kill the queue before the channel has processed the
    %% 'publish'.
    ok = rabbit_channel:flush(Ch),
    %% Crash the queue
    QPid1 ! boom,
    %% Wait for a nack
    receive
        #'basic.nack'{} -> ok;
        #'basic.ack'{}  -> throw(received_ack_instead_of_nack)
    after ?TIMEOUT-> throw(did_not_receive_nack)
    end,
    receive
        #'basic.ack'{} -> throw(received_ack_when_none_expected)
    after 1000 -> ok
    end,
    %% Cleanup
    rabbit_channel:do(Ch, #'queue.delete'{queue = QName2}),
    receive
        #'queue.delete_ok'{} -> ok
    after ?TIMEOUT -> throw(failed_to_cleanup_queue)
    end,
    unlink(Ch),
    ok = rabbit_channel:shutdown(Ch),

    passed.

gen_server2_with_state(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, gen_server2_with_state1, [Config]).

gen_server2_with_state1(_Config) ->
    fhc_state = gen_server2:with_state(file_handle_cache,
                                       fun (S) -> element(1, S) end),
    passed.

mcall(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, mcall1, [Config]).

mcall1(_Config) ->
    P1 = spawn(fun gs2_test_listener/0),
    register(foo, P1),
    global:register_name(gfoo, P1),

    P2 = spawn(fun() -> exit(bang) end),
    %% ensure P2 is dead (ignore the race setting up the monitor)
    await_exit(P2),

    P3 = spawn(fun gs2_test_crasher/0),

    %% since P2 crashes almost immediately and P3 after receiving its first
    %% message, we have to spawn a few more processes to handle the additional
    %% cases we're interested in here
    register(baz, spawn(fun gs2_test_crasher/0)),
    register(bog, spawn(fun gs2_test_crasher/0)),
    global:register_name(gbaz, spawn(fun gs2_test_crasher/0)),

    NoNode = rabbit_nodes:make("nonode"),

    Targets =
        %% pids
        [P1, P2, P3]
        ++
        %% registered names
        [foo, bar, baz]
        ++
        %% {Name, Node} pairs
        [{foo, node()}, {bar, node()}, {bog, node()}, {foo, NoNode}]
        ++
        %% {global, Name}
        [{global, gfoo}, {global, gbar}, {global, gbaz}],

    GoodResults = [{D, goodbye} || D <- [P1, foo,
                                         {foo, node()},
                                         {global, gfoo}]],

    BadResults  = [{P2,             noproc},   % died before use
                   {P3,             boom},     % died on first use
                   {bar,            noproc},   % never registered
                   {baz,            boom},     % died on first use
                   {{bar, node()},  noproc},   % never registered
                   {{bog, node()},  boom},     % died on first use
                   {{foo, NoNode},  nodedown}, % invalid node
                   {{global, gbar}, noproc},   % never registered globally
                   {{global, gbaz}, boom}],    % died on first use

    {Replies, Errors} = gen_server2:mcall([{T, hello} || T <- Targets]),
    true = lists:sort(Replies) == lists:sort(GoodResults),
    true = lists:sort(Errors)  == lists:sort(BadResults),

    %% cleanup (ignore the race setting up the monitor)
    P1 ! stop,
    await_exit(P1),
    passed.

await_exit(Pid) ->
    MRef = erlang:monitor(process, Pid),
    receive
        {'DOWN', MRef, _, _, _} -> ok
    end.

gs2_test_crasher() ->
    receive
        {'$gen_call', _From, hello} -> exit(boom)
    end.

gs2_test_listener() ->
    receive
        {'$gen_call', From, hello} ->
            gen_server2:reply(From, goodbye),
            gs2_test_listener();
        stop ->
            ok
    end.

configurable_server_properties(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, configurable_server_properties1, [Config]).

configurable_server_properties1(_Config) ->
    %% List of the names of the built-in properties do we expect to find
    BuiltInPropNames = [<<"product">>, <<"version">>, <<"platform">>,
                        <<"copyright">>, <<"information">>],

    Protocol = rabbit_framing_amqp_0_9_1,

    %% Verify that the built-in properties are initially present
    ActualPropNames = [Key || {Key, longstr, _} <-
                                  rabbit_reader:server_properties(Protocol)],
    true = lists:all(fun (X) -> lists:member(X, ActualPropNames) end,
                     BuiltInPropNames),

    %% Get the initial server properties configured in the environment
    {ok, ServerProperties} = application:get_env(rabbit, server_properties),

    %% Helper functions
    ConsProp = fun (X) -> application:set_env(rabbit,
                                              server_properties,
                                              [X | ServerProperties]) end,
    IsPropPresent =
        fun (X) ->
                lists:member(X, rabbit_reader:server_properties(Protocol))
        end,

    %% Add a wholly new property of the simplified {KeyAtom, StringValue} form
    NewSimplifiedProperty = {NewHareKey, NewHareVal} = {hare, "soup"},
    ConsProp(NewSimplifiedProperty),
    %% Do we find hare soup, appropriately formatted in the generated properties?
    ExpectedHareImage = {list_to_binary(atom_to_list(NewHareKey)),
                         longstr,
                         list_to_binary(NewHareVal)},
    true = IsPropPresent(ExpectedHareImage),

    %% Add a wholly new property of the {BinaryKey, Type, Value} form
    %% and check for it
    NewProperty = {<<"new-bin-key">>, signedint, -1},
    ConsProp(NewProperty),
    %% Do we find the new property?
    true = IsPropPresent(NewProperty),

    %% Add a property that clobbers a built-in, and verify correct clobbering
    {NewVerKey, NewVerVal} = NewVersion = {version, "X.Y.Z."},
    {BinNewVerKey, BinNewVerVal} = {list_to_binary(atom_to_list(NewVerKey)),
                                    list_to_binary(NewVerVal)},
    ConsProp(NewVersion),
    ClobberedServerProps = rabbit_reader:server_properties(Protocol),
    %% Is the clobbering insert present?
    true = IsPropPresent({BinNewVerKey, longstr, BinNewVerVal}),
    %% Is the clobbering insert the only thing with the clobbering key?
    [{BinNewVerKey, longstr, BinNewVerVal}] =
        [E || {K, longstr, _V} = E <- ClobberedServerProps, K =:= BinNewVerKey],

    application:set_env(rabbit, server_properties, ServerProperties),
    passed.

memory_high_watermark(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, memory_high_watermark1, [Config]).

memory_high_watermark1(_Config) ->
    %% set vm memory high watermark
    HWM = vm_memory_monitor:get_vm_memory_high_watermark(),
    %% this will trigger an alarm
    ok = control_action(set_vm_memory_high_watermark,
      ["absolute", "2000"]),
    [{{resource_limit,memory,_},[]}] = rabbit_alarm:get_alarms(),
    %% reset
    ok = control_action(set_vm_memory_high_watermark,
      [float_to_list(HWM)]),

    passed.

set_disk_free_limit_command(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, set_disk_free_limit_command1, [Config]).

set_disk_free_limit_command1(_Config) ->
    ok = control_action(set_disk_free_limit,
      ["2000kiB"]),
    2048000 = rabbit_disk_monitor:get_disk_free_limit(),
    ok = control_action(set_disk_free_limit,
      ["mem_relative", "1.1"]),
    ExpectedLimit = 1.1 * vm_memory_monitor:get_total_memory(),
    % Total memory is unstable, so checking order
    true = ExpectedLimit/rabbit_disk_monitor:get_disk_free_limit() < 1.2,
    true = ExpectedLimit/rabbit_disk_monitor:get_disk_free_limit() > 0.98,
    ok = control_action(set_disk_free_limit, ["50MB"]),
    passed.

disk_monitor(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, disk_monitor1, [Config]).

disk_monitor1(_Config) ->
    %% Issue: rabbitmq-server #91
    %% os module could be mocked using 'unstick', however it may have undesired
    %% side effects in following tests. Thus, we mock at rabbit_misc level
    ok = meck:new(rabbit_misc, [passthrough]),
    ok = meck:expect(rabbit_misc, os_cmd, fun(_) -> "\n" end),
    ok = rabbit_sup:stop_child(rabbit_disk_monitor_sup),
    ok = rabbit_sup:start_delayed_restartable_child(rabbit_disk_monitor, [1000]),
    meck:unload(rabbit_misc),
    passed.

disconnect_detected_during_alarm(Config) ->
    A = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    %% Set a low memory high watermark.
    rabbit_ct_broker_helpers:rabbitmqctl(Config, A,
      ["set_vm_memory_high_watermark", "0.000000001"]),

    %% Open a connection and a channel.
    Port = rabbit_ct_broker_helpers:get_node_config(Config, A, tcp_port_amqp),
    Heartbeat = 1,
    {ok, Conn} = amqp_connection:start(
      #amqp_params_network{port = Port,
                           heartbeat = Heartbeat}),
    {ok, Ch} = amqp_connection:open_channel(Conn),

    amqp_connection:register_blocked_handler(Conn, self()),
    Publish = #'basic.publish'{routing_key = <<"nowhere-to-go">>},
    amqp_channel:cast(Ch, Publish, #amqp_msg{payload = <<"foobar">>}),
    receive
        % Check that connection was indeed blocked
        #'connection.blocked'{} -> ok
    after
        1000 -> exit(connection_was_not_blocked)
    end,

    %% Connection is blocked, now we should forcefully kill it
    {'EXIT', _} = (catch amqp_connection:close(Conn, 10)),

    ListConnections =
        fun() ->
            rpc:call(A, rabbit_networking, connection_info_all, [])
        end,

    %% We've already disconnected, but blocked connection still should still linger on.
    [SingleConn] = ListConnections(),
    blocked = rabbit_misc:pget(state, SingleConn),

    %% It should definitely go away after 2 heartbeat intervals.
    timer:sleep(round(2.5 * 1000 * Heartbeat)),
    [] = ListConnections(),

    passed.

%% ---------------------------------------------------------------------------
%% Cluster-dependent tests.
%% ---------------------------------------------------------------------------

delegates_async(Config) ->
    {I, J} = ?config(test_direction, Config),
    From = rabbit_ct_broker_helpers:get_node_config(Config, I, nodename),
    To = rabbit_ct_broker_helpers:get_node_config(Config, J, nodename),
    rabbit_ct_broker_helpers:add_code_path_to_node(To, ?MODULE),
    passed = rabbit_ct_broker_helpers:rpc(Config,
      From, ?MODULE, delegates_async1, [Config, To]).

delegates_async1(_Config, SecondaryNode) ->
    Self = self(),
    Sender = fun (Pid) -> Pid ! {invoked, Self} end,

    Responder = make_responder(fun ({invoked, Pid}) -> Pid ! response end),

    ok = delegate:invoke_no_result(spawn(Responder), Sender),
    ok = delegate:invoke_no_result(spawn(SecondaryNode, Responder), Sender),
    await_response(2),

    LocalPids = spawn_responders(node(), Responder, 10),
    RemotePids = spawn_responders(SecondaryNode, Responder, 10),
    ok = delegate:invoke_no_result(LocalPids ++ RemotePids, Sender),
    await_response(20),

    passed.

delegates_sync(Config) ->
    {I, J} = ?config(test_direction, Config),
    From = rabbit_ct_broker_helpers:get_node_config(Config, I, nodename),
    To = rabbit_ct_broker_helpers:get_node_config(Config, J, nodename),
    rabbit_ct_broker_helpers:add_code_path_to_node(To, ?MODULE),
    passed = rabbit_ct_broker_helpers:rpc(Config,
      From, ?MODULE, delegates_sync1, [Config, To]).

delegates_sync1(_Config, SecondaryNode) ->
    Sender = fun (Pid) -> gen_server:call(Pid, invoked, infinity) end,
    BadSender = fun (_Pid) -> exit(exception) end,

    Responder = make_responder(fun ({'$gen_call', From, invoked}) ->
                                       gen_server:reply(From, response)
                               end),

    BadResponder = make_responder(fun ({'$gen_call', From, invoked}) ->
                                          gen_server:reply(From, response)
                                  end, bad_responder_died),

    response = delegate:invoke(spawn(Responder), Sender),
    response = delegate:invoke(spawn(SecondaryNode, Responder), Sender),

    must_exit(fun () -> delegate:invoke(spawn(BadResponder), BadSender) end),
    must_exit(fun () ->
                      delegate:invoke(spawn(SecondaryNode, BadResponder), BadSender) end),

    LocalGoodPids = spawn_responders(node(), Responder, 2),
    RemoteGoodPids = spawn_responders(SecondaryNode, Responder, 2),
    LocalBadPids = spawn_responders(node(), BadResponder, 2),
    RemoteBadPids = spawn_responders(SecondaryNode, BadResponder, 2),

    {GoodRes, []} = delegate:invoke(LocalGoodPids ++ RemoteGoodPids, Sender),
    true = lists:all(fun ({_, response}) -> true end, GoodRes),
    GoodResPids = [Pid || {Pid, _} <- GoodRes],

    Good = lists:usort(LocalGoodPids ++ RemoteGoodPids),
    Good = lists:usort(GoodResPids),

    {[], BadRes} = delegate:invoke(LocalBadPids ++ RemoteBadPids, BadSender),
    true = lists:all(fun ({_, {exit, exception, _}}) -> true end, BadRes),
    BadResPids = [Pid || {Pid, _} <- BadRes],

    Bad = lists:usort(LocalBadPids ++ RemoteBadPids),
    Bad = lists:usort(BadResPids),

    MagicalPids = [rabbit_misc:string_to_pid(Str) ||
                      Str <- ["<nonode@nohost.0.1.0>", "<nonode@nohost.0.2.0>"]],
    {[], BadNodes} = delegate:invoke(MagicalPids, Sender),
    true = lists:all(
             fun ({_, {exit, {nodedown, nonode@nohost}, _Stack}}) -> true end,
             BadNodes),
    BadNodesPids = [Pid || {Pid, _} <- BadNodes],

    Magical = lists:usort(MagicalPids),
    Magical = lists:usort(BadNodesPids),

    passed.

queue_cleanup(Config) ->
    {I, J} = ?config(test_direction, Config),
    From = rabbit_ct_broker_helpers:get_node_config(Config, I, nodename),
    To = rabbit_ct_broker_helpers:get_node_config(Config, J, nodename),
    rabbit_ct_broker_helpers:add_code_path_to_node(To, ?MODULE),
    passed = rabbit_ct_broker_helpers:rpc(Config,
      From, ?MODULE, queue_cleanup1, [Config, To]).

queue_cleanup1(_Config, _SecondaryNode) ->
    {_Writer, Ch} = test_spawn(),
    rabbit_channel:do(Ch, #'queue.declare'{ queue = ?CLEANUP_QUEUE_NAME }),
    receive #'queue.declare_ok'{queue = ?CLEANUP_QUEUE_NAME} ->
            ok
    after ?TIMEOUT -> throw(failed_to_receive_queue_declare_ok)
    end,
    rabbit_channel:shutdown(Ch),
    rabbit:stop(),
    rabbit:start(),
    {_Writer2, Ch2} = test_spawn(),
    rabbit_channel:do(Ch2, #'queue.declare'{ passive = true,
                                             queue   = ?CLEANUP_QUEUE_NAME }),
    receive
        #'channel.close'{reply_code = ?NOT_FOUND} ->
            ok
    after ?TIMEOUT -> throw(failed_to_receive_channel_exit)
    end,
    rabbit_channel:shutdown(Ch2),
    passed.

declare_on_dead_queue(Config) ->
    {I, J} = ?config(test_direction, Config),
    From = rabbit_ct_broker_helpers:get_node_config(Config, I, nodename),
    To = rabbit_ct_broker_helpers:get_node_config(Config, J, nodename),
    rabbit_ct_broker_helpers:add_code_path_to_node(To, ?MODULE),
    passed = rabbit_ct_broker_helpers:rpc(Config,
      From, ?MODULE, declare_on_dead_queue1, [Config, To]).

declare_on_dead_queue1(_Config, SecondaryNode) ->
    QueueName = rabbit_misc:r(<<"/">>, queue, ?CLEANUP_QUEUE_NAME),
    Self = self(),
    Pid = spawn(SecondaryNode,
                fun () ->
                        {new, #amqqueue{name = QueueName, pid = QPid}} =
                            rabbit_amqqueue:declare(QueueName, false, false, [],
                                                    none),
                        exit(QPid, kill),
                        Self ! {self(), killed, QPid}
                end),
    receive
        {Pid, killed, OldPid} ->
            Q = dead_queue_loop(QueueName, OldPid),
            {ok, 0} = rabbit_amqqueue:delete(Q, false, false),
            passed
    after ?TIMEOUT -> throw(failed_to_create_and_kill_queue)
    end.

refresh_events(Config) ->
    {I, J} = ?config(test_direction, Config),
    From = rabbit_ct_broker_helpers:get_node_config(Config, I, nodename),
    To = rabbit_ct_broker_helpers:get_node_config(Config, J, nodename),
    rabbit_ct_broker_helpers:add_code_path_to_node(To, ?MODULE),
    passed = rabbit_ct_broker_helpers:rpc(Config,
      From, ?MODULE, refresh_events1, [Config, To]).

refresh_events1(Config, SecondaryNode) ->
    dummy_event_receiver:start(self(), [node(), SecondaryNode],
                               [channel_created, queue_created]),

    {_Writer, Ch} = test_spawn(),
    expect_events(pid, Ch, channel_created),
    rabbit_channel:shutdown(Ch),

    {_Writer2, Ch2} = test_spawn(SecondaryNode),
    expect_events(pid, Ch2, channel_created),
    rabbit_channel:shutdown(Ch2),

    {new, #amqqueue{name = QName} = Q} =
        rabbit_amqqueue:declare(queue_name(Config, <<"refresh_events-q">>),
                                false, false, [], none),
    expect_events(name, QName, queue_created),
    rabbit_amqqueue:delete(Q, false, false),

    dummy_event_receiver:stop(),
    passed.

make_responder(FMsg) -> make_responder(FMsg, timeout).
make_responder(FMsg, Throw) ->
    fun () ->
            receive Msg -> FMsg(Msg)
            after ?TIMEOUT -> throw(Throw)
            end
    end.

spawn_responders(Node, Responder, Count) ->
    [spawn(Node, Responder) || _ <- lists:seq(1, Count)].

await_response(0) ->
    ok;
await_response(Count) ->
    receive
        response -> ok,
                    await_response(Count - 1)
    after ?TIMEOUT -> throw(timeout)
    end.

must_exit(Fun) ->
    try
        Fun(),
        throw(exit_not_thrown)
    catch
        exit:_ -> ok
    end.

dead_queue_loop(QueueName, OldPid) ->
    {existing, Q} = rabbit_amqqueue:declare(QueueName, false, false, [], none),
    case Q#amqqueue.pid of
        OldPid -> timer:sleep(25),
                  dead_queue_loop(QueueName, OldPid);
        _      -> true = rabbit_misc:is_process_alive(Q#amqqueue.pid),
                  Q
    end.

expect_events(Tag, Key, Type) ->
    expect_event(Tag, Key, Type),
    rabbit:force_event_refresh(make_ref()),
    expect_event(Tag, Key, Type).

expect_event(Tag, Key, Type) ->
    receive #event{type = Type, props = Props} ->
            case rabbit_misc:pget(Tag, Props) of
                Key -> ok;
                _   -> expect_event(Tag, Key, Type)
            end
    after ?TIMEOUT -> throw({failed_to_receive_event, Type})
    end.

%% ---------------------------------------------------------------------------
%% rabbitmqctl helpers.
%% ---------------------------------------------------------------------------

control_action(Command, Args) ->
    control_action(Command, node(), Args, default_options()).

control_action(Command, Args, NewOpts) ->
    control_action(Command, node(), Args,
                   expand_options(default_options(), NewOpts)).

control_action(Command, Node, Args, Opts) ->
    case catch rabbit_control_main:action(
                 Command, Node, Args, Opts,
                 fun (Format, Args1) ->
                         io:format(Format ++ " ...~n", Args1)
                 end) of
        ok ->
            io:format("done.~n"),
            ok;
        {ok, Result} ->
            rabbit_control_misc:print_cmd_result(Command, Result),
            ok;
        Other ->
            io:format("failed: ~p~n", [Other]),
            Other
    end.

control_action_t(Command, Args, Timeout) when is_number(Timeout) ->
    control_action_t(Command, node(), Args, default_options(), Timeout).

control_action_t(Command, Args, NewOpts, Timeout) when is_number(Timeout) ->
    control_action_t(Command, node(), Args,
                     expand_options(default_options(), NewOpts),
                     Timeout).

control_action_t(Command, Node, Args, Opts, Timeout) when is_number(Timeout) ->
    case catch rabbit_control_main:action(
                 Command, Node, Args, Opts,
                 fun (Format, Args1) ->
                         io:format(Format ++ " ...~n", Args1)
                 end, Timeout) of
        ok ->
            io:format("done.~n"),
            ok;
        {ok, Result} ->
            rabbit_control_misc:print_cmd_result(Command, Result),
            ok;
        Other ->
            io:format("failed: ~p~n", [Other]),
            Other
    end.

control_action_opts(Raw) ->
    NodeStr = atom_to_list(node()),
    case rabbit_control_main:parse_arguments(Raw, NodeStr) of
        {ok, {Cmd, Opts, Args}} ->
            case control_action(Cmd, node(), Args, Opts) of
                ok    -> ok;
                Error -> Error
            end;
        Error ->
            Error
    end.

info_action(Command, Args, CheckVHost) ->
    ok = control_action(Command, []),
    if CheckVHost -> ok = control_action(Command, [], ["-p", "/"]);
       true       -> ok
    end,
    ok = control_action(Command, lists:map(fun atom_to_list/1, Args)),
    {bad_argument, dummy} = control_action(Command, ["dummy"]),
    ok.

info_action_t(Command, Args, CheckVHost, Timeout) when is_number(Timeout) ->
    if CheckVHost -> ok = control_action_t(Command, [], ["-p", "/"], Timeout);
       true       -> ok
    end,
    ok = control_action_t(Command, lists:map(fun atom_to_list/1, Args), Timeout),
    ok.

default_options() -> [{"-p", "/"}, {"-q", "false"}].

expand_options(As, Bs) ->
    lists:foldl(fun({K, _}=A, R) ->
                        case proplists:is_defined(K, R) of
                            true -> R;
                            false -> [A | R]
                        end
                end, Bs, As).
