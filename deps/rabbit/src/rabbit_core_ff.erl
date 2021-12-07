%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2018-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_core_ff).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/logging.hrl").

-include("vhost.hrl").
-include("internal_user.hrl").

-export([quorum_queue_migration/3,
         stream_queue_migration/3,
         implicit_default_bindings_migration/3,
         virtual_host_metadata_migration/3,
         maintenance_mode_status_migration/3,
         user_limits_migration/3,
         mds_phase1_migration/3]).

-rabbit_feature_flag(
   {quorum_queue,
    #{desc          => "Support queues of type `quorum`",
      doc_url       => "https://www.rabbitmq.com/quorum-queues.html",
      stability     => stable,
      migration_fun => {?MODULE, quorum_queue_migration}
     }}).

-rabbit_feature_flag(
   {stream_queue,
    #{desc          => "Support queues of type `stream`",
      doc_url       => "https://www.rabbitmq.com/stream.html",
      stability     => stable,
      depends_on    => [quorum_queue],
      migration_fun => {?MODULE, stream_queue_migration}
     }}).

-rabbit_feature_flag(
   {implicit_default_bindings,
    #{desc          => "Default bindings are now implicit, instead of "
                       "being stored in the database",
      stability     => stable,
      migration_fun => {?MODULE, implicit_default_bindings_migration}
     }}).

-rabbit_feature_flag(
   {virtual_host_metadata,
    #{desc          => "Virtual host metadata (description, tags, etc)",
      stability     => stable,
      migration_fun => {?MODULE, virtual_host_metadata_migration}
     }}).

-rabbit_feature_flag(
   {maintenance_mode_status,
    #{desc          => "Maintenance mode status",
      stability     => stable,
      migration_fun => {?MODULE, maintenance_mode_status_migration}
     }}).

-rabbit_feature_flag(
    {user_limits,
     #{desc          => "Configure connection and channel limits for a user",
       stability     => stable,
       migration_fun => {?MODULE, user_limits_migration}
     }}).

-rabbit_feature_flag(
   {raft_based_metadata_store_phase1,
    #{desc          => "Use the new Raft-based metadata store [phase 1]",
      doc_url       => "", %% TODO
      stability     => experimental,
      depends_on    => [maintenance_mode_status,
                        user_limits,
                        virtual_host_metadata],
      migration_fun => {?MODULE, mds_phase1_migration}
     }}).

%% -------------------------------------------------------------------
%% Quorum queues.
%% -------------------------------------------------------------------

-define(quorum_queue_tables, [rabbit_queue,
                              rabbit_durable_queue]).

quorum_queue_migration(FeatureName, _FeatureProps, enable) ->
    Tables = ?quorum_queue_tables,
    rabbit_table:wait(Tables, _Retry = true),
    Fields = amqqueue:fields(amqqueue_v2),
    migrate_to_amqqueue_with_type(FeatureName, Tables, Fields);
quorum_queue_migration(_FeatureName, _FeatureProps, is_enabled) ->
    Tables = ?quorum_queue_tables,
    rabbit_table:wait(Tables, _Retry = true),
    Fields = amqqueue:fields(amqqueue_v2),
    mnesia:table_info(rabbit_queue, attributes) =:= Fields andalso
    mnesia:table_info(rabbit_durable_queue, attributes) =:= Fields;
quorum_queue_migration(_FeatureName, _FeatureProps, post_enabled_locally) ->
    ok.

stream_queue_migration(_FeatureName, _FeatureProps, _Enable) ->
    ok.

migrate_to_amqqueue_with_type(FeatureName, [Table | Rest], Fields) ->
    rabbit_log_feature_flags:info(
      "Feature flag `~s`:   migrating Mnesia table ~s...",
      [FeatureName, Table]),
    Fun = fun(Queue) -> amqqueue:upgrade_to(amqqueue_v2, Queue) end,
    case mnesia:transform_table(Table, Fun, Fields) of
        {atomic, ok}      -> migrate_to_amqqueue_with_type(FeatureName,
                                                           Rest,
                                                           Fields);
        {aborted, Reason} -> {error, Reason}
    end;
migrate_to_amqqueue_with_type(FeatureName, [], _) ->
    rabbit_log_feature_flags:info(
      "Feature flag `~s`:   Mnesia tables migration done",
      [FeatureName]),
    ok.

%% -------------------------------------------------------------------
%% Default bindings.
%% -------------------------------------------------------------------

implicit_default_bindings_migration(FeatureName, _FeatureProps,
                                    enable) ->
    %% Default exchange bindings are now implicit (not stored in the
    %% route tables). It should be safe to remove them outside of a
    %% transaction.
    rabbit_table:wait([rabbit_queue]),
    Queues = mnesia:dirty_all_keys(rabbit_queue),
    remove_explicit_default_bindings(FeatureName, Queues);
implicit_default_bindings_migration(_FeatureName, _FeatureProps,
                                    is_enabled) ->
    undefined;
implicit_default_bindings_migration(_FeatureName, _FeatureProps,
                                    post_enabled_locally) ->
    ok.

remove_explicit_default_bindings(_FeatureName, []) ->
    ok;
remove_explicit_default_bindings(FeatureName, Queues) ->
    rabbit_log_feature_flags:info(
      "Feature flag `~s`:   deleting explicit default bindings "
      "for ~b queues (it may take some time)...",
      [FeatureName, length(Queues)]),
    [rabbit_binding:remove_default_exchange_binding_rows_of(Q)
     || Q <- Queues],
    ok.

%% -------------------------------------------------------------------
%% Virtual host metadata.
%% -------------------------------------------------------------------

virtual_host_metadata_migration(_FeatureName, _FeatureProps, enable) ->
    Tab = rabbit_vhost,
    rabbit_table:wait([Tab], _Retry = true),
    Fun = fun(Row) -> vhost:upgrade_to(vhost_v2, Row) end,
    case mnesia:transform_table(Tab, Fun, vhost:fields(vhost_v2)) of
        {atomic, ok}      -> ok;
        {aborted, Reason} -> {error, Reason}
    end;
virtual_host_metadata_migration(_FeatureName, _FeatureProps, is_enabled) ->
    try
        mnesia:table_info(rabbit_vhost, attributes) =:= vhost:fields(vhost_v2)
    catch
        exit:{aborted, {no_exists, rabbit_vhost, _}} ->
            %% If the `rabbit_vhost' table is gone, it means the
            %% `raft_based_metadata_store_phase1' feature is enabled and this
            %% flag too.
            true
    end;
virtual_host_metadata_migration(
  _FeatureName, _FeatureProps, post_enabled_locally) ->
    ok.

%% -------------------------------------------------------------------
%% Maintenance mode.
%% -------------------------------------------------------------------

maintenance_mode_status_migration(FeatureName, _FeatureProps, enable) ->
    TableName = rabbit_maintenance:status_table_name(),
    rabbit_log:info(
      "Creating table ~s for feature flag `~s`",
      [TableName, FeatureName]),
    try
        _ = rabbit_table:create(
              TableName,
              rabbit_maintenance:status_table_definition()),
        _ = rabbit_table:ensure_table_copy(TableName, node())
    catch throw:Reason  ->
        rabbit_log:error(
          "Failed to create maintenance status table: ~p",
          [Reason])
    end;
maintenance_mode_status_migration(_FeatureName, _FeatureProps, is_enabled) ->
    rabbit_table:exists(rabbit_maintenance:status_table_name());
maintenance_mode_status_migration(
  _FeatureName, _FeatureProps, post_enabled_locally) ->
    ok.

%% -------------------------------------------------------------------
%% User limits.
%% -------------------------------------------------------------------

user_limits_migration(_FeatureName, _FeatureProps, enable) ->
    Tab = rabbit_user,
    rabbit_table:wait([Tab], _Retry = true),
    Fun = fun(Row) -> internal_user:upgrade_to(internal_user_v2, Row) end,
    Ret = mnesia:transform_table(
            Tab, Fun, internal_user:fields(internal_user_v2)),
    case Ret of
        {atomic, ok}      -> ok;
        {aborted, Reason} -> {error, Reason}
    end;
user_limits_migration(_FeatureName, _FeatureProps, is_enabled) ->
    try
        mnesia:table_info(rabbit_user, attributes) =:=
        internal_user:fields(internal_user_v2)
    catch
        exit:{aborted, {no_exists, rabbit_user, _}} ->
            %% If the `rabbit_user' table is gone, it means the
            %% `raft_based_metadata_store_phase1' feature is enabled and this
            %% flag too.
            true
    end;
user_limits_migration(_FeatureName, _FeatureProps, post_enabled_locally) ->
    ok.

%% -------------------------------------------------------------------
%% Raft-based metadata store (phase 1).
%% -------------------------------------------------------------------

%% Phase 1 covers the migration of the following data:
%%     * virtual hosts
%%     * users and their permissions
%%     * runtime parameters
%% They all depend on each others in Mnesia transactions. That's why they must
%% be migrated atomically.

%% This table order is important. For instance, user permissions depend on
%% both vhosts and users to exist in the metadata store.
-define(MDS_PHASE1_TABLES, [rabbit_vhost,
                            rabbit_user,
                            rabbit_user_permission,
                            rabbit_topic_permission]).

mds_phase1_migration(_FeatureName, _FeatureProps, is_enabled) ->
    %% We don't check if the migration was done already because we also need
    %% to make sure the cluster membership of Khepri matches the Mnesia
    %% cluster.
    undefined;
mds_phase1_migration(FeatureName, _FeatureProps, enable) ->
    case ensure_khepri_cluster_matches_mnesia(FeatureName) of
        ok ->
            Tables = ?MDS_PHASE1_TABLES,
            case is_mds_migration_done(Tables) of
                false -> migrate_tables_to_khepri(FeatureName, Tables);
                true  -> ok
            end;
        Error ->
            Error
    end;
mds_phase1_migration(FeatureName, _FeatureProps, post_enabled_locally) ->
    ?assert(rabbit_khepri:is_enabled(non_blocking)),
    Tables = ?MDS_PHASE1_TABLES,
    empty_unused_mnesia_tables(FeatureName, Tables).

ensure_khepri_cluster_matches_mnesia(FeatureName) ->
    %% Initialize Khepri cluster based on Mnesia running nodes. Verify that
    %% all Mnesia nodes are running (all == running). It would be more
    %% difficult to add them later to the node when they start.
    ?LOG_DEBUG(
       "Feature flag `~s`:   ensure Khepri Ra system is running",
       [FeatureName]),
    ok = rabbit_khepri:setup(),
    ?LOG_DEBUG(
       "Feature flag `~s`:   making sure all Mnesia nodes are running",
       [FeatureName]),
    AllMnesiaNodes = lists:sort(rabbit_mnesia:cluster_nodes(all)),
    RunningMnesiaNodes = lists:sort(rabbit_mnesia:cluster_nodes(running)),
    MissingMnesiaNodes = AllMnesiaNodes -- RunningMnesiaNodes,
    case MissingMnesiaNodes of
        [] ->
            %% This is the first time Khepri will be used for real. Therefore
            %% we need to make sure the Khepri cluster matches the Mnesia
            %% cluster.
            ?LOG_DEBUG(
               "Feature flag `~s`:   updating the Khepri cluster to match "
               "the Mnesia cluster",
               [FeatureName]),
            case expand_khepri_cluster(FeatureName, AllMnesiaNodes) of
                ok ->
                    ok;
                Error ->
                    ?LOG_ERROR(
                       "Feature flag `~s`:   failed to migrate from Mnesia "
                       "to Khepri: failed to create Khepri cluster: ~p",
                       [FeatureName, Error]),
                    Error
            end;
        _ ->
            ?LOG_ERROR(
               "Feature flag `~s`:   failed to migrate from Mnesia to Khepri: "
               "all Mnesia nodes must run; the following nodes are missing: "
               "~p",
               [FeatureName, MissingMnesiaNodes]),
            {error, all_mnesia_nodes_must_run}
    end.

expand_khepri_cluster(FeatureName, AllMnesiaNodes) ->
    %% All Mnesia nodes are running (this is a requirement to enable this
    %% feature flag). We use this unique list of nodes to find the largest
    %% Khepri clusters among all of them.
    %%
    %% The idea is that at the beginning, each Mnesia node will also be an
    %% unclustered Khepri node. Therefore, the first node in the sorted list
    %% of Mnesia nodes will be picked (a "cluster" with 1 member, but the
    %% "largest" at the beginning).
    %%
    %% After the first nodes join that single node, its cluster will grow and
    %% will continue to be the largest.
    %%
    %% This function is executed on the node enabling the feature flag. It will
    %% take care of adding all nodes in the Mnesia cluster to a Khepri cluster
    %% (except those which are already part of it).
    %%
    %% This should avoid the situation where a large established cluster is
    %% reset and joins a single new/empty node.
    %%
    %% Also, we only consider Khepri clusters which are in use (i.e. the
    %% feature flag is enabled). Here is an example:
    %%     - Node2 is the only node in the Mnesia cluster at the time the
    %%       feature flag is enabled. It joins no other node and runs its own
    %%       one-node Khepri cluster.
    %%     - Node1 joins the Mnesia cluster which is now Node1 + Node2. Given
    %%       the sorting, Khepri clusters will be [[Node1], [Node2]] when
    %%       sorted by name and size. With this order, Node1 should "join"
    %%       itself. But the feature is not enabled yet on this node,
    %%       therefore, we skip this cluster to consider the following one,
    %%       [Node2].
    KhepriCluster = find_largest_khepri_cluster(FeatureName),
    add_nodes_to_khepri_cluster(FeatureName, KhepriCluster, AllMnesiaNodes).

add_nodes_to_khepri_cluster(FeatureName, KhepriCluster, [Node | Rest]) ->
    add_node_to_khepri_cluster(FeatureName, KhepriCluster, Node),
    add_nodes_to_khepri_cluster(FeatureName, KhepriCluster, Rest);
add_nodes_to_khepri_cluster(_FeatureName, _KhepriCluster, []) ->
    ok.

add_node_to_khepri_cluster(FeatureName, KhepriCluster, Node) ->
    ?assertNotEqual([], KhepriCluster),
    case lists:member(Node, KhepriCluster) of
        true ->
            ?LOG_DEBUG(
               "Feature flag `~s`:   node ~p is already a member of "
               "the largest cluster: ~p",
               [FeatureName, Node, KhepriCluster]),
            ok;
        false ->
            ?LOG_DEBUG(
               "Feature flag `~s`:   adding node ~p to the largest "
               "Khepri cluster found among Mnesia nodes: ~p",
               [FeatureName, Node, KhepriCluster]),
            case rabbit_khepri:add_member(Node, KhepriCluster) of
                ok                   -> ok;
                {ok, already_member} -> ok
            end
    end.

find_largest_khepri_cluster(FeatureName) ->
    case list_all_khepri_clusters(FeatureName) of
        [] ->
            [node()];
        KhepriClusters ->
            KhepriClustersBySize = sort_khepri_clusters_by_size(
                                     KhepriClusters),
            ?LOG_DEBUG(
               "Feature flag `~s`:   existing Khepri clusters (sorted by "
               "size): ~p",
               [FeatureName, KhepriClustersBySize]),
            LargestKhepriCluster = hd(KhepriClustersBySize),
            LargestKhepriCluster
    end.

list_all_khepri_clusters(FeatureName) ->
    MnesiaNodes = lists:sort(rabbit_mnesia:cluster_nodes(all)),
    ?LOG_DEBUG(
       "Feature flag `~s`:   querying the following Mnesia nodes to learn "
       "their Khepri cluster membership: ~p",
       [FeatureName, MnesiaNodes]),
    KhepriClusters = lists:foldl(
                       fun(MnesiaNode, Acc) ->
                               case khepri_cluster_on_node(MnesiaNode) of
                                   []        -> Acc;
                                   Cluster   -> Acc#{Cluster => true}
                               end
                       end, #{}, MnesiaNodes),
    lists:sort(maps:keys(KhepriClusters)).

sort_khepri_clusters_by_size(KhepriCluster) ->
    lists:sort(
      fun(A, B) -> length(A) >= length(B) end,
      KhepriCluster).

khepri_cluster_on_node(Node) ->
    lists:sort(
      rabbit_misc:rpc_call(Node, rabbit_khepri, nodes_if_khepri_enabled, [])).

migrate_tables_to_khepri(FeatureName, Tables) ->
    rabbit_table:wait(Tables, _Retry = true),
    ?LOG_NOTICE(
       "Feature flag `~s`:   starting migration from Mnesia "
       "to Khepri; expect decrease in performance and "
       "increase in memory footprint",
       [FeatureName]),
    Pid = spawn(
            fun() ->
                    migrate_tables_to_khepri_run(FeatureName, Tables)
            end),
    MonitorRef = erlang:monitor(process, Pid),
    receive
        {'DOWN', MonitorRef, process, Pid, normal} ->
            ?LOG_NOTICE(
               "Feature flag `~s`:   migration from Mnesia to Khepri "
               "finished",
               [FeatureName]),
            ok;
        {'DOWN', MonitorRef, process, Pid, Info} ->
            ?LOG_ERROR(
               "Feature flag `~s`:   "
               "failed to migrate Mnesia tables to Khepri:~n  ~p",
               [FeatureName, Info]),
            {error, {migration_failure, Info}}
    end.

migrate_tables_to_khepri_run(FeatureName, Tables) ->
    %% Clear data in Khepri which could come from a previously aborted copy
    %% attempt. The table list order is important so we need to reverse that
    %% order to clear the data.
    ?LOG_DEBUG(
       "Feature flag `~s`:   clear data from any aborted migration attempts "
       "(if any)",
       [FeatureName]),
    ok = clear_data_from_previous_attempt(FeatureName, lists:reverse(Tables)),

    %% Subscribe to Mnesia events: we want to know about all writes and
    %% deletions happening in parallel to the copy we are about to start.
    ?LOG_DEBUG(
       "Feature flag `~s`:   subscribe to Mnesia writes",
       [FeatureName]),
    ok = subscribe_to_mnesia_changes(FeatureName, Tables),

    %% Copy from Mnesia to Khepri. Tables are copied in a specific order to
    %% make sure that if term A depends on term B, term B was copied before.
    ?LOG_DEBUG(
       "Feature flag `~s`:   copy records from Mnesia to Khepri",
       [FeatureName]),
    ok = copy_from_mnesia_to_khepri(FeatureName, Tables),

    %% Mnesia transaction to handle received Mnesia events and tables removal.
    ?LOG_DEBUG(
       "Feature flag `~s`:   final sync and Mnesia table removal",
       [FeatureName]),
    ok = final_sync_from_mnesia_to_khepri(FeatureName, Tables),

    %% Unsubscribe to Mnesia events. All Mnesia tables are synchronized and
    %% read-only at this point.
    ?LOG_DEBUG(
       "Feature flag `~s`:   subscribe to Mnesia writes",
       [FeatureName]),
    ok = unsubscribe_to_mnesia_changes(FeatureName, Tables).

clear_data_from_previous_attempt(
  FeatureName, [rabbit_vhost | Rest]) ->
    ok = rabbit_vhost:clear_data_in_khepri(),
    clear_data_from_previous_attempt(FeatureName, Rest);
clear_data_from_previous_attempt(
  FeatureName, [rabbit_user | Rest]) ->
    ok = rabbit_auth_backend_internal:clear_data_in_khepri(),
    clear_data_from_previous_attempt(FeatureName, Rest);
clear_data_from_previous_attempt(
  FeatureName, [rabbit_user_permission | Rest]) ->
    clear_data_from_previous_attempt(FeatureName, Rest);
clear_data_from_previous_attempt(
  FeatureName, [rabbit_topic_permission | Rest]) ->
    clear_data_from_previous_attempt(FeatureName, Rest);
clear_data_from_previous_attempt(_, []) ->
    ok.

subscribe_to_mnesia_changes(FeatureName, [Table | Rest]) ->
    ?LOG_DEBUG(
       "Feature flag `~s`:     subscribe to writes to ~s",
       [FeatureName, Table]),
    case mnesia:subscribe({table, Table, simple}) of
        {ok, _} -> subscribe_to_mnesia_changes(FeatureName, Rest);
        Error   -> Error
    end;
subscribe_to_mnesia_changes(_, []) ->
    ok.

unsubscribe_to_mnesia_changes(FeatureName, [Table | Rest]) ->
    ?LOG_DEBUG(
       "Feature flag `~s`:     subscribe to writes to ~s",
       [FeatureName, Table]),
    case mnesia:unsubscribe({table, Table, simple}) of
        {ok, _} -> unsubscribe_to_mnesia_changes(FeatureName, Rest);
        Error   -> Error
    end;
unsubscribe_to_mnesia_changes(_, []) ->
    ok.

copy_from_mnesia_to_khepri(
  FeatureName, [rabbit_vhost = Table | Rest]) ->
    Fun = fun rabbit_vhost:mnesia_write_to_khepri/1,
    do_copy_from_mnesia_to_khepri(FeatureName, Table, Fun),
    copy_from_mnesia_to_khepri(FeatureName, Rest);
copy_from_mnesia_to_khepri(
  FeatureName, [rabbit_user = Table | Rest]) ->
    Fun = fun rabbit_auth_backend_internal:mnesia_write_to_khepri/1,
    do_copy_from_mnesia_to_khepri(FeatureName, Table, Fun),
    copy_from_mnesia_to_khepri(FeatureName, Rest);
copy_from_mnesia_to_khepri(
  FeatureName, [rabbit_user_permission = Table | Rest]) ->
    Fun = fun rabbit_auth_backend_internal:mnesia_write_to_khepri/1,
    do_copy_from_mnesia_to_khepri(FeatureName, Table, Fun),
    copy_from_mnesia_to_khepri(FeatureName, Rest);
copy_from_mnesia_to_khepri(
  FeatureName, [rabbit_topic_permission = Table | Rest]) ->
    Fun = fun rabbit_auth_backend_internal:mnesia_write_to_khepri/1,
    do_copy_from_mnesia_to_khepri(FeatureName, Table, Fun),
    copy_from_mnesia_to_khepri(FeatureName, Rest);
copy_from_mnesia_to_khepri(_, []) ->
    ok.

do_copy_from_mnesia_to_khepri(FeatureName, Table, Fun) ->
    Count = mnesia:table_info(Table, size),
    ?LOG_DEBUG(
       "Feature flag `~s`:     table ~s: about ~b record(s) to copy",
       [FeatureName, Table, Count]),
    FirstKey = mnesia:dirty_first(Table),
    do_copy_from_mnesia_to_khepri(
      FeatureName, Table, FirstKey, Fun, Count, 0).

do_copy_from_mnesia_to_khepri(
  FeatureName, Table, '$end_of_table', _, Count, Copied) ->
    ?LOG_DEBUG(
       "Feature flag `~s`:     table ~s: copy of ~b record(s) (out of ~b "
       "initially) finished",
       [FeatureName, Table, Copied, Count]),
    ok;
do_copy_from_mnesia_to_khepri(
  FeatureName, Table, Key, Fun, Count, Copied) ->
    %% TODO: Batch several records in a single Khepri insert.
    %% TODO: Can/should we parallelize?
    case Copied rem 100 of
        0 ->
            ?LOG_DEBUG(
               "Feature flag `~s`:     table ~s: copying record ~b/~b",
               [FeatureName, Table, Copied, Count]);
        _ ->
            ok
    end,
    case mnesia:dirty_read(Table, Key) of
        [Record] -> ok = Fun(Record);
        []       -> ok
    end,
    NextKey = mnesia:dirty_next(Table, Key),
    do_copy_from_mnesia_to_khepri(
      FeatureName, Table, NextKey, Fun, Count, Copied + 1).

final_sync_from_mnesia_to_khepri(FeatureName, Tables) ->
    %% Switch all tables to read-only. All concurrent and future Mnesia
    %% transaction involving a write to one of them will fail with the
    %% `{no_exists, Table}` exception.
    lists:foreach(
      fun(Table) ->
              ?LOG_DEBUG(
                 "Feature flag `~s`:     switch table ~s to read-only",
                 [FeatureName, Table]),
              {atomic, ok} = mnesia:change_table_access_mode(Table, read_only)
      end, Tables),

    %% During the first round of copy, we received all write events as
    %% messages (parallel writes were authorized). Now, we want to consume
    %% those messages to record the writes we probably missed.
    ok = consume_mnesia_events(FeatureName),

    ok.

consume_mnesia_events(FeatureName) ->
    {_, Count} = erlang:process_info(self(), message_queue_len),
    ?LOG_DEBUG(
       "Feature flag `~s`:     handling queued Mnesia events "
       "(about ~b events)",
       [FeatureName, Count]),
    consume_mnesia_events(FeatureName, Count, 0).

consume_mnesia_events(FeatureName, Count, Handled) ->
    %% TODO: Batch several events in a single Khepri command.
    Handled1 = Handled + 1,
    receive
        {mnesia_table_event, {write, NewRecord, _}} ->
            ?LOG_DEBUG(
               "Feature flag `~s`:       handling event ~b/~b (write)",
               [FeatureName, Handled1, Count]),
            handle_mnesia_write(NewRecord),
            consume_mnesia_events(FeatureName, Count, Handled1);
        {mnesia_table_event, {delete_object, OldRecord, _}} ->
            ?LOG_DEBUG(
               "Feature flag `~s`:       handling event ~b/~b (delete)",
               [FeatureName, Handled1, Count]),
            handle_mnesia_delete(OldRecord),
            consume_mnesia_events(FeatureName, Count, Handled1);
        {mnesia_table_event, {delete, {Table, Key}, _}} ->
            ?LOG_DEBUG(
               "Feature flag `~s`:       handling event ~b/~b (delete)",
               [FeatureName, Handled1, Count]),
            handle_mnesia_delete(Table, Key),
            consume_mnesia_events(FeatureName, Count, Handled1)
    after 0 ->
              {_, MsgCount} = erlang:process_info(self(), message_queue_len),
              ?LOG_DEBUG(
                 "Feature flag `~s`:     ~b messages remaining",
                 [FeatureName, MsgCount]),
              %% TODO: Wait for confirmation from Khepri.
              ok
    end.

handle_mnesia_write(NewRecord) when ?is_vhost(NewRecord) ->
    rabbit_vhost:mnesia_write_to_khepri(NewRecord);
handle_mnesia_write(NewRecord) when is_record(NewRecord, user_permission) ->
    rabbit_auth_backend_internal:mnesia_write_to_khepri(NewRecord);
handle_mnesia_write(NewRecord) when is_record(NewRecord, topic_permission) ->
    rabbit_auth_backend_internal:mnesia_write_to_khepri(NewRecord);
handle_mnesia_write(NewRecord) ->
    %% The record and the Mnesia table have different names.
    NewRecord1 = erlang:setelement(1, NewRecord, internal_user),
    true = ?is_internal_user(NewRecord1),
    rabbit_auth_backend_internal:mnesia_write_to_khepri(NewRecord1).

handle_mnesia_delete(OldRecord) when ?is_vhost(OldRecord) ->
    rabbit_vhost:mnesia_delete_to_khepri(OldRecord);
handle_mnesia_delete(OldRecord) when ?is_internal_user(OldRecord) ->
    rabbit_auth_backend_internal:mnesia_delete_to_khepri(OldRecord);
handle_mnesia_delete(OldRecord) when is_record(OldRecord, user_permission) ->
    rabbit_auth_backend_internal:mnesia_delete_to_khepri(OldRecord);
handle_mnesia_delete(OldRecord) when is_record(OldRecord, topic_permission) ->
    rabbit_auth_backend_internal:mnesia_delete_to_khepri(OldRecord).

handle_mnesia_delete(rabbit_vhost, VHost) ->
    rabbit_vhost:mnesia_delete_to_khepri(VHost);
handle_mnesia_delete(rabbit_user, Username) ->
    rabbit_auth_backend_internal:mnesia_delete_to_khepri(Username);
handle_mnesia_delete(rabbit_user_permission, UserVHost) ->
    rabbit_auth_backend_internal:mnesia_delete_to_khepri(UserVHost);
handle_mnesia_delete(rabbit_topic_permission, TopicPermissionKey) ->
    rabbit_auth_backend_internal:mnesia_delete_to_khepri(TopicPermissionKey).

%% We can't remove unused tables at this point yet. The reason is that tables
%% are synchronized before feature flags in `rabbit_mnesia`. So if a node is
%% already using Khepri and another node wants to join him, but is using Mnesia
%% only, it will hang while trying to sync the dropped tables.
%%
%% We can't simply reverse the two steps (i.e. synchronize feature flags before
%% tables) because some feature flags like `quorum_queue` need tables to modify
%% their schema.
%%
%% Another solution would be to have two groups of feature flags, depending on
%% whether a feature flag should be synchronized before or after Mnesia
%% tables.
%%
%% But for now, let's just empty the tables, add a forged record to mark them
%% as migrated and leave them around.

empty_unused_mnesia_tables(FeatureName, [Table | Rest]) ->
    %% The feature flag is enabled at this point. It means there should be no
    %% code trying to read or write the Mnesia tables.
    case mnesia:change_table_access_mode(Table, read_write) of
        {atomic, ok} ->
            ?LOG_DEBUG(
               "Feature flag `~s`:   dropping content from unused Mnesia "
               "table ~s",
               [FeatureName, Table]),
            ok = empty_unused_mnesia_table(Table),
            ok = write_migrated_mark_to_mnesia(FeatureName, Table),
            ?assert(is_table_migrated(Table));
        {aborted, {already_exists, Table, _}} ->
            %% Another node is already taking care of this table.
            ?LOG_DEBUG(
               "Feature flag `~s`:   Mnesia table ~s already emptied",
               [FeatureName, Table]),
            ok
    end,
    empty_unused_mnesia_tables(FeatureName, Rest);
empty_unused_mnesia_tables(FeatureName, []) ->
            ?LOG_DEBUG(
               "Feature flag `~s`:   done with emptying unused Mnesia tables",
               [FeatureName]),
            ok.

empty_unused_mnesia_table(Table) ->
    FirstKey = mnesia:dirty_first(Table),
    empty_unused_mnesia_table(Table, FirstKey).

empty_unused_mnesia_table(_Table, '$end_of_table') ->
    ok;
empty_unused_mnesia_table(Table, Key) ->
    NextKey = mnesia:dirty_next(Table, Key),
    ok = mnesia:dirty_delete(Table, Key),
    empty_unused_mnesia_table(Table, NextKey).

-define(MDS_MIGRATION_MARK_KEY, '$migrated_to_khepri').

write_migrated_mark_to_mnesia(FeatureName, Table) ->
    TableDefs = rabbit_table:definitions(),
    TableDef = proplists:get_value(Table, TableDefs),
    Match = proplists:get_value(match, TableDef),
    ForgedRecord = create_migrated_marker(Match),
    ?LOG_DEBUG(
       "Feature flag `~s`:   write a forged record to Mnesia table ~s to "
       "mark it as migrated",
       [FeatureName, Table]),
    mnesia:dirty_write(Table, ForgedRecord).

create_migrated_marker(Record) ->
    insert_migrated_marker(Record).

insert_migrated_marker('_') ->
    ?MDS_MIGRATION_MARK_KEY;
insert_migrated_marker(Atom) when is_atom(Atom) ->
    Atom;
insert_migrated_marker(Tuple) when is_tuple(Tuple) ->
    Fields = tuple_to_list(Tuple),
    Fields1 = [insert_migrated_marker(Field) || Field <- Fields],
    list_to_tuple(Fields1).

is_mds_migration_done(Tables) ->
    lists:all(fun is_table_migrated/1, Tables).

is_table_migrated(Table) ->
    case mnesia:table_info(Table, size) of
        1 ->
            Key = mnesia:dirty_first(Table),
            is_migration_marker(Key);
        _ ->
            false
    end.

is_migration_marker(?MDS_MIGRATION_MARK_KEY) ->
    true;
is_migration_marker(Tuple) when is_tuple(Tuple) ->
    Fields = tuple_to_list(Tuple),
    lists:any(fun is_migration_marker/1, Fields);
is_migration_marker(_) ->
    false.
