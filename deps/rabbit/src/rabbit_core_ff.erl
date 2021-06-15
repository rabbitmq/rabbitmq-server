%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2018-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_core_ff).

-include_lib("kernel/include/logger.hrl").
-include_lib("eunit/include/eunit.hrl").

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/logging.hrl").

-include("include/vhost.hrl").
-include("include/internal_user.hrl").

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
      doc_url       => "https://www.rabbitmq.com/stream-queues.html",
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
      depends_on    => [virtual_host_metadata, user_limits],
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
    mnesia:table_info(rabbit_vhost, attributes) =:= vhost:fields(vhost_v2);
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
    mnesia:table_info(rabbit_user, attributes) =:=
    internal_user:fields(internal_user_v2);
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
-define(mds_phase1_tables, [rabbit_vhost,
                            rabbit_user,
                            rabbit_user_permission,
                            rabbit_topic_permission]).

mds_phase1_migration(FeatureName, _FeatureProps, is_enabled) ->
    %% Migrated tables do not exist anymore.
    lists:all(
      fun(Table) ->
              case does_table_exist(Table) of
                  true ->
                      ?LOG_DEBUG(
                         "Feature flag `~s`:   table ~s exists, feature flag "
                         "disabled",
                         [FeatureName, Table]),
                      false;
                  false ->
                      true
              end
      end, ?mds_phase1_tables);
mds_phase1_migration(FeatureName, _FeatureProps, enable) ->
    %% Initialize Khepri cluster based on Mnesia running nodes. Verify that
    %% all Mnesia nodes are running (all == running). It would be more
    %% difficult to add them later to the node when they start.
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
            %%
            %% Even if some nodes are already part of the Khepri cluster, we
            %% reset every nodes (except the one running this function) and
            %% add them to this node's Khepri cluster.
            KhepriNodes = lists:sort(rabbit_khepri:nodes()),
            OtherMnesiaNodes = AllMnesiaNodes -- KhepriNodes,
            ?LOG_DEBUG(
               "Feature flag `~s`:   updating the Khepri cluster to match the "
               "Mnesia cluster by adding the following nodes: ~p",
               [FeatureName, OtherMnesiaNodes]),
            case expand_khepri_cluster(OtherMnesiaNodes) of
                ok ->
                    Tables = ?mds_phase1_tables,
                    rabbit_table:wait(Tables, _Retry = true),
                    ?LOG_NOTICE(
                       "Feature flag `~s`:   starting migration from Mnesia "
                       "to Khepri; expect decrease in performance and "
                       "increase in memory footprint",
                       [FeatureName]),
                    Ret = migrate_tables_to_khepri(FeatureName, Tables),
                    ?LOG_NOTICE(
                       "Feature flag `~s`:   migration from Mnesia to Khepri "
                       "finished",
                       [FeatureName]),
                    Ret;
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
    end;
mds_phase1_migration(FeatureName, _FeatureProps, post_enabled_locally) ->
    ?assert(rabbit_khepri:is_enabled(non_blocking)),
    drop_unused_mnesia_tables(FeatureName, ?mds_phase1_tables).

expand_khepri_cluster([MnesiaNode | Rest]) ->
    case rabbit_khepri:add_member(MnesiaNode) of
        ok    -> expand_khepri_cluster(Rest);
        Error -> Error
    end;
expand_khepri_cluster([]) ->
    ok.

does_table_exist(Table) ->
    try
        _ = mnesia:table_info(Table, type),
        true
    catch
        exit:{aborted, {no_exists, Table, type}} ->
            false
    end.

migrate_tables_to_khepri(FeatureName, Tables) ->
    Pid = spawn(
            fun() -> migrate_tables_to_khepri_run(FeatureName, Tables) end),
    MonitorRef = erlang:monitor(process, Pid),
    receive
        {'DOWN', MonitorRef, process, Pid, normal} ->
            ok;
        {'DOWN', MonitorRef, process, Pid, Info} ->
            ?LOG_ERROR(
               "Feature flag `~s`:   "
               "failed to migrate Mnesia tables to Khepri:~n  ~p",
               [FeatureName, Info]),
            {error, {migration_failure, Info}}
    end.

migrate_tables_to_khepri_run(FeatureName, Tables) ->
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
    ok = final_sync_from_mnesia_to_khepri(FeatureName, Tables).

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
       "Feature flag `~s`:     table ~s: about ~b record to copy",
       [FeatureName, Table, Count]),
    _ = mnesia:transaction(
          fun() ->
                  _ = mnesia:foldl(
                        fun(Record, Copied) ->
                                Copied1 = Copied + 1,
                                ?LOG_DEBUG(
                                   "Feature flag `~s`:     table ~s: copying "
                                   "record ~b/~b",
                                   [FeatureName, Table, Copied1, Count]),
                                ok = Fun(Record),
                                Copied1
                        end, 0, Table)
          end),
    ok.

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
    Handled1 = Handled + 1,
    receive
        {write, NewRecord, _} ->
            ?LOG_DEBUG(
               "Feature flag `~s`:       handling event ~b/~b (write)",
               [FeatureName, Handled1, Count]),
            handle_mnesia_write(NewRecord),
            consume_mnesia_events(FeatureName, Count, Handled1);
        {delete_object, OldRecord, _} ->
            ?LOG_DEBUG(
               "Feature flag `~s`:       handling event ~b/~b (delete)",
               [FeatureName, Handled1, Count]),
            handle_mnesia_delete(OldRecord),
            consume_mnesia_events(FeatureName, Count, Handled1);
        {delete, {Table, Key}, _} ->
            ?LOG_DEBUG(
               "Feature flag `~s`:       handling event ~b/~b (delete)",
               [FeatureName, Handled1, Count]),
            handle_mnesia_delete(Table, Key),
            consume_mnesia_events(FeatureName, Count, Handled1)
    after 0 ->
              %% TODO: Wait for confirmation from Khepri.
              ok
    end.

handle_mnesia_write(NewRecord) when ?is_vhost(NewRecord) ->
    rabbit_vhost:mnesia_write_to_khepri(NewRecord);
handle_mnesia_write(NewRecord) when ?is_internal_user(NewRecord) ->
    rabbit_auth_backend_internal:mnesia_write_to_khepri(NewRecord);
handle_mnesia_write(NewRecord) when is_record(NewRecord, user_permission) ->
    rabbit_auth_backend_internal:mnesia_write_to_khepri(NewRecord);
handle_mnesia_write(NewRecord) when is_record(NewRecord, topic_permission) ->
    rabbit_auth_backend_internal:mnesia_write_to_khepri(NewRecord).

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

drop_unused_mnesia_tables(FeatureName, [Table | Rest]) ->
    %% The feature flag is enabled at this point. It means there should be no
    %% code trying to read or write the Mnesia tables.
    ?LOG_DEBUG(
       "Feature flag `~s`:   dropping unused Mnesia table ~s",
       [FeatureName, Table]),
    case mnesia:change_table_access_mode(Table, read_write) of
        {atomic, ok} ->
            case mnesia:delete_table(Table) of
                {atomic, ok}                       -> ok;
                {aborted, {no_exists, {Table, _}}} -> ok
            end;
        {aborted, {no_exists, {Table, _}}} ->
            ok
    end,
    drop_unused_mnesia_tables(FeatureName, Rest);
drop_unused_mnesia_tables(_, []) ->
            ok.
