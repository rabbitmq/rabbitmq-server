%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2018-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_core_ff).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include_lib("rabbit_common/include/logging.hrl").

-export([mds_phase1_migration_enable/1,
         mds_phase1_migration_post_enable/1]).
-export([mds_plugin_migration_enable/2,
         mds_migration_post_enable/2]).

-rabbit_feature_flag(
   {classic_mirrored_queue_version,
    #{desc          => "Support setting version for classic mirrored queues",
      stability     => required
     }}).

-rabbit_feature_flag(
   {quorum_queue,
    #{desc          => "Support queues of type `quorum`",
      doc_url       => "https://www.rabbitmq.com/quorum-queues.html",
      stability     => required
     }}).

-rabbit_feature_flag(
   {stream_queue,
    #{desc          => "Support queues of type `stream`",
      doc_url       => "https://www.rabbitmq.com/stream.html",
      stability     => required,
      depends_on    => [quorum_queue]
     }}).

-rabbit_feature_flag(
   {implicit_default_bindings,
    #{desc          => "Default bindings are now implicit, instead of "
                       "being stored in the database",
      stability     => required
     }}).

-rabbit_feature_flag(
   {virtual_host_metadata,
    #{desc          => "Virtual host metadata (description, tags, etc)",
      stability     => required
     }}).

-rabbit_feature_flag(
   {maintenance_mode_status,
    #{desc          => "Maintenance mode status",
      stability     => required
     }}).

-rabbit_feature_flag(
    {user_limits,
     #{desc          => "Configure connection and channel limits for a user",
       stability     => required
     }}).

-rabbit_feature_flag(
   {stream_single_active_consumer,
    #{desc          => "Single active consumer for streams",
      doc_url       => "https://www.rabbitmq.com/stream.html",
      stability     => required,
      depends_on    => [stream_queue]
     }}).

-rabbit_feature_flag(
    {feature_flags_v2,
     #{desc          => "Feature flags subsystem V2",
       stability     => required
     }}).

-rabbit_feature_flag(
   {direct_exchange_routing_v2,
    #{desc       => "v2 direct exchange routing implementation",
      stability  => required,
      depends_on => [feature_flags_v2, implicit_default_bindings]
     }}).

-rabbit_feature_flag(
   {listener_records_in_ets,
    #{desc       => "Store listener records in ETS instead of Mnesia",
      stability  => required,
      depends_on => [feature_flags_v2]
     }}).

-rabbit_feature_flag(
   {tracking_records_in_ets,
    #{desc          => "Store tracking records in ETS instead of Mnesia",
      stability     => required,
      depends_on    => [feature_flags_v2]
     }}).

-rabbit_feature_flag(
   {classic_queue_type_delivery_support,
    #{desc          => "Bug fix for classic queue deliveries using mixed versions",
      doc_url       => "https://github.com/rabbitmq/rabbitmq-server/issues/5931",
      %%TODO remove compatibility code
      stability     => required,
      depends_on    => [stream_queue]
     }}).

-rabbit_feature_flag(
   {restart_streams,
    #{desc          => "Support for restarting streams with optional preferred next leader argument. "
      "Used to implement stream leader rebalancing",
      stability     => stable,
      depends_on    => [stream_queue]
     }}).


-rabbit_feature_flag(
   {stream_sac_coordinator_unblock_group,
    #{desc          => "Bug fix to unblock a group of consumers in a super stream partition",
      doc_url       => "https://github.com/rabbitmq/rabbitmq-server/issues/7743",
      stability     => stable,
      depends_on    => [stream_single_active_consumer]
     }}).

-rabbit_feature_flag(
   {raft_based_metadata_store_phase1,
    #{desc          => "Use the new Raft-based metadata store [phase 1]",
      doc_url       => "", %% TODO
      stability     => experimental,
      depends_on    => [feature_flags_v2,
                        direct_exchange_routing_v2,
                        maintenance_mode_status,
                        user_limits,
                        virtual_host_metadata,
                        tracking_records_in_ets,
                        listener_records_in_ets],
      callbacks     => #{enable =>
                             {?MODULE, mds_phase1_migration_enable},
                         post_enable =>
                             {?MODULE, mds_phase1_migration_post_enable}}
     }}).

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

-define(MDS_TABLES,
        [
         {[rabbit_vhost], rabbit_db_vhost_m2k_converter},
         {[rabbit_user, rabbit_user_permission, rabbit_topic_permission],
          rabbit_db_user_m2k_converter},
         {[rabbit_runtime_parameters], rabbit_db_rtparams_m2k_converter},
         {[rabbit_queue], rabbit_db_queue_m2k_converter},
         {[rabbit_exchange, rabbit_exchange_serial], rabbit_db_exchange_m2k_converter},
         {[rabbit_route], rabbit_db_binding_m2k_converter},
         {[rabbit_node_maintenance_states], rabbit_db_maintenance_m2k_converter},
         {[mirrored_sup_childspec], rabbit_db_msup_m2k_converter}
        ]
       ).

-define(MDS_CLEANUP_TABLES,
        [
         {[rabbit_vhost], rabbit_db_vhost_m2k_converter},
         {[rabbit_user, rabbit_user_permission, rabbit_topic_permission],
          rabbit_db_user_m2k_converter},
         {[rabbit_runtime_parameters], rabbit_db_rtparams_m2k_converter},
         {[rabbit_queue, rabbit_durable_queue], rabbit_db_queue_m2k_converter},
         {[rabbit_exchange, rabbit_durable_exchange, rabbit_exchange_serial], rabbit_db_exchange_m2k_converter},
         {[rabbit_route, rabbit_durable_route, rabbit_semi_durable_route, rabbit_reverse_route, rabbit_index_route], rabbit_db_binding_m2k_converter},
         {[rabbit_node_maintenance_states], rabbit_db_maintenance_m2k_converter},
         {[mirrored_sup_childspec], rabbit_db_msup_m2k_converter}
        ]
       ).

mds_phase1_migration_enable(#{feature_name := FeatureName}) ->
    %% Channel and connection tracking are core features with difference:
    %% tables cannot be predeclared as they include the node name
    global:set_lock({FeatureName, self()}),
    Ret = case rabbit_khepri:is_ready() of
              true ->
                  ok;
              false ->
                  mds_migration_enable(FeatureName, ?MDS_TABLES)
          end,
    global:del_lock({FeatureName, self()}),
    Ret.

mds_phase1_migration_post_enable(#{feature_name := FeatureName}) ->
    %% Some tables are not necessary for the migration, but they
    %% must be emptied afterwards (i.e. rabbit_durable_queue,
    %% rabbit_reverse_route...)
    Tables = ?MDS_CLEANUP_TABLES,
    mds_migration_post_enable(FeatureName, Tables).

mds_migration_enable(FeatureName, TablesAndOwners) ->
    case ensure_khepri_cluster_matches_mnesia(FeatureName) of
        ok ->
            mds_migrate_tables_to_khepri(FeatureName, TablesAndOwners);
        Error ->
            Error
    end.

mds_migration_post_enable(FeatureName, TablesAndOwners) ->
    ?assert(rabbit_khepri:is_enabled(non_blocking)),
    {Tables, _} = lists:unzip(TablesAndOwners),
    case rabbit_table:is_present() of
        true ->
            empty_unused_mnesia_tables(FeatureName, lists:flatten(Tables));
        false ->
            ok
    end.

ensure_khepri_cluster_matches_mnesia(FeatureName) ->
    %% This is the first time Khepri will be used for real. Therefore
    %% we need to make sure the Khepri cluster matches the Mnesia
    %% cluster.
    ?LOG_DEBUG(
       "Feature flag `~s`:   updating the Khepri cluster to match "
       "the Mnesia cluster",
       [FeatureName]),
    try
        rabbit_khepri:init_cluster()
    catch
        error:{khepri_mnesia_migration_ex, _, _} = Reason ->
            {error, Reason}
    end.

mds_plugin_migration_enable(FeatureName, TablesAndOwners) ->
    global:set_lock({FeatureName, self()}),
    Ret = mds_migrate_tables_to_khepri(FeatureName, TablesAndOwners),
    global:del_lock({FeatureName, self()}),
    Ret.

mds_migrate_tables_to_khepri(FeatureName, TablesAndOwners) ->
    case rabbit_table:is_present() of
        true ->
            mds_migrate_tables_to_khepri0(FeatureName, TablesAndOwners);
        false ->
            ?LOG_NOTICE(
               "Feature flag `~s`:   migration from Mnesia to Khepri "
               "finished, no Mnesia tables are present",
               [FeatureName]),
            rabbit_db:set_migration_flag(FeatureName),
            _ = rabbit_khepri:set_ready(),
            ok
    end.

mds_migrate_tables_to_khepri0(FeatureName, TablesAndOwners) ->
    {Tables0, _} = lists:unzip(TablesAndOwners),
    Tables = lists:flatten(Tables0),
    rabbit_table:wait(Tables, _Retry = true),
    ?LOG_NOTICE(
       "Feature flag `~s`:   starting migration from Mnesia "
       "to Khepri; expect decrease in performance and "
       "increase in memory footprint",
       [FeatureName]),
    Pid = spawn(
            fun() ->
                    mds_migrate_tables_to_khepri_run(FeatureName, TablesAndOwners)
            end),
    MonitorRef = erlang:monitor(process, Pid),
    receive
        {'DOWN', MonitorRef, process, Pid, normal} ->
            ?LOG_NOTICE(
               "Feature flag `~s`:   migration from Mnesia to Khepri "
               "finished",
               [FeatureName]),
            %% Mark the migration as done. This flag is necessary to skip clearing
            %% data from previous migrations when a node joins an existing cluster.
            %% If the migration has never been completed, then it's fair to remove
            %% the existing data in Khepri.
            rabbit_db:set_migration_flag(FeatureName),
            _ = rabbit_khepri:set_ready(),
            ok;
        {'DOWN', MonitorRef, process, Pid, Info} ->
            ?LOG_ERROR(
               "Feature flag `~s`:   "
               "failed to migrate Mnesia tables to Khepri:~n  ~p",
               [FeatureName, Info]),
            {error, {migration_failure, Info}}
    end.

mds_migrate_tables_to_khepri_run(FeatureName, TablesAndOwners) ->
    %% Clear data in Khepri which could come from a previously aborted copy
    %% attempt. The table list order is important so we need to reverse that
    %% order to clear the data.
    ?LOG_DEBUG(
       "Feature flag `~s`:   clear data from any aborted migration attempts "
       "(if any)",
       [FeatureName]),
    case rabbit_db:is_migration_done(FeatureName) of
        %% This flag is necessary to skip clearing
        %% data from previous migrations when a node joins an existing cluster.
        %% If the migration has never been completed, then it's fair to remove
        %% the existing data in Khepri.
        true ->
            ok;
        false ->
            ok = clear_data_from_previous_attempt(FeatureName, lists:reverse(TablesAndOwners)),
            [mnesia_to_khepri:copy_tables(metadata_store, Tables, Mod)
             || {Tables, Mod} <- TablesAndOwners]
    end.

clear_data_from_previous_attempt(
  FeatureName, [{Tables, Mod} | Rest]) ->
    [ok = Mod:clear_data_in_khepri(Table) || Table <- Tables],
    clear_data_from_previous_attempt(FeatureName, Rest);
clear_data_from_previous_attempt(_, []) ->
    ok.

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

%% Just emptying the tables can cause Mnesia synchronisation issues
%% if the node is killed and restarted soon after. The change
%% from `read_only` to `read_write` might have not be synced
%% on all nodes, and the schema differ on the next boot.
%% The likelyhood of this happening is probably very low,
%% so we'll leave the tables as this. Deleting a local table
%% copy has a lot of side effects when adding a node that
%% has not yet enabled the feature flag to a Khepri cluster.
%% Operations as wait for tables, checks for presence
%% and so on will fail.

empty_unused_mnesia_tables(FeatureName, [Table | Rest]) ->
    %% The feature flag is enabled at this point. It means there should be no
    %% code trying to read or write the Mnesia tables.
    case mnesia:change_table_access_mode(Table, read_write) of
        {atomic, ok} ->
            ?LOG_DEBUG(
               "Feature flag `~s`:   dropping content from unused Mnesia "
               "table ~s",
               [FeatureName, Table]),
            ok = empty_unused_mnesia_table(Table);
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
