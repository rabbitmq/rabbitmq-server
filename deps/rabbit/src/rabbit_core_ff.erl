%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2018-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_core_ff).

-export([classic_mirrored_queue_version_migration/3,
         quorum_queue_migration/3,
         stream_queue_migration/3,
         implicit_default_bindings_migration/3,
         virtual_host_metadata_migration/3,
         maintenance_mode_status_migration/3,
         user_limits_migration/3,
         stream_single_active_consumer_migration/3,
         direct_exchange_routing_v2_migration/3]).

-rabbit_feature_flag(
   {classic_mirrored_queue_version,
    #{desc          => "Support setting version for classic mirrored queues",
      stability     => stable,
      migration_fun => {?MODULE, classic_mirrored_queue_version_migration}
     }}).

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
   {stream_single_active_consumer,
    #{desc          => "Single active consumer for streams",
      doc_url       => "https://www.rabbitmq.com/stream.html",
      stability     => stable,
      depends_on    => [stream_queue],
      migration_fun => {?MODULE, stream_single_active_consumer_migration}
     }}).

-rabbit_feature_flag(
   {direct_exchange_routing_v2,
    #{desc          => "v2 direct exchange routing implementation",
      stability     => stable,
      depends_on    => [implicit_default_bindings],
      migration_fun => {?MODULE, direct_exchange_routing_v2_migration}
     }}).

-rabbit_feature_flag(
    {feature_flags_v2,
     #{desc          => "Feature flags subsystem V2",
       stability     => stable
     }}).

classic_mirrored_queue_version_migration(_FeatureName, _FeatureProps, _Enable) ->
    ok.

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
    mnesia:table_info(rabbit_durable_queue, attributes) =:= Fields.

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
implicit_default_bindings_migration(_Feature_Name, _FeatureProps,
                                    is_enabled) ->
    undefined.

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
    mnesia:table_info(rabbit_vhost, attributes) =:= vhost:fields(vhost_v2).

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
        _ = rabbit_table:ensure_table_copy(TableName, node(), disc_copies)
    catch throw:Reason  ->
        rabbit_log:error(
          "Failed to create maintenance status table: ~p",
          [Reason])
    end;
maintenance_mode_status_migration(_FeatureName, _FeatureProps, is_enabled) ->
    rabbit_table:exists(rabbit_maintenance:status_table_name()).

%% -------------------------------------------------------------------
%% User limits.
%% -------------------------------------------------------------------

user_limits_migration(_FeatureName, _FeatureProps, enable) ->
    Tab = rabbit_user,
    rabbit_table:wait([Tab], _Retry = true),
    Fun = fun(Row) -> internal_user:upgrade_to(internal_user_v2, Row) end,
    case mnesia:transform_table(Tab, Fun, internal_user:fields(internal_user_v2)) of
        {atomic, ok}      -> ok;
        {aborted, Reason} -> {error, Reason}
    end;
user_limits_migration(_FeatureName, _FeatureProps, is_enabled) ->
    mnesia:table_info(rabbit_user, attributes) =:= internal_user:fields(internal_user_v2).

%% -------------------------------------------------------------------
%% Stream single active consumer.
%% -------------------------------------------------------------------

stream_single_active_consumer_migration(_FeatureName, _FeatureProps, enable) ->
    ok;
stream_single_active_consumer_migration(_FeatureName, _FeatureProps, is_enabled) ->
    undefined.

%% -------------------------------------------------------------------
%% Direct exchange routing v2.
%% -------------------------------------------------------------------

direct_exchange_routing_v2_migration(FeatureName, _FeatureProps, enable) ->
    TableName = rabbit_index_route,
    ok = rabbit_table:wait([rabbit_route], _Retry = true),
    try
        ok = rabbit_table:create(TableName,
                                 rabbit_binding:index_route_table_definition()),
        case rabbit_table:ensure_table_copy(TableName, node(), ram_copies) of
            ok ->
                rabbit_binding:populate_index_route_table();
            {error, Err} = Error ->
                rabbit_log_feature_flags:error("Failed to add copy of table ~s to node ~p: ~p",
                                               [TableName, node(), Err]),
                Error
        end
    catch throw:Reason  ->
              rabbit_log_feature_flags:error("Enabling feature flag ~s failed: ~p",
                                             [FeatureName, Reason]),
              {error, Reason}
    end;
direct_exchange_routing_v2_migration(_FeatureName, _FeatureProps, is_enabled) ->
    TableName = rabbit_index_route,
    Enabled = rabbit_table:exists(TableName),
    case Enabled of
        true ->
            %% Always ensure every node has a table copy.
            %% If we were not to add a table copy here, `make start-cluster NODES=2`
            %% would result in only `rabbit-1` having a copy.
            case rabbit_table:ensure_table_copy(TableName, node(), ram_copies) of
                ok ->
                    ok;
                {error, Reason} ->
                    rabbit_log_feature_flags:warning("Failed to add copy of table ~s to node ~p: ~p",
                                                     [TableName, node(), Reason])
            end;
        _ ->
            ok
    end,
    Enabled.
