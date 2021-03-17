%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2018-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_core_ff).

-export([quorum_queue_migration/3,
         implicit_default_bindings_migration/3,
         virtual_host_metadata_migration/3,
         maintenance_mode_status_migration/3,
         user_limits_migration/3]).

-rabbit_feature_flag(
   {quorum_queue,
    #{desc          => "Support queues of type `quorum`",
      doc_url       => "https://www.rabbitmq.com/quorum-queues.html",
      stability     => stable,
      migration_fun => {?MODULE, quorum_queue_migration}
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
     #{
         desc          => "Maintenance mode status",
         stability     => stable,
         migration_fun => {?MODULE, maintenance_mode_status_migration}
      }}).

-rabbit_feature_flag(
    {user_limits,
     #{desc          => "Configure connection and channel limits for a user",
       stability     => stable,
       migration_fun => {?MODULE, user_limits_migration}
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
    mnesia:table_info(rabbit_durable_queue, attributes) =:= Fields.

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


%%
%% Maintenance mode
%%

maintenance_mode_status_migration(FeatureName, _FeatureProps, enable) ->
    TableName = rabbit_maintenance:status_table_name(),
    _ = rabbit_log:info("Creating table ~s for feature flag `~s`", [TableName, FeatureName]),
    try
        _ = rabbit_table:create(TableName, rabbit_maintenance:status_table_definition()),
        _ = rabbit_table:ensure_table_copy(TableName, node())
    catch throw:Reason  ->
        _ = rabbit_log:error("Failed to create maintenance status table: ~p", [Reason])
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
