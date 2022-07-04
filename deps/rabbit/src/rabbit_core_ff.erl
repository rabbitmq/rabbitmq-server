%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2018-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_core_ff).

-export([direct_exchange_routing_v2_migration/3]).

-rabbit_feature_flag(
   {classic_mirrored_queue_version,
    #{desc          => "Support setting version for classic mirrored queues",
      stability     => stable
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
      stability     => stable,
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
      stability     => stable,
      depends_on    => [stream_queue]
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
