%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_features).

-export([is_op_policy_updating_disabled/0,
         is_qq_replica_operations_disabled/0,
         are_stats_enabled/0,
         get_definitions_settings/0,
         is_sessions_enabled/0,
         get_sessions_settings/0]).

is_sessions_enabled() ->
    application:get_env(rabbitmq_management, sessions_enabled, false).

get_sessions_settings() ->
    [
        {enabled, is_sessions_enabled()},
        {max_concurrent, application:get_env(rabbitmq_management, sessions_max_concurrent, 1)},
        {heartbeat_interval, application:get_env(rabbitmq_management, sessions_heartbeat_interval, 30)}
    ].

is_qq_replica_operations_disabled() ->
    get_restriction([quorum_queue_replica_operations, disabled]).

is_op_policy_updating_disabled() ->
    case get_restriction([operator_policy_changes, disabled]) of
        true -> true;
        _ -> false
    end.

are_stats_enabled() ->
    DisabledFromConf = application:get_env(
      rabbitmq_management, disable_management_stats, false),
    case DisabledFromConf of
        true -> false;
        _    -> rabbit_mgmt_agent_config:is_metrics_collector_permitted()
    end.

get_definitions_settings() ->
    [
        {require_definition_json_extension,
         application:get_env(rabbitmq_management, require_definition_json_extension, false)}
    ].

%% Private

get_restriction(Path) ->
    Restrictions = application:get_env(rabbitmq_management,  restrictions, []),
    rabbit_misc:deep_pget(Path, Restrictions, false).
