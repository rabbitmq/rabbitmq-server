%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2023-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_features).

-export([is_op_policy_updating_disabled/0,
         is_qq_replica_operations_disabled/0,
         are_stats_enabled/0]).

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

%% Private

get_restriction(Path) ->
    Restrictions = application:get_env(rabbitmq_management,  restrictions, []),
    rabbit_misc:deep_pget(Path, Restrictions, false).
