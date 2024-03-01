%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_features).

-export([is_op_policy_updating_disabled/0,
         is_qq_replica_operations_disabled/0,
         is_feature_flag_blocked/1,
         are_stats_enabled/0]).

is_qq_replica_operations_disabled() ->
    get_restriction([quorum_queue_replica_operations, disabled]).

is_op_policy_updating_disabled() ->
    case get_restriction([operator_policy_changes, disabled]) of
        true -> true;
        _ -> false
    end.

-spec is_feature_flag_blocked(rabbit_feature_flags:feature_name()) -> {true, string()} | false.
is_feature_flag_blocked(FeatureFlag) ->
    case get_restriction([feature_flag_blocked, FeatureFlag]) of
        Msg when is_list(Msg) -> {true, Msg};
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
