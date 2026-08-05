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
         get_sessions_settings/0,
         get_settings/1,
         get_product_info/0]).

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

get_settings(ReqData) ->
    RatesMode = rabbit_mgmt_agent_config:get_env(rates_mode),
    SRP = get_sample_retention_policies(),
    ExchangeTypes = lists:sort(
        fun(ET1, ET2) ->
            proplists:get_value(name, ET1, none)
            =<
            proplists:get_value(name, ET2, none)
        end,
        rabbit_mgmt_external_stats:list_registry_plugins(exchange)),
    [
        {rates_mode,                RatesMode},
        {sample_retention_policies, SRP},
        {exchange_types,            ExchangeTypes},
        {cluster_tags,              cluster_tags()},
        {node_tags,                 node_tags()},
        {disable_stats,             rabbit_mgmt_util:disable_stats(ReqData)},
        {default_queue_type,        rabbit_queue_type:default_alias()},
        {is_op_policy_updating_enabled, not is_op_policy_updating_disabled()},
        {enable_queue_totals,       rabbit_mgmt_util:enable_queue_totals(ReqData)},
        {sessions,                  get_sessions_settings()},
        {definitions,               get_definitions_settings()}
    ].

get_product_info() ->
    [
        {product_name,        list_to_binary(rabbit:product_name())},
        {product_version,     list_to_binary(rabbit:product_version())},
        {rabbitmq_version,    list_to_binary(rabbit:base_product_version())},
        {management_version,  version(rabbitmq_management)},
        {erlang_version,      erlang_version()},
        {erlang_full_version, erlang_full_version()}
    ].

%% Private

version(App) ->
    {ok, V} = application:get_key(App, vsn),
    list_to_binary(V).

erlang_version() -> list_to_binary(rabbit_misc:otp_release()).

erlang_full_version() ->
    list_to_binary(rabbit_misc:otp_system_version()).

get_restriction(Path) ->
    Restrictions = application:get_env(rabbitmq_management,  restrictions, []),
    rabbit_misc:deep_pget(Path, Restrictions, false).

get_sample_retention_policies() ->
    P = rabbit_mgmt_agent_config:get_env(sample_retention_policies),
    get_sample_retention_policies(P).

get_sample_retention_policies(undefined) ->
    [{global, []}, {basic, []}, {detailed, []}];
get_sample_retention_policies(Policies) ->
    [transform_retention_policy(Pol, Policies) || Pol <- [global, basic, detailed]].

transform_retention_policy(Pol, Policies) ->
    case proplists:lookup(Pol, Policies) of
        none ->
            {Pol, []};
        {Pol, Intervals} ->
            {Pol, transform_retention_intervals(Intervals, [])}
    end.

transform_retention_intervals([], Acc) ->
    lists:sort(Acc);
transform_retention_intervals([{MaxAgeInSeconds, _}|Rest], Acc) ->
    %
    % Seconds | Interval
    % 60      | last minute
    % 600     | last 10 minutes
    % 3600    | last hour
    % 28800   | last 8 hours
    % 86400   | last day
    %
    % rabbitmq/rabbitmq-management#635
    %
    % We check for the max age in seconds to be within 10% of the value above.
    % The reason being that the default values are "bit higher" to accommodate
    % edge cases (see deps/rabbitmq_management_agent/Makefile)
    AccVal = if
                 MaxAgeInSeconds >= 0 andalso MaxAgeInSeconds =< 66 ->
                     60;
                 MaxAgeInSeconds >= 540 andalso MaxAgeInSeconds =< 660 ->
                     600;
                 MaxAgeInSeconds >= 3240 andalso MaxAgeInSeconds =< 3960 ->
                     3600;
                 MaxAgeInSeconds >= 25920 andalso MaxAgeInSeconds =< 31681 ->
                     28800;
                 MaxAgeInSeconds >= 77760 andalso MaxAgeInSeconds =< 95041 ->
                     86400;
                 true ->
                     0
             end,
    transform_retention_intervals(Rest, [AccVal|Acc]).

cluster_tags() ->
    Val = case rabbit_runtime_parameters:value_global(cluster_tags) of
        not_found ->
            [];
        Tags -> Tags
    end,
    rabbit_data_coercion:to_map(Val).

node_tags() ->
    Val = application:get_env(rabbit, node_tags, []),
    rabbit_data_coercion:to_map(Val).
