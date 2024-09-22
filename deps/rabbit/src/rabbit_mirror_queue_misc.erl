%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mirror_queue_misc).

-include_lib("stdlib/include/assert.hrl").

-include("amqqueue.hrl").

%% Deprecated feature callback.
-export([are_cmqs_used/1]).

-include_lib("rabbit_common/include/rabbit.hrl").

-rabbit_deprecated_feature(
   {classic_queue_mirroring,
    #{deprecation_phase => removed,
      messages =>
      #{when_permitted =>
        "Classic mirrored queues are deprecated.\n"
        "By default, they can still be used for now.\n"
        "Their use will not be permitted by default in the next minor"
        "RabbitMQ version (if any) and they will be removed from "
        "RabbitMQ 4.0.0.\n"
        "To continue using classic mirrored queues when they are not "
        "permitted by default, set the following parameter in your "
        "configuration:\n"
        "    \"deprecated_features.permit.classic_queue_mirroring = true\"\n"
        "To test RabbitMQ as if they were removed, set this in your "
        "configuration:\n"
        "    \"deprecated_features.permit.classic_queue_mirroring = false\"",

        when_denied =>
        "Classic mirrored queues are deprecated.\n"
        "Their use is not permitted per the configuration (overriding the "
        "default, which is permitted):\n"
        "    \"deprecated_features.permit.classic_queue_mirroring = false\"\n"
        "Their use will not be permitted by default in the next minor "
        "RabbitMQ version (if any) and they will be removed from "
        "RabbitMQ 4.0.0.\n"
        "To continue using classic mirrored queues when they are not "
        "permitted by default, set the following parameter in your "
        "configuration:\n"
        "    \"deprecated_features.permit.classic_queue_mirroring = true\"",

        when_removed =>
        "Classic mirrored queues have been removed.\n"
       },
      doc_url => "https://blog.rabbitmq.com/posts/2021/08/4.0-deprecation-announcements/#removal-of-classic-queue-mirroring",
      callbacks => #{is_feature_used => {?MODULE, are_cmqs_used}}
     }}).

%%----------------------------------------------------------------------------

are_cmqs_used(_) ->
    case rabbit_khepri:get_feature_state() of
        enabled ->
            false;
        _ ->
            %% If we are using Mnesia, we want to check manually if the table
            %% exists first. Otherwise it can conflict with the way
            %% `rabbit_khepri:handle_fallback/1` works. Indeed, this function
            %% and `rabbit_khepri:handle_fallback/1` rely on the `no_exists`
            %% exception.
            AllTables = mnesia:system_info(tables),
            RuntimeParamsReady = lists:member(
                                   rabbit_runtime_parameters, AllTables),
            case RuntimeParamsReady of
                true ->
                    %% We also wait for the table because it could exist but
                    %% may be unavailable. For instance, Mnesia needs another
                    %% replica on another node before it considers it to be
                    %% available.
                    rabbit_table:wait_silent(
                      [rabbit_runtime_parameters], _Retry = true),
                    are_cmqs_used1();
                false ->
                    false
            end
    end.

are_cmqs_used1() ->
    try
        LocalPolicies = rabbit_policy:list(),
        LocalOpPolicies = rabbit_policy:list_op(),
        has_ha_policies(LocalPolicies ++ LocalOpPolicies)
    catch
        exit:{aborted, {no_exists, _}} ->
            %% This node is being initialized for the first time. Therefore it
            %% must have no policies.
            ?assert(rabbit_mnesia:is_running()),
            false
    end.

has_ha_policies(Policies) ->
    lists:any(
      fun(Policy) ->
              KeyList = proplists:get_value(definition, Policy),
              does_policy_configure_cmq(KeyList)
      end, Policies).

does_policy_configure_cmq(Map) when is_map(Map) ->
    is_map_key(<<"ha-mode">>, Map);
does_policy_configure_cmq(KeyList) when is_list(KeyList) ->
    lists:keymember(<<"ha-mode">>, 1, KeyList).
