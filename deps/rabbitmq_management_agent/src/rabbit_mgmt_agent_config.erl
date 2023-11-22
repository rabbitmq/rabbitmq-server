%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%%   Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%
-module(rabbit_mgmt_agent_config).

-export([get_env/1, get_env/2]).
-export([is_metrics_collector_enabled/0,
         is_metrics_collector_permitted/0]).

-rabbit_deprecated_feature(
   {management_metrics_collection,
    #{deprecation_phase => permitted_by_default,
      doc_url => "https://blog.rabbitmq.com/posts/2021/08/4.0-deprecation-announcements/#disable-metrics-delivery-via-the-management-api--ui"
     }}).

%% some people have reasons to only run with the agent enabled:
%% make it possible for them to configure key management app
%% settings such as rates_mode.
get_env(Key) ->
    rabbit_misc:get_env(rabbitmq_management, Key,
                        rabbit_misc:get_env(rabbitmq_management_agent, Key,
                                            undefined)).

get_env(Key, Default) ->
    rabbit_misc:get_env(rabbitmq_management, Key,
                        rabbit_misc:get_env(rabbitmq_management_agent, Key,
                                            Default)).

is_metrics_collector_enabled() ->
    DisabledFromConf = application:get_env(
      rabbitmq_management_agent, disable_metrics_collector, false),
    case DisabledFromConf of
        true -> false;
        _    -> is_metrics_collector_permitted()
    end.

is_metrics_collector_permitted() ->
    FeatureName = management_metrics_collection,
    rabbit_deprecated_features:is_permitted(FeatureName).
