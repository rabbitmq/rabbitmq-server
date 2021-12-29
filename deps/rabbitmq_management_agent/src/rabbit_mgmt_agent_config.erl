%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%%   Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(rabbit_mgmt_agent_config).

-export([get_env/1, get_env/2]).

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
