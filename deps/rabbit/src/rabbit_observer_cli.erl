%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_observer_cli).

-export([init/0]).

init() ->
    application:set_env(observer_cli, plugins, [
        rabbit_observer_cli_classic_queues:plugin_info(),
        rabbit_observer_cli_quorum_queues:plugin_info()
    ]).
