%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_observer_cli).

-export([init/0, add_plugin/1]).

init() ->
    %% prepare observer_cli.plugins for add_plugin/1
    application:set_env(observer_cli, plugins, application:get_env(observer_cli, plugins, [])).

%% must be executed after observer_cli boot_step
add_plugin(PluginInfo) ->
    case application:get_env(observer_cli, plugins, undefined) of
        undefined -> %% shouldn't be there, die
            exit({rabbit_observer_cli_step_not_there, "Can't add observer_cli plugin, required boot_step wasn't executed"});
        Plugins when is_list(Plugins) ->
            application:set_env(observer_cli, plugins, Plugins ++ [PluginInfo]);
        _ ->
            exit({rabbit_observer_cli_plugins_error, "Can't add observer_cli plugin, existing entry is not a list"})
    end.
