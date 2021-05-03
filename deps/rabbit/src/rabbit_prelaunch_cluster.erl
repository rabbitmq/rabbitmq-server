%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(rabbit_prelaunch_cluster).

-export([setup/1]).

setup(Context) ->
    _ = rabbit_log_prelaunch:debug(""),
    _ = rabbit_log_prelaunch:debug("== Clustering =="),
    _ = rabbit_log_prelaunch:debug("Preparing cluster status files"),
    rabbit_node_monitor:prepare_cluster_status_files(),
    case Context of
        #{initial_pass := true} ->
            _ = rabbit_log_prelaunch:debug("Upgrading Mnesia schema"),
            ok = rabbit_upgrade:maybe_upgrade_mnesia();
        _ ->
            ok
    end,
    %% It's important that the consistency check happens after
    %% the upgrade, since if we are a secondary node the
    %% primary node will have forgotten us
    _ = rabbit_log_prelaunch:debug("Checking cluster consistency"),
    rabbit_mnesia:check_cluster_consistency(),
    ok.
