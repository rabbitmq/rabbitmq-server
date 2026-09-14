%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-define(BACKEND_CONFIG_KEY, peer_discovery_etcd).

-record(statem_data, {
    endpoints,
    tls_options,
    connection_name,
    connection_pid,
    connection_monitor,
    key_prefix,
    cluster_name,
    node_key_lease_id,
    node_key_ttl_in_seconds,
    %% the pid of the process returned by eetcd_lease:keep_alive/2
    %% which refreshes this node's key lease
    node_lease_keepalive_pid,
    %% true once register/0,1 has been asked to register this node's key,
    %% whether or not the put has actually succeeded yet; node_key_lease_id
    %% can't stand in for this, since it is undefined both before
    %% registration and after a lapsed lease. Seeded from a persistent_term
    %% (see ?NODE_KEY_REGISTERED_PTERM_KEY) so a freshly started client
    %% still knows to repair a registration an earlier one had requested.
    node_key_registered = false,
    lock_ttl_in_seconds,
    lock_lease_id,
    %% the pid of the process returned by eetcd_lease:keep_alive/2
    %% which refreshes the registration lock's lease
    lock_lease_keepalive_pid,
    %% the pid of the process that called lock/1 and currently holds the
    %% lock, monitored so the lock is released if that process dies
    %% before calling unlock/1
    lock_owner_pid,
    lock_owner_monitor,
    username,
    obfuscated_password
}).
