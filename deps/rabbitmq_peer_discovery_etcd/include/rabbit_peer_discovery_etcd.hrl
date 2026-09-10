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
    %% true once register/0,1 has successfully put this node's key at
    %% least once; node_key_lease_id can't tell that apart from a lapsed
    %% lease, since it is undefined in both cases
    node_key_registered = false,
    lock_ttl_in_seconds,
    lock_lease_id,
    %% the pid of the process returned by eetcd_lease:keep_alive/2
    %% which refreshes the registration lock's lease
    lock_lease_keepalive_pid,
    username,
    obfuscated_password
}).
