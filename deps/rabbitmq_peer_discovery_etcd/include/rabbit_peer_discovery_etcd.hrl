%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2022 VMware, Inc. or its affiliates.  All rights reserved.
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
    lock_ttl_in_seconds,
    username,
    obfuscated_password
}).
