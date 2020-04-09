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
