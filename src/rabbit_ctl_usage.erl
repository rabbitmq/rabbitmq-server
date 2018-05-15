%% Generated, do not edit!
-module(rabbit_ctl_usage).
-export([usage/0]).
usage() -> "Usage:
rabbitmqctl [-n <node>] [-t <timeout>] [-q] <command> [<command options>] 

Options:
    -n node
    -q
    -t timeout

Default node is \"rabbit@server\", where server is the local host. On a host 
named \"server.example.com\", the node name of the RabbitMQ Erlang node will 
usually be rabbit@server (unless RABBITMQ_NODENAME has been set to some 
non-default value at broker startup time). The output of hostname -s is usually 
the correct suffix to use after the \"@\" sign. See rabbitmq-server(1) for 
details of configuring the RabbitMQ broker.

Quiet output mode is selected with the \"-q\" flag. Informational messages are 
suppressed when quiet mode is in effect.

Operation timeout in seconds. Only applicable to \"list\" commands. Default is 
\"infinity\".

Commands:
    stop [<pid_file>]
    shutdown
    stop_app
    start_app
    wait <pid_file>
    reset
    force_reset
    rotate_logs <suffix>
    hipe_compile <directory>

    join_cluster <clusternode> [--ram]
    cluster_status
    change_cluster_node_type disc | ram
    forget_cluster_node [--offline]
    rename_cluster_node oldnode1 newnode1 [oldnode2] [newnode2 ...]
    update_cluster_nodes clusternode
    force_boot
    sync_queue [-p <vhost>] queue
    cancel_sync_queue [-p <vhost>] queue
    purge_queue [-p <vhost>] queue
    set_cluster_name name

    add_user <username> <password>
    delete_user <username>
    change_password <username> <newpassword>
    clear_password <username>
    authenticate_user <username> <password>
    set_user_tags <username> <tag> ...
    list_users

    add_vhost <vhost>
    delete_vhost <vhost>
    list_vhosts [<vhostinfoitem> ...]
    set_permissions [-p <vhost>] <user> <conf> <write> <read>
    clear_permissions [-p <vhost>] <username>
    list_permissions [-p <vhost>]
    list_user_permissions <username>

    set_parameter [-p <vhost>] <component_name> <name> <value>
    clear_parameter [-p <vhost>] <component_name> <key>
    list_parameters [-p <vhost>]
    set_global_parameter <name> <value>
    clear_global_parameter <name>
    list_global_parameters

    set_policy [-p <vhost>] [--priority <priority>] [--apply-to <apply-to>] 
<name> <pattern>  <definition>
    clear_policy [-p <vhost>] <name>
    list_policies [-p <vhost>]

    list_queues [-p <vhost>] [--offline|--online|--local] [<queueinfoitem> ...]
    list_exchanges [-p <vhost>] [<exchangeinfoitem> ...]
    list_bindings [-p <vhost>] [<bindinginfoitem> ...]
    list_connections [<connectioninfoitem> ...]
    list_channels [<channelinfoitem> ...]
    list_consumers [-p <vhost>]
    status
    node_health_check
    environment
    report
    eval <expr>

    close_connection <connectionpid> <explanation>
    trace_on [-p <vhost>]
    trace_off [-p <vhost>]
    set_vm_memory_high_watermark <fraction>
    set_vm_memory_high_watermark absolute <memory_limit>
    set_disk_free_limit <disk_limit>
    set_disk_free_limit mem_relative <fraction>
    encode [--decode] [<value>] [<passphrase>] [--list-ciphers] [--list-hashes] 
[--cipher <cipher>] [--hash <hash>] [--iterations <iterations>]
    decode [<value>] [<passphrase>][--cipher <cipher>] [--hash <hash>] 
[--iterations <iterations>]
    list_hashes
    list_ciphers

<vhostinfoitem> must be a member of the list [name, tracing].

The list_queues, list_exchanges and list_bindings commands accept an optional 
virtual host parameter for which to display results. The default value is \"/\".

<queueinfoitem> must be a member of the list [name, durable, auto_delete, 
arguments, policy, pid, owner_pid, exclusive, exclusive_consumer_pid, 
exclusive_consumer_tag, messages_ready, messages_unacknowledged, messages, 
messages_ready_ram, messages_unacknowledged_ram, messages_ram, 
messages_persistent, message_bytes, message_bytes_ready, 
message_bytes_unacknowledged, message_bytes_ram, message_bytes_persistent, 
head_message_timestamp, disk_reads, disk_writes, consumers, 
consumer_utilisation, memory, slave_pids, synchronised_slave_pids, state].

<exchangeinfoitem> must be a member of the list [name, type, durable, 
auto_delete, internal, arguments, policy].

<bindinginfoitem> must be a member of the list [source_name, source_kind, 
destination_name, destination_kind, routing_key, arguments].

<connectioninfoitem> must be a member of the list [pid, name, port, host, 
peer_port, peer_host, ssl, ssl_protocol, ssl_key_exchange, ssl_cipher, 
ssl_hash, peer_cert_subject, peer_cert_issuer, peer_cert_validity, state, 
channels, protocol, auth_mechanism, user, vhost, timeout, frame_max, 
channel_max, client_properties, recv_oct, recv_cnt, send_oct, send_cnt, 
send_pend, connected_at].

<channelinfoitem> must be a member of the list [pid, connection, name, number, 
user, vhost, transactional, confirm, consumer_count, messages_unacknowledged, 
messages_uncommitted, acks_uncommitted, messages_unconfirmed, prefetch_count, 
global_prefetch_count].


".
