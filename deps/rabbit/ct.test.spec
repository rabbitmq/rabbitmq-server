{logdir, "logs/"}.
{logdir, master, "logs/"}.
{create_priv_dir, all_nodes, auto_per_run}.

{node, shard1, 'rabbit_shard1@localhost'}.
{node, shard2, 'rabbit_shard2@localhost'}.
{node, shard3, 'rabbit_shard3@localhost'}.
{node, shard4, 'rabbit_shard4@localhost'}.

%%
%% Sets of test suites that take around the same time to complete.
%%

{define, 'Set1', [
    amqp_address_SUITE
,   amqp_auth_SUITE
,   amqp_client_SUITE
,   amqp_credit_api_v2_SUITE
,   amqp_filtex_SUITE
,   amqp_proxy_protocol_SUITE
,   amqp_dotnet_SUITE
,   amqp_jms_SUITE
,   amqpl_consumer_ack_SUITE
,   amqpl_direct_reply_to_SUITE
,   amqqueue_backward_compatibility_SUITE
,   backing_queue_SUITE
,   bindings_SUITE
,   channel_interceptor_SUITE
,   channel_operation_timeout_SUITE
,   classic_queue_SUITE
,   classic_queue_prop_SUITE
]}.

{define, 'Set2', [
    cluster_SUITE
,   config_schema_SUITE
,   confirms_rejects_SUITE
,   consumer_timeout_SUITE
,   crashing_queues_SUITE
,   deprecated_features_SUITE
,   direct_exchange_routing_v2_SUITE
,   disconnect_detected_during_alarm_SUITE
,   disk_monitor_SUITE
,   dynamic_qq_SUITE
,   exchanges_SUITE
,   prevent_startup_if_node_was_reset_SUITE
,   rabbit_stream_queue_SUITE
]}.

{define, 'Set3', [
    cli_forget_cluster_node_SUITE
,   feature_flags_SUITE
,   feature_flags_v2_SUITE
,   list_consumers_sanity_check_SUITE
,   list_queues_online_and_offline_SUITE
,   logging_SUITE
,   lqueue_SUITE
,   maintenance_mode_SUITE
,   mc_unit_SUITE
,   message_containers_deaths_v2_SUITE
,   message_size_limit_SUITE
,   metadata_store_migration_SUITE
,   metadata_store_phase1_SUITE
,   metrics_SUITE
,   mirrored_supervisor_SUITE
,   peer_discovery_classic_config_SUITE
]}.

{define, 'Set4', [
    msg_size_metrics_SUITE
,   peer_discovery_dns_SUITE
,   peer_discovery_tmp_hidden_node_SUITE
,   per_node_limit_SUITE
,   per_user_connection_channel_limit_SUITE
,   per_user_connection_channel_tracking_SUITE
,   per_user_connection_tracking_SUITE
,   per_vhost_connection_limit_SUITE
,   per_vhost_msg_store_SUITE
,   per_vhost_queue_limit_SUITE
,   policy_SUITE
,   priority_queue_SUITE
,   priority_queue_recovery_SUITE
,   product_info_SUITE
,   proxy_protocol_SUITE
,   publisher_confirms_parallel_SUITE
,   unit_msg_size_metrics_SUITE
]}.

{define, 'Set5', [
    clustering_recovery_SUITE
,   metadata_store_clustering_SUITE
,   queue_length_limits_SUITE
,   queue_parallel_SUITE
,   quorum_queue_SUITE
,   rabbit_access_control_SUITE
,   rabbit_confirms_SUITE
,   rabbit_core_metrics_gc_SUITE
,   rabbit_cuttlefish_SUITE
,   rabbit_db_binding_SUITE
,   rabbit_db_exchange_SUITE
,   rabbit_db_maintenance_SUITE
,   rabbit_db_msup_SUITE
,   rabbit_db_policy_SUITE
,   rabbit_db_queue_SUITE
,   rabbit_db_topic_exchange_SUITE
,   rabbit_direct_reply_to_prop_SUITE
]}.

{define, 'Set6', [
    queue_type_SUITE
,   quorum_queue_member_reconciliation_SUITE
,   rabbit_fifo_SUITE
,   rabbit_fifo_dlx_SUITE
,   rabbit_fifo_dlx_integration_SUITE
,   rabbit_fifo_int_SUITE
,   rabbit_fifo_prop_SUITE
,   rabbit_fifo_v0_SUITE
,   rabbit_local_random_exchange_SUITE
,   rabbit_msg_interceptor_SUITE
,   rabbit_stream_coordinator_SUITE
,   rabbit_stream_sac_coordinator_v4_SUITE
,   rabbit_stream_sac_coordinator_SUITE
,   rabbitmq_4_0_deprecations_SUITE
,   rabbitmq_queues_cli_integration_SUITE
,   rabbitmqctl_integration_SUITE
,   rabbitmqctl_shutdown_SUITE
,   routing_SUITE
,   runtime_parameters_SUITE
]}.

{define, 'Set7', [
    cluster_limit_SUITE
,   cluster_minority_SUITE
,   clustering_management_SUITE
,   signal_handling_SUITE
,   single_active_consumer_SUITE
,   term_to_binary_compat_prop_SUITE
,   topic_permission_SUITE
,   transactions_SUITE
,   unicode_SUITE
,   unit_access_control_SUITE
,   unit_access_control_authn_authz_context_propagation_SUITE
,   unit_access_control_credential_validation_SUITE
,   unit_amqp091_content_framing_SUITE
,   unit_amqp091_server_properties_SUITE
,   unit_app_management_SUITE
,   unit_cluster_formation_locking_mocks_SUITE
,   unit_cluster_formation_sort_nodes_SUITE
,   unit_collections_SUITE
,   unit_config_value_encryption_SUITE
,   unit_connection_tracking_SUITE
]}.

{define, 'Set8', [
    dead_lettering_SUITE
,   definition_import_SUITE
,   per_user_connection_channel_limit_partitions_SUITE
,   per_vhost_connection_limit_partitions_SUITE
,   unit_credit_flow_SUITE
,   unit_disk_monitor_SUITE
,   unit_file_handle_cache_SUITE
,   unit_gen_server2_SUITE
,   unit_log_management_SUITE
,   unit_operator_policy_SUITE
,   unit_pg_local_SUITE
,   unit_plugin_directories_SUITE
,   unit_plugin_versioning_SUITE
,   unit_policy_validators_SUITE
,   unit_priority_queue_SUITE
,   unit_queue_consumers_SUITE
,   unit_queue_location_SUITE
,   unit_quorum_queue_SUITE
,   unit_stats_and_metrics_SUITE
,   unit_supervisor2_SUITE
,   unit_vm_memory_monitor_SUITE
,   upgrade_preparation_SUITE
,   vhost_SUITE
]}.

{suites, shard1, "test/", 'Set1'}.
{suites, shard1, "test/", 'Set2'}.

{suites, shard2, "test/", 'Set3'}.
{suites, shard2, "test/", 'Set4'}.

{suites, shard3, "test/", 'Set5'}.
{suites, shard3, "test/", 'Set6'}.

{suites, shard4, "test/", 'Set7'}.
{suites, shard4, "test/", 'Set8'}.
