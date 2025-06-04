%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

%% This test suite uses test cases shared by rabbitmq_mqtt.
-module(web_mqtt_shared_SUITE).
-compile([export_all,
          nowarn_export_all]).

all() ->
    mqtt_shared_SUITE:all().

groups() ->
    mqtt_shared_SUITE:groups().

suite() ->
    mqtt_shared_SUITE:suite().

init_per_suite(Config) ->
    mqtt_shared_SUITE:init_per_suite(Config).

end_per_suite(Config) ->
    mqtt_shared_SUITE:end_per_suite(Config).

init_per_group(mqtt, Config) ->
    %% This is the main difference with rabbitmq_mqtt.
    rabbit_ct_helpers:set_config(Config, {websocket, true});
init_per_group(Group, Config) ->
    mqtt_shared_SUITE:init_per_group(Group, Config).

end_per_group(Group, Config) ->
    mqtt_shared_SUITE:end_per_group(Group, Config).

init_per_testcase(Testcase, Config) ->
    mqtt_shared_SUITE:init_per_testcase(Testcase, Config).

end_per_testcase(Testcase, Config) ->
    mqtt_shared_SUITE:end_per_testcase(Testcase, Config).

global_counters(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
message_size_metrics(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
block_only_publisher(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
many_qos1_messages(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
session_expiry(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
cli_close_all_connections(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
cli_close_all_user_connections(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
management_plugin_connection(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
management_plugin_enable(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
disconnect(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
pubsub_shared_connection(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
pubsub_separate_connections(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
will_with_disconnect(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
will_without_disconnect(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
decode_basic_properties(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
quorum_queue_rejects(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
events(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
internal_event_handler(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
non_clean_sess_reconnect_qos1(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
non_clean_sess_reconnect_qos0(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
non_clean_sess_reconnect_qos0_and_qos1(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
non_clean_sess_empty_client_id(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
subscribe_same_topic_same_qos(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
subscribe_same_topic_different_qos(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
subscribe_multiple(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
large_message_mqtt_to_mqtt(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
large_message_amqp_to_mqtt(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
keepalive(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
keepalive_turned_off(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
block(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
amqp_to_mqtt_qos0(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
clean_session_disconnect_client(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
clean_session_node_restart(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
clean_session_node_kill(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
rabbit_status_connection_count(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
trace(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
trace_large_message(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
max_packet_size_unauthenticated(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
max_packet_size_authenticated(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
default_queue_type(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
message_interceptors(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
utf8(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
retained_message_conversion(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
bind_exchange_to_exchange(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
bind_exchange_to_exchange_single_message(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
pubsub(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
queue_down_qos1(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
consuming_classic_queue_down(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
flow_classic_queue(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
flow_quorum_queue(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
flow_stream(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
rabbit_mqtt_qos0_queue(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
rabbit_mqtt_qos0_queue_kill_node(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
cli_list_queues(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
delete_create_queue(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
session_reconnect(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
session_takeover(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
duplicate_client_id(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
publish_to_all_queue_types_qos0(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
publish_to_all_queue_types_qos1(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
maintenance(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
notify_consumer_classic_queue_deleted(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
notify_consumer_quorum_queue_deleted(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
notify_consumer_qos0_queue_deleted(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
zero_session_expiry_interval_disconnect_client(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).
zero_session_expiry_disconnect_autodeletes_qos0_queue(Config) -> mqtt_shared_SUITE:?FUNCTION_NAME(Config).