%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

%% This test suite uses test cases shared by rabbitmq_mqtt.
-module(web_mqtt_v5_SUITE).
-compile([export_all,
          nowarn_export_all]).

all() ->
    v5_SUITE:all().

groups() ->
    v5_SUITE:groups().

suite() ->
    v5_SUITE:suite().

init_per_suite(Config0) ->
    Config = v5_SUITE:init_per_suite(Config0),
    rabbit_ct_helpers:set_config(Config, {test_plugins, [rabbitmq_mqtt,
                                                         rabbitmq_web_mqtt]}).

end_per_suite(Config) ->
    v5_SUITE:end_per_suite(Config).

init_per_group(mqtt, Config) ->
    %% This is the main difference with rabbitmq_mqtt.
    rabbit_ct_helpers:set_config(Config, {websocket, true});
init_per_group(Group, Config0) ->
    v5_SUITE:init_per_group(Group, Config0).

end_per_group(Group, Config) ->
    v5_SUITE:end_per_group(Group, Config).

init_per_testcase(Testcase, Config) ->
    v5_SUITE:init_per_testcase(Testcase, Config).

end_per_testcase(Testcase, Config) ->
    v5_SUITE:end_per_testcase(Testcase, Config).

client_set_max_packet_size_publish(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
client_set_max_packet_size_connack(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
client_set_max_packet_size_invalid(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
message_expiry(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
message_expiry_will_message(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
message_expiry_retained_message(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
session_expiry_classic_queue_disconnect_decrease(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
session_expiry_quorum_queue_disconnect_decrease(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
session_expiry_disconnect_zero_to_non_zero(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
session_expiry_disconnect_non_zero_to_zero(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
session_expiry_disconnect_infinity_to_zero(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
session_expiry_disconnect_to_infinity(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
session_expiry_reconnect_non_zero(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
session_expiry_reconnect_zero(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
session_expiry_reconnect_infinity_to_zero(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
zero_session_expiry_disconnect_autodeletes_qos0_queue(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
client_publish_qos2(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
client_rejects_publish(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
client_receive_maximum_min(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
client_receive_maximum_large(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
unsubscribe_success(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
unsubscribe_topic_not_found(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
subscription_option_no_local(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
subscription_option_no_local_wildcards(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
subscription_option_retain_as_published(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
subscription_option_retain_as_published_wildcards(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
subscription_option_retain_handling(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
subscription_identifier(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
subscription_identifier_amqp091(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
subscription_identifier_at_most_once_dead_letter(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
at_most_once_dead_letter_detect_cycle(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
subscription_options_persisted(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
subscription_options_modify(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
subscription_options_modify_qos1(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
subscription_options_modify_qos0(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
session_upgrade_v3_v5_qos1(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
session_upgrade_v3_v5_qos0(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
session_upgrade_v3_v5_amqp091_pub(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
compatibility_v3_v5(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
session_upgrade_v3_v5_unsubscribe(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
session_upgrade_v4_v5_no_queue_bind_permission(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
amqp091_cc_header(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
publish_property_content_type(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
publish_property_payload_format_indicator(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
publish_property_response_topic_correlation_data(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
publish_property_user_property(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
disconnect_with_will(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
will_qos2(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
will_delay_greater_than_session_expiry(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
will_delay_less_than_session_expiry(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
will_delay_equals_session_expiry(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
will_delay_session_expiry_zero(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
will_delay_reconnect_no_will(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
will_delay_reconnect_with_will(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
will_delay_session_takeover(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
will_delay_message_expiry(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
will_delay_message_expiry_publish_properties(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
will_delay_properties(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
will_properties(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
retain_properties(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
topic_alias_client_to_server(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
topic_alias_server_to_client(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
topic_alias_bidirectional(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
topic_alias_invalid(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
topic_alias_unknown(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
topic_alias_disallowed(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
topic_alias_retained_message(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
topic_alias_disallowed_retained_message(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
extended_auth(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
headers_exchange(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
consistent_hash_exchange(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
session_migrate_v3_v5(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
session_takeover_v3_v5(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
will_delay_node_restart(Config) -> v5_SUITE:?FUNCTION_NAME(Config).
