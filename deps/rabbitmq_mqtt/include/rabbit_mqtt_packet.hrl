%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-define(PERSISTENT_TERM_MAX_PACKET_SIZE_UNAUTHENTICATED, mqtt_max_packet_size_unauthenticated).
-define(PERSISTENT_TERM_MAX_PACKET_SIZE_AUTHENTICATED, mqtt_max_packet_size_authenticated).

-type protocol_version() :: 3..5.

-type property_name() :: atom().
-type property_value() :: any().
-type properties() :: #{property_name() := property_value()}.

%% Packet identifier is a non zero two byte integer.
-define(MAX_PACKET_ID, 16#FFFF).
-type packet_id() :: 1..?MAX_PACKET_ID.

%% Defining a single correlation term (sequence number) for the will message is
%% sufficient because there can be only a single will message per MQTT session.
%% To prevent clashes with a Packet ID and given Packet IDs must be non-zero, we choose 0.
-define(WILL_MSG_QOS_1_CORRELATION, 0).

%% MQTT 3.1.1 spec:
%% "This allows applications to send Control Packets of size up to 268,435,455 (256 MB).
%% The representation of this number on the wire is: 0xFF, 0xFF, 0xFF, 0x7F."
%% 268,435,455 = 16#FFFFFFF
-define(VARIABLE_BYTE_INTEGER_MAX, 16#FFFFFFF).
-define(MAX_PACKET_SIZE, ?VARIABLE_BYTE_INTEGER_MAX).
-type max_packet_size() :: 1..?MAX_PACKET_SIZE.

-define(UINT_MAX, 16#FFFFFFFF).

%% MQTT Control Packet types
-define(CONNECT, 1).
-define(CONNACK, 2).
-define(PUBLISH, 3).
-define(PUBACK, 4).
-define(PUBREC, 5).
-define(PUBREL, 6).
-define(PUBCOMP, 7).
-define(SUBSCRIBE, 8).
-define(SUBACK, 9).
-define(UNSUBSCRIBE, 10).
-define(UNSUBACK, 11).
-define(PINGREQ, 12).
-define(PINGRESP, 13).
-define(DISCONNECT, 14).
-define(AUTH, 15). %% only MQTT v5.0
%%
-type packet_type() :: ?CONNECT..?AUTH.

%% set in CONNECT variable header
-define(PROTOCOL_NAMES,
        [{3, <<"MQIsdp">>},
         {4, <<"MQTT">>},
         {5, <<"MQTT">>}]).

%% MQTT 5.0 Reason Codes are used across various MQTT Control Packets.
%%
%% Reason Codes less than 0x80 indicate successful completion of an operation.
-define(RC_SUCCESS, 16#00). %% The normal Reason Code for success
-define(RC_NORMAL_DISCONNECTION, 16#00).
-define(RC_GRANTED_QOS_0, 16#00).
-define(RC_GRANTED_QOS_1, 16#01).
-define(RC_GRANTED_QOS_2, 16#02).
-define(RC_DISCONNECT_WITH_WILL_MESSAGE, 16#04).
-define(RC_NO_MATCHING_SUBSCRIBERS, 16#10).
-define(RC_NO_SUBSCRIPTION_EXISTED, 16#11).
-define(RC_CONTINUE_AUTHENTICATION, 16#18).
-define(RC_RE_AUTHENTICATE, 16#19).
%% Reason Code values of 0x80 or greater indicate failure.
-define(RC_UNSPECIFIED_ERROR, 16#80).
-define(RC_MALFORMED_PACKET, 16#81).
-define(RC_PROTOCOL_ERROR, 16#82).
-define(RC_IMPLEMENTATION_SPECIFIC_ERROR, 16#83).
-define(RC_UNSUPPORTED_PROTOCOL_VERSION, 16#84).
-define(RC_CLIENT_IDENTIFIER_NOT_VALID, 16#85).
-define(RC_BAD_USER_NAME_OR_PASSWORD, 16#86).
-define(RC_NOT_AUTHORIZED, 16#87).
-define(RC_SERVER_UNAVAILABLE, 16#88).
-define(RC_SERVER_BUSY, 16#89).
-define(RC_BANNED, 16#8A).
-define(RC_SERVER_SHUTTING_DOWN, 16#8B).
-define(RC_BAD_AUTHENTICATION_METHOD, 16#8C).
-define(RC_KEEP_ALIVE_TIMEOUT, 16#8D).
-define(RC_SESSION_TAKEN_OVER, 16#8E).
-define(RC_TOPIC_FILTER_INVALID, 16#8F).
-define(RC_TOPIC_NAME_INVALID, 16#90).
-define(RC_PACKET_IDENTIFIER_IN_USE, 16#91).
-define(RC_PACKET_IDENTIFIER_NOT_FOUND, 16#92).
-define(RC_RECEIVE_MAXIMUM_EXCEEDED, 16#93).
-define(RC_TOPIC_ALIAS_INVALID, 16#94).
-define(RC_PACKET_TOO_LARGE, 16#95).
-define(RC_MESSAGE_RATE_TOO_HIGH, 16#96).
-define(RC_QUOTA_EXCEEDED, 16#97).
-define(RC_ADMINISTRATIVE_ACTION, 16#98).
-define(RC_PAYLOAD_FORMAT_INVALID, 16#99).
-define(RC_RETAIN_NOT_SUPPORTED, 16#9A).
-define(RC_QOS_NOT_SUPPORTED, 16#9B).
-define(RC_USE_ANOTHER_SERVER, 16#9C).
-define(RC_SERVER_MOVED, 16#9D).
-define(RC_SHARED_SUBSCRIPTIONS_NOT_SUPPORTED, 16#9E).
-define(RC_CONNECTION_RATE_EXCEEDED, 16#9F).
-define(RC_MAXIMUM_CONNECT_TIME, 16#A0).
-define(RC_SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED, 16#A1).
-define(RC_WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED, 16#A2).
%%
-type reason_code() :: ?RC_SUCCESS..?RC_RE_AUTHENTICATE |
                       ?RC_UNSPECIFIED_ERROR..?RC_WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED.

%% MQTT 3.1.1 Connect return codes in CONNACK variable header
%%
%% Connection accepted.
-define(CONNACK_ACCEPT, 0).
%% The Server does not support the level of the MQTT protocol requested by the Client.
-define(CONNACK_UNACCEPTABLE_PROTO_VER, 1).
%% The Client identifier is correct UTF-8 but not allowed by the Server.
-define(CONNACK_ID_REJECTED, 2).
%% The Network Connection has been made but the MQTT service is unavailable.
-define(CONNACK_SERVER_UNAVAILABLE, 3).
%% The data in the user name or password is malformed.
-define(CONNACK_BAD_CREDENTIALS, 4).
%% The Client is not authorized to connect.
-define(CONNACK_NOT_AUTHORIZED, 5).
%%
-type connect_return_code() :: ?CONNACK_ACCEPT..?CONNACK_NOT_AUTHORIZED.

-type connect_code() :: connect_return_code() | %% v3 or v4
                        reason_code(). %% v5

-define(SUBACK_FAILURE, ?RC_UNSPECIFIED_ERROR).

%% Quality of Service levels
-define(QOS_0, 0). %% at most once
-define(QOS_1, 1). %% at least once
-define(QOS_2, 2). %% exactly once
%%
-type qos() :: ?QOS_0 | ?QOS_1 | ?QOS_2.

-record(mqtt_packet_fixed, {type :: packet_type(),
                            dup = false :: boolean(),
                            qos = 0 :: qos(),
                            retain = false :: boolean()
                           }).

-record(mqtt_packet, {fixed :: #mqtt_packet_fixed{},
                      variable :: option(tuple()),
                      payload :: option(iodata())
                     }).

-type mqtt_packet() :: #mqtt_packet{}.

-record(mqtt_packet_connect, {proto_ver :: protocol_version(),
                              will_retain :: boolean(),
                              will_qos :: qos(),
                              will_flag :: boolean(),
                              %% corresponds to Clean Start in MQTT 5.0
                              clean_sess :: boolean(),
                              keep_alive :: non_neg_integer(),
                              props :: properties(),
                              client_id :: binary(),
                              will_props :: properties(),
                              will_topic :: option(binary()),
                              will_msg :: option(binary()),
                              username :: option(binary()),
                              password :: option(binary())
                             }).

-record(mqtt_packet_connack, {session_present :: boolean(),
                              code :: connect_code(),
                              props = #{} :: properties()}).

-record(mqtt_packet_publish, {topic_name :: binary(),
                              %% "The Packet Identifier field is only present in
                              %% PUBLISH packets where the QoS level is 1 or 2."
                              packet_id :: option(packet_id()),
                              props = #{} :: properties()
                             }).

-record(mqtt_packet_puback, {packet_id :: packet_id(),
                             reason_code = ?RC_SUCCESS :: reason_code(),
                             props = #{} :: properties()
                            }).

-record(mqtt_topic, {filter :: binary(),
                     qos :: qos(),
                     no_local :: boolean(),
                     retain_as_published :: boolean(),
                     retain_handling :: 0..2
                    }).

-record(mqtt_packet_subscribe, {packet_id :: packet_id(),
                                props :: properties(),
                                topics :: [#mqtt_topic{}, ...]
                               }).

-record(mqtt_packet_suback, {packet_id :: packet_id(),
                             props = #{} :: properties(),
                             reason_codes :: [reason_code(), ...]
                            }).

-record(mqtt_packet_unsubscribe, {packet_id :: packet_id(),
                                  props :: properties(),
                                  topics :: [binary(), ...]
                                 }).

-record(mqtt_packet_unsuback, {packet_id :: packet_id(),
                               props = #{} :: properties(),
                               reason_codes = [] :: [reason_code()]
                              }).

-record(mqtt_packet_disconnect, {reason_code = ?RC_NORMAL_DISCONNECTION :: reason_code(),
                                 props = #{} :: properties()
                                }).

%% TODO Add compatibility for old #mqtt_msg{} record that could still be stored on disk
%% in retained message stores (both ETS and DETS): Could either migrate or do the conversion
%% lazily in rabbit_mqtt_retained_msg_store:lookup/2.
%%
%% MQTT application message.
-record(mqtt_msg, {retain :: boolean(),
                   qos :: qos(),
                   topic :: binary(),
                   dup :: boolean(),
                   packet_id :: option(packet_id()) | ?WILL_MSG_QOS_1_CORRELATION,
                   payload :: binary(),
                   %% PUBLISH or Will properties
                   props :: properties(),
                   timestamp :: option(integer())
                  }).

-type mqtt_msg() :: #mqtt_msg{}.

%% does not include vhost because vhost is used in the (D)ETS table name
-record(retained_message, {topic :: binary(),
                           mqtt_msg :: mqtt_msg()
                          }).

-type retained_message() :: #retained_message{}.

-type option(T) :: undefined | T.
