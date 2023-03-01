%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-type option(T) :: undefined | T.

-define(PROTOCOL_NAMES,
        [{3, <<"MQIsdp">>},
         {4, <<"MQTT">>}]).

%% packet types

-define(CONNECT,      1).
-define(CONNACK,      2).
-define(PUBLISH,      3).
-define(PUBACK,       4).
-define(PUBREC,       5).
-define(PUBREL,       6).
-define(PUBCOMP,      7).
-define(SUBSCRIBE,    8).
-define(SUBACK,       9).
-define(UNSUBSCRIBE, 10).
-define(UNSUBACK,    11).
-define(PINGREQ,     12).
-define(PINGRESP,    13).
-define(DISCONNECT,  14).

-type packet_type() :: ?CONNECT .. ?DISCONNECT.

%% connect return codes

%% Connection accepted.
-define(CONNACK_ACCEPT,                 0).
%% The Server does not support the level of the MQTT protocol requested by the Client.
-define(CONNACK_UNACCEPTABLE_PROTO_VER, 1).
%% The Client identifier is correct UTF-8 but not allowed by the Server.
-define(CONNACK_ID_REJECTED,            2).
%% The Network Connection has been made but the MQTT service is unavailable.
-define(CONNACK_SERVER_UNAVAILABLE,     3).
%% The data in the user name or password is malformed.
-define(CONNACK_BAD_CREDENTIALS,        4).
%% The Client is not authorized to connect.
-define(CONNACK_NOT_AUTHORIZED,         5).

-type connack_return_code() :: ?CONNACK_ACCEPT..?CONNACK_NOT_AUTHORIZED.

%% qos levels

-define(QOS_0, 0).
-define(QOS_1, 1).
-define(QOS_2, 2).
-define(SUBACK_FAILURE, 16#80).

-type qos() :: ?QOS_0 | ?QOS_1 | ?QOS_2.

%% Packet identifier is a non zero two byte integer.
-type packet_id() :: 1..16#ffff.

%% Defining a single correlation term (sequence number) for the will message is
%% sufficient because there can be only a single will message per MQTT session.
%% To prevent clashes with a Packet ID and given Packet IDs must be non-zero, we choose 0.
-define(WILL_MSG_QOS_1_CORRELATION, 0).

-record(mqtt_packet_fixed,    {type = 0 :: packet_type(),
                               dup = false :: boolean(),
                               qos = 0 :: qos(),
                               retain = false :: boolean()
                              }).

-record(mqtt_packet,          {fixed :: #mqtt_packet_fixed{},
                               variable :: option(tuple()),
                               payload :: option(iodata())
                              }).

-type mqtt_packet() :: #mqtt_packet{}.

-record(mqtt_packet_connect,  {proto_ver :: 3 | 4,
                               will_retain :: boolean(),
                               will_qos :: qos(),
                               will_flag :: boolean(),
                               clean_sess :: boolean(),
                               keep_alive :: non_neg_integer(),
                               client_id :: binary(),
                               will_topic :: option(binary()),
                               will_msg :: option(binary()),
                               username :: option(binary()),
                               password :: option(binary())}).

-record(mqtt_packet_connack,  {session_present :: boolean(),
                               return_code :: connack_return_code()}).

-record(mqtt_packet_publish,  {topic_name :: undefined | binary(),
                               packet_id :: packet_id()}).

-record(mqtt_topic,           {name :: binary(),
                               qos}).

-record(mqtt_packet_subscribe,{packet_id :: packet_id(),
                               topic_table :: nonempty_list(#mqtt_topic{})
                              }).

-record(mqtt_packet_suback,   {packet_id :: packet_id(),
                               qos_table = []}).

%% MQTT application message.
-record(mqtt_msg, {retain :: boolean(),
                   qos :: qos(),
                   topic :: binary(),
                   dup :: boolean(),
                   packet_id :: option(packet_id()) | ?WILL_MSG_QOS_1_CORRELATION,
                   payload :: binary()}).

-type mqtt_msg() :: #mqtt_msg{}.

%% does not include vhost because vhost is used in the (D)ETS table name
-record(retained_message, {topic :: binary(),
                           mqtt_msg :: mqtt_msg()}).

-type retained_message() :: #retained_message{}.
