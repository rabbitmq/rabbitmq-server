%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-define(PROTOCOL_NAMES,  [{3, "MQIsdp"}, {4, "MQTT"}]).

%% frame types

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

%% qos levels

-define(QOS_0, 0).
-define(QOS_1, 1).
-define(QOS_2, 2).

%% TODO
-type message_id() :: any().

-record(mqtt_frame, {fixed,
                     variable,
                     payload}).

-record(mqtt_frame_fixed,    {type   = 0,
                              dup    = 0,
                              qos    = 0,
                              retain = 0}).

-record(mqtt_frame_connect,  {proto_ver,
                              will_retain,
                              will_qos,
                              will_flag,
                              clean_sess,
                              keep_alive,
                              client_id,
                              will_topic,
                              will_msg,
                              username,
                              password}).

-record(mqtt_frame_connack,  {session_present,
                              return_code}).

-record(mqtt_frame_publish,  {topic_name,
                              message_id}).

-record(mqtt_topic,          {name,
                              qos}).

-record(mqtt_frame_subscribe,{message_id,
                              topic_table :: nonempty_list(#mqtt_topic{})
                             }).

-record(mqtt_frame_suback,   {message_id,
                              qos_table = []}).

-record(mqtt_frame_other,    {other}).

-record(mqtt_msg,            {retain :: boolean(),
                              qos :: ?QOS_0 | ?QOS_1 | ?QOS_2,
                              topic :: string(),
                              dup :: boolean(),
                              message_id :: message_id(),
                              payload :: binary()}).

-type mqtt_msg() :: #mqtt_msg{}.
