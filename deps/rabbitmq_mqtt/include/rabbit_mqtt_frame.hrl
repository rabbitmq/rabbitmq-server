%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2020 Pivotal Software, Inc.  All rights reserved.
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

-define(CONNACK_ACCEPT,      0).
-define(CONNACK_PROTO_VER,   1). %% unacceptable protocol version
-define(CONNACK_INVALID_ID,  2). %% identifier rejected
-define(CONNACK_SERVER,      3). %% server unavailable
-define(CONNACK_CREDENTIALS, 4). %% bad user name or password
-define(CONNACK_AUTH,        5). %% not authorized

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

-record(mqtt_frame_subscribe,{message_id,
                              topic_table}).

-record(mqtt_frame_suback,   {message_id,
                              qos_table = []}).

-record(mqtt_topic,          {name,
                              qos}).

-record(mqtt_frame_other,    {other}).

-record(mqtt_msg,            {retain :: boolean(),
                              qos :: ?QOS_0 | ?QOS_1 | ?QOS_2,
                              topic :: string(),
                              dup :: boolean(),
                              message_id :: message_id(),
                              payload :: binary()}).

-type mqtt_msg() :: #mqtt_msg{}.
