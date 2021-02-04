%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-define(CLIENT_ID_MAXLEN, 23).

%% reader state
-record(state,      { socket,
                      conn_name,
                      await_recv,
                      deferred_recv,
                      received_connect_frame,
                      connection_state,
                      keepalive,
                      keepalive_sup,
                      conserve,
                      parse_state,
                      proc_state,
                      connection,
                      stats_timer }).

%% processor state
-record(proc_state, { socket,
                      subscriptions,
                      consumer_tags,
                      unacked_pubs,
                      awaiting_ack,
                      awaiting_seqno,
                      message_id,
                      client_id,
                      clean_sess,
                      will_msg,
                      channels,
                      connection,
                      exchange,
                      adapter_info,
                      ssl_login_name,
                      %% Retained messages handler. See rabbit_mqtt_retainer_sup
                      %% and rabbit_mqtt_retainer.
                      retainer_pid,
                      auth_state,
                      send_fun,
                      peer_addr,
                      mqtt2amqp_fun,
                      amqp2mqtt_fun,
                      register_state }).

-record(auth_state, {username,
                     user,
                     vhost}).

%% does not include vhost: it is used in
%% the table name
-record(retained_message, {topic,
                           mqtt_msg}).

-define(INFO_ITEMS,
    [host,
     port,
     peer_host,
     peer_port,
     protocol,
     channels,
     channel_max,
     frame_max,
     client_properties,
     ssl,
     ssl_protocol,
     ssl_key_exchange,
     ssl_cipher,
     ssl_hash,
     conn_name,
     connection_state,
     connection,
     consumer_tags,
     unacked_pubs,
     awaiting_ack,
     awaiting_seqno,
     message_id,
     client_id,
     clean_sess,
     will_msg,
     exchange,
     ssl_login_name,
     retainer_pid,
     user,
     vhost]).

-define(MQTT_GUIDE_URL, <<"https://rabbitmq.com/mqtt.html">>).
