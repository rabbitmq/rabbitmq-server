%% The contents of this file are subject to the Mozilla Public License
%% Version 2.0 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/en-US/MPL/2.0/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is Pivotal Software, Inc.
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-define(COMMAND_DECLARE_PUBLISHER, 1).
-define(COMMAND_PUBLISH, 2).
-define(COMMAND_PUBLISH_CONFIRM, 3).
-define(COMMAND_PUBLISH_ERROR, 4).
-define(COMMAND_QUERY_PUBLISHER_SEQUENCE, 5).
-define(COMMAND_DELETE_PUBLISHER, 6).
-define(COMMAND_SUBSCRIBE, 7).
-define(COMMAND_DELIVER, 8).
-define(COMMAND_CREDIT, 9).
-define(COMMAND_STORE_OFFSET, 10).
-define(COMMAND_QUERY_OFFSET, 11).
-define(COMMAND_UNSUBSCRIBE, 12).
-define(COMMAND_CREATE_STREAM, 13).
-define(COMMAND_DELETE_STREAM, 14).
-define(COMMAND_METADATA, 15).
-define(COMMAND_METADATA_UPDATE, 16).
-define(COMMAND_PEER_PROPERTIES, 17).
-define(COMMAND_SASL_HANDSHAKE, 18).
-define(COMMAND_SASL_AUTHENTICATE, 19).
-define(COMMAND_TUNE, 20).
-define(COMMAND_OPEN, 21).
-define(COMMAND_CLOSE, 22).
-define(COMMAND_HEARTBEAT, 23).
-define(COMMAND_ROUTE, 24).
-define(COMMAND_PARTITIONS, 25).
-define(COMMAND_CONSUMER_UPDATE, 26).
-define(COMMAND_EXCHANGE_COMMAND_VERSIONS, 27).
-define(COMMAND_STREAM_STATS, 28).
-define(COMMAND_CREATE_SUPER_STREAM, 29).
-define(COMMAND_DELETE_SUPER_STREAM, 30).

-define(REQUEST, 0).
-define(RESPONSE, 1).

-define(VERSION_1, 1).
-define(VERSION_2, 2).

-define(RESPONSE_CODE_OK, 1).
-define(RESPONSE_CODE_STREAM_DOES_NOT_EXIST, 2).
-define(RESPONSE_CODE_SUBSCRIPTION_ID_ALREADY_EXISTS, 3).
-define(RESPONSE_CODE_SUBSCRIPTION_ID_DOES_NOT_EXIST, 4).
-define(RESPONSE_CODE_STREAM_ALREADY_EXISTS, 5).
-define(RESPONSE_CODE_STREAM_NOT_AVAILABLE, 6).
-define(RESPONSE_SASL_MECHANISM_NOT_SUPPORTED, 7).
-define(RESPONSE_AUTHENTICATION_FAILURE, 8).
-define(RESPONSE_SASL_ERROR, 9).
-define(RESPONSE_SASL_CHALLENGE, 10).
-define(RESPONSE_SASL_AUTHENTICATION_FAILURE_LOOPBACK, 11).
-define(RESPONSE_VHOST_ACCESS_FAILURE, 12).
-define(RESPONSE_CODE_UNKNOWN_FRAME, 13).
-define(RESPONSE_CODE_FRAME_TOO_LARGE, 14).
-define(RESPONSE_CODE_INTERNAL_ERROR, 15).
-define(RESPONSE_CODE_ACCESS_REFUSED, 16).
-define(RESPONSE_CODE_PRECONDITION_FAILED, 17).
-define(RESPONSE_CODE_PUBLISHER_DOES_NOT_EXIST, 18).
-define(RESPONSE_CODE_NO_OFFSET, 19).
-define(RESPONSE_SASL_CANNOT_CHANGE_MECHANISM, 20).
-define(RESPONSE_SASL_CANNOT_CHANGE_USERNAME, 21).

-define(OFFSET_TYPE_NONE, 0).
-define(OFFSET_TYPE_FIRST, 1).
-define(OFFSET_TYPE_LAST, 2).
-define(OFFSET_TYPE_NEXT, 3).
-define(OFFSET_TYPE_OFFSET, 4).
-define(OFFSET_TYPE_TIMESTAMP, 5).

-define(DEFAULT_INITIAL_CREDITS, 50000).
-define(DEFAULT_CREDITS_REQUIRED_FOR_UNBLOCKING, 12500).
-define(DEFAULT_FRAME_MAX, 1048576). %% 1 MiB
-define(DEFAULT_HEARTBEAT, 60). %% 60 seconds

-define(STREAM_QUEUE_TYPE, rabbit_stream_queue).

-define(INFO_ITEMS,
  [conn_name,
    pid,
    node,
    port,
    peer_port,
    host,
    peer_cert_issuer,
    peer_cert_subject,
    peer_cert_validity,
    peer_host,
    user,
    vhost,
    subscriptions,
    ssl,
    ssl_cipher,
    ssl_hash,
    ssl_key_exchange,
    ssl_protocol,
    connection_state,
    auth_mechanism,
    heartbeat,
    frame_max,
    client_properties,
    connected_at
    ]).

-define(CONSUMER_INFO_ITEMS, [
  connection_pid,
  node,
  subscription_id,
  stream,
  messages_consumed,
  offset,
  offset_lag,
  credits,
  active,
  activity_status,
  properties
  ]).

-define(PUBLISHER_INFO_ITEMS, [
  connection_pid,
  node,
  publisher_id,
  stream,
  reference,
  messages_published,
  messages_confirmed,
  messages_errored
  ]).

-define(CONSUMER_GROUP_INFO_ITEMS, [
  stream,
  reference,
  partition_index,
  consumers
  ]).

-define(GROUP_CONSUMER_INFO_ITEMS, [
  subscription_id,
  connection_name,
  state
]).

-define(STREAMS_GUIDE_URL, <<"https://rabbitmq.com/docs/streams">>).
