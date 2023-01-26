%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mqtt_retained_msg_store).

-include("rabbit_mqtt_packet.hrl").

-callback new(Directory :: file:name_all(), rabbit_types:vhost()) ->
    State :: any().

-callback recover(Directory :: file:name_all(), rabbit_types:vhost()) ->
    {ok, State :: any()} | {error, Reason :: term()}.

-callback insert(Topic :: binary(), mqtt_msg(), State :: any()) ->
    ok.

-callback lookup(Topic :: binary(), State :: any()) ->
    retained_message() | not_found.

-callback delete(Topic :: binary(), State :: any()) ->
    ok.

-callback terminate(State :: any()) ->
    ok.

%% TODO Support retained messages in RabbitMQ cluster, for
%% 1. support PUBLISH with retain on a different node than SUBSCRIBE
%% 2. replicate retained message for data safety
%%
%% Possible solution for 1.
%% * retained message store backend does RPCs to peer nodes to lookup and delete
%%
%% Possible solutions for 2.
%% * rabbitmq_mqtt_retained_msg_store_khepri
%% * rabbitmq_mqtt_retained_msg_store_ra (implementing our own ra machine)
