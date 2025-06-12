%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(rabbitmq_amqp_address).

-export([exchange/1,
         exchange/2,
         queue/1]).

-spec exchange(unicode:unicode_binary()) ->
    unicode:unicode_binary().
exchange(ExchangeName) ->
    ExchangeNameQuoted = uri_string:quote(ExchangeName),
    <<"/exchanges/", ExchangeNameQuoted/binary>>.

-spec exchange(unicode:unicode_binary(), unicode:unicode_binary()) ->
    unicode:unicode_binary().
exchange(ExchangeName, RoutingKey) ->
    ExchangeNameQuoted = uri_string:quote(ExchangeName),
    RoutingKeyQuoted = uri_string:quote(RoutingKey),
    <<"/exchanges/", ExchangeNameQuoted/binary, "/", RoutingKeyQuoted/binary>>.

-spec queue(unicode:unicode_binary()) ->
    unicode:unicode_binary().
queue(QueueName) ->
    QueueNameQuoted = uri_string:quote(QueueName),
    <<"/queues/", QueueNameQuoted/binary>>.
