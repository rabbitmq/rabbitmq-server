%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.

%% This module exists since 3.12 replacing plugins rabbitmq-message-timestamp
%% and rabbitmq-routing-node-stamp. Instead of using these plugins, RabbitMQ core can
%% now be configured to add such headers. This enables non-AMQP 0.9.1 protocols (that
%% do not use rabbit_channel) to also add AMQP 0.9.1 headers to incoming messages.
-module(rabbit_message_interceptor).

-export([intercept/1]).

-define(HEADER_TIMESTAMP, <<"timestamp_in_ms">>).
-define(HEADER_ROUTING_NODE, <<"x-routed-by">>).

-spec intercept(mc:state()) -> mc:state().
intercept(Msg) ->
    Interceptors = persistent_term:get({rabbit, incoming_message_interceptors}, []),
    lists:foldl(fun({InterceptorName, Overwrite}, M) ->
                        intercept(M, InterceptorName, Overwrite)
                end, Msg, Interceptors).

intercept(Msg, set_header_routing_node, Overwrite) ->
    Node = atom_to_binary(node()),
    set_annotation(Msg, ?HEADER_ROUTING_NODE, Node, Overwrite);
intercept(Msg0, set_header_timestamp, Overwrite) ->
    Millis = os:system_time(millisecond),
    Msg = set_annotation(Msg0, ?HEADER_TIMESTAMP, Millis, Overwrite),
    set_timestamp(Msg, Millis, Overwrite).

-spec set_annotation(mc:state(), mc:ann_key(), mc:ann_value(), boolean()) -> mc:state().
set_annotation(Msg, Key, Value, Overwrite) ->
    case {mc:x_header(Key, Msg), Overwrite} of
        {Val, false} when Val =/= undefined ->
            Msg;
        _ ->
            mc:set_annotation(Key, Value, Msg)
    end.

-spec set_timestamp(mc:state(), pos_integer(), boolean()) -> mc:state().
set_timestamp(Msg, Timestamp, Overwrite) ->
    case {mc:timestamp(Msg), Overwrite} of
        {Ts, false} when is_integer(Ts) ->
            Msg;
        _ ->
            mc:set_annotation(timestamp, Timestamp, Msg)
    end.
