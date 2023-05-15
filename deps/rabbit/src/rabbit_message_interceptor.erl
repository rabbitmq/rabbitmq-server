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

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").

-define(HEADER_TIMESTAMP, <<"timestamp_in_ms">>).
-define(HEADER_ROUTING_NODE, <<"x-routed-by">>).

-type content() :: rabbit_types:decoded_content().

-spec intercept(content()) -> content().
intercept(Content) ->
    Interceptors = persistent_term:get({rabbit, incoming_message_interceptors}, []),
    lists:foldl(fun(I, C) ->
                        intercept(C, I)
                end, Content, Interceptors).

intercept(Content, {set_header_routing_node, Overwrite}) ->
    Node = atom_to_binary(node()),
    set_header(Content, {?HEADER_ROUTING_NODE, longstr, Node}, Overwrite);
intercept(Content0, {set_header_timestamp, Overwrite}) ->
    NowMs = os:system_time(millisecond),
    NowSecs = NowMs div 1_000,
    Content = set_header(Content0, {?HEADER_TIMESTAMP, long, NowMs}, Overwrite),
    set_property_timestamp(Content, NowSecs, Overwrite).

-spec set_header(content(),
                 {binary(), rabbit_framing:amqp_field_type(), rabbit_framing:amqp_value()},
                 boolean()) ->
    content().
set_header(Content = #content{properties = Props = #'P_basic'{headers = Headers0}},
           Header = {Key, Type, Value}, Overwrite) ->
    case {rabbit_basic:header(Key, Headers0), Overwrite} of
        {Val, false} when Val =/= undefined ->
            Content;
        _ ->
            Headers = if Headers0 =:= undefined -> [Header];
                         true -> rabbit_misc:set_table_value(Headers0, Key, Type, Value)
                      end,
            Content#content{properties = Props#'P_basic'{headers = Headers},
                            properties_bin = none}
    end.

-spec set_property_timestamp(content(), pos_integer(), boolean()) -> content().
set_property_timestamp(Content = #content{properties = Props = #'P_basic'{timestamp = Ts}},
                       Timestamp, Overwrite) ->
    case {Ts, Overwrite} of
        {Secs, false} when is_integer(Secs) ->
            Content;
        _ ->
            Content#content{properties = Props#'P_basic'{timestamp = Timestamp},
                            properties_bin = none}
    end.
