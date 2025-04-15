%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(rabbit_message_interceptor_routing_node).
-behaviour(rabbit_message_interceptor).

-define(HEADER_ROUTING_NODE, <<"x-routed-by">>).

-export([intercept/4]).

intercept(Msg, _Ctx, incoming_message_interceptors, Config) ->
    Node = atom_to_binary(node()),
    Overwrite = maps:get(overwrite, Config),
    rabbit_message_interceptor:set_annotation(Msg,
                                              ?HEADER_ROUTING_NODE,
                                              Node,
                                              Overwrite);
intercept(Msg, _Ctx, _Group, _Config) ->
    Msg.
