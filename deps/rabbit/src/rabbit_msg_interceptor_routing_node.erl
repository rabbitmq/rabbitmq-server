%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(rabbit_msg_interceptor_routing_node).
-behaviour(rabbit_msg_interceptor).

-define(KEY, <<"x-routed-by">>).

-export([intercept/4]).

intercept(Msg, _Ctx, incoming, Cfg) ->
    Node = atom_to_binary(node()),
    Overwrite = maps:get(overwrite, Cfg),
    rabbit_msg_interceptor:set_annotation(Msg, ?KEY, Node, Overwrite);
intercept(Msg, _Ctx, _Stage, _Cfg) ->
    Msg.
