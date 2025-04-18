%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(rabbit_msg_interceptor_timestamp).
-behaviour(rabbit_msg_interceptor).

-include("mc.hrl").

%% For backwards compat, we use the key defined in the old plugin
%% https://github.com/rabbitmq/rabbitmq-message-timestamp
-define(KEY_INCOMING, <<"timestamp_in_ms">>).
-define(KEY_OUTGOING, <<"x-opt-rabbitmq-sent-time">>).

-export([intercept/4]).

intercept(Msg0, _Ctx, incoming, #{incoming := _True} = Cfg) ->
    Overwrite = maps:get(overwrite, Cfg),
    Ts = mc:get_annotation(?ANN_RECEIVED_AT_TIMESTAMP, Msg0),
    Msg = rabbit_msg_interceptor:set_annotation(Msg0, ?KEY_INCOMING, Ts, Overwrite),
    set_timestamp(Msg, Ts, Overwrite);
intercept(Msg, _Ctx, outgoing, #{outgoing := _True}) ->
    Ts = os:system_time(millisecond),
    mc:set_annotation(?KEY_OUTGOING, Ts, Msg);
intercept(Msg, _MsgIcptCtx, _Stage, _Cfg) ->
    Msg.

set_timestamp(Msg, Ts, true) ->
    mc:set_annotation(?ANN_TIMESTAMP, Ts, Msg);
set_timestamp(Msg, Ts, false) ->
    case mc:timestamp(Msg) of
        undefined ->
            mc:set_annotation(?ANN_TIMESTAMP, Ts, Msg);
        _ ->
            Msg
    end.
