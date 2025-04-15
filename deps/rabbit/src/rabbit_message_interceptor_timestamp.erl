%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(rabbit_message_interceptor_timestamp).
-behaviour(rabbit_message_interceptor).

-include("mc.hrl").

-define(HEADER_TIMESTAMP, <<"timestamp_in_ms">>).

-export([intercept/4]).

intercept(Msg0, _Ctx, incoming_message_interceptors, Config) ->
    Ts = mc:get_annotation(?ANN_RECEIVED_AT_TIMESTAMP, Msg0),
    Overwrite = maps:get(overwrite, Config),
    Msg = rabbit_message_interceptor:set_annotation(Msg0,
                                                    ?HEADER_TIMESTAMP,
                                                    Ts,
                                                    Overwrite),
    set_timestamp(Msg, Ts, Overwrite);
intercept(Msg, _MsgInterceptorCtx, _Group, _Config) ->
    Msg.

set_timestamp(Msg, Ts, Overwrite) ->
    case {mc:timestamp(Msg), Overwrite} of
        {ExistingTs, false} when is_integer(ExistingTs) ->
            Msg;
        _ ->
            mc:set_annotation(?ANN_TIMESTAMP, Ts, Msg)
    end.
