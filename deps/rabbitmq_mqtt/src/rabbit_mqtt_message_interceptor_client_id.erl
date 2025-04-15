%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(rabbit_mqtt_message_interceptor_client_id).
-behaviour(rabbit_message_interceptor).

-export([intercept/4]).

-define(KEY, <<"x-opt-mqtt-client-id">>).

intercept(Msg,
          #{protocol := Proto,
            client_id := ClientId},
          incoming_message_interceptors,
          _Config)
  when Proto =:= mqtt50 orelse
       Proto =:= mqtt311 orelse
       Proto =:= mqtt310 ->
    mc:set_annotation(?KEY, ClientId, Msg);
intercept(Msg, _Ctx, _Group, _Config) ->
    Msg.
