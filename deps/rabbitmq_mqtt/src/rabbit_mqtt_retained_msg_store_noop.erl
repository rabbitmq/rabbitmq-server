%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mqtt_retained_msg_store_noop).

-behaviour(rabbit_mqtt_retained_msg_store).

-export([new/2, recover/2, insert/3, lookup/2, delete/2, terminate/1]).

new(_Dir, _VHost) ->
  ok.

recover(_Dir, _VHost) ->
  {ok, ok, #{}}.

insert(_Topic, _Msg, _State) ->
  ok.

lookup(_Topic, _State) ->
  undefined.

delete(_Topic, _State) ->
  ok.

terminate(_State) ->
  ok.
