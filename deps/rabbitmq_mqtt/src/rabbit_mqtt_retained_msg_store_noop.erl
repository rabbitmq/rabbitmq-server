%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mqtt_retained_msg_store_noop).

-behaviour(rabbit_mqtt_retained_msg_store).
-include("rabbit_mqtt.hrl").

-export([new/2, recover/2, insert/3, lookup/2, delete/2, terminate/1]).

new(_Dir, _VHost) ->
  ok.

recover(_Dir, _VHost) ->
  {ok, ok}.

insert(_Topic, _Msg, _State) ->
  ok.

lookup(_Topic, _State) ->
  not_found.

delete(_Topic, _State) ->
  ok.

terminate(_State) ->
  ok.
