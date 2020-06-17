%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
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
