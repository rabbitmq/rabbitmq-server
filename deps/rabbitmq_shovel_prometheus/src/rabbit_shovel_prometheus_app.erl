%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%
-module(rabbit_shovel_prometheus_app).

-behavior(application).

-export([start/0, stop/0, start/2, stop/1]).

start(normal, []) ->
  {ok, _} = application:ensure_all_started(prometheus),
    _ = rabbit_shovel_prometheus_collector:start(),
    rabbit_shovel_prometheus_sup:start_link().

stop(_State) ->
  _ = rabbit_shovel_prometheus_collector:stop(),
  ok.


start() ->
  _ = rabbit_shovel_prometheus_collector:start().

stop() -> ok.

