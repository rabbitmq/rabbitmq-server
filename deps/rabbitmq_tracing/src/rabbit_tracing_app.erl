%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_tracing_app).

-behaviour(application).
-export([start/2, stop/1]).

start(_Type, _StartArgs) ->
    rabbit_tracing_sup:start_link().

stop(_State) ->
    ok.
