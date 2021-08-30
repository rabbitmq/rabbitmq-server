%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_web_dispatch_app).

-behaviour(application).
-export([start/2,stop/1]).

%% @spec start(_Type, _StartArgs) -> ServerRet
%% @doc application start callback for rabbit_web_dispatch.
start(_Type, _StartArgs) ->
    rabbit_web_dispatch_sup:start_link().

%% @spec stop(_State) -> ServerRet
%% @doc application stop callback for rabbit_web_dispatch.
stop(_State) ->
    ok.
