%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_web_stomp_app).

-behaviour(application).
-export([start/2, stop/1]).

%%----------------------------------------------------------------------------

-spec start(_, _) -> {ok, pid()}.
start(_Type, _StartArgs) ->
    ok = rabbit_web_stomp_listener:init(),
    rabbit_web_stomp_sup:start_link().

-spec stop(_) -> ok.
stop(_State) ->
    ok.
