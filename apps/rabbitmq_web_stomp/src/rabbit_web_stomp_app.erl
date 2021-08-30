%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_web_stomp_app).

-behaviour(application).
-export([start/2, stop/1]).

%%----------------------------------------------------------------------------

-spec start(_, _) -> {ok, pid()}.
start(_Type, _StartArgs) ->
    ok = rabbit_web_stomp_listener:init(),
    EMPid = case rabbit_event:start_link() of
              {ok, Pid}                       -> Pid;
              {error, {already_started, Pid}} -> Pid
            end,
    gen_event:add_handler(EMPid, rabbit_web_stomp_internal_event_handler, []),
    rabbit_web_stomp_sup:start_link().

-spec stop(_) -> ok.
stop(State) ->
    rabbit_web_stomp_listener:stop(State).
