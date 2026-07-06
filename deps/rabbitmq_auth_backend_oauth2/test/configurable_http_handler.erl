%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(configurable_http_handler).

-behavior(cowboy_handler).

-export([init/2, terminate/3]).

%% Replies with a fixed status, headers and body, taken verbatim from the
%% route's State. Used to simulate HTTP responses that a real server would
%% not normally hand back to the exact route under test.
init(Req, State) ->
    Status = maps:get(status, State, 200),
    Headers = maps:get(headers, State, #{<<"content-type">> => <<"application/json">>}),
    Body = maps:get(body, State, <<"{}">>),
    Req2 = cowboy_req:reply(Status, Headers, Body, Req),
    {ok, Req2, State}.

terminate(_Reason, _Req, _State) ->
    ok.
