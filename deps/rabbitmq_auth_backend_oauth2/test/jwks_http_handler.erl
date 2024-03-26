%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(jwks_http_handler).

-behavior(cowboy_handler).

-export([init/2, terminate/3]).

init(Req, State) ->
    Keys = proplists:get_value(keys, State, []),
    Body = rabbit_json:encode(#{keys => Keys}),
    Headers = #{<<"content-type">> => <<"application/json">>},
    Req2 = cowboy_req:reply(200, Headers, Body, Req),
    {ok, Req2, State}.

terminate(_Reason, _Req, _State) ->
    ok.
