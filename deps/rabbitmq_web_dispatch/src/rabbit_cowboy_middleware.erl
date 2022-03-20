%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_cowboy_middleware).
-behavior(cowboy_middleware).

-export([execute/2]).

execute(Req, Env) ->
    %% Find the correct dispatch list for this path.
    Listener = maps:get(rabbit_listener, Env),
    case rabbit_web_dispatch_registry:lookup(Listener, Req) of
        {ok, Dispatch} ->
            {ok, Req, maps:put(dispatch, Dispatch, Env)};
        {error, Reason} ->
            Req2 = cowboy_req:reply(500,
                #{<<"content-type">> => <<"text/plain">>},
                "Registry Error: " ++ io_lib:format("~p", [Reason]), Req),
            {stop, Req2}
    end.
