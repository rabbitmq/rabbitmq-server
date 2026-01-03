%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(auth_http_server).

-export([init/2]).

init(Req0, #{} = State) ->
    Path = cowboy_req:path(Req0),
    Query = cowboy_req:parse_qs(Req0),
    ct:pal("~s received request on path ~s with query ~tp",
           [?MODULE, Path, Query]),
    RespBody = handle(Path, Query),
    Req = cowboy_req:reply(200,
                           #{<<"content-type">> => <<"text/plain; charset=utf-8">>},
                           RespBody,
                           Req0),
    {ok, Req, State}.

handle(<<"/auth/user">>, _Query) ->
    <<"allow">>;
handle(<<"/auth/vhost">>, _Query) ->
    <<"allow">>;
handle(<<"/auth/resource">>, Query) ->
    case proplists:get_value(<<"resource">>, Query) of
        <<"queue">> ->
            <<"allow">>;
        <<"exchange">> ->
            <<"deny Creating or deleting exchanges is forbidden for all client apps ❌"/utf8>>
    end.
