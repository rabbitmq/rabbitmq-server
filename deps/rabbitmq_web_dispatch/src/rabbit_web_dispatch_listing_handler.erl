%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_web_dispatch_listing_handler).

-export([init/2]).

init(Req0, Listener) ->
    HTMLPrefix =
        "<html xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en\">"
        "<head><title>RabbitMQ Web Server</title></head>"
        "<body><h1>RabbitMQ Web Server</h1><p>Contexts available:</p><ul>",
    HTMLSuffix = "</ul></body></html>",
    List =
        case rabbit_web_dispatch_registry:list(Listener) of
            [] ->
                "<li>No contexts installed</li>";
            Contexts ->
                [["<li><a href=\"/", Path, "/\">", Desc, "</a></li>"]
                 || {Path, Desc} <- Contexts]
        end,
    Req = cowboy_req:reply(200, #{}, [HTMLPrefix, List, HTMLSuffix], Req0),
    {ok, Req, Listener}.
