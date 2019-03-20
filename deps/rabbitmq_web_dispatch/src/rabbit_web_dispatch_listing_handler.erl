%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2010-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_web_dispatch_listing_handler).

-export([init/2]).
-export([terminate/3]).

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

terminate(_, _, _) ->
    ok.
