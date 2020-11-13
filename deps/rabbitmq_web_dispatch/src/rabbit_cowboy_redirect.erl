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
%% Copyright (c) 2010-2015 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_cowboy_redirect).

-export([init/3]).
-export([handle/2]).
-export([terminate/3]).

init(_, Req, RedirectPort) ->
    {ok, Req, RedirectPort}.

handle(Req0, RedirectPort) ->
    URI = cowboy_req:uri(Req0, #{port => RedirectPort}),
    Req = cowboy_req:reply(301, #{<<"location">> => URI}, Req0),
    {ok, Req, RedirectPort}.

terminate(_, _, _) ->
    ok.
