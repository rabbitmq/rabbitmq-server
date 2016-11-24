%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
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

-module(rabbit_cowboy_middleware).
-behavior(cowboy_middleware).

-export([execute/2]).
-export([onresponse/4]).

execute(Req, Env) ->
    %% Pre-parse the query string.
    {_, Req1} = cowboy_req:qs_vals(Req),

    %% Find the correct dispatch list for this path.
    {_, Listener} = lists:keyfind(rabbit_listener, 1, Env),
    case rabbit_web_dispatch_registry:lookup(Listener, Req1) of
        {ok, Dispatch} ->
            {ok, Req1, [{dispatch, Dispatch}|Env]};
        {error, Reason} ->
            {ok, Req2} = cowboy_req:reply(500,
                [{<<"content-type">>, <<"text/plain">>}],
                "Registry Error: " ++ io_lib:format("~p", [Reason]), Req1),
            {halt, Req2}
    end.

onresponse(Status = 404, Headers0, Body = <<>>, Req0) ->
    log_access(Status, Body, Req0),
    Json0 = {struct,
            [{error,  list_to_binary(httpd_util:reason_phrase(Status))},
             {reason, <<"Not Found">>}]},
    Json = mochijson2:encode(Json0),
    Headers1 = lists:keystore(<<"content-type">>, 1, Headers0,
                              {<<"content-type">>, <<"application/json">>}),
    Headers = lists:keystore(<<"content-length">>, 1, Headers1,
                              {<<"content-length">>, integer_to_list(iolist_size(Json))}),
    {ok, Req} = cowboy_req:reply(Status, Headers, Json , Req0),
    Req;

onresponse(Status, _, Body, Req) ->
    log_access(Status, Body, Req),
    Req.

log_access(Status, Body, Req) ->
    webmachine_log:log_access({Status, Body, Req}).
