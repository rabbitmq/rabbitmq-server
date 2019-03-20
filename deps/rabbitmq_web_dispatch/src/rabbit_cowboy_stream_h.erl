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
%% Copyright (c) 2017 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_cowboy_stream_h).
-behavior(cowboy_stream).

-export([init/3]).
-export([data/4]).
-export([info/3]).
-export([terminate/3]).
-export([early_error/5]).

-record(state, {
    next :: any(),
    req :: cowboy_req:req()
}).

init(StreamId, Req, Opts) ->
    {Commands, Next} = cowboy_stream:init(StreamId, Req, Opts),
    {Commands, #state{next = Next, req = Req}}.

data(StreamId, IsFin, Data, State = #state{next = Next}) ->
    {Commands, Next1} = cowboy_stream:data(StreamId, IsFin, Data, Next),
    {Commands, State#state{next = Next1}}.

info(StreamId, Response, State = #state{next = Next, req = Req}) ->
    Response1 = case Response of
        {response, 404, Headers0, <<>>} ->
            %% TODO: should we log the actual response?
            log_response(Response, Req),
            Json = rabbit_json:encode(#{
                error  => list_to_binary(httpd_util:reason_phrase(404)),
                reason => <<"Not Found">>}),
            Headers1 = maps:put(<<"content-length">>, integer_to_list(iolist_size(Json)), Headers0),
            Headers = maps:put(<<"content-type">>, <<"application/json">>, Headers1),
            {response, 404, Headers, Json};
        {response, _, _, _} ->
            log_response(Response, Req),
            Response;
        {headers, _, _} ->
            log_stream_response(Response, Req),
            Response;
        _ ->
            Response
    end,
    {Commands, Next1} = cowboy_stream:info(StreamId, Response1, Next),
    {Commands, State#state{next = Next1}}.

terminate(StreamId, Reason, #state{next = Next}) ->
    cowboy_stream:terminate(StreamId, Reason, Next).

early_error(StreamId, Reason, PartialReq, Resp, Opts) ->
    cowboy_stream:early_error(StreamId, Reason, PartialReq, Resp, Opts).

log_response({response, Status, _Headers, Body}, Req) ->
    webmachine_log:log_access({Status, Body, Req}).

log_stream_response({headers, Status, _Headers}, Req) ->
    webmachine_log:log_access({Status, <<>>, Req}).
