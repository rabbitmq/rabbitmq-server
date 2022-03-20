%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
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
