%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_cowboy_stream_h).
-behavior(cowboy_stream).

-include_lib("rabbit_common/include/logging.hrl").

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
    logger:log(info, #{formatted => format_access_log(Status, Body, Req)},
               #{domain => ?RMQLOG_DOMAIN_HTTP_ACCESS}).

log_stream_response({headers, Status, _Headers}, Req) ->
    logger:log(info, #{formatted => format_access_log(Status, <<>>, Req)},
               #{domain => ?RMQLOG_DOMAIN_HTTP_ACCESS}).

format_access_log(Status, Body, Req) ->
    User = user_from_req(Req),
    Time = format_time(),
    StatusStr = integer_to_list(Status),
    Length = integer_to_list(body_length(Body)),
    Method = cowboy_req:method(Req),
    Path = cowboy_req:path(Req),
    {Peer, _Port} = cowboy_req:peer(Req),
    Version = cowboy_req:version(Req),
    Referer = cowboy_req:header(<<"referer">>, Req, <<>>),
    UserAgent = cowboy_req:header(<<"user-agent">>, Req, <<>>),
    [fmt_ip(Peer), " - ", User, " ", Time, " \"", Method, " ", Path,
     " ", atom_to_list(Version), "\" ",
     StatusStr, " ", Length, " \"", Referer,
     "\" \"", UserAgent, "\""].

body_length({sendfile, _, Length, _}) -> Length;
body_length(Body) -> iolist_size(Body).

user_from_req(Req) ->
    try cowboy_req:parse_header(<<"authorization">>, Req) of
        {basic, Username, _} ->
            Username;
        {bearer, _} ->
            rabbit_data_coercion:to_binary(
              application:get_env(rabbitmq_management, oauth_client_id, ""));
        _ ->
            "-"
    catch _:_ ->
        "-"
    end.

fmt_ip(IP) when is_tuple(IP) ->
    inet_parse:ntoa(IP).

format_time() ->
    {{Year, Month, Date}, {Hour, Min, Sec}} = calendar:local_time(),
    io_lib:format("[~2..0w/~ts/~4..0w:~2..0w:~2..0w:~2..0w ~ts]",
                  [Date, month(Month), Year, Hour, Min, Sec, zone()]).

month(1) -> "Jan";
month(2) -> "Feb";
month(3) -> "Mar";
month(4) -> "Apr";
month(5) -> "May";
month(6) -> "Jun";
month(7) -> "Jul";
month(8) -> "Aug";
month(9) -> "Sep";
month(10) -> "Oct";
month(11) -> "Nov";
month(12) -> "Dec".

zone() ->
    Time = erlang:universaltime(),
    LocalTime = calendar:universal_time_to_local_time(Time),
    DiffSecs = calendar:datetime_to_gregorian_seconds(LocalTime) -
        calendar:datetime_to_gregorian_seconds(Time),
    Hours = DiffSecs div 3600,
    Minutes = abs(DiffSecs rem 3600) div 60,
    zone(Hours * 100 + sign(DiffSecs) * Minutes).

sign(V) when V < 0 -> -1;
sign(_) -> 1.

zone(Val) when Val < 0 ->
    io_lib:format("-~4..0w", [abs(Val)]);
zone(Val) when Val >= 0 ->
    io_lib:format("+~4..0w", [Val]).
