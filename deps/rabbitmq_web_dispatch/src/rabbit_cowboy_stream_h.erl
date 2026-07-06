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
-export([set_authenticated_username/2]).
<<<<<<< HEAD
=======
-export([unset_authenticated_username/1]).
-export([get_authenticated_username/1]).
>>>>>>> 1796abb3b7 (Log username determined by the auth-backend)

-record(state, {
    next :: any(),
    req :: cowboy_req:req()
}).

-define(AUTH_USER_HEADER, <<"x-rabbitmq-authenticated-user">>).

init(StreamId, Req, Opts) ->
    {Commands, Next} = cowboy_stream:init(StreamId, Req, Opts),
    {Commands, #state{next = Next, req = Req}}.

data(StreamId, IsFin, Data, State = #state{next = Next}) ->
    {Commands, Next1} = cowboy_stream:data(StreamId, IsFin, Data, Next),
    {Commands, State#state{next = Next1}}.

info(StreamId, Response, State = #state{next = Next, req = Req}) ->
    Response1 = case Response of
        {response, 404, Headers0, <<>>} ->
<<<<<<< HEAD
            log_response(Response, Req),
=======
            log_response({response, 404, Headers0, <<>>}, Req),
>>>>>>> 1796abb3b7 (Log username determined by the auth-backend)
            H1 = unset_authenticated_username(Headers0),
            Json = rabbit_json:encode(#{
                error  => list_to_binary(httpd_util:reason_phrase(404)),
                reason => <<"Not Found">>}),
            H2 = maps:put(<<"content-length">>, integer_to_list(iolist_size(Json)), H1),
            H3 = maps:put(<<"content-type">>, <<"application/json">>, H2),
            {response, 404, H3, Json};
        {response, Status, Headers0, Body} ->
<<<<<<< HEAD
            log_response(Response, Req),
            H1 = unset_authenticated_username(Headers0),
            {response, Status, H1, Body};
        {headers, Status, Headers0} ->
            log_stream_response(Response, Req),
=======
            log_response({response, Status, Headers0, Body}, Req),
            H1 = unset_authenticated_username(Headers0),
            {response, Status, H1, Body};
        {headers, Status, Headers0} ->
            log_stream_response({headers, Status, Headers0}, Req),
>>>>>>> 1796abb3b7 (Log username determined by the auth-backend)
            H1 = unset_authenticated_username(Headers0),
            {headers, Status, H1};
        _ ->
            Response
    end,
    {Commands, Next1} = cowboy_stream:info(StreamId, Response1, Next),
    {Commands, State#state{next = Next1}}.

terminate(StreamId, Reason, #state{next = Next}) ->
    cowboy_stream:terminate(StreamId, Reason, Next).

early_error(StreamId, Reason, PartialReq, Resp, Opts) ->
    cowboy_stream:early_error(StreamId, Reason, PartialReq, Resp, Opts).

set_authenticated_username(Username, Req) ->
<<<<<<< HEAD
    cowboy_req:set_resp_header(?AUTH_USER_HEADER,
                               rabbit_data_coercion:to_binary(Username), Req).
=======
    cowboy_req:set_resp_header(?AUTH_USER_HEADER, Username, Req).
>>>>>>> 1796abb3b7 (Log username determined by the auth-backend)

unset_authenticated_username(Headers) ->
    maps:remove(?AUTH_USER_HEADER, Headers).

get_authenticated_username(Headers) ->
    maps:get(?AUTH_USER_HEADER, Headers, <<"-">>).

log_response({response, Status, Headers, Body}, Req) ->
    Username = get_authenticated_username(Headers),
    logger:log(info, #{formatted => format_access_log(Status, Body, Username, Req)},
               #{domain => ?RMQLOG_DOMAIN_HTTP_ACCESS}).

log_stream_response({headers, Status, Headers}, Req) ->
    Username = get_authenticated_username(Headers),
    logger:log(info, #{formatted => format_access_log(Status, <<>>, Username, Req)},
               #{domain => ?RMQLOG_DOMAIN_HTTP_ACCESS}).

format_access_log(Status, Body, Username, Req) ->
    User = sanitize(Username),
    Time = format_time(),
    StatusStr = integer_to_list(Status),
    Length = integer_to_list(body_length(Body)),
    Method = escape_for_quoted_log(cowboy_req:method(Req)),
    Path = escape_for_quoted_log(cowboy_req:path(Req)),
    {Peer, _Port} = cowboy_req:peer(Req),
    Version = cowboy_req:version(Req),
    Referer = escape_for_quoted_log(
                cowboy_req:header(<<"referer">>, Req, <<>>)),
    UserAgent = escape_for_quoted_log(
                  cowboy_req:header(<<"user-agent">>, Req, <<>>)),
    [fmt_ip(Peer), " - ", User, " ", Time, " \"", Method, " ", Path,
     " ", atom_to_list(Version), "\" ",
     StatusStr, " ", Length, " \"", Referer,
     "\" \"", UserAgent, "\""].

body_length({sendfile, _, Length, _}) -> Length;
body_length(Body) -> iolist_size(Body).

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

sanitize(Value) ->
    Bin0 = rabbit_data_coercion:to_binary(Value),
    Bin1 = binary:replace(Bin0, [<<"\r">>, <<"\n">>], <<>>, [global]),
    << <<(case C < 32 orelse C =:= 127 of
              true  -> $?;
              false -> C
          end)>> || <<C>> <= Bin1 >>.

escape_for_quoted_log(Value) ->
    Bin = sanitize(Value),
    Bin1 = binary:replace(Bin, <<"\\">>, <<"\\\\">>, [global]),
    binary:replace(Bin1, <<"\"">>, <<"\\\"">>, [global]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

sanitize_test() ->
    ?assertEqual(<<"hello">>, sanitize(<<"hello">>)),
    ?assertEqual(<<"hello">>, sanitize("hello")),
    %% CR/LF are removed (to keep log lines intact)
    ?assertEqual(<<"helloworld">>, sanitize(<<"hello\nworld">>)),
    ?assertEqual(<<"helloworld">>, sanitize(<<"hello\r\nworld">>)),
    %% Other control characters are replaced with ?
    ?assertEqual(<<"hello?world">>, sanitize(<<"hello\tworld">>)),
    ?assertEqual(<<"hello?world?">>, sanitize(<<"hello\x00world\x1f">>)),
    ?assertEqual(<<"abc">>, sanitize(<<"a\rb\nc">>)),
    ok.

escape_for_quoted_log_test() ->
    ?assertEqual(<<"hello">>, escape_for_quoted_log(<<"hello">>)),
    %% A literal backslash in the input gets escaped to \\
    ?assertEqual(<<"hello\\\\nworld">>, escape_for_quoted_log(<<"hello\\nworld">>)),
    %% Double quotes are escaped
    ?assertEqual(<<"say \\\"hi\\\"">>, escape_for_quoted_log(<<"say \"hi\"">>)),
    %% Combined: controls removed + quotes escaped
    ?assertEqual(<<"pathwith\\\"quote\\\"">>, escape_for_quoted_log(<<"path\nwith\"quote\"">>)),
    ok.

-endif.
