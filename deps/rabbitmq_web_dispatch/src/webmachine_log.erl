%% Copyright (c) 2011-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   https://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.

%% @doc Helper functions for webmachine's default log handlers

-module(webmachine_log).

-include_lib("kernel/include/logger.hrl").
-include("webmachine_logger.hrl").

-export([add_handler/2,
         call/2,
         call/3,
         datehour/0,
         datehour/1,
         defer_refresh/1,
         delete_handler/1,
         fix_log/2,
         fmt_ip/1,
         fmtnow/0,
         log_access/1,
         log_close/3,
         log_open/1,
         log_open/2,
         log_write/2,
         maybe_rotate/3,
         month/1,
         refresh/2,
         suffix/1,
         zeropad/2,
         zone/0]).

-record(state, {hourstamp :: non_neg_integer(),
                filename :: string(),
                handle :: file:io_device()}).

%% @doc Add a handler to receive log events
-type add_handler_result() :: ok | {'EXIT', term()} | term().
-spec add_handler(atom() | {atom(), term()}, term()) -> add_handler_result().
add_handler(Mod, Args) ->
    gen_event:add_handler(?EVENT_LOGGER, Mod, Args).

%% @doc Make a synchronous call directly to a specific event handler
%% module
-type error() :: {error, bad_module} | {'EXIT', term()} | term().
-spec call(atom(), term()) -> term() | error().
call(Mod, Msg) ->
    gen_event:call(?EVENT_LOGGER, Mod, Msg).

%% @doc Make a synchronous call directly to a specific event handler
%% module
-spec call(atom(), term(), timeout()) -> term() | error().
call(Mod, Msg, Timeout) ->
    gen_event:call(?EVENT_LOGGER, Mod, Msg, Timeout).

%% @doc Return a four-tuple containing year, month, day, and hour
%% of the current time.
-type datehour() :: {calendar:year(), calendar:month(), calendar:day(), calendar:hour()}.
-spec datehour() -> datehour().
datehour() ->
    datehour(os:timestamp()).

%% @doc Return a four-tuple containing year, month, day, and hour
%% of the specified time.
-spec datehour(erlang:timestamp()) -> datehour().
datehour(TS) ->
    {{Y, M, D}, {H, _, _}} = calendar:now_to_universal_time(TS),
    {Y, M, D, H}.

%% @doc Defer the refresh of a log file.
-spec defer_refresh(atom()) -> {ok, timer:tref()} | {error, term()}.
defer_refresh(Mod) ->
    {_, {_, M, S}} = calendar:universal_time(),
    Time = 1000 * (3600 - ((M * 60) + S)),
    timer:apply_after(Time, ?MODULE, refresh, [Mod, os:timestamp()]).

%% @doc Remove a log handler
-type delete_handler_result() :: term() | {error, module_not_found} | {'EXIT', term()}.
-spec delete_handler(atom() | {atom(), term()}) -> delete_handler_result().
delete_handler(Mod) ->
    gen_event:delete_handler(?EVENT_LOGGER, Mod, []).

%% Seek backwards to the last valid log entry
-spec fix_log(file:io_device(), non_neg_integer()) -> ok.
fix_log(_FD, 0) ->
    ok;
fix_log(FD, 1) ->
    {ok, 0} = file:position(FD, 0),
    ok;
fix_log(FD, Location) ->
    case file:pread(FD, Location - 1, 1) of
        {ok, [$\n | _]} ->
            ok;
        {ok, _} ->
            fix_log(FD, Location - 1)
    end.

%% @doc Format an IP address or host name
-spec fmt_ip(undefined | string() | inet:ip4_address() | inet:ip6_address()) -> string().
fmt_ip(IP) when is_tuple(IP) ->
    inet_parse:ntoa(IP);
fmt_ip(undefined) ->
    "0.0.0.0";
fmt_ip(HostName) ->
    HostName.

%% @doc Format the current time into a string
-spec fmtnow() -> string().
fmtnow() ->
    {{Year, Month, Date}, {Hour, Min, Sec}} = calendar:local_time(),
    io_lib:format("[~2..0w/~s/~4..0w:~2..0w:~2..0w:~2..0w ~s]",
                  [Date,month(Month),Year, Hour, Min, Sec, zone()]).

%% @doc Notify registered log event handler of an access event.
-spec log_access(tuple()) -> ok.
log_access({_, _, _}=LogData) ->
    log_to_erlang_logger(LogData),
    gen_event:sync_notify(?EVENT_LOGGER, {log_access, LogData}).

%% @doc Close a log file.
-spec log_close(atom(), string(), file:io_device()) -> ok | {error, term()}.
log_close(Mod, Name, FD) ->
    logger:info("~p: closing log file: ~p", [Mod, Name]),
    file:close(FD).

%% @doc Open a new log file for writing
-spec log_open(string()) -> {file:io_device(), non_neg_integer()}.
log_open(FileName) ->
    DateHour = datehour(),
    {log_open(FileName, DateHour), DateHour}.

%% @doc Open a new log file for writing
-spec log_open(string(), non_neg_integer()) -> file:io_device().
log_open(FileName, DateHour) ->
    LogName = FileName ++ suffix(DateHour),
    logger:info("opening log file: ~p", [LogName]),
    filelib:ensure_dir(LogName),
    {ok, FD} = file:open(LogName, [read, write, raw]),
    {ok, Location} = file:position(FD, eof),
    fix_log(FD, Location),
    file:truncate(FD),
    FD.

-spec log_write(file:io_device(), iolist()) -> ok | {error, term()}.
log_write(FD, IoData) ->
    file:write(FD, lists:flatten(IoData)).

%% @doc Rotate a log file if the hour it represents
%% has passed.
-spec maybe_rotate(atom(), erlang:timestamp(), #state{}) -> #state{}.
maybe_rotate(Mod, Time, State) ->
    ThisHour = datehour(Time),
    if ThisHour == State#state.hourstamp ->
            State;
       true ->
            defer_refresh(Mod),
            log_close(Mod, State#state.filename, State#state.handle),
            Handle = log_open(State#state.filename, ThisHour),
            State#state{hourstamp=ThisHour, handle=Handle}
    end.

%% @doc Convert numeric month value to the abbreviation
-spec month(1..12) -> string().
month(1) ->
    "Jan";
month(2) ->
    "Feb";
month(3) ->
    "Mar";
month(4) ->
    "Apr";
month(5) ->
    "May";
month(6) ->
    "Jun";
month(7) ->
    "Jul";
month(8) ->
    "Aug";
month(9) ->
    "Sep";
month(10) ->
    "Oct";
month(11) ->
    "Nov";
month(12) ->
    "Dec".

%% @doc Make a synchronous call to instruct a log handler to refresh
%% itself.
-spec refresh(atom(), erlang:timestamp()) -> ok | {error, term()}.
refresh(Mod, Time) ->
    call(Mod, {refresh, Time}, infinity).

-spec suffix(datehour()) -> string().
suffix({Y, M, D, H}) ->
    YS = zeropad(Y, 4),
    MS = zeropad(M, 2),
    DS = zeropad(D, 2),
    HS = zeropad(H, 2),
    lists:flatten([$., YS, $_, MS, $_, DS, $_, HS]).

-spec zeropad(integer(), integer()) -> string().
zeropad(Num, MinLength) ->
    NumStr = integer_to_list(Num),
    zeropad_str(NumStr, MinLength - length(NumStr)).

-spec zeropad_str(string(), integer()) -> string().
zeropad_str(NumStr, Zeros) when Zeros > 0 ->
    zeropad_str([$0 | NumStr], Zeros - 1);
zeropad_str(NumStr, _) ->
    NumStr.

-spec zone() -> string().
zone() ->
    Time = erlang:universaltime(),
    LocalTime = calendar:universal_time_to_local_time(Time),
    DiffSecs = calendar:datetime_to_gregorian_seconds(LocalTime) -
        calendar:datetime_to_gregorian_seconds(Time),
    zone((DiffSecs/3600)*100).

%% Ugly reformatting code to get times like +0000 and -1300

-spec zone(integer()) -> string().
zone(Val) when Val < 0 ->
    io_lib:format("-~4..0w", [trunc(abs(Val))]);
zone(Val) when Val >= 0 ->
    io_lib:format("+~4..0w", [trunc(abs(Val))]).

log_to_erlang_logger({Status, Body, Req} = LogData) ->
    User = webmachine_log_handler:user_from_req(Req),

    %% From the request.
    Method = cowboy_req:method(Req),
    Path = cowboy_req:path(Req),
    Version = cowboy_req:version(Req),
    Peer = case cowboy_req:peer(Req) of
                       {Peer0, _Port} -> Peer0;
                       Other          -> Other
                   end,
    Referer = cowboy_req:header(<<"referer">>, Req, <<>>),
    UserAgent = cowboy_req:header(<<"user-agent">>, Req, <<>>),

    %% From the response.
    Length = case Body of
                 {sendfile, _, L, _} -> L;
                 _                   -> iolist_size(Body)
             end,

    FormatStr = "~s",
    Msg = webmachine_log_handler:format_req(LogData),
    Meta = #{domain => ?RMQLOG_DOMAIN_HTTP_ACCESS_LOG,
             http_status => Status,
             http_user => User,
             http_req_method => Method,
             http_req_path => Path,
             http_req_version => Version,
             http_req_peer => fmt_ip(Peer),
             http_req_referer => Referer,
             http_req_user_agent => UserAgent,
             http_resp_content_length => Length},
    case is_http_error(Status) of
        false -> ?LOG_INFO(FormatStr, [Msg], Meta);
        true  -> ?LOG_ERROR(FormatStr, [Msg], Meta)
    end.

is_http_error(Status) when Status >= 400 andalso Status < 600 ->
    true;
is_http_error(_) ->
    false.
