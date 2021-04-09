%% Copyright (c) 2011-2013 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc Default log handler for webmachine

-module(webmachine_log_handler).

-behaviour(gen_event).

-export([format_req/1,
         user_from_req/1]).

%% gen_event callbacks
-export([init/1,
         handle_call/2,
         handle_event/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("webmachine_logger.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(state, {hourstamp, filename, handle}).

-define(FILENAME, "access.log").

%% ===================================================================
%% gen_event callbacks
%% ===================================================================

%% @private
init([BaseDir]) ->
    webmachine_log:defer_refresh(?MODULE),
    FileName = filename:join(BaseDir, ?FILENAME),
    {Handle, DateHour} = webmachine_log:log_open(FileName),
    {ok, #state{filename=FileName, handle=Handle, hourstamp=DateHour}}.

%% @private
handle_call({_Label, MRef, get_modules}, State) ->
    {ok, {MRef, [?MODULE]}, State};
handle_call({refresh, Time}, State) ->
    {ok, ok, webmachine_log:maybe_rotate(?MODULE, Time, State)};
handle_call(_Request, State) ->
    {ok, ok, State}.

%% @private
handle_event({log_access, LogData}, State) ->
    NewState = webmachine_log:maybe_rotate(?MODULE, os:timestamp(), State),
    Msg = format_req(LogData),
    webmachine_log:log_write(NewState#state.handle, [Msg, $\n]),
    {ok, NewState};
handle_event(_Event, State) ->
    {ok, State}.

%% @private
handle_info(_Info, State) ->
    {ok, State}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ===================================================================
%% Internal functions
%% ===================================================================

%% We currently keep most of the Webmachine logging facility. But
%% since we are now using Cowboy, a few small parts had to change.
%% This is one such part. The code is however equivalent to Webmachine's.

format_req({Status0, Body, Req}) ->
    User = user_from_req(Req),
    Time = webmachine_log:fmtnow(),
    Status = integer_to_list(Status0),
    Length1 = case Body of
        {sendfile, _, Length0, _} -> Length0;
        _ -> iolist_size(Body)
    end,
    Length = integer_to_list(Length1),
    Method = cowboy_req:method(Req),
    Path = cowboy_req:path(Req),
    Peer = case cowboy_req:peer(Req) of
                       {Peer0, _Port} -> Peer0;
                       Other -> Other
                   end,
    Version = cowboy_req:version(Req),
    Referer = cowboy_req:header(<<"referer">>, Req, <<>>),
    UserAgent = cowboy_req:header(<<"user-agent">>, Req, <<>>),
    fmt_alog(Time, Peer, User, Method, Path, Version,
             Status, Length, Referer, UserAgent).

fmt_alog(Time, Ip, User, Method, Path, Version,
         Status,  Length, Referrer, UserAgent) ->
    [webmachine_log:fmt_ip(Ip), " - ", User, [$\s], Time, [$\s, $"], Method, " ", Path,
     " ", atom_to_list(Version), [$",$\s],
     Status, [$\s], Length, [$\s,$"], Referrer,
     [$",$\s,$"], UserAgent, $"].

user_from_req(Req) ->
    try cowboy_req:parse_header(<<"authorization">>, Req) of
        {basic, Username, _} ->
            Username;
        {bearer, _} ->
            rabbit_data_coercion:to_binary(
              application:get_env(rabbitmq_management, uaa_client_id, ""));
        _ ->
            "-"
    catch _:_ ->
        "-"
    end.
