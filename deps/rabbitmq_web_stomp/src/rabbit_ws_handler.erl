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
%% Copyright (c) 2007-2015 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_ws_handler).
-behaviour(cowboy_websocket_handler).

%% Websocket.
-export([init/3]).
-export([websocket_init/3]).
-export([websocket_handle/3]).
-export([websocket_info/3]).
-export([websocket_terminate/3]).

%% SockJS interface
-export([info/1]).
-export([send/2]).
-export([close/3]).

-record(state, {pid, type}).

%% Websocket.

init(_, _Req, _Opts) ->
    {upgrade, protocol, cowboy_websocket}.

websocket_init(_TransportName, Req, [{type, FrameType}]) ->
    {Peername, _} = cowboy_req:peer(Req),
    [Socket, Transport] = cowboy_req:get([socket, transport], Req),
    {ok, Sockname} = Transport:sockname(Socket),
    Conn = {?MODULE, self(), [
        {peername, Peername},
        {sockname, Sockname}]},
    {ok, _Sup, Pid} = rabbit_ws_sup:start_client({Conn}),
    {ok, Req, #state{pid=Pid, type=FrameType}}.

websocket_handle({text, Data}, Req, State=#state{pid=Pid}) ->
    rabbit_ws_client:sockjs_msg(Pid, Data),
    {ok, Req, State};
websocket_handle({binary, Data}, Req, State=#state{pid=Pid}) ->
    rabbit_ws_client:sockjs_msg(Pid, Data),
    {ok, Req, State};
websocket_handle(_Frame, Req, State) ->
    {ok, Req, State}.

websocket_info({send, Msg}, Req, State=#state{type=FrameType}) ->
    {reply, {FrameType, Msg}, Req, State};
websocket_info(Frame = {close, _, _}, Req, State) ->
    {reply, Frame, Req, State};
websocket_info(_Info, Req, State) ->
    {ok, Req, State}.

websocket_terminate(_Reason, _Req, #state{pid=Pid}) ->
    rabbit_ws_client:sockjs_closed(Pid),
    ok.

%% SockJS connection handling.

%% The following functions are replicating the functionality
%% found in sockjs_session. I am not too happy about using
%% a tuple-call, but at the time of writing this code it is
%% necessary in order to share the existing code with SockJS.
%%
%% Ideally all the STOMP interaction should be done from
%% within the Websocket process. This could be a good refactoring
%% once SockJS gets removed.

info({?MODULE, _, Info}) ->
    Info.

send(Data, {?MODULE, Pid, _}) ->
    Pid ! {send, Data},
    ok.

close(Code, Reason, {?MODULE, Pid, _}) ->
    Pid ! {close, Code, Reason},
    ok.
