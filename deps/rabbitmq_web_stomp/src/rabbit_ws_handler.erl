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
-behaviour(cowboy_websocket).

%% Websocket.
-export([init/2]).
-export([websocket_init/1]).
-export([websocket_handle/2]).
-export([websocket_info/2]).
-export([terminate/3]).

%% SockJS interface
-export([info/1]).
-export([send/2]).
-export([close/3]).

-record(state, {conn, pid, type}).

%% Websocket.
init(Req0, Opts) ->
    Req = case cowboy_req:header(<<"sec-websocket-protocol">>, Req0) of
        undefined  -> Req0;
        ProtocolHd ->
            Protocols = parse_sec_websocket_protocol_req(ProtocolHd),
            case filter_stomp_protocols(Protocols) of
                [] -> Req0;
                [StompProtocol|_] ->
                    cowboy_req:set_resp_header(<<"sec-websocket-protocol">>,
                        StompProtocol, Req0)
            end
    end,
    {_, FrameType} = lists:keyfind(type, 1, Opts),
    Socket = maps:get(socket, Req0),
    Peername = cowboy_req:peer(Req),
    {ok, Sockname} = rabbit_net:sockname(Socket),
    Headers = case cowboy_req:header(<<"authorization">>, Req) of
        undefined -> [];
        AuthHd    -> [{authorization, binary_to_list(AuthHd)}]
    end,
    {cowboy_websocket, Req, {Socket, Peername, Sockname, Headers, FrameType}}.

websocket_init({Socket, Peername, Sockname, Headers, FrameType}) ->
    Conn = {?MODULE, self(), [
        {socket, Socket},
        {peername, Peername},
        {sockname, Sockname},
        {headers, Headers}]},
    {ok, _Sup, Pid} = rabbit_ws_sup:start_client({Conn, heartbeat}),
    {ok, #state{pid=Pid, type=FrameType}}.

websocket_handle({text, Data}, State=#state{pid=Pid}) ->
    rabbit_ws_client:msg(Pid, Data),
    {ok, State};
websocket_handle({binary, Data}, State=#state{pid=Pid}) ->
    rabbit_ws_client:msg(Pid, Data),
    {ok, State};
websocket_handle(_Frame, State) ->
    {ok, State}.

websocket_info({send, Msg}, State=#state{type=FrameType}) ->
    {reply, {FrameType, Msg}, State};
websocket_info(Frame = {close, _, _}, State) ->
    {reply, Frame, State};
websocket_info(_Info, State) ->
    {ok, State}.

terminate(_Reason, _Req, {_, _, _, _, _}) ->
    ok;
terminate(_Reason, _Req, #state{pid=Pid}) ->
    rabbit_ws_client:closed(Pid),
    ok.

%% When moving to Cowboy 2, this code should be replaced
%% with a simple call to cow_http_hd:parse_sec_websocket_protocol_req/1.

parse_sec_websocket_protocol_req(Bin) ->
    Protocols = binary:split(Bin, [<<$,>>, <<$\s>>], [global]),
    [P || P <- Protocols, P =/= <<>>].

%% The protocols v10.stomp, v11.stomp and v12.stomp are registered
%% at IANA: https://www.iana.org/assignments/websocket/websocket.xhtml

filter_stomp_protocols(Protocols) ->
    lists:reverse(lists:sort(lists:filter(
        fun(<< "v1", C, ".stomp">>)
            when C =:= $2; C =:= $1; C =:= $0 -> true;
           (_) ->
            false
        end,
        Protocols))).

%% TODO
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
