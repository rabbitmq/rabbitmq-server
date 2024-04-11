%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% The purpose of this module is to select a sub-protocol
%% and switch to the handler for the selected sub-protocol.
%% This module makes it possible for a single Websocket
%% endpoint to be shared between multiple plugins, for
%% example Web-MQTT and Web-STOMP.

-module(rabbit_web_ws_h).
-behaviour(cowboy_handler).
-behaviour(cowboy_sub_protocol).

%% HTTP handler.
-export([init/2]).

%% Cowboy sub-protocol.
-export([upgrade/4]).
-export([upgrade/5]).
-export([takeover/7]).

-include_lib("kernel/include/logger.hrl").

%% HTTP handler.

init(Req = #{ref := Ref}, _) ->
    case cowboy_req:parse_header(<<"sec-websocket-protocol">>, Req) of
        undefined ->
            no_supported_sub_protocol(undefined, Req);
        ReqProtocols ->
            Config = rabbit_web:get_websocket_config_for_ref(Ref),
            ?LOG_ERROR("Config: ~tp", [Config]),
            case select_sub_protocol(ReqProtocols, Config) of
                false ->
                    no_supported_sub_protocol(ReqProtocols, Req);
                {Proto, Handler, WsOpts} ->
                    %% We call the handler's init function to initialise
                    %% its state. The module returned MUST be this module.
                    %% @todo What do we give as initial state?
                    {?MODULE, Req1, HandlerState} = Handler:init(Req, []),
                    %% We must use the Cowboy protocol interface to
                    %% switch to Websocket using a new handler module.
                    Req2 = cowboy_req:set_resp_header(<<"sec-websocket-protocol">>, Proto, Req1),
                    {?MODULE, Req2, {Handler, HandlerState}, WsOpts}
             end
    end.

select_sub_protocol([], _) ->
    false;
select_sub_protocol([Proto|Tail], Config) ->
    case select_sub_protocol1(Proto, Config) of
        false ->
            select_sub_protocol(Tail, Config);
        Selected ->
            Selected
    end.

select_sub_protocol1(_, []) ->
    false;
select_sub_protocol1(Proto, [WsConfig|Tail]) ->
    #{
        protocols := Protocols,
        handler := Handler,
        opts := WsOpts
    } = WsConfig,
    case lists:member(Proto, Protocols) of
        true ->
            {Proto, Handler, WsOpts};
        false ->
            select_sub_protocol1(Proto, Tail)
    end.

no_supported_sub_protocol(ReqProtocols, Req) ->
    %% @todo Figure out what to say in the error message.
    %% @todo Probably pass an option with a listener name as string or something.
    ?LOG_ERROR("Web: ~tp", [ReqProtocols]),
    %% The client MUST include “mqtt” in the list of WebSocket Sub Protocols it offers [MQTT-6.0.0-3].
%    ?LOG_ERROR("Web MQTT: 'mqtt' not included in client offered subprotocols: ~tp", [Protocol]),
    {ok,
     %% @todo Connection: close is invalid for HTTP/2. Just don't close?
     cowboy_req:reply(400, #{<<"connection">> => <<"close">>}, Req),
     undefined}.

%% Cowboy sub-protocol.

%% cowboy_sub_protcol
upgrade(Req, Env, Handler, HandlerState) ->
    upgrade(Req, Env, Handler, HandlerState, #{}).

%% We set the Handler to switch to in our HandlerState.
%% Now is the time to switch handlers.
upgrade(Req, Env, ?MODULE, {Handler, HandlerState}, Opts) ->
    cowboy_websocket:upgrade(Req, Env, Handler, HandlerState, Opts).

takeover(Parent, Ref, Socket, Transport, Opts, Buffer, HandlerInfo) ->
    cowboy_websocket:takeover(Parent, Ref, Socket, Transport, Opts, Buffer, HandlerInfo).
