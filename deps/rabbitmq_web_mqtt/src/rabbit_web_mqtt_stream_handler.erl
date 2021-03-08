%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(rabbit_web_mqtt_stream_handler).

-behavior(cowboy_stream).

-export([init/3]).
-export([data/4]).
-export([info/3]).
-export([terminate/3]).
-export([early_error/5]).


-record(state, {next}).

init(StreamID, Req, Opts) ->
    {Commands, Next} = cowboy_stream:init(StreamID, Req, Opts),
    {Commands, #state{next = Next}}.

data(StreamID, IsFin, Data, State = #state{next = Next0}) ->
    {Commands, Next} = cowboy_stream:data(StreamID, IsFin, Data, Next0),
    {Commands, State#state{next = Next}}.

info(StreamID, {switch_protocol, Headers, _, InitialState}, State) ->
    do_info(StreamID, {switch_protocol, Headers, rabbit_web_mqtt_handler, InitialState}, State);
info(StreamID, Info, State) ->
    do_info(StreamID, Info, State).

do_info(StreamID, Info, State = #state{next = Next0}) ->
    {Commands, Next} = cowboy_stream:info(StreamID, Info, Next0),
    {Commands, State#state{next = Next}}.

terminate(StreamID, Reason, State = #state{next = Next}) ->
    cowboy_stream:terminate(StreamID, Reason, Next).

early_error(StreamID, Reason, PartialReq, Resp, Opts) ->
    cowboy_stream:early_error(StreamID, Reason, PartialReq, Resp, Opts).
