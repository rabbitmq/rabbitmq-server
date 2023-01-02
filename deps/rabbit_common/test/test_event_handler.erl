%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(test_event_handler).

-behaviour(gen_event).

-export([get_events/0, reset/0]).

-export([init/1, handle_call/2, handle_event/2, handle_info/2,
         terminate/2, code_change/3]).

-include("rabbit.hrl").


%%
%% API
%%
get_events() ->
    gen_event:call(rabbit_event, ?MODULE, get_events).

reset() ->
    gen_event:call(rabbit_event, ?MODULE, reset).

%%
%% Callbacks
%%
init([]) ->
    {ok, []}.

handle_event(Event = #event{type = supervisor2_error_report}, State) ->
    {ok, [Event | State]};
handle_event(_Event, State) ->
    {ok, State}.

handle_call(get_events, State) ->
    {ok, {ok, State}, State};
handle_call(reset, _State) ->
    {ok, ok, []}.

handle_info(_Info, State) ->
    {ok, State}.

terminate(_Arg, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
