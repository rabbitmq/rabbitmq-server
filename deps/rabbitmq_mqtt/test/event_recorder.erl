%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(event_recorder).
-behaviour(gen_event).
-export([init/1, handle_event/2, handle_call/2]).
-define(INIT_STATE, []).

-include_lib("rabbit_common/include/rabbit.hrl").

init(_) ->
    {ok, ?INIT_STATE}.

handle_event(#event{type = T}, State)
  when T =:= node_stats orelse
       T =:= node_node_stats orelse
       T =:= node_node_deleted ->
    {ok, State};
handle_event(Event, State) ->
    {ok, [Event | State]}.

handle_call(take_state, State) ->
    {ok, lists:reverse(State), ?INIT_STATE}.
