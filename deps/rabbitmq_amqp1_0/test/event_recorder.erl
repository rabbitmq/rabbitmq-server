%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(event_recorder).
-behaviour(gen_event).

-include_lib("stdlib/include/assert.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%% gen_event callbacks
-export([init/1,
         handle_event/2,
         handle_call/2]).
%% client API
-export([start/1,
         stop/1,
         get_events/1]).
-export([assert_event_type/2,
         assert_event_prop/2]).

-import(rabbit_ct_broker_helpers,
        [get_node_config/3]).

-define(INIT_STATE, []).

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

start(Config) ->
    ok = rabbit_ct_broker_helpers:add_code_path_to_all_nodes(Config, ?MODULE),
    ok = gen_event:add_handler(event_manager_ref(Config), ?MODULE, []).

stop(Config) ->
    ok = gen_event:delete_handler(event_manager_ref(Config), ?MODULE, []).

get_events(Config) ->
    %% events are sent and processed asynchronously
    timer:sleep(500),
    Result = gen_event:call(event_manager_ref(Config), ?MODULE, take_state),
    ?assert(is_list(Result)),
    Result.

event_manager_ref(Config) ->
    Node = get_node_config(Config, 0, nodename),
    {rabbit_event, Node}.

assert_event_type(ExpectedType, #event{type = ActualType}) ->
    ?assertEqual(ExpectedType, ActualType).

assert_event_prop(ExpectedProp = {Key, _Value}, #event{props = Props}) ->
    ?assertEqual(ExpectedProp, lists:keyfind(Key, 1, Props));
assert_event_prop(ExpectedProps, Event)
  when is_list(ExpectedProps) ->
    lists:foreach(fun(P) ->
                          assert_event_prop(P, Event)
                  end, ExpectedProps).
