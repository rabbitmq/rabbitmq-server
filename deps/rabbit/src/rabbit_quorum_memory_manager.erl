%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(rabbit_quorum_memory_manager).

-behaviour(gen_event).

-export([register/1,
         unregister/1]).

-export([init/1,
         handle_event/2,
         handle_call/2,
         handle_info/2,
         terminate/2]).

-record(state, {queue_mod :: module(),
                last_roll_over :: never | integer(),
                interval :: pos_integer()}).

register(QueueMod) ->
    gen_event:add_handler(rabbit_alarm, {?MODULE, QueueMod}, [QueueMod]).

unregister(QueueMod) ->
    gen_event:delete_handler(rabbit_alarm, {?MODULE, QueueMod}, [QueueMod]).

init([QueueMod]) ->
    Interval = application:get_env(rabbit, min_wal_roll_over_interval, 20_000),
    {ok, #state{queue_mod = QueueMod,
                last_roll_over = never,
                interval = Interval}}.

handle_event({set_alarm, {{resource_limit, memory, Node}, []}},
             #state{last_roll_over = Last,
                    interval = Interval} = State)
  when Node =:= node() ->
    case Last =:= never orelse
         erlang:system_time(millisecond) > (Last + Interval) of
        true ->
            {ok, force_roll_over(State)};
        false ->
            {ok, State}
    end;
handle_event(_Event, State) ->
    {ok, State}.

handle_call(_Request, State) ->
    {ok, ok, State}.

handle_info(_Info, State) ->
    {ok, State}.

terminate(_Args, _State) ->
    ok.

force_roll_over(#state{queue_mod = QueueMod} = State) ->
    QueueMod:wal_force_roll_over(node()),
    State#state{last_roll_over = erlang:system_time(millisecond)}.
