%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2010 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2010 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_event).

-include("rabbit.hrl").

-export([init_stats_timer/0, ensure_stats_timer/3, stop_stats_timer/2]).
-export([ensure_stats_timer_after/2, reset_stats_timer_after/1]).
-export([stats_level/1]).
-export([notify/2]).

%%----------------------------------------------------------------------------

-record(state, {level, timer}).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([event_type/0, event_props/0, event_timestamp/0, event/0]).

-type(event_type() :: atom()).
-type(event_props() :: term()).
-type(event_timestamp() ::
        {non_neg_integer(), non_neg_integer(), non_neg_integer()}).

-type(event() :: #event {
             type :: event_type(),
             props :: event_props(),
             timestamp :: event_timestamp()
            }).

-type(level() :: 'none' | 'coarse' | 'fine').

-opaque(state() :: #state {
               level :: level(),
               timer :: atom()
              }).

-type(timer_fun() :: fun (() -> 'ok')).

-spec(init_stats_timer/0 :: () -> state()).
-spec(ensure_stats_timer/3 :: (state(), timer_fun(), timer_fun()) -> state()).
-spec(stop_stats_timer/2 :: (state(), timer_fun()) -> state()).
-spec(ensure_stats_timer_after/2 :: (state(), timer_fun()) -> state()).
-spec(reset_stats_timer_after/1 :: (state()) -> state()).
-spec(stats_level/1 :: (state()) -> level()).
-spec(notify/2 :: (event_type(), event_props()) -> 'ok').

-endif.

%%----------------------------------------------------------------------------

init_stats_timer() ->
    {ok, StatsLevel} = application:get_env(rabbit, collect_statistics),
    #state{level = StatsLevel,
           timer = undefined}.

ensure_stats_timer(State = #state{level = none}, _NowFun, _TimerFun) ->
    State;
ensure_stats_timer(State = #state{timer = undefined}, NowFun, TimerFun) ->
    NowFun(),
    {ok, TRef} = timer:apply_interval(?STATS_INTERVAL,
                                      erlang, apply, [TimerFun, []]),
    State#state{timer = TRef};
ensure_stats_timer(State, _NowFun, _TimerFun) ->
    State.

stop_stats_timer(State = #state{level = none}, _NowFun) ->
    State;
stop_stats_timer(State = #state{timer = undefined}, _NowFun) ->
    State;
stop_stats_timer(State = #state{timer = TRef}, NowFun) ->
    {ok, cancel} = timer:cancel(TRef),
    NowFun(),
    State#state{timer = undefined}.

ensure_stats_timer_after(State = #state{level = none}, _TimerFun) ->
    State;
ensure_stats_timer_after(State = #state{timer = undefined}, TimerFun) ->
    {ok, TRef} = timer:apply_after(?STATS_INTERVAL,
                                   erlang, apply, [TimerFun, []]),
    State#state{timer = TRef};
ensure_stats_timer_after(State, _TimerFun) ->
    State.

reset_stats_timer_after(State) ->
    State#state{timer = undefined}.

stats_level(#state{level = Level}) ->
    Level.

notify(Type, Props) ->
    try
        gen_event:notify(rabbit_event, #event{type = Type,
                                              props = Props,
                                              timestamp = os:timestamp()})
    catch error:badarg ->
            %% badarg means rabbit_event is no longer registered. We never
            %% unregister it so the great likelihood is that we're shutting
            %% down the broker but some events were backed up. Ignore it.
            ok
    end.
