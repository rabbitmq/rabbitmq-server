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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_event).

-include("rabbit.hrl").

-export([start_link/0]).
-export([init_stats_timer/0, ensure_stats_timer/3, stop_stats_timer/1]).
-export([reset_stats_timer/1]).
-export([stats_level/1, if_enabled/2]).
-export([notify/2, notify_if/3]).

%%----------------------------------------------------------------------------

-record(state, {level, interval, timer}).

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
               interval :: integer(),
               timer :: atom()
              }).

-type(timer_fun() :: fun (() -> 'ok')).

-spec(start_link/0 :: () -> rabbit_types:ok_pid_or_error()).
-spec(init_stats_timer/0 :: () -> state()).
-spec(ensure_stats_timer/3 :: (state(), pid(), term()) -> state()).
-spec(stop_stats_timer/1 :: (state()) -> state()).
-spec(reset_stats_timer/1 :: (state()) -> state()).
-spec(stats_level/1 :: (state()) -> level()).
-spec(if_enabled/2 :: (state(), timer_fun()) -> 'ok').
-spec(notify/2 :: (event_type(), event_props()) -> 'ok').
-spec(notify_if/3 :: (boolean(), event_type(), event_props()) -> 'ok').

-endif.

%%----------------------------------------------------------------------------

start_link() ->
    gen_event:start_link({local, ?MODULE}).

%% The idea is, for each stat-emitting object:
%%
%% On startup:
%%   Timer = init_stats_timer()
%%   notify(created event)
%%   if_enabled(internal_emit_stats) - so we immediately send something
%%
%% On wakeup:
%%   ensure_stats_timer(Timer, Pid, emit_stats)
%%   (Note we can't emit stats immediately, the timer may have fired 1ms ago.)
%%
%% emit_stats:
%%   if_enabled(internal_emit_stats)
%%   reset_stats_timer(Timer) - just bookkeeping
%%
%% Pre-hibernation:
%%   if_enabled(internal_emit_stats)
%%   stop_stats_timer(Timer)
%%
%% internal_emit_stats:
%%   notify(stats)

init_stats_timer() ->
    {ok, StatsLevel} = application:get_env(rabbit, collect_statistics),
    {ok, Interval} = application:get_env(rabbit, collect_statistics_interval),
    #state{level = StatsLevel, interval = Interval, timer = undefined}.

ensure_stats_timer(State = #state{level = none}, _Pid, _Msg) ->
    State;
ensure_stats_timer(State = #state{interval = Interval,
                                  timer    = undefined}, Pid, Msg) ->
    TRef = erlang:send_after(Interval, Pid, Msg),
    State#state{timer = TRef};
ensure_stats_timer(State, _Pid, _Msg) ->
    State.

stop_stats_timer(State = #state{level = none}) ->
    State;
stop_stats_timer(State = #state{timer = undefined}) ->
    State;
stop_stats_timer(State = #state{timer = TRef}) ->
    erlang:cancel_timer(TRef),
    State#state{timer = undefined}.

reset_stats_timer(State) ->
    State#state{timer = undefined}.

stats_level(#state{level = Level}) ->
    Level.

if_enabled(#state{level = none}, _Fun) ->
    ok;
if_enabled(_State, Fun) ->
    Fun(),
    ok.

notify_if(true,   Type,  Props) -> notify(Type, Props);
notify_if(false, _Type, _Props) -> ok.

notify(Type, Props) ->
    %% TODO: switch to os:timestamp() when we drop support for
    %% Erlang/OTP < R13B01
    gen_event:notify(rabbit_event, #event{type = Type,
                                          props = Props,
                                          timestamp = now()}).
