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
%% Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_event).

-include("rabbit.hrl").

-export([start_link/0]).
-export([init_stats_timer/2, init_disabled_stats_timer/2,
         ensure_stats_timer/3, stop_stats_timer/2, reset_stats_timer/2]).
-export([stats_level/2, if_enabled/3]).
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

-type(event() :: #event { type      :: event_type(),
                          props     :: event_props(),
                          timestamp :: event_timestamp() }).

-type(level() :: 'none' | 'coarse' | 'fine').

-type(timer_fun() :: fun (() -> 'ok')).
-type(container() :: tuple()).
-type(pos() :: non_neg_integer()).

-spec(start_link/0 :: () -> rabbit_types:ok_pid_or_error()).
-spec(init_stats_timer/2 :: (container(), pos()) -> container()).
-spec(init_disabled_stats_timer/2 :: (container(), pos()) -> container()).
-spec(ensure_stats_timer/3 :: (container(), pos(), term()) -> container()).
-spec(stop_stats_timer/2 :: (container(), pos()) -> container()).
-spec(reset_stats_timer/2 :: (container(), pos()) -> container()).
-spec(stats_level/2 :: (container(), pos()) -> level()).
-spec(if_enabled/3 :: (container(), pos(), timer_fun()) -> 'ok').
-spec(notify/2 :: (event_type(), event_props()) -> 'ok').
-spec(notify_if/3 :: (boolean(), event_type(), event_props()) -> 'ok').

-endif.

%%----------------------------------------------------------------------------

start_link() ->
    gen_event:start_link({local, ?MODULE}).

%% The idea is, for each stat-emitting object:
%%
%% On startup:
%%   init_stats_timer(State)
%%   notify(created event)
%%   if_enabled(internal_emit_stats) - so we immediately send something
%%
%% On wakeup:
%%   ensure_stats_timer(State, emit_stats)
%%   (Note we can't emit stats immediately, the timer may have fired 1ms ago.)
%%
%% emit_stats:
%%   if_enabled(internal_emit_stats)
%%   reset_stats_timer(State) - just bookkeeping
%%
%% Pre-hibernation:
%%   if_enabled(internal_emit_stats)
%%   stop_stats_timer(State)
%%
%% internal_emit_stats:
%%   notify(stats)

init_stats_timer(C, P) ->
    {ok, StatsLevel} = application:get_env(rabbit, collect_statistics),
    {ok, Interval}   = application:get_env(rabbit, collect_statistics_interval),
    setelement(P, C, #state{level = StatsLevel, interval = Interval,
                            timer = undefined}).

init_disabled_stats_timer(C, P) ->
    setelement(P, C, #state{level = none, interval = 0, timer = undefined}).

ensure_stats_timer(C, P, Msg) ->
    case element(P, C) of
        #state{level = Level, interval = Interval, timer = undefined} = State
          when Level =/= none ->
            TRef = erlang:send_after(Interval, self(), Msg),
            setelement(P, C, State#state{timer = TRef});
        #state{} ->
            C
    end.

stop_stats_timer(C, P) ->
    case element(P, C) of
        #state{timer = TRef} = State when TRef =/= undefined ->
            case erlang:cancel_timer(TRef) of
                false -> C;
                _     -> setelement(P, C, State#state{timer = undefined})
            end;
        #state{} ->
            C
    end.

reset_stats_timer(C, P) ->
    case element(P, C) of
        #state{timer = TRef} = State when TRef =/= undefined ->
            setelement(P, C, State#state{timer = undefined});
        #state{} ->
            C
    end.

stats_level(C, P) ->
    #state{level = Level} = element(P, C),
    Level.

if_enabled(C, P, Fun) ->
    case element(P, C) of
        #state{level = none} -> ok;
        #state{}             -> Fun(), ok
    end.

notify_if(true,   Type,  Props) -> notify(Type, Props);
notify_if(false, _Type, _Props) -> ok.

notify(Type, Props) ->
    gen_event:notify(?MODULE, #event{type      = Type,
                                     props     = Props,
                                     timestamp = os:timestamp()}).
