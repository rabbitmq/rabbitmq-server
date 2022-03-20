%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_event).

-include("rabbit.hrl").

-export([start_link/0]).
-export([init_stats_timer/2, init_disabled_stats_timer/2,
         ensure_stats_timer/3, stop_stats_timer/2, reset_stats_timer/2]).
-export([stats_level/2, if_enabled/3]).
-export([notify/2, notify/3, notify_if/3]).
-export([sync_notify/2, sync_notify/3]).

-ignore_xref([{gen_event, start_link, 2}]).
-dialyzer([{no_missing_calls, start_link/0}]).

%%----------------------------------------------------------------------------

-record(state, {level, interval, timer}).

%%----------------------------------------------------------------------------

-export_type([event_type/0, event_props/0, event_timestamp/0, event/0]).

-type event_type() :: atom().
-type event_props() :: term().
-type event_timestamp() :: non_neg_integer().

-type event() :: #event { type      :: event_type(),
                          props     :: event_props(),
                          reference :: 'none' | reference(),
                          timestamp :: event_timestamp() }.

-type level() :: 'none' | 'coarse' | 'fine'.

-type timer_fun() :: fun (() -> 'ok').
-type container() :: tuple().
-type pos() :: non_neg_integer().

-spec start_link() -> rabbit_types:ok_pid_or_error().
-spec init_stats_timer(container(), pos()) -> container().
-spec init_disabled_stats_timer(container(), pos()) -> container().
-spec ensure_stats_timer(container(), pos(), term()) -> container().
-spec stop_stats_timer(container(), pos()) -> container().
-spec reset_stats_timer(container(), pos()) -> container().
-spec stats_level(container(), pos()) -> level().
-spec if_enabled(container(), pos(), timer_fun()) -> 'ok'.
-spec notify(event_type(), event_props()) -> 'ok'.
-spec notify(event_type(), event_props(), reference() | 'none') -> 'ok'.
-spec notify_if(boolean(), event_type(), event_props()) -> 'ok'.
-spec sync_notify(event_type(), event_props()) -> 'ok'.
-spec sync_notify(event_type(), event_props(), reference() | 'none') -> 'ok'.

%%----------------------------------------------------------------------------

start_link() ->
    %% gen_event:start_link/2 is not available before OTP 20
    %% RabbitMQ 3.7 supports OTP >= 19.3
    case erlang:function_exported(gen_event, start_link, 2) of
        true ->
            gen_event:start_link(
              {local, ?MODULE},
              [{spawn_opt, [{fullsweep_after, 0}]}]
            );
        false ->
            gen_event:start_link({local, ?MODULE})
    end.

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
    %% If the rabbit app is not loaded - use default none:5000
    StatsLevel = application:get_env(rabbit, collect_statistics, none),
    Interval   = application:get_env(rabbit, collect_statistics_interval, 5000),
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

notify(Type, Props) -> notify(Type, rabbit_data_coercion:to_proplist(Props), none).

notify(Type, Props, Ref) ->
    %% Using {Name, node()} here to not fail if the event handler is not started
    gen_event:notify({?MODULE, node()}, event_cons(Type, rabbit_data_coercion:to_proplist(Props), Ref)).

sync_notify(Type, Props) -> sync_notify(Type, Props, none).

sync_notify(Type, Props, Ref) ->
    gen_event:sync_notify(?MODULE, event_cons(Type, Props, Ref)).

event_cons(Type, Props, Ref) ->
    #event{type      = Type,
           props     = Props,
           reference = Ref,
           timestamp = os:system_time(milli_seconds)}.

