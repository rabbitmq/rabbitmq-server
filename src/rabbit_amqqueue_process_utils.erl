%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 201-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_amqqueue_process_utils).

-define(SYNC_INTERVAL,                 25). %% milliseconds
-define(RAM_DURATION_UPDATE_INTERVAL,  5000).

-export([backing_queue_pre_hibernate/2,
         ensure_sync_timer/3, stop_sync_timer/3,
         ensure_rate_timer/3, stop_rate_timer/3]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(bq_mod() :: atom()).
-type(bq_state() :: any()). %% A good example of dialyzer's shortcomings

-type(queue_state() :: any()). %% Another such example.
-type(getter(A) :: fun ((queue_state()) -> A)).
-type(setter(A) :: fun ((A, queue_state()) -> queue_state())).

-type(tref() :: term()). %% Sigh. According to timer docs.

-spec(backing_queue_pre_hibernate/2 :: (bq_mod(), bq_state()) -> bq_state()).

-spec(ensure_sync_timer/3 :: (getter('undefined'|tref()),
                              setter('undefined'|tref()),
                              queue_state()) -> queue_state()).
-spec(stop_sync_timer/3 :: (getter('undefined'|tref()),
                            setter('undefined'|tref()),
                            queue_state()) -> queue_state()).

-spec(ensure_rate_timer/3 :: (getter('undefined'|'just_measured'|tref()),
                              setter('undefined'|'just_measured'|tref()),
                              queue_state()) -> queue_state()).
-spec(stop_rate_timer/3 :: (getter('undefined'|'just_measured'|tref()),
                            setter('undefined'|'just_measured'|tref()),
                            queue_state()) -> queue_state()).

-endif.

%%----------------------------------------------------------------------------

backing_queue_pre_hibernate(BQ, BQS) ->
    {RamDuration, BQS1} = BQ:ram_duration(BQS),
    DesiredDuration =
        rabbit_memory_monitor:report_ram_duration(self(), RamDuration),
    BQS2 = BQ:set_ram_duration_target(DesiredDuration, BQS1),
    BQ:handle_pre_hibernate(BQS2).

ensure_sync_timer(Getter, Setter, State) ->
    case Getter(State) of
        undefined -> {ok, TRef} = timer:apply_after(
                                    ?SYNC_INTERVAL, rabbit_amqqueue,
                                    sync_timeout, [self()]),
                     Setter(TRef, State);
        _TRef     -> State
    end.

stop_sync_timer(Getter, Setter, State) ->
    case Getter(State) of
        undefined -> State;
        TRef      -> {ok, cancel} = timer:cancel(TRef),
                     Setter(undefined, State)
    end.

ensure_rate_timer(Getter, Setter, State) ->
    case Getter(State) of
        undefined     -> {ok, TRef} =
                             timer:apply_after(
                               ?RAM_DURATION_UPDATE_INTERVAL, rabbit_amqqueue,
                               update_ram_duration, [self()]),
                         Setter(TRef, State);
        just_measured -> Setter(undefined, State);
        _TRef         -> State
    end.

stop_rate_timer(Getter, Setter, State) ->
    case Getter(State) of
        undefined     -> State;
        just_measured -> Setter(undefined, State);
        TRef          -> {ok, cancel} = timer:cancel(TRef),
                         Setter(undefined, State)
    end.
