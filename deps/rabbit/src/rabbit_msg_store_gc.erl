%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_msg_store_gc).

-behaviour(gen_server2).

-export([start_link/1, compact/2, truncate/4, delete/2, stop/1]).

-export([set_maximum_since_use/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, prioritise_cast/3]).

-record(state,
        { pending,
          timer_ref,
          msg_store_state
        }).

-include_lib("rabbit_common/include/rabbit.hrl").

%%----------------------------------------------------------------------------

-spec start_link(rabbit_msg_store:gc_state()) ->
                           rabbit_types:ok_pid_or_error().

start_link(MsgStoreState) ->
    gen_server2:start_link(?MODULE, [MsgStoreState],
                           [{timeout, infinity}]).

-spec compact(pid(), rabbit_msg_store:file_num()) -> 'ok'.

compact(Server, File) ->
    gen_server2:cast(Server, {compact, File}).

-spec truncate(pid(), rabbit_msg_store:file_num(), non_neg_integer(), integer()) -> 'ok'.

truncate(Server, File, TruncateSize, ThresholdTimestamp) ->
    gen_server2:cast(Server, {truncate, File, TruncateSize, ThresholdTimestamp}).

-spec delete(pid(), rabbit_msg_store:file_num()) -> 'ok'.

delete(Server, File) ->
    gen_server2:cast(Server, {delete, File}).

-spec stop(pid()) -> 'ok'.

stop(Server) ->
    gen_server2:call(Server, stop, infinity).

-spec set_maximum_since_use(pid(), non_neg_integer()) -> 'ok'.

set_maximum_since_use(Pid, Age) ->
    gen_server2:cast(Pid, {set_maximum_since_use, Age}).

%%----------------------------------------------------------------------------

init([MsgStoreState]) ->
    ok = file_handle_cache:register_callback(?MODULE, set_maximum_since_use,
                                             [self()]),
    {ok, #state { pending = #{},
                  msg_store_state    = MsgStoreState }, hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

prioritise_cast({set_maximum_since_use, _Age}, _Len, _State) -> 8;
prioritise_cast(_Msg,                          _Len, _State) -> 0.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

handle_cast({compact, File}, State) ->
    %% Since we don't compact files that have a valid size of 0,
    %% we cannot have a delete queued at the same time as we are
    %% asked to compact. We can always compact.
    {noreply, attempt_action(compact, [File], State), hibernate};

handle_cast({truncate, File, TruncateSize, ThresholdTimestamp}, State = #state{pending = Pending}) ->
    case Pending of
        %% No need to truncate if we are going to delete.
        #{File := {delete, _}} ->
            {noreply, State, hibernate};
        %% Attempt to truncate otherwise. If a truncate was already
        %% scheduled we drop it in favor of the new truncate.
        _ ->
            State1 = State#state{pending = maps:remove(File, Pending)},
            {noreply, attempt_action(truncate, [File, TruncateSize, ThresholdTimestamp], State1), hibernate}
    end;

handle_cast({delete, File}, State = #state{pending = Pending}) ->
    %% We drop any pending action because deletion takes precedence over truncation.
    State1 = State#state{pending = maps:remove(File, Pending)},
    {noreply, attempt_action(delete, [File], State1), hibernate};

handle_cast({set_maximum_since_use, Age}, State) ->
    ok = file_handle_cache:set_maximum_since_use(Age),
    {noreply, State, hibernate}.

%% Run all pending actions.
handle_info({timeout, TimerRef, do_pending},
            State = #state{ pending = Pending,
                            timer_ref = TimerRef }) ->
    State1 = State#state{ pending = #{},
                          timer_ref = undefined },
    State2 = maps:fold(fun(_File, {Action, Args}, StateFold) ->
        attempt_action(Action, Args, StateFold)
    end, State1, Pending),
    {noreply, State2, hibernate};

handle_info(Info, State) ->
    {stop, {unhandled_info, Info}, State}.

terminate(_Reason, State) ->
    State.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

attempt_action(Action, Args,
               State = #state { pending = Pending,
                                msg_store_state    = MsgStoreState }) ->
    case do_action(Action, Args, MsgStoreState) of
        ok ->
            State;
        defer ->
            [File|_] = Args,
            Pending1 = maps:put(File, {Action, Args}, Pending),
            ensure_pending_timer(State #state { pending = Pending1 })
    end.

do_action(compact, [File], MsgStoreState) ->
    rabbit_msg_store:compact_file(File, MsgStoreState);
do_action(truncate, [File, Size, ThresholdTimestamp], MsgStoreState) ->
    rabbit_msg_store:truncate_file(File, Size, ThresholdTimestamp, MsgStoreState);
do_action(delete, [File], MsgStoreState) ->
    rabbit_msg_store:delete_file(File, MsgStoreState).

ensure_pending_timer(State = #state{timer_ref = undefined}) ->
    TimerRef = erlang:start_timer(5000, self(), do_pending),
    State#state{timer_ref = TimerRef};
ensure_pending_timer(State) ->
    State.
