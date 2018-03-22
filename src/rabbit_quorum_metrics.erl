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
%% Copyright (c) 2018 Pivotal Software, Inc.  All rights reserved.
%%
-module(rabbit_quorum_metrics).

-include("rabbit.hrl").

-behaviour(gen_server).

-record(state, {stats_timer
               }).

-spec start_link() -> rabbit_types:ok_pid_or_error().

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_) ->
    State = rabbit_event:init_stats_timer(#state{}, #state.stats_timer),
    {ok, rabbit_event:ensure_stats_timer(State, #state.stats_timer, emit_stats)}.

handle_call(test, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(emit_stats, State) ->
    case ets:info(ra_fifo_metrics, name) of
        undefined -> ok;
        _ -> emit_stats()
    end,
    State1 = rabbit_event:reset_stats_timer(State, #state.stats_timer),
    {noreply, rabbit_event:ensure_stats_timer(State1, #state.stats_timer, emit_stats)}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

emit_stats() ->
    %% TODO if we have a ra cluster, shall we emit on all nodes???  Don't think so
    ets:foldl(fun({Name, Enqueue, Checkout, Settle, Return}, no_acc) ->
                      M = Enqueue - Settle,
                      MR = Enqueue - Checkout + Return,
                      MU = Checkout - Settle - Return,
                      R = reductions(Name),
                      QName = rabbit_quorum_queue:queue_name(Name),
                      rabbit_core_metrics:queue_stats(QName, MR, MU, M, R),
                      Infos = rabbit_quorum_queue:infos(QName),
                      rabbit_core_metrics:queue_stats(QName, Infos),
                      rabbit_event:notify(queue_stats, Infos ++ [{name, QName},
                                                                 {messages, M},
                                                                 {messages_ready, MR},
                                                                 {messages_unacknowledged, MU},
                                                                 {reductions, R}]),
                      no_acc
              end, no_acc, ra_fifo_metrics).

reductions(Name) ->
    try
        {reductions, R} = process_info(whereis(Name), reductions),
        R
    catch
        error:badarg ->
            0
    end.
