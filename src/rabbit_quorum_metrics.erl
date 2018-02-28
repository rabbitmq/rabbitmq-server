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

-define(STATISTICS_KEYS,
        [policy,
         operator_policy,
         effective_policy_definition,
         exclusive_consumer_pid,
         exclusive_consumer_tag,
         consumers,
         consumer_utilisation,
         memory,
         state,
         garbage_collection
        ]).


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
    emit_stats(),
    State1 = rabbit_event:reset_stats_timer(State, #state.stats_timer),
    {noreply, rabbit_event:ensure_stats_timer(State1, #state.stats_timer, emit_stats)}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

emit_stats() ->
    ets:foldl(fun({Name, Enqueue, Checkout, Settle, Return}, no_acc) ->
                      M = Enqueue - Settle,
                      MR = Enqueue - Checkout + Return,
                      MU = Checkout - Settle - Return,
                      R = reductions(Name),
                      [{_, QName}] = ets:lookup(quorum_mapping, Name),
                      rabbit_core_metrics:queue_stats(QName, MR, MU, M, R),
                      Infos = infos(QName),
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

infos(QName) ->
    {ok, Q} = rabbit_amqqueue:lookup(QName),
    [{Item, i(Item, Q)} || Item <- ?STATISTICS_KEYS].

i(policy, Q) ->
    case rabbit_policy:name(Q) of
        none   -> '';
        Policy -> Policy
    end;
i(operator_policy, Q) ->
    case rabbit_policy:name_op(Q) of
        none   -> '';
        Policy -> Policy
    end;
i(effective_policy_definition, Q) ->
    case rabbit_policy:effective_definition(Q) of
        undefined -> [];
        Def       -> Def
    end;
i(exclusive_consumer_pid, _Q) ->
    %% TODO
    '';
i(exclusive_consumer_tag, _Q) ->
    %% TODO
    '';
i(consumers, _Q) ->
    %% TODO
    0;
i(consumer_utilisation, _Q) ->
    %% TODO!
    0;
i(memory, #amqqueue{pid = {Name, _}}) ->
    try
        {memory, M} = process_info(whereis(Name), memory),
        M
    catch
        error:badarg ->
            0
    end;
i(state, #amqqueue{pid = {Name, _}}) ->
    %% TODO guess this is it by now?
    case whereis(Name) of
        undefined -> down;
        _ -> running
    end;
i(garbage_collection, #amqqueue{pid = {Name, _}}) ->
    try
        rabbit_misc:get_gc_info(whereis(Name))
    catch
        error:badarg ->
            []
    end.
