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
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_queue_mode_manager).

-behaviour(gen_server2).

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([register/1, report_memory/4]).

-define(TOTAL_TOKENS, 1000).
-define(LOW_WATER_MARK_FRACTION, 0.25).
-define(EXPIRY_INTERVAL_MICROSECONDS, 5000000).
-define(ACTIVITY_THRESHOLD, 10).
-define(INITIAL_TOKEN_ALLOCATION, 10).

-define(SERVER, ?MODULE).

-ifdef(use_specs).

-type(queue_mode() :: ( 'mixed' | 'disk' )).

-spec(start_link/0 :: () ->
              ({'ok', pid()} | 'ignore' | {'error', any()})).
-spec(register/1 :: (pid()) -> {'ok', queue_mode()}).
-spec(report_memory/4 :: (pid(), non_neg_integer(),
                          non_neg_integer(), non_neg_integer()) -> 'ok').

-endif.

-record(state, { remaining_tokens,
                 mixed_queues,
                 disk_queues,
                 bytes_per_token
               }).

start_link() ->
    gen_server2:start_link({local, ?SERVER}, ?MODULE, [], []).

register(Pid) ->
    gen_server2:call(?SERVER, {register, Pid}).

report_memory(Pid, Memory, Gain, Loss) ->
    gen_server2:cast(?SERVER, {report_memory, Pid, Memory, Gain, Loss}).

init([]) ->
    process_flag(trap_exit, true),
    %% todo, fix up this call as os_mon may not be running
    {MemTotal, _MemUsed, _BigProc} = memsup:get_memory_data(),
    {ok, #state { remaining_tokens = ?TOTAL_TOKENS,
                  mixed_queues = dict:new(),
                  disk_queues = sets:new(),
                  bytes_per_token = MemTotal / ?TOTAL_TOKENS
                }}.

handle_call({register, Pid}, _From,
            State = #state { remaining_tokens = Remaining,
                             mixed_queues = Mixed,
                             disk_queues = Disk }) ->
    _MRef = erlang:monitor(process, Pid),
    {Result, State1} =
        case Remaining >= ?INITIAL_TOKEN_ALLOCATION of
            true ->
                {mixed, State #state { remaining_tokens =
                                       Remaining - ?INITIAL_TOKEN_ALLOCATION,
                                       mixed_queues = dict:store
                                       (Pid, {?INITIAL_TOKEN_ALLOCATION, now()},
                                        Mixed) }};
                                              
            false ->
                {disk, State #state { disk_queues =
                                      sets:add_element(Pid, Disk) }}
        end,
    {reply, {ok, Result}, State1 }.

handle_cast({report_memory, Pid, Memory, BytesGained, BytesLost}, State) ->
    {noreply, State}.

handle_info({'DOWN', _MRef, process, Pid, _Reason},
            State = #state { remaining_tokens = Remaining,
                             mixed_queues = Mixed }) ->
    State1 = case find_queue(Pid, State) of
                 disk ->
                     State;
                 {mixed, {Tokens, _When}} ->
                     State #state { remaining_tokens = Remaining + Tokens,
                                    mixed_queues = dict:erase(Pid, Mixed) }
             end,
    {noreply, State1};
handle_info({'EXIT', _Pid, Reason}, State) ->
    {stop, Reason, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    State.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

find_queue(Pid, #state { disk_queues = Disk, mixed_queues = Mixed }) ->
    case sets:is_element(Pid, Disk) of
        true -> disk;
        false -> {mixed, dict:fetch(Pid, Mixed)}
    end.
            
