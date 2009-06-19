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

-export([register/1, change_memory_footprint/2,
         reduce_memory_footprint/0, increase_memory_footprint/0,
         report_memory/2
        ]).

-define(SERVER, ?MODULE).

-ifdef(use_specs).

-type(queue_mode() :: ( 'mixed' | 'disk' )).

-spec(start_link/0 :: () ->
              ({'ok', pid()} | 'ignore' | {'error', any()})).
-spec(register/1 :: (pid()) -> {'ok', queue_mode()}).
-spec(change_memory_footprint/2 :: (pid(), bool()) -> 'ok').
-spec(reduce_memory_footprint/0 :: () -> 'ok').
-spec(increase_memory_footprint/0 :: () -> 'ok').
-spec(report_memory/2 :: (pid(), non_neg_integer()) -> 'ok').

-endif.

-record(state, { mode,
                 queues
               }).

start_link() ->
    gen_server2:start_link({local, ?SERVER}, ?MODULE, [], []).

register(Pid) ->
    gen_server2:call(?SERVER, {register, Pid}).

change_memory_footprint(_Pid, Conserve) ->
    gen_server2:cast(?SERVER, {change_memory_footprint, Conserve}).

reduce_memory_footprint() ->
    gen_server2:cast(?SERVER, {change_memory_footprint, true}).
                           
increase_memory_footprint() ->
    gen_server2:cast(?SERVER, {change_memory_footprint, false}).

report_memory(Pid, Memory) ->
    gen_server2:cast(?SERVER, {report_memory, Pid, Memory}).

init([]) ->
    process_flag(trap_exit, true),
    ok = rabbit_alarm:register(self(), {?MODULE, change_memory_footprint, []}),
    {ok, #state { mode = unlimited,
                  queues = dict:new()
                }}.

handle_call({register, Pid}, _From,
            State = #state { queues = Qs, mode = Mode }) ->
    _MRef = erlang:monitor(process, Pid),
    Result = case Mode of
                 unlimited -> mixed;
                 _ -> disk
             end,
    {reply, {ok, Result}, State #state { queues = dict:store(Pid, 0, Qs) }}.

handle_cast({change_memory_footprint, true},
            State = #state { mode = disk_only }) ->
    {noreply, State};
handle_cast({change_memory_footprint, true},
            State = #state { mode = ram_disk }) ->
    constrain_queues(true, State #state.queues),
    {noreply, State #state { mode = disk_only }};
handle_cast({change_memory_footprint, true},
            State = #state { mode = unlimited }) ->
    ok = rabbit_disk_queue:to_disk_only_mode(),
    {noreply, State #state { mode = ram_disk }};

handle_cast({change_memory_footprint, false},
            State = #state { mode = unlimited }) ->
    {noreply, State};
handle_cast({change_memory_footprint, false},
            State = #state { mode = ram_disk }) ->
    ok = rabbit_disk_queue:to_ram_disk_mode(),
    {noreply, State #state { mode = unlimited }};
handle_cast({change_memory_footprint, false},
            State = #state { mode = disk_only }) ->
    constrain_queues(false, State #state.queues),
    {noreply, State #state { mode = ram_disk }};

handle_cast({report_memory, Pid, Memory}, State = #state { queues = Qs }) ->
    io:format("Queue ~w requested ~w bytes~n", [Pid, Memory]),
    {noreply, State #state { queues = dict:store(Pid, Memory, Qs) }}.

handle_info({'DOWN', _MRef, process, Pid, _Reason},
            State = #state { queues = Qs }) ->
    {noreply, State #state { queues = dict:erase(Pid, Qs) }};
handle_info({'EXIT', _Pid, Reason}, State) ->
    {stop, Reason, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    State.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

constrain_queues(Constrain, Qs) ->
    dict:fold(
      fun (QPid, _Mem, ok) ->
              rabbit_amqqueue:constrain_memory(QPid, Constrain)
      end, ok, Qs).
