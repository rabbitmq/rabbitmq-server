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

%% In practice erlang shouldn't be allowed to grow to more than a half
%% of available memory. The pessimistic scenario is when erlang VM has  
%% a single erlang process that's consuming all the memory.
%% In such case during garbage collection erlang tries to allocate
%% huge chunk of continuous memory, which can result in a crash
%% (likely on 32-bit machine) or heavy swapping (likely on 64-bit).
%% 
%% This module tries to warn Rabbit before such situations happen,
%% so that it has higher chances to prevent running out of memory.
%%
%% This code depends on Erlang Memsup supervisor. Setting the update interval
%% causes a side effect of setting the interval on Memsup.
%% This should rarely be an issue.

-module(rabbit_memguard).

-behaviour(gen_server2).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([update/0]).


-define(SERVER, rabbit_memguard).
-define(DEFAULT_MEMORY_CHECK_INTERVAL, 1000).

-record(state, {memory_limit,
                timeout,
                timer,
                alarmed
               }).

%%----------------------------------------------------------------------------

start_link(Args) ->
    gen_server2:start_link({local, ?SERVER}, ?MODULE, [Args], []).

update() ->
    gen_server2:cast(?SERVER, update).

%%----------------------------------------------------------------------------
get_system_memory_data(Key) ->
    dict:fetch(Key, 
            dict:from_list( memsup:get_system_memory_data() )).

%% On a 32-bit machine, if you're using more than 2 gigs of RAM
%% you're in big trouble anyway.
get_vm_limit() ->
    case erlang:system_info(wordsize) of
        4 -> 2*1024*1024*1024;
        8 -> infinity
    end.


min(A,B) -> 
    case A<B of
        true -> A;
        false -> B
    end.

mem_fraction_to_limit(MemFraction) ->
    get_system_memory_data(system_total_memory) * MemFraction.

mem_limit_to_fraction(MemLimit) ->
    MemLimit / get_system_memory_data(system_total_memory).


get_mem_limit(MemFraction) ->
    min(mem_fraction_to_limit(MemFraction), get_vm_limit()).

init([MemFraction]) -> 
    MemLimit = get_mem_limit(MemFraction),
    rabbit_log:info("Memory alarm set to ~p.~n", [MemLimit]),
    adjust_memsup_interval(?DEFAULT_MEMORY_CHECK_INTERVAL),
    TRef = start_timer(?DEFAULT_MEMORY_CHECK_INTERVAL),
    State = #state { memory_limit = MemLimit,
                     timeout = ?DEFAULT_MEMORY_CHECK_INTERVAL,
                     timer = TRef,
                     alarmed = false},
    {ok, internal_update(State)}.

start_timer(Timeout) ->
    {ok, TRef} = timer:apply_interval(Timeout, ?MODULE, update, []),
    TRef.

handle_call(get_memory_high_watermark, _From, State) ->
    {reply, mem_limit_to_fraction(State#state.memory_limit), State};

handle_call({set_memory_high_watermark, MemFraction}, _From, State) ->
    MemLimit = get_mem_limit(MemFraction),
    rabbit_log:info("Memory alarm set to ~p.~n", [MemLimit]),
    {reply, ok, State#state{memory_limit = MemLimit}};

handle_call(get_check_interval, _From, State) ->
    {reply, State#state.timeout, State};

handle_call({set_check_interval, Timeout}, _From, State) ->
    {ok, cancel} = timer:cancel(State#state.timer),
    adjust_memsup_interval(Timeout),
    {reply, ok, State#state{timeout = Timeout, timer = start_timer(Timeout)}};

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(update, State) ->
    {noreply, internal_update(State)};

handle_cast(_Request, State) -> 
    {noreply, State}.

handle_info(_Info, State) -> 
    {noreply, State}.

terminate(_Reason, _State) -> 
    ok.

code_change(_OldVsn, State, _Extra) -> 
    {ok, State}.
 
emit_update_info(State, MemUsed, MemLimit) ->
    {_Total, _Allocated, {_WorstPid, WorstAllocated}} 
                                                = memsup:get_memory_data(),
    FreeMemory = get_system_memory_data(free_memory),
    rabbit_log:info("memory_high_watermark ~p. Memory used:~p allowed:~p "
                    "heaviest_process:~p free:~p.~n", 
                       [State, MemUsed, MemLimit, WorstAllocated, FreeMemory]).

internal_update(State = #state { memory_limit = MemLimit,
                                 alarmed = Alarmed}) ->
    MemUsed = erlang:memory(total),
    NewAlarmed = MemUsed > MemLimit,
    case {Alarmed, NewAlarmed} of
        {false, true} ->
            emit_update_info(set, MemUsed, MemLimit),
            alarm_handler:set_alarm({memory_high_watermark, []});
        {true, false} ->
            emit_update_info(clear, MemUsed, MemLimit),
            alarm_handler:clear_alarm(memory_high_watermark);
        _ ->
            ok
    end,    
    State #state {alarmed = NewAlarmed}.

adjust_memsup_interval(IntervalMs) ->                                                            
    %% The default memsup check interval is 1 minute, which is way too                 
    %% long - rabbit can gobble up all memory in a matter of seconds.                  
    %% Unfortunately the memory_check_interval configuration parameter                 
    %% and memsup:set_check_interval/1 function only provide a                         
    %% granularity of minutes. So we have to peel off one layer of the                 
    %% API to get to the underlying layer which operates at the                        
    %% granularity of milliseconds.                                                    
    %%                                                                                 
    %% Note that the new setting will only take effect after the first                 
    %% check has completed, i.e. after one minute. So if rabbit eats                   
    %% all the memory within the first minute after startup then we                    
    %% are out of luck.                                                                
    ok = os_mon:call(memsup,                                                           
                     {set_check_interval, IntervalMs},                     
                     infinity).                                                        
                                                                                       
