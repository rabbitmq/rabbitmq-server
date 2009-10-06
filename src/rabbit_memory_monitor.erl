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


%% This module handles the node-wide memory statistics. 
%% It receives statistics from all queues, counts the desired
%% queue length (in seconds), and sends this information back to
%% queues.
%%
%% Normally, messages are exchanged like that:
%%
%%           (1)      (2)                     (3)
%% Timer      |        |
%%            v        v
%% Queue -----+--------+-----<***hibernated***>------------->
%%            | ^      | ^                     ^               
%%            v |      v |                     |
%% Monitor X--*-+--X---*-+--X------X----X-----X+----------->
%%
%% Or to put it in words. Queue periodically sends (casts) 'push_drain_ratio'
%% message to the Monitor (cases 1 and 2 on the asciiart above). Monitor 
%% _always_ replies with a 'set_bufsec_limit' cast. This way, 
%% we're pretty sure that the Queue is not hibernated.
%% Monitor periodically recounts numbers ('X' on asciiart). If, during this
%% update we notice that a queue was using too much memory, we send a message
%% back. This will happen even if the queue is hibernated, as we really do want
%% it to reduce its memory footprint.


-module(rabbit_memory_monitor).

-behaviour(gen_server2).

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([update/0]).

-export([register/1]).

-record(state, {timer,          %% 'internal_update' timer
                drain_dict,     %% dict, queue_pid:seconds_till_queue_is_empty
                drain_avg,      %% global, the desired queue depth (in seconds)
                memory_limit    %% how much memory we intend to use
               }).

-define(SERVER, ?MODULE).
-define(DEFAULT_UPDATE_INTERVAL_MS, 2500).

%% Enable debug reports in stdout:
-define(debug, true).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-endif.

%%----------------------------------------------------------------------------

start_link() ->
    gen_server2:start_link({local, ?SERVER}, ?MODULE, [], []).

update() ->
    gen_server2:cast(?SERVER, update).

%%----------------------------------------------------------------------------

register(Pid) ->
    gen_server2:cast(?SERVER, {register, Pid}).

%%----------------------------------------------------------------------------

init([]) -> 
    %% TODO: References to os_mon and rabbit_memsup_linux 
    %%       should go away as bug 21457 removes it.
    %%       BTW: memsup:get_system_memory_data() doesn't work.
    {state, TotalMemory, _Allocated} = rabbit_memsup_linux:update({state, 0,0}),
    
    {ok, TRef} = timer:apply_interval(?DEFAULT_UPDATE_INTERVAL_MS, 
                                                        ?SERVER, update, []),
    MemoryHighWatermark = os_mon:get_env(memsup, system_memory_high_watermark),
    MemoryLimit = erlang:trunc(TotalMemory * MemoryHighWatermark),
    {ok, #state{timer = TRef,
                drain_dict = dict:new(),
                drain_avg = infinity,
                memory_limit = MemoryLimit}}.

handle_call(_Request, _From, State) ->
    {noreply, State}.


handle_cast(update, State) ->
    {noreply, internal_update(State)};

handle_cast({register, Pid}, State) ->
    _MRef = erlang:monitor(process, Pid),
    {noreply, State};

handle_cast({push_drain_ratio, Pid, DrainRatio}, State) ->
    gen_server2:cast(Pid, {set_bufsec_limit, State#state.drain_avg}),
    {noreply, State#state{drain_dict = 
                        dict:store(Pid, DrainRatio, State#state.drain_dict)}};

handle_cast(_Request, State) ->
    {noreply, State}.


handle_info({'DOWN', _MRef, process, Pid, _Reason}, State) ->
    {noreply, State#state{drain_dict = dict:erase(Pid, State#state.drain_dict)}};

handle_info(_Info, State) -> 
    {noreply, State}.


terminate(_Reason, _State) -> 
    ok.

code_change(_OldVsn, State, _Extra) -> 
    {ok, State}.

-ifdef(debug). 
ftoa(Float) ->
    Str = case is_float(Float) of
        true  -> io_lib:format("~11.3f",[Float]);
        false -> io_lib:format("~p", [Float])
    end,
    lists:flatten(Str).

print_debug_info(UsedSeconds, AvailableSeconds, UsedMemory, TotalMemory, 
                                                PerQueueSeconds, QueueSec) ->
    io:format("Update ~s/~s ~s/~s PerQueueSeconds:~s ~s~n", 
                [ftoa(UsedSeconds), ftoa(AvailableSeconds),
                ftoa(UsedMemory/1024.0/1024.0), ftoa(TotalMemory/1024.0/1024.0),
                ftoa(PerQueueSeconds), 
                [" "] ++ lists:flatten([ftoa(Q)++" " || Q <- QueueSec])
                ]).
-else.
print_debug_info(_UsedSeconds, _AvailableSeconds, _UsedMemory, _TotalMemory, 
                                                _PerQueueSeconds, _QueueSec) ->
    ok.

-endif.

internal_update(State) ->
    UsedMemory = erlang:memory(total),
    TotalMemory = State#state.memory_limit,
    QueueSec = [V || {_K, V} <- dict:to_list(State#state.drain_dict) ],
    UsedSeconds = lists:sum( lists:filter(fun (A) -> 
                                                is_number(A) or is_float(A) 
                                            end, 
                                          QueueSec) ),
    AvailableSeconds = case UsedSeconds of
        0 -> infinity;
        0.0 -> infinity;
        _ -> TotalMemory / (UsedMemory / UsedSeconds)
    end,
    QueuesNumber = dict:size(State#state.drain_dict),
    PerQueueSeconds = case (QueuesNumber > 0) and (AvailableSeconds /= infinity) of
        true -> AvailableSeconds / QueuesNumber;
        false -> infinity
    end,
    print_debug_info(UsedSeconds, AvailableSeconds, UsedMemory, TotalMemory, 
                                                    PerQueueSeconds, QueueSec),
    %% Inform the queue to reduce it's memory usage when needed.
    %% This can sometimes wake the queue from hibernation. Well, we don't care.
    ReduceMemory = fun ({Pid, QueueS}) ->
        case QueueS > PerQueueSeconds of 
            true -> 
                gen_server2:cast(Pid, {set_bufsec_limit, PerQueueSeconds});
            _ -> ok
        end 
    end,
    lists:map(ReduceMemory, dict:to_list(State#state.drain_dict)),
    State#state{drain_avg = PerQueueSeconds}.


