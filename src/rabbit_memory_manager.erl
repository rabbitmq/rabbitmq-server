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

-module(rabbit_memory_manager).

-behaviour(gen_server2).

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([register/5, report_memory/3, info/0, conserve_memory/2]).

-define(TOTAL_TOKENS, 10000000).
-define(THRESHOLD_MULTIPLIER, 0.05).
-define(THRESHOLD_OFFSET, ?TOTAL_TOKENS * ?THRESHOLD_MULTIPLIER).

-define(SERVER, ?MODULE).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/0 :: () ->
              ({'ok', pid()} | 'ignore' | {'error', any()})).
-spec(register/5 :: (pid(), boolean(), atom(), atom(), list()) -> 'ok').
-spec(report_memory/3 :: (pid(), non_neg_integer(), boolean()) -> 'ok').
-spec(info/0 :: () -> [{atom(), any()}]).
-spec(conserve_memory/2 :: (pid(), bool()) -> 'ok').

-endif.

%%----------------------------------------------------------------------------

-record(state, { available_tokens,
                 processes,
                 callbacks,
                 tokens_per_byte,
                 hibernate,
                 unoppressable,
                 alarmed
               }).

%% Token-credit based memory management

%% Start off by working out the amount of memory available in the
%% system (RAM). Then, work out how many tokens each byte corresponds
%% to. This is the tokens_per_byte field. When a process registers, it
%% must provide an M-F-A triple to a function that needs one further
%% argument, which is the new mode. This will either be 'liberated' or
%% 'oppressed'.
%%
%% Processes then report their own memory usage, in bytes, and the
%% manager takes care of the rest.
%%
%% There are a finite number of tokens in the system. These are
%% allocated to processes as the processes report their memory
%% usage. We keep track of processes which have hibernated. When a
%% process reports memory use which can't be satisfied by the
%% available tokens, we try and oppress processes first from the
%% hibernated group. The hibernated group is a simple queue, and so is
%% implicitly sorted by the order in which processes were added to the
%% queue. This means that when removing from the queue, we evict the
%% sleepiest (and most passive) pid first.
%%
%% If the reported memory use still can't be satisfied after
%% oppressing everyone from those two groups (and note that we check
%% first whether or not oppressing them would make available enough
%% tokens to satisfy the reported use rather than just oppressing all
%% those processes and then going "whoops, didn't help after all"),
%% then we oppress the reporting process. When a process registers, it
%% can declare itself "unoppressable". If a process is unoppressable
%% then it will not be oppressed as a result of other processes
%% needing more tokens. However, if it itself needs additional tokens
%% which aren't available then it is still oppressed as before. This
%% feature is only used by the disk_queue, because if the disk queue
%% is not being used, and hibernates, and then memory pressure gets
%% tight, the disk_queue would typically be one of the first processes
%% to be oppressed (sent to disk_only mode), which cripples
%% performance. Thus by setting it unoppressable, it is only possible
%% for the disk_queue to be oppressed when it is active and attempting
%% to increase its memory allocation.
%%
%% If a process has been oppressed, it continues making memory
%% reports, as if it was liberated. As soon as a reported amount of
%% memory can be satisfied (and this can include oppressing other
%% processes in the way described above), *and* the number of
%% available tokens has changed by ?THRESHOLD_MULTIPLIER since the
%% processes was oppressed, it will be liberated. This later condition
%% prevents processes from continually oppressing each other if they
%% themselves can be liberated by oppressing other processes.
%%
%% Note that the hibernate group can get very out of date. This is
%% fine, and somewhat unavoidable given the absence of useful APIs for
%% queues. Thus we allow them to get out of date (processes will be
%% left in there when they change groups, duplicates can appear, dead
%% processes are not pruned etc etc etc), and when we go through the
%% groups, summing up their allocated tokens, we tidy up at that
%% point.
%%
%% A liberated process, which is reporting a smaller amount of RAM
%% than its last report will remain liberated. A liberated process
%% that is busy but consuming an unchanging amount of RAM will never
%% be oppressed.

%% Specific notes as applied to queues and the disk_queue:
%%
%% The disk_queue is managed in the same way as queues. This means
%% that a queue that has gone back to mixed mode after being in disk
%% mode now has its messages counted twice as they are counted both in
%% the report made by the queue (even though they may not yet be in
%% RAM (though see the prefetcher)) and also by the disk_queue. Thus
%% the amount of available RAM must be higher when going disk -> mixed
%% than when going mixed -> disk. This is fairly sensible as it
%% reduces the risk of any oscillations occurring.
%%
%% The queue process deliberately reports 4 times its estimated RAM
%% usage, and the disk_queue 2.5 times. In practise, this seems to
%% work well. Note that we are deliberately running out of tokes a
%% little early because of the fact that the mixed -> disk transition
%% can transiently eat a lot of memory and take some time (flushing a
%% few million messages to disk is never going to be instantaneous).

start_link() ->
    gen_server2:start_link({local, ?SERVER}, ?MODULE, [], []).

register(Pid, Unoppressable, Module, Function, Args) ->
    gen_server2:cast(?SERVER, {register, Pid, Unoppressable,
                               Module, Function, Args}).

report_memory(Pid, Memory, Hibernating) ->
    gen_server2:cast(?SERVER, {report_memory, Pid, Memory, Hibernating}).

info() ->
    gen_server2:call(?SERVER, info).

conserve_memory(_Pid, Conserve) ->
    gen_server2:pcast(?SERVER, 9, {conserve_memory, Conserve}).

%%----------------------------------------------------------------------------

init([]) ->
    process_flag(trap_exit, true),
    rabbit_alarm:register(self(), {?MODULE, conserve_memory, []}),
    {MemTotal, MemUsed, _BigProc} = memsup:get_memory_data(),
    MemAvail = MemTotal - MemUsed,
    TPB = if MemAvail == 0 -> 0;
             true -> ?TOTAL_TOKENS / MemAvail
          end,
    {ok, #state { available_tokens    = ?TOTAL_TOKENS,
                  processes           = dict:new(),
                  callbacks           = dict:new(),
                  tokens_per_byte     = TPB,
                  hibernate           = queue:new(),
                  unoppressable       = sets:new(),
                  alarmed             = false
                }}.

handle_call(info, _From, State) ->
    State1 = #state { available_tokens    = Avail,
                      processes           = Procs,
                      hibernate           = Sleepy,
                      unoppressable       = Unoppressable } =
        free_upto(undefined, 1 + ?TOTAL_TOKENS, State), %% just tidy
    {reply, [{ available_tokens,        Avail                       },
             { processes,               dict:to_list(Procs)         },
             { hibernated_processes,    queue:to_list(Sleepy)       },
             { unoppressable_processes, sets:to_list(Unoppressable) }], State1}.

handle_cast({report_memory, Pid, Memory, Hibernating},
            State = #state { processes        = Procs,
                             available_tokens = Avail,
                             callbacks        = Callbacks,
                             tokens_per_byte  = TPB,
                             alarmed          = Alarmed }) ->
    Req = rabbit_misc:ceil(TPB * Memory),
    LibreActivity = if Hibernating -> hibernate;
                       true -> active
                    end,
    {StateN = #state { hibernate = Sleepy }, ActivityNew} =
        case find_process(Pid, Procs) of
            {libre, OAlloc, _OActivity} ->
                Avail1 = Avail + OAlloc,
                State1 = #state { available_tokens = Avail2,
                                  processes = Procs1 }
                    = free_upto(Pid, Req,
                                State #state { available_tokens = Avail1 }),
                case Req > Avail2 of
                    true -> %% nowt we can do, oppress the process
                        Procs2 =
                            set_process_mode(Procs1, Callbacks, Pid, oppressed,
                                             {oppressed, Avail2}),
                        {State1 #state { processes = Procs2 }, oppressed};
                    false -> %% keep liberated
                        {State1 #state
                         { processes =
                           dict:store(Pid, {libre, Req, LibreActivity}, Procs1),
                           available_tokens = Avail2 - Req },
                         LibreActivity}
                end;
            {oppressed, OrigAvail} ->
                case Req > 0 andalso
                    ( Alarmed orelse Hibernating orelse
                      (Avail > (OrigAvail - ?THRESHOLD_OFFSET) andalso
                       Avail < (OrigAvail + ?THRESHOLD_OFFSET)) ) of
                    true ->
                        {State, oppressed};
                    false ->
                        State1 = #state { available_tokens = Avail1,
                                          processes = Procs1 } =
                            free_upto(Pid, Req, State),
                        case Req > Avail1 of
                            true ->
                                %% not enough space, so stay oppressed
                                {State1, oppressed};
                            false -> %% can liberate the process
                                Procs2 = set_process_mode(
                                           Procs1, Callbacks, Pid, liberated,
                                           {libre, Req, LibreActivity}),
                                {State1 #state {
                                   processes = Procs2,
                                   available_tokens = Avail1 - Req },
                                 LibreActivity}
                        end
                end
        end,
    StateN1 =
        case ActivityNew of
            active    -> StateN;
            oppressed -> StateN;
            hibernate ->
                StateN #state { hibernate = queue:in(Pid, Sleepy) }
        end,
    {noreply, StateN1};

handle_cast({register, Pid, IsUnoppressable, Module, Function, Args},
            State = #state { callbacks = Callbacks,
                             unoppressable = Unoppressable }) ->
    _MRef = erlang:monitor(process, Pid),
    Unoppressable1 = case IsUnoppressable of
                         true -> sets:add_element(Pid, Unoppressable);
                         false -> Unoppressable
                     end,
    {noreply, State #state { callbacks = dict:store
                             (Pid, {Module, Function, Args}, Callbacks),
                             unoppressable = Unoppressable1
                           }};

handle_cast({conserve_memory, Conserve}, State) ->
    {noreply, State #state { alarmed = Conserve }}.

handle_info({'DOWN', _MRef, process, Pid, _Reason},
            State = #state { available_tokens = Avail,
                             processes        = Procs,
                             callbacks        = Callbacks }) ->
    State1 = State #state { processes = dict:erase(Pid, Procs),
                            callbacks = dict:erase(Pid, Callbacks) },
    {noreply, case find_process(Pid, Procs) of
                  {oppressed, _OrigReq} ->
                      State1;
                  {libre, Alloc, _Activity} ->
                      State1 #state { available_tokens = Avail + Alloc }
              end};
handle_info({'EXIT', _Pid, Reason}, State) ->
    {stop, Reason, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    State.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------

find_process(Pid, Procs) ->
    case dict:find(Pid, Procs) of
        {ok, Value} -> Value;
        error       -> {oppressed, 0}
    end.

set_process_mode(Procs, Callbacks, Pid, Mode, Record) ->
    {Module, Function, Args} = dict:fetch(Pid, Callbacks),
    ok = erlang:apply(Module, Function, Args ++ [Mode]),
    dict:store(Pid, Record, Procs).

tidy_and_sum_sleepy(IgnorePids, Sleepy, Procs) ->
    tidy_and_sum(hibernate, Procs, fun queue:out/1,
                 fun (Pid, _Alloc, Queue) -> queue:in(Pid, Queue) end,
                 IgnorePids, Sleepy, queue:new(), 0).

tidy_and_sum(AtomExpected, Procs, Generator, Consumer, DupCheckSet,
             GenInit, ConInit, AllocAcc) ->
    case Generator(GenInit) of
        {empty, _GetInit} -> {ConInit, AllocAcc};
        {{value, Pid}, GenInit1} ->
            {DupCheckSet1, ConInit1, AllocAcc1} =
                case sets:is_element(Pid, DupCheckSet) of
                    true ->
                        {DupCheckSet, ConInit, AllocAcc};
                    false ->
                        case find_process(Pid, Procs) of
                            {libre, Alloc, AtomExpected} ->
                                {sets:add_element(Pid, DupCheckSet),
                                 Consumer(Pid, Alloc, ConInit),
                                 Alloc + AllocAcc};
                            _ ->
                                {DupCheckSet, ConInit, AllocAcc}
                        end
                end,
            tidy_and_sum(AtomExpected, Procs, Generator, Consumer,
                         DupCheckSet1, GenInit1, ConInit1, AllocAcc1)
    end.

free_upto_sleepy(IgnorePids, Callbacks, Sleepy, Procs, Req, Avail) ->
    free_from(Callbacks,
              fun(Procs1, Sleepy1, SleepyAcc) ->
                      case queue:out(Sleepy1) of
                          {empty, _Sleepy2} ->
                              empty;
                          {{value, Pid}, Sleepy2} ->
                              case sets:is_element(Pid, IgnorePids) of
                                  true  -> {skip, Sleepy2,
                                            queue:in(Pid, SleepyAcc)};
                                  false -> {libre, Alloc, hibernate} =
                                               dict:fetch(Pid, Procs1),
                                           {value, Sleepy2, Pid, Alloc}
                              end
                      end
              end, fun queue:join/2, Procs, Sleepy, queue:new(), Req, Avail).

free_from(
  Callbacks, Transformer, BaseCase, Procs, DestroyMe, CreateMe, Req, Avail) ->
    case Transformer(Procs, DestroyMe, CreateMe) of
        empty ->
            {CreateMe, Procs, Req};
        {skip, DestroyMe1, CreateMe1} ->
            free_from(Callbacks, Transformer, BaseCase, Procs, DestroyMe1,
                      CreateMe1, Req, Avail);
        {value, DestroyMe1, Pid, Alloc} ->
            Procs1 = set_process_mode(
                       Procs, Callbacks, Pid, oppressed, {oppressed, Avail}),
            Req1 = Req - Alloc,
            case Req1 > 0 of
                true -> free_from(Callbacks, Transformer, BaseCase, Procs1,
                                  DestroyMe1, CreateMe, Req1, Avail);
                false -> {BaseCase(DestroyMe1, CreateMe), Procs1, Req1}
            end
    end.

free_upto(Pid, Req, State = #state { available_tokens = Avail,
                                     processes        = Procs,
                                     callbacks        = Callbacks,
                                     hibernate        = Sleepy,
                                     unoppressable    = Unoppressable })
  when Req > Avail ->
    Unoppressable1 = sets:add_element(Pid, Unoppressable),
    {Sleepy1, SleepySum} = tidy_and_sum_sleepy(Unoppressable1, Sleepy, Procs),
    case Req > Avail + SleepySum of
        true -> %% not enough in sleepy, just return tidied state
            State #state { hibernate = Sleepy1 };
        false -> 
            %% ReqRem will be <= 0 because it's likely we'll have
            %% freed more than we need, thus Req - ReqRem is total
            %% freed
            {Sleepy2, Procs1, ReqRem} =
                free_upto_sleepy(Unoppressable1, Callbacks,
                                 Sleepy1, Procs, Req, Avail),
            State #state { available_tokens = Avail + (Req - ReqRem),
                           processes        = Procs1,
                           hibernate        = Sleepy2 }
    end;
free_upto(_Pid, _Req, State) ->
    State.
