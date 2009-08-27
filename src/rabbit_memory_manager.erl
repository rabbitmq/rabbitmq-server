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

-export([register/5, report_memory/3, report_memory/5, info/0,
         oppress/1, liberate/1, conserve_memory/2]).

-define(TOTAL_TOKENS, 10000000).
-define(ACTIVITY_THRESHOLD, 25).

-define(SERVER, ?MODULE).

-ifdef(use_specs).

-spec(start_link/0 :: () ->
              ({'ok', pid()} | 'ignore' | {'error', any()})).
-spec(register/5 :: (pid(), boolean(), atom(), atom(), list()) -> 'ok').
-spec(report_memory/3 :: (pid(), non_neg_integer(), bool()) -> 'ok').
-spec(report_memory/5 :: (pid(), non_neg_integer(),
                          (non_neg_integer() | 'undefined'),
                          (non_neg_integer() | 'undefined'), bool()) ->
             'ok').
-spec(oppress/1 :: (pid()) -> 'ok').
-spec(liberate/1 :: (pid()) -> 'ok').
-spec(info/0 :: () -> [{atom(), any()}]).
-spec(conserve_memory/2 :: (pid(), bool()) -> 'ok').

-endif.

-record(state, { available_tokens,
                 liberated_processes,
                 callbacks,
                 tokens_per_byte,
                 lowrate,
                 hibernate,
                 oppressive_pins,
                 unevictable,
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
%% usage. We keep track of processes which have hibernated, and
%% processes that are doing only a low rate of work (reported as a low
%% gain or loss in memory between memory reports). When a process
%% reports memory use which can't be satisfied by the available
%% tokens, we try and oppress processes first from the hibernated
%% group, and then from the lowrate group. The hibernated group is a
%% simple queue, and so is implicitly sorted by the order in which
%% processes were added to the queue. This means that when removing
%% from the queue, we evict the sleepiest (and most passive) pid
%% first. The lowrate group is a priority queue, where the priority is
%% the truncated log (base e) of the amount of memory allocated. Thus
%% when we remove from the queue, we first remove the queue from the
%% highest bucket.
%%
%% If the reported memory use still can't be satisfied after
%% oppressing everyone from those two groups (and note that we check
%% first whether or not oppressing them would make available enough
%% tokens to satisfy the reported use rather than just oppressing all
%% those processes and then going "whoops, didn't help after all"),
%% then we oppress the reporting process. When a process registers, it
%% can declare itself "unoppressable". If a process is unoppressable
%% then it will not be sent to disk as a result of other processes
%% needing more tokens. However, if it itself needs additional tokens
%% which aren't available then it is still oppressed as before. This
%% feature is only used by the disk_queue, because if the disk queue
%% is not being used, and hibernates, and then memory pressure gets
%% tight, the disk_queue would typically be one of the first processes
%% to be oppressed (sent to disk_only mode), which cripples
%% performance. Thus by setting it unoppressable, it is only possible
%% for the disk_queue to be oppressed when it is active and
%% attempting to increase its memory allocation.
%%
%% If a process has been oppressed, it continues making memory
%% reports, as if it was liberated. As soon as a reported amount of
%% memory can be satisfied (and this can include oppressing other
%% processes in the way described above), it will be liberated. We do
%% not keep any information about oppressed processes.
%%
%% Note that the lowrate and hibernate groups can get very out of
%% date. This is fine, and somewhat unavoidable given the absence of
%% useful APIs for queues. Thus we allow them to get out of date
%% (processes will be left in there when they change groups,
%% duplicates can appear, dead processes are not pruned etc etc etc),
%% and when we go through the groups, summing up their allocated
%% tokens, we tidy up at that point.
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

oppress(Pid) ->
    gen_server2:call(?SERVER, {oppress, Pid}).

liberate(Pid) ->
    gen_server2:call(?SERVER, {liberate, Pid}).

report_memory(Pid, Memory, Hibernating) ->
    report_memory(Pid, Memory, undefined, undefined, Hibernating).

report_memory(Pid, Memory, Gain, Loss, Hibernating) ->
    gen_server2:cast(?SERVER,
                     {report_memory, Pid, Memory, Gain, Loss, Hibernating}).

info() ->
    gen_server2:call(?SERVER, info).

conserve_memory(_Pid, Conserve) ->
    gen_server2:pcast(?SERVER, 9, {conserve_memory, Conserve}).

init([]) ->
    process_flag(trap_exit, true),
    rabbit_alarm:register(self(), {?MODULE, conserve_memory, []}),
    {MemTotal, MemUsed, _BigProc} = memsup:get_memory_data(),
    MemAvail = MemTotal - MemUsed,
    TPB = if MemAvail == 0 -> 0;
             true -> ?TOTAL_TOKENS / MemAvail
          end,
    {ok, #state { available_tokens = ?TOTAL_TOKENS,
                  liberated_processes = dict:new(),
                  callbacks = dict:new(),
                  tokens_per_byte = TPB,
                  lowrate = priority_queue:new(),
                  hibernate = queue:new(),
                  oppressive_pins = sets:new(),
                  unoppressable = sets:new(),
                  alarmed = false
                }}.

handle_call({oppress, Pid}, _From,
            State = #state { liberated_processes = Libre,
                             callbacks = Callbacks,
                             available_tokens = Avail,
                             oppressive_pins = Pins }) ->
    State1 =
        case sets:is_element(Pid, Pins) of
            true -> State;
            false ->
                State2 = State #state { oppressive_pins =
                                        sets:add_element(Pid, Pins) },
                case find_process(Pid, Libre) of
                    {libre, {OAlloc, _OActivity}} ->
                        ok = set_process_mode(Callbacks, Pid, oppressed),
                        State2 #state
                          { liberated_processes = dict:erase(Pid, Libre),
                            available_tokens = Avail + OAlloc };
                    oppressed ->
                        State2
                end
        end,
    {reply, ok, State1};

handle_call({liberate, Pid}, _From,
            State = #state { oppressive_pins = Pins }) ->
    {reply, ok, State #state { oppressive_pins = sets:del_element(Pid, Pins) }};

handle_call(info, _From, State) ->
    State1 = #state { available_tokens = Avail,
                      liberated_processes = Libre,
                      lowrate = Lazy,
                      hibernate = Sleepy,
                      oppressive_pins = Pins,
                      unoppressable = Unoppressable } =
        free_upto(undef, 1 + ?TOTAL_TOKENS, State), %% this'll just do tidying
    {reply, [{ available_tokens, Avail },
             { liberated_processes, dict:to_list(Libre) },
             { lowrate_queues, priority_queue:to_list(Lazy) },
             { hibernated_queues, queue:to_list(Sleepy) },
             { oppressive_pins, sets:to_list(Pins) },
             { unoppressable_queues, sets:to_list(Unoppressable) }], State1}.

handle_cast({report_memory, Pid, Memory, BytesGained, BytesLost, Hibernating},
            State = #state { liberated_processes = Libre,
                             available_tokens = Avail,
                             callbacks = Callbacks,
                             oppressive_pins = Pins,
                             tokens_per_byte = TPB,
                             alarmed = Alarmed }) ->
    Req = rabbit_misc:ceil(TPB * Memory),
    LowRate = case {BytesGained, BytesLost} of
                  {undefined, _} -> false;
                  {_, undefined} -> false;
                  {G, L} -> G < ?ACTIVITY_THRESHOLD andalso
                            L < ?ACTIVITY_THRESHOLD
              end,
    LibreActivity = if Hibernating -> hibernate;
                       LowRate -> lowrate;
                       true -> active
                    end,
    {StateN = #state { lowrate = Lazy, hibernate = Sleepy }, ActivityNew} =
        case find_process(Pid, Libre) of
            {libre, {OAlloc, _OActivity}} ->
                Avail1 = Avail + OAlloc,
                State1 = #state { available_tokens = Avail2,
                                  liberated_processes = Libre1 }
                    = free_upto(Pid, Req,
                                State #state { available_tokens = Avail1 }),
                case Req > Avail2 of
                    true -> %% nowt we can do, oppress the process
                        ok = set_process_mode(Callbacks, Pid, oppressed),
                        {State1 #state { liberated_processes =
                                         dict:erase(Pid, Libre1) }, oppressed};
                    false -> %% keep liberated
                        {State1 #state
                         { liberated_processes =
                           dict:store(Pid, {Req, LibreActivity}, Libre1),
                           available_tokens = Avail2 - Req },
                         LibreActivity}
                end;
            oppressed ->
                case sets:is_element(Pid, Pins) orelse Alarmed of
                    true ->
                        {State, oppressed};
                    false ->
                        State1 = #state { available_tokens = Avail1,
                                          liberated_processes = Libre1 } =
                            free_upto(Pid, Req, State),
                        case Req > Avail1 orelse Hibernating orelse LowRate of
                            true ->
                                %% not enough space, or no compelling
                                %% reason, so stay oppressed
                                {State1, oppressed};
                            false -> %% can liberate the process
                                set_process_mode(Callbacks, Pid, liberated),
                                {State1 #state {
                                   liberated_processes =
                                   dict:store(Pid, {Req, LibreActivity}, Libre1),
                                   available_tokens = Avail1 - Req },
                                 LibreActivity}
                        end
                end
        end,
    StateN1 =
        case ActivityNew of
            active    -> StateN;
            oppressed -> StateN;
            lowrate ->
                StateN #state { lowrate = add_to_lowrate(Pid, Req, Lazy) };
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
                             liberated_processes = Libre }) ->
    State1 = case find_process(Pid, Libre) of
                 oppressed ->
                     State;
                 {libre, {Alloc, _Activity}} ->
                     State #state { available_tokens = Avail + Alloc,
                                    liberated_processes = dict:erase(Pid, Libre) }
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

add_to_lowrate(Pid, Alloc, Lazy) ->
    Bucket = if Alloc == 0 -> 0; %% can't take log(0)
                true -> trunc(math:log(Alloc)) %% log base e
             end,
    priority_queue:in({Pid, Bucket, Alloc}, Bucket, Lazy).

find_process(Pid, Libre) ->
    case dict:find(Pid, Libre) of
        {ok, Value} -> {libre, Value};
        error -> oppressed
    end.

set_process_mode(Callbacks, Pid, Mode) ->
    {Module, Function, Args} = dict:fetch(Pid, Callbacks),
    erlang:apply(Module, Function, Args ++ [Mode]).

tidy_and_sum_lazy(IgnorePids, Lazy, Libre) ->
    tidy_and_sum(lowrate, Libre,
                 fun (Lazy1) ->
                         case priority_queue:out(Lazy1) of
                             {empty, Lazy2} ->
                                 {empty, Lazy2};
                             {{value, {Pid, _Bucket, _Alloc}}, Lazy2} ->
                                 {{value, Pid}, Lazy2}
                         end
                 end, fun add_to_lowrate/3, IgnorePids, Lazy,
                 priority_queue:new(), 0).
            
tidy_and_sum_sleepy(IgnorePids, Sleepy, Libre) ->
    tidy_and_sum(hibernate, Libre, fun queue:out/1,
                 fun (Pid, _Alloc, Queue) -> queue:in(Pid, Queue) end,
                 IgnorePids, Sleepy, queue:new(), 0).

tidy_and_sum(AtomExpected, Libre, Catamorphism, Anamorphism, DupCheckSet,
             CataInit, AnaInit, AllocAcc) ->
    case Catamorphism(CataInit) of
        {empty, _CataInit} -> {AnaInit, AllocAcc};
        {{value, Pid}, CataInit1} ->
            {DupCheckSet1, AnaInit1, AllocAcc1} =
                case sets:is_element(Pid, DupCheckSet) of
                    true ->
                        {DupCheckSet, AnaInit, AllocAcc};
                    false ->
                        case find_process(Pid, Libre) of
                            {libre, {Alloc, AtomExpected}} ->
                                {sets:add_element(Pid, DupCheckSet),
                                 Anamorphism(Pid, Alloc, AnaInit),
                                 Alloc + AllocAcc};
                            _ ->
                                {DupCheckSet, AnaInit, AllocAcc}
                        end
                end,
            tidy_and_sum(AtomExpected, Libre, Catamorphism, Anamorphism,
                          DupCheckSet1, CataInit1, AnaInit1, AllocAcc1)
    end.

free_upto_lazy(IgnorePids, Callbacks, Lazy, Libre, Req) ->
    free_from(
      Callbacks,
      fun(_Libre, Lazy1, LazyAcc) ->
              case priority_queue:out(Lazy1) of
                  {empty, _Lazy2} ->
                      empty;
                  {{value, V = {Pid, Bucket, Alloc}}, Lazy2} ->
                      case sets:is_element(Pid, IgnorePids) of
                          true  -> {skip, Lazy2,
                                    priority_queue:in(V, Bucket, LazyAcc)};
                          false -> {value, Lazy2, Pid, Alloc}
                      end
              end
      end, fun priority_queue:join/2, Libre, Lazy, priority_queue:new(), Req).

free_upto_sleepy(IgnorePids, Callbacks, Sleepy, Libre, Req) ->
    free_from(Callbacks,
              fun(Libre1, Sleepy1, SleepyAcc) ->
                      case queue:out(Sleepy1) of
                          {empty, _Sleepy2} ->
                              empty;
                          {{value, Pid}, Sleepy2} ->
                              case sets:is_element(Pid, IgnorePids) of
                                  true  -> {skip, Sleepy2,
                                            queue:in(Pid, SleepyAcc)};
                                  false -> {Alloc, hibernate} =
                                               dict:fetch(Pid, Libre1),
                                           {value, Sleepy2, Pid, Alloc}
                              end
                      end
              end, fun queue:join/2, Libre, Sleepy, queue:new(), Req).

free_from(Callbacks, Hylomorphism, BaseCase, Libre, CataInit, AnaInit, Req) ->
    case Hylomorphism(Libre, CataInit, AnaInit) of
        empty ->
            {AnaInit, Libre, Req};
        {skip, CataInit1, AnaInit1} ->
            free_from(Callbacks, Hylomorphism, BaseCase, Libre, CataInit1,
                      AnaInit1, Req);
        {value, CataInit1, Pid, Alloc} ->
            Libre1 = dict:erase(Pid, Libre),
            ok = set_process_mode(Callbacks, Pid, oppressed),
            case Req > Alloc of
                true -> free_from(Callbacks, Hylomorphism, BaseCase, Libre1,
                                  CataInit1, AnaInit, Req - Alloc);
                false -> {BaseCase(CataInit1, AnaInit), Libre1, Req - Alloc}
            end
    end.

free_upto(Pid, Req, State = #state { available_tokens = Avail,
                                     liberated_processes = Libre,
                                     callbacks = Callbacks,
                                     lowrate = Lazy,
                                     hibernate = Sleepy,
                                     unoppressable = Unoppressable })
  when Req > Avail ->
    Unoppressable1 = sets:add_element(Pid, Unoppressable),
    {Sleepy1, SleepySum} = tidy_and_sum_sleepy(Unoppressable1, Sleepy, Libre),
    case Req > Avail + SleepySum of
        true -> %% not enough in sleepy, have a look in lazy too
            {Lazy1, LazySum} = tidy_and_sum_lazy(Unoppressable1, Lazy, Libre),
            case Req > Avail + SleepySum + LazySum of
                true -> %% can't free enough, just return tidied state
                    State #state { lowrate = Lazy1, hibernate = Sleepy1 };
                false -> %% need to free all of sleepy, and some of lazy
                    {Sleepy2, Libre1, ReqRem} =
                        free_upto_sleepy(Unoppressable1, Callbacks,
                                         Sleepy1, Libre, Req),
                    {Lazy2, Libre2, ReqRem1} =
                        free_upto_lazy(Unoppressable1, Callbacks,
                                       Lazy1, Libre1, ReqRem),
                    %% ReqRem1 will be <= 0 because it's
                    %% likely we'll have freed more than we
                    %% need, thus Req - ReqRem1 is total freed
                    State #state { available_tokens = Avail + (Req - ReqRem1),
                                   liberated_processes = Libre2, lowrate = Lazy2,
                                   hibernate = Sleepy2 }
            end;
        false -> %% enough available in sleepy, don't touch lazy
            {Sleepy2, Libre1, ReqRem} =
                free_upto_sleepy(Unoppressable1, Callbacks, Sleepy1, Libre, Req),
            State #state { available_tokens = Avail + (Req - ReqRem),
                           liberated_processes = Libre1, hibernate = Sleepy2 }
    end;
free_upto(_Pid, _Req, State) ->
    State.
