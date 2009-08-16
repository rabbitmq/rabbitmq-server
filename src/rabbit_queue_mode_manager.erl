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

-export([register/5, report_memory/3, report_memory/5, info/0,
         pin_to_disk/1, unpin_from_disk/1]).

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
-spec(pin_to_disk/1 :: (pid()) -> 'ok').
-spec(unpin_from_disk/1 :: (pid()) -> 'ok').

-endif.

-record(state, { available_tokens,
                 mixed_queues,
                 callbacks,
                 tokens_per_byte,
                 lowrate,
                 hibernate,
                 disk_mode_pins,
                 unevictable
               }).

%% Token-credit based memory management

%% Start off by working out the amount of memory available in the
%% system (RAM). Then, work out how many tokens each byte corresponds
%% to. This is the tokens_per_byte field. When a process registers, it
%% must provide an M-F-A triple to a function that needs one further
%% argument, which is the new mode. This will either be 'mixed' or
%% 'disk'.
%%
%% Processes then report their own memory usage, in bytes, and the
%% manager takes care of the rest.
%%
%% There are a finite number of tokens in the system. These are
%% allocated to processes as they are requested. We keep track of
%% processes which have hibernated, and processes that are doing only
%% a low rate of work. When a request for memory can't be satisfied,
%% we try and evict processes first from the hibernated group, and
%% then from the lowrate group. The hibernated group is a simple
%% queue, and so is implicitly sorted by the order in which processes
%% were added to the queue. This means that when removing from the
%% queue, we hibernate the sleepiest pid first. The lowrate group is a
%% priority queue, where the priority is the truncated log (base e) of
%% the amount of memory allocated. Thus when we remove from the queue,
%% we first remove the queue from the highest bucket.
%%
%% If the request still can't be satisfied after evicting to disk
%% everyone from those two groups (and note that we check first
%% whether or not freeing them would make available enough tokens to
%% satisfy the request rather than just sending all those queues to
%% disk and then going "whoops, didn't help after all"), then we send
%% the requesting process to disk. When a queue registers, it can
%% declare itself "unevictable". If a queue is unevictable then it
%% will not be sent to disk as a result of other processes requesting
%% more memory. However, if it itself is requesting more memory and
%% that request can't be satisfied then it is still sent to disk as
%% before. This feature is only used by the disk_queue, because if the
%% disk queue is not being used, and hibernates, and then memory
%% pressure gets tight, the disk_queue would typically be one of the
%% first processes to get sent to disk, which cripples
%% performance. Thus by setting it unevictable, it is only possible
%% for the disk_queue to be sent to disk when it is active and
%% attempting to increase its memory allocation.
%%
%% If a process has been sent to disk, it continues making
%% requests. As soon as a request can be satisfied (and this can
%% include sending other processes to disk in the way described
%% above), it will be told to come back into mixed mode. We do not
%% keep any information about queues in disk mode.
%%
%% Note that the lowrate and hibernate groups can get very out of
%% date. This is fine, and somewhat unavoidable given the absence of
%% useful APIs for queues. Thus we allow them to get out of date
%% (processes will be left in there when they change groups,
%% duplicates can appear, dead processes are not pruned etc etc etc),
%% and when we go through the groups, summing up their amount of
%% memory, we tidy up at that point.
%%
%% A process which is not evicted to disk, and is requesting a smaller
%% amount of RAM than its last request will always be satisfied. A
%% mixed-mode process that is busy but consuming an unchanging amount
%% of RAM will never be sent to disk. The disk_queue is also managed
%% in the same way. This means that a queue that has gone back to
%% being mixed after being in disk mode now has its messages counted
%% twice as they are counted both in the request made by the queue
%% (even though they may not yet be in RAM (though see the
%% prefetcher)) and also by the disk_queue. Thus the amount of
%% available RAM must be higher when going disk -> mixed than when
%% going mixed -> disk. This is fairly sensible as it reduces the risk
%% of any oscillations occurring.
%%
%% The queue process deliberately reports 4 times its estimated RAM
%% usage, and the disk_queue 2.5 times. In practise, this seems to
%% work well. Note that we are deliberately running out of tokes a
%% little early because of the fact that the mixed -> disk transition
%% can transiently eat a lot of memory and take some time (flushing a
%% few million messages to disk is never going to be instantaneous).

start_link() ->
    gen_server2:start_link({local, ?SERVER}, ?MODULE, [], []).

register(Pid, Unevictable, Module, Function, Args) ->
    gen_server2:cast(?SERVER, {register, Pid, Unevictable,
                               Module, Function, Args}).

pin_to_disk(Pid) ->
    gen_server2:call(?SERVER, {pin_to_disk, Pid}).

unpin_from_disk(Pid) ->
    gen_server2:call(?SERVER, {unpin_from_disk, Pid}).

report_memory(Pid, Memory, Hibernating) ->
    report_memory(Pid, Memory, undefined, undefined, Hibernating).

report_memory(Pid, Memory, Gain, Loss, Hibernating) ->
    gen_server2:cast(?SERVER,
                     {report_memory, Pid, Memory, Gain, Loss, Hibernating}).

info() ->
    gen_server2:call(?SERVER, info).

init([]) ->
    process_flag(trap_exit, true),
    %% todo, fix up this call as os_mon may not be running
    {MemTotal, MemUsed, _BigProc} = memsup:get_memory_data(),
    MemAvail = MemTotal - MemUsed,
    {ok, #state { available_tokens = ?TOTAL_TOKENS,
                  mixed_queues = dict:new(),
                  callbacks = dict:new(),
                  tokens_per_byte = ?TOTAL_TOKENS / MemAvail,
                  lowrate = priority_queue:new(),
                  hibernate = queue:new(),
                  disk_mode_pins = sets:new(),
                  unevictable = sets:new()
                }}.

handle_call({pin_to_disk, Pid}, _From,
            State = #state { mixed_queues = Mixed,
                             callbacks = Callbacks,
                             available_tokens = Avail,
                             disk_mode_pins = Pins }) ->
    {Res, State1} =
        case sets:is_element(Pid, Pins) of
            true -> {ok, State};
            false ->
                case find_queue(Pid, Mixed) of
                    {mixed, {OAlloc, _OActivity}} ->
                        ok = set_queue_mode(Callbacks, Pid, disk),
                        {ok, State #state { mixed_queues =
                                            dict:erase(Pid, Mixed),
                                            available_tokens = Avail + OAlloc,
                                            disk_mode_pins =
                                            sets:add_element(Pid, Pins)
                                          }};
                    disk ->
                        {ok, State #state { disk_mode_pins =
                                            sets:add_element(Pid, Pins) }}
                end
        end,
    {reply, Res, State1};

handle_call({unpin_from_disk, Pid}, _From,
            State = #state { disk_mode_pins = Pins }) ->
    {reply, ok, State #state { disk_mode_pins = sets:del_element(Pid, Pins) }};

handle_call(info, _From, State) ->
    State1 = #state { available_tokens = Avail,
                      mixed_queues = Mixed,
                      lowrate = Lazy,
                      hibernate = Sleepy,
                      disk_mode_pins = Pins,
                      unevictable = Unevictable } =
        free_upto(undef, 1 + ?TOTAL_TOKENS, State), %% this'll just do tidying
    {reply, [{ available_tokens, Avail },
             { mixed_queues, dict:to_list(Mixed) },
             { lowrate_queues, priority_queue:to_list(Lazy) },
             { hibernated_queues, queue:to_list(Sleepy) },
             { queues_pinned_to_disk, sets:to_list(Pins) },
             { unevictable_queues, sets:to_list(Unevictable) }], State1}.


handle_cast({report_memory, Pid, Memory, BytesGained, BytesLost, Hibernating},
            State = #state { mixed_queues = Mixed,
                             available_tokens = Avail,
                             callbacks = Callbacks,
                             disk_mode_pins = Pins,
                             tokens_per_byte = TPB }) ->
    Req = rabbit_misc:ceil(TPB * Memory),
    LowRate = case {BytesGained, BytesLost} of
                  {undefined, _} -> false;
                  {_, undefined} -> false;
                  {G, L} -> G < ?ACTIVITY_THRESHOLD andalso
                            L < ?ACTIVITY_THRESHOLD
              end,
    MixedActivity = if Hibernating -> hibernate;
                       LowRate -> lowrate;
                       true -> active
                    end,
    {StateN = #state { lowrate = Lazy, hibernate = Sleepy }, ActivityNew} =
        case find_queue(Pid, Mixed) of
            {mixed, {OAlloc, _OActivity}} ->
                Avail1 = Avail + OAlloc,
                State1 =
                    #state { available_tokens = Avail2, mixed_queues = Mixed1 }
                    = free_upto(Pid, Req,
                                State #state { available_tokens = Avail1 }),
                case Req > Avail2 of
                    true -> %% nowt we can do, send to disk
                        ok = set_queue_mode(Callbacks, Pid, disk),
                        {State1 #state { mixed_queues =
                                         dict:erase(Pid, Mixed1) }, disk};
                    false -> %% keep mixed
                        {State1 #state
                         { mixed_queues =
                           dict:store(Pid, {Req, MixedActivity}, Mixed1),
                           available_tokens = Avail2 - Req },
                         MixedActivity}
                end;
            disk ->
                case sets:is_element(Pid, Pins) of
                    true ->
                        {State, disk};
                    false ->
                        State1 = #state { available_tokens = Avail1,
                                          mixed_queues = Mixed1 } =
                            free_upto(Pid, Req, State),
                        case Req > Avail1 orelse Hibernating orelse LowRate of
                            true ->
                                %% not enough space, or no compelling
                                %% reason, so stay as disk
                                {State1, disk};
                            false -> %% can go to mixed mode
                                set_queue_mode(Callbacks, Pid, mixed),
                                {State1 #state {
                                   mixed_queues =
                                   dict:store(Pid, {Req, MixedActivity}, Mixed1),
                                   available_tokens = Avail1 - Req },
                                 MixedActivity}
                        end
                end
        end,
    StateN1 =
        case ActivityNew of
            active -> StateN;
            disk -> StateN;
            lowrate ->
                StateN #state { lowrate = add_to_lowrate(Pid, Req, Lazy) };
            hibernate ->
                StateN #state { hibernate = queue:in(Pid, Sleepy) }
        end,
    {noreply, StateN1};

handle_cast({register, Pid, IsUnevictable, Module, Function, Args},
            State = #state { callbacks = Callbacks,
                             unevictable = Unevictable }) ->
    _MRef = erlang:monitor(process, Pid),
    Unevictable1 = case IsUnevictable of
                       true -> sets:add_element(Pid, Unevictable);
                       false -> Unevictable
                   end,
    {noreply, State #state { callbacks = dict:store
                             (Pid, {Module, Function, Args}, Callbacks),
                             unevictable = Unevictable1
                           }}.

handle_info({'DOWN', _MRef, process, Pid, _Reason},
            State = #state { available_tokens = Avail,
                             mixed_queues = Mixed }) ->
    State1 = case find_queue(Pid, Mixed) of
                 disk ->
                     State;
                 {mixed, {Alloc, _Activity}} ->
                     State #state { available_tokens = Avail + Alloc,
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

add_to_lowrate(Pid, Alloc, Lazy) ->
    Bucket = trunc(math:log(Alloc)), %% log base e
    priority_queue:in({Pid, Bucket, Alloc}, Bucket, Lazy).

find_queue(Pid, Mixed) ->
    case dict:find(Pid, Mixed) of
        {ok, Value} -> {mixed, Value};
        error -> disk
    end.

set_queue_mode(Callbacks, Pid, Mode) ->
    {Module, Function, Args} = dict:fetch(Pid, Callbacks),
    erlang:apply(Module, Function, Args ++ [Mode]).

tidy_and_sum_lazy(IgnorePids, Lazy, Mixed) ->
    tidy_and_sum(lowrate, Mixed,
                 fun (Lazy1) ->
                         case priority_queue:out(Lazy1) of
                             {empty, Lazy2} ->
                                 {empty, Lazy2};
                             {{value, {Pid, _Bucket, _Alloc}}, Lazy2} ->
                                 {{value, Pid}, Lazy2}
                         end
                 end, fun add_to_lowrate/3, IgnorePids, Lazy,
                 priority_queue:new(), 0).
            
tidy_and_sum_sleepy(IgnorePids, Sleepy, Mixed) ->
    tidy_and_sum(hibernate, Mixed, fun queue:out/1,
                 fun (Pid, _Alloc, Queue) -> queue:in(Pid, Queue) end,
                 IgnorePids, Sleepy, queue:new(), 0).

tidy_and_sum(AtomExpected, Mixed, Catamorphism, Anamorphism, DupCheckSet,
             CataInit, AnaInit, AllocAcc) ->
    case Catamorphism(CataInit) of
        {empty, _CataInit} -> {AnaInit, AllocAcc};
        {{value, Pid}, CataInit1} ->
            {DupCheckSet1, AnaInit1, AllocAcc1} =
                case sets:is_element(Pid, DupCheckSet) of
                    true ->
                        {DupCheckSet, AnaInit, AllocAcc};
                    false ->
                        case find_queue(Pid, Mixed) of
                            {mixed, {Alloc, AtomExpected}} ->
                                {sets:add_element(Pid, DupCheckSet),
                                 Anamorphism(Pid, Alloc, AnaInit),
                                 Alloc + AllocAcc};
                            _ ->
                                {DupCheckSet, AnaInit, AllocAcc}
                        end
                end,
            tidy_and_sum(AtomExpected, Mixed, Catamorphism, Anamorphism,
                          DupCheckSet1, CataInit1, AnaInit1, AllocAcc1)
    end.

free_upto_lazy(IgnorePids, Callbacks, Lazy, Mixed, Req) ->
    free_from(
      Callbacks,
      fun(_Mixed, Lazy1, LazyAcc) ->
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
      end, fun priority_queue:join/2, Mixed, Lazy, priority_queue:new(), Req).

free_upto_sleepy(IgnorePids, Callbacks, Sleepy, Mixed, Req) ->
    free_from(Callbacks,
              fun(Mixed1, Sleepy1, SleepyAcc) ->
                      case queue:out(Sleepy1) of
                          {empty, _Sleepy2} ->
                              empty;
                          {{value, Pid}, Sleepy2} ->
                              case sets:is_element(Pid, IgnorePids) of
                                  true  -> {skip, Sleepy2,
                                            queue:in(Pid, SleepyAcc)};
                                  false -> {Alloc, hibernate} =
                                               dict:fetch(Pid, Mixed1),
                                           {value, Sleepy2, Pid, Alloc}
                              end
                      end
              end, fun queue:join/2, Mixed, Sleepy, queue:new(), Req).

free_from(Callbacks, Hylomorphism, BaseCase, Mixed, CataInit, AnaInit, Req) ->
    case Hylomorphism(Mixed, CataInit, AnaInit) of
        empty ->
            {AnaInit, Mixed, Req};
        {skip, CataInit1, AnaInit1} ->
            free_from(Callbacks, Hylomorphism, BaseCase, Mixed, CataInit1,
                      AnaInit1, Req);
        {value, CataInit1, Pid, Alloc} ->
            Mixed1 = dict:erase(Pid, Mixed),
            ok = set_queue_mode(Callbacks, Pid, disk),
            case Req > Alloc of
                true -> free_from(Callbacks, Hylomorphism, BaseCase, Mixed1,
                                  CataInit1, AnaInit, Req - Alloc);
                false -> {BaseCase(CataInit1, AnaInit), Mixed1, Req - Alloc}
            end
    end.

free_upto(Pid, Req, State = #state { available_tokens = Avail,
                                     mixed_queues = Mixed,
                                     callbacks = Callbacks,
                                     lowrate = Lazy,
                                     hibernate = Sleepy,
                                     unevictable = Unevictable })
  when Req > Avail ->
    Unevictable1 = sets:add_element(Pid, Unevictable),
    {Sleepy1, SleepySum} = tidy_and_sum_sleepy(Unevictable1, Sleepy, Mixed),
    case Req > Avail + SleepySum of
        true -> %% not enough in sleepy, have a look in lazy too
            {Lazy1, LazySum} = tidy_and_sum_lazy(Unevictable1, Lazy, Mixed),
            case Req > Avail + SleepySum + LazySum of
                true -> %% can't free enough, just return tidied state
                    State #state { lowrate = Lazy1, hibernate = Sleepy1 };
                false -> %% need to free all of sleepy, and some of lazy
                    {Sleepy2, Mixed1, ReqRem} =
                        free_upto_sleepy(Unevictable1, Callbacks,
                                         Sleepy1, Mixed, Req),
                    {Lazy2, Mixed2, ReqRem1} =
                        free_upto_lazy(Unevictable1, Callbacks,
                                       Lazy1, Mixed1, ReqRem),
                    %% ReqRem1 will be <= 0 because it's
                    %% likely we'll have freed more than we
                    %% need, thus Req - ReqRem1 is total freed
                    State #state { available_tokens = Avail + (Req - ReqRem1),
                                   mixed_queues = Mixed2, lowrate = Lazy2,
                                   hibernate = Sleepy2 }
            end;
        false -> %% enough available in sleepy, don't touch lazy
            {Sleepy2, Mixed1, ReqRem} =
                free_upto_sleepy(Unevictable1, Callbacks, Sleepy1, Mixed, Req),
            State #state { available_tokens = Avail + (Req - ReqRem),
                           mixed_queues = Mixed1, hibernate = Sleepy2 }
    end;
free_upto(_Pid, _Req, State) ->
    State.
