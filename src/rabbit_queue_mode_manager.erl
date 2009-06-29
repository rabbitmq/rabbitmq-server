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
-define(ACTIVITY_THRESHOLD, 25).
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

-record(state, { available_tokens,
                 available_etokens,
                 mixed_queues,
                 tokens_per_byte
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
    {MemTotal, MemUsed, _BigProc} = memsup:get_memory_data(),
    MemAvail = MemTotal - MemUsed,
    Avail = ceil(?TOTAL_TOKENS * (1 - ?LOW_WATER_MARK_FRACTION)),
    EAvail = ?TOTAL_TOKENS - Avail,
    {ok, #state { available_tokens = Avail,
                  available_etokens = EAvail,
                  mixed_queues = dict:new(),
                  tokens_per_byte = ?TOTAL_TOKENS / MemAvail
                }}.

handle_call({register, Pid}, _From,
            State = #state { available_tokens = Avail,
                             mixed_queues = Mixed }) ->
    _MRef = erlang:monitor(process, Pid),
    {Result, State1} =
        case ?INITIAL_TOKEN_ALLOCATION > Avail of
            true ->
                {disk, State};
            false ->
                {mixed, State #state { mixed_queues = dict:store
                                       (Pid, {?INITIAL_TOKEN_ALLOCATION, 0}, Mixed) }}
        end,
    {reply, {ok, Result}, State1}.
                                              
handle_cast(O = {report_memory, Pid, Memory, BytesGained, BytesLost},
            State = #state { available_tokens = Avail,
                             available_etokens = EAvail,
                             tokens_per_byte = TPB,
                             mixed_queues = Mixed
                           }) ->
    Req = ceil(Memory * TPB),
    io:format("~w : ~w  ~w ~n", [Pid, Memory, Req]),
    LowRate = (BytesGained < ?ACTIVITY_THRESHOLD)
        andalso (BytesLost < ?ACTIVITY_THRESHOLD),
    io:format("~w ~w~n", [O, LowRate]),
    State1 =
        case find_queue(Pid, State) of
            disk ->
                case Req > Avail orelse (2*Req) > (Avail + EAvail) orelse
                    LowRate of
                    true -> State; %% remain as disk queue
                    false ->
                        %% go to mixed, allocate double Req, and use Extra
                        rabbit_amqqueue:set_mode(Pid, mixed),
                        Alloc = lists:min([2*Req, Avail]),
                        EAlloc = (2*Req) - Alloc,
                        State #state { available_tokens = Avail - Alloc,
                                       available_etokens = EAvail - EAlloc,
                                       mixed_queues = dict:store
                                       (Pid, {Alloc, EAlloc}, Mixed)
                                      }
                end;
            {mixed, {OAlloc, OEAlloc}} ->
                io:format("~w ; ~w ~w ~n", [Pid, OAlloc, OEAlloc]),
                Avail1 = Avail + OAlloc,
                EAvail1 = EAvail + OEAlloc,
                case Req > (OAlloc + OEAlloc) of
                    true -> %% getting bigger
                        case Req > Avail1 of
                            true -> %% go to disk
                                rabbit_amqqueue:set_mode(Pid, disk),
                                State #state { available_tokens = Avail1,
                                               available_etokens = EAvail1,
                                               mixed_queues =
                                               dict:erase(Pid, Mixed) };
                            false -> %% request not too big, stay mixed
                                State #state { available_tokens = Avail1 - Req,
                                               available_etokens = EAvail1,
                                               mixed_queues = dict:store
                                               (Pid, {Req, 0}, Mixed) }
                        end;
                    false -> %% getting smaller (or staying same)
                        case 0 =:= OEAlloc of
                            true ->
                                case Req > Avail1 orelse LowRate of
                                    true -> %% go to disk
                                        rabbit_amqqueue:set_mode(Pid, disk),
                                        State #state { available_tokens = Avail1,
                                                       available_etokens = EAvail1,
                                                       mixed_queues =
                                                       dict:erase(Pid, Mixed) };
                                    false -> %% request not too big, stay mixed
                                        State #state { available_tokens = Avail1 - Req,
                                                       available_etokens = EAvail1,
                                                       mixed_queues = dict:store
                                                       (Pid, {Req, 0}, Mixed) }
                                end;
                            false ->
                                case Req > Avail1 of
                                    true ->
                                        EReq = Req - Avail1,
                                        case EReq > EAvail1 of
                                            true ->  %% go to disk
                                                rabbit_amqqueue:set_mode(Pid, disk),
                                                State #state { available_tokens = Avail1,
                                                               available_etokens = EAvail1,
                                                               mixed_queues =
                                                               dict:erase(Pid, Mixed) };
                                            false -> %% request not too big, stay mixed
                                                State #state { available_tokens = 0,
                                                               available_etokens = EAvail1 - EReq,
                                                               mixed_queues = dict:store
                                                               (Pid, {Avail1, EReq}, Mixed) }
                                        end;
                                    false -> %% request not too big, stay mixed
                                        State #state { available_tokens = Avail1 - Req,
                                                       available_etokens = EAvail1,
                                                       mixed_queues = dict:store
                                                       (Pid, {Req, 0}, Mixed) }
                                end
                        end
                end
        end,
    {noreply, State1}.

handle_info({'DOWN', _MRef, process, Pid, _Reason},
            State = #state { available_tokens = Avail,
                             available_etokens = EAvail,
                             mixed_queues = Mixed }) ->
    State1 = case find_queue(Pid, State) of
                 disk ->
                     State;
                 {mixed, {Alloc, EAlloc}} ->
                     State #state { available_tokens = Avail + Alloc,
                                    available_etokens = EAvail + EAlloc,
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

find_queue(Pid, #state { mixed_queues = Mixed }) ->
    case dict:find(Pid, Mixed) of
        {ok, Value} -> {mixed, Value};
        error -> disk
    end.

ceil(N) when N - trunc(N) > 0 ->
    1 + trunc(N);
ceil(N) ->
    N.
