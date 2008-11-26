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
%%   The Initial Developers of the Original Code are LShift Ltd.,
%%   Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd., Cohesive Financial Technologies
%%   LLC., and Rabbit Technologies Ltd. are Copyright (C) 2007-2008
%%   LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit
%%   Technologies Ltd.;
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_linux_memory).

-behaviour(gen_server).

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([update/0]).

-define(SERVER, ?MODULE).

-define(MEMORY_CHECK_INTERVAL, 1000).

-record(state, {memory_fraction, alarmed}).             

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).


init(_Args) -> 
    Fraction = os_mon:get_env(memsup, system_memory_high_watermark),
    {ok, _Tref} = timer:apply_interval(?MEMORY_CHECK_INTERVAL, ?MODULE, update, []),
    {ok, #state{alarmed = false, memory_fraction = Fraction}, ?MEMORY_CHECK_INTERVAL}.

update() ->
    gen_server:cast(?SERVER, do_update).

%% Export the same API as the real memsup. Note that get_sysmem_high_watermark
%% gives an int in the range 0 - 100, while set_sysmem_high_watermark 
%% takes a float in the range 0.0 - 1.0.
handle_call(get_sysmem_high_watermark, _From, State) ->
    {reply, trunc(100 * State#state.memory_fraction), State};

handle_call({set_sysmem_high_watermark, Float}, _From, State) ->
    {reply, ok, State#state{memory_fraction=Float}};

handle_call(_Request, _From, State) -> 
    {noreply, State}.


handle_cast(do_update, State = #state{alarmed = Alarmed, memory_fraction = MemoryFraction}) -> 
    File = read_proc_file("/proc/meminfo"),
    Lines = string:tokens(File, "\n"),
    Dict = dict:from_list(lists:map(fun parse_line/1, Lines)),
    MemTotal = dict:fetch('MemTotal', Dict),
    MemUsed = MemTotal 
            - dict:fetch('MemFree', Dict)
            - dict:fetch('Buffers', Dict)
            - dict:fetch('Cached', Dict),
    NewAlarmed = MemUsed / MemTotal > MemoryFraction,
    case {Alarmed, NewAlarmed} of
        {false, true} ->
            alarm_handler:set_alarm({system_memory_high_watermark, []}),
            ok;
        {true, false} ->
            alarm_handler:clear_alarm(system_memory_high_watermark),
            ok;
        _ ->
            ok
    end,
    {noreply,  State#state{alarmed = NewAlarmed}};

handle_cast(_Request, State) -> 
    {noreply, State}.

handle_info(_Info, State) -> 
    {noreply, State}.


-define(BUFFER_SIZE, 1024).

%% file:read_file does not work on files in /proc as it seems to get the size
%% of the file first and then read that many bytes. But files in /proc always
%% have length 0, we just have to read until we get eof.
read_proc_file(File) ->
    {ok, IoDevice} = file:open(File, [read, raw]),
    read_proc_file(IoDevice, []).
    
read_proc_file(IoDevice, Acc) ->
    case file:read(IoDevice, ?BUFFER_SIZE) of
        {ok, Res} ->
            read_proc_file(IoDevice, Acc ++ Res);
        eof ->
            Acc
    end.

%% A line looks like "FooBar: 123456 kB"
parse_line(Line) ->
    [NameS, ValueS | _] = string:tokens(Line, ": "),   
    Name = list_to_atom(NameS),
    Value = list_to_integer(string:sub_word(ValueS, 1)),
    {Name, Value}.


terminate(_Reason, _State) -> 
    ok.


code_change(_OldVsn, State, _Extra) -> 
    {ok, State}.
