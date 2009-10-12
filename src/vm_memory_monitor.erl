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

-module(vm_memory_monitor).

-behaviour(gen_server2).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([update/0, get_total_memory/1, 
        get_check_interval/0, set_check_interval/1,
        get_vm_memory_high_watermark/0, set_vm_memory_high_watermark/1]).


-define(SERVER, ?MODULE).
-define(DEFAULT_MEMORY_CHECK_INTERVAL, 1000).

%% For unknown OS, we assume that we have 512 MB of memory, which is pretty 
%% safe value, even for 32 bit systems. It's better to be slow than to crash.
-define(MEMORY_SIZE_FOR_UNKNOWN_OS, 512*1024*1024).

-record(state, {total_memory,
                memory_limit,
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
%% get_total_memory(OS) -> Total
%% Windows and Freebsd code based on: memsup:get_memory_usage/1
%% Original code was part of OTP and released under "Erlang Public License".

%% Darwin: Uses vm_stat command.
get_total_memory({unix,darwin}) ->
    File = os:cmd("/usr/bin/vm_stat"),
    Lines = string:tokens(File, "\n"),
    Dict = dict:from_list(lists:map(fun parse_line_mach/1, Lines)),
    [PageSize, Inactive, Active, Free, Wired] =
        [dict:fetch(Key, Dict) ||
            Key <- [page_size, 'Pages inactive', 'Pages active', 'Pages free',
                    'Pages wired down']],
    MemTotal = PageSize * (Inactive + Active + Free + Wired),
    MemTotal;

%% FreeBSD: Look in /usr/include/sys/vmmeter.h for the format of struct
%% vmmeter
get_total_memory({unix,freebsd}) ->
    PageSize  = freebsd_sysctl("vm.stats.vm.v_page_size"),
    PageCount = freebsd_sysctl("vm.stats.vm.v_page_count"),
    NMemTotal = PageCount * PageSize,
    NMemTotal;

%% Win32: Find out how much memory is in use by asking
%% the os_mon_sysinfo process.
get_total_memory({win32,_OSname}) ->
    [Result|_] = os_mon_sysinfo:get_mem_info(),
    {ok, [_MemLoad, TotPhys, _AvailPhys,
          _TotPage, _AvailPage, _TotV, _AvailV], _RestStr} =
        io_lib:fread("~d~d~d~d~d~d~d", Result),
    TotPhys;

%% Linux: Look in /proc/meminfo
get_total_memory({unix, linux}) ->
    File = read_proc_file("/proc/meminfo"),
    Lines = string:tokens(File, "\n"),
    Dict = dict:from_list(lists:map(fun parse_line_linux/1, Lines)),
    MemTotal = dict:fetch('MemTotal', Dict),
    MemTotal;

get_total_memory(_OsType) ->
    unknown.

%% A line looks like "Foo bar: 123456."
parse_line_mach(Line) ->
    [Name, RHS | _Rest] = string:tokens(Line, ":"),
    case Name of
        "Mach Virtual Memory Statistics" ->
            ["(page", "size", "of", PageSize, "bytes)"] =
                string:tokens(RHS, " "),
            {page_size, list_to_integer(PageSize)};
        _ ->
            [Value | _Rest1] = string:tokens(RHS, " ."),
            {list_to_atom(Name), list_to_integer(Value)}
    end.

freebsd_sysctl(Def) ->
    list_to_integer(os:cmd("/sbin/sysctl -n " ++ Def) -- "\n").

%% file:read_file does not work on files in /proc as it seems to get
%% the size of the file first and then read that many bytes. But files
%% in /proc always have length 0, we just have to read until we get
%% eof.
read_proc_file(File) ->
    {ok, IoDevice} = file:open(File, [read, raw]),
    Res = read_proc_file(IoDevice, []),
    file:close(IoDevice),
    lists:flatten(lists:reverse(Res)).

-define(BUFFER_SIZE, 1024).
read_proc_file(IoDevice, Acc) ->
    case file:read(IoDevice, ?BUFFER_SIZE) of
        {ok, Res} -> read_proc_file(IoDevice, [Res | Acc]);
        eof       -> Acc
    end.

%% A line looks like "FooBar: 123456 kB"
parse_line_linux(Line) ->
    [Name, RHS | _Rest] = string:tokens(Line, ":"),
    [Value | UnitsRest] = string:tokens(RHS, " "),
    Value1 = case UnitsRest of
                 [] -> list_to_integer(Value); %% no units
                 ["kB"] -> list_to_integer(Value) * 1024
             end,
    {list_to_atom(Name), Value1}.



%%----------------------------------------------------------------------------

%% On a 32-bit machine, if you're using more than 2 gigs of RAM
%% you're in big trouble anyway.
get_vm_limit() ->
    case erlang:system_info(wordsize) of
        4 -> 2*1024*1024*1024;                  % 2 GiB for 32 bits
        8 -> 64*1024*1024*1024*1024             % 64 TiB for 64 bits
    end.


get_mem_limit(MemFraction, TotalMemory) ->
    lists:min([erlang:trunc(TotalMemory * MemFraction),
                get_vm_limit()]).

init([MemFraction]) -> 
    TotalMemory = case get_total_memory(os:type()) of
        unknown ->
            rabbit_log:info("Unknown total memory size for your OS ~p. "
                        "Assuming memory size is ~p bytes.~n", 
                            [os:type(), ?MEMORY_SIZE_FOR_UNKNOWN_OS]),
            ?MEMORY_SIZE_FOR_UNKNOWN_OS;
        M -> M
    end,
    MemLimit = get_mem_limit(MemFraction, TotalMemory),
    rabbit_log:info("Memory limit set to ~pMiB.~n", 
                                              [erlang:trunc(MemLimit/1048576)]),
    TRef = start_timer(?DEFAULT_MEMORY_CHECK_INTERVAL),
    State = #state { total_memory = TotalMemory,
                     memory_limit = MemLimit,
                     timeout = ?DEFAULT_MEMORY_CHECK_INTERVAL,
                     timer = TRef,
                     alarmed = false},
    {ok, internal_update(State)}.

start_timer(Timeout) ->
    {ok, TRef} = timer:apply_interval(Timeout, ?MODULE, update, []),
    TRef.


get_check_interval() ->
    gen_server2:call(?MODULE, get_check_interval).

set_check_interval(Fraction) ->
    gen_server2:call(?MODULE, {set_check_interval, Fraction}).

get_vm_memory_high_watermark() ->
    gen_server2:call(?MODULE, get_vm_memory_high_watermark).

set_vm_memory_high_watermark(Fraction) ->
    gen_server2:call(?MODULE, {set_vm_memory_high_watermark, Fraction}).


handle_call(get_vm_memory_high_watermark, _From, State) ->
    {reply, State#state.memory_limit / State#state.total_memory, State};

handle_call({set_vm_memory_high_watermark, MemFraction}, _From, State) ->
    MemLimit = get_mem_limit(MemFraction, State#state.total_memory),
    rabbit_log:info("Memory alarm changed to ~p, ~p bytes.~n", 
                                                      [MemFraction, MemLimit]),
    {reply, ok, State#state{memory_limit = MemLimit}};

handle_call(get_check_interval, _From, State) ->
    {reply, State#state.timeout, State};

handle_call({set_check_interval, Timeout}, _From, State) ->
    {ok, cancel} = timer:cancel(State#state.timer),
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
    rabbit_log:info("vm_memory_high_watermark ~p. Memory used:~p allowed:~p~n",
                       [State, MemUsed, MemLimit]).

internal_update(State = #state { memory_limit = MemLimit,
                                 alarmed = Alarmed}) ->
    MemUsed = erlang:memory(total),
    NewAlarmed = MemUsed > MemLimit,
    case {Alarmed, NewAlarmed} of
        {false, true} ->
            emit_update_info(set, MemUsed, MemLimit),
            alarm_handler:set_alarm({vm_memory_high_watermark, []});
        {true, false} ->
            emit_update_info(clear, MemUsed, MemLimit),
            alarm_handler:clear_alarm(vm_memory_high_watermark);
        _ ->
            ok
    end,    
    State #state {alarmed = NewAlarmed}.

