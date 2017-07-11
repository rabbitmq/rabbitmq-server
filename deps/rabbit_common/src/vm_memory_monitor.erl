%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

%% In practice Erlang shouldn't be allowed to grow to more than a half
%% of available memory. The pessimistic scenario is when the Erlang VM
%% has a single process that's consuming all memory. In such a case,
%% during garbage collection, Erlang tries to allocate a huge chunk of
%% continuous memory, which can result in a crash or heavy swapping.
%%
%% This module tries to warn Rabbit before such situations occur, so
%% that it has a higher chance to avoid running out of memory.

-module(vm_memory_monitor).

-behaviour(gen_server).

-export([start_link/1, start_link/3]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([get_total_memory/0, get_vm_limit/0,
         get_check_interval/0, set_check_interval/1,
         get_vm_memory_high_watermark/0, set_vm_memory_high_watermark/1,
         get_memory_limit/0, get_memory_use/1,
         get_process_memory/0, get_memory_calculation_strategy/0]).

%% for tests
-export([parse_line_linux/1, parse_mem_limit/1]).

-define(SERVER, ?MODULE).

-record(state, {total_memory,
                memory_limit,
                memory_config_limit,
                timeout,
                timer,
                alarmed,
                alarm_funs
               }).

-include("rabbit_memory.hrl").

%%----------------------------------------------------------------------------

-type vm_memory_high_watermark() :: (float() | {'absolute', integer() | string()}).
-spec start_link(float()) -> rabbit_types:ok_pid_or_error().
-spec start_link(float(), fun ((any()) -> 'ok'),
                       fun ((any()) -> 'ok')) -> rabbit_types:ok_pid_or_error().
-spec get_total_memory() -> (non_neg_integer() | 'unknown').
-spec get_vm_limit() -> non_neg_integer().
-spec get_check_interval() -> non_neg_integer().
-spec set_check_interval(non_neg_integer()) -> 'ok'.
-spec get_vm_memory_high_watermark() -> vm_memory_high_watermark().
-spec set_vm_memory_high_watermark(vm_memory_high_watermark()) -> 'ok'.
-spec get_memory_limit() -> non_neg_integer().
-spec get_memory_use(bytes) -> {non_neg_integer(),  float() | infinity};
                    (ratio) -> float() | infinity.

%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------

get_total_memory() ->
    case application:get_env(rabbit, total_memory_available_override_value) of
        {ok, Value} ->
            case rabbit_resource_monitor_misc:parse_information_unit(Value) of
                {ok, ParsedTotal} ->
                    ParsedTotal;
                {error, parse_error} ->
                    rabbit_log:warning(
                      "The override value for the total memmory available is "
                      "not a valid value: ~p, getting total from the system.~n",
                      [Value]),
                    get_total_memory_from_os()
            end;
        undefined ->
            get_total_memory_from_os()
    end.

get_vm_limit() -> get_vm_limit(os:type()).

get_check_interval() ->
    gen_server:call(?MODULE, get_check_interval, infinity).

set_check_interval(Fraction) ->
    gen_server:call(?MODULE, {set_check_interval, Fraction}, infinity).

get_vm_memory_high_watermark() ->
    gen_server:call(?MODULE, get_vm_memory_high_watermark, infinity).

set_vm_memory_high_watermark(Fraction) ->
    gen_server:call(?MODULE, {set_vm_memory_high_watermark, Fraction},
                    infinity).

get_memory_limit() ->
    gen_server:call(?MODULE, get_memory_limit, infinity).

get_memory_use(bytes) ->
    MemoryLimit = get_memory_limit(),
    {get_process_memory(), case MemoryLimit > 0.0 of
                               true  -> MemoryLimit;
                               false -> infinity
                           end};
get_memory_use(ratio) ->
    MemoryLimit = get_memory_limit(),
    case MemoryLimit > 0.0 of
        true  -> get_process_memory() / MemoryLimit;
        false -> infinity
    end.

%% Memory reported by erlang:memory(total) is not supposed to
%% be equal to the total size of all pages mapped to the emulator,
%% according to http://erlang.org/doc/man/erlang.html#memory-0
%% erlang:memory(total) under-reports memory usage by around 20%
-spec get_process_memory() -> Bytes :: integer().
get_process_memory() ->
    case get_memory_calculation_strategy() of
        rss ->
            case get_system_process_resident_memory() of
                {ok, MemInBytes} ->
                    MemInBytes;
                {error, Reason}  ->
                    rabbit_log:debug("Unable to get system memory used. Reason: ~p."
                                     " Falling back to erlang memory reporting",
                                     [Reason]),
                    erlang:memory(total)
            end;
        erlang ->
            erlang:memory(total)
    end.

-spec get_memory_calculation_strategy() -> rss | erlang.
get_memory_calculation_strategy() ->
    case rabbit_misc:get_env(rabbit, vm_memory_calculation_strategy, rss) of
        erlang ->
            erlang;
        rss ->
            rss;
        UnsupportedValue ->
            rabbit_log:warning(
              "Unsupported value '~p' for vm_memory_calculation_strategy. "
              "Supported values: (rss|erlang). "
              "Defaulting to 'rss'",
              [UnsupportedValue]
            ),
            rss
    end.

-spec get_system_process_resident_memory() -> {ok, Bytes :: integer()} | {error, term()}.
get_system_process_resident_memory() ->
    try
        get_system_process_resident_memory(os:type())
    catch _:Error ->
            {error, {"Failed to get process resident memory", Error}}
    end.

get_system_process_resident_memory({unix,darwin}) ->
    get_ps_memory();

get_system_process_resident_memory({unix, linux}) ->
    get_ps_memory();

get_system_process_resident_memory({unix,freebsd}) ->
    get_ps_memory();

get_system_process_resident_memory({unix,openbsd}) ->
    get_ps_memory();

get_system_process_resident_memory({win32,_OSname}) ->
    OsPid = os:getpid(),
    Cmd = "wmic process where processid=" ++ OsPid ++ " get WorkingSetSize /value 2>&1",
    CmdOutput = os:cmd(Cmd),
    %% Memory usage is displayed in bytes
    case re:run(CmdOutput, "WorkingSetSize=([0-9]+)", [{capture, all_but_first, binary}]) of
        {match, [Match]} ->
            {ok, binary_to_integer(Match)};
        _ ->
            {error, {unexpected_output_from_command, Cmd, CmdOutput}}
    end;

get_system_process_resident_memory({unix, sunos}) ->
    get_ps_memory();

get_system_process_resident_memory({unix, aix}) ->
    get_ps_memory();

get_system_process_resident_memory(_OsType) ->
    {error, not_implemented_for_os}.

get_ps_memory() ->
    OsPid = os:getpid(),
    Cmd = "ps -p " ++ OsPid ++ " -o rss=",
    CmdOutput = os:cmd(Cmd),
    case re:run(CmdOutput, "[0-9]+", [{capture, first, list}]) of
        {match, [Match]} ->
            {ok, list_to_integer(Match) * 1024};
        _ ->
            {error, {unexpected_output_from_command, Cmd, CmdOutput}}
    end.

%%----------------------------------------------------------------------------
%% gen_server callbacks
%%----------------------------------------------------------------------------

start_link(MemFraction) ->
    start_link(MemFraction,
               fun alarm_handler:set_alarm/1, fun alarm_handler:clear_alarm/1).

start_link(MemFraction, AlarmSet, AlarmClear) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE,
                          [MemFraction, {AlarmSet, AlarmClear}], []).

init([MemFraction, AlarmFuns]) ->
    TRef = start_timer(?DEFAULT_MEMORY_CHECK_INTERVAL),
    State = #state { timeout    = ?DEFAULT_MEMORY_CHECK_INTERVAL,
                     timer      = TRef,
                     alarmed    = false,
                     alarm_funs = AlarmFuns },
    {ok, set_mem_limits(State, MemFraction)}.

handle_call(get_vm_memory_high_watermark, _From,
            #state{memory_config_limit = MemLimit} = State) ->
    {reply, MemLimit, State};

handle_call({set_vm_memory_high_watermark, MemLimit}, _From, State) ->
    {reply, ok, set_mem_limits(State, MemLimit)};

handle_call(get_check_interval, _From, State) ->
    {reply, State#state.timeout, State};

handle_call({set_check_interval, Timeout}, _From, State) ->
    {ok, cancel} = timer:cancel(State#state.timer),
    {reply, ok, State#state{timeout = Timeout, timer = start_timer(Timeout)}};

handle_call(get_memory_limit, _From, State) ->
    {reply, State#state.memory_limit, State};

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(update, State) ->
    {noreply, internal_update(State)};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------
%% Server Internals
%%----------------------------------------------------------------------------
get_total_memory_from_os() ->
    try
        get_total_memory(os:type())
    catch _:Error ->
            rabbit_log:warning(
              "Failed to get total system memory: ~n~p~n~p~n",
              [Error, erlang:get_stacktrace()]),
            unknown
    end.

set_mem_limits(State, MemLimit) ->
    case erlang:system_info(wordsize) of
        4 ->
            rabbit_log:warning(
              "You are using a 32-bit version of Erlang: you may run into "
              "memory address~n"
              "space exhaustion or statistic counters overflow.~n");
        _ ->
            ok
    end,
    TotalMemory =
        case get_total_memory() of
            unknown ->
                case State of
                    #state { total_memory = undefined,
                             memory_limit = undefined } ->
                        rabbit_log:warning(
                          "Unknown total memory size for your OS ~p. "
                          "Assuming memory size is ~p MiB (~p bytes).~n",
                          [os:type(),
                           trunc(?MEMORY_SIZE_FOR_UNKNOWN_OS/?ONE_MiB),
                           ?MEMORY_SIZE_FOR_UNKNOWN_OS]);
                    _ ->
                        ok
                end,
                ?MEMORY_SIZE_FOR_UNKNOWN_OS;
            Memory -> Memory
        end,
    UsableMemory =
        case get_vm_limit() of
            Limit when Limit < TotalMemory ->
                rabbit_log:warning(
                  "Only ~p MiB (~p bytes) of ~p MiB (~p bytes) memory usable due to "
                  "limited address space.~n"
                  "Crashes due to memory exhaustion are possible - see~n"
                  "http://www.rabbitmq.com/memory.html#address-space~n",
                  [trunc(Limit/?ONE_MiB), Limit, trunc(TotalMemory/?ONE_MiB),
                   TotalMemory]),
                Limit;
            _ ->
                TotalMemory
        end,
    MemLim = interpret_limit(parse_mem_limit(MemLimit), UsableMemory),
    rabbit_log:info(
        "Memory high watermark set to ~p MiB (~p bytes)"
        " of ~p MiB (~p bytes) total~n",
        [trunc(MemLim/?ONE_MiB), MemLim,
         trunc(TotalMemory/?ONE_MiB), TotalMemory]
    ),
    internal_update(State #state { total_memory    = TotalMemory,
                                   memory_limit    = MemLim,
                                   memory_config_limit = MemLimit}).

interpret_limit({'absolute', MemLim}, UsableMemory) ->
    erlang:min(MemLim, UsableMemory);
interpret_limit(MemFraction, UsableMemory) ->
    trunc(MemFraction * UsableMemory).

parse_mem_limit({absolute, Limit}) ->
    case rabbit_resource_monitor_misc:parse_information_unit(Limit) of
        {ok, ParsedLimit} -> {absolute, ParsedLimit};
        {error, parse_error} ->
            rabbit_log:error("Unable to parse vm_memory_high_watermark value ~p", [Limit]),
            ?DEFAULT_VM_MEMORY_HIGH_WATERMARK
    end;
parse_mem_limit(MemLimit) when is_integer(MemLimit) ->
    parse_mem_limit(float(MemLimit));
parse_mem_limit(MemLimit) when is_float(MemLimit), MemLimit =< ?MAX_VM_MEMORY_HIGH_WATERMARK ->
    MemLimit;
parse_mem_limit(MemLimit) when is_float(MemLimit), MemLimit > ?MAX_VM_MEMORY_HIGH_WATERMARK ->
    rabbit_log:warning(
      "Memory high watermark of ~p is above the allowed maximum, falling back to ~p~n",
      [MemLimit, ?MAX_VM_MEMORY_HIGH_WATERMARK]
    ),
    ?MAX_VM_MEMORY_HIGH_WATERMARK;
parse_mem_limit(MemLimit) ->
    rabbit_log:warning(
      "Memory high watermark of ~p is invalid, defaulting to ~p~n",
      [MemLimit, ?DEFAULT_VM_MEMORY_HIGH_WATERMARK]
    ),
    ?DEFAULT_VM_MEMORY_HIGH_WATERMARK.

internal_update(State = #state { memory_limit = MemLimit,
                                 alarmed      = Alarmed,
                                 alarm_funs   = {AlarmSet, AlarmClear} }) ->
    MemUsed = get_process_memory(),
    NewAlarmed = MemUsed > MemLimit,
    case {Alarmed, NewAlarmed} of
        {false, true} -> emit_update_info(set, MemUsed, MemLimit),
                         AlarmSet({{resource_limit, memory, node()}, []});
        {true, false} -> emit_update_info(clear, MemUsed, MemLimit),
                         AlarmClear({resource_limit, memory, node()});
        _             -> ok
    end,
    State #state {alarmed = NewAlarmed}.

emit_update_info(AlarmState, MemUsed, MemLimit) ->
    rabbit_log:info(
      "vm_memory_high_watermark ~p. Memory used:~p allowed:~p~n",
      [AlarmState, MemUsed, MemLimit]).

start_timer(Timeout) ->
    {ok, TRef} = timer:send_interval(Timeout, update),
    TRef.

%% According to http://msdn.microsoft.com/en-us/library/aa366778(VS.85).aspx
%% Windows has 2GB and 8TB of address space for 32 and 64 bit accordingly.
get_vm_limit({win32,_OSname}) ->
    case erlang:system_info(wordsize) of
        4 -> 2*1024*1024*1024;          %% 2 GB for 32 bits  2^31
        8 -> 8*1024*1024*1024*1024      %% 8 TB for 64 bits  2^42
    end;

%% On a 32-bit machine, if you're using more than 2 gigs of RAM you're
%% in big trouble anyway.
get_vm_limit(_OsType) ->
    case erlang:system_info(wordsize) of
        4 -> 2*1024*1024*1024;          %% 2 GB for 32 bits  2^31
        8 -> 256*1024*1024*1024*1024    %% 256 TB for 64 bits 2^48
             %%http://en.wikipedia.org/wiki/X86-64#Virtual_address_space_details
    end.

%%----------------------------------------------------------------------------
%% Internal Helpers
%%----------------------------------------------------------------------------
cmd(Command) ->
    Exec = hd(string:tokens(Command, " ")),
    case os:find_executable(Exec) of
        false -> throw({command_not_found, Exec});
        _     -> os:cmd(Command)
    end.

%% get_total_memory(OS) -> Total
%% Windows and Freebsd code based on: memsup:get_memory_usage/1
%% Original code was part of OTP and released under "Erlang Public License".

get_total_memory({unix,darwin}) ->
    sysctl("hw.memsize");

get_total_memory({unix,freebsd}) ->
    PageSize  = sysctl("vm.stats.vm.v_page_size"),
    PageCount = sysctl("vm.stats.vm.v_page_count"),
    PageCount * PageSize;

get_total_memory({unix,openbsd}) ->
    sysctl("hw.usermem");

get_total_memory({win32,_OSname}) ->
    [Result|_] = os_mon_sysinfo:get_mem_info(),
    {ok, [_MemLoad, TotPhys, _AvailPhys, _TotPage, _AvailPage, _TotV, _AvailV],
     _RestStr} =
        io_lib:fread("~d~d~d~d~d~d~d", Result),
    TotPhys;

get_total_memory({unix, linux}) ->
    File = read_proc_file("/proc/meminfo"),
    Lines = string:tokens(File, "\n"),
    Dict = dict:from_list(lists:map(fun parse_line_linux/1, Lines)),
    dict:fetch('MemTotal', Dict);

get_total_memory({unix, sunos}) ->
    File = cmd("/usr/sbin/prtconf"),
    Lines = string:tokens(File, "\n"),
    Dict = dict:from_list(lists:map(fun parse_line_sunos/1, Lines)),
    dict:fetch('Memory size', Dict);

get_total_memory({unix, aix}) ->
    File = cmd("/usr/bin/vmstat -v"),
    Lines = string:tokens(File, "\n"),
    Dict = dict:from_list(lists:map(fun parse_line_aix/1, Lines)),
    dict:fetch('memory pages', Dict) * 4096;

get_total_memory(_OsType) ->
    unknown.

%% A line looks like "MemTotal:         502968 kB"
%% or (with broken OS/modules) "Readahead      123456 kB"
parse_line_linux(Line) ->
    {Name, Value, UnitRest} =
        case string:tokens(Line, ":") of
            %% no colon in the line
            [S] ->
                [K, RHS] = re:split(S, "\s", [{parts, 2}, {return, list}]),
                [V | Unit] = string:tokens(RHS, " "),
                {K, V, Unit};
            [K, RHS | _Rest] ->
                [V | Unit] = string:tokens(RHS, " "),
                {K, V, Unit}
        end,
    Value1 = case UnitRest of
        []     -> list_to_integer(Value); %% no units
        ["kB"] -> list_to_integer(Value) * 1024
    end,
    {list_to_atom(Name), Value1}.

%% A line looks like "Memory size: 1024 Megabytes"
parse_line_sunos(Line) ->
    case string:tokens(Line, ":") of
        [Name, RHS | _Rest] ->
            [Value1 | UnitsRest] = string:tokens(RHS, " "),
            Value2 = case UnitsRest of
                         ["Gigabytes"] ->
                             list_to_integer(Value1) * ?ONE_MiB * 1024;
                         ["Megabytes"] ->
                             list_to_integer(Value1) * ?ONE_MiB;
                         ["Kilobytes"] ->
                             list_to_integer(Value1) * 1024;
                         _ ->
                             Value1 ++ UnitsRest %% no known units
                     end,
            {list_to_atom(Name), Value2};
        [Name] -> {list_to_atom(Name), none}
    end.

%% Lines look like " 12345 memory pages"
%% or              "  80.1 maxpin percentage"
parse_line_aix(Line) ->
    [Value | NameWords] = string:tokens(Line, " "),
    Name = string:join(NameWords, " "),
    {list_to_atom(Name),
     case lists:member($., Value) of
         true  -> trunc(list_to_float(Value));
         false -> list_to_integer(Value)
     end}.

sysctl(Def) ->
    list_to_integer(cmd("/usr/bin/env sysctl -n " ++ Def) -- "\n").

%% file:read_file does not work on files in /proc as it seems to get
%% the size of the file first and then read that many bytes. But files
%% in /proc always have length 0, we just have to read until we get
%% eof.
read_proc_file(File) ->
    {ok, IoDevice} = file:open(File, [read, raw]),
    Res = read_proc_file(IoDevice, []),
    _ = file:close(IoDevice),
    lists:flatten(lists:reverse(Res)).

-define(BUFFER_SIZE, 1024).
read_proc_file(IoDevice, Acc) ->
    case file:read(IoDevice, ?BUFFER_SIZE) of
        {ok, Res} -> read_proc_file(IoDevice, [Res | Acc]);
        eof       -> Acc
    end.
