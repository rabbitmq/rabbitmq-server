%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
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
         get_memory_limit/0,
         %% TODO: refactor in master
         get_memory_use/1,
         get_process_memory/0,
         get_process_memory/1,
         get_memory_calculation_strategy/0,
         get_rss_memory/0,
         interpret_limit/2
        ]).

%% for tests
-export([parse_line_linux/1, parse_mem_limit/1]).

-define(SERVER, ?MODULE).

-record(state, {total_memory,
                memory_limit,
                process_memory,
                memory_config_limit,
                timeout,
                timer,
                alarmed,
                alarm_funs,
                os_type = undefined,
                os_pid  = undefined,
                page_size = undefined,
                proc_file = undefined}).

-include("rabbit_memory.hrl").

%%----------------------------------------------------------------------------

-type memory_calculation_strategy() :: rss | erlang | allocated.
-type vm_memory_high_watermark() :: float() | {'absolute', integer() | string()} | {'relative', float() | integer() | string()}.
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
-spec get_cached_process_memory_and_limit() -> {non_neg_integer(),
                                                float() | infinity}.
-spec get_rss_memory() -> non_neg_integer().

-export_type([memory_calculation_strategy/0]).

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
                    _ = rabbit_log:warning(
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
    {ProcessMemory, MemoryLimit} = get_cached_process_memory_and_limit(),
    {ProcessMemory, case MemoryLimit > 0.0 of
                        true  -> MemoryLimit;
                        false -> infinity
                    end};
get_memory_use(ratio) ->
    {ProcessMemory, MemoryLimit} = get_cached_process_memory_and_limit(),
    case MemoryLimit of
        infinity -> 0.0;
        Num when is_number(Num) andalso Num > 0.0 ->
            ProcessMemory / MemoryLimit;
        _        -> infinity
    end.

%% Memory reported by erlang:memory(total) is not supposed to
%% be equal to the total size of all pages mapped to the emulator,
%% according to http://erlang.org/doc/man/erlang.html#memory-0
%% erlang:memory(total) under-reports memory usage by around 20%
%%
%% Win32 Note: 3.6.12 shipped with code that used wmic.exe to get the
%% WorkingSetSize value for the running erl.exe process. Unfortunately
%% even with a moderate invocation rate of 1 ops/second that uses more
%% CPU resources than some Windows users are willing to tolerate.
%% See rabbitmq/rabbitmq-server#1343 and rabbitmq/rabbitmq-common#224
%% for details.
-spec get_process_memory() -> Bytes :: integer().
get_process_memory() ->
    {ProcMem, _} = get_memory_use(bytes),
    ProcMem.

-spec get_process_memory(cached | current) -> Bytes :: integer().
get_process_memory(cached) ->
    {ProcMem, _} = get_memory_use(bytes),
    ProcMem;
get_process_memory(current) ->
    get_process_memory_uncached().

-spec get_memory_calculation_strategy() -> memory_calculation_strategy().
get_memory_calculation_strategy() ->
    case rabbit_misc:get_env(rabbit, vm_memory_calculation_strategy, rss) of
        allocated -> allocated;
        erlang -> erlang;
        legacy -> erlang; %% backwards compatibility
        rss -> rss;
        UnsupportedValue ->
            _ = rabbit_log:warning(
              "Unsupported value '~p' for vm_memory_calculation_strategy. "
              "Supported values: (allocated|erlang|legacy|rss). "
              "Defaulting to 'rss'",
              [UnsupportedValue]
            ),
            rss
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
    TRef = erlang:send_after(?DEFAULT_MEMORY_CHECK_INTERVAL, self(), update),
    State0 = #state{timeout    = ?DEFAULT_MEMORY_CHECK_INTERVAL,
                    timer      = TRef,
                    alarmed    = false,
                    alarm_funs = AlarmFuns},
    State1 = update_process_memory(init_state_by_os(State0)),
    {ok, set_mem_limits(State1, MemFraction)}.

handle_call(get_vm_memory_high_watermark, _From,
            #state{memory_config_limit = MemLimit} = State) ->
    {reply, MemLimit, State};

handle_call({set_vm_memory_high_watermark, MemLimit}, _From, State) ->
    {reply, ok, set_mem_limits(State, MemLimit)};

handle_call(get_check_interval, _From, State) ->
    {reply, State#state.timeout, State};

handle_call({set_check_interval, Timeout}, _From, State) ->
    State1 = case erlang:cancel_timer(State#state.timer) of
        false ->
            State#state{timeout = Timeout};
        _ ->
            State#state{timeout = Timeout,
                        timer = erlang:send_after(Timeout, self(), update)}
    end,
    {reply, ok, State1};

handle_call(get_memory_limit, _From, State) ->
    {reply, State#state.memory_limit, State};

handle_call(get_cached_process_memory_and_limit, _From, State) ->
    {reply, {State#state.process_memory, State#state.memory_limit}, State};

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(update, State) ->
    _ = erlang:cancel_timer(State#state.timer),
    State1 = internal_update(State),
    TRef = erlang:send_after(State1#state.timeout, self(), update),
    {noreply, State1#state{ timer = TRef }};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------
%% Server Internals
%%----------------------------------------------------------------------------
get_rss_memory() ->
    TmpState = init_state_by_os(#state{}),
    {ok, ProcMem} = get_process_memory_using_strategy(rss, TmpState),
    ProcMem.

get_cached_process_memory_and_limit() ->
    try
        gen_server:call(?MODULE, get_cached_process_memory_and_limit, infinity)
    catch exit:{noproc, Error} ->
        _ = rabbit_log:warning("Memory monitor process not yet started: ~p~n", [Error]),
        ProcessMemory = get_process_memory_uncached(),
        {ProcessMemory, infinity}
    end.

get_process_memory_uncached() ->
    TmpState = update_process_memory(init_state_by_os(#state{})),
    TmpState#state.process_memory.

update_process_memory(State) ->
    Strategy = get_memory_calculation_strategy(),
    {ok, ProcMem} = get_process_memory_using_strategy(Strategy, State),
    State#state{process_memory = ProcMem}.

init_state_by_os(State = #state{os_type = undefined}) ->
    OsType = os:type(),
    OsPid = os:getpid(),
    init_state_by_os(State#state{os_type = OsType, os_pid = OsPid});
init_state_by_os(State0 = #state{os_type = {unix, linux}, os_pid = OsPid}) ->
    PageSize = get_linux_pagesize(),
    ProcFile = io_lib:format("/proc/~s/statm", [OsPid]),
    State0#state{page_size = PageSize, proc_file = ProcFile};
init_state_by_os(State) ->
    State.

get_process_memory_using_strategy(rss, #state{os_type = {unix, linux},
                                              page_size = PageSize,
                                              proc_file = ProcFile}) ->
    Data = read_proc_file(ProcFile),
    [_|[RssPagesStr|_]] = string:tokens(Data, " "),
    ProcMem = list_to_integer(RssPagesStr) * PageSize,
    {ok, ProcMem};
get_process_memory_using_strategy(rss, #state{os_type = {unix, _},
                                              os_pid = OsPid}) ->
    Cmd = "ps -p " ++ OsPid ++ " -o rss=",
    CmdOutput = os:cmd(Cmd),
    case re:run(CmdOutput, "[0-9]+", [{capture, first, list}]) of
        {match, [Match]} ->
            ProcMem = list_to_integer(Match) * 1024,
            {ok, ProcMem};
        _ ->
            {error, {unexpected_output_from_command, Cmd, CmdOutput}}
    end;
get_process_memory_using_strategy(rss, _State) ->
    {ok, recon_alloc:memory(allocated)};
get_process_memory_using_strategy(allocated, _State) ->
    {ok, recon_alloc:memory(allocated)};
get_process_memory_using_strategy(erlang, _State) ->
    {ok, erlang:memory(total)}.

get_total_memory_from_os() ->
    try
        get_total_memory(os:type())
    catch _:Error:Stacktrace ->
            _ = rabbit_log:warning(
              "Failed to get total system memory: ~n~p~n~p~n",
              [Error, Stacktrace]),
            unknown
    end.

set_mem_limits(State, {relative, MemLimit}) ->
    set_mem_limits(State, MemLimit);
set_mem_limits(State, MemLimit) ->
    case erlang:system_info(wordsize) of
        4 ->
            _ = rabbit_log:warning(
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
                        _ = rabbit_log:warning(
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
                _ = rabbit_log:warning(
                  "Only ~p MiB (~p bytes) of ~p MiB (~p bytes) memory usable due to "
                  "limited address space.~n"
                  "Crashes due to memory exhaustion are possible - see~n"
                  "https://www.rabbitmq.com/memory.html#address-space~n",
                  [trunc(Limit/?ONE_MiB), Limit, trunc(TotalMemory/?ONE_MiB),
                   TotalMemory]),
                Limit;
            _ ->
                TotalMemory
        end,
    MemLim = interpret_limit(parse_mem_limit(MemLimit), UsableMemory),
    _ = rabbit_log:info(
        "Memory high watermark set to ~p MiB (~p bytes)"
        " of ~p MiB (~p bytes) total~n",
        [trunc(MemLim/?ONE_MiB), MemLim,
         trunc(TotalMemory/?ONE_MiB), TotalMemory]
    ),
    internal_update(State #state { total_memory    = TotalMemory,
                                   memory_limit    = MemLim,
                                   memory_config_limit = MemLimit}).


-spec interpret_limit(vm_memory_high_watermark(), non_neg_integer()) -> non_neg_integer().
interpret_limit({absolute, MemLim}, UsableMemory) ->
    erlang:min(MemLim, UsableMemory);
interpret_limit({relative, MemLim}, UsableMemory) ->
    interpret_limit(MemLim, UsableMemory);
interpret_limit(MemFraction, UsableMemory) ->
    trunc(MemFraction * UsableMemory).

-spec parse_mem_limit(vm_memory_high_watermark()) -> float().
parse_mem_limit({absolute, Limit}) ->
    case rabbit_resource_monitor_misc:parse_information_unit(Limit) of
        {ok, ParsedLimit} -> {absolute, ParsedLimit};
        {error, parse_error} ->
            _ = rabbit_log:error("Unable to parse vm_memory_high_watermark value ~p", [Limit]),
            ?DEFAULT_VM_MEMORY_HIGH_WATERMARK
    end;
parse_mem_limit({relative, MemLimit}) ->
    parse_mem_limit(MemLimit);
parse_mem_limit(MemLimit) when is_integer(MemLimit) ->
    parse_mem_limit(float(MemLimit));
parse_mem_limit(MemLimit) when is_float(MemLimit), MemLimit =< ?MAX_VM_MEMORY_HIGH_WATERMARK ->
    MemLimit;
parse_mem_limit(MemLimit) when is_float(MemLimit), MemLimit > ?MAX_VM_MEMORY_HIGH_WATERMARK ->
    _ = rabbit_log:warning(
      "Memory high watermark of ~p is above the allowed maximum, falling back to ~p~n",
      [MemLimit, ?MAX_VM_MEMORY_HIGH_WATERMARK]
    ),
    ?MAX_VM_MEMORY_HIGH_WATERMARK;
parse_mem_limit(MemLimit) ->
    _ = rabbit_log:warning(
      "Memory high watermark of ~p is invalid, defaulting to ~p~n",
      [MemLimit, ?DEFAULT_VM_MEMORY_HIGH_WATERMARK]
    ),
    ?DEFAULT_VM_MEMORY_HIGH_WATERMARK.

internal_update(State0 = #state{memory_limit = MemLimit,
                                alarmed      = Alarmed,
                                alarm_funs   = {AlarmSet, AlarmClear}}) ->
    State1 = update_process_memory(State0),
    ProcMem = State1#state.process_memory,
    NewAlarmed = ProcMem  > MemLimit,
    case {Alarmed, NewAlarmed} of
        {false, true} -> emit_update_info(set, ProcMem, MemLimit),
                         AlarmSet({{resource_limit, memory, node()}, []});
        {true, false} -> emit_update_info(clear, ProcMem, MemLimit),
                         AlarmClear({resource_limit, memory, node()});
        _             -> ok
    end,
    State1#state{alarmed = NewAlarmed}.

emit_update_info(AlarmState, MemUsed, MemLimit) ->
    _ = rabbit_log:info(
      "vm_memory_high_watermark ~p. Memory used:~p allowed:~p~n",
      [AlarmState, MemUsed, MemLimit]).

%% According to https://msdn.microsoft.com/en-us/library/aa366778(VS.85).aspx
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
             %%https://en.wikipedia.org/wiki/X86-64#Virtual_address_space_details
    end.

%%----------------------------------------------------------------------------
%% Internal Helpers
%%----------------------------------------------------------------------------
cmd(Command) ->
    cmd(Command, true).

cmd(Command, ThrowIfMissing) ->
    Exec = hd(string:tokens(Command, " ")),
    case {ThrowIfMissing, os:find_executable(Exec)} of
        {true, false} ->
            throw({command_not_found, Exec});
        {false, false} ->
            {error, command_not_found};
        {_, _Filename} ->
            os:cmd(Command)
    end.

default_linux_pagesize(CmdOutput) ->
    _ = rabbit_log:warning(
      "Failed to get memory page size, using 4096. Reason: ~s",
      [CmdOutput]),
    4096.

get_linux_pagesize() ->
    case cmd("getconf PAGESIZE", false) of
        {error, command_not_found} ->
            default_linux_pagesize("getconf not found in PATH");
        CmdOutput ->
            case re:run(CmdOutput, "^[0-9]+", [{capture, first, list}]) of
                {match, [Match]} -> list_to_integer(Match);
                _ ->
                    default_linux_pagesize(CmdOutput)
            end
    end.

%% get_total_memory(OS) -> Total
%% Windows and Freebsd code based on: memsup:get_memory_usage/1
%% Original code was part of OTP and released under "Erlang Public License".

get_total_memory({unix, darwin}) ->
    sysctl("hw.memsize");

get_total_memory({unix, freebsd}) ->
    PageSize  = sysctl("vm.stats.vm.v_page_size"),
    PageCount = sysctl("vm.stats.vm.v_page_count"),
    PageCount * PageSize;

get_total_memory({unix, openbsd}) ->
    sysctl("hw.usermem");

get_total_memory({win32, _OSname}) ->
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
        ["kB"] -> list_to_integer(Value) * 1024;
        ["KB"] -> list_to_integer(Value) * 1024
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
