%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_disk_monitor).

%% Disk monitoring server. Monitors free disk space
%% periodically and sets alarms when it is below a certain
%% watermark (configurable either as an absolute value or
%% relative to the memory limit).
%%
%% Disk monitoring is done by shelling out to /usr/bin/df
%% instead of related built-in OTP functions because currently
%% this is the most reliable way of determining free disk space
%% for the partition our internal database is on.
%%
%% Update interval is dynamically calculated assuming disk
%% space is being filled at FAST_RATE.

-behaviour(gen_server).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([get_disk_free_limit/0, set_disk_free_limit/1,
         get_min_check_interval/0, set_min_check_interval/1,
         get_max_check_interval/0, set_max_check_interval/1,
         get_disk_free/0, set_enabled/1]).

-define(SERVER, ?MODULE).
-define(ETS_NAME, ?MODULE).
-define(DEFAULT_MIN_DISK_CHECK_INTERVAL, 100).
-define(DEFAULT_MAX_DISK_CHECK_INTERVAL, 10000).
-define(DEFAULT_DISK_FREE_LIMIT, 50000000).
%% 250MB/s i.e. 250kB/ms
-define(FAST_RATE, (250 * 1000)).

-record(state, {
          %% monitor partition on which this directory resides
          dir,
          %% configured limit in bytes
          limit,
          %% last known free disk space amount in bytes
          actual,
          %% minimum check interval
          min_interval,
          %% maximum check interval
          max_interval,
          %% timer that drives periodic checks
          timer,
          %% is free disk space alarm currently in effect?
          alarmed,
          %% is monitoring enabled? false on unsupported
          %% platforms
          enabled,
          %% number of retries to enable monitoring if it fails
          %% on start-up
          retries,
          %% Interval between retries
          interval,
          %% Operating system in use
          os,
          %% Port running sh to execute df commands
          port
}).

%%----------------------------------------------------------------------------

-type disk_free_limit() :: integer() | {'absolute', integer()} | string() | {'mem_relative', float() | integer()}.

%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------

-spec get_disk_free_limit() -> integer().
get_disk_free_limit() ->
    safe_ets_lookup(disk_free_limit, ?DEFAULT_DISK_FREE_LIMIT).

-spec set_disk_free_limit(disk_free_limit()) -> 'ok'.
set_disk_free_limit(Limit) ->
    gen_server:call(?MODULE, {set_disk_free_limit, Limit}).

-spec get_min_check_interval() -> integer().
get_min_check_interval() ->
    safe_ets_lookup(min_check_interval, ?DEFAULT_MIN_DISK_CHECK_INTERVAL).

-spec set_min_check_interval(integer()) -> 'ok'.
set_min_check_interval(Interval) ->
    gen_server:call(?MODULE, {set_min_check_interval, Interval}).

-spec get_max_check_interval() -> integer().
get_max_check_interval() ->
    safe_ets_lookup(max_check_interval, ?DEFAULT_MAX_DISK_CHECK_INTERVAL).

-spec set_max_check_interval(integer()) -> 'ok'.
set_max_check_interval(Interval) ->
    gen_server:call(?MODULE, {set_max_check_interval, Interval}).

-spec get_disk_free() -> (integer() | 'NaN').
get_disk_free() ->
    safe_ets_lookup(disk_free, 'NaN').

-spec set_enabled(string()) -> 'ok'.
set_enabled(Enabled) ->
    gen_server:call(?MODULE, {set_enabled, Enabled}).

%%----------------------------------------------------------------------------
%% gen_server callbacks
%%----------------------------------------------------------------------------

-spec start_link(disk_free_limit()) -> rabbit_types:ok_pid_or_error().
start_link(Args) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Args], []).

init([Limit]) ->
    Dir = dir(),
    {ok, Retries} = application:get_env(rabbit, disk_monitor_failure_retries),
    {ok, Interval} = application:get_env(rabbit, disk_monitor_failure_retry_interval),
    ?ETS_NAME = ets:new(?ETS_NAME, [protected, set, named_table]),
    State0 = #state{dir          = Dir,
                    alarmed      = false,
                    enabled      = true,
                    limit        = Limit,
                    retries      = Retries,
                    interval     = Interval},
    State1 = set_min_check_interval(?DEFAULT_MIN_DISK_CHECK_INTERVAL, State0),
    State2 = set_max_check_interval(?DEFAULT_MAX_DISK_CHECK_INTERVAL, State1),

    OS = os:type(),
    Port = case OS of
               {unix, _} ->
                   start_portprogram();
               {win32, _OSname} ->
                   not_used
           end,
    State3 = State2#state{port=Port, os=OS},

    State4 = enable(State3),

    {ok, State4}.

handle_call({set_disk_free_limit, _}, _From, #state{enabled = false} = State) ->
    rabbit_log:info("Cannot set disk free limit: "
                    "disabled disk free space monitoring", []),
    {reply, ok, State};

handle_call({set_disk_free_limit, Limit}, _From, State) ->
    {reply, ok, set_disk_limits(State, Limit)};

handle_call(get_max_check_interval, _From, State) ->
    {reply, State#state.max_interval, State};

handle_call({set_min_check_interval, MinInterval}, _From, State) ->
    {reply, ok, set_min_check_interval(MinInterval, State)};

handle_call({set_max_check_interval, MaxInterval}, _From, State) ->
    {reply, ok, set_max_check_interval(MaxInterval, State)};

handle_call({set_enabled, _Enabled = true}, _From, State) ->
    _ = start_timer(set_disk_limits(State, State#state.limit)),
    rabbit_log:info("Free disk space monitor was enabled"),
    {reply, ok, State#state{enabled = true}};

handle_call({set_enabled, _Enabled = false}, _From, State) ->
    _ = erlang:cancel_timer(State#state.timer),
    rabbit_log:info("Free disk space monitor was manually disabled"),
    {reply, ok, State#state{enabled = false}};

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(try_enable, #state{retries = Retries} = State) ->
    {noreply, enable(State#state{retries = Retries - 1})};

handle_info(update, State) ->
    {noreply, start_timer(internal_update(State))};

handle_info(Info, State) ->
    rabbit_log:debug("~tp unhandled msg: ~tp", [?MODULE, Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------
%% Internal functions
%%----------------------------------------------------------------------------

start_portprogram() ->
    Args = ["-s", "rabbit_disk_monitor"],
    Opts = [stream, stderr_to_stdout, {args, Args}],
    erlang:open_port({spawn_executable, "/bin/sh"}, Opts).

run_port_cmd(Cmd0, Port) ->
    %% Insert a carriage return, ^M or ASCII 13, after the command,
    %% to indicate end of output
    Cmd1 = io_lib:format("~ts < /dev/null; echo \"\^M\"~n", [Cmd0]),
    Cmd2 = rabbit_data_coercion:to_utf8_binary(Cmd1),
    Port ! {self(), {command, [Cmd2, 10]}}, % The 10 at the end is a newline
    get_reply(Port, []).

get_reply(Port, O) ->
    receive
        {Port, {data, N}} ->
            case newline(N, O) of
                {ok, Str} ->
                    Str;
                {more, Acc} ->
                    get_reply(Port, Acc)
            end;
        {'EXIT', Port, Reason} ->
            exit({port_died, Reason})
    end.

% Character 13 is ^M or carriage return
newline([13|_], B) ->
    {ok, lists:reverse(B)};
newline([H|T], B) ->
    newline(T, [H|B]);
newline([], B) ->
    {more, B}.

find_cmd(Cmd) ->
    os:find_executable(Cmd).

safe_ets_lookup(Key, Default) ->
    try
        case ets:lookup(?ETS_NAME, Key) of
            [{Key, Value}] ->
                Value;
            [] ->
                Default
        end
    catch
        error:badarg ->
            Default
    end.

% the partition / drive containing this directory will be monitored
dir() -> rabbit:data_dir().

set_min_check_interval(MinInterval, State) ->
    ets:insert(?ETS_NAME, {min_check_interval, MinInterval}),
    State#state{min_interval = MinInterval}.

set_max_check_interval(MaxInterval, State) ->
    ets:insert(?ETS_NAME, {max_check_interval, MaxInterval}),
    State#state{max_interval = MaxInterval}.

set_disk_limits(State, Limit0) ->
    Limit = interpret_limit(Limit0),
    State1 = State#state { limit = Limit },
    rabbit_log:info("Disk free limit set to ~bMB",
                    [trunc(Limit / 1000000)]),
    ets:insert(?ETS_NAME, {disk_free_limit, Limit}),
    internal_update(State1).

internal_update(State = #state{limit   = Limit,
                               dir     = Dir,
                               alarmed = Alarmed,
                               os      = OS,
                               port    = Port}) ->
    CurrentFree = get_disk_free(Dir, OS, Port),
    NewAlarmed = CurrentFree < Limit,
    case {Alarmed, NewAlarmed} of
        {false, true} ->
            emit_update_info("insufficient", CurrentFree, Limit),
            rabbit_alarm:set_alarm({{resource_limit, disk, node()}, []});
        {true, false} ->
            emit_update_info("sufficient", CurrentFree, Limit),
            rabbit_alarm:clear_alarm({resource_limit, disk, node()});
        _ ->
            ok
    end,
    ets:insert(?ETS_NAME, {disk_free, CurrentFree}),
    State#state{alarmed = NewAlarmed, actual = CurrentFree}.

get_disk_free(Dir, {unix, Sun}, Port)
  when Sun =:= sunos; Sun =:= sunos4; Sun =:= solaris ->
    Df = find_cmd("df"),
    parse_free_unix(run_port_cmd(Df ++ " -k '" ++ Dir ++ "'", Port));
get_disk_free(Dir, {unix, _}, Port) ->
    Df = find_cmd("df"),
    parse_free_unix(run_port_cmd(Df ++ " -kP '" ++ Dir ++ "'", Port));
get_disk_free(Dir, {win32, _}, not_used) ->
    % Dir:
    % "c:/Users/username/AppData/Roaming/RabbitMQ/db/rabbit2@username-z01-mnesia"
    case win32_get_drive_letter(Dir) of
        error ->
            rabbit_log:warning("Expected the mnesia directory absolute "
                               "path to start with a drive letter like "
                               "'C:'. The path is: '~tp'", [Dir]),
            {ok, Free} = win32_get_disk_free_dir(Dir),
            Free;
        DriveLetter ->
            % Note: yes, "$\s" is the $char sequence for an ASCII space
            F = fun([D, $:, $\\, $\s | _]) when D =:= DriveLetter ->
                        true;
                   (_) -> false
                end,
            % Note: we can use os_mon_sysinfo:get_disk_info/1 after the following is fixed:
            % https://github.com/erlang/otp/issues/6156
            [DriveInfoStr] = lists:filter(F, os_mon_sysinfo:get_disk_info()),

            % Note: DriveInfoStr is in this format
            % "C:\\ DRIVE_FIXED 720441434112 1013310287872 720441434112\n"
            [DriveLetter, $:, $\\, $\s | DriveInfo] = DriveInfoStr,

            % https://docs.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-getdiskfreespaceexa
            % lib/os_mon/c_src/win32sysinfo.c:
            % if (fpGetDiskFreeSpaceEx(drive,&availbytes,&totbytes,&totbytesfree)){
            %     sprintf(answer,"%s DRIVE_FIXED %I64u %I64u %I64u\n",drive,availbytes,totbytes,totbytesfree);
            ["DRIVE_FIXED", FreeBytesAvailableToCallerStr,
             _TotalNumberOfBytesStr, _TotalNumberOfFreeBytesStr] = string:tokens(DriveInfo, " "),
            list_to_integer(FreeBytesAvailableToCallerStr)
    end.

parse_free_unix(Str) ->
    case string:tokens(Str, "\n") of
        [_, S | _] -> case string:tokens(S, " \t") of
                          [_, _, _, Free | _] -> list_to_integer(Free) * 1024;
                          _                   -> exit({unparseable, Str})
                      end;
        _          -> exit({unparseable, Str})
    end.

win32_get_drive_letter([DriveLetter, $:, $/ | _]) when (DriveLetter >= $a andalso DriveLetter =< $z) ->
    % Note: os_mon_sysinfo returns drives with uppercase letters, so uppercase it here
    DriveLetter - 32;
win32_get_drive_letter([DriveLetter, $:, $/ | _]) when (DriveLetter >= $A andalso DriveLetter =< $Z) ->
    DriveLetter;
win32_get_drive_letter(_) ->
    error.

win32_get_disk_free_dir(Dir) ->
    %% On Windows, the Win32 API enforces a limit of 260 characters
    %% (MAX_PATH). If we call `dir` with a path longer than that, it
    %% fails with "File not found". Starting with Windows 10 version
    %% 1607, this limit was removed, but the administrator has to
    %% configure that.
    %%
    %% NTFS supports paths up to 32767 characters. Therefore, paths
    %% longer than 260 characters exist but they are "inaccessible" to
    %% `dir`.
    %%
    %% A workaround is to tell the Win32 API to not parse a path and
    %% just pass it raw to the underlying filesystem. To do this, the
    %% path must be prepended with "\\?\". That's what we do here.
    %%
    %% However, the underlying filesystem may not support forward
    %% slashes transparently, as the Win32 API does. Therefore, we
    %% convert all forward slashes to backslashes.
    %%
    %% See the following page to learn more about this:
    %% https://ss64.com/nt/syntax-filenames.html
    RawDir = "\\\\?\\" ++ string:replace(Dir, "/", "\\", all),
    case run_os_cmd("dir /-C /W \"" ++ RawDir ++ "\"") of
        {error, Error} ->
            exit({unparseable, Error});
        CommandResult ->
            LastLine0 = lists:last(string:tokens(CommandResult, "\r\n")),
            LastLine1 = lists:reverse(LastLine0),
            {match, [Free]} = re:run(LastLine1, "(\\d+)",
                                     [{capture, all_but_first, list}]),
            {ok, list_to_integer(lists:reverse(Free))}
    end.

interpret_limit({mem_relative, Relative})
    when is_number(Relative) ->
    round(Relative * vm_memory_monitor:get_total_memory());
interpret_limit({absolute, Absolute}) ->
    interpret_limit(Absolute);
interpret_limit(Absolute) ->
    case rabbit_resource_monitor_misc:parse_information_unit(Absolute) of
        {ok, ParsedAbsolute} -> ParsedAbsolute;
        {error, parse_error} ->
            rabbit_log:error("Unable to parse disk_free_limit value ~tp",
                             [Absolute]),
            ?DEFAULT_DISK_FREE_LIMIT
    end.

emit_update_info(StateStr, CurrentFree, Limit) ->
    rabbit_log:info(
      "Free disk space is ~ts. Free bytes: ~b. Limit: ~b",
      [StateStr, CurrentFree, Limit]).

start_timer(State) ->
    State#state{timer = erlang:send_after(interval(State), self(), update)}.

interval(#state{alarmed      = true,
                max_interval = MaxInterval}) ->
    MaxInterval;
interval(#state{limit        = Limit,
                actual       = Actual,
                min_interval = MinInterval,
                max_interval = MaxInterval}) ->
    IdealInterval = 2 * (Actual - Limit) / ?FAST_RATE,
    trunc(erlang:max(MinInterval, erlang:min(MaxInterval, IdealInterval))).

enable(#state{retries = 0} = State) ->
    rabbit_log:error("Free disk space monitor failed to start!"),
    State;
enable(#state{dir = Dir, os = OS, port = Port} = State) ->
    enable_handle_disk_free(catch get_disk_free(Dir, OS, Port), State).

enable_handle_disk_free(DiskFree, State) when is_integer(DiskFree) ->
    enable_handle_total_memory(catch vm_memory_monitor:get_total_memory(), DiskFree, State);
enable_handle_disk_free(Error, #state{interval = Interval, retries = Retries} = State) ->
    rabbit_log:warning("Free disk space monitor encountered an error "
                       "(e.g. failed to parse output from OS tools). "
                       "Retries left: ~b Error:~n~tp",
                       [Retries, Error]),
    erlang:send_after(Interval, self(), try_enable),
    State#state{enabled = false}.

enable_handle_total_memory(TotalMemory, DiskFree, #state{limit = Limit} = State) when is_integer(TotalMemory) ->
    rabbit_log:info("Enabling free disk space monitoring "
                    "(disk free space: ~b, total memory: ~b)", [DiskFree, TotalMemory]),
    start_timer(set_disk_limits(State, Limit));
enable_handle_total_memory(Error, _DiskFree, #state{interval = Interval, retries = Retries} = State) ->
    rabbit_log:warning("Free disk space monitor encountered an error "
                       "retrieving total memory. "
                       "Retries left: ~b Error:~n~tp",
                       [Retries, Error]),
    erlang:send_after(Interval, self(), try_enable),
    State#state{enabled = false}.

run_os_cmd(Cmd) ->
    Pid = self(),
    Ref = make_ref(),
    CmdFun = fun() ->
        CmdResult = rabbit_misc:os_cmd(Cmd),
        Pid ! {Pid, Ref, CmdResult}
    end,
    CmdPid = spawn(CmdFun),
    receive
        {Pid, Ref, CmdResult} ->
            CmdResult
    after 5000 ->
        exit(CmdPid, kill),
        rabbit_log:error("Command timed out: '~ts'", [Cmd]),
        {error, timeout}
    end.
