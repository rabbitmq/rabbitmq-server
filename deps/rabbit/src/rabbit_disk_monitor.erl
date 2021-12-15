%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
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
          interval
}).

%%----------------------------------------------------------------------------

-type disk_free_limit() :: integer() | {'absolute', integer()} | string() | {'mem_relative', float() | integer()}.

%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------

-spec get_disk_free_limit() -> integer().

get_disk_free_limit() ->
    gen_server:call(?MODULE, get_disk_free_limit).

-spec set_disk_free_limit(disk_free_limit()) -> 'ok'.

set_disk_free_limit(Limit) ->
    gen_server:call(?MODULE, {set_disk_free_limit, Limit}).

-spec get_min_check_interval() -> integer().

get_min_check_interval() ->
    gen_server:call(?MODULE, get_min_check_interval).

-spec set_min_check_interval(integer()) -> 'ok'.

set_min_check_interval(Interval) ->
    gen_server:call(?MODULE, {set_min_check_interval, Interval}).

-spec get_max_check_interval() -> integer().

get_max_check_interval() ->
    gen_server:call(?MODULE, get_max_check_interval).

-spec set_max_check_interval(integer()) -> 'ok'.

set_max_check_interval(Interval) ->
    gen_server:call(?MODULE, {set_max_check_interval, Interval}).

-spec get_disk_free() -> (integer() | 'unknown').

get_disk_free() ->
    gen_server:call(?MODULE, get_disk_free).

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
    State = #state{dir          = Dir,
                   min_interval = ?DEFAULT_MIN_DISK_CHECK_INTERVAL,
                   max_interval = ?DEFAULT_MAX_DISK_CHECK_INTERVAL,
                   alarmed      = false,
                   enabled      = true,
                   limit        = Limit,
                   retries      = Retries,
                   interval     = Interval},
    {ok, enable(State)}.

handle_call(get_disk_free_limit, _From, State = #state{limit = Limit}) ->
    {reply, Limit, State};

handle_call({set_disk_free_limit, _}, _From, #state{enabled = false} = State) ->
    rabbit_log:info("Cannot set disk free limit: "
		    "disabled disk free space monitoring", []),
    {reply, ok, State};

handle_call({set_disk_free_limit, Limit}, _From, State) ->
    {reply, ok, set_disk_limits(State, Limit)};

handle_call(get_min_check_interval, _From, State) ->
    {reply, State#state.min_interval, State};

handle_call(get_max_check_interval, _From, State) ->
    {reply, State#state.max_interval, State};

handle_call({set_min_check_interval, MinInterval}, _From, State) ->
    {reply, ok, State#state{min_interval = MinInterval}};

handle_call({set_max_check_interval, MaxInterval}, _From, State) ->
    {reply, ok, State#state{max_interval = MaxInterval}};

handle_call(get_disk_free, _From, State = #state { actual = Actual }) ->
    {reply, Actual, State};

handle_call({set_enabled, _Enabled = true}, _From, State) ->
    start_timer(set_disk_limits(State, State#state.limit)),
    rabbit_log:info("Free disk space monitor was enabled"),
    {reply, ok, State#state{enabled = true}};
handle_call({set_enabled, _Enabled = false}, _From, State) ->
    erlang:cancel_timer(State#state.timer),
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

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------
%% Server Internals
%%----------------------------------------------------------------------------

% the partition / drive containing this directory will be monitored
dir() -> rabbit_mnesia:dir().

set_disk_limits(State, Limit0) ->
    Limit = interpret_limit(Limit0),
    State1 = State#state { limit = Limit },
    rabbit_log:info("Disk free limit set to ~pMB",
                    [trunc(Limit / 1000000)]),
    internal_update(State1).

internal_update(State = #state { limit   = Limit,
                                 dir     = Dir,
                                 alarmed = Alarmed}) ->
    CurrentFree = get_disk_free(Dir),
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
    State #state {alarmed = NewAlarmed, actual = CurrentFree}.

get_disk_free(Dir) ->
    get_disk_free(Dir, os:type()).

get_disk_free(Dir, {unix, Sun})
  when Sun =:= sunos; Sun =:= sunos4; Sun =:= solaris ->
    Df = os:find_executable("df"),
    parse_free_unix(run_cmd(Df ++ " -k " ++ Dir));
get_disk_free(Dir, {unix, _}) ->
    Df = os:find_executable("df"),
    parse_free_unix(run_cmd(Df ++ " -kP " ++ Dir));
get_disk_free(Dir, {win32, _}) ->
    case win32_get_disk_free_fsutil(Dir) of
        {ok, Free0} -> Free0;
        error ->
            case win32_get_disk_free_pwsh(Dir) of
                {ok, Free1} -> Free1;
                _ -> exit(could_not_determine_disk_free)
            end
    end.

parse_free_unix(Str) ->
    case string:tokens(Str, "\n") of
        [_, S | _] -> case string:tokens(S, " \t") of
                          [_, _, _, Free | _] -> list_to_integer(Free) * 1024;
                          _                   -> exit({unparseable, Str})
                      end;
        _          -> exit({unparseable, Str})
    end.

win32_get_disk_free_fsutil(Dir) ->
    % Dir:
    % "c:/Users/username/AppData/Roaming/RabbitMQ/db/rabbit2@username-z01-mnesia"
    Drive = string:slice(Dir, 0, 2),

    % Drive: c:
    FsutilCmd = "fsutil.exe volume diskfree " ++ Drive,

    % C:\windows\system32>fsutil volume diskfree c:
    % Total free bytes        :   812,733,878,272 (756.9 GB)
    % Total bytes             : 1,013,310,287,872 (943.7 GB)
    % Total quota free bytes  :   812,733,878,272 (756.9 GB)
    case run_cmd(FsutilCmd) of
        {error, timeout} ->
            error;
        FsutilResult ->
            case string:slice(FsutilResult, 0, 5) of
                "Error" ->
                    error;
                "Total" ->
                    FirstLine = hd(string:tokens(FsutilResult, "\r\n")),
                    {match, [FreeStr]} = re:run(FirstLine, "(\\d+,?)+", [{capture, first, list}]),
                    {ok, list_to_integer(lists:flatten(string:tokens(FreeStr, ",")))}
            end
    end.


win32_get_disk_free_pwsh(Dir) ->
    % Dir:
    % "c:/Users/username/AppData/Roaming/RabbitMQ/db/rabbit2@username-z01-mnesia"
    Drive = string:slice(Dir, 0, 1),
    PoshCmd = "powershell.exe -NoLogo -NoProfile -NonInteractive -Command (Get-PSDrive " ++ Drive ++ ").Free",
    case run_cmd(PoshCmd) of
        {error, timeout} ->
            error;
        PoshResultStr ->
            % Note: remove \r\n
            PoshResult = string:slice(PoshResultStr, 0, length(PoshResultStr) - 2),
            {ok, list_to_integer(PoshResult)}
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
            rabbit_log:error("Unable to parse disk_free_limit value ~p",
                             [Absolute]),
            ?DEFAULT_DISK_FREE_LIMIT
    end.

emit_update_info(StateStr, CurrentFree, Limit) ->
    rabbit_log:info(
      "Free disk space is ~s. Free bytes: ~p. Limit: ~p",
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
    State;
enable(#state{dir = Dir, interval = Interval, limit = Limit, retries = Retries}
       = State) ->
    case {catch get_disk_free(Dir),
          vm_memory_monitor:get_total_memory()} of
        {N1, N2} when is_integer(N1), is_integer(N2) ->
            rabbit_log:info("Enabling free disk space monitoring", []),
            start_timer(set_disk_limits(State, Limit));
        Err ->
            rabbit_log:info("Free disk space monitor encountered an error "
                            "(e.g. failed to parse output from OS tools): ~p, retries left: ~b",
                            [Err, Retries]),
            erlang:send_after(Interval, self(), try_enable),
            State#state{enabled = false}
    end.

run_cmd(Cmd) ->
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
        rabbit_log:error("Command timed out: '~s'", [Cmd]),
        {error, timeout}
    end.
