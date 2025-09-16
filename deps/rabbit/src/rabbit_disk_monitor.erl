%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_disk_monitor).

-include_lib("kernel/include/logger.hrl").


%% Disk monitoring server. Monitors free disk space
%% periodically and sets alarms when it is below a certain
%% watermark (configurable either as an absolute value or
%% relative to the memory limit).
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
          interval
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

    State3 = enable(State2),

    {ok, State3}.

handle_call({set_disk_free_limit, _}, _From, #state{enabled = false} = State) ->
    ?LOG_INFO("Cannot set disk free limit: "
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

handle_call({set_enabled, _Enabled = true}, _From, State = #state{enabled = true}) ->
    _ = start_timer(set_disk_limits(State, State#state.limit)),
    ?LOG_INFO("Free disk space monitor was already enabled"),
    {reply, ok, State#state{enabled = true}};

handle_call({set_enabled, _Enabled = true}, _From, State = #state{enabled = false}) ->
  _ = start_timer(set_disk_limits(State, State#state.limit)),
  ?LOG_INFO("Free disk space monitor was manually enabled"),
  {reply, ok, State#state{enabled = true}};

handle_call({set_enabled, _Enabled = false}, _From, State = #state{enabled = true}) ->
    _ = erlang:cancel_timer(State#state.timer),
    ?LOG_INFO("Free disk space monitor was manually disabled"),
    {reply, ok, State#state{enabled = false}};

handle_call({set_enabled, _Enabled = false}, _From, State = #state{enabled = false}) ->
  _ = erlang:cancel_timer(State#state.timer),
  ?LOG_INFO("Free disk space monitor was already disabled"),
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
    ?LOG_DEBUG("~tp unhandled msg: ~tp", [?MODULE, Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------
%% Internal functions
%%----------------------------------------------------------------------------

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
    ?LOG_INFO("Disk free limit set to ~bMB",
                    [trunc(Limit / 1000000)]),
    ets:insert(?ETS_NAME, {disk_free_limit, Limit}),
    internal_update(State1).

internal_update(State = #state{limit   = Limit,
                               dir     = Dir,
                               alarmed = Alarmed}) ->
    CurrentFree = get_disk_free(Dir),
    %% note: 'NaN' is considered to be less than a number
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

-spec get_disk_free(file:filename_all()) ->
    AvailableBytes :: non_neg_integer() | 'NaN'.
get_disk_free(Dir) ->
    case disksup:get_disk_info(Dir) of
        [{D, 0, 0, 0, 0}] when D =:= Dir orelse D =:= "none" ->
            'NaN';
        [{_MountPoint, _TotalKiB, AvailableKiB, _Capacity}] ->
            AvailableKiB * 1024;
        _DiskInfo ->
            'NaN'
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
            ?LOG_ERROR("Unable to parse disk_free_limit value ~tp",
                             [Absolute]),
            ?DEFAULT_DISK_FREE_LIMIT
    end.

emit_update_info(StateStr, CurrentFree, Limit) ->
    ?LOG_INFO(
      "Free disk space is ~ts. Free bytes: ~b. Limit: ~b",
      [StateStr, CurrentFree, Limit]).

start_timer(State) ->
    State#state{timer = erlang:send_after(interval(State), self(), update)}.

interval(#state{alarmed      = true,
                max_interval = MaxInterval}) ->
    MaxInterval;
interval(#state{actual       = 'NaN',
                max_interval = MaxInterval}) ->
    MaxInterval;
interval(#state{limit        = Limit,
                actual       = Actual,
                min_interval = MinInterval,
                max_interval = MaxInterval}) ->
    IdealInterval = 2 * (Actual - Limit) / ?FAST_RATE,
    trunc(erlang:max(MinInterval, erlang:min(MaxInterval, IdealInterval))).

enable(#state{retries = 0} = State) ->
    ?LOG_ERROR("Free disk space monitor failed to start!"),
    State;
enable(#state{dir = Dir} = State) ->
    enable_handle_disk_free(get_disk_free(Dir), State).

enable_handle_disk_free(DiskFree, State) when is_integer(DiskFree) ->
    enable_handle_total_memory(catch vm_memory_monitor:get_total_memory(), DiskFree, State);
enable_handle_disk_free(Error, #state{interval = Interval, retries = Retries} = State) ->
    ?LOG_WARNING("Free disk space monitor encountered an error "
                       "(e.g. failed to parse output from OS tools). "
                       "Retries left: ~b Error:~n~tp",
                       [Retries, Error]),
    erlang:send_after(Interval, self(), try_enable),
    State#state{enabled = false}.

enable_handle_total_memory(TotalMemory, DiskFree, #state{limit = Limit} = State) when is_integer(TotalMemory) ->
    ?LOG_INFO("Enabling free disk space monitoring "
                    "(disk free space: ~b, total memory: ~b)", [DiskFree, TotalMemory]),
    start_timer(set_disk_limits(State, Limit));
enable_handle_total_memory(Error, _DiskFree, #state{interval = Interval, retries = Retries} = State) ->
    ?LOG_WARNING("Free disk space monitor encountered an error "
                       "retrieving total memory. "
                       "Retries left: ~b Error:~n~tp",
                       [Retries, Error]),
    erlang:send_after(Interval, self(), try_enable),
    State#state{enabled = false}.
