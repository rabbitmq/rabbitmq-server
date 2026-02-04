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
         set_disk_free_limit/2,
         get_min_check_interval/0, set_min_check_interval/1,
         get_max_check_interval/0, set_max_check_interval/1,
         get_disk_free/0, get_mount_free/0, set_enabled/1]).

-define(SERVER, ?MODULE).
-define(ETS_NAME, ?MODULE).
-define(MOUNT_ETS_NAME, rabbit_disk_monitor_per_mount).
-define(DEFAULT_MIN_DISK_CHECK_INTERVAL, 100).
-define(DEFAULT_MAX_DISK_CHECK_INTERVAL, 10000).
-define(DEFAULT_DISK_FREE_LIMIT, 50000000).

-record(mount,
        {%% name set in configuration
         name :: binary(),
         %% number set in configuration, used to order the disks in the UI
         precedence :: integer(),
         %% minimum bytes available
         limit :: non_neg_integer(),
         %% detected available disk space in bytes
         available = 'NaN' :: non_neg_integer() | 'NaN',
         %% set of queue types which should be blocked if the limit is exceeded
         queue_types :: sets:set(rabbit_queue_type:queue_type())}).

-record(state, {
          %% monitor partition on which the data directory resides
          dir,
          %% configured limit in bytes
          limit,
          %% last known free disk space amount in bytes
          actual,
          %% extra file systems to monitor mapped to the queue types to
          mounts = #{} :: mounts(),
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

-type mounts() :: #{file:filename() => #mount{}}.

%%----------------------------------------------------------------------------

%% This needs to wait until the recovery phase so that queue types have a
%% chance to register themselves.
-rabbit_boot_step({monitor_mounts,
                   [{description, "monitor per-queue-type mounts"},
                    {mfa,         {gen_server, call,
                                   [?MODULE, monitor_mounts]}},
                    {requires,    recovery},
                    {enables,     routing_ready}]}).

%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------

-spec get_disk_free_limit() -> integer().
get_disk_free_limit() ->
    safe_ets_lookup(disk_free_limit, ?DEFAULT_DISK_FREE_LIMIT).

-spec set_disk_free_limit(disk_free_limit()) -> 'ok'.
set_disk_free_limit(Limit) ->
    gen_server:call(?MODULE, {set_disk_free_limit, Limit}).

-spec set_disk_free_limit(MountName :: binary(), integer()) -> 'ok'.
set_disk_free_limit(MountName, Limit)
  when is_binary(MountName) andalso is_integer(Limit) ->
    gen_server:call(?MODULE, {set_disk_free_limit, MountName, Limit}).

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

-spec get_mount_free() ->
    [#{name := binary(),
       available := non_neg_integer() | 'NaN',
       limit := pos_integer()}].
get_mount_free() ->
    Ms0 = try
              ets:tab2list(?MOUNT_ETS_NAME)
          catch
              error:badarg ->
                  []
          end,
    Ms = lists:sort(
           fun(#mount{precedence = A}, #mount{precedence = B}) ->
                   %% ascending
                   A < B
           end, Ms0),
    [#{name => Name,
       available => Available,
       limit => Limit} || #mount{name = Name,
                                 available = Available,
                                 limit = Limit} <- Ms].

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
    {ok, Retries} = application:get_env(rabbit, disk_monitor_failure_retries),
    {ok, Interval} = application:get_env(rabbit, disk_monitor_failure_retry_interval),
    MinInterval = application:get_env(rabbit, disk_monitor_min_interval,
                                      ?DEFAULT_MIN_DISK_CHECK_INTERVAL),
    MaxInterval = application:get_env(rabbit, disk_monitor_max_interval,
                                      ?DEFAULT_MAX_DISK_CHECK_INTERVAL),
    ?ETS_NAME = ets:new(?ETS_NAME, [protected, set, named_table]),
    ?MOUNT_ETS_NAME = ets:new(?MOUNT_ETS_NAME, [protected, set, named_table,
                                                {keypos, #mount.name}]),
    State0 = #state{alarmed      = false,
                    enabled      = true,
                    limit        = Limit,
                    retries      = Retries,
                    interval     = Interval},
    State1 = set_min_check_interval(MinInterval, State0),
    State2 = set_max_check_interval(MaxInterval, State1),

    State3 = enable(State2),

    {ok, State3}.

handle_call({set_disk_free_limit, _}, _From, #state{enabled = false} = State) ->
    ?LOG_INFO("Cannot set disk free limit: "
                    "disabled disk free space monitoring", []),
    {reply, ok, State};

handle_call({set_disk_free_limit, Limit}, _From, State) ->
    {reply, ok, set_disk_limits(State, Limit)};

handle_call({set_disk_free_limit, Name, Limit}, _From,
            #state{mounts = Mounts0} = State) ->
    MatchingMount = lists:search(
                      fun({_Path, #mount{name = N}}) ->
                              Name =:= N
                      end, maps:to_list(Mounts0)),
    case MatchingMount of
        {value, {Path, Mount}} ->
            ?LOG_INFO("Updated disk free limit of mount '~ts'", [Name]),
            Mounts = Mounts0#{Path := Mount#mount{limit = Limit}},
            {reply, ok, State#state{mounts = Mounts}};
        false ->
            ?LOG_WARNING("Cannot set disk free limit for mount '~ts' since "
                         "the name does not match any known mounts.", [Name]),
            {reply, ok, State}
    end;

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

handle_call(monitor_mounts, _From, State) ->
    case State of
        #state{enabled = true} ->
            State1 = State#state{mounts = mounts()},
            {reply, ok, internal_update(State1)};
        #state{enabled = false} ->
            {reply, ok, State}
    end;

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

internal_update(#state{limit = DataDirLimit,
                       dir = Dir,
                       mounts = Mounts,
                       alarmed = Alarmed} = State) ->
    DiskFree = get_disk_free(State),
    DataDirFree = maps:get(Dir, DiskFree, 'NaN'),
    %% note: 'NaN' is considered to be less than a number
    NewAlarmed = DataDirFree < DataDirLimit,
    case {Alarmed, NewAlarmed} of
        {false, true} ->
            emit_update_info("insufficient", DataDirFree, DataDirLimit),
            rabbit_alarm:set_alarm({{resource_limit, disk, node()}, []});
        {true, false} ->
            emit_update_info("sufficient", DataDirFree, DataDirLimit),
            rabbit_alarm:clear_alarm({resource_limit, disk, node()});
        _ ->
            ok
    end,
    ets:insert(?ETS_NAME, {disk_free, DataDirFree}),

    NewMounts = maps:map(
                 fun(Path, M) ->
                         Available = maps:get(Path, DiskFree, 'NaN'),
                         M#mount{available = Available}
                 end, Mounts),
    ets:insert(?MOUNT_ETS_NAME, [M || _Path := M <- NewMounts]),

    AlarmedMs = alarmed_mounts(Mounts),
    AlarmedQTs = alarmed_queue_types(Mounts),
    NewAlarmedMs = alarmed_mounts(NewMounts),
    NewAlarmedQTs = alarmed_queue_types(NewMounts),

    NewlyClearedMs = sets:subtract(AlarmedMs, NewAlarmedMs),
    NewlyClearedQTs = sets:subtract(AlarmedQTs, NewAlarmedQTs),
    NewlyAlarmedMs = sets:subtract(NewAlarmedMs, AlarmedMs),
    NewlyAlarmedQTs = sets:subtract(NewAlarmedQTs, AlarmedQTs),

    lists:foreach(
      fun(Path) ->
              #mount{name = Name,
                     limit = Limit,
                     available = Available} = maps:get(Path, NewMounts),
              emit_update_info(Name, "insufficient", Available, Limit)
      end, lists:sort(sets:to_list(NewlyAlarmedMs))),
    lists:foreach(
      fun(QT) ->
              Alarm = {resource_limit, {disk, QT}, node()},
              rabbit_alarm:set_alarm({Alarm, []})
      end, lists:sort(sets:to_list(NewlyAlarmedQTs))),
    lists:foreach(
      fun(Path) ->
              #mount{name = Name,
                     limit = Limit,
                     available = Available} = maps:get(Path, NewMounts),
              emit_update_info(Name, "sufficient", Available, Limit)
      end, lists:sort(sets:to_list(NewlyClearedMs))),
    lists:foreach(
      fun(QT) ->
              Alarm = {resource_limit, {disk, QT}, node()},
              rabbit_alarm:clear_alarm(Alarm)
      end, lists:sort(sets:to_list(NewlyClearedQTs))),

    State#state{alarmed = NewAlarmed,
                actual = DataDirFree,
                mounts = NewMounts}.

emit_update_info(StateStr, CurrentFree, Limit) ->
    ?LOG_INFO(
      "Free disk space is ~ts. Free bytes: ~b. Limit: ~b",
      [StateStr, CurrentFree, Limit]).
emit_update_info(MountPoint, StateStr, CurrentFree, Limit) ->
    ?LOG_INFO(
      "Free space of disk '~ts' is ~ts. Free bytes: ~b. Limit: ~b",
      [MountPoint, StateStr, CurrentFree, Limit]).

-spec alarmed_mounts(mounts()) -> sets:set(file:filename()).
alarmed_mounts(Mounts) ->
    maps:fold(
      fun (Path, #mount{available = Available,
                        limit = Limit}, Acc) when Available < Limit ->
              %% Note: 'NaN' < Limit is true (Erlang term ordering), which is
              %% correct fail-safe behavior when disk info is unavailable
              sets:add_element(Path, Acc);
          (_Path, _Mount, Acc) ->
              Acc
      end, sets:new([{version, 2}]), Mounts).

-spec alarmed_queue_types(mounts()) ->
    sets:set(module()).
alarmed_queue_types(MountPoints) ->
    maps:fold(
      fun (_Path, #mount{available = Available,
                         limit = Limit,
                         queue_types = QTs}, Acc) when Available < Limit ->
              %% Note: 'NaN' < Limit is true (Erlang term ordering), which is
              %% correct fail-safe behavior when disk info is unavailable
              sets:union(QTs, Acc);
          (_Path, _Mount, Acc) ->
              Acc
      end, sets:new([{version, 2}]), MountPoints).

-spec get_disk_free(#state{}) ->
    #{file:filename() => AvailableB :: non_neg_integer()}.
get_disk_free(#state{dir = DataDir, mounts = Mounts}) ->
    #{Mount => AvailableKiB * 1024 ||
      {Mount, Total, AvailableKiB, Capacity} <- disksup:get_disk_info(),
      {Total, AvailableKiB, Capacity} =/= {0, 0, 0},
      Mount =:= DataDir orelse is_map_key(Mount, Mounts)}.

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

start_timer(State) ->
    State#state{timer = erlang:send_after(interval(State), self(), update)}.

interval(#state{actual = DataDirAvailable,
                limit = DataDirLimit,
                mounts = Mounts,
                min_interval = MinInterval,
                max_interval = MaxInterval}) ->
    DataDirGap = case DataDirAvailable of
                     N when is_integer(N) ->
                         N - DataDirLimit;
                     _ ->
                         1_000_000_000
                 end,
    SmallestGap = maps:fold(
                    fun (_Path, #mount{available = A, limit = L}, Min)
                          when is_integer(A) ->
                            erlang:min(A - L, Min);
                        (_Path, _Mount, Min) ->
                            Min
                    end, DataDirGap, Mounts),
    IdealInterval = 2 * SmallestGap / fast_rate(),
    trunc(erlang:max(MinInterval, erlang:min(MaxInterval, IdealInterval))).

fast_rate() ->
    %% 250MB/s i.e. 250kB/ms
    application:get_env(rabbit, disk_monitor_fast_rate, 250_000).

-spec mounts() -> mounts().
mounts() ->
    case application:get_env(rabbit, disk_free_limits) of
        {ok, Limits} ->
            maps:fold(
              fun(Prec, #{name := Name,
                          mount := Path,
                          limit := Limit0,
                          queue_types := QTs0}, Acc) ->
                      Res = rabbit_resource_monitor_misc:parse_information_unit(
                              Limit0),
                      case Res of
                          {ok, Limit} ->
                              {Known, Unknown} = resolve_queue_types(QTs0),
                              case Unknown of
                                  [_ | _] ->
                                      ?LOG_WARNING(
                                        "Unknown queue types configured for "
                                        "disk '~ts': ~ts",
                                        [Name, lists:join(", ", Unknown)]),
                                      ok;
                                  _ ->
                                      ok
                              end,
                              case Known of
                                  [] ->
                                      ?LOG_ERROR("No known queue types "
                                                 "configured for disk '~ts'. "
                                                 "The disk will not be "
                                                 "monitored for free "
                                                 "disk space.", [Name]),
                                      Acc;
                                  _ ->
                                      QTs = sets:from_list(Known,
                                                           [{version, 2}]),
                                      Mount = #mount{name = Name,
                                                     precedence = Prec,
                                                     limit = Limit,
                                                     queue_types = QTs},
                                      Acc#{Path => Mount}
                              end;
                          {error, parse_error} ->
                              ?LOG_ERROR("Unable to parse free disk limit "
                                         "'~ts' for disk '~ts'. The disk will "
                                         "not be monitored for free space.",
                                         [Limit0, Name]),
                              Acc
                      end
              end, #{}, Limits);
        undefined ->
            #{}
    end.

resolve_queue_types(QTs) ->
    resolve_queue_types(QTs, {[], []}).

resolve_queue_types([], Acc) ->
    Acc;
resolve_queue_types([QT | Rest], {Known, Unknown}) ->
    case rabbit_registry:lookup_type_module(queue, QT) of
        {ok, TypeModule} ->
            resolve_queue_types(Rest, {[TypeModule | Known], Unknown});
        {error, not_found} ->
            resolve_queue_types(Rest, {Known, [QT | Unknown]})
    end.

enable(#state{retries = 0} = State) ->
    ?LOG_ERROR("Free disk space monitor failed to start!"),
    State;
enable(#state{dir = undefined,
              interval = Interval,
              retries = Retries} = State) ->
    case resolve_data_dir() of
        {ok, MountPoint} ->
            enable(State#state{dir = MountPoint});
        {error, Reason} ->
            ?LOG_WARNING("Free disk space monitor encounter an error "
                         "resolving the data directory '~ts'. Retries left: "
                         "~b Error:~n~tp",
                         [rabbit:data_dir(), Retries, Reason]),
            erlang:send_after(Interval, self(), try_enable),
            State#state{enabled = false}
    end;
enable(#state{dir = Dir,
              retries = Retries,
              interval = Interval,
              limit = Limit} = State) ->
    DiskFree = get_disk_free(State),
    case vm_memory_monitor:get_total_memory() of
        TotalMemory when is_integer(TotalMemory) ->
            ?LOG_INFO("Enabling free disk space monitoring (data dir free "
                      "space: ~b, total memory: ~b)",
                      [maps:get(Dir, DiskFree, unknown), TotalMemory]),
            start_timer(set_disk_limits(State, Limit));
        unknown ->
            ?LOG_WARNING("Free disk space monitor could not determine total "
                         "memory. Retries left: ~b", [Retries]),
            erlang:send_after(Interval, self(), try_enable),
            State#state{enabled = false}
    end.

resolve_data_dir() ->
    case disksup:get_disk_info(rabbit:data_dir()) of
        [{"none", 0, 0, 0}] ->
            {error, disksup_not_available};
        [{MountPoint, 0, 0, 0}] ->
            {error, {cannot_determine_space, MountPoint}};
        [{MountPoint, _TotalKiB, _AvailableKiB, _Capacity}] ->
            {ok, MountPoint};
        [] ->
            {error, no_disk_info};
        [_ | _] = Infos ->
            {error, {multiple_disks, length(Infos)}}
    end.
