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
%% Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_disk_monitor).

-behaviour(gen_server).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([get_disk_free_limit/0, set_disk_free_limit/1,
         get_min_check_interval/0, set_min_check_interval/1,
         get_max_check_interval/0, set_max_check_interval/1,
         get_disk_free/0]).

-define(SERVER, ?MODULE).
-define(DEFAULT_MIN_DISK_CHECK_INTERVAL, 100).
-define(DEFAULT_MAX_DISK_CHECK_INTERVAL, 10000).
%% 250MB/s i.e. 250kB/ms
-define(FAST_RATE, (250 * 1000)).

-record(state, {dir,
                limit,
                actual,
                min_interval,
                max_interval,
                timer,
                alarmed
               }).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(disk_free_limit() :: (integer() | {'mem_relative', float()})).
-spec(start_link/1 :: (disk_free_limit()) -> rabbit_types:ok_pid_or_error()).
-spec(get_disk_free_limit/0 :: () -> integer()).
-spec(set_disk_free_limit/1 :: (disk_free_limit()) -> 'ok').
-spec(get_min_check_interval/0 :: () -> integer()).
-spec(set_min_check_interval/1 :: (integer()) -> 'ok').
-spec(get_max_check_interval/0 :: () -> integer()).
-spec(set_max_check_interval/1 :: (integer()) -> 'ok').
-spec(get_disk_free/0 :: () -> (integer() | 'unknown')).

-endif.

%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------

get_disk_free_limit() ->
    gen_server:call(?MODULE, get_disk_free_limit, infinity).

set_disk_free_limit(Limit) ->
    gen_server:call(?MODULE, {set_disk_free_limit, Limit}, infinity).

get_min_check_interval() ->
    gen_server:call(?MODULE, get_min_check_interval, infinity).

set_min_check_interval(Interval) ->
    gen_server:call(?MODULE, {set_min_check_interval, Interval}, infinity).

get_max_check_interval() ->
    gen_server:call(?MODULE, get_max_check_interval, infinity).

set_max_check_interval(Interval) ->
    gen_server:call(?MODULE, {set_max_check_interval, Interval}, infinity).

get_disk_free() ->
    gen_server:call(?MODULE, get_disk_free, infinity).

%%----------------------------------------------------------------------------
%% gen_server callbacks
%%----------------------------------------------------------------------------

start_link(Args) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Args], []).

init([Limit]) ->
    Dir = dir(),
    State = #state{dir          = Dir,
                   min_interval = ?DEFAULT_MIN_DISK_CHECK_INTERVAL,
                   max_interval = ?DEFAULT_MAX_DISK_CHECK_INTERVAL,
                   alarmed      = false},
    case {catch get_disk_free(Dir),
          vm_memory_monitor:get_total_memory()} of
        {N1, N2} when is_integer(N1), is_integer(N2) ->
            {ok, start_timer(set_disk_limits(State, Limit))};
        Err ->
            rabbit_log:info("Disabling disk free space monitoring "
                            "on unsupported platform: ~p~n", [Err]),
            {stop, unsupported_platform}
    end.

handle_call(get_disk_free_limit, _From, State) ->
    {reply, interpret_limit(State#state.limit), State};

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

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

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

set_disk_limits(State, Limit) ->
    State1 = State#state { limit = Limit },
    rabbit_log:info("Disk free limit set to ~pMB~n",
                    [trunc(interpret_limit(Limit) / 1000000)]),
    internal_update(State1).

internal_update(State = #state { limit   = Limit,
                                 dir     = Dir,
                                 alarmed = Alarmed}) ->
    CurrentFreeBytes = get_disk_free(Dir),
    LimitBytes = interpret_limit(Limit),
    NewAlarmed = CurrentFreeBytes < LimitBytes,
    case {Alarmed, NewAlarmed} of
        {false, true} ->
            emit_update_info("insufficient", CurrentFreeBytes, LimitBytes),
            rabbit_alarm:set_alarm({{resource_limit, disk, node()}, []});
        {true, false} ->
            emit_update_info("sufficient", CurrentFreeBytes, LimitBytes),
            rabbit_alarm:clear_alarm({resource_limit, disk, node()});
        _ ->
            ok
    end,
    State #state {alarmed = NewAlarmed, actual = CurrentFreeBytes}.

get_disk_free(Dir) ->
    get_disk_free(Dir, os:type()).

get_disk_free(Dir, {unix, Sun})
  when Sun =:= sunos; Sun =:= sunos4; Sun =:= solaris ->
    parse_free_unix(rabbit_misc:os_cmd("/usr/bin/df -k " ++ Dir));
get_disk_free(Dir, {unix, _}) ->
    parse_free_unix(rabbit_misc:os_cmd("/bin/df -kP " ++ Dir));
get_disk_free(Dir, {win32, _}) ->
    parse_free_win32(rabbit_misc:os_cmd("dir /-C /W \"" ++ Dir ++ [$"]));
get_disk_free(_, Platform) ->
    {unknown, Platform}.

parse_free_unix(CommandResult) ->
    [_, Stats | _] = string:tokens(CommandResult, "\n"),
    [_FS, _Total, _Used, Free | _] = string:tokens(Stats, " \t"),
    list_to_integer(Free) * 1024.

parse_free_win32(CommandResult) ->
    LastLine = lists:last(string:tokens(CommandResult, "\r\n")),
    {match, [Free]} = re:run(lists:reverse(LastLine), "(\\d+)",
                             [{capture, all_but_first, list}]),
    list_to_integer(lists:reverse(Free)).

interpret_limit({mem_relative, R}) ->
    round(R * vm_memory_monitor:get_total_memory());
interpret_limit(L) ->
    L.

emit_update_info(StateStr, CurrentFree, Limit) ->
    rabbit_log:info(
      "Disk free space ~s. Free bytes:~p Limit:~p~n",
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
