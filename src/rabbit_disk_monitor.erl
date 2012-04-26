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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_disk_monitor).

-behaviour(gen_server).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([get_disk_free_limit/0, set_disk_free_limit/1, get_check_interval/0,
         set_check_interval/1, get_disk_free/0]).

-define(SERVER, ?MODULE).
-define(DEFAULT_DISK_CHECK_INTERVAL, 60000).

-record(state, {dir,
                limit,
                timeout,
                timer,
                alarmed
               }).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(disk_free_limit() :: (integer() | {'mem_relative', float()})).
-spec(start_link/1 :: (disk_free_limit()) -> rabbit_types:ok_pid_or_error()).
-spec(get_disk_free_limit/0 :: () -> integer()).
-spec(set_disk_free_limit/1 :: (disk_free_limit()) -> 'ok').
-spec(get_check_interval/0 :: () -> integer()).
-spec(set_check_interval/1 :: (integer()) -> 'ok').
-spec(get_disk_free/0 :: () -> (integer() | 'unknown')).

-endif.

%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------

get_disk_free_limit() ->
    gen_server:call(?MODULE, get_disk_free_limit, infinity).

set_disk_free_limit(Limit) ->
    gen_server:call(?MODULE, {set_disk_free_limit, Limit}, infinity).

get_check_interval() ->
    gen_server:call(?MODULE, get_check_interval, infinity).

set_check_interval(Interval) ->
    gen_server:call(?MODULE, {set_check_interval, Interval}, infinity).

get_disk_free() ->
    gen_server:call(?MODULE, get_disk_free, infinity).

%%----------------------------------------------------------------------------
%% gen_server callbacks
%%----------------------------------------------------------------------------

start_link(Args) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Args], []).

init([Limit]) ->
    TRef = start_timer(?DEFAULT_DISK_CHECK_INTERVAL),
    Dir = dir(),
    State = #state { dir     = Dir,
                     timeout = ?DEFAULT_DISK_CHECK_INTERVAL,
                     timer   = TRef,
                     alarmed = false},
    case {catch get_disk_free(Dir),
          vm_memory_monitor:get_total_memory()} of
        {N1, N2} when is_integer(N1), is_integer(N2) ->
            {ok, set_disk_limits(State, Limit)};
        Err ->
            rabbit_log:info("Disabling disk free space monitoring "
                            "on unsupported platform: ~p~n", [Err]),
            {stop, unsupported_platform}
    end.

handle_call(get_disk_free_limit, _From, State) ->
    {reply, interpret_limit(State#state.limit), State};

handle_call({set_disk_free_limit, Limit}, _From, State) ->
    {reply, ok, set_disk_limits(State, Limit)};

handle_call(get_check_interval, _From, State) ->
    {reply, State#state.timeout, State};

handle_call({set_check_interval, Timeout}, _From, State) ->
    {ok, cancel} = timer:cancel(State#state.timer),
    {reply, ok, State#state{timeout = Timeout, timer = start_timer(Timeout)}};

handle_call(get_disk_free, _From, State = #state { dir = Dir }) ->
    {reply, get_disk_free(Dir), State};

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

% the partition / drive containing this directory will be monitored
dir() -> rabbit_mnesia:dir().

set_disk_limits(State, Limit) ->
    State1 = State#state { limit = Limit },
    rabbit_log:info("Disk free limit set to ~pMB~n",
                    [trunc(interpret_limit(Limit) / 1048576)]),
    internal_update(State1).

internal_update(State = #state { limit   = Limit,
                                 dir     = Dir,
                                 alarmed = Alarmed}) ->
    CurrentFreeBytes = get_disk_free(Dir),
    LimitBytes = interpret_limit(Limit),
    NewAlarmed = CurrentFreeBytes < LimitBytes,
    case {Alarmed, NewAlarmed} of
        {false, true} ->
            emit_update_info("exceeded", CurrentFreeBytes, LimitBytes),
            alarm_handler:set_alarm({{resource_limit, disk, node()}, []});
        {true, false} ->
            emit_update_info("below limit", CurrentFreeBytes, LimitBytes),
            alarm_handler:clear_alarm({resource_limit, disk, node()});
        _ ->
            ok
    end,
    State #state {alarmed = NewAlarmed}.

get_disk_free(Dir) ->
    get_disk_free(Dir, os:type()).

get_disk_free(Dir, {unix, Sun})
  when Sun =:= sunos; Sun =:= sunos4; Sun =:= solaris ->
    parse_free_unix(rabbit_misc:os_cmd("/usr/bin/df -k " ++ Dir));
get_disk_free(Dir, {unix, _}) ->
    parse_free_unix(rabbit_misc:os_cmd("/bin/df -kP " ++ Dir));
get_disk_free(Dir, {win32, _}) ->
    parse_free_win32(os:cmd("dir /-C /W \"" ++ Dir ++ [$"]));
get_disk_free(_, _) ->
    unknown.

parse_free_unix(CommandResult) ->
    [_, Stats | _] = string:tokens(CommandResult, "\n"),
    [_FS, _Total, _Used, Free | _] = string:tokens(Stats, " \t"),
    list_to_integer(Free) * 1024.

parse_free_win32(CommandResult) ->
    LastLine = lists:last(string:tokens(CommandResult, "\r\n")),
    [_, _Dir, Free, "bytes", "free"] = string:tokens(LastLine, " "),
    list_to_integer(Free).

interpret_limit({mem_relative, R}) ->
    round(R * vm_memory_monitor:get_total_memory());
interpret_limit(L) ->
    L.

emit_update_info(State, CurrentFree, Limit) ->
    rabbit_log:info(
      "Disk free space limit now ~s. Free bytes:~p Limit:~p~n",
      [State, CurrentFree, Limit]).

start_timer(Timeout) ->
    {ok, TRef} = timer:send_interval(Timeout, update),
    TRef.
