%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% This module provides access to runtime metrics that are exposed
%% via CLI tools, management UI or otherwise used by the broker.

-module(rabbit_runtime).

%%
%% API
%%

-export([guess_number_of_cpu_cores/0, msacc_stats/1]).
-export([get_gc_info/1, gc_all_processes/0]).
-export([get_erl_path/0]).
-export([ulimit/0]).

-spec guess_number_of_cpu_cores() -> pos_integer().
guess_number_of_cpu_cores() ->
    case erlang:system_info(logical_processors_available) of
        unknown -> % Happens on Mac OS X.
            erlang:system_info(schedulers);
        N -> N
    end.

-spec gc_all_processes() -> ok.
gc_all_processes() ->
  %% Run GC asynchronously. We don't care for completion notifications, so
  %% don't use the asynchonous execution option.
  spawn(fun() -> [erlang:garbage_collect(P, []) || P <- erlang:processes()] end),
  ok.

-spec get_gc_info(pid()) -> nonempty_list(tuple()).
get_gc_info(Pid) ->
    {garbage_collection, GC} = erlang:process_info(Pid, garbage_collection),
    case proplists:get_value(max_heap_size, GC) of
        I when is_integer(I) ->
            GC;
        undefined ->
            GC;
        Map ->
            lists:keyreplace(max_heap_size, 1, GC,
                             {max_heap_size, maps:get(size, Map)})
    end.

-spec msacc_stats(integer()) -> nonempty_list(#{atom() => any()}).
msacc_stats(TimeInMs) ->
    msacc:start(TimeInMs),
    S = msacc:stats(),
    msacc:stop(),
    S.

% get the full path to the erl executable used to start this VM
-spec get_erl_path() -> file:filename_all().
get_erl_path() ->
    {ok, [[BinDir]]} = init:get_argument(bindir),
    case os:type() of
        {win32, _} ->
            filename:join(BinDir, "erl.exe");
        _ ->
            filename:join(BinDir, "erl")
    end.

%% To increase the number of file descriptors: on Windows set ERL_MAX_PORTS
%% environment variable, on Linux set `ulimit -n`.
ulimit() ->
    IOStats = case erlang:system_info(check_io) of
        [Val | _] when is_list(Val) -> Val;
        Val when is_list(Val)       -> Val;
        _Other                      -> []
    end,
    case proplists:get_value(max_fds, IOStats) of
        MaxFds when is_integer(MaxFds) andalso MaxFds > 1 ->
            case os:type() of
                {win32, _OsName} ->
                    %% On Windows max_fds is twice the number of open files:
                    %%   https://github.com/erlang/otp/blob/64c8ae9966a720b9127f6d5a7e1fb4f9aeaca9b6/erts/emulator/sys/win32/sys.c#L2773-L2782
                    MaxFds div 2;
                _Any ->
                    %% For other operating systems trust Erlang.
                    MaxFds
            end;
        _ ->
            unknown
    end.
