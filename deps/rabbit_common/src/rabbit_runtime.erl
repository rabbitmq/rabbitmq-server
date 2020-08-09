%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2018 Pivotal Software, Inc.  All rights reserved.
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
    {ok, [[Root]]} = init:get_argument(root),
    Bin = filename:join(Root, "bin"),
    case os:type() of
        {win32, _} ->
            filename:join(Bin, "erl.exe");
        _ ->
            filename:join(Bin, "erl")
    end.
