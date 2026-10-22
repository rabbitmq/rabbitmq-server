%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_logger_fmt_helpers_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile([export_all, nowarn_export_all]).

all() ->
    [crash_report_formatting,
     format_msg_with_large_args].

crash_report_formatting(_Config) ->
    Big = binary:copy(<<"a">>, 16 bsl 20),
    Deep = lists:foldl(fun(_, Acc) -> [Acc] end, x, lists:seq(1, 100_000)),
    Info = [{initial_call, {?MODULE, init, []}},
            {pid, self()},
            {registered_name, []},
            {error_info, {error, {badmatch, {Big, Deep}}, []}},
            {ancestors, []},
            {message_queue_len, 0},
            {messages, []},
            {links, []},
            {dictionary, []},
            {trap_exit, false},
            {status, running},
            {heap_size, 1},
            {stack_size, 1},
            {reductions, 1}],
    Msg = {report, #{label => {proc_lib, crash}, report => [Info, []]}},
    Meta = #{report_cb => fun proc_lib:report_cb/2},
    Str = format_msg_in_process(Msg, Meta),
    ?assertNotEqual(nomatch, string:find(Str, "crasher")).

format_msg_with_large_args(_Config) ->
    Big = binary:copy(<<"a">>, 16 bsl 20),
    _ = format_msg_in_process({"~0p", [Big]}, #{}).

format_msg_in_process(Msg, Meta) ->
    Parent = self(),
    {Pid, MRef} = spawn_opt(
                    fun() ->
                            Str = lists:flatten(
                                    rabbit_logger_fmt_helpers:format_msg(Msg, Meta, #{})),
                            Parent ! {self(), Str}
                    end,
                    [monitor,
                     {max_heap_size, #{size => 32 bsl 20,
                                       kill => true,
                                       error_logger => false}}]),
    receive
        {Pid, Str} ->
            erlang:demonitor(MRef, [flush]),
            Str;
        {'DOWN', MRef, process, Pid, Reason} ->
            ct:fail({format_msg_failed, Reason})
    end.
