%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% This module is a debugging utility mainly meant for debugging
%% test failures. It should not be used in production in its
%% current form.
%%
%% The tracer is started and configured against a pid and a set
%% of applications. Then update the code where you need to
%% obtain events, such as a crash. Wrapping the crash in
%% a try/catch and adding rabbit_time_travel_dbg:print()
%% will print up to 1000 events before the crash occured,
%% allowing you to easily figure out what happened.

-module(rabbit_time_travel_dbg).
-compile(export_all).
-compile(nowarn_export_all).

start(Pid) ->
    start(Pid, [rabbit, rabbit_common]).

start(Pid, Apps) ->
    Mods = apps_to_mods(Apps, []),
    TracerPid = spawn_link(?MODULE, init, []),
    {ok, _} = dbg:tracer(process, {fun (Msg, _) -> TracerPid ! Msg end, []}),
    _ = [dbg:tpl(M, []) || M <- Mods],
    dbg:p(Pid, [c]),
    ok.

apps_to_mods([], Acc) ->
    lists:flatten(Acc);
apps_to_mods([App|Tail], Acc) ->
    _ = application:load(App),
    {ok, Mods} = application:get_key(App, modules),
    apps_to_mods(Tail, [Mods|Acc]).

dump() ->
    ?MODULE ! dump,
    ok.

print() ->
    ?MODULE ! print,
    ok.

stop() ->
    dbg:stop_clear(),
    ?MODULE ! stop,
    ok.

init() ->
    register(?MODULE, self()),
    loop(queue:new()).

loop(Q) ->
    receive
        dump ->
            _ = file:write_file("time_travel.dbg",
                [io_lib:format("~0p~n", [E]) || E <- queue:to_list(Q)]),
            loop(Q);
        print ->
            _ = [logger:error("~0p", [E]) || E <- queue:to_list(Q)],
            loop(Q);
        stop ->
            ok;
        Msg ->
            case queue:len(Q) of
                1000 ->
                    {_, Q1} = queue:out(Q),
                    loop(queue:in(Msg, Q1));
                _ ->
                    loop(queue:in(Msg, Q))
            end
    end.
