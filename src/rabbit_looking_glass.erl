%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_looking_glass).

-ignore_xref([{lg, trace, 4}]).
-ignore_xref([{maps, from_list, 1}]).

-export([boot/0]).
-export([connections/0]).

boot() ->
    case os:getenv("RABBITMQ_TRACER") of
        false ->
            ok;
        Value ->
            Input = parse_value(Value),
            rabbit_log:info(
                "Enabling Looking Glass profiler, input value: ~p",
                [Input]
            ),
            {ok, _} = application:ensure_all_started(looking_glass),
            lg:trace(
                Input,
                lg_file_tracer,
                "traces.lz4",
                maps:from_list([
                    {mode, profile},
                    {process_dump, true},
                    {running, true},
                    {send, true}]
                )
             )
    end.

parse_value(Value) ->
    [begin
        [Mod, Fun] = string:tokens(C, ":"),
        {callback, list_to_atom(Mod), list_to_atom(Fun)}
    end || C <- string:tokens(Value, ",")].

connections() ->
    Pids = [Pid || {{conns_sup, _}, Pid} <- ets:tab2list(ranch_server)],
    ['_', {scope, Pids}].
