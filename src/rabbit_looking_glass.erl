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
%% Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
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
