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
%% Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_diagnostics).

-define(PROCESS_INFO,
        [current_stacktrace, initial_call, dictionary, message_queue_len,
         links, monitors, monitored_by, heap_size]).

-export([maybe_stuck/0, maybe_stuck/1]).

maybe_stuck() -> maybe_stuck(5000).

maybe_stuck(Timeout) ->
    Pids = processes(),
    io:format("There are ~p processes.~n", [length(Pids)]),
    maybe_stuck(Pids, Timeout).

maybe_stuck(Pids, Timeout) when Timeout =< 0 ->
    io:format("Found ~p suspicious processes.~n", [length(Pids)]),
    [io:format("~p~n", [info(Pid)]) || Pid <- Pids],
    ok;
maybe_stuck(Pids, Timeout) ->
    Pids2 = [P || P  <- Pids, looks_stuck(P)],
    io:format("Investigated ~p processes this round, ~pms to go.~n",
              [length(Pids2), Timeout]),
    timer:sleep(500),
    maybe_stuck(Pids2, Timeout - 500).

looks_stuck(Pid) ->
    case process_info(Pid, status) of
        {status, waiting} ->
            %% It's tempting to just check for message_queue_len > 0
            %% here rather than mess around with stack traces and
            %% heuristics. But really, sometimes freshly stuck
            %% processes can have 0 messages...
            case erlang:process_info(Pid, current_stacktrace) of
                {current_stacktrace, [H|_]} ->
                    maybe_stuck_stacktrace(H);
                _ ->
                    false
            end;
        _ ->
            false
    end.

maybe_stuck_stacktrace({gen_server2,      process_next_msg, _}) -> false;
maybe_stuck_stacktrace({gen_event,        fetch_msg,        _}) -> false;
maybe_stuck_stacktrace({prim_inet,        accept0,          _}) -> false;
maybe_stuck_stacktrace({prim_inet,        recv0,            _}) -> false;
maybe_stuck_stacktrace({rabbit_heartbeat, heartbeater,      _}) -> false;
maybe_stuck_stacktrace({rabbit_net,       recv,             _}) -> false;
maybe_stuck_stacktrace({mochiweb_http,    request,          _}) -> false;
maybe_stuck_stacktrace({group,            _,                _}) -> false;
maybe_stuck_stacktrace({shell,            _,                _}) -> false;
maybe_stuck_stacktrace({io,               _,                _}) -> false;
maybe_stuck_stacktrace({M, F, A, _}) ->
    maybe_stuck_stacktrace({M, F, A});
maybe_stuck_stacktrace({_M, F, _A}) ->
    case string:str(atom_to_list(F), "loop") of
        0 -> true;
        _ -> false
    end.

info(Pid) ->
    [{pid, Pid} | process_info(Pid, ?PROCESS_INFO)].
