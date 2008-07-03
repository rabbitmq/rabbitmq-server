%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd.,
%%   Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd., Cohesive Financial Technologies
%%   LLC., and Rabbit Technologies Ltd. are Copyright (C) 2007-2008
%%   LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit
%%   Technologies Ltd.;
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_multi).
-include("rabbit.hrl").

-export([start/0, stop/0]).

-define(RPC_SLEEP, 500).

start() ->
    RpcTimeout =
        case init:get_argument(maxwait) of
            {ok,[[N1]]} -> 1000 * list_to_integer(N1);
            _ -> 30000
        end,
    case init:get_plain_arguments() of
        [] ->
            usage();
        FullCommand ->
            {Command, Args} = parse_args(FullCommand),
            case catch action(Command, Args, RpcTimeout) of
                ok ->
                    io:format("done.~n"),
                    init:stop();
                {'EXIT', {function_clause, [{?MODULE, action, _} | _]}} ->
                    io:format("Invalid command ~p~n", [FullCommand]),
                    usage();
                timeout ->
                    io:format("timeout starting some nodes.~n"),
                    halt(1);
                Other ->
                    io:format("~nrabbit_multi action ~p failed:~n~p~n",
                              [Command, Other]),
                    halt(2)
            end
    end.

parse_args([Command | Args]) ->
    {list_to_atom(Command), Args}.

stop() ->
    ok.

usage() ->
    io:format("Usage: rabbitmq-multi <command>

Available commands:

  start_all <NodeCount> - start a local cluster of RabbitMQ nodes.
  stop_all              - stops all local RabbitMQ nodes.
"),
    halt(3).

action(start_all, [NodeCount], RpcTimeout) ->
    io:format("Starting all nodes...~n", []),
    N = list_to_integer(NodeCount),
    {NodePids, Running} = start_nodes(N, N, [], true,
                                      getenv("NODENAME"),
                                      getenv("NODE_PORT"),
                                      RpcTimeout),
    write_pids_file(NodePids),
    case Running of
        true  -> ok;
        false -> timeout
    end;

action(stop_all, [], RpcTimeout) ->
    io:format("Stopping all nodes...~n", []),
    case read_pids_file() of
        []       -> throw(no_nodes_running);
        NodePids -> stop_nodes(NodePids, RpcTimeout),
                    delete_pids_file()
    end.

%% PNodePid is the list of PIDs
%% Running is a boolean exhibiting success at some moment
start_nodes(0, _, PNodePid, Running, _, _, _) -> {PNodePid, Running};

start_nodes(N, Total, PNodePid, Running,
            NodeNameBase, NodePortBase, RpcTimeout) ->
    NodeNumber = Total - N,
    NodeName = if NodeNumber == 0 ->
                       %% For compatibility with running a single node
                       NodeNameBase;
                  true ->           
                       NodeNameBase ++ "_" ++ integer_to_list(NodeNumber)
               end,
    {NodePid, Started} = start_node(NodeName,
                                    list_to_integer(NodePortBase) + NodeNumber,
                                    RpcTimeout),
    start_nodes(N - 1, Total, [NodePid | PNodePid],
                Started and Running,
                NodeNameBase, NodePortBase, RpcTimeout).

start_node(NodeName, NodePort, RpcTimeout) ->
    os:putenv("NODENAME", NodeName),
    os:putenv("NODE_PORT", integer_to_list(NodePort)),
    Node = rabbit_misc:localnode(list_to_atom(NodeName)),
    io:format("Starting node ~s...~n", [Node]),
    case rpc:call(Node, os, getpid, []) of
        {badrpc, _} ->
            Port = run_cmd(script_filename()),
            Started = wait_for_rabbit_to_start(Node, RpcTimeout, Port),
            Pid = case rpc:call(Node, os, getpid, []) of
                      {badrpc, _} -> throw(cannot_get_pid);
                      PidS -> list_to_integer(PidS)
                  end,
            io:format("~s~n", [case Started of
                                   true  -> "OK";
                                   false -> "timeout"
                               end]),
            {{Node, Pid}, Started};
        PidS ->
            Pid = list_to_integer(PidS),
            throw({node_already_running, Node, Pid})
    end.

wait_for_rabbit_to_start(_ , RpcTimeout, _) when RpcTimeout < 0 ->
    false;
wait_for_rabbit_to_start(Node, RpcTimeout, Port) ->
    case parse_status(rpc:call(Node, rabbit, status, [])) of
        true  -> true;
        false -> receive
                     {'EXIT', Port, PosixCode} ->
                         throw({node_start_failed, PosixCode})
                 after ?RPC_SLEEP ->
                         wait_for_rabbit_to_start(
                           Node, RpcTimeout - ?RPC_SLEEP, Port)
                 end
    end.

run_cmd(FullPath) ->
    erlang:open_port({spawn, FullPath}, [nouse_stdio]).

parse_status({badrpc, _}) ->
    false;

parse_status(Status) ->
    case lists:keysearch(running_applications, 1, Status) of
        {value, {running_applications, Apps}} ->
            lists:keymember(rabbit, 1, Apps);
        _ ->
            false
    end.

with_os(Handlers) ->
    {OsFamily, _} = os:type(),
    case lists:keysearch(OsFamily, 1, Handlers) of
        {value, {_, Handler}} -> Handler();
        false -> throw({unsupported_os, OsFamily})
    end.

script_filename() ->
    ScriptHome = getenv("SCRIPT_HOME"),
    ScriptName = with_os(
                   [{unix , fun () -> "rabbitmq-server" end},
                    {win32, fun () -> "rabbitmq-server.bat" end}]),
    ScriptHome ++ "/" ++ ScriptName ++ " -noinput".

pids_file() -> getenv("PIDS_FILE").

write_pids_file(Pids) ->
    FileName = pids_file(),
    Handle = case file:open(FileName, [write]) of
                 {ok, Device} ->
                     Device;
                 {error, Reason} ->
                     throw({error, {cannot_create_pids_file,
                                    FileName, Reason}})
             end,
    try
        ok = io:write(Handle, Pids),
        ok = io:put_chars(Handle, [$.])
    after
        case file:close(Handle) of
            ok -> ok;
            {error, Reason1} ->
                throw({error, {cannot_create_pids_file,
                               FileName, Reason1}})
        end
    end,
    ok.

delete_pids_file() ->
    FileName = pids_file(),
    case file:delete(FileName) of
        ok              -> ok;
        {error, enoent} -> ok;
        {error, Reason} -> throw({error, {cannot_delete_pids_file,
                                          FileName, Reason}})
    end.

read_pids_file() ->
    FileName = pids_file(),
    case file:consult(FileName) of
        {ok, [Pids]}    -> Pids;
        {error, enoent} -> [];
        {error, Reason} -> throw({error, {cannot_read_pids_file,
                                          FileName, Reason}})
    end.

stop_nodes([],_) -> ok;

stop_nodes([NodePid | Rest], RpcTimeout) ->
    stop_node(NodePid, RpcTimeout),
    stop_nodes(Rest, RpcTimeout).

stop_node({Node, Pid}, RpcTimeout) ->
    io:format("Stopping node ~p~n", [Node]),
    rpc:call(Node, rabbit, stop_and_halt, []),
    case kill_wait(Pid, RpcTimeout, false) of
        false -> kill_wait(Pid, RpcTimeout, true);
    	true  -> ok
    end,
    io:format("OK~n", []).

kill_wait(Pid, TimeLeft, Forceful) when TimeLeft < 0 ->
    Cmd = with_os([{unix, fun () -> if Forceful -> "kill -9";
                                       true     -> "kill"
                                    end
                          end},
                   %% Kill forcefully always on Windows, since erl.exe
                   %% seems to completely ignore non-forceful killing
                   %% even when everything is working
                   {win32, fun () -> "taskkill /f /pid" end}]),
    os:cmd(Cmd ++ " " ++ integer_to_list(Pid)),
    false; % Don't assume what we did just worked!

% Returns true if the process is dead, false otherwise.
kill_wait(Pid, TimeLeft, Forceful) ->
    timer:sleep(?RPC_SLEEP),
    io:format(".", []),
    is_dead(Pid) orelse kill_wait(Pid, TimeLeft - ?RPC_SLEEP, Forceful).

% Test using some OS clunkiness since we shouldn't trust 
% rpc:call(os, getpid, []) at this point
is_dead(Pid) ->
    PidS = integer_to_list(Pid),
    with_os([{unix, fun () ->
                            Res = os:cmd("ps --no-headers --pid " ++ PidS),
                            Res == ""
                    end},
             {win32, fun () ->
                             Res = os:cmd("tasklist /nh /fi \"pid eq " ++
                                          PidS ++ "\""),
                             case regexp:first_match(Res, "erl.exe") of
                                 {match, _, _} -> false;
                                 _             -> true
                             end
                     end}]).

getenv(Var) ->
    case os:getenv(Var) of
        false -> throw({missing_env_var, Var});
        Value -> Value
    end.
