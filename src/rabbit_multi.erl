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
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2010 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2010 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_multi).
-include("rabbit.hrl").

-export([start/0, stop/0]).

-define(RPC_SLEEP, 500).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start/0 :: () -> no_return()).
-spec(stop/0 :: () -> 'ok').
-spec(usage/0 :: () -> no_return()).

-endif.

%%----------------------------------------------------------------------------

start() ->
    RpcTimeout =
        case init:get_argument(maxwait) of
            {ok,[[N1]]} -> 1000 * list_to_integer(N1);
            _           -> ?MAX_WAIT
        end,
    case init:get_plain_arguments() of
        [] ->
            usage();
        FullCommand ->
            {Command, Args} = parse_args(FullCommand),
            case catch action(Command, Args, RpcTimeout) of
                ok ->
                    io:format("done.~n"),
                    halt();
                {'EXIT', {function_clause, [{?MODULE, action, _} | _]}} ->
                    error("invalid command '~s'",
                          [lists:flatten(
                             rabbit_misc:intersperse(" ", FullCommand))]),
                    usage();
                timeout ->
                    error("timeout starting some nodes.", []),
                    halt(1);
                Other ->
                    error("~p", [Other]),
                    halt(2)
            end
    end.

error(Format, Args) ->
    rabbit_misc:format_stderr("Error: " ++ Format ++ "~n", Args).

parse_args([Command | Args]) ->
    {list_to_atom(Command), Args}.

stop() ->
    ok.

usage() ->
    io:format("~s", [rabbit_multi_usage:usage()]),
    halt(1).

action(start_all, [NodeCount], RpcTimeout) ->
    io:format("Starting all nodes...~n", []),
    application:load(rabbit),
    NodeName = rabbit_misc:nodeparts(getenv("RABBITMQ_NODENAME")),
    {NodePids, Running} =
        case list_to_integer(NodeCount) of
            1 -> {NodePid, Started} = start_node(rabbit_misc:makenode(NodeName),
                                                 RpcTimeout),
                 {[NodePid], Started};
            N -> start_nodes(N, N, [], true, NodeName,
                             get_node_tcp_listener(), RpcTimeout)
        end,
    write_pids_file(NodePids),
    case Running of
        true  -> ok;
        false -> timeout
    end;

action(status, [], RpcTimeout) ->
    io:format("Status of all running nodes...~n", []),
    call_all_nodes(
      fun({Node, Pid}) ->
              RabbitRunning =
                  case is_rabbit_running(Node, RpcTimeout) of
                      false -> not_running;
                      true  -> running
                  end,
              io:format("Node '~p' with Pid ~p: ~p~n",
                        [Node, Pid, RabbitRunning])
      end);

action(stop_all, [], RpcTimeout) ->
    io:format("Stopping all nodes...~n", []),
    call_all_nodes(fun({Node, Pid}) ->
                           io:format("Stopping node ~p~n", [Node]),
                           rpc:call(Node, rabbit, stop_and_halt, []),
                           case kill_wait(Pid, RpcTimeout, false) of
                               false -> kill_wait(Pid, RpcTimeout, true);
                               true  -> ok
                           end,
                           io:format("OK~n", [])
                   end),
    delete_pids_file();

action(rotate_logs, [], RpcTimeout) ->
    action(rotate_logs, [""], RpcTimeout);

action(rotate_logs, [Suffix], RpcTimeout) ->
    io:format("Rotating logs for all nodes...~n", []),
    BinarySuffix = list_to_binary(Suffix),
    call_all_nodes(
      fun ({Node, _}) ->
              io:format("Rotating logs for node ~p", [Node]),
              case rpc:call(Node, rabbit, rotate_logs,
                            [BinarySuffix], RpcTimeout) of
                  {badrpc, Error} -> io:format(": ~p.~n", [Error]);
                  ok              -> io:format(": ok.~n", [])
              end
      end).

%% PNodePid is the list of PIDs
%% Running is a boolean exhibiting success at some moment
start_nodes(0, _, PNodePid, Running, _, _, _) -> {PNodePid, Running};

start_nodes(N, Total, PNodePid, Running, NodeNameBase, Listener, RpcTimeout) ->
    {NodePre, NodeSuff} = NodeNameBase,
    NodeNumber = Total - N,
    NodePre1 = case NodeNumber of
                   %% For compatibility with running a single node
                   0 -> NodePre;
                   _ -> NodePre ++ "_" ++ integer_to_list(NodeNumber)
               end,
    Node = rabbit_misc:makenode({NodePre1, NodeSuff}),
    os:putenv("RABBITMQ_NODENAME", atom_to_list(Node)),
    case Listener of
        {NodeIpAddress, NodePortBase} ->
            NodePort = NodePortBase + NodeNumber,
            os:putenv("RABBITMQ_NODE_PORT", integer_to_list(NodePort)),
            os:putenv("RABBITMQ_NODE_IP_ADDRESS", NodeIpAddress);
        undefined ->
            ok
    end,
    {NodePid, Started} = start_node(Node, RpcTimeout),
    start_nodes(N - 1, Total, [NodePid | PNodePid],
                Started and Running, NodeNameBase, Listener, RpcTimeout).

start_node(Node, RpcTimeout) ->
    io:format("Starting node ~s...~n", [Node]),
    case rpc:call(Node, os, getpid, []) of
        {badrpc, _} ->
            Port = run_rabbitmq_server(),
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
    case is_rabbit_running(Node, RpcTimeout) of
        true  -> true;
        false -> receive
                     {'EXIT', Port, PosixCode} ->
                         throw({node_start_failed, PosixCode})
                 after ?RPC_SLEEP ->
                         wait_for_rabbit_to_start(
                           Node, RpcTimeout - ?RPC_SLEEP, Port)
                 end
    end.

run_rabbitmq_server() ->
    with_os([{unix, fun run_rabbitmq_server_unix/0},
             {win32, fun run_rabbitmq_server_win32/0}]).

run_rabbitmq_server_unix() ->
    CmdLine = getenv("RABBITMQ_SCRIPT_HOME") ++ "/rabbitmq-server -noinput",
    erlang:open_port({spawn, CmdLine}, [nouse_stdio]).

run_rabbitmq_server_win32() ->
    Cmd = filename:nativename(os:find_executable("cmd")),
    CmdLine = "\"" ++ getenv("RABBITMQ_SCRIPT_HOME")
                                         ++ "\\rabbitmq-server.bat\" -noinput",
    erlang:open_port({spawn_executable, Cmd},
                     [{arg0, Cmd}, {args, ["/q", "/s", "/c", CmdLine]},
                      nouse_stdio, hide]).

is_rabbit_running(Node, RpcTimeout) ->
    case rpc:call(Node, rabbit, status, [], RpcTimeout) of
        {badrpc, _} -> false;
        Status      -> case proplists:get_value(running_applications, Status) of
                           undefined -> false;
                           Apps      -> lists:keymember(rabbit, 1, Apps)
                       end
    end.

with_os(Handlers) ->
    {OsFamily, _} = os:type(),
    case proplists:get_value(OsFamily, Handlers) of
        undefined -> throw({unsupported_os, OsFamily});
        Handler   -> Handler()
    end.

pids_file() -> getenv("RABBITMQ_PIDS_FILE").

write_pids_file(Pids) ->
    FileName = pids_file(),
    Handle = case file:open(FileName, [write]) of
                 {ok, Device} ->
                     Device;
                 {error, Reason} ->
                     throw({cannot_create_pids_file, FileName, Reason})
             end,
    try
        ok = io:write(Handle, Pids),
        ok = io:put_chars(Handle, [$.])
    after
        case file:close(Handle) of
            ok -> ok;
            {error, Reason1} ->
                throw({cannot_create_pids_file, FileName, Reason1})
        end
    end,
    ok.

delete_pids_file() ->
    FileName = pids_file(),
    case file:delete(FileName) of
        ok              -> ok;
        {error, enoent} -> ok;
        {error, Reason} -> throw({cannot_delete_pids_file, FileName, Reason})
    end.

read_pids_file() ->
    FileName = pids_file(),
    case file:consult(FileName) of
        {ok, [Pids]}    -> Pids;
        {error, enoent} -> [];
        {error, Reason} -> throw({cannot_read_pids_file, FileName, Reason})
    end.

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

call_all_nodes(Func) ->
    case read_pids_file() of
        []       -> throw(no_nodes_running);
        NodePids -> lists:foreach(Func, NodePids)
    end.

getenv(Var) ->
    case os:getenv(Var) of
        false -> throw({missing_env_var, Var});
        Value -> Value
    end.

get_node_tcp_listener() ->
    try
        {getenv("RABBITMQ_NODE_IP_ADDRESS"),
         list_to_integer(getenv("RABBITMQ_NODE_PORT"))}
    catch _ ->
            case application:get_env(rabbit, tcp_listeners) of
                {ok, [{_IpAddy, _Port} = Listener]} ->
                    Listener;
                {ok, []} ->
                    undefined;
                {ok, Other} ->
                    throw({cannot_start_multiple_nodes, multiple_tcp_listeners,
                           Other});
                undefined ->
                    throw({missing_configuration, tcp_listeners})
            end
    end.
