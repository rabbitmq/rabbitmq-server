%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_nodes_common).

-define(EPMD_OPERATION_TIMEOUT, 6000).
-define(NAME_LOOKUP_ATTEMPTS, 10).
-define(TCP_DIAGNOSTIC_TIMEOUT, 5000).
-define(ERROR_LOGGER_HANDLER, rabbit_error_logger_handler).

-include_lib("kernel/include/inet.hrl").

%%
%% API
%%

-export([make/1, make/2, parts/1, names/1, name_type/1, ensure_epmd/0, is_running/2, is_process_running/2]).
-export([cookie_hash/0, epmd_port/0, diagnostics/1]).

-spec make({string(), string()} | string()) -> node().
-spec make(string(), string()) -> node().
-spec parts(node() | string()) -> {string(), string()}.
-spec ensure_epmd() -> 'ok'.
-spec epmd_port() -> string().

-spec names(string()) ->
          rabbit_types:ok_or_error2([{string(), integer()}], term()).
-spec epmd_names(string()) ->
  rabbit_types:ok_or_error2([{string(), integer()}], term()).

-spec diagnostics([node()]) -> string().
-spec cookie_hash() -> string().

%% net_adm:name/1 returns a new value, 'noport', in Erlang 24. This value being
%% absent in the function spec in previous versions of Erlang, we get a warning
%% from Dialyzer until we start to the yet-to-be-release Erlang 24 in CI.
%% Therefore we disable this specific warning.
-dialyzer({nowarn_function, diagnostics_node/1}).

%% In same case the hostname resolution can take a moment.
%% In K8s for example *.nodes.default needs some second.

names(Hostname) ->
  names(Hostname, ?NAME_LOOKUP_ATTEMPTS).

names(Hostname, 0) ->
  epmd_names(Hostname);
names(Hostname, RetriesLeft) ->
  _ = rabbit_log:debug("Getting epmd names for hostname '~s', ~b retries left",
    [Hostname, RetriesLeft]),
  case catch epmd_names(Hostname) of
    {ok, R } -> {ok, R};
    noport ->
      names(Hostname, RetriesLeft - 1);
    {error, _} ->
      names(Hostname, RetriesLeft - 1)
  end.

epmd_names(Hostname) ->
    Self = self(),
    Ref = make_ref(),
    {Pid, MRef} = spawn_monitor(
                    fun () -> Self ! {Ref, net_adm:names(Hostname)} end),
    _ = timer:exit_after(?EPMD_OPERATION_TIMEOUT, Pid, timeout),
    receive
        {Ref, Names}                         -> erlang:demonitor(MRef, [flush]),
                                                Names;
        {'DOWN', MRef, process, Pid, Reason} -> {error, Reason}
    end.

make({Prefix, Suffix}) -> rabbit_data_coercion:to_atom(
                            lists:append([rabbit_data_coercion:to_list(Prefix),
                                          "@",
                                          rabbit_data_coercion:to_list(Suffix)]));
make(NodeStr)          -> make(parts(NodeStr)).

make(Prefix, Suffix) -> make({Prefix, Suffix}).

parts(Node) when is_atom(Node) ->
    parts(atom_to_list(Node));
parts(NodeStr) ->
    case lists:splitwith(fun (E) -> E =/= $@ end, NodeStr) of
        {Prefix, []}     -> {_, Suffix} = parts(node()),
                            {Prefix, Suffix};
        {Prefix, Suffix} -> {Prefix, tl(Suffix)}
    end.

name_type(Node) ->
    {_, HostPart} = parts(Node),
    case lists:member($., HostPart) of
        false -> shortnames;
        true  -> longnames
    end.

epmd_port() ->
    case init:get_argument(epmd_port) of
        {ok, [[Port | _] | _]} when is_list(Port) -> Port;
        error                                     -> "4369"
    end.

ensure_epmd() ->
    Exe = rabbit_runtime:get_erl_path(),
    ID = rabbit_misc:random(1000000000),
    Port = open_port(
             {spawn_executable, Exe},
             [{args, ["-boot", "no_dot_erlang",
                      "-sname", rabbit_misc:format("epmd-starter-~b", [ID]),
                      "-noinput", "-s", "erlang", "halt"]},
              exit_status, stderr_to_stdout, use_stdio]),
    port_shutdown_loop(Port).

port_shutdown_loop(Port) ->
    receive
        {Port, {exit_status, _Rc}} -> ok;
        {Port, closed}             -> ok;
        {Port, {data, _}}          -> port_shutdown_loop(Port);
        {'EXIT', Port, Reason}     ->
            rabbit_log:error("Failed to start a one-off Erlang VM to keep epmd alive: ~p", [Reason])
    after 15000 ->
        %% ensure the port is closed
        Port ! {self(), close},
        receive
            {Port, closed } -> ok
        after 5000 -> ok
        end
    end.

cookie_hash() ->
    base64:encode_to_string(erlang:md5(atom_to_list(erlang:get_cookie()))).

diagnostics(Nodes) ->
    verbose_erlang_distribution(true),
    NodeDiags = [{"~nDIAGNOSTICS~n===========~n~n"
                  "attempted to contact: ~p~n", [Nodes]}] ++
        [diagnostics_node(Node) || Node <- Nodes] ++
        current_node_details(),
    verbose_erlang_distribution(false),
    rabbit_misc:format_many(lists:flatten(NodeDiags)).

verbose_erlang_distribution(true) ->
    net_kernel:verbose(1),
    error_logger:add_report_handler(?ERROR_LOGGER_HANDLER);
verbose_erlang_distribution(false) ->
    net_kernel:verbose(0),
    error_logger:delete_report_handler(?ERROR_LOGGER_HANDLER).

current_node_details() ->
    [{"~nCurrent node details:~n * node name: ~w", [node()]},
     case init:get_argument(home) of
         {ok, [[Home]]} -> {" * effective user's home directory: ~s", [filename:absname(Home)]};
         Other          -> {" * effective user has no home directory: ~p", [Other]}
     end,
     {" * Erlang cookie hash: ~s", [cookie_hash()]}].

diagnostics_node(Node) ->
    {Name, Host} = parts(Node),
    [{"~s:", [Node]} |
     case names(Host) of
         {error, Reason} ->
             [{"  * unable to connect to epmd (port ~s) on ~s: ~s~n",
               [epmd_port(), Host, rabbit_misc:format_inet_error(Reason)]}];
         noport ->
             [{"  * unable to connect to epmd (port ~s) on ~s: "
               "couldn't resolve hostname~n",
               [epmd_port(), Host]}];
         {ok, NamePorts} ->
             [{"  * connected to epmd (port ~s) on ~s",
               [epmd_port(), Host]}] ++
                 case net_adm:ping(Node) of
                     pong -> dist_working_diagnostics(Node);
                     pang -> dist_broken_diagnostics(Name, Host, NamePorts)
                 end
     end].

dist_working_diagnostics(Node) ->
    case is_process_running(Node, rabbit) of
        true  -> [{"  * node ~s up, 'rabbit' application running", [Node]}];
        false -> [{"  * node ~s up, 'rabbit' application not running~n"
                   "  * running applications on ~s: ~p~n"
                   "  * suggestion: use rabbitmqctl start_app on ~s",
                   [Node, Node, remote_apps(Node), Node]}]
    end.

remote_apps(Node) ->
    %% We want a timeout here because really, we don't trust the node,
    %% the last thing we want to do is hang.
    case rpc:call(Node, application, which_applications, [5000]) of
        {badrpc, _} = E -> E;
        Apps            -> [App || {App, _, _} <- Apps]
    end.

dist_broken_diagnostics(Name, Host, NamePorts) ->
    case [{N, P} || {N, P} <- NamePorts, N =:= Name] of
        [] ->
            {SelfName, SelfHost} = parts(node()),
            Others = [list_to_atom(N) || {N, _} <- NamePorts,
                                         N =/= case SelfHost of
                                                   Host -> SelfName;
                                                   _    -> never_matches
                                               end],
            OthersDiag = case Others of
                             [] -> [{"                  no other nodes on ~s",
                                     [Host]}];
                             _  -> [{"                  other nodes on ~s: ~p",
                                     [Host, Others]}]
                         end,
            [{"  * epmd reports: node '~s' not running at all", [Name]},
             OthersDiag, {"  * suggestion: start the node", []}];
        [{Name, Port}] ->
            [{"  * epmd reports node '~s' uses port ~b for inter-node and CLI tool traffic ", [Name, Port]} |
             case diagnose_connect(Host, Port) of
                 ok ->
                     connection_succeeded_diagnostics();
                 {error, Reason} ->
                     [{"  * can't establish TCP connection to the target node, reason: ~s~n"
                       "  * suggestion: check if host '~s' resolves, is reachable and ports ~b, 4369 are not blocked by firewall",
                       [rabbit_misc:format_inet_error(Reason), Host, Port]}]
             end]
    end.

connection_succeeded_diagnostics() ->
    case gen_event:call(error_logger, ?ERROR_LOGGER_HANDLER, get_connection_report) of
        [] ->
            [{"  * TCP connection succeeded but Erlang distribution failed ~n"
              "  * suggestion: check if the Erlang cookie identical for all server nodes and CLI tools~n"
              "  * suggestion: check if all server nodes and CLI tools use consistent hostnames when addressing each other~n"
              "  * suggestion: check if inter-node connections may be configured to use TLS. If so, all nodes and CLI tools must do that~n"
             "   * suggestion: see the CLI, clustering and networking guides on https://rabbitmq.com/documentation.html to learn more~n", []}];
        Report ->
            [{"  * TCP connection succeeded but Erlang distribution "
              "failed ~n", []}]
                ++ Report
    end.

diagnose_connect(Host, Port) ->
    case inet:gethostbyname(Host) of
        {ok, #hostent{h_addrtype = Family}} ->
            case gen_tcp:connect(Host, Port, [Family],
                                 ?TCP_DIAGNOSTIC_TIMEOUT) of
                {ok, Socket}   -> gen_tcp:close(Socket),
                                  ok;
                {error, _} = E -> E
            end;
        {error, _} = E ->
            E
    end.

is_running(Node, Application) ->
    case rpc:call(Node, rabbit_misc, which_applications, []) of
        {badrpc, _} -> false;
        Apps        -> proplists:is_defined(Application, Apps)
    end.

is_process_running(Node, Process) ->
    case rpc:call(Node, erlang, whereis, [Process]) of
        {badrpc, _}      -> false;
        undefined        -> false;
        P when is_pid(P) -> true
    end.
