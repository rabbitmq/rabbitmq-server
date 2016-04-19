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
%% Copyright (c) 2007-2015 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_ct_broker_helpers).

-include_lib("common_test/include/ct.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-export([
    setup_steps/0,
    teardown_steps/0,
    start_rabbitmq_nodes/1,
    stop_rabbitmq_nodes/1,
    get_node_config/2,
    control_action/2, control_action/3, control_action/4,
    control_action_t/3, control_action_t/4, control_action_t/5,
    control_action_opts/1,
    info_action/3, info_action_t/4,
    add_test_path_to_broker/2,
    run_on_broker/4,
    run_on_broker_i/5,
    get_connection_pids/1,
    get_queue_sup_pid/1,
    test_channel/0
  ]).

-define(DEFAULT_USER, "guest").
-define(NODE_START_ATTEMPTS, 10).
-define(TCP_PORTS_BASE, 21000).
-define(TCP_PORTS_LIST, [
    tcp_port_amqp,
    tcp_port_amqp_tls,
    tcp_port_mgmt,
    tcp_port_erlang_dist
  ]).

%% -------------------------------------------------------------------
%% Broker setup/teardown steps.
%% -------------------------------------------------------------------

setup_steps() ->
    [
      fun start_rabbitmq_nodes/1
    ].

teardown_steps() ->
    [
      fun stop_rabbitmq_nodes/1
    ].

start_rabbitmq_nodes(Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_username, list_to_binary(?DEFAULT_USER)},
        {rmq_password, list_to_binary(?DEFAULT_USER)},
        {rmq_hostname, "localhost"},
        {rmq_vhost, <<"/">>},
        {rmq_channel_max, 0}]),
    NodesCount0 = rabbit_ct_helpers:get_config(Config1, rmq_nodes_count),
    {NodesCount, Clustered} = case NodesCount0 of
        undefined ->
            {1, false};
        {N, C} when is_integer(N) andalso N >= 1 andalso is_boolean(C) ->
            {N, C}
    end,
    Master = self(),
    Starters = [
      spawn_link(fun() -> start_rabbitmq_node(Master, Config1, [], I) end)
      || I <- lists:seq(0, NodesCount - 1)
    ],
    wait_for_rabbitmq_nodes(Config1, Starters, [], Clustered).

wait_for_rabbitmq_nodes(Config, [], NodeConfigs, Clustered) ->
    Config1 = rabbit_ct_helpers:set_config(Config, {rmq_nodes, NodeConfigs}),
    if
        Clustered ->
            Rabbitmqctl = ?config(rabbitmqctl_cmd, Config),
            Nodename = ?config(rmq_nodename, hd(NodeConfigs)),
            Cmd = Rabbitmqctl ++ " -n \"" ++ atom_to_list(Nodename) ++ "\"" ++
            " cluster_status",
            case rabbit_ct_helpers:run_cmd(Cmd) of
                true ->
                    Config1;
                false ->
                    stop_rabbitmq_nodes(Config1),
                    {skip, "Could not confirm cluster was up and running"}
            end;
        true ->
            Config1
    end;
wait_for_rabbitmq_nodes(Config, Starting, NodeConfigs, Clustered) ->
    receive
        {_, {skip, _} = Error} ->
            Config1 = rabbit_ct_helpers:set_config(Config,
              {rmq_nodes, NodeConfigs}),
            stop_rabbitmq_nodes(Config1),
            Error;
        {Pid, NodeConfig} when NodeConfigs =:= [] ->
            wait_for_rabbitmq_nodes(Config, Starting -- [Pid],
              [NodeConfig | NodeConfigs], Clustered);
        {Pid, NodeConfig} when not Clustered ->
            wait_for_rabbitmq_nodes(Config, Starting -- [Pid],
              [NodeConfig | NodeConfigs], Clustered);
        {Pid, NodeConfig} when Clustered ->
            case cluster_nodes(Config, NodeConfig, hd(NodeConfigs)) of
                ok ->
                    wait_for_rabbitmq_nodes(Config, Starting -- [Pid],
                      [NodeConfig | NodeConfigs], Clustered);
                {skip, _} = Error ->
                    Config1 = rabbit_ct_helpers:set_config(Config,
                      {rmq_nodes, [NodeConfig | NodeConfigs]}),
                    stop_rabbitmq_nodes(Config1),
                    Error
            end
    end.

%% To start a RabbitMQ node, we need to:
%%   1. Pick TCP port numbers
%%   2. Generate a node name
%%   3. Write a configuration file
%%   4. Start the node
%%
%% If this fails (usually because the node name is taken or a TCP port
%% is already in use), we start again with another set of TCP ports. The
%% node name is derived from the AMQP TCP port so a new node name is
%% generated.

start_rabbitmq_node(Master, Config, NodeConfig, I) ->
    Attempts0 = rabbit_ct_helpers:get_config(NodeConfig,
      rmq_failed_boot_attempts),
    Attempts = case Attempts0 of
        undefined -> 0;
        N         -> N
    end,
    NodeConfig1 = init_tcp_port_numbers(Config, NodeConfig, I),
    NodeConfig2 = init_nodename(Config, NodeConfig1, I),
    NodeConfig3 = init_config_filename(Config, NodeConfig2, I),
    Steps = [
      fun write_config_file/3,
      fun do_start_rabbitmq_node/3
    ],
    case run_node_steps(Config, NodeConfig3, I, Steps) of
        {skip, _} = Error
        when Attempts >= ?NODE_START_ATTEMPTS ->
            %% It's unlikely we'll ever succeed to start RabbitMQ.
            Master ! {self(), Error},
            unlink(Master);
        {skip, _} ->
            %% Try again with another TCP port numbers base.
            NodeConfig4 = move_nonworking_nodedir_away(NodeConfig3),
            NodeConfig5 = rabbit_ct_helpers:set_config(NodeConfig4,
              {rmq_failed_boot_attempts, Attempts + 1}),
            start_rabbitmq_node(Master, Config, NodeConfig5, I);
        NodeConfig4 ->
            Master ! {self(), NodeConfig4},
            unlink(Master)
    end.

run_node_steps(Config, NodeConfig, I, [Step | Rest]) ->
    case Step(Config, NodeConfig, I) of
        {skip, _} = Error -> Error;
        NodeConfig1       -> run_node_steps(Config, NodeConfig1, I, Rest)
    end;
run_node_steps(_, NodeConfig, _, []) ->
    NodeConfig.

init_tcp_port_numbers(_Config, NodeConfig, I) ->
    %% If there is no TCP port numbers base previously calculated,
    %% use the TCP port 21000. If a base was previously calculated,
    %% increment it by the number of TCP ports we may open.
    %%
    %% Port 21000 is an arbitrary choice. We don't want to use the
    %% default AMQP port of 5672 so other AMQP clients on the same host
    %% do not accidentally use the testsuite broker. There seems to be
    %% no registered service around this port in /etc/services. And it
    %% should be far enough away from the default ephemeral TCP ports
    %% range.
    TcpPortsCount = length(?TCP_PORTS_LIST),
    Base = case rabbit_ct_helpers:get_config(NodeConfig, tcp_ports_base) of
        undefined -> ?TCP_PORTS_BASE + I * TcpPortsCount * ?NODE_START_ATTEMPTS;
        P         -> P + TcpPortsCount
    end,
    NodeConfig1 = rabbit_ct_helpers:set_config(NodeConfig,
      {tcp_ports_base, Base}),
    %% Now, compute all TCP port numbers from this base.
    {NodeConfig2, _} = lists:foldl(
      fun(PortName, {NewConfig, NextPort}) ->
          {
            rabbit_ct_helpers:set_config(NewConfig, {PortName, NextPort}),
            NextPort + 1
          }
      end,
      {NodeConfig1, Base}, ?TCP_PORTS_LIST),
    %% Finally, update the RabbitMQ configuration with the computed TCP
    %% port numbers.
    update_tcp_ports_in_rmq_config(NodeConfig2, ?TCP_PORTS_LIST).

update_tcp_ports_in_rmq_config(NodeConfig, [tcp_port_amqp = Key | Rest]) ->
    NodeConfig1 = rabbit_ct_helpers:merge_app_env_in_config(NodeConfig,
      {rabbit, [{tcp_listeners, [?config(Key, NodeConfig)]}]}),
    update_tcp_ports_in_rmq_config(NodeConfig1, Rest);
update_tcp_ports_in_rmq_config(NodeConfig, [tcp_port_amqp_tls = Key | Rest]) ->
    NodeConfig1 = rabbit_ct_helpers:merge_app_env_in_config(NodeConfig,
      {rabbit, [{ssl_listeners, [?config(Key, NodeConfig)]}]}),
    update_tcp_ports_in_rmq_config(NodeConfig1, Rest);
update_tcp_ports_in_rmq_config(NodeConfig, [tcp_port_mgmt = Key | Rest]) ->
    NodeConfig1 = rabbit_ct_helpers:merge_app_env_in_config(NodeConfig,
      {rabbitmq_management, [{listener, [{port, ?config(Key, NodeConfig)}]}]}),
    update_tcp_ports_in_rmq_config(NodeConfig1, Rest);
update_tcp_ports_in_rmq_config(NodeConfig, [tcp_port_erlang_dist | Rest]) ->
    %% The Erlang distribution port doesn't appear in the configuration file.
    update_tcp_ports_in_rmq_config(NodeConfig, Rest);
update_tcp_ports_in_rmq_config(NodeConfig, []) ->
    NodeConfig.

init_nodename(Config, NodeConfig, I) ->
    Base = ?config(tcp_ports_base, NodeConfig),
    Suffix0 = rabbit_ct_helpers:get_config(Config, rmq_nodename_suffix),
    Suffix = case Suffix0 of
        undefined -> "";
        _         -> [$- | Suffix0]
    end,
    Nodename = list_to_atom(
      rabbit_misc:format("rmq-ct~s-~b-~b@localhost", [Suffix, I + 1, Base])),
    rabbit_ct_helpers:set_config(NodeConfig, {rmq_nodename, Nodename}).

init_config_filename(Config, NodeConfig, _I) ->
    PrivDir = ?config(priv_dir, Config),
    Nodename = ?config(rmq_nodename, NodeConfig),
    ConfigDir = filename:join(PrivDir, Nodename),
    ConfigFile = filename:join(ConfigDir, Nodename),
    rabbit_ct_helpers:set_config(NodeConfig,
      {erlang_node_config_filename, ConfigFile}).

write_config_file(Config, NodeConfig, _I) ->
    %% Prepare a RabbitMQ configuration.
    ErlangConfigBase = ?config(erlang_node_config, Config),
    ErlangConfigOverlay = ?config(erlang_node_config, NodeConfig),
    ErlangConfig = rabbit_ct_helpers:merge_app_env(ErlangConfigBase,
      ErlangConfigOverlay),
    ConfigFile = ?config(erlang_node_config_filename, NodeConfig),
    ConfigDir = filename:dirname(ConfigFile),
    Ret1 = file:make_dir(ConfigDir),
    Ret2 = file:write_file(ConfigFile ++ ".config",
                          io_lib:format("% vim:ft=erlang:~n~n~p.~n",
                                        [ErlangConfig])),
    case {Ret1, Ret2} of
        {ok, ok} ->
            NodeConfig;
        {{error, eexist}, ok} ->
            NodeConfig;
        {{error, Reason}, _} when Reason =/= eexist ->
            {skip, "Failed to create Erlang node config directory \"" ++
             ConfigDir ++ "\": " ++ file:format_error(Reason)};
        {_, {error, Reason}} ->
            {skip, "Failed to create Erlang node config file \"" ++
             ConfigFile ++ "\": " ++ file:format_error(Reason)}
    end.

do_start_rabbitmq_node(Config, NodeConfig, _I) ->
    Make = ?config(make_cmd, Config),
    SrcDir = ?config(rabbit_srcdir, Config),
    PrivDir = ?config(priv_dir, Config),
    Nodename = ?config(rmq_nodename, NodeConfig),
    DistPort = ?config(tcp_port_erlang_dist, NodeConfig),
    ConfigFile = ?config(erlang_node_config_filename, NodeConfig),
    Cmd = Make ++ " -C " ++ SrcDir ++ rabbit_ct_helpers:make_verbosity() ++
      " start-background-broker" ++
      " RABBITMQ_NODENAME='" ++ atom_to_list(Nodename) ++ "'" ++
      " RABBITMQ_DIST_PORT='" ++ integer_to_list(DistPort) ++ "'" ++
      " RABBITMQ_CONFIG_FILE='" ++ ConfigFile ++ "'" ++
      " TEST_TMPDIR='" ++ PrivDir ++ "'",
    case rabbit_ct_helpers:run_cmd(Cmd) of
        true  -> NodeConfig;
        false -> {skip, "Failed to initialize RabbitMQ"}
    end.

cluster_nodes(Config, NodeConfig1, NodeConfig2) ->
    Rabbitmqctl = ?config(rabbitmqctl_cmd, Config),
    Nodename1 = ?config(rmq_nodename, NodeConfig1),
    Nodename2 = ?config(rmq_nodename, NodeConfig2),
    Cmd =
      Rabbitmqctl ++ " -n \"" ++ atom_to_list(Nodename1) ++ "\"" ++
      " stop_app && " ++
      Rabbitmqctl ++ " -n \"" ++ atom_to_list(Nodename1) ++ "\"" ++
      " join_cluster \"" ++ atom_to_list(Nodename2) ++ "\" && " ++
      Rabbitmqctl ++ " -n \"" ++ atom_to_list(Nodename1) ++ "\"" ++
      " start_app",
    case rabbit_ct_helpers:run_cmd(Cmd) of
        true  -> ok;
        false -> {skip,
                  "Failed to cluster nodes \"" ++ atom_to_list(Nodename1) ++
                  "\" and \"" ++ atom_to_list(Nodename2) ++ "\""}
    end.

move_nonworking_nodedir_away(NodeConfig) ->
    ConfigFile = ?config(erlang_node_config_filename, NodeConfig),
    ConfigDir = filename:dirname(ConfigFile),
    NewName = filename:join(
      filename:dirname(ConfigDir),
      "_unused_nodedir_" ++ filename:basename(ConfigDir)),
    file:rename(ConfigDir, NewName),
    lists:keydelete(erlang_node_config_filename, 1, NodeConfig).

stop_rabbitmq_nodes(Config) ->
    NodeConfigs = ?config(rmq_nodes, Config),
    [stop_rabbitmq_node(Config, NodeConfig) || NodeConfig <- NodeConfigs],
    Config.

stop_rabbitmq_node(Config, NodeConfig) ->
    Make = ?config(make_cmd, Config),
    SrcDir = ?config(rabbit_srcdir, Config),
    PrivDir = ?config(priv_dir, Config),
    Nodename = ?config(rmq_nodename, NodeConfig),
    Cmd = Make ++ " -C " ++ SrcDir ++ rabbit_ct_helpers:make_verbosity() ++
      " stop-rabbit-on-node stop-node" ++
      " RABBITMQ_NODENAME='" ++ atom_to_list(Nodename) ++ "'" ++
      " TEST_TMPDIR='" ++ PrivDir ++ "'",
    rabbit_ct_helpers:run_cmd(Cmd),
    NodeConfig.

get_node_config(Config, I) ->
    NodeConfigs = ?config(rmq_nodes, Config),
    lists:nth(I + 1, NodeConfigs).

%% -------------------------------------------------------------------
%% Calls to rabbitmqctl from Erlang.
%% -------------------------------------------------------------------

control_action(Command, Args) ->
    control_action(Command, node(), Args, default_options()).

control_action(Command, Args, NewOpts) ->
    control_action(Command, node(), Args,
                   expand_options(default_options(), NewOpts)).

control_action(Command, Node, Args, Opts) ->
    case catch rabbit_control_main:action(
                 Command, Node, Args, Opts,
                 fun (Format, Args1) ->
                         io:format(Format ++ " ...~n", Args1)
                 end) of
        ok ->
            io:format("done.~n"),
            ok;
        {ok, Result} ->
            rabbit_control_misc:print_cmd_result(Command, Result),
            ok;
        Other ->
            io:format("failed.~n"),
            Other
    end.

control_action_t(Command, Args, Timeout) when is_number(Timeout) ->
    control_action_t(Command, node(), Args, default_options(), Timeout).

control_action_t(Command, Args, NewOpts, Timeout) when is_number(Timeout) ->
    control_action_t(Command, node(), Args,
                     expand_options(default_options(), NewOpts),
                     Timeout).

control_action_t(Command, Node, Args, Opts, Timeout) when is_number(Timeout) ->
    case catch rabbit_control_main:action(
                 Command, Node, Args, Opts,
                 fun (Format, Args1) ->
                         io:format(Format ++ " ...~n", Args1)
                 end, Timeout) of
        ok ->
            io:format("done.~n"),
            ok;
        Other ->
            io:format("failed.~n"),
            Other
    end.

control_action_opts(Raw) ->
    NodeStr = atom_to_list(node()),
    case rabbit_control_main:parse_arguments(Raw, NodeStr) of
        {ok, {Cmd, Opts, Args}} ->
            case control_action(Cmd, node(), Args, Opts) of
                ok    -> ok;
                Error -> Error
            end;
        Error ->
            Error
    end.

info_action(Command, Args, CheckVHost) ->
    ok = control_action(Command, []),
    if CheckVHost -> ok = control_action(Command, [], ["-p", "/"]);
       true       -> ok
    end,
    ok = control_action(Command, lists:map(fun atom_to_list/1, Args)),
    {bad_argument, dummy} = control_action(Command, ["dummy"]),
    ok.

info_action_t(Command, Args, CheckVHost, Timeout) when is_number(Timeout) ->
    if CheckVHost -> ok = control_action_t(Command, [], ["-p", "/"], Timeout);
       true       -> ok
    end,
    ok = control_action_t(Command, lists:map(fun atom_to_list/1, Args), Timeout),
    ok.

default_options() -> [{"-p", "/"}, {"-q", "false"}].

expand_options(As, Bs) ->
    lists:foldl(fun({K, _}=A, R) ->
                        case proplists:is_defined(K, R) of
                            true -> R;
                            false -> [A | R]
                        end
                end, Bs, As).

%% -------------------------------------------------------------------
%% Other helpers.
%% -------------------------------------------------------------------

add_test_path_to_broker(Node, Module) ->
    Path1 = filename:dirname(code:which(Module)),
    Path2 = filename:dirname(code:which(?MODULE)),
    Paths = lists:usort([Path1, Path2]),
    ExistingPaths = rpc:call(Node, code, get_path, []),
    lists:foreach(
      fun(P) ->
          case lists:member(P, ExistingPaths) of
              true  -> ok;
              false -> true = rpc:call(Node, code, add_pathz, [P])
          end
      end, Paths).

run_on_broker(Node, Module, Function, Args) ->
    %% We add some directories to the broker node search path.
    add_test_path_to_broker(Node, Module),
    %% If there is an exception, rpc:call/4 returns the exception as
    %% a "normal" return value. If there is an exit signal, we raise
    %% it again. In both cases, we have no idea of the module and line
    %% number which triggered the issue.
    case rpc:call(Node, Module, Function, Args) of
        {badrpc, {'EXIT', Reason}} -> exit(Reason);
        {badrpc, Reason}           -> exit(Reason);
        Ret                        -> Ret
    end.

run_on_broker_i(Config, I, Module, Function, Args) ->
    Node = ?config(rmq_nodename,
      rabbit_ct_broker_helpers:get_node_config(Config, I)),
    run_on_broker(Node, Module, Function, Args).

%% From a given list of gen_tcp client connections, return the list of
%% connection handler PID in RabbitMQ.
get_connection_pids(Connections) ->
    ConnInfos = [
      begin
          {ok, {Addr, Port}} = inet:sockname(Connection),
          [{peer_host, Addr}, {peer_port, Port}]
      end || Connection <- Connections],
    lists:filter(
      fun(Conn) ->
          ConnInfo = rabbit_networking:connection_info(Conn,
            [peer_host, peer_port]),
          lists:member(ConnInfo, ConnInfos)
      end, rabbit_networking:connections()).

%% Return the PID of the given queue's supervisor.
get_queue_sup_pid(QueuePid) ->
    Sups = supervisor:which_children(rabbit_amqqueue_sup_sup),
    get_queue_sup_pid(Sups, QueuePid).

get_queue_sup_pid([{_, SupPid, _, _} | Rest], QueuePid) ->
    WorkerPids = [Pid || {_, Pid, _, _} <- supervisor:which_children(SupPid)],
    case lists:member(QueuePid, WorkerPids) of
        true  -> SupPid;
        false -> get_queue_sup_pid(Rest, QueuePid)
    end;
get_queue_sup_pid([], _QueuePid) ->
    undefined.

test_channel() ->
    Me = self(),
    Writer = spawn(fun () -> test_writer(Me) end),
    {ok, Limiter} = rabbit_limiter:start_link(no_id),
    {ok, Ch} = rabbit_channel:start_link(
                 1, Me, Writer, Me, "", rabbit_framing_amqp_0_9_1,
                 user(<<"guest">>), <<"/">>, [], Me, Limiter),
    {Writer, Limiter, Ch}.

test_writer(Pid) ->
    receive
        {'$gen_call', From, flush} -> gen_server:reply(From, ok),
                                      test_writer(Pid);
        {send_command, Method}     -> Pid ! Method,
                                      test_writer(Pid);
        shutdown                   -> ok
    end.

user(Username) ->
    #user{username       = Username,
          tags           = [administrator],
          authz_backends = [{rabbit_auth_backend_internal, none}]}.
