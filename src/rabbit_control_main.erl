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
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_control_main).
-include("rabbit.hrl").
-include("rabbit_cli.hrl").
-include("rabbit_misc.hrl").

-export([start/0, stop/0, parse_arguments/2, action/5, action/6,
         sync_queue/1, cancel_sync_queue/1, become/1,
         purge_queue/1]).

-import(rabbit_misc, [rpc_call/4, rpc_call/5, rpc_call/7]).

-define(EXTERNAL_CHECK_INTERVAL, 1000).

-define(GLOBAL_DEFS(Node), [?QUIET_DEF, ?NODE_DEF(Node), ?TIMEOUT_DEF]).

-define(COMMANDS,
        [stop,
         shutdown,
         stop_app,
         start_app,
         wait,
         reset,
         force_reset,
         rotate_logs,
         hipe_compile,

         {join_cluster, [?RAM_DEF]},
         change_cluster_node_type,
         update_cluster_nodes,
         {forget_cluster_node, [?OFFLINE_DEF]},
         rename_cluster_node,
         force_boot,
         cluster_status,
         {sync_queue, [?VHOST_DEF]},
         {cancel_sync_queue, [?VHOST_DEF]},
         {purge_queue, [?VHOST_DEF]},

         add_user,
         delete_user,
         change_password,
         clear_password,
         authenticate_user,
         set_user_tags,
         list_users,

         add_vhost,
         delete_vhost,
         list_vhosts,
         {set_permissions, [?VHOST_DEF]},
         {clear_permissions, [?VHOST_DEF]},
         {list_permissions, [?VHOST_DEF]},
         list_user_permissions,

         {set_parameter, [?VHOST_DEF]},
         {clear_parameter, [?VHOST_DEF]},
         {list_parameters, [?VHOST_DEF]},

         set_global_parameter,
         clear_global_parameter,
         list_global_parameters,

         {set_policy, [?VHOST_DEF, ?PRIORITY_DEF, ?APPLY_TO_DEF]},
         {clear_policy, [?VHOST_DEF]},
         {list_policies, [?VHOST_DEF]},

         {list_queues, [?VHOST_DEF, ?OFFLINE_DEF, ?ONLINE_DEF, ?LOCAL_DEF]},
         {list_exchanges, [?VHOST_DEF]},
         {list_bindings, [?VHOST_DEF]},
         {list_connections, [?VHOST_DEF]},
         list_channels,
         {list_consumers, [?VHOST_DEF]},
         status,
         environment,
         report,
         set_cluster_name,
         eval,
         node_health_check,

         close_connection,
         {trace_on, [?VHOST_DEF]},
         {trace_off, [?VHOST_DEF]},
         set_vm_memory_high_watermark,
         set_disk_free_limit,
         help,
         {encode, [?DECODE_DEF, ?CIPHER_DEF, ?HASH_DEF, ?ITERATIONS_DEF, ?LIST_CIPHERS_DEF, ?LIST_HASHES_DEF]}
        ]).

-define(GLOBAL_QUERIES,
        [{"Connections", rabbit_networking, connection_info_all,
          connection_info_keys},
         {"Channels",  rabbit_channel,  info_all, info_keys}]).

-define(VHOST_QUERIES,
        [{"Queues",    rabbit_amqqueue, info_all, info_keys},
         {"Exchanges", rabbit_exchange, info_all, info_keys},
         {"Bindings",  rabbit_binding,  info_all, info_keys},
         {"Consumers", rabbit_amqqueue, consumers_all, consumer_info_keys},
         {"Permissions", rabbit_auth_backend_internal, list_vhost_permissions,
          vhost_perms_info_keys},
         {"Policies",   rabbit_policy,             list_formatted, info_keys},
         {"Parameters", rabbit_runtime_parameters, list_formatted, info_keys}]).

-define(COMMANDS_NOT_REQUIRING_APP,
        [stop, shutdown, stop_app, start_app, wait, reset, force_reset, rotate_logs,
         join_cluster, change_cluster_node_type, update_cluster_nodes,
         forget_cluster_node, rename_cluster_node, cluster_status, status,
         environment, eval, force_boot, help, hipe_compile, encode]).

%% [Command | {Command, DefaultTimeoutInMilliSeconds}]
-define(COMMANDS_WITH_TIMEOUT,
        [list_user_permissions, list_policies, list_queues, list_exchanges,
         list_bindings, list_connections, list_channels, list_consumers,
         list_vhosts, list_parameters, list_global_parameters,
         purge_queue,
         {node_health_check, 70000}]).

%%----------------------------------------------------------------------------

-spec start() -> no_return().
-spec stop() -> 'ok'.
-spec action
        (atom(), node(), [string()], [{string(), any()}],
         fun ((string(), [any()]) -> 'ok')) ->
            'ok'.

-spec action
        (atom(), node(), [string()], [{string(), any()}],
         fun ((string(), [any()]) -> 'ok'), timeout()) ->
            'ok'.

%%----------------------------------------------------------------------------

start() ->
    rabbit_cli:main(
      fun (Args, NodeStr) ->
              parse_arguments(Args, NodeStr)
      end,
      fun (Command, Node, Args, Opts) ->
              Quiet = proplists:get_bool(?QUIET_OPT, Opts),
              Inform = case Quiet of
                           true  -> fun (_Format, _Args1) -> ok end;
                           false -> fun (Format, Args1) ->
                                            io:format(Format ++ "~n", Args1)
                                    end
                       end,
              try
                  T = case get_timeout(Command, Opts) of
                          {ok, Timeout} ->
                              Timeout;
                          {error, _} ->
                              %% since this is an error with user input, ignore the quiet
                              %% setting
                              io:format("Failed to parse provided timeout value, using ~s~n", [?RPC_TIMEOUT]),
                              ?RPC_TIMEOUT
                  end,
                  do_action(Command, Node, Args, Opts, Inform, T)
              catch _:E -> E
              end
      end, rabbit_ctl_usage).

parse_arguments(CmdLine, NodeStr) ->
    rabbit_cli:parse_arguments(
      ?COMMANDS, ?GLOBAL_DEFS(NodeStr), ?NODE_OPT, CmdLine).

print_report(Node, {Descr, Module, InfoFun, KeysFun}) ->
    io:format("~s:~n", [Descr]),
    print_report0(Node, {Module, InfoFun, KeysFun}, []).

print_report(Node, {Descr, Module, InfoFun, KeysFun}, VHostArg) ->
    io:format("~s on ~s:~n", [Descr, VHostArg]),
    print_report0(Node, {Module, InfoFun, KeysFun}, VHostArg).

print_report0(Node, {Module, InfoFun, KeysFun}, VHostArg) ->
    case rpc_call(Node, Module, InfoFun, VHostArg) of
        [_|_] = Results -> InfoItems = rpc_call(Node, Module, KeysFun, []),
                           display_row([atom_to_list(I) || I <- InfoItems]),
                           display_info_list(Results, InfoItems);
        _               -> ok
    end,
    io:nl().

get_timeout(Command, Opts) ->
    Default = case proplists:lookup(Command, ?COMMANDS_WITH_TIMEOUT) of
                  none ->
                      infinity;
                  {Command, true} ->
                      ?RPC_TIMEOUT;
                  {Command, D} ->
                      D
              end,
    Result = case proplists:get_value(?TIMEOUT_OPT, Opts, Default) of
        use_default ->
            parse_timeout(Default);
        Value ->
            parse_timeout(Value)
    end,
    Result.


parse_number(N) when is_list(N) ->
    try list_to_integer(N) of
        Val -> Val
    catch error:badarg ->
            %% could have been a float, give it
            %% another shot
            list_to_float(N)
    end.

parse_timeout("infinity") ->
    {ok, infinity};
parse_timeout(infinity) ->
    {ok, infinity};
parse_timeout(N) when is_list(N) ->
    try parse_number(N) of
        M ->
            Y = case M >= 0 of
                    true  -> round(M) * 1000;
                    false -> ?RPC_TIMEOUT
                end,
            {ok, Y}
    catch error:badarg ->
        {error, infinity}
    end;
parse_timeout(N) ->
    {ok, N}.

announce_timeout(infinity, _Inform) ->
    %% no-op
    ok;
announce_timeout(Timeout, Inform) when is_number(Timeout) ->
    Inform("Timeout: ~w seconds", [Timeout/1000]),
    ok.

stop() ->
    ok.

%%----------------------------------------------------------------------------

do_action(Command, Node, Args, Opts, Inform, Timeout) ->
    case lists:member(Command, ?COMMANDS_NOT_REQUIRING_APP) of
        false ->
            case ensure_app_running(Node) of
                ok ->
                    case proplists:lookup(Command, ?COMMANDS_WITH_TIMEOUT) of
                        {Command, _}  ->
                            announce_timeout(Timeout, Inform),
                            action(Command, Node, Args, Opts, Inform, Timeout);
                        none ->
                            action(Command, Node, Args, Opts, Inform)
                    end;
                E  -> E
            end;
        true  ->
            action(Command, Node, Args, Opts, Inform)
    end.

shutdown_node_and_wait_pid_to_stop(Node, Pid, Inform) ->
    Inform("Shutting down RabbitMQ node ~p running at PID ~s", [Node, Pid]),
    Res = call(Node, {rabbit, stop_and_halt, []}),
    case Res of
        ok ->
            Inform("Waiting for PID ~s to terminate", [Pid]),
            wait_for_process_death(Pid),
            Inform(
              "RabbitMQ node ~p running at PID ~s successfully shut down",
              [Node, Pid]);
        _  -> ok
    end,
    Res.

action(shutdown, Node, [], _Opts, Inform) ->
    case rpc:call(Node, os, getpid, []) of
        Pid when is_list(Pid) ->
            shutdown_node_and_wait_pid_to_stop(Node, Pid, Inform);
        Error -> Error
    end;

action(stop, Node, Args, _Opts, Inform) ->
    Inform("Stopping and halting node ~p", [Node]),
    Res = call(Node, {rabbit, stop_and_halt, []}),
    case {Res, Args} of
        {ok, [PidFile]} -> wait_for_process_death(
                             read_pid_file(PidFile, false));
        {ok, [_, _| _]} -> exit({badarg, Args});
        _               -> ok
    end,
    Res;

action(stop_app, Node, [], _Opts, Inform) ->
    Inform("Stopping rabbit application on node ~p", [Node]),
    call(Node, {rabbit, stop, []});

action(start_app, Node, [], _Opts, Inform) ->
    Inform("Starting node ~p", [Node]),
    call(Node, {rabbit, start, []});

action(reset, Node, [], _Opts, Inform) ->
    Inform("Resetting node ~p", [Node]),
    require_mnesia_stopped(Node,
                           fun() ->
                                   call(Node, {rabbit_mnesia, reset, []})
                           end);

action(force_reset, Node, [], _Opts, Inform) ->
    Inform("Forcefully resetting node ~p", [Node]),
    require_mnesia_stopped(Node,
                           fun() ->
                                   call(Node, {rabbit_mnesia, force_reset, []})
                           end);

action(join_cluster, Node, [ClusterNodeS], Opts, Inform) ->
    ClusterNode = list_to_atom(ClusterNodeS),
    NodeType = case proplists:get_bool(?RAM_OPT, Opts) of
                   true  -> ram;
                   false -> disc
               end,
    Inform("Clustering node ~p with ~p", [Node, ClusterNode]),
    require_mnesia_stopped(Node,
                           fun() ->
                                   rpc_call(Node, rabbit_mnesia, join_cluster, [ClusterNode, NodeType])
                           end);

action(change_cluster_node_type, Node, ["ram"], _Opts, Inform) ->
    Inform("Turning ~p into a ram node", [Node]),
    require_mnesia_stopped(Node,
                           fun() ->
                                   rpc_call(Node, rabbit_mnesia, change_cluster_node_type, [ram])
                           end);
action(change_cluster_node_type, Node, [Type], _Opts, Inform)
  when Type =:= "disc" orelse Type =:= "disk" ->
    Inform("Turning ~p into a disc node", [Node]),
    require_mnesia_stopped(Node,
                           fun() ->
                                   rpc_call(Node, rabbit_mnesia, change_cluster_node_type, [disc])
                           end);

action(update_cluster_nodes, Node, [ClusterNodeS], _Opts, Inform) ->
    ClusterNode = list_to_atom(ClusterNodeS),
    Inform("Updating cluster nodes for ~p from ~p", [Node, ClusterNode]),
    require_mnesia_stopped(Node,
                          fun() ->
                                  rpc_call(Node, rabbit_mnesia, update_cluster_nodes, [ClusterNode])
                          end);

action(forget_cluster_node, Node, [ClusterNodeS], Opts, Inform) ->
    ClusterNode = list_to_atom(ClusterNodeS),
    RemoveWhenOffline = proplists:get_bool(?OFFLINE_OPT, Opts),
    Inform("Removing node ~p from cluster", [ClusterNode]),
    case RemoveWhenOffline of
        true  -> become(Node),
                 rabbit_mnesia:forget_cluster_node(ClusterNode, true);
        false -> rpc_call(Node, rabbit_mnesia, forget_cluster_node,
                          [ClusterNode, false])
    end;

action(rename_cluster_node, Node, NodesS, _Opts, Inform) ->
    Nodes = split_list([list_to_atom(N) || N <- NodesS]),
    Inform("Renaming cluster nodes:~n~s~n",
           [lists:flatten([rabbit_misc:format("  ~s -> ~s~n", [F, T]) ||
                              {F, T} <- Nodes])]),
    rabbit_mnesia_rename:rename(Node, Nodes);

action(force_boot, Node, [], _Opts, Inform) ->
    Inform("Forcing boot for Mnesia dir ~s", [mnesia:system_info(directory)]),
    case rabbit:is_running(Node) of
        false -> rabbit_mnesia:force_load_next_boot();
        true  -> {error, rabbit_running}
    end;

action(sync_queue, Node, [Q], Opts, Inform) ->
    VHost = proplists:get_value(?VHOST_OPT, Opts),
    QName = rabbit_misc:r(list_to_binary(VHost), queue, list_to_binary(Q)),
    Inform("Synchronising ~s", [rabbit_misc:rs(QName)]),
    rpc_call(Node, rabbit_control_main, sync_queue, [QName]);

action(cancel_sync_queue, Node, [Q], Opts, Inform) ->
    VHost = proplists:get_value(?VHOST_OPT, Opts),
    QName = rabbit_misc:r(list_to_binary(VHost), queue, list_to_binary(Q)),
    Inform("Stopping synchronising ~s", [rabbit_misc:rs(QName)]),
    rpc_call(Node, rabbit_control_main, cancel_sync_queue, [QName]);

action(wait, Node, [PidFile], _Opts, Inform) ->
    Inform("Waiting for ~p", [Node]),
    wait_for_application(Node, PidFile, rabbit_and_plugins, Inform);
action(wait, Node, [PidFile, App], _Opts, Inform) ->
    Inform("Waiting for ~p on ~p", [App, Node]),
    wait_for_application(Node, PidFile, list_to_atom(App), Inform);

action(status, Node, [], _Opts, Inform) ->
    Inform("Status of node ~p", [Node]),
    display_call_result(Node, {rabbit, status, []});

action(cluster_status, Node, [], _Opts, Inform) ->
    Inform("Cluster status of node ~p", [Node]),
    Status = unsafe_rpc(Node, rabbit_mnesia, status, []),
    io:format("~p~n", [Status ++ [{alarms,
        [alarms_by_node(Name) || Name <- nodes_in_cluster(Node)]}]]),
    ok;

action(environment, Node, _App, _Opts, Inform) ->
    Inform("Application environment of node ~p", [Node]),
    display_call_result(Node, {rabbit, environment, []});

action(rotate_logs, Node, [], _Opts, Inform) ->
    Inform("Reopening logs for node ~p", [Node]),
    call(Node, {rabbit, rotate_logs, [""]});
action(rotate_logs, Node, Args = [Suffix], _Opts, Inform) ->
    Inform("Rotating logs to files with suffix \"~s\"", [Suffix]),
    call(Node, {rabbit, rotate_logs, Args});

action(hipe_compile, _Node, [TargetDir], _Opts, _Inform) ->
    ok = application:load(rabbit),
    case rabbit_hipe:can_hipe_compile() of
        true ->
            {ok, _, _} = rabbit_hipe:compile_to_directory(TargetDir),
            ok;
        false ->
            {error, "HiPE compilation is not supported"}
    end;

action(close_connection, Node, [PidStr, Explanation], _Opts, Inform) ->
    Inform("Closing connection \"~s\"", [PidStr]),
    rpc_call(Node, rabbit_networking, close_connection,
             [rabbit_misc:string_to_pid(PidStr), Explanation]);

action(add_user, Node, Args = [Username, _Password], _Opts, Inform) ->
    Inform("Creating user \"~s\"", [Username]),
    call(Node, {rabbit_auth_backend_internal, add_user, Args});

action(delete_user, Node, Args = [_Username], _Opts, Inform) ->
    Inform("Deleting user \"~s\"", Args),
    call(Node, {rabbit_auth_backend_internal, delete_user, Args});

action(change_password, Node, Args = [Username, _Newpassword], _Opts, Inform) ->
    Inform("Changing password for user \"~s\"", [Username]),
    call(Node, {rabbit_auth_backend_internal, change_password, Args});

action(clear_password, Node, Args = [Username], _Opts, Inform) ->
    Inform("Clearing password for user \"~s\"", [Username]),
    call(Node, {rabbit_auth_backend_internal, clear_password, Args});

action(authenticate_user, Node, Args = [Username, _Password], _Opts, Inform) ->
    Inform("Authenticating user \"~s\"", [Username]),
    call(Node, {rabbit_access_control, check_user_pass_login, Args});

action(set_user_tags, Node, [Username | TagsStr], _Opts, Inform) ->
    Tags = [list_to_atom(T) || T <- TagsStr],
    Inform("Setting tags for user \"~s\" to ~p", [Username, Tags]),
    rpc_call(Node, rabbit_auth_backend_internal, set_tags,
             [list_to_binary(Username), Tags]);

action(add_vhost, Node, Args = [_VHostPath], _Opts, Inform) ->
    Inform("Creating vhost \"~s\"", Args),
    call(Node, {rabbit_vhost, add, Args});

action(delete_vhost, Node, Args = [_VHostPath], _Opts, Inform) ->
    Inform("Deleting vhost \"~s\"", Args),
    call(Node, {rabbit_vhost, delete, Args});

action(trace_on, Node, [], Opts, Inform) ->
    VHost = proplists:get_value(?VHOST_OPT, Opts),
    Inform("Starting tracing for vhost \"~s\"", [VHost]),
    rpc_call(Node, rabbit_trace, start, [list_to_binary(VHost)]);

action(trace_off, Node, [], Opts, Inform) ->
    VHost = proplists:get_value(?VHOST_OPT, Opts),
    Inform("Stopping tracing for vhost \"~s\"", [VHost]),
    rpc_call(Node, rabbit_trace, stop, [list_to_binary(VHost)]);

action(set_vm_memory_high_watermark, Node, [Arg], _Opts, Inform) ->
    Frac = list_to_float(case string:chr(Arg, $.) of
                             0 -> Arg ++ ".0";
                             _ -> Arg
                         end),
    Inform("Setting memory threshold on ~p to ~p", [Node, Frac]),
    rpc_call(Node, vm_memory_monitor, set_vm_memory_high_watermark, [Frac]);

action(set_vm_memory_high_watermark, Node, ["absolute", Arg], _Opts, Inform) ->
    case rabbit_resource_monitor_misc:parse_information_unit(Arg) of
        {ok, Limit} ->
            Inform("Setting memory threshold on ~p to ~p bytes", [Node, Limit]),
            rpc_call(Node, vm_memory_monitor, set_vm_memory_high_watermark,
                 [{absolute, Limit}]);
        {error, parse_error} ->
            {error_string, rabbit_misc:format(
                "Unable to parse absolute memory limit value ~p", [Arg])}
    end;

action(set_disk_free_limit, Node, [Arg], _Opts, Inform) ->
    case rabbit_resource_monitor_misc:parse_information_unit(Arg) of
        {ok, Limit} ->
            Inform("Setting disk free limit on ~p to ~p bytes", [Node, Limit]),
            rpc_call(Node, rabbit_disk_monitor, set_disk_free_limit, [Limit]);
        {error, parse_error} ->
            {error_string, rabbit_misc:format(
                "Unable to parse disk free limit value ~p", [Arg])}
    end;

action(set_disk_free_limit, Node, ["mem_relative", Arg], _Opts, Inform) ->
    Frac = list_to_float(case string:chr(Arg, $.) of
                             0 -> Arg ++ ".0";
                             _ -> Arg
                         end),
    Inform("Setting disk free limit on ~p to ~p of total RAM", [Node, Frac]),
    rpc_call(Node,
             rabbit_disk_monitor,
             set_disk_free_limit,
             [{mem_relative, Frac}]);


action(set_permissions, Node, [Username, CPerm, WPerm, RPerm], Opts, Inform) ->
    VHost = proplists:get_value(?VHOST_OPT, Opts),
    Inform("Setting permissions for user \"~s\" in vhost \"~s\"",
           [Username, VHost]),
    call(Node, {rabbit_auth_backend_internal, set_permissions,
                [Username, VHost, CPerm, WPerm, RPerm]});

action(clear_permissions, Node, [Username], Opts, Inform) ->
    VHost = proplists:get_value(?VHOST_OPT, Opts),
    Inform("Clearing permissions for user \"~s\" in vhost \"~s\"",
           [Username, VHost]),
    call(Node, {rabbit_auth_backend_internal, clear_permissions,
                [Username, VHost]});

action(set_parameter, Node, [Component, Key, Value], Opts, Inform) ->
    VHostArg = list_to_binary(proplists:get_value(?VHOST_OPT, Opts)),
    Inform("Setting runtime parameter ~p for component ~p to ~p",
           [Key, Component, Value]),
    rpc_call(
      Node, rabbit_runtime_parameters, parse_set,
      [VHostArg, list_to_binary(Component), list_to_binary(Key), Value, none]);

action(clear_parameter, Node, [Component, Key], Opts, Inform) ->
    VHostArg = list_to_binary(proplists:get_value(?VHOST_OPT, Opts)),
    Inform("Clearing runtime parameter ~p for component ~p", [Key, Component]),
    rpc_call(Node, rabbit_runtime_parameters, clear, [VHostArg,
                                                      list_to_binary(Component),
                                                      list_to_binary(Key)]);

action(set_global_parameter, Node, [Key, Value], _Opts, Inform) ->
    Inform("Setting global runtime parameter ~p to ~p", [Key, Value]),
    rpc_call(
        Node, rabbit_runtime_parameters, parse_set_global,
        [rabbit_data_coercion:to_atom(Key), rabbit_data_coercion:to_binary(Value)]
    );

action(clear_global_parameter, Node, [Key], _Opts, Inform) ->
    Inform("Clearing global runtime parameter ~p", [Key]),
    rpc_call(
        Node, rabbit_runtime_parameters, clear_global,
        [rabbit_data_coercion:to_atom(Key)]
    );

action(set_policy, Node, [Key, Pattern, Defn], Opts, Inform) ->
    Msg = "Setting policy ~p for pattern ~p to ~p with priority ~p",
    VHostArg = list_to_binary(proplists:get_value(?VHOST_OPT, Opts)),
    PriorityArg = proplists:get_value(?PRIORITY_OPT, Opts),
    ApplyToArg = list_to_binary(proplists:get_value(?APPLY_TO_OPT, Opts)),
    Inform(Msg, [Key, Pattern, Defn, PriorityArg]),
    Res = rpc_call(
      Node, rabbit_policy, parse_set,
      [VHostArg, list_to_binary(Key), Pattern, Defn, PriorityArg, ApplyToArg]),
    case Res of
        {error, Format, Args} when is_list(Format) andalso is_list(Args) ->
            {error_string, rabbit_misc:format(Format, Args)};
        _ ->
            Res
    end;

action(clear_policy, Node, [Key], Opts, Inform) ->
    VHostArg = list_to_binary(proplists:get_value(?VHOST_OPT, Opts)),
    Inform("Clearing policy ~p", [Key]),
    rpc_call(Node, rabbit_policy, delete, [VHostArg, list_to_binary(Key)]);

action(report, Node, _Args, _Opts, Inform) ->
    Inform("Reporting server status on ~p~n~n", [erlang:universaltime()]),
    [begin ok = action(Action, N, [], [], Inform), io:nl() end ||
        N      <- unsafe_rpc(Node, rabbit_mnesia, cluster_nodes, [running]),
        Action <- [status, cluster_status, environment]],
    VHosts = unsafe_rpc(Node, rabbit_vhost, list, []),
    [print_report(Node, Q)      || Q <- ?GLOBAL_QUERIES],
    [print_report(Node, Q, [V]) || Q <- ?VHOST_QUERIES, V <- VHosts],
    ok;

action(set_cluster_name, Node, [Name], _Opts, Inform) ->
    Inform("Setting cluster name to ~s", [Name]),
    rpc_call(Node, rabbit_nodes, set_cluster_name, [list_to_binary(Name)]);

action(eval, Node, [Expr], _Opts, _Inform) ->
    case erl_scan:string(Expr) of
        {ok, Scanned, _} ->
            case erl_parse:parse_exprs(Scanned) of
                {ok, Parsed} -> {value, Value, _} =
                                    unsafe_rpc(
                                      Node, erl_eval, exprs, [Parsed, []]),
                                io:format("~p~n", [Value]),
                                ok;
                {error, E}   -> {error_string, format_parse_error(E)}
            end;
        {error, E, _} ->
            {error_string, format_parse_error(E)}
    end;

action(help, _Node, _Args, _Opts, _Inform) ->
    io:format("~s", [rabbit_ctl_usage:usage()]);

action(encode, _Node, Args, Opts, _Inform) ->
    ListCiphers = lists:member({?LIST_CIPHERS_OPT, true}, Opts),
    ListHashes = lists:member({?LIST_HASHES_OPT, true}, Opts),
    Decode = lists:member({?DECODE_OPT, true}, Opts),
    Cipher = list_to_atom(proplists:get_value(?CIPHER_OPT, Opts)),
    Hash = list_to_atom(proplists:get_value(?HASH_OPT, Opts)),
    Iterations = list_to_integer(proplists:get_value(?ITERATIONS_OPT, Opts)),

    {_, Msg} = rabbit_control_pbe:encode(ListCiphers, ListHashes, Decode, Cipher, Hash, Iterations, Args),
    io:format(Msg ++ "~n");

action(Command, Node, Args, Opts, Inform) ->
    %% For backward compatibility, run commands accepting a timeout with
    %% the default timeout.
    action(Command, Node, Args, Opts, Inform, ?RPC_TIMEOUT).

action(purge_queue, _Node, [], _Opts, _Inform, _Timeout) ->
    {error, "purge_queue takes queue name as an argument"};

action(purge_queue, Node, [Q], Opts, Inform, Timeout) ->
    VHost = proplists:get_value(?VHOST_OPT, Opts),
    QRes = rabbit_misc:r(list_to_binary(VHost), queue, list_to_binary(Q)),
    Inform("Purging ~s", [rabbit_misc:rs(QRes)]),
    rpc_call(Node, rabbit_control_main, purge_queue, [QRes], Timeout);

action(list_users, Node, [], _Opts, Inform, Timeout) ->
    Inform("Listing users", []),
    call(Node, {rabbit_auth_backend_internal, list_users, []},
         rabbit_auth_backend_internal:user_info_keys(), true, Timeout);

action(list_permissions, Node, [], Opts, Inform, Timeout) ->
    VHost = proplists:get_value(?VHOST_OPT, Opts),
    Inform("Listing permissions in vhost \"~s\"", [VHost]),
    call(Node, {rabbit_auth_backend_internal, list_vhost_permissions, [VHost]},
         rabbit_auth_backend_internal:vhost_perms_info_keys(), true, Timeout,
         true);

action(list_parameters, Node, [], Opts, Inform, Timeout) ->
    VHostArg = list_to_binary(proplists:get_value(?VHOST_OPT, Opts)),
    Inform("Listing runtime parameters", []),
    call(Node, {rabbit_runtime_parameters, list_formatted, [VHostArg]},
         rabbit_runtime_parameters:info_keys(), Timeout);

action(list_global_parameters, Node, [], _Opts, Inform, Timeout) ->
    Inform("Listing global runtime parameters", []),
    call(Node, {rabbit_runtime_parameters, list_global_formatted, []},
         rabbit_runtime_parameters:global_info_keys(), Timeout);

action(list_policies, Node, [], Opts, Inform, Timeout) ->
    VHostArg = list_to_binary(proplists:get_value(?VHOST_OPT, Opts)),
    Inform("Listing policies", []),
    call(Node, {rabbit_policy, list_formatted, [VHostArg]},
         rabbit_policy:info_keys(), Timeout);

action(list_vhosts, Node, Args, _Opts, Inform, Timeout) ->
    Inform("Listing vhosts", []),
    ArgAtoms = default_if_empty(Args, [name]),
    call(Node, {rabbit_vhost, info_all, []}, ArgAtoms, true, Timeout);

action(list_user_permissions, _Node, _Args = [], _Opts, _Inform, _Timeout) ->
    {error_string,
     "list_user_permissions expects a username argument, but none provided."};
action(list_user_permissions, Node, Args = [_Username], _Opts, Inform, Timeout) ->
    Inform("Listing permissions for user ~p", Args),
    call(Node, {rabbit_auth_backend_internal, list_user_permissions, Args},
         rabbit_auth_backend_internal:user_perms_info_keys(), true, Timeout,
         true);

action(list_queues, Node, Args, Opts, Inform, Timeout) ->
    case rabbit_cli:mutually_exclusive_flags(
           Opts, all, [{?ONLINE_OPT, online}
                      ,{?OFFLINE_OPT, offline}
                      ,{?LOCAL_OPT, local}]) of
        {ok, Filter} ->
            Inform("Listing queues", []),
            VHostArg = list_to_binary(proplists:get_value(?VHOST_OPT, Opts)),
            ArgAtoms = default_if_empty(Args, [name, messages]),
            call(Node, {rabbit_amqqueue, info_all, [VHostArg, ArgAtoms, Filter]},
                 ArgAtoms, Timeout);
        {error, ErrStr} ->
            {error_string, ErrStr}
    end;

action(list_exchanges, Node, Args, Opts, Inform, Timeout) ->
    Inform("Listing exchanges", []),
    VHostArg = list_to_binary(proplists:get_value(?VHOST_OPT, Opts)),
    ArgAtoms = default_if_empty(Args, [name, type]),
    call(Node, {rabbit_exchange, info_all, [VHostArg, ArgAtoms]},
         ArgAtoms, Timeout);

action(list_bindings, Node, Args, Opts, Inform, Timeout) ->
    Inform("Listing bindings", []),
    VHostArg = list_to_binary(proplists:get_value(?VHOST_OPT, Opts)),
    ArgAtoms = default_if_empty(Args, [source_name, source_kind,
                                       destination_name, destination_kind,
                                       routing_key, arguments]),
    call(Node, {rabbit_binding, info_all, [VHostArg, ArgAtoms]},
         ArgAtoms, Timeout);

action(list_connections, Node, Args, _Opts, Inform, Timeout) ->
    Inform("Listing connections", []),
    ArgAtoms = default_if_empty(Args, [user, peer_host, peer_port, state]),
    call(Node, {rabbit_networking, connection_info_all, [ArgAtoms]},
         ArgAtoms, Timeout);

action(list_channels, Node, Args, _Opts, Inform, Timeout) ->
    Inform("Listing channels", []),
    ArgAtoms = default_if_empty(Args, [pid, user, consumer_count,
                                       messages_unacknowledged]),
    call(Node, {rabbit_channel, info_all, [ArgAtoms]},
         ArgAtoms, Timeout);

action(list_consumers, Node, _Args, Opts, Inform, Timeout) ->
    Inform("Listing consumers", []),
    VHostArg = list_to_binary(proplists:get_value(?VHOST_OPT, Opts)),
    call(Node, {rabbit_amqqueue, consumers_all, [VHostArg]},
         rabbit_amqqueue:consumer_info_keys(), Timeout);

action(node_health_check, Node, _Args, _Opts, Inform, Timeout) ->
    Inform("Checking health of node ~p", [Node]),
    case rabbit_health_check:node(Node, Timeout) of
        ok ->
            io:format("Health check passed~n"),
            ok;
        Other ->
            Other
    end.

format_parse_error({_Line, Mod, Err}) -> lists:flatten(Mod:format_error(Err)).

sync_queue(Q) ->
    rabbit_mirror_queue_misc:sync_queue(Q).

cancel_sync_queue(Q) ->
    rabbit_mirror_queue_misc:cancel_sync_queue(Q).

purge_queue(Q) ->
    rabbit_amqqueue:with(
      Q, fun(Q1) ->
                 rabbit_amqqueue:purge(Q1),
                 ok
         end).

%%----------------------------------------------------------------------------

require_mnesia_stopped(Node, Fun) ->
    case Fun() of
        {error, mnesia_unexpectedly_running} ->
            {error_string, rabbit_misc:format(
                             " Mnesia is still running on node ~p.
        Please stop the node with rabbitmqctl stop_app first.", [Node])};
        Other -> Other
    end.

wait_for_application(Node, PidFile, Application, Inform) ->
    Pid = read_pid_file(PidFile, true),
    Inform("pid is ~s", [Pid]),
    wait_for_application(Node, Pid, Application).

wait_for_application(Node, Pid, rabbit_and_plugins) ->
    wait_for_startup(Node, Pid);
wait_for_application(Node, Pid, Application) ->
    while_process_is_alive(
      Node, Pid, fun() -> rabbit_nodes:is_running(Node, Application) end).

wait_for_startup(Node, Pid) ->
    while_process_is_alive(
      Node, Pid, fun() -> rpc:call(Node, rabbit, await_startup, []) =:= ok end).

while_process_is_alive(Node, Pid, Activity) ->
    case rabbit_misc:is_os_process_alive(Pid) of
        true  -> case Activity() of
                     true  -> ok;
                     false -> timer:sleep(?EXTERNAL_CHECK_INTERVAL),
                              while_process_is_alive(Node, Pid, Activity)
                 end;
        false -> {error, process_not_running}
    end.

wait_for_process_death(Pid) ->
    case rabbit_misc:is_os_process_alive(Pid) of
        true  -> timer:sleep(?EXTERNAL_CHECK_INTERVAL),
                 wait_for_process_death(Pid);
        false -> ok
    end.

read_pid_file(PidFile, Wait) ->
    case {file:read_file(PidFile), Wait} of
        {{ok, Bin}, _} ->
            S = binary_to_list(Bin),
            {match, [PidS]} = re:run(S, "[^\\s]+",
                                     [{capture, all, list}]),
            try list_to_integer(PidS)
            catch error:badarg ->
                    exit({error, {garbage_in_pid_file, PidFile}})
            end,
            PidS;
        {{error, enoent}, true} ->
            timer:sleep(?EXTERNAL_CHECK_INTERVAL),
            read_pid_file(PidFile, Wait);
        {{error, _} = E, _} ->
            exit({error, {could_not_read_pid, E}})
    end.

become(BecomeNode) ->
    error_logger:tty(false),
    case net_adm:ping(BecomeNode) of
        pong -> exit({node_running, BecomeNode});
        pang -> ok = net_kernel:stop(),
                io:format("  * Impersonating node: ~s...", [BecomeNode]),
                {ok, _} = rabbit_cli:start_distribution(BecomeNode),
                io:format(" done~n", []),
                Dir = mnesia:system_info(directory),
                io:format("  * Mnesia directory  : ~s~n", [Dir])
    end.

%%----------------------------------------------------------------------------

default_if_empty(List, Default) when is_list(List) ->
    if List == [] -> Default;
       true       -> [list_to_atom(X) || X <- List]
    end.

display_info_message_row(IsEscaped, Result, InfoItemKeys) ->
    display_row([format_info_item(
                   case proplists:lookup(X, Result) of
                       none when is_list(Result), length(Result) > 0 ->
                           exit({error, {bad_info_key, X}});
                       none -> Result;
                       {X, Value} -> Value
                   end, IsEscaped) || X <- InfoItemKeys]).

display_info_message(IsEscaped) ->
    fun ([], _) ->
            ok;
        ([FirstResult|_] = List, InfoItemKeys) when is_list(FirstResult) ->
            lists:foreach(fun(Result) ->
                                  display_info_message_row(IsEscaped, Result, InfoItemKeys)
                          end,
                          List),
            ok;
        (Result, InfoItemKeys) ->
            display_info_message_row(IsEscaped, Result, InfoItemKeys)
    end.

display_info_list(Results, InfoItemKeys) when is_list(Results) ->
    lists:foreach(
      fun (Result) -> display_row(
                        [format_info_item(proplists:get_value(X, Result), true)
                         || X <- InfoItemKeys])
      end, lists:sort(Results)),
    ok;
display_info_list(Other, _) ->
    Other.

display_row(Row) ->
    io:fwrite(string:join(Row, "\t")),
    io:nl().

-define(IS_U8(X),  (X >= 0 andalso X =< 255)).
-define(IS_U16(X), (X >= 0 andalso X =< 65535)).

format_info_item(#resource{name = Name}, IsEscaped) ->
    escape(Name, IsEscaped);
format_info_item({N1, N2, N3, N4} = Value, _IsEscaped) when
      ?IS_U8(N1), ?IS_U8(N2), ?IS_U8(N3), ?IS_U8(N4) ->
    rabbit_misc:ntoa(Value);
format_info_item({K1, K2, K3, K4, K5, K6, K7, K8} = Value, _IsEscaped) when
      ?IS_U16(K1), ?IS_U16(K2), ?IS_U16(K3), ?IS_U16(K4),
      ?IS_U16(K5), ?IS_U16(K6), ?IS_U16(K7), ?IS_U16(K8) ->
    rabbit_misc:ntoa(Value);
format_info_item(Value, _IsEscaped) when is_pid(Value) ->
    rabbit_misc:pid_to_string(Value);
format_info_item(Value, IsEscaped) when is_binary(Value) ->
    escape(Value, IsEscaped);
format_info_item(Value, IsEscaped) when is_atom(Value) ->
    escape(atom_to_list(Value), IsEscaped);
format_info_item([{TableEntryKey, TableEntryType, _TableEntryValue} | _] =
                     Value, IsEscaped) when is_binary(TableEntryKey) andalso
                                              is_atom(TableEntryType) ->
    io_lib:format("~1000000000000p", [prettify_amqp_table(Value, IsEscaped)]);
format_info_item([T | _] = Value, IsEscaped)
  when is_tuple(T) orelse is_pid(T) orelse is_binary(T) orelse is_atom(T) orelse
       is_list(T) ->
    "[" ++
        lists:nthtail(2, lists:append(
                           [", " ++ format_info_item(E, IsEscaped)
                            || E <- Value])) ++ "]";
format_info_item(Value, _IsEscaped) ->
    io_lib:format("~w", [Value]).

display_call_result(Node, MFA) ->
    case call(Node, MFA) of
        {badrpc, _} = Res -> throw(Res);
        Res               -> io:format("~p~n", [Res]),
                             ok
    end.

unsafe_rpc(Node, Mod, Fun, Args) ->
    case rpc_call(Node, Mod, Fun, Args) of
        {badrpc, _} = Res -> throw(Res);
        Normal            -> Normal
    end.

ensure_app_running(Node) ->
    case call(Node, {rabbit, is_running, []}) of
        true  -> ok;
        false -> {error_string,
                  rabbit_misc:format(
                    "rabbit application is not running on node ~s.~n"
                    " * Suggestion: start it with \"rabbitmqctl start_app\" "
                    "and try again", [Node])};
        Other -> Other
    end.

call(Node, {Mod, Fun, Args}) ->
    rpc_call(Node, Mod, Fun, lists:map(fun list_to_binary_utf8/1, Args)).

call(Node, {Mod, Fun, Args}, InfoKeys, Timeout) ->
    call(Node, {Mod, Fun, Args}, InfoKeys, false, Timeout, false).

call(Node, {Mod, Fun, Args}, InfoKeys, ToBinUtf8, Timeout) ->
    call(Node, {Mod, Fun, Args}, InfoKeys, ToBinUtf8, Timeout, false).

call(Node, {Mod, Fun, Args}, InfoKeys, ToBinUtf8, Timeout, IsEscaped) ->
    Args0 = case ToBinUtf8 of
                true  -> lists:map(fun list_to_binary_utf8/1, Args);
                false -> Args
            end,
    Ref = make_ref(),
    Pid = self(),
    spawn_link(
      fun () ->
              case rabbit_cli:rpc_call(Node, Mod, Fun, Args0,
                                       Ref, Pid, Timeout) of
                  {error, _} = Error        ->
                      Pid ! {error, Error};
                  {bad_argument, _} = Error ->
                      Pid ! {error, Error};
                  _                         ->
                      ok
              end
      end),
    rabbit_control_misc:wait_for_info_messages(
      Pid, Ref, InfoKeys, display_info_message(IsEscaped), Timeout).

list_to_binary_utf8(L) ->
    B = list_to_binary(L),
    case rabbit_binary_parser:validate_utf8(B) of
        ok    -> B;
        error -> throw({error, {not_utf_8, L}})
    end.

%% escape does C-style backslash escaping of non-printable ASCII
%% characters.  We don't escape characters above 127, since they may
%% form part of UTF-8 strings.

escape(Atom, IsEscaped) when is_atom(Atom) ->
    escape(atom_to_list(Atom), IsEscaped);
escape(Bin, IsEscaped)  when is_binary(Bin) ->
    escape(binary_to_list(Bin), IsEscaped);
escape(L, false) when is_list(L) ->
    escape_char(lists:reverse(L), []);
escape(L, true) when is_list(L) ->
    L.

escape_char([$\\ | T], Acc) ->
    escape_char(T, [$\\, $\\ | Acc]);
escape_char([X | T], Acc) when X >= 32, X /= 127 ->
    escape_char(T, [X | Acc]);
escape_char([X | T], Acc) ->
    escape_char(T, [$\\, $0 + (X bsr 6), $0 + (X band 8#070 bsr 3),
                    $0 + (X band 7) | Acc]);
escape_char([], Acc) ->
    Acc.

prettify_amqp_table(Table, IsEscaped) ->
    [{escape(K, IsEscaped), prettify_typed_amqp_value(T, V, IsEscaped)}
     || {K, T, V} <- Table].

prettify_typed_amqp_value(longstr, Value, IsEscaped) ->
    escape(Value, IsEscaped);
prettify_typed_amqp_value(table, Value, IsEscaped) ->
    prettify_amqp_table(Value, IsEscaped);
prettify_typed_amqp_value(array, Value, IsEscaped) ->
    [prettify_typed_amqp_value(T, V, IsEscaped) || {T, V} <- Value];
prettify_typed_amqp_value(_Type, Value, _IsEscaped) ->
    Value.

split_list([])         -> [];
split_list([_])        -> exit(even_list_needed);
split_list([A, B | T]) -> [{A, B} | split_list(T)].

nodes_in_cluster(Node) ->
    unsafe_rpc(Node, rabbit_mnesia, cluster_nodes, [running]).

alarms_by_node(Name) ->
    case rpc_call(Name, rabbit, status, []) of
        {badrpc,nodedown} -> {Name, [nodedown]};
        Status ->
            {_, As} = lists:keyfind(alarms, 1, Status),
            {Name, As}
    end.
