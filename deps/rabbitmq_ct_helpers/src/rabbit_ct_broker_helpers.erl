%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_ct_broker_helpers).

-include_lib("common_test/include/ct.hrl").
-include_lib("kernel/include/inet.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
    setup_steps/0,
    setup_steps_for_vms/0,
    teardown_steps/0,
    teardown_steps_for_vms/0,
    run_make_dist/1,
    start_rabbitmq_nodes/1,
    start_rabbitmq_nodes_on_vms/1,
    stop_rabbitmq_nodes/1,
    stop_rabbitmq_nodes_on_vms/1,
    rewrite_node_config_file/2,
    cluster_nodes/1, cluster_nodes/2,

    get_node_configs/1, get_node_configs/2,
    get_node_config/2, get_node_config/3, set_node_config/3,
    nodename_to_index/2,
    node_uri/2, node_uri/3,

    control_action/2, control_action/3, control_action/4,
    rabbitmqctl/3, rabbitmqctl/4, rabbitmqctl_list/3,
    rabbitmq_queues/3,

    add_code_path_to_node/2,
    add_code_path_to_all_nodes/2,
    rpc/5, rpc/6,
    rpc_all/4, rpc_all/5,

    start_node/2,
    start_broker/2,
    restart_broker/2,
    stop_broker/2,
    restart_node/2,
    stop_node/2,
    stop_node_after/3,
    kill_node/2,
    kill_node_after/3,

    reset_node/2,
    force_reset_node/2,

    forget_cluster_node/3,
    forget_cluster_node/4,

    cluster_members_online/2,

    is_feature_flag_supported/2,
    is_feature_flag_supported/3,
    enable_feature_flag/2,
    enable_feature_flag/3,

    drain_node/2,
    revive_node/2,
    mark_as_being_drained/2,
    unmark_as_being_drained/2,
    is_being_drained_local_read/2,
    is_being_drained_local_read/3,
    is_being_drained_consistent_read/2,
    is_being_drained_consistent_read/3,

    set_partition_handling_mode/3,
    set_partition_handling_mode_globally/2,
    configure_dist_proxy/1,
    block_traffic_between/2,
    allow_traffic_between/2,

    get_connection_pids/1,
    close_all_connections/3,

    set_policy/6,
    set_policy/7,
    set_policy_in_vhost/7,
    set_policy_in_vhost/8,

    clear_policy/3,
    clear_policy/4,
    set_operator_policy/6,
    clear_operator_policy/3,
    set_ha_policy/4, set_ha_policy/5,
    set_ha_policy_all/1,
    set_ha_policy_all/2,
    set_ha_policy_two_pos/1,
    set_ha_policy_two_pos_batch_sync/1,

    set_parameter/5,
    set_parameter/6,
    set_parameter/7,
    clear_parameter/4,
    clear_parameter/5,
    clear_parameter/6,

    set_global_parameter/3,
    set_global_parameter/4,
    clear_global_parameter/2,
    clear_global_parameter/3,

    add_vhost/2,
    add_vhost/3,
    add_vhost/4,
    delete_vhost/2,
    delete_vhost/3,
    delete_vhost/4,

    force_vhost_failure/2,
    force_vhost_failure/3,

    set_alarm/3,
    get_alarms/2,
    get_local_alarms/2,
    clear_alarm/3,
    clear_all_alarms/2,

    add_user/2,
    add_user/3,
    add_user/4,
    add_user/5,
    set_user_tags/4,
    set_user_tags/5,

    delete_user/2,
    delete_user/3,
    delete_user/4,

    change_password/5,
    clear_password/4,

    change_password/3,

    switch_credential_validator/2,
    switch_credential_validator/3,

    set_permissions/6,
    set_permissions/7,
    set_permissions/8,
    set_full_permissions/2,
    set_full_permissions/3,
    set_full_permissions/4,

    clear_permissions/2,
    clear_permissions/3,
    clear_permissions/4,
    clear_permissions/5,

    set_vhost_limit/5,

    set_user_limits/3,
    set_user_limits/4,
    clear_user_limits/3,
    clear_user_limits/4,

    enable_plugin/3,
    disable_plugin/3,

    test_channel/0,
    test_writer/1,
    user/1
  ]).

%% Internal functions exported to be used by rpc:call/4.
-export([
    do_restart_broker/0
  ]).

-define(DEFAULT_USER, "guest").
-define(NODE_START_ATTEMPTS, 10).

-define(TCP_PORTS_BASE, 21000).
-define(TCP_PORTS_LIST, [
    tcp_port_amqp,
    tcp_port_amqp_tls,
    tcp_port_mgmt,
    tcp_port_erlang_dist,
    tcp_port_erlang_dist_proxy,
    tcp_port_mqtt,
    tcp_port_mqtt_tls,
    tcp_port_web_mqtt,
    tcp_port_stomp,
    tcp_port_stomp_tls,
    tcp_port_web_stomp,
    tcp_port_web_stomp_tls,
    tcp_port_stream,
    tcp_port_stream_tls
  ]).

%% -------------------------------------------------------------------
%% Broker setup/teardown steps.
%% -------------------------------------------------------------------

setup_steps() ->
    case os:getenv("RABBITMQ_RUN") of
        false ->
            [
                fun run_make_dist/1,
                fun rabbit_ct_helpers:ensure_rabbitmqctl_cmd/1,
                fun rabbit_ct_helpers:ensure_rabbitmqctl_app/1,
                fun rabbit_ct_helpers:ensure_rabbitmq_plugins_cmd/1,
                fun set_lager_flood_limit/1,
                fun start_rabbitmq_nodes/1,
                fun share_dist_and_proxy_ports_map/1
            ];
        _ ->
            [
                fun rabbit_ct_helpers:ensure_rabbitmqctl_cmd/1,
                fun rabbit_ct_helpers:load_rabbitmqctl_app/1,
                fun rabbit_ct_helpers:ensure_rabbitmq_plugins_cmd/1,
                fun set_lager_flood_limit/1,
                fun start_rabbitmq_nodes/1,
                fun share_dist_and_proxy_ports_map/1
            ]
    end.

teardown_steps() ->
    [
      fun stop_rabbitmq_nodes/1
    ].

setup_steps_for_vms() ->
    [
      fun rabbit_ct_helpers:ensure_rabbitmqctl_cmd/1,
      fun rabbit_ct_helpers:ensure_rabbitmqctl_app/1,
      fun rabbit_ct_helpers:ensure_rabbitmq_plugins_cmd/1,
      fun start_rabbitmq_nodes_on_vms/1,
      fun maybe_cluster_nodes/1
    ].

teardown_steps_for_vms() ->
    [
      fun stop_rabbitmq_nodes_on_vms/1
    ].

run_make_dist(Config) ->
    LockId = {make_dist, self()},
    global:set_lock(LockId, [node()]),
    case os:getenv("SKIP_MAKE_TEST_DIST") of
        false ->
            SrcDir = ?config(current_srcdir, Config),
            case rabbit_ct_helpers:make(Config, SrcDir, ["test-dist"]) of
                {ok, _} ->
                    %% The caller can set $SKIP_MAKE_TEST_DIST to
                    %% manually skip this step which can be time
                    %% consuming. But we also use this variable to
                    %% record the fact we went through it already so we
                    %% save redundant calls.
                    os:putenv("SKIP_MAKE_TEST_DIST", "true"),
                    global:del_lock(LockId, [node()]),
                    Config;
                _ ->
                    global:del_lock(LockId, [node()]),
                    {skip, "Failed to run \"make test-dist\""}
            end;
        _ ->
            global:del_lock(LockId, [node()]),
            ct:pal(?LOW_IMPORTANCE, "(skip `$MAKE test-dist`)", []),
            Config
    end.

set_lager_flood_limit(Config) ->
    rabbit_ct_helpers:merge_app_env(Config,
      {lager, [{error_logger_hwm, 10000}]}).

start_rabbitmq_nodes_on_vms(Config) ->
    ConfigsPerVM = configs_per_vm(Config),
    start_rabbitmq_nodes_on_vms(Config, ConfigsPerVM, []).

start_rabbitmq_nodes_on_vms(Config, [{Node, C} | Rest], NodeConfigsList) ->
    Config1 = rabbit_ct_helpers:set_config(Config, {rmq_nodes_clustered, false}),
    Ret = rabbit_ct_vm_helpers:rpc(Config1,
                                   Node,
                                   ?MODULE,
                                   start_rabbitmq_nodes,
                                   [C]),
    case Ret of
        {skip, _} = Error ->
            Error;
        _ ->
            NodeConfigs = get_node_configs(Ret),
            start_rabbitmq_nodes_on_vms(Config, Rest,
                                        [NodeConfigs | NodeConfigsList])
    end;
start_rabbitmq_nodes_on_vms(Config, [], NodeConfigsList) ->
    merge_node_configs(Config, lists:reverse(NodeConfigsList)).

start_rabbitmq_nodes(Config) ->
    Config0 = rabbit_ct_helpers:set_config(Config, [
        {rmq_username, list_to_binary(?DEFAULT_USER)},
        {rmq_password, list_to_binary(?DEFAULT_USER)},
        {rmq_vhost, <<"/">>},
        {rmq_channel_max, 0}]),
    Config1 = case rabbit_ct_helpers:get_config(Config0, rmq_hostname) of
                  undefined ->
                      rabbit_ct_helpers:set_config(
                        Config0, {rmq_hostname, "localhost"});
                  _ ->
                      Config0
              end,
    NodesCount = get_nodes_count(Config1),
    Clustered0 = rabbit_ct_helpers:get_config(Config1, rmq_nodes_clustered),
    Clustered = case Clustered0 of
        undefined            -> true;
        C when is_boolean(C) -> C
    end,
    Master = self(),
    Starters = [
      spawn_link(fun() -> start_rabbitmq_node(Master, Config1, [], I) end)
      || I <- lists:seq(0, NodesCount - 1)
    ],
    wait_for_rabbitmq_nodes(Config1, Starters, [], Clustered).

get_nodes_count(Config) ->
    NodesCount = rabbit_ct_helpers:get_config(Config, rmq_nodes_count),
    case NodesCount of
        undefined                                -> 1;
        N when is_integer(N) andalso N >= 1      -> N;
        L when is_list(L) andalso length(L) >= 1 -> length(L)
    end.

set_nodes_count(Config, NodesCount) ->
    rabbit_ct_helpers:set_config(Config, {rmq_nodes_count, NodesCount}).

configs_per_vm(Config) ->
    CTPeers = rabbit_ct_vm_helpers:get_ct_peers(Config),
    NodesCount = get_nodes_count(Config),
    InstanceCount = length(CTPeers),
    NodesPerVM = NodesCount div InstanceCount,
    Remaining = NodesCount rem InstanceCount,
    configs_per_vm(CTPeers, Config, [], NodesPerVM, Remaining).

configs_per_vm([CTPeer | Rest], Config, ConfigsPerVM, NodesPerVM, Remaining) ->
    Hostname = rabbit_ct_helpers:nodename_to_hostname(CTPeer),
    Config0 = rabbit_ct_helpers:set_config(Config, {rmq_hostname, Hostname}),
    NodesCount = if
                     Remaining > 0 -> NodesPerVM + 1;
                     true          -> NodesPerVM
                 end,
    if
        NodesCount > 0 ->
            Config1 = set_nodes_count(Config0, NodesCount),
            configs_per_vm(Rest, Config, [{CTPeer, Config1} | ConfigsPerVM],
                           NodesPerVM, Remaining - 1);
        true ->
            configs_per_vm(Rest, Config, ConfigsPerVM,
                           NodesPerVM, Remaining)
    end;
configs_per_vm([], _, ConfigsPerVM, _, _) ->
    lists:reverse(ConfigsPerVM).

merge_node_configs(Config, NodeConfigsList) ->
    merge_node_configs(Config, NodeConfigsList, []).

merge_node_configs(Config, [], MergedNodeConfigs) ->
    rabbit_ct_helpers:set_config(Config, {rmq_nodes, MergedNodeConfigs});
merge_node_configs(Config, NodeConfigsList, MergedNodeConfigs) ->
    HeadsAndTails = [{H, T} || [H | T] <- NodeConfigsList],
    Heads = [H || {H, _} <- HeadsAndTails],
    Tails = [T || {_, T} <- HeadsAndTails],
    merge_node_configs(Config, Tails, MergedNodeConfigs ++ Heads).

wait_for_rabbitmq_nodes(Config, [], NodeConfigs, Clustered) ->
    NodeConfigs1 = [NC || {_, NC} <- lists:keysort(1, NodeConfigs)],
    Config1 = rabbit_ct_helpers:set_config(Config, {rmq_nodes, NodeConfigs1}),
    if
        Clustered -> cluster_nodes(Config1);
        true      -> Config1
    end;
wait_for_rabbitmq_nodes(Config, Starting, NodeConfigs, Clustered) ->
    receive
        {_, {skip, _} = Error} ->
            NodeConfigs1 = [NC || {_, NC} <- NodeConfigs],
            Config1 = rabbit_ct_helpers:set_config(Config,
              {rmq_nodes, NodeConfigs1}),
            stop_rabbitmq_nodes(Config1),
            Error;
        {Pid, I, NodeConfig} when NodeConfigs =:= [] ->
            wait_for_rabbitmq_nodes(Config, Starting -- [Pid],
              [{I, NodeConfig} | NodeConfigs], Clustered);
        {Pid, I, NodeConfig} ->
            wait_for_rabbitmq_nodes(Config, Starting -- [Pid],
              [{I, NodeConfig} | NodeConfigs], Clustered)
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
    Attempts0 = rabbit_ct_helpers:get_config(NodeConfig, failed_boot_attempts),
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
              {failed_boot_attempts, Attempts + 1}),
            start_rabbitmq_node(Master, Config, NodeConfig5, I);
        NodeConfig4 ->
            Master ! {self(), I, NodeConfig4},
            unlink(Master)
    end.

run_node_steps(Config, NodeConfig, I, [Step | Rest]) ->
    case Step(Config, NodeConfig, I) of
        {skip, _} = Error -> Error;
        NodeConfig1       -> run_node_steps(Config, NodeConfig1, I, Rest)
    end;
run_node_steps(_, NodeConfig, _, []) ->
    NodeConfig.

init_tcp_port_numbers(Config, NodeConfig, I) ->
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
    ExtraPorts = case rabbit_ct_helpers:get_config(Config, rmq_extra_tcp_ports) of
        undefined           -> [];
        EP when is_list(EP) -> EP
    end,
    PortsCount = length(?TCP_PORTS_LIST) + length(ExtraPorts),
    Base = case rabbit_ct_helpers:get_config(NodeConfig, tcp_ports_base) of
        undefined -> tcp_port_base_for_broker(Config, I, PortsCount);
        P         -> P + PortsCount
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
      {NodeConfig1, Base}, ?TCP_PORTS_LIST ++ ExtraPorts),
    %% Finally, update the RabbitMQ configuration with the computed TCP
    %% port numbers. Extra TCP ports are not added automatically to the
    %% configuration.
    update_tcp_ports_in_rmq_config(NodeConfig2, ?TCP_PORTS_LIST).

tcp_port_base_for_broker(Config, I, PortsCount) ->
    Base = case rabbit_ct_helpers:get_config(Config, tcp_ports_base) of
        undefined ->
            ?TCP_PORTS_BASE;
        {skip_n_nodes, N} ->
            tcp_port_base_for_broker1(?TCP_PORTS_BASE, N, PortsCount);
        B ->
            B
    end,
    tcp_port_base_for_broker1(Base, I, PortsCount).

tcp_port_base_for_broker1(Base, I, PortsCount) ->
    Base + I * PortsCount * ?NODE_START_ATTEMPTS.

update_tcp_ports_in_rmq_config(NodeConfig, [tcp_port_amqp = Key | Rest]) ->
    NodeConfig1 = rabbit_ct_helpers:merge_app_env(NodeConfig,
      {rabbit, [{tcp_listeners, [?config(Key, NodeConfig)]}]}),
    update_tcp_ports_in_rmq_config(NodeConfig1, Rest);
update_tcp_ports_in_rmq_config(NodeConfig, [tcp_port_amqp_tls = Key | Rest]) ->
    NodeConfig1 = rabbit_ct_helpers:merge_app_env(NodeConfig,
      {rabbit, [{ssl_listeners, [?config(Key, NodeConfig)]}]}),
    update_tcp_ports_in_rmq_config(NodeConfig1, Rest);
update_tcp_ports_in_rmq_config(NodeConfig, [tcp_port_mgmt = Key | Rest]) ->
    NodeConfig1 = rabbit_ct_helpers:merge_app_env(NodeConfig,
      {rabbitmq_management, [{tcp_config, [{port, ?config(Key, NodeConfig)}]}]}),
    update_tcp_ports_in_rmq_config(NodeConfig1, Rest);
update_tcp_ports_in_rmq_config(NodeConfig, [tcp_port_mqtt = Key | Rest]) ->
    NodeConfig1 = rabbit_ct_helpers:merge_app_env(NodeConfig,
      {rabbitmq_mqtt, [{tcp_listeners, [?config(Key, NodeConfig)]}]}),
    update_tcp_ports_in_rmq_config(NodeConfig1, Rest);
update_tcp_ports_in_rmq_config(NodeConfig, [tcp_port_mqtt_tls = Key | Rest]) ->
    NodeConfig1 = rabbit_ct_helpers:merge_app_env(NodeConfig,
      {rabbitmq_mqtt, [{ssl_listeners, [?config(Key, NodeConfig)]}]}),
    update_tcp_ports_in_rmq_config(NodeConfig1, Rest);
update_tcp_ports_in_rmq_config(NodeConfig, [tcp_port_web_mqtt = Key | Rest]) ->
    NodeConfig1 = rabbit_ct_helpers:merge_app_env(NodeConfig,
      {rabbitmq_web_mqtt, [{tcp_config, [{port, ?config(Key, NodeConfig)}]}]}),
    update_tcp_ports_in_rmq_config(NodeConfig1, Rest);
update_tcp_ports_in_rmq_config(NodeConfig, [tcp_port_web_stomp = Key | Rest]) ->
    NodeConfig1 = rabbit_ct_helpers:merge_app_env(NodeConfig,
      {rabbitmq_web_stomp, [{tcp_config, [{port, ?config(Key, NodeConfig)}]}]}),
    update_tcp_ports_in_rmq_config(NodeConfig1, Rest);
update_tcp_ports_in_rmq_config(NodeConfig, [tcp_port_web_stomp_tls | Rest]) ->
    %% Skip this one, because we need more than just a port to configure
    update_tcp_ports_in_rmq_config(NodeConfig, Rest);
update_tcp_ports_in_rmq_config(NodeConfig, [tcp_port_stomp = Key | Rest]) ->
    NodeConfig1 = rabbit_ct_helpers:merge_app_env(NodeConfig,
      {rabbitmq_stomp, [{tcp_listeners, [?config(Key, NodeConfig)]}]}),
    update_tcp_ports_in_rmq_config(NodeConfig1, Rest);
update_tcp_ports_in_rmq_config(NodeConfig, [tcp_port_stomp_tls = Key | Rest]) ->
    NodeConfig1 = rabbit_ct_helpers:merge_app_env(NodeConfig,
      {rabbitmq_stomp, [{ssl_listeners, [?config(Key, NodeConfig)]}]}),
    update_tcp_ports_in_rmq_config(NodeConfig1, Rest);
update_tcp_ports_in_rmq_config(NodeConfig, [tcp_port_stream = Key | Rest]) ->
    NodeConfig1 = rabbit_ct_helpers:merge_app_env(NodeConfig,
        {rabbitmq_stream, [{tcp_listeners, [?config(Key, NodeConfig)]}]}),
    update_tcp_ports_in_rmq_config(NodeConfig1, Rest);
update_tcp_ports_in_rmq_config(NodeConfig, [tcp_port_stream_tls = Key | Rest]) ->
    NodeConfig1 = rabbit_ct_helpers:merge_app_env(NodeConfig,
        {rabbitmq_stream, [{ssl_listeners, [?config(Key, NodeConfig)]}]}),
    update_tcp_ports_in_rmq_config(NodeConfig1, Rest);
update_tcp_ports_in_rmq_config(NodeConfig, [tcp_port_erlang_dist | Rest]) ->
    %% The Erlang distribution port doesn't appear in the configuration file.
    update_tcp_ports_in_rmq_config(NodeConfig, Rest);
update_tcp_ports_in_rmq_config(NodeConfig, [tcp_port_erlang_dist_proxy | Rest]) ->
    %% inet_proxy_dist port doesn't appear in the configuration file.
    update_tcp_ports_in_rmq_config(NodeConfig, Rest);
update_tcp_ports_in_rmq_config(NodeConfig, []) ->
    NodeConfig.

init_nodename(Config, NodeConfig, I) ->
    Hostname = ?config(rmq_hostname, Config),
    Nodename0 = case rabbit_ct_helpers:get_config(Config, rmq_nodes_count) of
        NodesList when is_list(NodesList) ->
            Name = lists:nth(I + 1, NodesList),
            rabbit_misc:format("~s@~s", [Name, Hostname]);
        _ ->
            Base = ?config(tcp_ports_base, NodeConfig),
            Suffix0 = rabbit_ct_helpers:get_config(Config,
              rmq_nodename_suffix),
            Suffix = case Suffix0 of
                undefined               -> "";
                _ when is_atom(Suffix0) -> [$- | atom_to_list(Suffix0)];
                _                       -> [$- | Suffix0]
            end,
            rabbit_misc:format("rmq-ct~s-~b-~b@~s",
              [Suffix, I + 1, Base, Hostname])
    end,
    Nodename = list_to_atom(Nodename0),
    rabbit_ct_helpers:set_config(NodeConfig, [
        {nodename, Nodename},
        {initial_nodename, Nodename}
      ]).

init_config_filename(Config, NodeConfig, _I) ->
    PrivDir = ?config(priv_dir, Config),
    Nodename = ?config(nodename, NodeConfig),
    ConfigDir = filename:join(PrivDir, Nodename),
    ConfigFile = filename:join(ConfigDir, Nodename),
    rabbit_ct_helpers:set_config(NodeConfig,
      {erlang_node_config_filename, ConfigFile}).

write_config_file(Config, NodeConfig, _I) ->
    %% Prepare a RabbitMQ configuration.
    ErlangConfigBase = ?config(erlang_node_config, Config),
    ErlangConfigOverlay = ?config(erlang_node_config, NodeConfig),
    ErlangConfig = rabbit_ct_helpers:merge_app_env_in_erlconf(ErlangConfigBase,
      ErlangConfigOverlay),
    ConfigFile = ?config(erlang_node_config_filename, NodeConfig),
    ConfigDir = filename:dirname(ConfigFile),
    Ret1 = file:make_dir(ConfigDir),
    Ret2 = file:write_file(ConfigFile ++ ".config",
      rabbit_ct_helpers:convert_to_unicode_binary(
        io_lib:format("% vim:ft=erlang:~n~n~p.~n", [ErlangConfig]))),
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

do_start_rabbitmq_node(Config, NodeConfig, I) ->
    WithPlugins0 = rabbit_ct_helpers:get_config(Config,
      broker_with_plugins),
    WithPlugins = case is_list(WithPlugins0) of
        true  -> lists:nth(I + 1, WithPlugins0);
        false -> WithPlugins0
    end,
    CanUseSecondary = (I + 1) rem 2 =:= 0,
    UseSecondaryUmbrella = case ?config(secondary_umbrella, Config) of
                               false -> false;
                               _     -> CanUseSecondary
                           end,
    SrcDir = case WithPlugins of
        false when UseSecondaryUmbrella -> ?config(secondary_rabbit_srcdir,
                                                   Config);
        false                           -> ?config(rabbit_srcdir, Config);
        _ when UseSecondaryUmbrella     -> ?config(secondary_current_srcdir,
                                                   Config);
        _                               -> ?config(current_srcdir, Config)
    end,
    PrivDir = ?config(priv_dir, Config),
    Nodename = ?config(nodename, NodeConfig),
    InitialNodename = ?config(initial_nodename, NodeConfig),
    DistPort = ?config(tcp_port_erlang_dist, NodeConfig),
    ConfigFile = ?config(erlang_node_config_filename, NodeConfig),
    %% Use inet_proxy_dist to handle distribution. This is used by the
    %% partitions testsuite.
    DistMod = rabbit_ct_helpers:get_config(Config, erlang_dist_module),
    StartArgs0 = case DistMod of
        undefined ->
            "";
        _ ->
            DistModS = atom_to_list(DistMod),
            DistModPath = filename:absname(
              filename:dirname(code:where_is_file(DistModS ++ ".beam"))),
            DistArg = re:replace(DistModS, "_dist$", "", [{return, list}]),
            "-pa \"" ++ DistModPath ++ "\" -proto_dist " ++ DistArg
    end,
    %% Set the net_ticktime.
    CurrentTicktime = case net_kernel:get_net_ticktime() of
        {ongoing_change_to, T} -> T;
        T                      -> T
    end,
    StartArgs1 = case rabbit_ct_helpers:get_config(Config, net_ticktime) of
        undefined ->
            case CurrentTicktime of
                60 -> ok;
                _  -> net_kernel:set_net_ticktime(60)
            end,
            StartArgs0;
        Ticktime ->
            case CurrentTicktime of
                Ticktime -> ok;
                _        -> net_kernel:set_net_ticktime(Ticktime)
            end,
            StartArgs0 ++ " -kernel net_ticktime " ++ integer_to_list(Ticktime)
    end,
    ExtraArgs0 = [],
    ExtraArgs1 = case rabbit_ct_helpers:get_config(Config, rmq_plugins_dir) of
                     undefined ->
                         ExtraArgs0;
                     ExtraPluginsDir ->
                         [{"EXTRA_PLUGINS_DIR=~s", [ExtraPluginsDir]}
                          | ExtraArgs0]
                 end,
    StartWithPluginsDisabled = rabbit_ct_helpers:get_config(
                                 Config, start_rmq_with_plugins_disabled),
    ExtraArgs2 = case StartWithPluginsDisabled of
                     true -> ["LEAVE_PLUGINS_DISABLED=yes" | ExtraArgs1];
                     _    -> ExtraArgs1
                 end,
    KeepPidFile = rabbit_ct_helpers:get_config(
                    Config, keep_pid_file_on_exit),
    ExtraArgs3 = case KeepPidFile of
                     true -> ["RABBITMQ_KEEP_PID_FILE_ON_EXIT=yes" | ExtraArgs2];
                     _    -> ExtraArgs2
                 end,
    ExtraArgs4 = case WithPlugins of
                     false -> ExtraArgs3;
                     _     -> ["NOBUILD=1" | ExtraArgs3]
                 end,
    ExtraArgs = case UseSecondaryUmbrella of
                    true ->
                        DepsDir = ?config(erlang_mk_depsdir, Config),
                        ErlLibs = os:getenv("ERL_LIBS"),
                        SecDepsDir = ?config(secondary_erlang_mk_depsdir,
                                             Config),
                        SecErlLibs = lists:flatten(
                                       string:replace(ErlLibs,
                                                      DepsDir,
                                                      SecDepsDir,
                                                      all)),
                        SecNewScriptsDir = filename:join([SecDepsDir,
                                                          SrcDir,
                                                          "sbin"]),
                        SecOldScriptsDir = filename:join([SecDepsDir,
                                                          "rabbit",
                                                          "scripts"]),
                        SecNewScriptsDirExists = filelib:is_dir(
                                                   SecNewScriptsDir),
                        SecScriptsDir = case SecNewScriptsDirExists of
                                            true  -> SecNewScriptsDir;
                                            false -> SecOldScriptsDir
                                        end,
                        [{"DEPS_DIR=~s", [SecDepsDir]},
                         {"REBAR_DEPS_DIR=~s", [SecDepsDir]},
                         {"ERL_LIBS=~s", [SecErlLibs]},
                         {"RABBITMQ_SCRIPTS_DIR=~s", [SecScriptsDir]},
                         {"RABBITMQ_SERVER=~s/rabbitmq-server", [SecScriptsDir]},
                         {"RABBITMQCTL=~s/rabbitmqctl", [SecScriptsDir]},
                         {"RABBITMQ_PLUGINS=~s/rabbitmq-plugins", [SecScriptsDir]}
                         | ExtraArgs4];
                    false ->
                        ExtraArgs4
                end,
    MakeVars = [
      {"RABBITMQ_NODENAME=~s", [Nodename]},
      {"RABBITMQ_NODENAME_FOR_PATHS=~s", [InitialNodename]},
      {"RABBITMQ_DIST_PORT=~b", [DistPort]},
      {"RABBITMQ_CONFIG_FILE=~s", [ConfigFile]},
      {"RABBITMQ_SERVER_START_ARGS=~s", [StartArgs1]},
      "RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS=+S 2 +sbwt very_short +A 24",
      "RABBITMQ_LOG=debug",
      "RMQCTL_WAIT_TIMEOUT=180",
      {"TEST_TMPDIR=~s", [PrivDir]}
      | ExtraArgs],
    Cmd = ["start-background-broker" | MakeVars],
    case rabbit_ct_helpers:get_config(Config, rabbitmq_run_cmd) of
        undefined ->
            case rabbit_ct_helpers:make(Config, SrcDir, Cmd) of
                {ok, _} ->
                    NodeConfig1 = rabbit_ct_helpers:set_config(
                                    NodeConfig,
                                    [{effective_srcdir, SrcDir},
                                    {make_vars_for_node_startup, MakeVars}]),
                    query_node(Config, NodeConfig1);
                _ ->
                    AbortCmd = ["stop-node" | MakeVars],
                    _ = rabbit_ct_helpers:make(Config, SrcDir, AbortCmd),
                    {skip, "Failed to initialize RabbitMQ"}
            end;
        RunCmd ->
            RmqRun = case CanUseSecondary of
                false -> RunCmd;
                _ -> rabbit_ct_helpers:get_config(Config, rabbitmq_run_secondary_cmd, RunCmd)
            end,
            case rabbit_ct_helpers:exec([RmqRun, "-C", SrcDir] ++ Cmd) of
                {ok, _} ->
                    NodeConfig1 = rabbit_ct_helpers:set_config(
                                    NodeConfig,
                                    [{make_vars_for_node_startup, MakeVars}]),
                    query_node(Config, NodeConfig1);
                _ ->
                    AbortCmd = ["stop-node" | MakeVars],
                    _ = rabbit_ct_helpers:exec([RunCmd | AbortCmd]),
                    {skip, "Failed to initialize RabbitMQ"}
            end
    end.

query_node(Config, NodeConfig) ->
    Nodename = ?config(nodename, NodeConfig),
    PidFile = rpc(Config, Nodename, os, getenv, ["RABBITMQ_PID_FILE"]),
    MnesiaDir = rpc(Config, Nodename, mnesia, system_info, [directory]),
    {ok, PluginsDir} = rpc(Config, Nodename, application, get_env,
      [rabbit, plugins_dir]),
    {ok, EnabledPluginsFile} = rpc(Config, Nodename, application, get_env,
      [rabbit, enabled_plugins_file]),
    Vars0 = [{pid_file, PidFile},
             {mnesia_dir, MnesiaDir},
             {plugins_dir, PluginsDir},
             {enabled_plugins_file, EnabledPluginsFile}],
    Vars = try
               EnabledFeatureFlagsFile = rpc(Config, Nodename,
                                             rabbit_feature_flags,
                                             enabled_feature_flags_list_file,
                                             []),
               [{enabled_feature_flags_list_file, EnabledFeatureFlagsFile}
                | Vars0]
           catch
               exit:{undef, [{rabbit_feature_flags, _, _, _} | _]} ->
                   %% This happens if the queried node is a RabbitMQ
                   %% 3.7.x node. If this is the case, we can ignore
                   %% this and leave the `enabled_plugins_file` config
                   %% variable unset.
                   ct:pal("NO RABBITMQ_FEATURE_FLAGS_FILE"),
                   Vars0
           end,
    rabbit_ct_helpers:set_config(NodeConfig, Vars).

maybe_cluster_nodes(Config) ->
    Clustered0 = rabbit_ct_helpers:get_config(Config, rmq_nodes_clustered),
    Clustered = case Clustered0 of
        undefined            -> true;
        C when is_boolean(C) -> C
    end,
    if
        Clustered -> cluster_nodes(Config);
        true      -> Config
    end.

cluster_nodes(Config) ->
    [NodeConfig1 | NodeConfigs] = get_node_configs(Config),
    cluster_nodes1(Config, NodeConfig1, NodeConfigs).

cluster_nodes(Config, Nodes) ->
    [NodeConfig1 | NodeConfigs] = [
      get_node_config(Config, Node) || Node <- Nodes],
    cluster_nodes1(Config, NodeConfig1, NodeConfigs).

cluster_nodes1(Config, NodeConfig1, [NodeConfig2 | Rest]) ->
    case cluster_nodes(Config, NodeConfig2, NodeConfig1) of
        ok    -> cluster_nodes1(Config, NodeConfig1, Rest);
        Error -> Error
    end;
cluster_nodes1(Config, _, []) ->
    Config.

cluster_nodes(Config, NodeConfig1, NodeConfig2) ->
    Nodename1 = ?config(nodename, NodeConfig1),
    Nodename2 = ?config(nodename, NodeConfig2),
    Cmds = [
      ["stop_app"],
      ["join_cluster", Nodename2],
      ["start_app"]
    ],
    cluster_nodes1(Config, Nodename1, Nodename2, Cmds).

cluster_nodes1(Config, Nodename1, Nodename2, [Cmd | Rest]) ->
    case rabbitmqctl(Config, Nodename1, Cmd) of
        {ok, _} -> cluster_nodes1(Config, Nodename1, Nodename2, Rest);
        _       -> {skip,
                    "Failed to cluster nodes \"" ++ atom_to_list(Nodename1) ++
                    "\" and \"" ++ atom_to_list(Nodename2) ++ "\""}
    end;
cluster_nodes1(_, _, _, []) ->
    ok.

handle_nodes_in_parallel(NodeConfigs, Fun) ->
    T0 = erlang:timestamp(),
    Parent = self(),
    Procs = [
             begin
                 timer:sleep(rand:uniform(1000)),
      spawn_link(fun() ->
                         T1 = erlang:timestamp(),
                         Ret = Fun(NodeConfig),
                         T2 = erlang:timestamp(),
                         ct:pal(
                           ?LOW_IMPORTANCE,
                           "Time to run ~p for node ~s: ~b µs",
                          [Fun,
                           ?config(nodename, NodeConfig),
                           timer:now_diff(T2, T1)]),
                         Parent ! {parallel_handling_ret,
                                   self(),
                                   NodeConfig,
                                   Ret}
                 end) end
      || NodeConfig <- NodeConfigs
    ],
    wait_for_node_handling(Procs, Fun, T0, []).

wait_for_node_handling([], Fun, T0, Results) ->
    T3 = erlang:timestamp(),
    ct:pal(
      ?LOW_IMPORTANCE,
      "Time to run ~p for all nodes: ~b µs",
      [Fun, timer:now_diff(T3, T0)]),
    Results;
wait_for_node_handling(Procs, Fun, T0, Results) ->
    receive
        {parallel_handling_ret, Proc, NodeConfig, Ret} ->
            Results1 = [{NodeConfig, Ret} | Results],
            wait_for_node_handling(Procs -- [Proc], Fun, T0, Results1)
    end.

move_nonworking_nodedir_away(NodeConfig) ->
    ConfigFile = ?config(erlang_node_config_filename, NodeConfig),
    ConfigDir = filename:dirname(ConfigFile),
    case os:getenv("RABBITMQ_CT_HELPERS_DELETE_UNUSED_NODES") =/= false
        andalso ?OTP_RELEASE >= 23 of
        true ->
            file:del_dir_r(ConfigDir);
        _ ->
            NewName = filename:join(
                        filename:dirname(ConfigDir),
                        "_unused_nodedir_" ++ filename:basename(ConfigDir)),
            file:rename(ConfigDir, NewName)
    end,
    lists:keydelete(erlang_node_config_filename, 1, NodeConfig).

share_dist_and_proxy_ports_map(Config) ->
    Map = [
      {
        ?config(tcp_port_erlang_dist, NodeConfig),
        ?config(tcp_port_erlang_dist_proxy, NodeConfig)
      } || NodeConfig <- get_node_configs(Config)],
    rpc_all(Config,
      application, set_env, [kernel, dist_and_proxy_ports_map, Map]),
    Config.

rewrite_node_config_file(Config, Node) ->
    NodeConfig = get_node_config(Config, Node),
    I = if
        is_integer(Node) -> Node;
        true             -> nodename_to_index(Config, Node)
    end,
    %% Keep copies of previous config file.
    ConfigFile = ?config(erlang_node_config_filename, NodeConfig),
    case rotate_config_file(ConfigFile) of
        ok ->
            ok;
        {error, Reason} ->
            ct:pal("Failed to rotate config file ~s: ~s",
              [ConfigFile, file:format_error(Reason)])
    end,
    %% Now we can write the new file. The caller is responsible for
    %% restarting the broker/node.
    case write_config_file(Config, NodeConfig, I) of
        {skip, Error} -> {error, Error};
        _NodeConfig1  -> ok
    end.

rotate_config_file(ConfigFile) ->
    rotate_config_file(ConfigFile, ConfigFile ++ ".config", 1).

rotate_config_file(ConfigFile, OldName, Ext) ->
    NewName = rabbit_misc:format("~s.config.~b", [ConfigFile, Ext]),
    case filelib:is_file(NewName) of
        true  ->
            case rotate_config_file(ConfigFile, NewName, Ext + 1) of
                ok    -> file:rename(OldName, NewName);
                Error -> Error
            end;
        false ->
            file:rename(OldName, NewName)
    end.

stop_rabbitmq_nodes_on_vms(Config) ->
    NodeConfigs = get_node_configs(Config),
    NodeConfigsPerCTPeer = [
                            {
                             rabbit_ct_helpers:nodename_to_hostname(CTPeer),
                             CTPeer,
                             []
                            }
                            || CTPeer <-
                               rabbit_ct_vm_helpers:get_ct_peers(Config)],
    stop_rabbitmq_nodes_on_vms(Config, NodeConfigs, NodeConfigsPerCTPeer).

stop_rabbitmq_nodes_on_vms(Config, [NodeConfig | Rest],
                           NodeConfigsPerCTPeer) ->
    RabbitMQNode = ?config(nodename, NodeConfig),
    Hostname = rabbit_ct_helpers:nodename_to_hostname(RabbitMQNode),
    {H, N, NodeConfigs} = lists:keyfind(Hostname, 1, NodeConfigsPerCTPeer),
    NewEntry = {H, N, [NodeConfig | NodeConfigs]},
    NodeConfigsPerCTPeer1 = lists:keystore(Hostname, 1,
                                           NodeConfigsPerCTPeer,
                                           NewEntry),
    stop_rabbitmq_nodes_on_vms(Config, Rest, NodeConfigsPerCTPeer1);
stop_rabbitmq_nodes_on_vms(Config, [], NodeConfigsPerCTPeer) ->
    lists:foreach(
      fun({_, CTPeer, NodeConfigs}) ->
              Config1 = rabbit_ct_helpers:set_config(Config,
                                                     {rmq_nodes,
                                                      NodeConfigs}),
              rabbit_ct_vm_helpers:rpc(Config1,
                                       CTPeer,
                                       ?MODULE,
                                       stop_rabbitmq_nodes,
                                       [Config1])
      end, NodeConfigsPerCTPeer),
    rabbit_ct_helpers:delete_config(Config, rmq_nodes).

stop_rabbitmq_nodes(Config) ->
    NodeConfigs = get_node_configs(Config),
    _ = handle_nodes_in_parallel(
          NodeConfigs,
          fun(NodeConfig) ->
                  stop_rabbitmq_node(Config, NodeConfig)
          end),
    proplists:delete(rmq_nodes, Config).

stop_rabbitmq_node(Config, NodeConfig) ->
    SrcDir = ?config(effective_srcdir, NodeConfig),
    InitialMakeVars = ?config(make_vars_for_node_startup, NodeConfig),
    Nodename = ?config(nodename, NodeConfig),
    InitialNodename = ?config(initial_nodename, NodeConfig),
    MakeVars = InitialMakeVars ++ [
      {"RABBITMQ_NODENAME=~s", [Nodename]},
      {"RABBITMQ_NODENAME_FOR_PATHS=~s", [InitialNodename]}
    ],
    Cmd = ["stop-node" | MakeVars],
    case rabbit_ct_helpers:get_config(Config, rabbitmq_run_cmd) of
        undefined ->
            rabbit_ct_helpers:make(Config, SrcDir, Cmd);
        RunCmd ->
            rabbit_ct_helpers:exec([RunCmd | Cmd])
    end,
    NodeConfig.

%% -------------------------------------------------------------------
%% Helpers for partition simulation
%% -------------------------------------------------------------------

configure_dist_proxy(Config) ->
    rabbit_ct_helpers:set_config(Config,
      {erlang_dist_module, inet_tcp_proxy_dist}).

block_traffic_between(NodeA, NodeB) ->
    ct:pal(
      ?LOW_IMPORTANCE,
      "Blocking traffic between ~s and ~s",
      [NodeA, NodeB]),
    ?assertEqual(ok, rpc:call(NodeA, inet_tcp_proxy_dist, block, [NodeB])),
    ?assertEqual(ok, rpc:call(NodeB, inet_tcp_proxy_dist, block, [NodeA])).

allow_traffic_between(NodeA, NodeB) ->
    ct:pal(
      ?LOW_IMPORTANCE,
      "Unblocking traffic between ~s and ~s",
      [NodeA, NodeB]),
    ?assertEqual(ok, rpc:call(NodeA, inet_tcp_proxy_dist, allow, [NodeB])),
    ?assertEqual(ok, rpc:call(NodeB, inet_tcp_proxy_dist, allow, [NodeA])).

set_partition_handling_mode_globally(Config, Mode) ->
    rpc_all(Config,
      application, set_env, [rabbit, cluster_partition_handling, Mode]).

set_partition_handling_mode(Config, Nodes, Mode) ->
    rpc(Config, Nodes,
      application, set_env, [rabbit, cluster_partition_handling, Mode]).

%% -------------------------------------------------------------------
%% Calls to rabbitmqctl from Erlang.
%% -------------------------------------------------------------------

control_action(Command, Node) ->
    control_action(Command, Node, [], []).

control_action(Command, Node, Args) ->
    control_action(Command, Node, Args, []).

control_action(Command, Node, Args, Opts) ->
    rabbit_control_helper:command(Command, Node, Args, Opts).

%% Use rabbitmqctl(1) instead of using the Erlang API.

rabbitmqctl(Config, Node, Args) ->
    rabbitmqctl(Config, Node, Args, infinity).

rabbitmqctl(Config, Node, Args, Timeout) ->
    Rabbitmqctl = ?config(rabbitmqctl_cmd, Config),
    NodeConfig = get_node_config(Config, Node),
    Nodename = ?config(nodename, NodeConfig),
    Env0 = [
      {"RABBITMQ_SCRIPTS_DIR", filename:dirname(Rabbitmqctl)},
      {"RABBITMQ_PID_FILE", ?config(pid_file, NodeConfig)},
      {"RABBITMQ_MNESIA_DIR", ?config(mnesia_dir, NodeConfig)},
      {"RABBITMQ_PLUGINS_DIR", ?config(plugins_dir, NodeConfig)},
      {"RABBITMQ_ENABLED_PLUGINS_FILE",
        ?config(enabled_plugins_file, NodeConfig)}
    ],
    Ret = rabbit_ct_helpers:get_config(
            NodeConfig, enabled_feature_flags_list_file),
    Env = case Ret of
              undefined ->
                  Env0;
              EnabledFeatureFlagsFile ->
                  Env0 ++
                  [{"RABBITMQ_FEATURE_FLAGS_FILE", EnabledFeatureFlagsFile}]
          end,
    Cmd = [Rabbitmqctl, "-n", Nodename | Args],
    rabbit_ct_helpers:exec(Cmd, [{env, Env}, {timeout, Timeout}]).

rabbitmqctl_list(Config, Node, Args) ->
    {ok, StdOut} = rabbitmqctl(Config, Node, Args),
    [<<"Timeout:", _/binary>>,
     <<"Listing", _/binary>>
     | Rows] = re:split(StdOut, <<"\n">>, [trim]),
    [re:split(Row, <<"\t">>) || Row <- Rows].

rabbitmq_queues(Config, Node, Args) ->
    RabbitmqQueues = ?config(rabbitmq_queues_cmd, Config),
    NodeConfig = rabbit_ct_broker_helpers:get_node_config(Config, Node),
    Nodename = ?config(nodename, NodeConfig),
    Env0 = [
      {"RABBITMQ_SCRIPTS_DIR", filename:dirname(RabbitmqQueues)},
      {"RABBITMQ_PID_FILE", ?config(pid_file, NodeConfig)},
      {"RABBITMQ_MNESIA_DIR", ?config(mnesia_dir, NodeConfig)},
      {"RABBITMQ_PLUGINS_DIR", ?config(plugins_dir, NodeConfig)},
      {"RABBITMQ_ENABLED_PLUGINS_FILE",
        ?config(enabled_plugins_file, NodeConfig)}
    ],
    Ret = rabbit_ct_helpers:get_config(
            NodeConfig, enabled_feature_flags_list_file),
    Env = case Ret of
              undefined ->
                  Env0;
              EnabledFeatureFlagsFile ->
                  Env0 ++
                  [{"RABBITMQ_FEATURE_FLAGS_FILE", EnabledFeatureFlagsFile}]
          end,
    Cmd = [RabbitmqQueues, "-n", Nodename | Args],
    rabbit_ct_helpers:exec(Cmd, [{env, Env}]).

%% -------------------------------------------------------------------
%% Other helpers.
%% -------------------------------------------------------------------

get_node_configs(Config) ->
    ?config(rmq_nodes, Config).

get_node_configs(Config, Key) ->
    NodeConfigs = get_node_configs(Config),
    [?config(Key, NodeConfig) || NodeConfig <- NodeConfigs].

get_node_config(Config, Node) when is_atom(Node) andalso Node =/= undefined ->
    NodeConfigs = get_node_configs(Config),
    get_node_config1(NodeConfigs, Node);
get_node_config(Config, I) when is_integer(I) andalso I >= 0 ->
    NodeConfigs = get_node_configs(Config),
    lists:nth(I + 1, NodeConfigs).

get_node_config1([NodeConfig | Rest], Node) ->
    case ?config(nodename, NodeConfig) of
        Node -> NodeConfig;
        _    -> case ?config(initial_nodename, NodeConfig) of
                    Node -> NodeConfig;
                    _    -> get_node_config1(Rest, Node)
                end
    end;
get_node_config1([], Node) ->
    exit({unknown_node, Node}).

get_node_config(Config, Node, Key) ->
    NodeConfig = get_node_config(Config, Node),
    ?config(Key, NodeConfig).

set_node_config(Config, Node, Tuples) ->
    NodeConfig = get_node_config(Config, Node),
    NodeConfig1 = rabbit_ct_helpers:set_config(NodeConfig, Tuples),
    replace_entire_node_config(Config, Node, NodeConfig1).

replace_entire_node_config(Config, Node, NewNodeConfig) ->
    NodeConfigs = get_node_configs(Config),
    NodeConfigs1 = lists:map(
      fun(NodeConfig) ->
          Match = case ?config(nodename, NodeConfig) of
              Node -> true;
              _    -> case ?config(initial_nodename, NodeConfig) of
                      Node -> true;
                      _    -> false
                  end
          end,
          if
              Match -> NewNodeConfig;
              true  -> NodeConfig
          end
      end, NodeConfigs),
    rabbit_ct_helpers:set_config(Config, {rmq_nodes, NodeConfigs1}).

nodename_to_index(Config, Node) ->
    NodeConfigs = get_node_configs(Config),
    nodename_to_index1(NodeConfigs, Node, 0).

nodename_to_index1([NodeConfig | Rest], Node, I) ->
    case ?config(nodename, NodeConfig) of
        Node -> I;
        _    -> case ?config(initial_nodename, NodeConfig) of
                    Node -> I;
                    _    -> nodename_to_index1(Rest, Node, I + 1)
                end
    end;
nodename_to_index1([], Node, _) ->
    exit({unknown_node, Node}).

node_uri(Config, Node) ->
    node_uri(Config, Node, []).

node_uri(Config, Node, amqp) ->
    node_uri(Config, Node, []);
node_uri(Config, Node, management) ->
    node_uri(Config, Node, [
        {scheme, "http"},
        {tcp_port_name, tcp_port_mgmt}
      ]);
node_uri(Config, Node, Options) ->
    Scheme = proplists:get_value(scheme, Options, "amqp"),
    Hostname = case proplists:get_value(use_ipaddr, Options, false) of
        true ->
            {ok, Hostent} = inet:gethostbyname(?config(rmq_hostname, Config)),
            format_ipaddr_for_uri(Hostent);
        Family when Family =:= inet orelse Family =:= inet6 ->
            {ok, Hostent} = inet:gethostbyname(?config(rmq_hostname, Config),
              Family),
            format_ipaddr_for_uri(Hostent);
        false ->
            ?config(rmq_hostname, Config)
    end,
    TcpPortName = proplists:get_value(tcp_port_name, Options, tcp_port_amqp),
    TcpPort = get_node_config(Config, Node, TcpPortName),
    UserPass = case proplists:get_value(with_user, Options, false) of
        true ->
            User = proplists:get_value(user, Options, "guest"),
            Password = proplists:get_value(password, Options, "guest"),
            io_lib:format("~s:~s@", [User, Password]);
        false ->
            ""
    end,
    list_to_binary(
      rabbit_misc:format("~s://~s~s:~b",
        [Scheme, UserPass, Hostname, TcpPort])).

format_ipaddr_for_uri(
  #hostent{h_addrtype = inet, h_addr_list = [IPAddr | _]}) ->
    {A, B, C, D} = IPAddr,
    io_lib:format("~b.~b.~b.~b", [A, B, C, D]);
format_ipaddr_for_uri(
  #hostent{h_addrtype = inet6, h_addr_list = [IPAddr | _]}) ->
    {A, B, C, D, E, F, G, H} = IPAddr,
    Res0 = io_lib:format(
      "~.16b:~.16b:~.16b:~.16b:~.16b:~.16b:~.16b:~.16b",
      [A, B, C, D, E, F, G, H]),
    Res1 = re:replace(Res0, "(^0(:0)+$|^(0:)+|(:0)+$)|:(0:)+", "::"),
    "[" ++ Res1 ++ "]".


%% Virtual host management

add_vhost(Config, VHost) ->
    add_vhost(Config, 0, VHost).

add_vhost(Config, Node, VHost) ->
    add_vhost(Config, Node, VHost, <<"acting-user">>).

add_vhost(Config, Node, VHost, Username) ->
    rpc(Config, Node, rabbit_vhost, add, [VHost, Username]).

delete_vhost(Config, VHost) ->
    delete_vhost(Config, 0, VHost).

delete_vhost(Config, Node, VHost) ->
    delete_vhost(Config, Node, VHost, <<"acting-user">>).

delete_vhost(Config, Node, VHost, Username) ->
    rpc(Config, Node, rabbit_vhost, delete, [VHost, Username]).

force_vhost_failure(Config, VHost) -> force_vhost_failure(Config, 0, VHost).

force_vhost_failure(Config, Node, VHost) ->
    force_vhost_failure(Config, Node, VHost, 100).

force_vhost_failure(_Config, _Node, VHost, 0) ->
    error({failed_to_force_vhost_failure, no_more_attempts_left, VHost});
force_vhost_failure(Config, Node, VHost, Attempts) ->
    case rpc(Config, Node, rabbit_vhost_sup_sup, is_vhost_alive, [VHost]) of
        true  ->
            try
                MessageStorePid = get_message_store_pid(Config, Node, VHost),
                rpc(Config, Node,
                    erlang, exit, [MessageStorePid, force_vhost_failure]),
                %% Give it a time to fail
                timer:sleep(300),
                force_vhost_failure(Config, Node, VHost, Attempts - 1)
            catch
                %% The vhost terminated while we were checking again.
                exit:{shutdown, _} ->
                    timer:sleep(300),
                    force_vhost_failure(Config, Node, VHost, Attempts - 1);
                exit:{badmatch,
                      {error,
                       {vhost_supervisor_not_running, VHost}}} ->
                    %% This badmatch may occur in get_message_store_pid/3 as a
                    %% result of `{ok, VHostSup} = rpc(...)`.
                    timer:sleep(300),
                    force_vhost_failure(Config, Node, VHost, Attempts - 1)
            end;
        false -> ok
    end.

set_alarm(Config, Node, file_descriptor_limit = Resource) ->
    rpc(Config, Node, rabbit_alarm, set_alarm, [{Resource, []}]);
set_alarm(Config, Node, memory = Resource) ->
    rpc(Config, Node, rabbit_alarm, set_alarm, [{{resource_limit, Resource, Node}, []}]);
set_alarm(Config, Node, disk = Resource) ->
    rpc(Config, Node, rabbit_alarm, set_alarm, [{{resource_limit, Resource, Node}, []}]).

get_alarms(Config, Node) ->
    rpc(Config, Node, rabbit_alarm, get_alarms, []).

get_local_alarms(Config, Node) ->
    rpc(Config, Node, rabbit_alarm, get_local_alarms, []).

clear_alarm(Config, Node, file_descriptor_limit = Resource) ->
    rpc(Config, Node, rabbit_alarm, clear_alarm, [Resource]);
clear_alarm(Config, Node, memory = Resource) ->
    rpc(Config, Node, rabbit_alarm, clear_alarm, [{resource_limit, Resource, Node}]);
clear_alarm(Config, Node, disk = Resource) ->
    rpc(Config, Node, rabbit_alarm, clear_alarm, [{resource_limit, Resource, Node}]).

clear_all_alarms(Config, Node) ->
    lists:foreach(fun ({file_descriptor_limit, _}) ->
                        clear_alarm(Config, Node, file_descriptor_limit);
                      ({{resource_limit, Resource, OnNode}, _}) when OnNode =:= Node ->
                        clear_alarm(Config, Node, Resource);
                      (_) -> ok
                  end, get_alarms(Config, Node)),
    ok.

get_message_store_pid(Config, Node, VHost) ->
    {ok, VHostSup} = rpc(Config, Node,
                         rabbit_vhost_sup_sup, get_vhost_sup, [VHost]),
    Children = rpc(Config, Node, supervisor, which_children, [VHostSup]),
    [MsgStorePid] = [Pid || {Name, Pid, _, _} <- Children,
                            Name == msg_store_persistent],
    MsgStorePid.

add_user(Config, Username) ->
    %% for many tests it is convenient that
    %% the username and password match
    add_user(Config, 0, Username, Username).

add_user(Config, Username, Password) ->
    add_user(Config, 0, Username, Password).

add_user(Config, Node, Username, Password) ->
    add_user(Config, Node, Username, Password, <<"acting-user">>).

add_user(Config, Node, Username, Password, AuditUsername) ->
    rpc(Config, Node, rabbit_auth_backend_internal, add_user,
        [rabbit_data_coercion:to_binary(Username),
         rabbit_data_coercion:to_binary(Password),
         AuditUsername]).

set_user_tags(Config, Node, Username, Tags) ->
    set_user_tags(Config, Node, Username, Tags, <<"acting-user">>).

set_user_tags(Config, Node, Username, Tags, AuditUsername) ->
    rpc(Config, Node, rabbit_auth_backend_internal, set_tags,
        [Username, Tags, AuditUsername]).

delete_user(Config, Username) ->
    delete_user(Config, 0, Username).

delete_user(Config, Node, Username) ->
    delete_user(Config, Node, Username, <<"acting-user">>).

delete_user(Config, Node, Username, AuditUsername) ->
    rpc(Config, Node, rabbit_auth_backend_internal, delete_user,
        [Username, AuditUsername]).

change_password(Config, Username, Password) ->
    change_password(Config, 0, Username, Password, <<"acting-user">>).

change_password(Config, Node, Username, Password, AuditUsername) ->
    rpc(Config, Node, rabbit_auth_backend_internal, change_password,
                                 [Username, Password, AuditUsername]).

clear_password(Config, Node, Username, AuditUsername) ->
    rpc(Config, Node, rabbit_auth_backend_internal, clear_password,
        [Username, AuditUsername]).

switch_credential_validator(Config, accept_everything) ->
    rpc(Config, 0, application, set_env,
        [rabbit, credential_validator,
         [{validation_backend, rabbit_credential_validator_accept_everything}]]);

switch_credential_validator(Config, min_length) ->
    switch_credential_validator(Config, min_length, 5);

switch_credential_validator(Config, regexp) ->
    switch_credential_validator(Config, regexp, <<"^xyz\\d{10,12}$">>).


switch_credential_validator(Config, min_length, MinLength) ->
    ok = rpc(Config, 0, application, set_env,
             [rabbit, credential_validator,
              [{validation_backend, rabbit_credential_validator_min_password_length},
               {min_length,         MinLength}]]);

switch_credential_validator(Config, regexp, RegExp) ->
    ok = rpc(Config, 0, application, set_env,
             [rabbit, credential_validator,
              [{validation_backend, rabbit_credential_validator_password_regexp},
               {regexp,             RegExp}]]).

set_full_permissions(Config, VHost) ->
    set_permissions(Config, 0, <<"guest">>, VHost, <<".*">>, <<".*">>, <<".*">>).
set_full_permissions(Config, Username, VHost) ->
    set_permissions(Config, 0, Username, VHost, <<".*">>, <<".*">>, <<".*">>).
set_full_permissions(Config, Node, Username, VHost) ->
    set_permissions(Config, Node, Username, VHost, <<".*">>, <<".*">>, <<".*">>).

set_permissions(Config, Username, VHost, ConfigurePerm, WritePerm, ReadPerm) ->
    set_permissions(Config, 0, Username, VHost, ConfigurePerm, WritePerm, ReadPerm).

set_permissions(Config, Node, Username, VHost, ConfigurePerm, WritePerm, ReadPerm) ->
    set_permissions(Config, Node, Username, VHost, ConfigurePerm, WritePerm, ReadPerm,
                    <<"acting-user">>).

set_permissions(Config, Node, Username, VHost, ConfigurePerm, WritePerm, ReadPerm,
                ActingUser) ->
    rpc(Config, Node,
        rabbit_auth_backend_internal,
        set_permissions,
        [rabbit_data_coercion:to_binary(Username),
         rabbit_data_coercion:to_binary(VHost),
         rabbit_data_coercion:to_binary(ConfigurePerm),
         rabbit_data_coercion:to_binary(WritePerm),
         rabbit_data_coercion:to_binary(ReadPerm),
         ActingUser]).

clear_permissions(Config, VHost) ->
    clear_permissions(Config, 0, <<"guest">>, VHost).
clear_permissions(Config, Username, VHost) ->
    clear_permissions(Config, 0, Username, VHost).

clear_permissions(Config, Node, Username, VHost) ->
    clear_permissions(Config, Node, Username, VHost, <<"acting-user">>).

clear_permissions(Config, Node, Username, VHost, ActingUser) ->
    rpc(Config, Node,
        rabbit_auth_backend_internal,
        clear_permissions,
        [rabbit_data_coercion:to_binary(Username),
         rabbit_data_coercion:to_binary(VHost),
         ActingUser]).

set_vhost_limit(Config, Node, VHost, Limit0, Value) ->
    Limit = case Limit0 of
      max_connections -> <<"max-connections">>;
      max_queues      -> <<"max-queues">>;
      Other -> rabbit_data_coercion:to_binary(Other)
    end,
    Definition = rabbit_json:encode(#{Limit => Value}),
    rpc(Config, Node,
        rabbit_vhost_limit,
        set,
        [VHost, Definition, <<"ct-tests">>]).

set_user_limits(Config, Username, Limits) ->
    set_user_limits(Config, 0, Username, Limits).

set_user_limits(Config, Node, Username, Limits0) when is_map(Limits0) ->
    Limits =
        maps:fold(fun(Limit0, Val, Acc) ->
                      Limit = case Limit0 of
                          max_connections -> <<"max-connections">>;
                          max_channels    -> <<"max-channels">>;
                          Other -> rabbit_data_coercion:to_binary(Other)
                      end,
                      maps:merge(#{Limit => Val}, Acc)
                  end, #{}, Limits0),
    rpc(Config, Node,
        rabbit_auth_backend_internal,
        set_user_limits,
        [Username, Limits, <<"ct-tests">>]).

clear_user_limits(Config, Username, Limit) ->
    clear_user_limits(Config, 0, Username, Limit).

clear_user_limits(Config, Node, Username, Limit0) ->
    Limit = case Limit0 of
      all             -> <<"all">>;
      max_connections -> <<"max-connections">>;
      max_channels    -> <<"max-channels">>;
      Other -> rabbit_data_coercion:to_binary(Other)
    end,
    rpc(Config, Node,
        rabbit_auth_backend_internal,
        clear_user_limits,
        [Username, Limit, <<"ct-tests">>]).

%% Functions to execute code on a remote node/broker.

add_code_path_to_node(Node, Module) ->
    Path1 = filename:dirname(code:which(Module)),
    Path2 = filename:dirname(code:which(?MODULE)),
    Paths = filter_ct_helpers_and_testsuites_paths(
              lists:usort([Path1, Path2])),
    case Paths of
        [] ->
            ok;
        _ ->
            case rpc:call(Node, code, get_path, []) of
                ExistingPaths when is_list(ExistingPaths) ->
                    lists:foreach(
                      fun(P) ->
                              case lists:member(P, ExistingPaths) of
                                  true ->
                                      ok;
                                  false ->
                                      case rpc:call(
                                             Node, code, add_pathz, [P]) of
                                          true ->
                                              ok;
                                          Error ->
                                              erlang:error({Error, P})
                                      end
                              end
                      end, Paths);
                Error ->
                    ct:pal(?LOW_IMPORTANCE,
                           "Failed to retrieve current code path from node ~s: ~p~n",
                           [Node, Error]),
                    ok
            end
    end.

filter_ct_helpers_and_testsuites_paths(CodePath) ->
    lists:filter(
      fun(Dir) ->
              DirName = filename:basename(Dir),
              ParentDirName = filename:basename(
                                filename:dirname(Dir)),
              Dir =/= "." andalso
              %% FIXME: This filtering is too naive. How to properly
              %% distinguish RabbitMQ-related applications from
              %% test-only modules?
              ((ParentDirName =:= "rabbitmq_ct_helpers" andalso
                DirName =:= "ebin") orelse
               (ParentDirName =:= "rabbitmq_ct_client_helpers" andalso
                DirName =:= "ebin") orelse
               (DirName =:= "test"))
      end, CodePath).

add_code_path_to_all_nodes(Config, Module) ->
    Nodenames = get_node_configs(Config, nodename),
    [ok = add_code_path_to_node(Nodename, Module)
      || Nodename <- Nodenames],
    ok.

rpc(Config, Node, Module, Function, Args)
when is_atom(Node) andalso Node =/= undefined ->
    rpc(Config, Node, Module, Function, Args, infinity);
rpc(Config, I, Module, Function, Args)
when is_integer(I) andalso I >= 0 ->
    Node = get_node_config(Config, I, nodename),
    rpc(Config, Node, Module, Function, Args);
rpc(Config, Nodes, Module, Function, Args)
when is_list(Nodes) ->
    [rpc(Config, Node, Module, Function, Args) || Node <- Nodes].

rpc(_Config, Node, Module, Function, Args, Timeout)
when is_atom(Node) andalso Node =/= undefined ->
    %% We add some directories to the broker node search path.
    add_code_path_to_node(Node, Module),
    %% If there is an exception, rpc:call/{4,5} returns the exception as
    %% a "normal" return value. If there is an exit signal, we raise
    %% it again. In both cases, we have no idea of the module and line
    %% number which triggered the issue.
    Ret = case Timeout of
        infinity -> rpc:call(Node, Module, Function, Args);
        _        -> rpc:call(Node, Module, Function, Args, Timeout)
    end,
    case Ret of
        {badrpc, {'EXIT', Reason}} -> exit(Reason);
        {badrpc, Reason}           -> exit(Reason);
        Ret                        -> Ret
    end;
rpc(Config, I, Module, Function, Args, Timeout)
when is_integer(I) andalso I >= 0 ->
    Node = get_node_config(Config, I, nodename),
    rpc(Config, Node, Module, Function, Args, Timeout);
rpc(Config, Nodes, Module, Function, Args, Timeout)
when is_list(Nodes) ->
    [rpc(Config, Node, Module, Function, Args, Timeout) || Node <- Nodes].

rpc_all(Config, Module, Function, Args) ->
    Nodes = get_node_configs(Config, nodename),
    rpc(Config, Nodes, Module, Function, Args).

rpc_all(Config, Module, Function, Args, Timeout) ->
    Nodes = get_node_configs(Config, nodename),
    rpc(Config, Nodes, Module, Function, Args, Timeout).

%% Functions to start/restart/stop only the broker or the full Erlang
%% node.

start_node(Config, Node) ->
    NodeConfig = get_node_config(Config, Node),
    I = if
        is_atom(Node) -> nodename_to_index(Config, Node);
        true          -> Node
    end,
    case do_start_rabbitmq_node(Config, NodeConfig, I) of
        {skip, _} = Error -> {error, Error};
        _                 -> ok
    end.

start_broker(Config, Node) ->
    ok = rpc(Config, Node, rabbit, start, []).

restart_broker(Config, Node) ->
    ok = rpc(Config, Node, ?MODULE, do_restart_broker, []).

do_restart_broker() ->
    ok = rabbit:stop(),
    ok = rabbit:start().

stop_broker(Config, Node) ->
    ok = rpc(Config, Node, rabbit, stop, []).

restart_node(Config, Node) ->
    ok = stop_node(Config, Node),
    ok = start_node(Config, Node).

stop_node(Config, Node) ->
    NodeConfig = get_node_config(Config, Node),
    case stop_rabbitmq_node(Config, NodeConfig) of
        {skip, _} = Error -> Error;
        _                 -> ok
    end.

stop_node_after(Config, Node, Sleep) ->
    timer:sleep(Sleep),
    stop_node(Config, Node).

kill_node(Config, Node) ->
    Pid = rpc(Config, Node, os, getpid, []),
    %% FIXME maybe_flush_cover(Cfg),
    Cmd = case os:type() of
              {win32, _} ->
                  case os:find_executable("taskkill.exe") of
                      false ->
                          rabbit_misc:format(
                            "PowerShell -Command "
                            "\"Stop-Process -Id ~s -Force\"",
                            [Pid]);
                      _ ->
                          rabbit_misc:format("taskkill /PID ~s /F", [Pid])
                  end;
              _ ->
                  rabbit_misc:format("kill -9 ~s", [Pid])
          end,
    os:cmd(Cmd),
    await_os_pid_death(Pid).

kill_node_after(Config, Node, Sleep) ->
    timer:sleep(Sleep),
    kill_node(Config, Node).

cluster_members_online(Config, Node) ->
    rpc(Config, Node, rabbit_nodes, all_running, []).

await_os_pid_death(Pid) ->
    case rabbit_misc:is_os_process_alive(Pid) of
        true  -> timer:sleep(100),
                 await_os_pid_death(Pid);
        false -> ok
    end.

reset_node(Config, Node) ->
    Name = rabbit_ct_broker_helpers:get_node_config(Config, Node, nodename),
    rabbit_control_helper:command(reset, Name).

force_reset_node(Config, Node) ->
    Name = rabbit_ct_broker_helpers:get_node_config(Config, Node, nodename),
    rabbit_control_helper:command(force_reset, Name).

forget_cluster_node(Config, Node, NodeToForget) ->
    forget_cluster_node(Config, Node, NodeToForget, []).
forget_cluster_node(Config, Node, NodeToForget, Opts) ->
    Name = rabbit_ct_broker_helpers:get_node_config(Config, Node, nodename),
    NameToForget =
        rabbit_ct_broker_helpers:get_node_config(Config, NodeToForget, nodename),
    rabbit_control_helper:command(forget_cluster_node, Name, [NameToForget], Opts).

is_feature_flag_supported(Config, FeatureName) ->
    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    is_feature_flag_supported(Config, Nodes, FeatureName).

is_feature_flag_supported(Config, [Node1 | _] = Nodes, FeatureName) ->
    rabbit_ct_broker_helpers:rpc(
      Config, Node1,
      rabbit_feature_flags, is_supported_remotely,
      [Nodes, [FeatureName], 60000]).

enable_feature_flag(Config, FeatureName) ->
    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    enable_feature_flag(Config, Nodes, FeatureName).

enable_feature_flag(Config, [Node1 | _] = Nodes, FeatureName) ->
    case is_feature_flag_supported(Config, Nodes, FeatureName) of
        true ->
            rabbit_ct_broker_helpers:rpc(
              Config, Node1, rabbit_feature_flags, enable, [FeatureName]);
        false ->
            {skip,
             lists:flatten(
               io_lib:format("'~s' feature flag is unsupported",
                             [FeatureName]))}
    end.

mark_as_being_drained(Config, Node) ->
    rabbit_ct_broker_helpers:rpc(Config, Node, rabbit_maintenance, mark_as_being_drained, []).
unmark_as_being_drained(Config, Node) ->
    rabbit_ct_broker_helpers:rpc(Config, Node, rabbit_maintenance, unmark_as_being_drained, []).

drain_node(Config, Node) ->
    rabbit_ct_broker_helpers:rpc(Config, Node, rabbit_maintenance, drain, []).
revive_node(Config, Node) ->
    rabbit_ct_broker_helpers:rpc(Config, Node, rabbit_maintenance, revive, []).

is_being_drained_consistent_read(Config, Node) ->
    rabbit_ct_broker_helpers:rpc(Config, Node, rabbit_maintenance, is_being_drained_consistent_read, [Node]).
is_being_drained_local_read(Config, Node) ->
    rabbit_ct_broker_helpers:rpc(Config, Node, rabbit_maintenance, is_being_drained_local_read, [Node]).

is_being_drained_consistent_read(Config, TargetNode, NodeToCheck) ->
    rabbit_ct_broker_helpers:rpc(Config, TargetNode, rabbit_maintenance, is_being_drained_consistent_read, [NodeToCheck]).
is_being_drained_local_read(Config, TargetNode, NodeToCheck) ->
    rabbit_ct_broker_helpers:rpc(Config, TargetNode, rabbit_maintenance, is_being_drained_local_read, [NodeToCheck]).

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
          %% On at least Mac OS X, for a connection on localhost, the
          %% client side of the connection gives its IPv4 address
          %% (127.0.0.1), but the server side gives some kind of
          %% non-standard IPv6 address (::ffff:7f00:1, not even the
          %% standard ::1). So let's test for this alternate form too.
          AltConnInfo = case proplists:get_value(peer_host, ConnInfo) of
              {0, 0, 0, 0, 0, 16#ffff, 16#7f00, N} ->
                  lists:keyreplace(peer_host, 1, ConnInfo,
                      {peer_host, {127, 0, 0, N}});
              _ ->
                  ConnInfo
          end,
          lists:member(ConnInfo, ConnInfos) orelse
          lists:member(AltConnInfo, ConnInfos)
      end, rabbit_networking:connections()).

close_all_connections(Config, Node, Reason) ->
    rpc(Config, Node, rabbit_networking, close_all_connections, [Reason]),
    ok.

%% -------------------------------------------------------------------
%% Policy helpers.
%% -------------------------------------------------------------------

set_policy(Config, Node, Name, Pattern, ApplyTo, Definition) ->
    set_policy(Config, Node, Name, Pattern, ApplyTo, Definition, <<"acting-user">>).

set_policy(Config, Node, Name, Pattern, ApplyTo, Definition, Username) ->
    set_policy_in_vhost(Config, Node, <<"/">>, Name, Pattern, ApplyTo, Definition, Username).

set_policy_in_vhost(Config, Node, VirtualHost, Name, Pattern, ApplyTo, Definition) ->
    ok = rpc(Config, Node,
             rabbit_policy, set, [VirtualHost, Name, Pattern, Definition, 0, ApplyTo,
                                  <<"acting-user">>]).
set_policy_in_vhost(Config, Node, VirtualHost, Name, Pattern, ApplyTo, Definition, Username) ->
    ok = rpc(Config, Node,
             rabbit_policy, set, [VirtualHost, Name, Pattern, Definition, 0, ApplyTo,
                                  Username]).

clear_policy(Config, Node, Name) ->
    clear_policy(Config, Node, Name, <<"acting-user">>).

clear_policy(Config, Node, Name, Username) ->
    rpc(Config, Node,
        rabbit_policy, delete, [<<"/">>, Name, Username]).

set_operator_policy(Config, Node, Name, Pattern, ApplyTo, Definition) ->
    ok = rpc(Config, Node,
      rabbit_policy, set_op, [<<"/">>, Name, Pattern, Definition, 0, ApplyTo,
                              <<"acting-user">>]).

clear_operator_policy(Config, Node, Name) ->
    rpc(Config, Node,
        rabbit_policy, delete_op, [<<"/">>, Name, <<"acting-user">>]).

set_ha_policy(Config, Node, Pattern, Policy) ->
    set_ha_policy(Config, Node, Pattern, Policy, []).

set_ha_policy(Config, Node, Pattern, Policy, Extra) ->
    set_policy(Config, Node, Pattern, Pattern, <<"queues">>,
      ha_policy(Policy) ++ Extra).

ha_policy(<<"all">>)      -> [{<<"ha-mode">>,   <<"all">>}];
ha_policy({Mode, Params}) -> [{<<"ha-mode">>,   Mode},
                              {<<"ha-params">>, Params}].

set_ha_policy_all(Config) ->
    set_ha_policy(Config, 0, <<".*">>, <<"all">>),
    Config.

set_ha_policy_all(Config, Extra) ->
    set_ha_policy(Config, 0, <<".*">>, <<"all">>, Extra),
    Config.

set_ha_policy_two_pos(Config) ->
    Members = [
      rabbit_misc:atom_to_binary(N)
      || N <- get_node_configs(Config, nodename)],
    TwoNodes = [M || M <- lists:sublist(Members, 2)],
    set_ha_policy(Config, 0, <<"^ha.two.">>, {<<"nodes">>, TwoNodes},
                  [{<<"ha-promote-on-shutdown">>, <<"always">>}]),
    set_ha_policy(Config, 0, <<"^ha.auto.">>, {<<"nodes">>, TwoNodes},
                  [{<<"ha-sync-mode">>,           <<"automatic">>},
                   {<<"ha-promote-on-shutdown">>, <<"always">>}]),
    Config.

set_ha_policy_two_pos_batch_sync(Config) ->
    Members = [
      rabbit_misc:atom_to_binary(N)
      || N <- get_node_configs(Config, nodename)],
    TwoNodes = [M || M <- lists:sublist(Members, 2)],
    set_ha_policy(Config, 0, <<"^ha.two.">>, {<<"nodes">>, TwoNodes},
                  [{<<"ha-promote-on-shutdown">>, <<"always">>}]),
    set_ha_policy(Config, 0, <<"^ha.auto.">>, {<<"nodes">>, TwoNodes},
                  [{<<"ha-sync-mode">>,           <<"automatic">>},
                   {<<"ha-sync-batch-size">>,     200},
                   {<<"ha-promote-on-shutdown">>, <<"always">>}]),
    Config.

%% -------------------------------------------------------------------
%% Parameter helpers.
%% -------------------------------------------------------------------

set_parameter(Config, Node, Component, Name, Value) ->
    set_parameter(Config, Node, <<"/">>, Component, Name, Value, none).

set_parameter(Config, Node, VHost, Component, Name, Value) ->
    set_parameter(Config, Node, VHost, Component, Name, Value, none).

set_parameter(Config, Node, VHost, Component, Name, Value, Username) ->
    ok = rpc(Config, Node,
      rabbit_runtime_parameters, set, [VHost, Component, Name, Value, Username]).

clear_parameter(Config, Node, Component, Name) ->
    clear_parameter(Config, Node, <<"/">>, Component, Name).

clear_parameter(Config, Node, VHost, Component, Name) ->
    clear_parameter(Config, Node, VHost, Component, Name, <<"acting-user">>).

clear_parameter(Config, Node, VHost, Component, Name, Username) ->
    ok = rpc(Config, Node,
      rabbit_runtime_parameters, clear, [VHost, Component, Name, Username]).

set_global_parameter(Config, Name, Value) ->
    set_global_parameter(Config, 0, Name, Value).
set_global_parameter(Config, Node, Name, Value) ->
    ok = rpc(Config, Node,
      rabbit_runtime_parameters, set_global, [Name, Value, <<"acting-user">>]).

clear_global_parameter(Config, Name) ->
    clear_global_parameter(Config, 0, Name).
clear_global_parameter(Config, Node, Name) ->
    ok = rpc(Config, Node,
      rabbit_runtime_parameters, clear_global, [Name, <<"acting-user">>]).

%% -------------------------------------------------------------------
%% Parameter helpers.
%% -------------------------------------------------------------------

enable_plugin(Config, Node, Plugin) ->
    plugin_action(Config, Node, [enable, Plugin]).

disable_plugin(Config, Node, Plugin) ->
    plugin_action(Config, Node, [disable, Plugin]).

plugin_action(Config, Node, Args) ->
    Rabbitmqplugins = ?config(rabbitmq_plugins_cmd, Config),
    NodeConfig = get_node_config(Config, Node),
    Nodename = ?config(nodename, NodeConfig),
    Env = [
      {"RABBITMQ_SCRIPTS_DIR", filename:dirname(Rabbitmqplugins)},
      {"RABBITMQ_PID_FILE", ?config(pid_file, NodeConfig)},
      {"RABBITMQ_MNESIA_DIR", ?config(mnesia_dir, NodeConfig)},
      {"RABBITMQ_PLUGINS_DIR", ?config(plugins_dir, NodeConfig)},
      {"RABBITMQ_ENABLED_PLUGINS_FILE",
        ?config(enabled_plugins_file, NodeConfig)}
    ],
    Cmd = [Rabbitmqplugins, "-n", Nodename | Args],
    {ok, _} = rabbit_ct_helpers:exec(Cmd, [{env, Env}]),
    ok.

%% -------------------------------------------------------------------

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
