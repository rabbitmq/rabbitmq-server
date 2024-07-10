%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2024 Broadcom. The term “Broadcom” refers to Broadcom Inc.
%% and/or its subsidiaries. All rights reserved. All rights reserved.
%%

-module(system_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0,
         groups/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_group/2,
         end_per_group/2,
         init_per_testcase/2,
         end_per_testcase/2,

         start_one_member_at_a_time/1,
         start_members_concurrently/1]).

-define(CONSUL_GIT_REPO, "https://github.com/hashicorp/consul.git").
-define(CONSUL_GIT_REF, "v1.18.1").

all() ->
    [
     {group, clustering}
    ].

groups() ->
    [
     {clustering, [], [start_one_member_at_a_time,
                       start_members_concurrently]}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(
      Config,
      [fun clone_consul/1,
       fun compile_consul/1,
       fun config_consul/1,
       fun start_consul/1]).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config, [fun stop_consul/1]).

init_per_group(clustering, Config) ->
    rabbit_ct_helpers:set_config(
      Config,
      [{rmq_nodes_count, 3},
       {rmq_nodes_clustered, false}]);
init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, Config) ->
    Config.

init_per_testcase(Testcase, Config)
  when Testcase =:= start_one_member_at_a_time orelse
       Testcase =:= start_members_concurrently ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    TestNumber = rabbit_ct_helpers:testcase_number(Config, ?MODULE, Testcase),
    ClusterSize = ?config(rmq_nodes_count, Config),
    Config1 = rabbit_ct_helpers:set_config(
                Config,
                [{rmq_nodename_suffix, Testcase},
                 {tcp_ports_base, {skip_n_nodes,
                                   TestNumber * ClusterSize}}
                ]),
    Config2 = rabbit_ct_helpers:merge_app_env(
                Config1, {rabbit, [{log, [{file, [{level, debug}]}]}]}),
    Config3 = rabbit_ct_helpers:run_steps(
                Config2,
                rabbit_ct_broker_helpers:setup_steps() ++
                rabbit_ct_client_helpers:setup_steps()),
    case Config3 of
        _ when is_list(Config3) ->
            try
                _ = rabbit_ct_broker_helpers:rpc_all(
                      Config3, rabbit_peer_discovery_backend, api_version, []),
                Config3
            catch
                error:{exception, undef,
                       [{rabbit_peer_discovery_backend, api_version, _, _}
                        | _]} ->
                    Config4 = rabbit_ct_helpers:run_steps(
                                Config3,
                                rabbit_ct_client_helpers:teardown_steps() ++
                                rabbit_ct_broker_helpers:teardown_steps()),
                    rabbit_ct_helpers:testcase_finished(Config4, Testcase),
                    {skip,
                     "Some nodes use the old discover->register order; "
                     "the testcase would likely fail"}
            end;
        {skip, _} ->
            Config3
    end;
init_per_testcase(_Testcase, Config) ->
    Config.

end_per_testcase(Testcase, Config)
  when Testcase =:= start_one_member_at_a_time orelse
       Testcase =:= start_members_concurrently ->
    Config1 = rabbit_ct_helpers:run_steps(
                Config,
                rabbit_ct_client_helpers:teardown_steps() ++
                rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase);
end_per_testcase(_Testcase, Config) ->
    Config.

clone_consul(Config) ->
    DataDir = ?config(data_dir, Config),
    ConsulSrcdir = filename:join(DataDir, "consul"),
    Cmd = case filelib:is_dir(ConsulSrcdir) of
              true ->
                  ct:pal(
                    "Checking out Consul Git reference, ref = ~s",
                    [?CONSUL_GIT_REF]),
                  ["git", "-C", ConsulSrcdir,
                   "checkout", ?CONSUL_GIT_REF];
              false ->
                  ct:pal(
                    "Cloning Consul Git repository, ref = ~s",
                    [?CONSUL_GIT_REF]),
                  ["git", "clone",
                   "--branch", ?CONSUL_GIT_REF,
                   ?CONSUL_GIT_REPO, ConsulSrcdir]
          end,
    case rabbit_ct_helpers:exec(Cmd) of
        {ok, _} ->
            rabbit_ct_helpers:set_config(
              Config, {consul_srcdir, ConsulSrcdir});
        {error, _} ->
            {skip, "Failed to clone Consul"}
    end.

compile_consul(Config) ->
    ConsulSrcdir = ?config(consul_srcdir, Config),
    ct:pal("Compiling Consul in ~ts", [ConsulSrcdir]),
    Cmd = ["go", "install"],
    GOPATH = filename:join(ConsulSrcdir, "go"),
    GOFLAGS = "-modcacherw",
    Options = [{cd, ConsulSrcdir},
               {env, [{"BINDIR", false},
                      {"GOPATH", GOPATH},
                      {"GOFLAGS", GOFLAGS}]}],
    case rabbit_ct_helpers:exec(Cmd, Options) of
        {ok, _} ->
            ConsulExe = case os:type() of
                            {win32, _} -> "consul.exe";
                            _          -> "consul"
                        end,
            ConsulBin = filename:join([GOPATH, "bin", ConsulExe]),
            ?assert(filelib:is_regular(ConsulBin)),
            rabbit_ct_helpers:set_config(Config, {consul_bin, ConsulBin});
        {error, _} ->
            {skip, "Failed to compile Consul"}
    end.

config_consul(Config) ->
    DataDir = ?config(data_dir, Config),
    PrivDir = ?config(priv_dir, Config),
    ConsulConfDir = filename:join(PrivDir, "conf.consul"),
    ConsulDataDir = filename:join(PrivDir, "data.consul"),
    ConsulHost = "localhost",
    ConsulTcpPort = 8500,

    ConsulConfTpl = filename:join(DataDir, "consul.hcl"),
    {ok, ConsulConf0} = file:read_file(ConsulConfTpl),
    ConsulConf1 = io_lib:format(
                    "~ts~n"
                    "node_name = \"~ts\"~n"
                    "domain  = \"~ts\"~n"
                    "data_dir = \"~ts\"~n"
                    "ports {~n"
                    "  http        = ~b~n"
                    "  grpc        = -1~n"
                    "}~n",
                    [ConsulConf0, ConsulHost, ConsulHost, ConsulDataDir,
                     ConsulTcpPort]),
    ConsulConfFile = filename:join(ConsulConfDir, "consul.hcl"),
    ok = file:make_dir(ConsulConfDir),
    ok = file:write_file(ConsulConfFile, ConsulConf1),
    rabbit_ct_helpers:set_config(
      Config,
      [{consul_conf_dir, ConsulConfDir},
       {consul_host, ConsulHost},
       {consul_tcp_port, ConsulTcpPort}]).

start_consul(Config) ->
    ct:pal("Starting Consul daemon"),
    ConsulBin = ?config(consul_bin, Config),
    ConsulConfDir = ?config(consul_conf_dir, Config),
    Cmd = [ConsulBin, "agent", "-config-dir", ConsulConfDir],
    ConsulPid = spawn(fun() -> rabbit_ct_helpers:exec(Cmd) end),
    rabbit_ct_helpers:set_config(Config, {consul_pid, ConsulPid}).

stop_consul(Config) ->
    case rabbit_ct_helpers:get_config(Config, consul_pid) of
        ConsulPid when is_pid(ConsulPid) ->
            ct:pal(
              "Stopping Consul daemon by killing control process ~p",
              [ConsulPid]),
            erlang:exit(ConsulPid, kill),
            _ = case os:type() of
                    {win32, _} -> ok;
                    _          -> rabbit_ct_helpers:exec(["pkill", "consul"])
                end;
        undefined ->
            ok
    end,
    Config.

%%
%% Test cases
%%

start_one_member_at_a_time(Config) ->
    Config1 = configure_peer_discovery(Config),

    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config1, nodename),
    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 ok,
                 rabbit_ct_broker_helpers:start_node(Config1, Node))
      end, Nodes),

    assert_full_cluster(Config1).

start_members_concurrently(Config) ->
    Config1 = configure_peer_discovery(Config),

    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config1, nodename),
    Parent = self(),
    Pids = lists:map(
             fun(Node) ->
                     spawn_link(
                       fun() ->
                               receive
                                   go ->
                                       ?assertEqual(
                                          ok,
                                          rabbit_ct_broker_helpers:start_node(
                                            Config1, Node)),
                                       Parent ! started
                               end
                       end)
             end, Nodes),

    lists:foreach(fun(Pid) -> Pid ! go end, Pids),
    lists:foreach(fun(_Pid) -> receive started -> ok end end, Pids),

    assert_full_cluster(Config1).

configure_peer_discovery(Config) ->
    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    lists:foreach(
      fun(Node) ->
              Members = lists:sort(
                          rabbit_ct_broker_helpers:cluster_members_online(
                            Config, Node)),
              ?assertEqual([Node], Members)
      end, Nodes),

    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 ok,
                 rabbit_ct_broker_helpers:stop_broker(Config, Node)),
              ?assertEqual(
                 ok,
                 rabbit_ct_broker_helpers:reset_node(Config, Node)),
              ?assertEqual(
                 ok,
                 rabbit_ct_broker_helpers:stop_node(Config, Node))
      end, Nodes),

    ConsulHost = ?config(consul_host, Config),
    ConsulTcpPort = ?config(consul_tcp_port, Config),
    lists:foreach(
      fun(Node) ->
              Config1 = rabbit_ct_helpers:merge_app_env(
                          Config,
                          {rabbit,
                           [{cluster_formation,
                             [{peer_discovery_backend,
                               rabbit_peer_discovery_consul},
                              {peer_discovery_consul,
                               [{consul_svc_id, atom_to_list(Node)},
                                {consul_host, ConsulHost},
                                {consul_port, ConsulTcpPort},
                                {consul_scheme, "http"}]}]}]}),
              ?assertEqual(
                 ok,
                 rabbit_ct_broker_helpers:rewrite_node_config_file(
                   Config1, Node))
      end, Nodes),

    Config.

assert_full_cluster(Config) ->
    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    ExpectedMembers = lists:sort(Nodes),
    lists:foreach(
      fun(Node) ->
              Members = lists:sort(
                          rabbit_ct_broker_helpers:cluster_members_online(
                            Config, Node)),
              ?assertEqual(ExpectedMembers, Members)
      end, Nodes).
