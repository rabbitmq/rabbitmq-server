%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% The Initial Developer of the Original Code is AWeber Communications.
%% Copyright (c) 2015-2016 AWeber Communications
%% Copyright (c) 2007-2024 Broadcom. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved. All rights reserved.
%%

-module(system_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("rabbit_peer_discovery_etcd.hrl").

-define(ETCD_GIT_REPO, "https://github.com/etcd-io/etcd.git").
-define(ETCD_GIT_REF, "v3.5.13").

-export([all/0,
         groups/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_group/2,
         end_per_group/2,
         init_per_testcase/2,
         end_per_testcase/2,

         etcd_connection_sanity_check_test/1,
         init_opens_a_connection_test/1,
         registration_with_locking_test/1,
         start_one_member_at_a_time/1,
         start_members_concurrently/1]).

all() ->
    [
     {group, v3_client},
     {group, clustering}
    ].

groups() ->
    [
     {v3_client, [], [
                    etcd_connection_sanity_check_test,
                    init_opens_a_connection_test,
                    registration_with_locking_test
                ]},
     {clustering, [], [start_one_member_at_a_time,
                       start_members_concurrently]}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(
      Config,
      [fun clone_etcd/1,
       fun compile_etcd/1,
       fun start_etcd/1]).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config, [fun stop_etcd/1]).

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
    try
        _ = rabbit_ct_broker_helpers:rpc_all(
              Config3, rabbit_peer_discovery_backend, api_version, []),
        Config3
    catch
        error:{exception, undef,
               [{rabbit_peer_discovery_backend, api_version, _, _} | _]} ->
            Config4 = rabbit_ct_helpers:run_steps(
                        Config3,
                        rabbit_ct_client_helpers:teardown_steps() ++
                        rabbit_ct_broker_helpers:teardown_steps()),
            rabbit_ct_helpers:testcase_finished(Config4, Testcase),
            {skip,
             "Some nodes use the old discover->register order; "
             "the testcase would likely fail"}
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

clone_etcd(Config) ->
    DataDir = ?config(data_dir, Config),
    EtcdSrcdir = filename:join(DataDir, "etcd"),
    Cmd = case filelib:is_dir(EtcdSrcdir) of
              true ->
                  ct:pal(
                    "Checking out etcd Git reference, ref = ~s",
                    [?ETCD_GIT_REF]),
                  ["git", "-C", EtcdSrcdir,
                   "checkout", ?ETCD_GIT_REF];
              false ->
                  ct:pal(
                    "Cloning etcd Git repository, ref = ~s",
                    [?ETCD_GIT_REF]),
                  ["git", "clone",
                   "--branch", ?ETCD_GIT_REF,
                   ?ETCD_GIT_REPO, EtcdSrcdir]
          end,
    case rabbit_ct_helpers:exec(Cmd) of
        {ok, _} ->
            rabbit_ct_helpers:set_config(Config, {etcd_srcdir, EtcdSrcdir});
        {error, _} ->
            {skip, "Failed to clone etcd"}
    end.

compile_etcd(Config) ->
    EtcdSrcdir = ?config(etcd_srcdir, Config),
    ct:pal("Compiling etcd in ~ts", [EtcdSrcdir]),
    Script0 = case os:type() of
                  {win32, _} -> "build.bat";
                  _          -> "build.sh"
              end,
    Script1 = filename:join(EtcdSrcdir, Script0),
    Cmd = [Script1],
    GOPATH = filename:join(EtcdSrcdir, "go"),
    GOFLAGS = "-modcacherw",
    Options = [{cd, EtcdSrcdir},
               {env, [{"BINDIR", false},
                      {"GOPATH", GOPATH},
                      {"GOFLAGS", GOFLAGS}]}],
    case rabbit_ct_helpers:exec(Cmd, Options) of
        {ok, _} ->
            EtcdExe = case os:type() of
                          {win32, _} -> "etcd.exe";
                          _          -> "etcd"
                      end,
            EtcdBin = filename:join([EtcdSrcdir, "bin", EtcdExe]),
            ?assert(filelib:is_regular(EtcdBin)),
            rabbit_ct_helpers:set_config(Config, {etcd_bin, EtcdBin});
        {error, _} ->
            {skip, "Failed to compile etcd"}
    end.

start_etcd(Config) ->
    ct:pal("Starting etcd daemon"),
    EtcdBin = ?config(etcd_bin, Config),
    PrivDir = ?config(priv_dir, Config),
    EtcdDataDir = filename:join(PrivDir, "data.etcd"),
    EtcdName = ?MODULE_STRING,
    EtcdHost = "localhost",
    EtcdClientPort = 2379,
    EtcdClientUrl = rabbit_misc:format(
                      "http://~s:~b", [EtcdHost, EtcdClientPort]),
    EtcdAdvPort = 2380,
    EtcdAdvUrl = rabbit_misc:format(
                   "http://~s:~b", [EtcdHost, EtcdAdvPort]),
    Cmd = [EtcdBin,
           "--data-dir", EtcdDataDir,
           "--name", EtcdName,
           "--initial-advertise-peer-urls", EtcdAdvUrl,
           "--listen-peer-urls", EtcdAdvUrl,
           "--advertise-client-urls", EtcdClientUrl,
           "--listen-client-urls", EtcdClientUrl,
           "--initial-cluster", EtcdName ++ "=" ++ EtcdAdvUrl,
           "--initial-cluster-state", "new",
           "--initial-cluster-token", "test-token",
           "--log-level", "debug", "--log-outputs", "stdout"],
    EtcdPid = spawn(fun() -> rabbit_ct_helpers:exec(Cmd) end),

    EtcdEndpoint = rabbit_misc:format("~s:~b", [EtcdHost, EtcdClientPort]),
    rabbit_ct_helpers:set_config(
      Config,
      [{etcd_pid, EtcdPid},
       {etcd_endpoints, [EtcdEndpoint]}]).

stop_etcd(Config) ->
    case rabbit_ct_helpers:get_config(Config, etcd_pid) of
        EtcdPid when is_pid(EtcdPid) ->
            ct:pal(
              "Stopping etcd daemon by killing control process ~p",
              [EtcdPid]),
            erlang:exit(EtcdPid, kill);
        undefined ->
            ok
    end,
    Config.

%%
%% Test cases
%%

etcd_connection_sanity_check_test(Config) ->
    application:ensure_all_started(eetcd),
    Endpoints = ?config(etcd_endpoints, Config),
    ?assertMatch({ok, _Pid}, eetcd:open(test, Endpoints)),

    Condition1 = fun() ->
                    1 =:= length(eetcd_conn_sup:info())
                end,
    try
        rabbit_ct_helpers:await_condition(Condition1, 60000)
    after
        eetcd:close(test)
    end,
    Condition2 = fun() ->
                    0 =:= length(eetcd_conn_sup:info())
                end,
    rabbit_ct_helpers:await_condition(Condition2, 60000).

init_opens_a_connection_test(Config) ->
    Endpoints = ?config(etcd_endpoints, Config),
    {ok, Pid} = start_client(Endpoints),
    Condition = fun() ->
                    1 =:= length(eetcd_conn_sup:info())
                end,
    try
        rabbit_ct_helpers:await_condition(Condition, 90000)
    after
        gen_statem:stop(Pid)
    end,
    ?assertEqual(0, length(eetcd_conn_sup:info())).


registration_with_locking_test(Config) ->
    Endpoints = ?config(etcd_endpoints, Config),
    {ok, Pid} = start_client(Endpoints),
    Condition1 = fun() ->
                    1 =:= length(eetcd_conn_sup:info())
                 end,
    rabbit_ct_helpers:await_condition(Condition1, 90000),

    {ok, LockOwnerKey} = rabbitmq_peer_discovery_etcd_v3_client:lock(Pid, node()),
    rabbitmq_peer_discovery_etcd_v3_client:register(Pid),
    ?assertEqual(ok, rabbitmq_peer_discovery_etcd_v3_client:unlock(Pid, LockOwnerKey)),

    Condition2 = fun() ->
                    case rabbitmq_peer_discovery_etcd_v3_client:list_nodes(Pid) of
                        [{_, N}] when N =:= node() ->
                            true;
                        _ ->
                            false
                    end
                 end,
    try
        rabbit_ct_helpers:await_condition(Condition2, 45000)
    after
        gen_statem:stop(Pid)
    end.

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

    Endpoints = ?config(etcd_endpoints, Config),
    Config1 = rabbit_ct_helpers:merge_app_env(
                Config,
                {rabbit,
                 [{cluster_formation,
                   [{peer_discovery_backend, rabbit_peer_discovery_etcd},
                    {peer_discovery_etcd,
                     [{endpoints, Endpoints},
                      {etcd_prefix, "rabbitmq"},
                      {cluster_name, atom_to_list(?FUNCTION_NAME)}]}]}]}),
    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 ok,
                 rabbit_ct_broker_helpers:rewrite_node_config_file(
                   Config1, Node))
      end, Nodes),

    Config1.

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

%%
%% Helpers
%%

start_client(Endpoints) ->
    case rabbitmq_peer_discovery_etcd_v3_client:start(#{endpoints => Endpoints}) of
        {ok, Pid} ->
            {ok, Pid};
        {error, {already_started, Pid}} ->
            {ok, Pid}
    end.
