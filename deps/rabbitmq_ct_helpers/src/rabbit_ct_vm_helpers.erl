%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2018-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_ct_vm_helpers).

-include_lib("common_test/include/ct.hrl").

-export([setup_steps/0,
         teardown_steps/0,

         get_ct_peers/1,
         get_ct_peer/2,
         get_ct_peer_configs/2,
         get_ct_peer_config/2, get_ct_peer_config/3,
         get_current_vm_config/2,
         rpc/4, rpc/5,
         rpc_all/3, rpc_all/4,

         ensure_terraform_cmd/1,
         determine_erlang_version/1,
         determine_erlang_git_ref/1,
         determine_elixir_version/1,
         compute_code_path/1,
         find_terraform_ssh_key/1,
         set_terraform_files_suffix/1,
         set_terraform_config_dirs/1,
         set_terraform_state/1,
         set_terraform_aws_ec2_region/1,
         init_terraform/1,
         compute_vpc_cidr_block/1,
         find_erlang_mk/1,
         find_rabbitmq_components/1,
         list_dirs_to_upload/1,
         list_dirs_to_download/1,
         maybe_prepare_dirs_to_upload_archive/1,
         spawn_terraform_vms/1, destroy_terraform_vms/1,
         query_terraform_uuid/1,
         query_ct_peer_nodenames_and_ipaddrs/1,
         set_inet_hosts/1,
         write_inetrc/1,
         wait_for_ct_peers/1,
         set_ct_peers_code_path/1,
         start_ct_logs_proxies/1,
         configure_ct_peers_environment/1,
         download_dirs/1,
         stop_ct_peers/1,

         aws_direct_vms_module/1,
         aws_autoscaling_group_module/1,
         vms_query_module/1,

         do_setup_proxy/2, proxy_loop/1,
         prepare_dirs_to_download_archives/1
        ]).

-define(UPLOAD_DIRS_ARCHIVE_PREFIX, "dirs-archive-").
-define(ERLANG_REMOTE_NODENAME, "control").

setup_steps() ->
    [
     fun ensure_terraform_cmd/1,
     fun determine_erlang_version/1,
     fun determine_erlang_git_ref/1,
     fun determine_elixir_version/1,
     fun compute_code_path/1,
     fun find_terraform_ssh_key/1,
     fun set_terraform_files_suffix/1,
     fun set_terraform_config_dirs/1,
     fun set_terraform_state/1,
     fun set_terraform_aws_ec2_region/1,
     fun init_terraform/1,
     fun compute_vpc_cidr_block/1,
     fun find_erlang_mk/1,
     fun find_rabbitmq_components/1,
     fun list_dirs_to_upload/1,
     fun list_dirs_to_download/1,
     fun maybe_prepare_dirs_to_upload_archive/1,
     fun spawn_terraform_vms/1,
     fun query_terraform_uuid/1,
     fun query_ct_peer_nodenames_and_ipaddrs/1,
     fun set_inet_hosts/1,
     fun write_inetrc/1,
     fun wait_for_ct_peers/1,
     fun set_ct_peers_code_path/1,
     fun start_ct_logs_proxies/1,
     fun configure_ct_peers_environment/1
    ].

teardown_steps() ->
    [
     fun download_dirs/1,
     fun stop_ct_peers/1,
     fun destroy_terraform_vms/1
    ].

ensure_terraform_cmd(Config) ->
    Terraform = case rabbit_ct_helpers:get_config(Config, terraform_cmd) of
        undefined ->
            case os:getenv("TERRAFORM") of
                false -> "terraform";
                T     -> T
            end;
        T ->
            T
    end,
    Cmd = [Terraform, "--version"],
    case rabbit_ct_helpers:exec(Cmd, [{match_stdout, "Terraform"}]) of
        {ok, _} ->
            rabbit_ct_helpers:set_config(Config, {terraform_cmd, Terraform});
        _ ->
            {skip, "terraform(1) required, " ++
             "please set TERRAFORM or 'terraform_cmd' in ct config"}
    end.

determine_erlang_version(Config) ->
    Version = case rabbit_ct_helpers:get_config(Config, erlang_version) of
                  undefined ->
                      case os:getenv("ERLANG_VERSION") of
                          false -> rabbit_misc:otp_release();
                          V     -> V
                      end;
                  V ->
                      V
              end,
    Regex = "([0-9]+\.[0-9]+|R[0-9]+(?:[AB])[0-9]+).*",
    ErlangVersion = re:replace(Version, Regex, "\\1"),
    ct:pal(?LOW_IMPORTANCE, "Erlang version: ~s", [ErlangVersion]),
    rabbit_ct_helpers:set_config(
      Config, {erlang_version, ErlangVersion}).

determine_erlang_git_ref(Config) ->
    GitRef = case rabbit_ct_helpers:get_config(Config, erlang_git_ref) of
                  undefined ->
                      case os:getenv("ERLANG_GIT_REF") of
                          false ->
                              Version = erlang:system_info(system_version),
                              ReOpts = [{capture, all_but_first, list}],
                              Match = re:run(Version,
                                             "source-([0-9a-fA-F]+)",
                                             ReOpts),
                              case Match of
                                  {match, [V]} -> V;
                                  _            -> ""
                              end;
                          V ->
                              V
                      end;
                  V ->
                      V
              end,
    ct:pal(?LOW_IMPORTANCE, "Erlang Git reference: ~s", [GitRef]),
    rabbit_ct_helpers:set_config(
      Config, {erlang_git_ref, GitRef}).

determine_elixir_version(Config) ->
    Version = case rabbit_ct_helpers:get_config(Config, elixir_version) of
                  undefined ->
                      case os:getenv("ELIXIR_VERSION") of
                          false ->
                              Cmd = ["elixir", "-e", "IO.puts System.version"],
                              case rabbit_ct_helpers:exec(Cmd) of
                                  {ok, Output} ->
                                      string:strip(Output, right, $\n);
                                  _ ->
                                      ""
                              end;
                          V ->
                              V
                      end;
                  V ->
                      V
              end,
    ct:pal(?LOW_IMPORTANCE, "Elixir version: ~s", [Version]),
    rabbit_ct_helpers:set_config(Config, {elixir_version, Version}).

compute_code_path(Config) ->
    EntireCodePath = code:get_path(),
    CodePath = filter_out_erlang_code_path(EntireCodePath),
    ct:pal(?LOW_IMPORTANCE, "Code path: ~p", [CodePath]),
    rabbit_ct_helpers:set_config(Config, {erlang_code_path, CodePath}).

filter_out_erlang_code_path(CodePath) ->
    ErlangRoot = code:root_dir(),
    ErlangRootLen = string:len(ErlangRoot),
    lists:filter(
      fun(Dir) ->
              Dir =/= "." andalso
              string:substr(Dir, 1, ErlangRootLen) =/= ErlangRoot
      end, CodePath).

find_terraform_ssh_key(Config) ->
    Config1 =
    case rabbit_ct_helpers:get_config(Config, terraform_ssh_key) of
        undefined ->
            case os:getenv("SSH_KEY") of
                false ->
                    HomeDir = os:getenv("HOME"),
                    Glob = filename:join([HomeDir, ".ssh", "*terraform*"]),
                    Filenames = lists:sort(filelib:wildcard(Glob)),
                    PrivKeys = lists:filter(
                                 fun(Filename) ->
                                         filename:extension(Filename) =:= ""
                                         andalso
                                         test_ssh_key(Filename)
                                 end, Filenames),
                    case PrivKeys of
                        [PrivKey | _] ->
                            rabbit_ct_helpers:set_config(
                              Config, {terraform_ssh_key, PrivKey});
                        _ ->
                            Config
                    end;
                PrivKey ->
                    case test_ssh_key(PrivKey) of
                        true ->
                            rabbit_ct_helpers:set_config(
                              Config, {terraform_ssh_key, PrivKey});
                        false ->
                            Config
                    end
            end;
        PrivKey ->
            case test_ssh_key(PrivKey) of
                true ->
                    rabbit_ct_helpers:delete_config(
                      Config, terraform_ssh_key);
                false ->
                    Config
            end
    end,
    case rabbit_ct_helpers:get_config(Config1, terraform_ssh_key) of
        undefined ->
            {skip, "Private SSH key required, " ++
             "please set SSH_KEY or terraform_ssh_key in ct config"};
        _ ->
            Config1
    end.

test_ssh_key(PrivKey) ->
    filelib:is_regular(PrivKey)
    andalso
    filelib:is_regular(PrivKey ++ ".pub").

set_terraform_files_suffix(Config) ->
    case rabbit_ct_helpers:get_config(Config, terraform_files_suffix) of
        undefined ->
            Suffix = rabbit_ct_helpers:random_term_checksum(),
            rabbit_ct_helpers:set_config(
              Config, {terraform_files_suffix, Suffix});
        _ ->
            Config
    end.

aws_direct_vms_module(Config) ->
    SrcDir = ?config(rabbitmq_ct_helpers_srcdir, Config),
    filename:join([SrcDir, "tools", "terraform", "direct-vms"]).

aws_autoscaling_group_module(Config) ->
    SrcDir = ?config(rabbitmq_ct_helpers_srcdir, Config),
    filename:join([SrcDir, "tools", "terraform", "autoscaling-group"]).

vms_query_module(Config) ->
    SrcDir = ?config(rabbitmq_ct_helpers_srcdir, Config),
    filename:join([SrcDir, "tools", "terraform", "vms-query"]).

set_terraform_config_dirs(Config) ->
    SpawnTfConfigDir = aws_direct_vms_module(Config),
    PollTfConfigDir = vms_query_module(Config),
    Config1 = rabbit_ct_helpers:set_config(
      Config, {terraform_poll_config_dir, PollTfConfigDir}),
    SpawnTfConfigDir0 = rabbit_ct_helpers:get_config(
                          Config, terraform_config_dir),
    case SpawnTfConfigDir0 of
        undefined ->
            rabbit_ct_helpers:set_config(
              Config1, {terraform_config_dir, SpawnTfConfigDir});
        _ ->
            Config1
    end.

set_terraform_state(Config) ->
    PrivDir = ?config(priv_dir, Config),
    Suffix = ?config(terraform_files_suffix, Config),
    SpawnDataDir = rabbit_misc:format(".terraform-~s", [Suffix]),
    SpawnStateFilename = rabbit_misc:format("terraform-~s.tfstate",
                                            [Suffix]),
    PollDataDir = rabbit_misc:format(".terraform-query-~s", [Suffix]),
    PollStateFilename = rabbit_misc:format("terraform-query-~s.tfstate",
                                           [Suffix]),
    SpawnTfState = filename:join(PrivDir, SpawnStateFilename),
    PollTfState = filename:join(PrivDir, PollStateFilename),
    ct:pal(?LOW_IMPORTANCE, "Terraform state: ~s", [SpawnTfState]),
    rabbit_ct_helpers:set_config(
      Config, [{terraform_state, SpawnTfState},
               {terraform_data_dir, SpawnDataDir},
               {terraform_poll_state, PollTfState},
               {terraform_poll_data_dir, PollDataDir}]).

set_terraform_aws_ec2_region(Config) ->
    case rabbit_ct_helpers:get_config(Config, terraform_aws_ec2_region) of
        undefined ->
            EC2Region = "eu-west-1",
            rabbit_ct_helpers:set_config(
              Config, {terraform_aws_ec2_region, EC2Region});
        _ ->
            Config
    end.

init_terraform(Config) ->
    SpawnDataDir = ?config(terraform_data_dir, Config),
    SpawnTfConfigDir = ?config(terraform_config_dir, Config),
    PollDataDir = ?config(terraform_poll_data_dir, Config),
    PollTfConfigDir = ?config(terraform_poll_config_dir, Config),
    init_terraform_dirs(Config, [{SpawnTfConfigDir, SpawnDataDir},
                                 {PollTfConfigDir, PollDataDir}]).

init_terraform_dirs(Config, [{ConfigDir, DataDir} | Rest]) ->
    Terraform = ?config(terraform_cmd, Config),
    Env = [
           {"TF_DATA_DIR", DataDir}
          ],
    Cmd = [
           Terraform,
           {"-chdir=~s", [ConfigDir]},
           "init"
          ],
    case rabbit_ct_helpers:exec(Cmd, [{env, Env}]) of
        {ok, _} -> init_terraform_dirs(Config, Rest);
        {error, Code, Reason} ->
            {skip, rabbit_misc:format("Failed to init Terraform. Exit code: ~p, message: ~s", [Code, Reason])}
    end;
init_terraform_dirs(Config, []) ->
    Config.

compute_vpc_cidr_block(Config) ->
    LockId = {compute_vpc_cidr_block, self()},
    LockNodes = [node()],
    global:set_lock(LockId, LockNodes),
    Seq = case os:getenv("NEXT_VPC_CIDR_BLOCK_SEQ") of
              false -> 1;
              V     -> erlang:list_to_integer(V)
          end,
    os:putenv("NEXT_VPC_CIDR_BLOCK_SEQ", integer_to_list(Seq + 1)),
    global:del_lock(LockId, LockNodes),
    CidrBlock = rabbit_misc:format("10.~b.0.0/16", [Seq]),
    rabbit_ct_helpers:set_config(Config, {terraform_vpc_cidr_block, CidrBlock}).

find_in_srcdir_or_grandparent(Config, Name, ConfigKey) when is_atom(ConfigKey) ->
    SrcDir = ?config(current_srcdir, Config),
    SrcDirChild = filename:join([SrcDir, Name]),
    SrcDirAncestor = filename:join([SrcDir, "..", "..", Name]),
    case {filelib:is_regular(SrcDirChild), filelib:is_regular(SrcDirAncestor)} of
        {true, _} -> rabbit_ct_helpers:set_config(Config, {ConfigKey, SrcDirChild});
        {false, true} -> rabbit_ct_helpers:set_config(Config, {ConfigKey, SrcDirAncestor});
        _ -> {skip, "Failed to find " ++ Name}
    end.

find_erlang_mk(Config) ->
    find_in_srcdir_or_grandparent(Config, "erlang.mk", erlang_mk_path).

find_rabbitmq_components(Config) ->
    find_in_srcdir_or_grandparent(Config, "rabbitmq-components.mk", rabbitmq_components_path).

list_dirs_to_upload(Config) ->
    SrcDir = ?config(current_srcdir, Config),
    LockId = {make_list_test_deps, self()},
    LockNodes = [node()],
    % `make list-test-deps` writes to a central file, a file we read
    % later. Therefore we protect that write+read with a lock.
    global:set_lock(LockId, LockNodes),
    Ret = rabbit_ct_helpers:make(Config, SrcDir, ["list-test-deps"]),
    case Ret of
        {ok, _} ->
            ListFile = filename:join([SrcDir,
                                      ".erlang.mk",
                                      "recursive-test-deps-list.log"]),
            {ok, Content} = file:read_file(ListFile),
            global:del_lock(LockId, LockNodes),
            DepsDirs0 = string:tokens(binary_to_list(Content), "\n"),
            DepsDirs = filter_out_subdirs(SrcDir, DepsDirs0),
            ErlangMkPath = ?config(erlang_mk_path, Config),
            RabbitmqComponentsPath = ?config(rabbitmq_components_path, Config),
            AllDirs = lists:sort(
                        [SrcDir, ErlangMkPath, RabbitmqComponentsPath] ++ DepsDirs
                       ),
            ct:pal(?LOW_IMPORTANCE, "Directories to upload: ~p", [AllDirs]),
            rabbit_ct_helpers:set_config(Config, {dirs_to_upload, AllDirs});
        _ ->
            global:del_lock(LockId, LockNodes),
            {skip, "Failed to get the list of test dependencies"}
    end.

list_dirs_to_download(Config) ->
    PrivDir = ?config(priv_dir, Config),
    PrivDirParent = filename:dirname(string:strip(PrivDir, right, $/)),
    Dirs1 = case rabbit_ct_helpers:get_config(Config, dirs_to_download) of
                undefined -> [PrivDirParent];
                Dirs0     -> [PrivDirParent
                              | filter_out_subdirs(PrivDirParent, Dirs0)]
            end,
    Dirs = lists:sort(Dirs1),
    ct:pal(?LOW_IMPORTANCE, "Directories to download: ~p", [Dirs]),
    rabbit_ct_helpers:set_config(Config, {dirs_to_download, Dirs}).

filter_out_subdirs(RootDir, Dirs) ->
    RootDirLen = length(RootDir),
    lists:filter(
      fun(Dir) ->
              Dir =/= RootDir andalso
              string:sub_string(Dir, 1, RootDirLen + 1)
              =/=
              RootDir ++ "/"
      end, Dirs).

maybe_prepare_dirs_to_upload_archive(Config) ->
    PrivDir = ?config(priv_dir, Config),
    Dirs = lists:sort(?config(dirs_to_upload, Config)),
    Checksum = rabbit_ct_helpers:term_checksum(Dirs),
    Archive = filename:join(
                PrivDir,
                rabbit_misc:format(
                  ?UPLOAD_DIRS_ARCHIVE_PREFIX "~s.tar.xz", [Checksum])),
    Config1 = rabbit_ct_helpers:set_config(
                Config, {upload_dirs_archive, Archive}),
    LockId = {{dirs_to_upload, Archive}, self()},
    LockNodes = [node()],
    % The upload dirs archive is unique per set of directories.
    % Therefore it can be shared by multiple setups. We want to create
    % the archive once and certainly don't want to create it multiple
    % times in parallel. Thus the lock.
    global:set_lock(LockId, LockNodes),
    case filelib:is_regular(Archive) of
        true ->
            global:del_lock(LockId, LockNodes),
            Config1;
        false ->
            Config2 = prepare_dirs_to_upload_archive(Config1, Archive, Dirs),
            global:del_lock(LockId, LockNodes),
            Config2
    end.

prepare_dirs_to_upload_archive(Config, Archive, Dirs) ->
    DirsList = string:join(
                 [rabbit_misc:format("~p", [Dir])
                  || Dir <- Dirs,
                     filelib:is_dir(Dir) orelse filelib:is_regular(Dir)],
                 " "),
    Cmd = rabbit_misc:format(
            "tar cf - -P"
            " --exclude '.terraform*'"
            " --exclude '" ?UPLOAD_DIRS_ARCHIVE_PREFIX "*'"
            " --exclude '" ?ERLANG_REMOTE_NODENAME "@*'"
            " ~s"
            " | xz --threads=0 > ~p",
            [DirsList, Archive]),
    ct:pal(
      ?LOW_IMPORTANCE,
      "Creating upload dirs archive `~s`:~n  ~s",
      [filename:basename(Archive), Cmd]),
    case os:cmd(Cmd) of
        "" ->
            Config;
        Output ->
            ct:pal(
              ?LOW_IMPORTANCE,
              "Failed to create upload dirs archive:~n~s",
              [Output]),
            {skip, "Failed to create upload dirs archive"}
    end.

spawn_terraform_vms(Config) ->
    TfConfigDir = ?config(terraform_config_dir, Config),
    TfDataDir = ?config(terraform_data_dir, Config),
    TfState = ?config(terraform_state, Config),
    TfVarFlags = terraform_var_flags(Config),
    Terraform = ?config(terraform_cmd, Config),
    Env = [
           {"TF_DATA_DIR", TfDataDir}
          ],
    Cmd = [
           Terraform,
           {"-chdir=~s", [TfConfigDir]},
           "apply",
           "-auto-approve=true",
           {"-state=~s", [TfState]}
          ] ++ TfVarFlags,
    case rabbit_ct_helpers:exec(Cmd, [{env, Env}]) of
        {ok, _} ->
            Config1 = rabbit_ct_helpers:set_config(
                        Config, {terraform_query_mode, direct}),
            % FIXME: This `register_teardown_steps()` function is just
            % wrong currently: when run_steps() is used at the end of
            % e.g. a testcase to run testcase-specific teardown steps,
            % the registered steps are not executed.
            rabbit_ct_helpers:register_teardown_steps(
              Config1, teardown_steps());
        _ ->
            destroy_terraform_vms(Config),
            {skip, "Terraform failed to spawn VM"}
    end.

destroy_terraform_vms(Config) ->
    TfConfigDir = ?config(terraform_config_dir, Config),
    TfDataDir = ?config(terraform_data_dir, Config),
    TfState = ?config(terraform_state, Config),
    TfVarFlags = terraform_var_flags(Config),
    Terraform = ?config(terraform_cmd, Config),
    Env = [
           {"TF_DATA_DIR", TfDataDir}
          ],
    Cmd = [
           Terraform,
           {"-chdir=~s", [TfConfigDir]},
           "destroy",
           "-force",
           {"-state=~s", [TfState]}
          ] ++ TfVarFlags,
    rabbit_ct_helpers:exec(Cmd, [{env, Env}]),
    Config.

terraform_var_flags(Config) ->
    ErlangVersion = ?config(erlang_version, Config),
    GitRef = ?config(erlang_git_ref, Config),
    ElixirVersion = ?config(elixir_version, Config),
    SshKey = ?config(terraform_ssh_key, Config),
    Suffix = ?config(terraform_files_suffix, Config),
    EC2Region = ?config(terraform_aws_ec2_region, Config),
    InstanceCount = instance_count(Config),
    InstanceName0 = rabbit_ct_helpers:get_config(Config, terraform_instance_name),
    InstanceName = case InstanceName0 of
                       undefined -> Suffix;
                       _         -> InstanceName0
                   end,
    CidrBlock = ?config(terraform_vpc_cidr_block, Config),
    ErlangApp = rabbit_ct_helpers:get_config(Config, tested_erlang_app),
    InstanceNamePrefix = case ErlangApp of
                             undefined ->
                                 "RabbitMQ testing: ";
                             _ ->
                                 rabbit_misc:format("~s: ", [ErlangApp])
                         end,
    TestedApp = ?config(tested_erlang_app, Config),
    _ = application:load(TestedApp),
    InstanceNameSuffix = case application:get_key(TestedApp, vsn) of
                             {ok, AppVer} -> " - " ++ AppVer;
                             undefined    -> ""
                         end,
    ct:pal(?LOW_IMPORTANCE, "Number of VMs requested: ~b", [InstanceCount]),
    Archive = ?config(upload_dirs_archive, Config),
    [
     {"-var=erlang_version=~s", [ErlangVersion]},
     {"-var=erlang_git_ref=~s", [GitRef]},
     {"-var=elixir_version=~s", [ElixirVersion]},
     {"-var=erlang_cookie=~s", [erlang:get_cookie()]},
     {"-var=erlang_nodename=~s", [?ERLANG_REMOTE_NODENAME]},
     {"-var=ssh_key=~s", [SshKey]},
     {"-var=instance_count=~b", [InstanceCount]},
     {"-var=instance_name=~s", [InstanceName]},
     {"-var=upload_dirs_archive=~s", [Archive]},
     {"-var=vpc_cidr_block=~s", [CidrBlock]},
     {"-var=files_suffix=~s", [Suffix]},
     {"-var=aws_ec2_region=~s", [EC2Region]},
     {"-var=instance_name_prefix=~s", [InstanceNamePrefix]},
     {"-var=instance_name_suffix=~s", [InstanceNameSuffix]}
    ].

instance_count(Config) ->
    InstanceCount0 = rabbit_ct_helpers:get_config(
                       Config, terraform_instance_count),
    case InstanceCount0 of
        undefined                           -> 1;
        N when is_integer(N) andalso N >= 1 -> N
    end.

query_terraform_uuid(Config) ->
    Terraform = ?config(terraform_cmd, Config),
    TfState = ?config(terraform_state, Config),
    Cmd = [
           Terraform,
           "output",
           "-no-color",
           {"-state=~s", [TfState]},
           "uuid"
          ],
    case rabbit_ct_helpers:exec(Cmd) of
        {ok, Output} ->
            Uuid = string:strip(string:strip(Output, right, $\n)),
            rabbit_ct_helpers:set_config(Config, {terraform_uuid, Uuid});
        _ ->
            {skip, "Terraform failed to query unique ID"}
    end.

query_ct_peer_nodenames_and_ipaddrs(Config) ->
    case query_terraform_map(Config, "ct_peer_nodenames") of
        {ok, NodenamesMap} ->
            case query_terraform_map(Config, "ct_peer_ipaddrs") of
                {ok, IPAddrsMap} ->
                    initialize_ct_peers(Config, NodenamesMap, IPAddrsMap);
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

query_terraform_map(Config, Var) ->
    QueryMode = ?config(terraform_query_mode, Config),
    case QueryMode of
        direct ->
            query_terraform_map_directly(Config, Var);
        polling ->
            poll_terraform_map(Config, Var)
    end.

query_terraform_map_directly(Config, Var) ->
    TfState = ?config(terraform_state, Config),
    case do_query_terraform_map(Config, TfState, Var) of
        {skip, _} ->
            Config1 = rabbit_ct_helpers:set_config(
                        Config, {terraform_query_mode, polling}),
            query_terraform_map(Config1, Var);
        Ret ->
            Ret
    end.

poll_terraform_map(Config, Var) ->
    case poll_vms(Config) of
        {skip, _} = Error ->
            Error;
        Config1 ->
            query_terraform_map_from_poll_state(Config1, Var)
    end.

poll_vms(Config) ->
    case rabbit_ct_helpers:get_config(Config, terraform_poll_done) of
        undefined ->
            Timeout = 5 * 60 * 1000,
            {ok, TRef} = timer:send_after(Timeout, terraform_poll_timeout),
            do_poll_vms(Config, TRef);
        true ->
            Config
    end.

do_poll_vms(Config, TRef) ->
    TfConfigDir = ?config(terraform_poll_config_dir, Config),
    TfDataDir = ?config(terraform_poll_data_dir, Config),
    TfState = ?config(terraform_poll_state, Config),
    Uuid = ?config(terraform_uuid, Config),
    Terraform = ?config(terraform_cmd, Config),
    Env = [
           {"TF_DATA_DIR", TfDataDir}
          ],
    Cmd = [
           Terraform,
           {"-chdir=~s", [TfConfigDir]},
           "apply",
           "-auto-approve=true",
           {"-state=~s", [TfState]},
           {"-var=uuid=~s", [Uuid]},
           {"-var=erlang_nodename=~s", [?ERLANG_REMOTE_NODENAME]}
          ],
    case rabbit_ct_helpers:exec(Cmd, [{env, Env}]) of
        {ok, _} -> ensure_instance_count(Config, TRef);
        _       -> {skip, "Terraform failed to query VMs"}
    end.

ensure_instance_count(Config, TRef) ->
    Terraform = ?config(terraform_cmd, Config),
    TfState = ?config(terraform_poll_state, Config),
    Cmd = [
           Terraform,
           "output",
           "-no-color",
           {"-state=~s", [TfState]},
           "instance_count"
          ],
    case rabbit_ct_helpers:exec(Cmd) of
        {ok, Output} ->
            CountStr = string:strip(string:strip(Output, right, $\n)),
            Current = erlang:list_to_integer(CountStr),
            Requested = instance_count(Config),
            ct:pal(?LOW_IMPORTANCE,
                   "Number of VMs ready: ~b (at least ~b requested)",
                   [Current, Requested]),
            if
                Current < Requested ->
                    receive
                        terraform_poll_timeout ->
                            {skip, "Terraform failed to query VMs (timeout)"}
                    after 5000 ->
                              poll_vms(Config)
                    end;
                true ->
                    timer:cancel(TRef),
                    rabbit_ct_helpers:set_config(Config,
                                                 {terraform_poll_done, true})
            end;
        _ ->
            {skip, "Terraform failed to query VMs"}
    end.

query_terraform_map_from_poll_state(Config, Var) ->
    TfState = ?config(terraform_poll_state, Config),
    do_query_terraform_map(Config, TfState, Var).

do_query_terraform_map(Config, TfState, Var) ->
    Terraform = ?config(terraform_cmd, Config),
    Cmd = [
           Terraform,
           "output",
           "-no-color",
           {"-state=~s", [TfState]},
           Var
          ],
    case rabbit_ct_helpers:exec(Cmd) of
        {ok, Output} ->
            Map = parse_terraform_map(Output),
            ct:pal(?LOW_IMPORTANCE, "Terraform map: ~p", [Map]),
            {ok, Map};
        _ ->
            {skip, "Terraform failed to query VMs"}
    end.

parse_terraform_map(Output) ->
    Lines = [string:strip(L, right, $,)
             || L <- string:tokens(
                       string:strip(Output, right, $\n),
                       "\n"),
                string:find(L, "=") =/= nomatch],
    [begin
         [K0, V0] = string:tokens(L, "="),
         K = string:strip(string:strip(K0), both, $"),
         V = string:strip(string:strip(V0), both, $"),
         {K, V}
     end || L <- Lines].

initialize_ct_peers(Config, NodenamesMap, IPAddrsMap) ->
    CTPeers = lists:map(
                fun({Hostname, NodenameStr}) ->
                        Nodename = list_to_atom(NodenameStr),
                        IPAddrStr = proplists:get_value(Hostname, IPAddrsMap),
                        {ok, IPAddr} = inet:parse_strict_address(IPAddrStr),
                        {Nodename,
                         [
                          {hostname, Hostname},
                          {ipaddr, IPAddr},
                          % FIXME: We assume some kind of Linux
                          % distribution here.
                          {make_cmd, "make"}
                         ]}
                end, NodenamesMap),
    ct:pal(?LOW_IMPORTANCE, "Remote Erlang nodes: ~p", [CTPeers]),
    rabbit_ct_helpers:set_config(Config, {ct_peers, CTPeers}).

set_inet_hosts(Config) ->
    CTPeers = get_ct_peer_entries(Config),
    inet_db:set_lookup([file, native]),
    [begin
         Hostname = ?config(hostname, CTPeerConfig),
         IPAddr = ?config(ipaddr, CTPeerConfig),
         inet_db:add_host(IPAddr, [Hostname]),
         rabbit_misc:format("{host, ~p, [~p]}.~n",
                            [IPAddr, Hostname])
     end || {_, CTPeerConfig} <- CTPeers],
    Config.

write_inetrc(Config) ->
    PrivDir = ?config(priv_dir, Config),
    Suffix = ?config(terraform_files_suffix, Config),
    Filename = filename:join(
                 PrivDir,
                 rabbit_misc:format("inetrc-~s", [Suffix])),
    LockId = {erlang_inetrc, self()},
    LockNodes = [node()],
    % We write an `inetrc` file per setup so there is no risk of
    % conflict here. However, we want to set the `$ERL_INETRC`
    % environment variable (i.e. something global) because it's exported
    % later by rabbit_ct_helpers:exec() to sub-processes. The lock here
    % ensures we query inetrc, write the file and set `$ERL_INETRC`
    % atomically: we don't want to point the environment variable to an
    % old copy of `inetrc`.
    global:set_lock(LockId, LockNodes),
    Inetrc = inet:get_rc(),
    Lines0 = lists:filter(
              fun
                  ({host, _, _}) -> true;
                  ({lookup, _})  -> true;
                  (_)            -> false
              end, Inetrc),
    Lines = [io_lib:format("~p.~n", [Line]) || Line <- Lines0],
    ct:pal(
      ?LOW_IMPORTANCE,
      "Erlang inetrc:~n~s",
      [string:strip(["  " ++ Line || Line <- Lines], right, $\n)]),
    case file:write_file(Filename, Lines) of
        ok ->
            os:putenv("ERL_INETRC", Filename),
            global:del_lock(LockId, LockNodes),
            Config;
        {error, Reason} ->
            global:del_lock(LockId, LockNodes),
            ct:pal(?LOW_IMPORTANCE, "Failed to write inetrc: ~p~n", [Reason]),
            {skip, "Failed to write inetrc"}
    end.

wait_for_ct_peers(Config) ->
    CTPeers = get_ct_peers(Config),
    Timeout = 40 * 60 * 1000,
    {ok, TRef} = timer:send_after(Timeout, ct_peers_timeout),
    wait_for_ct_peers(Config, CTPeers, TRef).

wait_for_ct_peers(Config, [CTPeer | Rest] = CTPeers, TRef) ->
    case net_adm:ping(CTPeer) of
        pong ->
            ct:pal(?LOW_IMPORTANCE, "Remote Erlang node ~p ready", [CTPeer]),
            wait_for_ct_peers(Config, Rest, TRef);
        pang ->
            receive
                ct_peers_timeout ->
                    ct:pal(?LOW_IMPORTANCE,
                           "Remote Erlang node ~p didn't respond to pings",
                           [CTPeer]),
                    {skip, "Failed to ping remote Erlang nodes (timeout)"}
            after 5000 ->
                      wait_for_ct_peers(Config, CTPeers, TRef)
            end
    end;
wait_for_ct_peers(Config, [], TRef) ->
    timer:cancel(TRef),
    Config.

set_ct_peers_code_path(Config) ->
    CodePath = ?config(erlang_code_path, Config),
    rpc_all(Config, code, add_pathsa, [lists:reverse(CodePath)]),
    Config.

start_ct_logs_proxies(Config) ->
    CTPeers = get_ct_peers(Config),
    do_setup_ct_logs_proxies(CTPeers),
    Config.

configure_ct_peers_environment(Config) ->
    Vars = ["DEPS_DIR"],
    Values = [{Var, Value}
              || Var <- Vars,
                 Value <- [os:getenv(Var)],
                 Value =/= false],
    ct:pal(?LOW_IMPORTANCE,
           "Env. variables to set on remote VMs: ~p~n", [Values]),
    lists:foreach(
      fun({Var, Value}) ->
              rpc_all(Config, os, putenv, [Var, Value])
      end, Values),
    Config.

download_dirs(Config) ->
    ConfigsPerCTPeer = rpc_all(
                         Config,
                         ?MODULE,
                         prepare_dirs_to_download_archives,
                         [Config]),
    inets:start(),
    download_dirs(Config, ConfigsPerCTPeer).

download_dirs(_, [{skip, _} = Error | _]) ->
    Error;
download_dirs(Config, [ConfigPerCTPeer | Rest]) ->
    Urls = ?config(download_dirs_archive_urls, ConfigPerCTPeer),
    case download_urls(Config, Urls) of
        {skip, _} = Error -> Error;
        Config1           -> download_dirs(Config1, Rest)
    end;
download_dirs(Config, []) ->
    Config.

download_urls(Config, [Url | Rest]) ->
    PrivDir = ?config(priv_dir, Config),
    Headers = [{"connection", "close"}],
    Options = [{body_format, binary}],
    ct:pal(?LOW_IMPORTANCE, "Fetching download dirs archive at `~s`", [Url]),
    Ret = httpc:request(get, {Url, Headers}, [], Options),
    case Ret of
        {ok, {{_, 200, _}, _, Body}} ->
            Archive = filename:join(PrivDir, filename:basename(Url)),
            case file:write_file(Archive, Body) of
                ok ->
                    download_urls(Config, Rest);
                {error, Reason} ->
                    ct:pal(
                      ?LOW_IMPORTANCE,
                      "Failed to write download dirs archive `~s` "
                      "to `~s`: ~p",
                      [Url, Archive, Reason]),
                    {skip, "Failed to write download dirs archive"}
            end;
        _ ->
            ct:pal(
              ?LOW_IMPORTANCE,
              "Failed to download dirs archive `~s`: ~p",
              [Url, Ret]),
            {skip, "Failed to download dirs archive"}
    end;
download_urls(Config, []) ->
    Config.

prepare_dirs_to_download_archives(Config) ->
    CTPeer = node(),
    Dirs = ?config(dirs_to_download, Config),
    prepare_dirs_to_download_archives(Config, CTPeer, Dirs, 1).

prepare_dirs_to_download_archives(Config, CTPeer, [Dir | Rest], I) ->
    Config1 = case filelib:is_dir(Dir) of
                  true ->
                      prepare_dirs_to_download_archive(
                        Config, CTPeer, Dir, I);
                  false ->
                      Config
              end,
    case Config1 of
        {skip, _} = Error ->
            Error;
        _ ->
            prepare_dirs_to_download_archives(Config1, CTPeer, Rest, I + 1)
    end;
prepare_dirs_to_download_archives(Config, _, [], _) ->
    start_http_server(Config).

prepare_dirs_to_download_archive(Config, CTPeer, Dir, I) ->
    PrivDir = ?config(priv_dir, Config),
    Archive = rabbit_misc:format(
                "~s-~b-~s.tar.gz", [CTPeer, I, filename:basename(Dir)]),
    FilesList = [File || File <- filelib:wildcard("**", Dir),
                         not filelib:is_dir(filename:join(Dir, File))],
    ct:pal(?LOW_IMPORTANCE, "Creating download dirs archive `~s`", [Archive]),
    Ret = erl_tar:create(
            filename:join(PrivDir, Archive),
            [{File, filename:join(Dir, File)} || File <- FilesList],
            [compressed]),
    case Ret of
        ok ->
            add_archive_to_list(Config, Archive);
        {error, Reason} ->
            ct:pal(
              ?LOW_IMPORTANCE,
              "Failed to create download dirs archive `~s` for dir `~s`: ~p",
              [Archive, Dir, Reason]),
            {skip, "Failed to create download dirs archive"}
    end.

add_archive_to_list(Config, Archive) ->
    AL = rabbit_ct_helpers:get_config(Config, download_dir_archives),
    ArchivesList = case AL of
                       undefined -> [];
                       _         -> AL
                   end,
    rabbit_ct_helpers:set_config(
      Config, {download_dir_archives, [Archive | ArchivesList]}).

start_http_server(Config) ->
    PrivDir = ?config(priv_dir, Config),
    {ok, Hostname} = inet:gethostname(),
    inets:start(),
    Options = [{port, 0},
               {server_name, Hostname},
               {server_root, PrivDir},
               {document_root, PrivDir},
               {keep_alive, false}],
    case inets:start(httpd, Options) of
        {ok, Pid} ->
            HttpInfo = httpd:info(Pid),
            ct:pal(
              ?LOW_IMPORTANCE,
              "Ready to serve download dirs archive at `~s`",
              [archive_name_to_url(HttpInfo, "")]),
            archive_names_to_urls(Config, HttpInfo);
        {error, Reason} ->
            ct:pal(
              ?LOW_IMPORTANCE,
              "Failed to start HTTP server to serve download dirs "
              "archives: ~p",
              [Reason]),
            {skiip, "Failed to start dirs archive HTTP server"}
    end.

archive_names_to_urls(Config, HttpInfo) ->
    ArchivesList = ?config(download_dir_archives, Config),
    UrlsList = [archive_name_to_url(HttpInfo, Archive)
                || Archive <- ArchivesList],
    rabbit_ct_helpers:set_config(
      Config, {download_dirs_archive_urls, UrlsList}).

archive_name_to_url(HttpInfo, Archive) ->
    Hostname = proplists:get_value(server_name, HttpInfo),
    Port = proplists:get_value(port, HttpInfo),
    rabbit_misc:format("http://~s:~b/~s", [Hostname, Port, Archive]).

stop_ct_peers(Config) ->
    CTPeers = get_ct_peers(Config),
    stop_ct_peers(Config, CTPeers).

stop_ct_peers(Config, [CTPeer | Rest]) ->
    erlang:monitor_node(CTPeer, true),
    rpc(Config, CTPeer, init, stop),
    receive
        {nodedown, CTPeer} -> ok
    end,
    stop_ct_peers(Config, Rest);
stop_ct_peers(Config, []) ->
    Config.

%% -------------------------------------------------------------------
%% CT logs + user I/O proxying.
%% -------------------------------------------------------------------

do_setup_ct_logs_proxies(Nodes) ->
    [begin
         user_io_proxy(Node),
         ct_logs_proxy(Node)
     end || Node <- Nodes].

user_io_proxy(Node) ->
    ok = setup_proxy(Node, user).

ct_logs_proxy(Node) ->
    ok = setup_proxy(Node, ct_logs).

setup_proxy(Node, RegName) ->
    case whereis(RegName) of
        undefined ->
            ok;
        Pid ->
            ok = rpc:call(Node, ?MODULE, do_setup_proxy, [RegName, Pid])
    end.

do_setup_proxy(RegName, Pid) ->
    case whereis(RegName) of
        undefined ->
            ok;
        OldProxy ->
            erlang:unregister(RegName),
            erlang:exit(OldProxy, normal)
    end,
    ProxyPid = erlang:spawn(?MODULE, proxy_loop, [Pid]),
    true = erlang:register(RegName, ProxyPid),
    ok.

proxy_loop(UpstreamPid) ->
    receive
        Msg ->
            UpstreamPid ! Msg,
            proxy_loop(UpstreamPid)
    end.

%% -------------------------------------------------------------------
%% Other helpers.
%% -------------------------------------------------------------------

get_ct_peer_entries(Config) ->
    case rabbit_ct_helpers:get_config(Config, ct_peers) of
        undefined -> [];
        CTPeers   -> CTPeers
    end.

get_ct_peer_entry(Config, VM) when is_integer(VM) andalso VM >= 0 ->
    CTPeers = get_ct_peer_entries(Config),
    case VM < length(CTPeers) of
        true  -> lists:nth(VM + 1, CTPeers);
        false -> throw({out_of_bound_ct_peer, VM, CTPeers})
    end;
get_ct_peer_entry(Config, VM) when is_atom(VM) ->
    CTPeers = get_ct_peer_entries(Config),
    case proplists:lookup(VM, CTPeers) of
        none   -> throw({unknown_ct_peer, VM, CTPeers});
        CTPeer -> CTPeer
    end.

get_ct_peers(Config) ->
    [CTPeer || {CTPeer, _} <- get_ct_peer_entries(Config)].

get_ct_peer(Config, VM) ->
    {CTPeer, _} = get_ct_peer_entry(Config, VM),
    CTPeer.

get_ct_peer_configs(Config, Key) ->
    CTPeerEntries = get_ct_peer_entries(Config),
    [?config(Key, CTPeerConfig) || {_, CTPeerConfig} <- CTPeerEntries].

get_ct_peer_config(Config, VM) ->
    {_, CTPeerConfig} = get_ct_peer_entry(Config, VM),
    CTPeerConfig.

get_ct_peer_config(Config, VM, Key) ->
    CTPeerConfig = get_ct_peer_config(Config, VM),
    ?config(Key, CTPeerConfig).

get_current_vm_config(Config, Key) ->
    try
        CTPeerConfig = get_ct_peer_config(Config, node()),
        case rabbit_ct_helpers:get_config(CTPeerConfig, Key) of
            undefined ->
                ?config(Key, Config);
            Value ->
                Value
        end
    catch
        throw:{Reason, _, _} when
              Reason =:= out_of_bound_ct_peer orelse
              Reason =:= unknown_ct_peer ->
            ?config(Key, Config)
    end.

rpc(Config, VM, Module, Function) ->
    rpc(Config, VM, Module, Function, []).

rpc(Config, VM, Module, Function, Args)
  when is_integer(VM) orelse is_atom(VM) ->
    CTPeer = get_ct_peer(Config, VM),
    %% We add some directories to the remote node search path.
    rabbit_ct_broker_helpers:add_code_path_to_node(CTPeer, Module),
    Ret = rpc:call(CTPeer, Module, Function, Args),
    case Ret of
        {badrpc, {'EXIT', Reason}} -> exit(Reason);
        {badrpc, Reason}           -> exit(Reason);
        Ret                        -> Ret
    end;
rpc(Config, VMs, Module, Function, Args)
  when is_list(VMs) ->
    [rpc(Config, VM, Module, Function, Args) || VM <- VMs].

rpc_all(Config, Module, Function) ->
    rpc_all(Config, Module, Function, []).

rpc_all(Config, Module, Function, Args) ->
    CTPeers = get_ct_peers(Config),
    rpc(Config, CTPeers, Module, Function, Args).
