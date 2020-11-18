%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(integration_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0,
         suite/0,
         groups/0,
         init_per_suite/1, end_per_suite/1,
         init_per_group/2, end_per_group/2,
         init_per_testcase/2, end_per_testcase/2,

         cluster_was_formed/1
        ]).

all() ->
    [
     {group, using_tags},
     {group, using_autoscaling_group}
    ].

suite() ->
    [
     {timetrap, {hours, 1}}
    ].

groups() ->
    [
     {using_tags, [parallel], [cluster_was_formed]},
     {using_autoscaling_group, [parallel], [cluster_was_formed]}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(
                Config,
                [
                 {terraform_files_suffix, rabbit_ct_helpers:random_term_checksum()},
                 {terraform_aws_ec2_region, "eu-west-1"},
                 {rmq_nodes_clustered, false}
                ]),
    Config2 = init_aws_credentials(Config1),
    rabbit_ct_helpers:run_setup_steps(Config2).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(using_tags, Config) ->
    TfConfigDir = rabbit_ct_vm_helpers:aws_autoscaling_group_module(Config),
    AccessKeyId = ?config(aws_access_key_id, Config),
    SecretAccessKey = ?config(aws_secret_access_key, Config),
    Suffix = ?config(terraform_files_suffix, Config),
    Config1 = rabbit_ct_helpers:set_config(
                Config, {terraform_config_dir, TfConfigDir}),
    rabbit_ct_helpers:merge_app_env(
      Config1,
      {rabbit,
       [{cluster_formation,
         [{peer_discovery_backend, rabbit_peer_discovery_aws},
          {peer_discovery_aws,
           [
            {aws_ec2_region, ?config(terraform_aws_ec2_region, Config)},
            {aws_access_key, AccessKeyId},
            {aws_secret_key, SecretAccessKey},
            {aws_ec2_tags, [{"rabbitmq-testing-suffix", Suffix}]}
           ]}]}]});
init_per_group(using_autoscaling_group, Config) ->
    TfConfigDir = rabbit_ct_vm_helpers:aws_autoscaling_group_module(Config),
    AccessKeyId = ?config(aws_access_key_id, Config),
    SecretAccessKey = ?config(aws_secret_access_key, Config),
    Config1 = rabbit_ct_helpers:set_config(
                Config, {terraform_config_dir, TfConfigDir}),
    rabbit_ct_helpers:merge_app_env(
      Config1,
      {rabbit,
       [{cluster_formation,
         [{peer_discovery_backend, rabbit_peer_discovery_aws},
          {peer_discovery_aws,
           [
            {aws_ec2_region, ?config(terraform_aws_ec2_region, Config)},
            {aws_access_key, AccessKeyId},
            {aws_secret_key, SecretAccessKey},
            {aws_autoscaling, true}
           ]}]}]}).

end_per_group(_Group, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    InstanceName = rabbit_ct_helpers:testcase_absname(Config, Testcase),
    InstanceCount = 2,
    ClusterSize = InstanceCount,
    Config1 = rabbit_ct_helpers:set_config(
                Config,
                [{terraform_instance_name, InstanceName},
                 {terraform_instance_count, InstanceCount},
                 {rmq_nodename_suffix, Testcase},
                 {rmq_nodes_count, ClusterSize}]),
    rabbit_ct_helpers:run_steps(
      Config1,
      [fun rabbit_ct_broker_helpers:run_make_dist/1] ++
      rabbit_ct_vm_helpers:setup_steps() ++
      rabbit_ct_broker_helpers:setup_steps_for_vms()).

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(
                Config,
                rabbit_ct_broker_helpers:teardown_steps_for_vms() ++
                rabbit_ct_vm_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

init_aws_credentials(Config) ->
    AccessKeyId = get_env_var_or_awscli_config_key(
                    "AWS_ACCESS_KEY_ID", "aws_access_key_id"),
    SecretAccessKey = get_env_var_or_awscli_config_key(
                        "AWS_SECRET_ACCESS_KEY", "aws_secret_access_key"),
    rabbit_ct_helpers:set_config(
      Config,
      [
       {aws_access_key_id, AccessKeyId},
       {aws_secret_access_key, SecretAccessKey}
      ]).

get_env_var_or_awscli_config_key(EnvVar, AwscliKey) ->
    case os:getenv(EnvVar) of
        false -> get_awscli_config_key(AwscliKey);
        Value -> Value
    end.

get_awscli_config_key(AwscliKey) ->
    AwscliConfig = read_awscli_config(),
    maps:get(AwscliKey, AwscliConfig, undefined).

read_awscli_config() ->
    Filename = filename:join([os:getenv("HOME"), ".aws", "credentials"]),
    case filelib:is_regular(Filename) of
        true  -> read_awscli_config(Filename);
        false -> #{}
    end.

read_awscli_config(Filename) ->
    {ok, Content} = file:read_file(Filename),
    Lines = string:tokens(binary_to_list(Content), "\n"),
    read_awscli_config(Lines, #{}).

read_awscli_config([Line | Rest], AwscliConfig) ->
    Line1 = string:strip(Line),
    case Line1 of
        [$# | _] ->
            read_awscli_config(Rest, AwscliConfig);
        [$[ | _] ->
            read_awscli_config(Rest, AwscliConfig);
        _ ->
            [Key, Value] = string:tokens(Line1, "="),
            Key1 = string:strip(Key),
            Value1 = string:strip(Value),
            read_awscli_config(Rest, AwscliConfig#{Key1 => Value1})
    end;
read_awscli_config([], AwscliConfig) ->
    AwscliConfig.

%% -------------------------------------------------------------------
%% Run arbitrary code.
%% -------------------------------------------------------------------

cluster_was_formed(Config) ->
    CTPeers = rabbit_ct_vm_helpers:get_ct_peers(Config),
    ?assertEqual(lists:duplicate(length(CTPeers), false),
                 [rabbit:is_running(CTPeer) || CTPeer <- CTPeers]),
    RabbitMQNodes = lists:sort(
                      rabbit_ct_broker_helpers:get_node_configs(
                        Config, nodename)),
    ?assertEqual(lists:duplicate(length(RabbitMQNodes), true),
                 [rabbit:is_running(Node) || Node <- RabbitMQNodes]),

    ?assertEqual(lists:duplicate(length(RabbitMQNodes), true),
                 rabbit_ct_broker_helpers:rpc_all(
                   Config, rabbit_mnesia, is_clustered, [])),
    ClusteredNodes = lists:sort(
                       rabbit_ct_broker_helpers:rpc(
                         Config, 0, rabbit_mnesia, cluster_nodes, [running])),
    ?assertEqual(ClusteredNodes, RabbitMQNodes).
