%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(aws_ecs_util).

-include_lib("common_test/include/ct.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-export([ensure_aws_cli/1,
         ensure_ecs_cli/1,
         init_aws_credentials/1,
         ensure_rabbitmq_image/1,
         start_ecs_cluster/1,
         destroy_ecs_cluster/1,
         register_task/2,
         deregister_tasks/1,
         create_service/1,
         delete_service/1,
         public_dns_names/1,
         fetch_nodes_endpoint/2]).

-define(ECS_CLUSTER_TIMEOUT, 120000).

%% NOTE:
%% These helpers assume certain permissions associated with the aws credentials
%% used. The user must have at least a policy with:
%% {
%%     "Version": "2012-10-17",
%%     "Statement": [
%%         {
%%             "Sid": "VisualEditor0",
%%             "Effect": "Allow",
%%             "Action": [
%%                 "iam:CreateInstanceProfile",
%%                 "iam:DeleteInstanceProfile",
%%                 "iam:PassRole",
%%                 "iam:DetachRolePolicy",
%%                 "iam:DeleteRolePolicy",
%%                 "iam:RemoveRoleFromInstanceProfile",
%%                 "iam:CreateRole",
%%                 "iam:DeleteRole",
%%                 "iam:AttachRolePolicy",
%%                 "iam:AddRoleToInstanceProfile"
%%             ],
%%             "Resource": "*"
%%         }
%%     ]
%% }
%%
%% Additionally, there must be a role called 'ecs-peer-discovery-aws' with the
%% following policies:
%%  - AmazonEC2FullAccess
%%  - AmazonECS_FullAccess
%%  - AmazonEC2ContainerServiceforEC2Role

ensure_aws_cli(Config) ->
    Aws = "aws",
    case rabbit_ct_helpers:exec([Aws, "--version"], [{match_stdout, "aws-cli"}]) of
        {ok, _} -> rabbit_ct_helpers:set_config(Config, {aws_cmd, Aws});
        _ -> {skip, "aws cli required"}
    end.

ensure_ecs_cli(Config) ->
    Ecs = "ecs-cli",
    case rabbit_ct_helpers:exec([Ecs, "--version"], [{match_stdout, "ecs-cli"}]) of
        {ok, _} -> rabbit_ct_helpers:set_config(Config, {ecs_cli_cmd, Ecs});
        _ -> {skip, "ecs-cli required"}
    end.

init_aws_credentials(Config) ->
    AccessKeyId = get_env_var_or_awscli_config_key(
                    "AWS_ACCESS_KEY_ID", "aws_access_key_id"),
    SecretAccessKey = get_env_var_or_awscli_config_key(
                        "AWS_SECRET_ACCESS_KEY", "aws_secret_access_key"),
    rabbit_ct_helpers:set_config(
      Config,
      [{aws_access_key_id, AccessKeyId},
       {aws_secret_access_key, SecretAccessKey}]).

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

ensure_rabbitmq_image(Config) ->
    Image = case rabbit_ct_helpers:get_config(Config, rabbitmq_image) of
                undefined -> os:getenv("RABBITMQ_IMAGE");
                I -> I
            end,
    case Image of
        false ->
            {skip, "rabbitmq image required," ++
                 "please set RABBITMQ_IMAGE or 'rabbitmq_image' " ++
                 "in ct config"};
        Img ->
            rabbit_ct_helpers:set_config(
              Config, {rabbitmq_image, Img})
    end.

ecs_configure(Config) ->
    EcsCliCmd = ?config(ecs_cli_cmd, Config),
    ClusterName = ?config(ecs_cluster_name, Config),
    Region = ?config(ecs_region, Config),
    ConfigureCmd = [EcsCliCmd, "configure",
                    "--cluster", ClusterName,
                    "--default-launch-type", "EC2",
                    "--config-name", ClusterName,
                    "--region", Region],
    case rabbit_ct_helpers:exec(ConfigureCmd, []) of
        {ok, _} -> Config;
        _ -> {skip, "Could not configure ecs"}
    end.

ecs_configure_profile(Config) ->
    EcsCliCmd = ?config(ecs_cli_cmd, Config),
    ProfileName = ?config(ecs_profile_name, Config),
    AccessKeyId = ?config(aws_access_key_id, Config),
    SecretAccessKey = ?config(aws_secret_access_key, Config),
    ConfigureProfileCmd = [EcsCliCmd, "configure", "profile",
                           "--access-key", AccessKeyId,
                           "--secret-key", SecretAccessKey,
                           "--profile-name", ProfileName],
    case rabbit_ct_helpers:exec(ConfigureProfileCmd, []) of
        {ok, _} -> Config;
        _ -> {skip, "Could not configure ecs profile"}
    end.

ecs_up(Config) ->
    EcsCliCmd = ?config(ecs_cli_cmd, Config),
    InstanceRole = ?config(ecs_instance_role, Config),
    ClusterName = ?config(ecs_cluster_name, Config),
    ProfileName = ?config(ecs_profile_name, Config),
    ClusterSize = ?config(ecs_cluster_size, Config),
    UpCmd = [EcsCliCmd, "up",
             "--instance-role", InstanceRole,
             "--size", integer_to_list(ClusterSize),
             "--instance-type", "t2.medium",
             "--keypair", "id_rsa_terraform",
             "--port", "15672",
             "--cluster-config", ClusterName,
             "--ecs-profile", ProfileName,
             "--tags", "service=rabbitmq"],
    case rabbit_ct_helpers:exec(UpCmd, []) of
        {ok, _} -> Config;
        _ -> {skip, "Could not start ecs cluster"}
    end.

ecs_down(Config) ->
    EcsCliCmd = ?config(ecs_cli_cmd, Config),
    ClusterName = ?config(ecs_cluster_name, Config),
    ProfileName = ?config(ecs_profile_name, Config),
    DownCmd = [EcsCliCmd, "down",
               "--force",
               "--cluster-config", ClusterName,
               "--ecs-profile", ProfileName],
    rabbit_ct_helpers:exec(DownCmd, []),
    Config.

adjust_security_group(Config) ->
    AwsCmd = ?config(aws_cmd, Config),
    ClusterName = ?config(ecs_cluster_name, Config),
    Region = ?config(ecs_region, Config),
    {ok, [GroupId]} = ?awaitMatch({ok, L} when length(L) == 1,
                                               security_group_ids(AwsCmd, ClusterName, Region),
                                               ?ECS_CLUSTER_TIMEOUT),
    AuthorizeSecurityGroupIngress = [AwsCmd, "ec2", "authorize-security-group-ingress",
                                     "--region", Region,
                                     "--group-id", GroupId,
                                     "--protocol", "tcp",
                                     "--port", "1-65535",
                                     "--source-group", GroupId],
    {ok, _} = rabbit_ct_helpers:exec(AuthorizeSecurityGroupIngress, []),
    Config.

start_ecs_cluster(Config) ->
    try rabbit_ct_helpers:run_steps(Config,
                                    [fun ecs_configure/1,
                                     fun ecs_configure_profile/1,
                                     fun ecs_up/1,
                                     fun adjust_security_group/1])
    catch
        Class:Reason:Stacktrace ->
            ecs_down(Config),
            erlang:raise(Class, Reason, Stacktrace)
    end.

destroy_ecs_cluster(Config) ->
    ecs_down(Config).

list_container_instances(AwsCmd, ClusterName, Region) ->
    ListContainerInstances = [AwsCmd, "ecs", "list-container-instances",
                              "--cluster", ClusterName,
                              "--region", Region],
    case rabbit_ct_helpers:exec(ListContainerInstances, [binary]) of
        {ok, Response} -> rabbit_json:try_decode(Response);
        Error -> Error
    end.

describe_container_instances(AwsCmd, ClusterName, Region) ->
    case list_container_instances(AwsCmd, ClusterName, Region) of
        {ok, #{<<"containerInstanceArns">> := []}} ->
            {error, no_instances};
        {ok, #{<<"containerInstanceArns">> := ContainerInstanceArns}} ->
            InputJson = rabbit_json:encode(#{<<"cluster">> => list_to_binary(ClusterName),
                                             <<"containerInstances">> => ContainerInstanceArns}),
            DescribeContainerInstances = [AwsCmd, "ecs", "describe-container-instances",
                                          "--region", Region,
                                          "--query", "containerInstances[*].ec2InstanceId",
                                          "--cli-input-json", InputJson],
            case rabbit_ct_helpers:exec(DescribeContainerInstances, []) of
                {ok, Response} -> rabbit_json:try_decode(list_to_binary(Response));
                Error -> Error
            end;
        Error ->
            Error
    end.

describe_instances(AwsCmd, ClusterName, Region, Query) ->
    case describe_container_instances(AwsCmd, ClusterName, Region) of
        {ok, InstanceIds} ->
            DescribeInstances = [AwsCmd, "ec2", "describe-instances",
                                 "--region", Region,
                                 "--query", Query,
                                 "--instance-ids"] ++ InstanceIds,
            case rabbit_ct_helpers:exec(DescribeInstances, []) of
                {ok, Response} -> rabbit_json:try_decode(list_to_binary(Response));
                Error -> Error
            end;
        Error ->
            Error
    end.

security_group_ids(AwsCmd, ClusterName, Region) ->
    Query = "Reservations[*].Instances[].NetworkInterfaces[].Groups[].GroupId",
    case describe_instances(AwsCmd, ClusterName, Region, Query) of
        {ok, InstancesResponse} ->
            {ok, lists:usort(InstancesResponse)};
        Error ->
            Error
    end.

register_task(Config, TaskJson) ->
    AwsCmd = ?config(aws_cmd, Config),
    Region = ?config(ecs_region, Config),
    Cmd = [AwsCmd, "ecs", "register-task-definition",
           "--region", Region,
           "--cli-input-json", TaskJson],
    case rabbit_ct_helpers:exec(Cmd, []) of
        {ok, _} -> Config;
        _ -> {skip, "Failed to register task with ecs"}
    end.

deregister_tasks(Config) ->
    AwsCmd = ?config(aws_cmd, Config),
    Region = ?config(ecs_region, Config),
    ServiceName = ?config(ecs_service_name, Config),
    ListCmd = [AwsCmd, "ecs", "list-task-definitions",
               "--region", Region,
               "--family-prefix", ServiceName],
    {ok, Defs} = rabbit_ct_helpers:exec(ListCmd, [binary]),
    #{<<"taskDefinitionArns">> := Arns} = rabbit_json:decode(Defs),
    [begin
         DelCmd = [AwsCmd, "ecs", "deregister-task-definition",
                   "--region", Region,
                   "--task-definition", Arn],
         rabbit_ct_helpers:exec(DelCmd, [])
     end || Arn <- Arns],
    Config.

create_service(Config) ->
    AwsCmd = ?config(aws_cmd, Config),
    Region = ?config(ecs_region, Config),
    ClusterName = ?config(ecs_cluster_name, Config),
    ServiceName = ?config(ecs_service_name, Config),
    ClusterSize = ?config(ecs_cluster_size, Config),
    Cmd = [AwsCmd, "ecs", "create-service",
           "--region", Region,
           "--cluster", ClusterName,
           "--service-name", ServiceName,
           "--desired-count", integer_to_list(ClusterSize),
           "--launch-type", "EC2",
           "--task-definition", ServiceName],
    case rabbit_ct_helpers:exec(Cmd, []) of
        {ok, _} -> Config;
        _ -> {skip, "Failed to create service in ecs"}
    end.

delete_service(Config) ->
    AwsCmd = ?config(aws_cmd, Config),
    Region = ?config(ecs_region, Config),
    ClusterName = ?config(ecs_cluster_name, Config),
    ServiceName = ?config(ecs_service_name, Config),
    Cmd = [AwsCmd, "ecs", "delete-service",
           "--region", Region,
           "--cluster", ClusterName,
           "--service", ServiceName,
           "--force"],
    rabbit_ct_helpers:exec(Cmd, []),
    Config.

public_dns_names(Config) ->
    AwsCmd = ?config(aws_cmd, Config),
    ClusterName = ?config(ecs_cluster_name, Config),
    Region = ?config(ecs_region, Config),
    Query = "Reservations[*].Instances[].PublicDnsName",
    describe_instances(AwsCmd, ClusterName, Region, Query).

fetch_nodes_endpoint(Config, Host) when is_list(Host)->
    DefaultUser = ?config(rabbitmq_default_user, Config),
    DefaultPass = ?config(rabbitmq_default_pass, Config),
    Url = "http://" ++ Host ++ ":15672/api/nodes",
    case httpc:request(
           get,
           {Url, [rabbit_mgmt_test_util:auth_header(DefaultUser, DefaultPass)]},
           [{version, "HTTP/1.0"}, {autoredirect, false}, {timeout, 10000}],
           [{body_format, binary}]) of
        {ok, {{"HTTP/1.1",200,"OK"}, _Headers, Body}} ->
            rabbit_json:try_decode(Body);
        Other ->
            Other
    end.
