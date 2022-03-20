%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(integration_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-define(CLUSTER_SIZE, 3).
-define(TIMEOUT_MILLIS, 180_000).

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
     {timetrap, {minutes, 20}}
    ].

groups() ->
    [
     {using_tags, [parallel], [cluster_was_formed]},
     {using_autoscaling_group, [parallel], [cluster_was_formed]}
    ].

init_per_suite(Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            %% These test would like passed in mixed versions, but they won't
            %% actually honor mixed versions as currently specified via env var
            {skip, "not mixed versions compatible"};
        _ ->
            inets:start(),
            rabbit_ct_helpers:log_environment(),
            Config1 = rabbit_ct_helpers:set_config(
                        Config, [
                                {ecs_region, "eu-west-1"},
                                {ecs_cluster_name, os:getenv("AWS_ECS_CLUSTER_NAME", "rabbitmq-peer-discovery-aws")},
                                {ecs_profile_name, "rabbitmq-peer-discovery-aws-profile"},
                                {ecs_instance_role, "ecs-peer-discovery-aws"},
                                {ecs_cluster_size, ?CLUSTER_SIZE},
                                {rabbitmq_default_user, "test"},
                                {rabbitmq_default_pass, rabbit_ct_helpers:random_term_checksum()},
                                {rabbitmq_erlang_cookie, rabbit_ct_helpers:random_term_checksum()}
                                ]),
            Config2 = rabbit_ct_helpers:register_teardown_step(Config1, fun aws_ecs_util:destroy_ecs_cluster/1),
            rabbit_ct_helpers:run_steps(
            Config2, [
                        fun rabbit_ct_helpers:init_skip_as_error_flag/1,
                        fun rabbit_ct_helpers:start_long_running_testsuite_monitor/1,
                        fun aws_ecs_util:ensure_aws_cli/1,
                        fun aws_ecs_util:ensure_ecs_cli/1,
                        fun aws_ecs_util:init_aws_credentials/1,
                        fun aws_ecs_util:ensure_rabbitmq_image/1,
                        fun aws_ecs_util:start_ecs_cluster/1
                    ])
    end.

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(using_tags, Config) ->
    Config1 = rabbit_ct_helpers:set_config(
                Config,
                [{ecs_service_name, "rabbitmq-tagged"}]),
    rabbit_ct_helpers:run_steps(Config1, [fun register_tagged_task/1,
                                          fun aws_ecs_util:create_service/1]);
init_per_group(using_autoscaling_group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(
                Config,
                [{ecs_service_name, "rabbitmq-autoscaled"}]),
    rabbit_ct_helpers:run_steps(Config1, [fun register_autoscaled_task/1,
                                          fun aws_ecs_util:create_service/1]).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config, [fun aws_ecs_util:delete_service/1,
                                         fun (C) ->
                                                 % A short delay so that all tasks
                                                 % associated with the service can
                                                 % be deregistered
                                                 timer:sleep(15000),
                                                 C
                                         end,
                                         fun aws_ecs_util:deregister_tasks/1]).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

cluster_was_formed(Config) ->
    {ok, [H1, H2, H3]} = ?awaitMatch({ok, L} when length(L) == ?CLUSTER_SIZE,
                                                  aws_ecs_util:public_dns_names(Config),
                                                  ?TIMEOUT_MILLIS),

    [N1Nodes, N2Nodes, N3Nodes] =
        [begin
             {ok, R} = ?awaitMatch({ok, R} when is_list(R) andalso length(R) == ?CLUSTER_SIZE,
                                                aws_ecs_util:fetch_nodes_endpoint(Config, binary_to_list(H)),
                                                ?TIMEOUT_MILLIS),
             [maps:get(<<"name">>, N) || N <- R]
         end || H <- [H1, H2, H3]],

    ?assertEqual(lists:sort(N1Nodes), lists:sort(N2Nodes)),
    ?assertEqual(lists:sort(N2Nodes), lists:sort(N3Nodes)).

register_tagged_task(Config) ->
    RabbitmqDefaultUser = ?config(rabbitmq_default_user, Config),
    RabbitmqDefaultPass = ?config(rabbitmq_default_pass, Config),
    RabbitmqConf = string:join([
                                "default_user = " ++ RabbitmqDefaultUser,
                                "default_pass = " ++ RabbitmqDefaultPass,
                                "cluster_formation.peer_discovery_backend = aws",
                                "cluster_formation.aws.instance_tags.service = rabbitmq",
                                ""
                               ], "\n"),
    TaskJson = task_json(Config, RabbitmqConf),
    aws_ecs_util:register_task(Config, TaskJson).

register_autoscaled_task(Config) ->
    RabbitmqDefaultUser = ?config(rabbitmq_default_user, Config),
    RabbitmqDefaultPass = ?config(rabbitmq_default_pass, Config),
    RabbitmqConf = string:join([
                                "default_user = " ++ RabbitmqDefaultUser,
                                "default_pass = " ++ RabbitmqDefaultPass,
                                "cluster_formation.peer_discovery_backend = aws",
                                "cluster_formation.aws.use_autoscaling_group = true",
                                ""
                               ], "\n"),
    TaskJson = task_json(Config, RabbitmqConf),
    aws_ecs_util:register_task(Config, TaskJson).

task_json(Config, RabbitmqConf) ->
    DataDir = ?config(data_dir, Config),
    RabbitmqImage = ?config(rabbitmq_image, Config),
    RabbitmqDefaultUser = ?config(rabbitmq_default_user, Config),
    RabbitmqDefaultPass = ?config(rabbitmq_default_pass, Config),
    RabbitmqErlangCookie = ?config(rabbitmq_erlang_cookie, Config),
    ServiceName = ?config(ecs_service_name, Config),

    {ok, Binary} = file:read_file(filename:join(DataDir, "task_definition.json")),
    TaskDef = rabbit_json:decode(Binary),

    [RabbitContainerDef, SidecarContainerDef] = maps:get(<<"containerDefinitions">>, TaskDef),
    RabbitContainerDef1 =
        RabbitContainerDef#{
                            <<"image">> := list_to_binary(RabbitmqImage),
                            <<"environment">> := [#{<<"name">> => <<"RABBITMQ_ERLANG_COOKIE">>,
                                                    <<"value">> => list_to_binary(RabbitmqErlangCookie)}]
                           },
    SidecarContainerDef1 =
        SidecarContainerDef#{<<"environment">> := [#{<<"name">> => <<"DATA">>,
                                                     <<"value">> => base64:encode(RabbitmqConf)}]},
    rabbit_json:encode(
      TaskDef#{<<"family">> := list_to_binary(ServiceName),
               <<"containerDefinitions">> := [RabbitContainerDef1, SidecarContainerDef1]}).
