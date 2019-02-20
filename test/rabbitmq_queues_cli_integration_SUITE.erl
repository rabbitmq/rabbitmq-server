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
%% Copyright (c) 2017-2019 Pivotal Software, Inc.  All rights reserved.
%%
-module(rabbitmq_queues_cli_integration_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

all() ->
    [
        {group, tests}
    ].

groups() ->
    [
        {tests, [], [
            shrink
        ]}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(tests, Config0) ->
    NumNodes = 3,
    Config1 = rabbit_ct_helpers:set_config(
                Config0, [{rmq_nodes_count, NumNodes},
                          {rmq_nodes_clustered, true}]),
    rabbit_ct_helpers:run_steps(Config1,
                                rabbit_ct_broker_helpers:setup_steps() ++
                                rabbit_ct_client_helpers:setup_steps()
    ).

end_per_group(tests, Config) ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_client_helpers:teardown_steps() ++
                                    rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config0) ->
    ensure_rabbitmq_queues_cmd(
      rabbit_ct_helpers:testcase_started(Config0, Testcase)).

end_per_testcase(Testcase, Config0) ->
    rabbit_ct_helpers:testcase_finished(Config0, Testcase).

shrink(Config) ->
    NodeConfig = rabbit_ct_broker_helpers:get_node_config(Config, 2),
    Nodename2 = ?config(nodename, NodeConfig),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Nodename2),
    %% declare a quorum queue
    QName = "shrink1",
    #'queue.declare_ok'{} = declare_qq(Ch, QName),
    {ok, Out1} = rabbitmq_queues(Config, 0, ["shrink", Nodename2]),
    ?assertMatch(#{{"/", "shrink1"} := {2, ok}}, parse_result(Out1)),
    Nodename1 = rabbit_ct_broker_helpers:get_node_config(Config, 1, nodename),
    {ok, Out2} = rabbitmq_queues(Config, 0, ["shrink", Nodename1]),
    ?assertMatch(#{{"/", "shrink1"} := {1, ok}}, parse_result(Out2)),
    Nodename0 = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    {ok, Out3} = rabbitmq_queues(Config, 0, ["shrink", Nodename0]),
    ?assertMatch(#{{"/", "shrink1"} := {1, error}}, parse_result(Out3)),
    ok.

parse_result(S) ->
    %% strip preamble and header
    [_, _ |Lines] = string:split(S, "\n", all),
    maps:from_list(
      [{{Vhost, QName},
        {erlang:list_to_integer(Size), case Result of
                                           "ok" -> ok;
                                           _ -> error
                                       end}}
       || [Vhost, QName, Size, Result] <-
          [string:split(L, "\t", all) || L <- Lines]]).

declare_qq(Ch, Q) ->
    Args = [{<<"x-queue-type">>, longstr, <<"quorum">>}],
    amqp_channel:call(Ch, #'queue.declare'{queue = list_to_binary(Q),
                                           durable = true,
                                           auto_delete = false,
                                           arguments = Args}).
ensure_rabbitmq_queues_cmd(Config) ->
    RabbitmqQueues = case rabbit_ct_helpers:get_config(Config, rabbitmq_queues_cmd) of
                         undefined ->
                             SrcDir = ?config(rabbit_srcdir, Config),
                             R = filename:join(SrcDir, "scripts/rabbitmq-queues"),
                             ct:pal(?LOW_IMPORTANCE, "Using rabbitmq-queues at ~p~n", [R]),
                             case filelib:is_file(R) of
                                 true  -> R;
                                 false -> false
                             end;
                         R ->
                             ct:pal(?LOW_IMPORTANCE,
                                    "Using rabbitmq-queues from rabbitmq_queues_cmd: ~p~n", [R]),
                             R
                     end,
    Error = {skip, "rabbitmq-queues required, " ++
             "please set 'rabbitmqctl_cmd' in ct config"},
    case RabbitmqQueues of
        false ->
            Error;
        _ ->
            Cmd = [RabbitmqQueues],
            case rabbit_ct_helpers:exec(Cmd, [drop_stdout]) of
                {error, 64, _} ->
                    rabbit_ct_helpers:set_config(Config,
                                                 {rabbitmq_queues_cmd,
                                                  RabbitmqQueues});
                {error, Code, Reason} ->
                    ct:pal(?LOW_IMPORTANCE, "Exec failed with exit code ~d: ~p", [Code, Reason]),
                    Error;
                _ ->
                    Error
            end
    end.

rabbitmq_queues(Config, Node, Args) ->
    RabbitmqQueues = ?config(rabbitmq_queues_cmd, Config),
    NodeConfig = rabbit_ct_broker_helpers:get_node_config(Config, Node),
    Nodename = ?config(nodename, NodeConfig),
    Env0 = [
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
