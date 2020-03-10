%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2017-2020 VMware, Inc. or its affiliates.  All rights reserved.
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
            shrink,
            grow,
            grow_invalid_node_filtered
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
    Config2 = rabbit_ct_helpers:run_steps(Config1,
                                          rabbit_ct_broker_helpers:setup_steps() ++
                                          rabbit_ct_client_helpers:setup_steps()),
    case rabbit_ct_broker_helpers:enable_feature_flag(Config2, quorum_queue) of
        ok ->
            Config2;
        Skip ->
            end_per_group(tests, Config2),
            Skip
    end.

end_per_group(tests, Config) ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_client_helpers:teardown_steps() ++
                                    rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config0) ->
    rabbit_ct_helpers:ensure_rabbitmq_queues_cmd(
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

grow(Config) ->
    NodeConfig = rabbit_ct_broker_helpers:get_node_config(Config, 2),
    Nodename2 = ?config(nodename, NodeConfig),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Nodename2),
    %% declare a quorum queue
    QName = "grow1",
    Args = [{<<"x-quorum-initial-group-size">>, long, 1}],
    #'queue.declare_ok'{} = declare_qq(Ch, QName, Args),
    Nodename0 = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    {ok, Out1} = rabbitmq_queues(Config, 0, ["grow", Nodename0, "all"]),
    ?assertMatch(#{{"/", "grow1"} := {2, ok}}, parse_result(Out1)),
    Nodename1 = rabbit_ct_broker_helpers:get_node_config(Config, 1, nodename),
    {ok, Out2} = rabbitmq_queues(Config, 0, ["grow", Nodename1, "all"]),
    ?assertMatch(#{{"/", "grow1"} := {3, ok}}, parse_result(Out2)),

    {ok, Out3} = rabbitmq_queues(Config, 0, ["grow", Nodename0, "all"]),
    ?assertNotMatch(#{{"/", "grow1"} := _}, parse_result(Out3)),
    ok.

grow_invalid_node_filtered(Config) ->
    NodeConfig = rabbit_ct_broker_helpers:get_node_config(Config, 2),
    Nodename2 = ?config(nodename, NodeConfig),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Nodename2),
    %% declare a quorum queue
    QName = "grow-err",
    Args = [{<<"x-quorum-initial-group-size">>, long, 1}],
    #'queue.declare_ok'{} = declare_qq(Ch, QName, Args),
    DummyNode = not_really_a_node@nothing,
    {ok, Out1} = rabbitmq_queues(Config, 0, ["grow", DummyNode, "all"]),
    ?assertNotMatch(#{{"/", "grow-err"} := _}, parse_result(Out1)),
    ok.

parse_result(S) ->
    Lines = string:split(S, "\n", all),
    maps:from_list(
      [{{Vhost, QName},
        {erlang:list_to_integer(Size), case Result of
                                           "ok" -> ok;
                                           _ -> error
                                       end}}
       || [Vhost, QName, Size, Result] <-
          [string:split(L, "\t", all) || L <- Lines]]).

declare_qq(Ch, Q, Args0) ->
    Args = [{<<"x-queue-type">>, longstr, <<"quorum">>}] ++ Args0,
    amqp_channel:call(Ch, #'queue.declare'{queue = list_to_binary(Q),
                                           durable = true,
                                           auto_delete = false,
                                           arguments = Args}).
declare_qq(Ch, Q) ->
    declare_qq(Ch, Q, []).

rabbitmq_queues(Config, N, Args) ->
    rabbit_ct_broker_helpers:rabbitmq_queues(Config, N, ["--silent" | Args]).
