%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
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
    case rabbit_ct_helpers:is_mixed_versions() of
        false ->
            rabbit_ct_helpers:log_environment(),
            rabbit_ct_helpers:run_setup_steps(Config);
        _ ->
            {skip, "growing and shrinking cannot be done in mixed mode"}
    end.

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(tests, Config0) ->
    NumNodes = 3,
    Config1 = rabbit_ct_helpers:set_config(
                Config0, [{rmq_nodes_count, NumNodes},
                          {rmq_nodes_clustered, true}]),
    rabbit_ct_helpers:run_steps(Config1,
                                rabbit_ct_broker_helpers:setup_steps() ++
                                rabbit_ct_client_helpers:setup_steps()).

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
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    %% declare a quorum queue
    QName = "shrink1",
    #'queue.declare_ok'{} = declare_qq(Ch, QName),
    publish_confirm(Ch, QName),
    {ok, Out1} = rabbitmq_queues(Config, 0, ["shrink", Nodename2]),
    ?assertMatch(#{{"/", "shrink1"} := {2, ok}}, parse_result(Out1)),
    %% removing a node can trigger a leader election, give this QQ some time
    %% to do it
    timer:sleep(1500),
    Nodename1 = rabbit_ct_broker_helpers:get_node_config(Config, 1, nodename),
    publish_confirm(Ch, QName),
    {ok, Out2} = rabbitmq_queues(Config, 0, ["shrink", Nodename1]),
    ?assertMatch(#{{"/", "shrink1"} := {1, ok}}, parse_result(Out2)),
    %% removing a node can trigger a leader election, give this QQ some time
    %% to do it
    timer:sleep(1500),
    Nodename0 = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    publish_confirm(Ch, QName),
    {ok, Out3} = rabbitmq_queues(Config, 0, ["shrink", Nodename0]),
    ?assertMatch(#{{"/", "shrink1"} := {1, error}}, parse_result(Out3)),
    ok.

grow(Config) ->
    NodeConfig = rabbit_ct_broker_helpers:get_node_config(Config, 2),
    Nodename2 = ?config(nodename, NodeConfig),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Nodename2),
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    %% declare a quorum queue
    QName = "grow1",
    Args = [{<<"x-quorum-initial-group-size">>, long, 1}],
    #'queue.declare_ok'{} = declare_qq(Ch, QName, Args),
    Nodename0 = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    publish_confirm(Ch, QName),
    {ok, Out1} = rabbitmq_queues(Config, 0, ["grow", Nodename0, "all"]),
    ?assertMatch(#{{"/", "grow1"} := {2, ok}}, parse_result(Out1)),
    timer:sleep(500),
    Nodename1 = rabbit_ct_broker_helpers:get_node_config(Config, 1, nodename),
    publish_confirm(Ch, QName),
    {ok, Out2} = rabbitmq_queues(Config, 0, ["grow", Nodename1, "all"]),
    ?assertMatch(#{{"/", "grow1"} := {3, ok}}, parse_result(Out2)),
    publish_confirm(Ch, QName),
    {ok, Out3} = rabbitmq_queues(Config, 0, ["grow", Nodename0, "all"]),
    ?assertNotMatch(#{{"/", "grow1"} := _}, parse_result(Out3)),
    ok.

grow_invalid_node_filtered(Config) ->
    NodeConfig = rabbit_ct_broker_helpers:get_node_config(Config, 2),
    Nodename2 = ?config(nodename, NodeConfig),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Nodename2),
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    %% declare a quorum queue
    QName = "grow-err",
    Args = [{<<"x-quorum-initial-group-size">>, long, 1}],
    #'queue.declare_ok'{} = declare_qq(Ch, QName, Args),
    DummyNode = not_really_a_node@nothing,
    publish_confirm(Ch, QName),
    %% validated as of rabbitmq-server#8007
    {error, _ExitCode, _} = rabbitmq_queues(Config, 0, ["grow", DummyNode, "all"]),
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

publish_confirm(Ch, QName) ->
    ok = amqp_channel:cast(Ch,
                           #'basic.publish'{routing_key = list_to_binary(QName)},
                           #amqp_msg{props   = #'P_basic'{delivery_mode = 2},
                                     payload = <<"msg">>}),
    amqp_channel:register_confirm_handler(Ch, self()),
    ct:pal("waiting for confirms from ~ts", [QName]),
    receive
        #'basic.ack'{} ->
            ct:pal("CONFIRMED! ~ts", [QName]),
            ok;
        #'basic.nack'{} ->
            ct:pal("NOT CONFIRMED! ~ts", [QName]),
            fail
    after 30000 ->
            exit(confirm_timeout)
    end.
