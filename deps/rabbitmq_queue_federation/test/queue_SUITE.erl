%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(queue_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(nowarn_export_all).
-compile(export_all).

-import(queue_federation_test_helpers,
        [expect/3, expect/4,
         set_upstream/4, set_upstream/5, clear_upstream/3, set_upstream_set/4, clear_upstream_set/3,
         set_policy/5, clear_policy/3,
         set_policy_pattern/5, set_policy_upstream/5, q/2, with_ch/3,
         maybe_declare_queue/3, delete_queue/2,
         await_running_federation/3]).
-define(EXPECT_FEDERATION_TIMEOUT, 15000).

all() ->
    [
     {group, classic_queue},
     {group, quorum_queue},
     {group, mixed},
     {group, federation_mechanics}
    ].

groups() ->
    [
     {classic_queue, [], queue_type_tests()},
     {quorum_queue, [], queue_type_tests()},
     {mixed, [], queue_type_tests()},
     {federation_mechanics, [], federation_mechanics_tests()}
    ].

queue_type_tests() ->
    [
     {without_disambiguate, [], [
                                 {cluster_size_1, [], [
                                                       simple,
                                                       multiple_downstreams,
                                                       message_flow
                                                      ]}
                                ]},
     {with_disambiguate, [], [
                              {cluster_size_2, [], [restart_upstream]}
                             ]}
    ].

federation_mechanics_tests() ->
    [
     {without_disambiguate, [], [
                                 {cluster_size_1, [], [
                                                       multiple_upstreams_pattern,
                                                       dynamic_reconfiguration,
                                                       federate_unfederate,
                                                       dynamic_plugin_stop_start,
                                                       supervisor_shutdown_concurrency_safety
                                                      ]}
                                ]}
    ].

%% -------------------------------------------------------------------
%% Test suite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(classic_queue, Config) ->
    rabbit_ct_helpers:set_config(
      Config,
      [
       {source_queue_type, classic},
       {source_queue_args, [{<<"x-queue-type">>, longstr, <<"classic">>}]},
       {target_queue_type, classic},
       {target_queue_args, [{<<"x-queue-type">>, longstr, <<"classic">>}]}
      ]);
init_per_group(quorum_queue, Config) ->
    rabbit_ct_helpers:set_config(
      Config,
      [
       {source_queue_type, quorum},
       {source_queue_args, [{<<"x-queue-type">>, longstr, <<"quorum">>}]},
       {target_queue_type, quorum},
       {target_queue_args, [{<<"x-queue-type">>, longstr, <<"quorum">>}]}
      ]);
init_per_group(mixed, Config) ->
    rabbit_ct_helpers:set_config(
      Config,
      [
       {source_queue_type, classic},
       {source_queue_args, [{<<"x-queue-type">>, longstr, <<"classic">>}]},
       {target_queue_type, quorum},
       {target_queue_args, [{<<"x-queue-type">>, longstr, <<"quorum">>}]}
      ]);
init_per_group(federation_mechanics, Config) ->
    rabbit_ct_helpers:set_config(
      Config,
      [
       {source_queue_type, classic},
       {source_queue_args, [{<<"x-queue-type">>, longstr, <<"classic">>}]},
       {target_queue_type, classic},
       {target_queue_args, [{<<"x-queue-type">>, longstr, <<"classic">>}]}
      ]);
init_per_group(without_disambiguate, Config) ->
    rabbit_ct_helpers:set_config(Config,
      {disambiguate_step, []});
init_per_group(with_disambiguate, Config) ->
    rabbit_ct_helpers:set_config(Config,
      {disambiguate_step, [fun queue_federation_test_helpers:disambiguate/1]});
init_per_group(cluster_size_1 = Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodes_count, 1}
      ]),
    init_per_group1(Group, Config1);
init_per_group(cluster_size_2 = Group, Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            {skip, "not mixed versions compatible"};
        _ ->
            Config1 = rabbit_ct_helpers:set_config(Config, [
                {rmq_nodes_count, 2}
            ]),
            init_per_group1(Group, Config1)
    end.

init_per_group1(Group, Config) ->
    SetupFederation = case Group of
        cluster_size_1 -> [fun queue_federation_test_helpers:setup_federation/1];
        cluster_size_2 -> []
    end,
    Disambiguate = ?config(disambiguate_step, Config),
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Suffix},
        {rmq_nodes_clustered, false}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
                                rabbit_ct_broker_helpers:setup_steps() ++
                                rabbit_ct_client_helpers:setup_steps() ++
                                SetupFederation ++ Disambiguate).

end_per_group(without_disambiguate, Config) ->
    Config;
end_per_group(with_disambiguate, Config) ->
    Config;
end_per_group(classic_queue, Config) ->
    Config;
end_per_group(quorum_queue, Config) ->
    Config;
end_per_group(mixed, Config) ->
    Config;
end_per_group(federation_mechanics, Config) ->
    Config;
end_per_group(_, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(dynamic_plugin_stop_start = Testcase, Config) ->
    ct:timetrap({seconds, 60}),
    rabbit_ct_helpers:testcase_started(Config, Testcase);
init_per_testcase(supervisor_shutdown_concurrency_safety = Testcase, Config) ->
    ct:timetrap({seconds, 60}),
    rabbit_ct_helpers:testcase_started(Config, Testcase);
init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------

simple(Config) ->
    with_ch(Config,
      fun (Ch) ->
              await_running_federation(Config,
                [{<<"fed1.downstream">>, <<"upstream">>}],
                ?EXPECT_FEDERATION_TIMEOUT),
              expect_federation(Ch, <<"upstream">>, <<"fed1.downstream">>)
      end, upstream_downstream(Config)).

multiple_upstreams_pattern(Config) ->
    set_upstream(Config, 0, <<"local453x">>,
        rabbit_ct_broker_helpers:node_uri(Config, 0), [
        {<<"exchange">>, <<"upstream">>},
        {<<"queue">>, <<"upstream">>}]),

    set_upstream(Config, 0, <<"zzzzzZZzz">>,
        rabbit_ct_broker_helpers:node_uri(Config, 0), [
        {<<"exchange">>, <<"upstream-zzz">>},
        {<<"queue">>, <<"upstream-zzz">>}]),

    set_upstream(Config, 0, <<"local3214x">>,
        rabbit_ct_broker_helpers:node_uri(Config, 0), [
        {<<"exchange">>, <<"upstream2">>},
        {<<"queue">>, <<"upstream2">>}]),

    set_policy_pattern(Config, 0, <<"pattern">>, <<"^pattern\.">>, <<"local\\d+x">>),

    SourceArgs = ?config(source_queue_args, Config),
    TargetArgs = ?config(target_queue_args, Config),
    with_ch(Config,
      fun (Ch) ->
              await_running_federation(Config,
                [{<<"pattern.downstream">>, <<"upstream">>},
                 {<<"pattern.downstream">>, <<"upstream2">>}],
                ?EXPECT_FEDERATION_TIMEOUT),
              expect_federation(Ch, <<"upstream">>, <<"pattern.downstream">>, ?EXPECT_FEDERATION_TIMEOUT),
              expect_federation(Ch, <<"upstream2">>, <<"pattern.downstream">>, ?EXPECT_FEDERATION_TIMEOUT)
      end, [q(<<"upstream">>, SourceArgs),
            q(<<"upstream2">>, SourceArgs),
            q(<<"pattern.downstream">>, TargetArgs)]),

    clear_upstream(Config, 0, <<"local453x">>),
    clear_upstream(Config, 0, <<"local3214x">>),
    clear_policy(Config, 0, <<"pattern">>).

multiple_downstreams(Config) ->
    Args = ?config(target_queue_args, Config),
    with_ch(Config,
      fun (Ch) ->
              await_running_federation(Config,
                [{<<"fed1.downstream">>, <<"upstream">>},
                 {<<"fed2.downstream">>, <<"upstream2">>}],
                ?EXPECT_FEDERATION_TIMEOUT),
              expect_federation(Ch, <<"upstream">>, <<"fed1.downstream">>, ?EXPECT_FEDERATION_TIMEOUT),
              expect_federation(Ch, <<"upstream2">>, <<"fed2.downstream">>, ?EXPECT_FEDERATION_TIMEOUT)
      end, upstream_downstream(Config) ++ [q(<<"fed2.downstream">>, Args)]).

message_flow(Config) ->
    %% TODO: specifc source / target here
    Args = ?config(source_queue_args, Config),
    with_ch(Config,
      fun (Ch) ->
              await_running_federation(Config,
                [{<<"one">>, <<"two">>},
                 {<<"two">>, <<"one">>}],
                ?EXPECT_FEDERATION_TIMEOUT),
              publish_expect(Ch, <<>>, <<"one">>, <<"one">>, <<"first one">>, ?EXPECT_FEDERATION_TIMEOUT),
              publish_expect(Ch, <<>>, <<"two">>, <<"two">>, <<"first two">>, ?EXPECT_FEDERATION_TIMEOUT),
              Seq = lists:seq(1, 10),
              [publish(Ch, <<>>, <<"one">>, <<"bulk">>) || _ <- Seq],
              [publish(Ch, <<>>, <<"two">>, <<"bulk">>) || _ <- Seq],
              expect(Ch, <<"one">>, repeat(20, <<"bulk">>)),
              expect_empty(Ch, <<"one">>),
              expect_empty(Ch, <<"two">>),
              [publish(Ch, <<>>, <<"one">>, <<"bulk">>) || _ <- Seq],
              [publish(Ch, <<>>, <<"two">>, <<"bulk">>) || _ <- Seq],
              expect(Ch, <<"two">>, repeat(20, <<"bulk">>)),
              expect_empty(Ch, <<"one">>),
              expect_empty(Ch, <<"two">>),
              %% We clear the federation configuration to avoid a race condition
              %% when deleting the queues in quorum mode. The federation link
              %% would restart and lead to a state where nothing happened for
              %% minutes.
              clear_upstream_set(Config, 0, <<"one">>),
              clear_upstream_set(Config, 0, <<"two">>)
      end, [q(<<"one">>, Args),
            q(<<"two">>, Args)]).

dynamic_reconfiguration(Config) ->
    with_ch(Config,
      fun (Ch) ->
              await_running_federation(Config,
                [{<<"fed1.downstream">>, <<"upstream">>}],
                ?EXPECT_FEDERATION_TIMEOUT),
              expect_federation(Ch, <<"upstream">>, <<"fed1.downstream">>, ?EXPECT_FEDERATION_TIMEOUT),

              %% Test that clearing connections works
              clear_upstream(Config, 0, <<"localhost">>),
              expect_no_federation(Ch, <<"upstream">>, <<"fed1.downstream">>),

              %% Test that reading them and changing them works
              set_upstream(Config, 0,
                <<"localhost">>, rabbit_ct_broker_helpers:node_uri(Config, 0)),
              %% Do it twice so we at least hit the no-restart optimisation
              URI = rabbit_ct_broker_helpers:node_uri(Config, 0, [use_ipaddr]),
              set_upstream(Config, 0, <<"localhost">>, URI),
              set_upstream(Config, 0, <<"localhost">>, URI),
              expect_federation(Ch, <<"upstream">>, <<"fed1.downstream">>)
      end, upstream_downstream(Config)).

federate_unfederate(Config) ->
    Args = ?config(target_queue_args, Config),
    with_ch(Config,
      fun (Ch) ->
              await_running_federation(Config,
                [{<<"fed1.downstream">>, <<"upstream">>},
                 {<<"fed2.downstream">>, <<"upstream2">>}],
                ?EXPECT_FEDERATION_TIMEOUT),
              expect_federation(Ch, <<"upstream">>, <<"fed1.downstream">>, ?EXPECT_FEDERATION_TIMEOUT),
              expect_federation(Ch, <<"upstream2">>, <<"fed2.downstream">>, ?EXPECT_FEDERATION_TIMEOUT),

              %% clear the policy
              rabbit_ct_broker_helpers:clear_policy(Config, 0, <<"fed">>),

              expect_no_federation(Ch, <<"upstream">>, <<"fed1.downstream">>),
              expect_no_federation(Ch, <<"upstream2">>, <<"fed2.downstream">>),

              rabbit_ct_broker_helpers:set_policy(Config, 0,
                <<"fed">>, <<"^fed1\.">>, <<"all">>, [
                {<<"federation-upstream-set">>, <<"upstream">>}])
      end, upstream_downstream(Config) ++ [q(<<"fed2.downstream">>, Args)]).

dynamic_plugin_stop_start(Config) ->
    DownQ2 = <<"fed2.downstream">>,
    Args = ?config(target_queue_args, Config),
    with_ch(Config,
      fun (Ch) ->
          UpQ1 = <<"upstream">>,
          UpQ2 = <<"upstream2">>,
          DownQ1 = <<"fed1.downstream">>,
          await_running_federation(Config,
            [{DownQ1, UpQ1}, {DownQ2, UpQ2}],
            ?EXPECT_FEDERATION_TIMEOUT),
          expect_federation(Ch, UpQ1, DownQ1, ?EXPECT_FEDERATION_TIMEOUT),
          expect_federation(Ch, UpQ2, DownQ2, ?EXPECT_FEDERATION_TIMEOUT),

          %% Disable the plugin, the link disappears
          ct:pal("Stopping rabbitmq_federation"),
          ok = rabbit_ct_broker_helpers:disable_plugin(Config, 0, "rabbitmq_queue_federation"),

          expect_no_federation(Ch, UpQ1, DownQ1),
          expect_no_federation(Ch, UpQ2, DownQ2),

          maybe_declare_queue(Config, Ch, q(DownQ1, Args)),
          maybe_declare_queue(Config, Ch, q(DownQ2, Args)),
          ct:pal("Re-starting rabbitmq_federation"),
          ok = rabbit_ct_broker_helpers:enable_plugin(Config, 0, "rabbitmq_queue_federation"),

          await_running_federation(Config,
            [{DownQ1, UpQ1}, {DownQ2, UpQ2}],
            30000),
          expect_federation(Ch, UpQ1, DownQ1, ?EXPECT_FEDERATION_TIMEOUT)
      end, upstream_downstream(Config) ++ [q(DownQ2, Args)]).

%% Stops the federation supervisor concurrently with runtime parameter
%% changes and queue deletion.
supervisor_shutdown_concurrency_safety(Config) ->
    DownQ2 = <<"fed1.downstream2">>,
    Args = ?config(target_queue_args, Config),
    with_ch(Config,
      fun (Ch) ->
          UpQ = <<"upstream">>,
          DownQ = <<"fed1.downstream">>,
          maybe_declare_queue(Config, Ch, q(DownQ2, Args)),

          %% Wait for both federation links to be established before proceeding.
          rabbit_ct_helpers:await_condition(
            fun() ->
                    Status = rabbit_ct_broker_helpers:rpc(Config, 0,
                               rabbit_federation_status, status, []),
                    L = [Entry || Entry <- Status,
                         proplists:get_value(queue, Entry) =:= DownQ orelse
                             proplists:get_value(queue, Entry) =:= DownQ2,
                         proplists:get_value(upstream_queue, Entry) =:= UpQ,
                         proplists:get_value(status, Entry) =:= running],
                    length(L) =:= 2
            end, ?EXPECT_FEDERATION_TIMEOUT),

          expect_federation(Ch, UpQ, DownQ, ?EXPECT_FEDERATION_TIMEOUT),
          expect_federation(Ch, UpQ, DownQ2, ?EXPECT_FEDERATION_TIMEOUT),

          %% Stop the federation supervisor directly (simulating shutdown)
          ct:pal("Stopping federation supervisor to simulate shutdown race"),
          ok = rabbit_ct_broker_helpers:rpc(Config, 0,
                 rabbit_queue_federation_sup, stop, []),

          %% Now trigger operations that would normally crash without the fix.
          %% These should return ok, not crash with {noproc, _}

          %% Test adjust/1 - this is called when parameters change
          ct:pal("Calling adjust/1 after supervisor stopped"),
          ok = rabbit_ct_broker_helpers:rpc(Config, 0,
                 rabbit_federation_queue_link_sup_sup, adjust, [everything]),
          ok = rabbit_ct_broker_helpers:rpc(Config, 0,
                 rabbit_federation_queue_link_sup_sup, adjust,
                 [{clear_upstream, <<"/">>, <<"test-upstream">>}]),

          %% Test stop_child/1 by deleting a federated queue while the supervisor is down.
          %% This triggers the decorator's shutdown callback, which calls stop_child.
          ct:pal("Deleting federated queue after supervisor stopped"),
          delete_queue(Ch, DownQ2),

          %% Test that the plugin can be cleanly disabled and re-enabled
          %% even after this manual supervisor stop
          ct:pal("Disabling and re-enabling plugin"),
          ok = rabbit_ct_broker_helpers:disable_plugin(Config, 0, "rabbitmq_queue_federation"),
          ok = rabbit_ct_broker_helpers:enable_plugin(Config, 0, "rabbitmq_queue_federation"),

          await_running_federation(Config,
            [{DownQ, UpQ}],
            30000),

          expect_federation(Ch, UpQ, DownQ, ?EXPECT_FEDERATION_TIMEOUT)
      end, upstream_downstream(Config)).
restart_upstream(Config) ->
    [Rabbit, Hare] = rabbit_ct_broker_helpers:get_node_configs(Config,
      nodename),
    set_policy_upstream(Config, Rabbit, <<"^test$">>,
      rabbit_ct_broker_helpers:node_uri(Config, Hare), []),

    Downstream = rabbit_ct_client_helpers:open_channel(Config, Rabbit),
    Upstream   = rabbit_ct_client_helpers:open_channel(Config, Hare),

    SourceArgs = ?config(source_queue_args, Config),
    TargetArgs = ?config(target_queue_args, Config),
    maybe_declare_queue(Config, Upstream, q(<<"test">>, SourceArgs)),
    maybe_declare_queue(Config, Downstream, q(<<"test">>, TargetArgs)),

    await_running_federation(Config,
      [{<<"test">>, <<"test">>}],
      ?EXPECT_FEDERATION_TIMEOUT),

    Seq = lists:seq(1, 40),
    [publish(Upstream, <<>>, <<"test">>, <<"bulk">>) || _ <- Seq],
    expect(Upstream, <<"test">>, repeat(10, <<"bulk">>)),
    expect(Downstream, <<"test">>, repeat(10, <<"bulk">>)),

    rabbit_ct_client_helpers:close_channels_and_connection(Config, Hare),
    ok = rabbit_ct_broker_helpers:restart_node(Config, Hare),
    Upstream2 = rabbit_ct_client_helpers:open_channel(Config, Hare),

    expect(Upstream2, <<"test">>, repeat(10, <<"bulk">>)),
    expect(Downstream, <<"test">>, repeat(10, <<"bulk">>)),
    expect_empty(Upstream2, <<"test">>),
    expect_empty(Downstream, <<"test">>),

    ok.

%upstream_has_no_federation(Config) ->
%    %% TODO
%    ok.

%%----------------------------------------------------------------------------
repeat(Count, Item) -> [Item || _ <- lists:seq(1, Count)].

%%----------------------------------------------------------------------------

publish(Ch, X, Key, Payload) when is_binary(Payload) ->
    publish(Ch, X, Key, #amqp_msg{payload = Payload});

publish(Ch, X, Key, Msg = #amqp_msg{}) ->
    amqp_channel:call(Ch, #'basic.publish'{exchange    = X,
                                           routing_key = Key}, Msg).

publish_expect(Ch, X, Key, Q, Payload) ->
    publish(Ch, X, Key, Payload),
    expect(Ch, Q, [Payload]).

publish_expect(Ch, X, Key, Q, Payload, Timeout) ->
    publish(Ch, X, Key, Payload),
    expect(Ch, Q, [Payload], Timeout).

%% Doubled due to our strange basic.get behaviour.
expect_empty(Ch, Q) ->
    queue_federation_test_helpers:expect_empty(Ch, Q),
    queue_federation_test_helpers:expect_empty(Ch, Q).

expect_federation(Ch, UpstreamQ, DownstreamQ) ->
    Base = <<"HELLO">>,
    Payload = <<Base/binary, "-to-", UpstreamQ/binary>>,
    publish_expect(Ch, <<>>, UpstreamQ, DownstreamQ, Payload).

expect_federation(Ch, UpstreamQ, DownstreamQ, Timeout) ->
    Base = <<"HELLO">>,
    Payload = <<Base/binary, "-to-", UpstreamQ/binary>>,
    publish_expect(Ch, <<>>, UpstreamQ, DownstreamQ, Payload, Timeout).

expect_no_federation(Ch, UpstreamQ, DownstreamQ) ->
    publish(Ch, <<>>, UpstreamQ, <<"HELLO">>),
    expect_empty(Ch, DownstreamQ),
    expect(Ch, UpstreamQ, [<<"HELLO">>]).

upstream_downstream() ->
    upstream_downstream([]).

upstream_downstream(Config) ->
    SourceArgs = ?config(source_queue_args, Config),
    TargetArgs = ?config(target_queue_args, Config),
    [q(<<"upstream">>, SourceArgs), q(<<"fed1.downstream">>, TargetArgs)].
