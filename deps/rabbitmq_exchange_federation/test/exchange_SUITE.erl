%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(exchange_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-include("rabbit_exchange_federation.hrl").

-compile(export_all).

-import(exchange_federation_test_helpers,
        [expect/3, expect/4, expect_empty/2, await_running_federation/3,
         setup_federation_with_upstream_params/2]).

all() ->
    [
      {group, essential},
      {group, cluster_size_3},
      {group, rolling_upgrade},
      {group, maintenance_mode}
    ].

groups() ->
    [
     {essential, [], essential()},
     {cluster_size_3, [], [max_hops]},
     {rolling_upgrade, [], [child_id_format]},
     {cycle_protection, [], [
                             %% TBD: port from v3.10.x in an Erlang 25-compatible way
                            ]},
     {channel_use_mod_single, [], [
                                   %% TBD: port from v3.10.x in an Erlang 25-compatible way
                                  ]},
     {maintenance_mode, [], [
                             maintenance_mode_disconnect_reconnect
                            ]}
    ].

essential() ->
    [
      single_upstream,
      single_upstream_quorum,
      multiple_upstreams,
      multiple_upstreams_pattern,
      single_upstream_multiple_uris,
      multiple_downstreams,
      e2e_binding,
      unbind_on_delete,
      unbind_on_client_unbind,
      exchange_federation_link_status,
      lookup_exchange_status,
      supervisor_shutdown_concurrency_safety
    ].

suite() ->
    [{timetrap, {minutes, 2}}].

%% -------------------------------------------------------------------
%% Setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
  rabbit_ct_helpers:log_environment(),
  rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
  rabbit_ct_helpers:run_teardown_steps(Config).

%% Some of the "regular" tests but in the single channel mode.
init_per_group(essential, Config) ->
  SetupFederation = [
      fun(Config1) ->
          setup_federation_with_upstream_params(Config1, [
              {<<"channel-use-mode">>, <<"single">>}
          ])
      end
  ],
  Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
  Config1 = rabbit_ct_helpers:set_config(Config, [
      {rmq_nodename_suffix, Suffix},
      {rmq_nodes_count, 1}
    ]),
  rabbit_ct_helpers:run_steps(Config1,
    rabbit_ct_broker_helpers:setup_steps() ++
    rabbit_ct_client_helpers:setup_steps() ++
    SetupFederation);
init_per_group(cluster_size_3 = Group, Config) ->
  Config1 = rabbit_ct_helpers:set_config(Config, [
      {rmq_nodes_count, 3}
    ]),
  init_per_group1(Group, Config1);
init_per_group(rolling_upgrade = Group, Config) ->
  Config1 = rabbit_ct_helpers:set_config(Config, [
      {rmq_nodes_count, 5},
      {rmq_nodes_clustered, false}
    ]),
  init_per_group1(Group, Config1);
init_per_group(maintenance_mode, Config) ->
  Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
  Config1 = rabbit_ct_helpers:set_config(Config, [
      {rmq_nodename_suffix, Suffix},
      {rmq_nodes_count, 1},
      %% When a node is put into maintenance mode, its connections are forcibly closed which causes
      %% Erlang AMQP 0-9-1 client connection processes to terminate abruptly.
      %% Ignore such crashes in the logs.
      {ignored_crashes, ["socket_closed"]}
    ]),
  rabbit_ct_helpers:run_steps(Config1,
    rabbit_ct_broker_helpers:setup_steps() ++
    rabbit_ct_client_helpers:setup_steps());
init_per_group(Group, Config) ->
  init_per_group1(Group, Config).


init_per_group1(_Group, Config) ->
  Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
  Config1 = rabbit_ct_helpers:set_config(Config, [
      {rmq_nodename_suffix, Suffix},
      {rmq_nodes_clustered, false}
    ]),
  rabbit_ct_helpers:run_steps(Config1,
    rabbit_ct_broker_helpers:setup_steps() ++
    rabbit_ct_client_helpers:setup_steps()).

end_per_group(_, Config) ->
  rabbit_ct_helpers:run_steps(Config,
    rabbit_ct_client_helpers:teardown_steps() ++
    rabbit_ct_broker_helpers:teardown_steps()
  ).

init_per_testcase(Testcase, Config) ->
  rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
  rabbit_ct_helpers:testcase_finished(Config, Testcase).


%%
%% Test cases
%%

single_upstream(Config) ->
  FedX = <<"single_upstream.federated">>,
  UpX = <<"single_upstream.upstream.x">>,
  rabbit_ct_broker_helpers:set_parameter(
    Config, 0, <<"federation-upstream">>, <<"localhost">>,
    [
      {<<"uri">>,      rabbit_ct_broker_helpers:node_uri(Config, 0)},
      {<<"exchange">>, UpX}
    ]),
  rabbit_ct_broker_helpers:set_policy(
    Config, 0,
    <<"fed.x">>, <<"^single_upstream.federated">>, <<"exchanges">>,
    [
      {<<"federation-upstream">>, <<"localhost">>}
    ]),

  Ch = rabbit_ct_client_helpers:open_channel(Config, 0),

  Xs = [
    exchange_declare_method(FedX)
  ],
  declare_exchanges(Ch, Xs),

  RK = <<"key">>,
  Q = declare_and_bind_queue(Ch, FedX, RK),
  await_binding(Config, 0, UpX, RK),
  publish_expect(Ch, UpX, RK, Q, <<"single_upstream payload">>),

  Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
  assert_federation_internal_queue_type(Config, Server, rabbit_classic_queue),

  rabbit_ct_client_helpers:close_channel(Ch),
  clean_up_federation_related_bits(Config).

single_upstream_quorum(Config) ->
  FedX = <<"single_upstream_quorum.federated">>,
  UpX = <<"single_upstream_quorum.upstream.x">>,
  rabbit_ct_broker_helpers:set_parameter(
    Config, 0, <<"federation-upstream">>, <<"localhost">>,
    [
      {<<"uri">>,      rabbit_ct_broker_helpers:node_uri(Config, 0)},
      {<<"exchange">>, UpX},
      {<<"queue-type">>, <<"quorum">>}
    ]),
  rabbit_ct_broker_helpers:set_policy(
    Config, 0,
    <<"fed.x">>, <<"^single_upstream_quorum.federated">>, <<"exchanges">>,
    [
      {<<"federation-upstream">>, <<"localhost">>}
    ]),

  Ch = rabbit_ct_client_helpers:open_channel(Config, 0),

  Xs = [
    exchange_declare_method(FedX)
  ],
  declare_exchanges(Ch, Xs),

  RK = <<"key">>,
  Q = declare_and_bind_queue(Ch, FedX, RK),
  await_binding(Config, 0, UpX, RK),
  publish_expect(Ch, UpX, RK, Q, <<"single_upstream_quorum payload">>),

  Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
  assert_federation_internal_queue_type(Config, Server, rabbit_quorum_queue),

  rabbit_ct_client_helpers:close_channel(Ch),
  clean_up_federation_related_bits(Config).

multiple_upstreams(Config) ->
  FedX = <<"multiple_upstreams.federated">>,
  UpX1 = <<"upstream.x.1">>,
  UpX2 = <<"upstream.x.2">>,
  set_up_upstreams(Config),
  rabbit_ct_broker_helpers:set_policy(
    Config, 0,
    <<"fed.x">>, <<"^multiple_upstreams.federated">>, <<"exchanges">>,
    [
      {<<"federation-upstream-set">>, <<"all">>}
    ]),

  Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
  Xs = [
    exchange_declare_method(FedX)
  ],
  declare_exchanges(Ch, Xs),

  RK = <<"multiple_upstreams.key">>,
  Q = declare_and_bind_queue(Ch, FedX, RK),
  await_binding(Config, 0, UpX1, RK),
  await_binding(Config, 0, UpX2, RK),
  publish_expect(Ch, UpX1, RK, Q, <<"multiple_upstreams payload">>),
  publish_expect(Ch, UpX2, RK, Q, <<"multiple_upstreams payload">>),

  rabbit_ct_client_helpers:close_channel(Ch),
  clean_up_federation_related_bits(Config).


multiple_upstreams_pattern(Config) ->
  FedX = <<"multiple_upstreams_pattern.federated">>,
  UpX1 = <<"upstream.x.1">>,
  UpX2 = <<"upstream.x.2">>,
  set_up_upstreams(Config),
  rabbit_ct_broker_helpers:set_policy(
    Config, 0,
    <<"fed.x">>, <<"^multiple_upstreams_pattern.federated">>, <<"exchanges">>,
    [
      {<<"federation-upstream-pattern">>, <<"^localhost">>}
    ]),

  Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
  Xs = [
    exchange_declare_method(FedX)
  ],
  declare_exchanges(Ch, Xs),

  RK = <<"multiple_upstreams_pattern.key">>,
  Q = declare_and_bind_queue(Ch, FedX, RK),
  await_binding(Config, 0, UpX1, RK),
  await_binding(Config, 0, UpX2, RK),
  publish_expect(Ch, UpX1, RK, Q, <<"multiple_upstreams_pattern payload">>),
  publish_expect(Ch, UpX2, RK, Q, <<"multiple_upstreams_pattern payload">>),

  rabbit_ct_client_helpers:close_channel(Ch),
  clean_up_federation_related_bits(Config).


single_upstream_multiple_uris(Config) ->
  FedX = <<"single_upstream_multiple_uris.federated">>,
  UpX = <<"single_upstream_multiple_uris.upstream.x">>,
  URIs = [
    rabbit_ct_broker_helpers:node_uri(Config, 0),
    rabbit_ct_broker_helpers:node_uri(Config, 0, [use_ipaddr])
  ],
  rabbit_ct_broker_helpers:set_parameter(
    Config, 0, <<"federation-upstream">>, <<"localhost">>,
    [
      {<<"uri">>,      URIs},
      {<<"exchange">>, UpX}
    ]),
  rabbit_ct_broker_helpers:set_policy(
    Config, 0,
    <<"fed.x">>, <<"^single_upstream_multiple_uris.federated">>, <<"exchanges">>,
    [
      {<<"federation-upstream">>, <<"localhost">>}
    ]),

  Ch = rabbit_ct_client_helpers:open_channel(Config, 0),

  Xs = [
    exchange_declare_method(FedX)
  ],
  declare_exchanges(Ch, Xs),

  RK = <<"key">>,
  Q = declare_and_bind_queue(Ch, FedX, RK),
  await_binding(Config, 0, UpX, RK),
  publish_expect(Ch, UpX, RK, Q, <<"single_upstream_multiple_uris payload">>),

  rabbit_ct_client_helpers:close_channel(Ch),
  clean_up_federation_related_bits(Config).

multiple_downstreams(Config) ->
  FedX = <<"multiple_downstreams.federated">>,
  UpX = <<"multiple_downstreams.upstream.x">>,
  rabbit_ct_broker_helpers:set_parameter(
    Config, 0, <<"federation-upstream">>, <<"localhost">>,
    [
      {<<"uri">>,      rabbit_ct_broker_helpers:node_uri(Config, 0)},
      {<<"exchange">>, UpX}
    ]),
  rabbit_ct_broker_helpers:set_policy(
    Config, 0,
    <<"fed.x">>, <<"^multiple_downstreams.federated">>, <<"exchanges">>,
    [
      {<<"federation-upstream">>, <<"localhost">>}
    ]),

  Ch = rabbit_ct_client_helpers:open_channel(Config, 0),

  Xs = [
    exchange_declare_method(FedX)
  ],
  declare_exchanges(Ch, Xs),

  RK = <<"key">>,
  Q1 = declare_and_bind_queue(Ch, FedX, RK),
  _ = declare_and_bind_queue(Ch, FedX, RK),
  await_binding(Config, 0, UpX, RK),
  publish(Ch, UpX, RK, <<"multiple_downstreams payload 1">>),
  publish(Ch, UpX, RK, <<"multiple_downstreams payload 2">>),
  expect(Ch, Q1, [<<"multiple_downstreams payload 1">>]),
  expect(Ch, Q1, [<<"multiple_downstreams payload 2">>]),

  rabbit_ct_client_helpers:close_channel(Ch),
  clean_up_federation_related_bits(Config).

e2e_binding(Config) ->
  FedX = <<"e2e_binding.federated">>,
  E2EX = <<"e2e_binding.e2e">>,
  UpX = <<"e2e_binding.upstream.x">>,
  rabbit_ct_broker_helpers:set_parameter(
    Config, 0, <<"federation-upstream">>, <<"localhost">>,
    [
      {<<"uri">>,      rabbit_ct_broker_helpers:node_uri(Config, 0)},
      {<<"exchange">>, UpX}
    ]),
  rabbit_ct_broker_helpers:set_policy(
    Config, 0,
    <<"fed.x">>, <<"^e2e_binding.federated">>, <<"exchanges">>,
    [
      {<<"federation-upstream">>, <<"localhost">>}
    ]),

  Ch = rabbit_ct_client_helpers:open_channel(Config, 0),

  Xs = [
    exchange_declare_method(FedX, <<"fanout">>),
    exchange_declare_method(E2EX, <<"fanout">>)
  ],
  declare_exchanges(Ch, Xs),
  Key = <<"key">>,
  %% federated exchange routes to the E2E fanout
  bind_exchange(Ch, E2EX, FedX, Key),

  RK = <<"key">>,
  Q = declare_and_bind_queue(Ch, E2EX, RK),
  await_binding(Config, 0, UpX, RK),
  publish_expect(Ch, UpX, RK, Q, <<"e2e_binding payload">>),

  rabbit_ct_client_helpers:close_channel(Ch),
  clean_up_federation_related_bits(Config).

unbind_on_delete(Config) ->
  FedX = <<"unbind_on_delete.federated">>,
  UpX = <<"unbind_on_delete.upstream.x">>,
  rabbit_ct_broker_helpers:set_parameter(
    Config, 0, <<"federation-upstream">>, <<"localhost">>,
    [
      {<<"uri">>,      rabbit_ct_broker_helpers:node_uri(Config, 0)},
      {<<"exchange">>, UpX}
    ]),
  rabbit_ct_broker_helpers:set_policy(
    Config, 0,
    <<"fed.x">>, <<"^unbind_on_delete.federated">>, <<"exchanges">>,
    [
      {<<"federation-upstream">>, <<"localhost">>}
    ]),

  Ch = rabbit_ct_client_helpers:open_channel(Config, 0),

  Xs = [
    exchange_declare_method(FedX)
  ],
  declare_exchanges(Ch, Xs),

  RK = <<"key">>,
  Q1 = declare_and_bind_queue(Ch, FedX, RK),
  Q2 = declare_and_bind_queue(Ch, FedX, RK),
  await_binding(Config, 0, UpX, RK),
  delete_queue(Ch, Q2),
  publish_expect(Ch, UpX, RK, Q1, <<"unbind_on_delete payload">>),

  rabbit_ct_client_helpers:close_channel(Ch),
  clean_up_federation_related_bits(Config).

unbind_on_client_unbind(Config) ->
  FedX = <<"unbind_on_client_unbind.federated">>,
  UpX = <<"unbind_on_client_unbind.upstream.x">>,
  rabbit_ct_broker_helpers:set_parameter(
    Config, 0, <<"federation-upstream">>, <<"localhost">>,
    [
      {<<"uri">>,      rabbit_ct_broker_helpers:node_uri(Config, 0)},
      {<<"exchange">>, UpX}
    ]),
  rabbit_ct_broker_helpers:set_policy(
    Config, 0,
    <<"fed.x">>, <<"^unbind_on_client_unbind.federated">>, <<"exchanges">>,
    [
      {<<"federation-upstream">>, <<"localhost">>}
    ]),

  Ch = rabbit_ct_client_helpers:open_channel(Config, 0),

  Xs = [
    exchange_declare_method(FedX)
  ],
  declare_exchanges(Ch, Xs),

  RK = <<"key">>,
  Q1 = declare_and_bind_queue(Ch, FedX, RK),
  Q2 = declare_and_bind_queue(Ch, FedX, RK),
  await_binding(Config, 0, UpX, RK),
  unbind_queue(Ch, Q2, UpX, RK),
  publish_expect(Ch, UpX, RK, Q1, <<"unbind_on_delete payload">>),

  rabbit_ct_client_helpers:close_channel(Ch),
  clean_up_federation_related_bits(Config).

max_hops(Config) ->
  case rabbit_ct_helpers:is_mixed_versions() of
    false ->
      [NodeA, NodeB, NodeC] = rabbit_ct_broker_helpers:get_node_configs(
        Config, nodename),
      await_credentials_obfuscation_seeding_on_two_nodes(Config),

      UpX = <<"ring">>,

      %% form of ring of upstreams,
      %% A upstream points at B
      rabbit_ct_broker_helpers:set_parameter(
        Config, NodeA, <<"federation-upstream">>, <<"upstream">>,
        [
          {<<"uri">>,      rabbit_ct_broker_helpers:node_uri(Config, NodeB)},
          {<<"exchange">>, UpX},
          {<<"max-hops">>, 2}
        ]),
      %% B upstream points at C
      rabbit_ct_broker_helpers:set_parameter(
        Config, NodeB, <<"federation-upstream">>, <<"upstream">>,
        [
          {<<"uri">>,      rabbit_ct_broker_helpers:node_uri(Config, NodeC)},
          {<<"exchange">>, UpX},
          {<<"max-hops">>, 2}
        ]),
      %% C upstream points at A
      rabbit_ct_broker_helpers:set_parameter(
        Config, NodeC, <<"federation-upstream">>, <<"upstream">>,
        [
          {<<"uri">>,      rabbit_ct_broker_helpers:node_uri(Config, NodeA)},
          {<<"exchange">>, UpX},
          {<<"max-hops">>, 2}
        ]),

      %% policy on A
      [begin
        rabbit_ct_broker_helpers:set_policy(
          Config, Node,
          <<"fed.x">>, <<"^ring">>, <<"exchanges">>,
          [
            {<<"federation-upstream">>, <<"upstream">>}
          ])
       end || Node <- [NodeA, NodeB, NodeC]],

      NodeACh = rabbit_ct_client_helpers:open_channel(Config, NodeA),
      NodeBCh = rabbit_ct_client_helpers:open_channel(Config, NodeB),
      NodeCCh = rabbit_ct_client_helpers:open_channel(Config, NodeC),

      FedX = <<"ring">>,
      X = exchange_declare_method(FedX),
      declare_exchange(NodeACh, X),
      declare_exchange(NodeBCh, X),
      declare_exchange(NodeCCh, X),

      Q1 = declare_and_bind_queue(NodeACh, <<"ring">>, <<"key">>),
      Q2 = declare_and_bind_queue(NodeBCh, <<"ring">>, <<"key">>),
      Q3 = declare_and_bind_queue(NodeCCh, <<"ring">>, <<"key">>),

      await_binding(Config, NodeA, <<"ring">>, <<"key">>, 3),
      await_binding(Config, NodeB, <<"ring">>, <<"key">>, 3),
      await_binding(Config, NodeC, <<"ring">>, <<"key">>, 3),

      publish(NodeACh, <<"ring">>, <<"key">>, <<"HELLO flopsy">>),
      publish(NodeBCh, <<"ring">>, <<"key">>, <<"HELLO mopsy">>),
      publish(NodeCCh, <<"ring">>, <<"key">>, <<"HELLO cottontail">>),

      Msgs = [<<"HELLO flopsy">>, <<"HELLO mopsy">>, <<"HELLO cottontail">>],
      expect(NodeACh, Q1, Msgs),
      expect(NodeBCh, Q2, Msgs),
      expect(NodeCCh, Q3, Msgs),
      expect_empty(NodeACh, Q1),
      expect_empty(NodeBCh, Q2),
      expect_empty(NodeCCh, Q3),

      clean_up_federation_related_bits(Config);
    true ->
      %% skip the test in mixed version mode
      {skip, "Should not run in mixed version environments"}
  end.

exchange_federation_link_status(Config) ->
  FedX = <<"single_upstream.federated">>,
  UpX = <<"single_upstream.upstream.x">>,
  rabbit_ct_broker_helpers:set_parameter(
    Config, 0, <<"federation-upstream">>, <<"localhost">>,
    [
      {<<"uri">>,      rabbit_ct_broker_helpers:node_uri(Config, 0)},
      {<<"exchange">>, UpX}
    ]),
  rabbit_ct_broker_helpers:set_policy(
    Config, 0,
    <<"fed.x">>, <<"^single_upstream.federated">>, <<"exchanges">>,
    [
      {<<"federation-upstream">>, <<"localhost">>}
    ]),

  Ch = rabbit_ct_client_helpers:open_channel(Config, 0),

  Xs = [
    exchange_declare_method(FedX)
  ],
  declare_exchanges(Ch, Xs),

  RK = <<"key">>,
  _ = declare_and_bind_queue(Ch, FedX, RK),
  await_binding(Config, 0, UpX, RK),

  [Link] = rabbit_ct_broker_helpers:rpc(Config, 0,
                                        rabbit_federation_status, status,
                                        []),
  true = is_binary(proplists:get_value(id, Link)),

  clean_up_federation_related_bits(Config).

lookup_exchange_status(Config) ->
  FedX = <<"single_upstream.federated">>,
  UpX = <<"single_upstream.upstream.x">>,
  rabbit_ct_broker_helpers:set_parameter(
    Config, 0, <<"federation-upstream">>, <<"localhost">>,
    [
      {<<"uri">>,      rabbit_ct_broker_helpers:node_uri(Config, 0)},
      {<<"exchange">>, UpX}
    ]),
  rabbit_ct_broker_helpers:set_policy(
    Config, 0,
    <<"fed.x">>, <<"^single_upstream.federated">>, <<"exchanges">>,
    [
      {<<"federation-upstream">>, <<"localhost">>}
    ]),

  Ch = rabbit_ct_client_helpers:open_channel(Config, 0),

  Xs = [
    exchange_declare_method(FedX)
  ],
  declare_exchanges(Ch, Xs),

  RK = <<"key">>,
  _ = declare_and_bind_queue(Ch, FedX, RK),
  await_binding(Config, 0, UpX, RK),

  [Link] = rabbit_ct_broker_helpers:rpc(Config, 0,
  rabbit_federation_status, status, []),
  Id = proplists:get_value(id, Link),
  Props = rabbit_ct_broker_helpers:rpc(Config, 0,
    rabbit_federation_status, lookup, [Id]),
  lists:all(fun(K) -> lists:keymember(K, 1, Props) end,
            [key, uri, status, timestamp, id, supervisor, upstream]),

  clean_up_federation_related_bits(Config).

%% Stops the federation supervisor concurrently with runtime parameter
%% changes and exchange deletion.
supervisor_shutdown_concurrency_safety(Config) ->
  FedX = <<"shutdown_race.federated">>,
  FedX2 = <<"shutdown_race.federated2">>,
  UpX = <<"shutdown_race.upstream.x">>,
  rabbit_ct_broker_helpers:set_parameter(
    Config, 0, <<"federation-upstream">>, <<"localhost">>,
    [
      {<<"uri">>,      rabbit_ct_broker_helpers:node_uri(Config, 0)},
      {<<"exchange">>, UpX}
    ]),
  rabbit_ct_broker_helpers:set_policy(
    Config, 0,
    <<"fed.x">>, <<"^shutdown_race.federated">>, <<"exchanges">>,
    [
      {<<"federation-upstream">>, <<"localhost">>}
    ]),

  Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
  Xs = [exchange_declare_method(FedX), exchange_declare_method(FedX2)],
  declare_exchanges(Ch, Xs),

  RK = <<"key">>,
  Q = declare_and_bind_queue(Ch, FedX, RK),
  _ = declare_and_bind_queue(Ch, FedX2, RK),
  await_binding(Config, 0, UpX, RK),

  %% Verify federation is working
  publish_expect(Ch, UpX, RK, Q, <<"before_shutdown">>),

  %% Stop the federation supervisor directly (simulating shutdown)
  ct:pal("Stopping federation supervisor to simulate shutdown race"),
  ok = rabbit_ct_broker_helpers:rpc(Config, 0,
         rabbit_exchange_federation_sup, stop, []),

  %% Now trigger operations that would normally crash without the fix.
  %% These should return ok, not crash with {noproc, _}

  %% Test adjust/1 - this is called when parameters change
  ct:pal("Calling adjust/1 after supervisor stopped"),
  ok = rabbit_ct_broker_helpers:rpc(Config, 0,
         rabbit_federation_exchange_link_sup_sup, adjust, [everything]),
  ok = rabbit_ct_broker_helpers:rpc(Config, 0,
         rabbit_federation_exchange_link_sup_sup, adjust,
         [{clear_upstream, <<"/">>, <<"test-upstream">>}]),

  %% Test stop_child/1 by deleting a federated exchange while the supervisor is down.
  %% This triggers the decorator's delete callback, which calls stop_child.
  ct:pal("Deleting federated exchange after supervisor stopped"),
  delete_exchange(Ch, FedX2),

  %% Test that the plugin can be cleanly disabled and re-enabled
  %% even after this manual supervisor stop
  ct:pal("Disabling and re-enabling plugin"),
  ok = rabbit_ct_broker_helpers:disable_plugin(Config, 0, "rabbitmq_exchange_federation"),
  ok = rabbit_ct_broker_helpers:enable_plugin(Config, 0, "rabbitmq_exchange_federation"),

  await_running_federation(Config,
    [{FedX, UpX}],
    30000),

  %% Verify federation still works after recovery
  await_binding(Config, 0, UpX, RK),
  publish_expect(Ch, UpX, RK, Q, <<"after_recovery">>),

  clean_up_federation_related_bits(Config).

child_id_format(Config) ->
  case rabbit_ct_helpers:is_mixed_versions() of
    true ->
          [UpstreamNode,
           OldNodeA,
           NewNodeB,
           OldNodeC,
           NewNodeD] = rabbit_ct_broker_helpers:get_node_configs(
                         Config, nodename),

          %% Create a cluster with the nodes running the old version of RabbitMQ in
          %% mixed-version testing.
          %%
          %% Note: we build this on the assumption that `rabbit_ct_broker_helpers'
          %% starts nodes this way:
          %%   Node 1: the primary copy of RabbitMQ the test is started from
          %%   Node 2: the secondary umbrella (if any)
          %%   Node 3: the primary copy
          %%   Node 4: the secondary umbrella
          %%   ...
          %%
          %% Therefore, `UpstreamNode' will use the primary copy, `OldNodeA' the
          %% secondary umbrella, `NewNodeB' the primary copy, and so on.
          Config1 = rabbit_ct_broker_helpers:cluster_nodes(
                      Config, [OldNodeA, OldNodeC]),

          %% The old nodes get the "rabbitmq_exchange_federation" plugin enabled but that is not found
          %% Let's switch to the old plugin name
          [rabbit_ct_broker_helpers:set_plugins(Config, Node, ["rabbitmq_federation"])
           || Node <- [OldNodeA, OldNodeC]],

          %% Prepare the whole federated exchange on that old cluster.
          UpstreamName = <<"fed_on_upgrade">>,
          rabbit_ct_broker_helpers:set_parameter(
            Config1, OldNodeA, <<"federation-upstream">>, UpstreamName,
            [
             {<<"uri">>, rabbit_ct_broker_helpers:node_uri(Config1, UpstreamNode)}
            ]),

          rabbit_ct_broker_helpers:set_policy(
            Config1, OldNodeA,
            <<"fed_on_upgrade_policy">>, <<"^fed_">>, <<"all">>,
            [
             {<<"federation-upstream-pattern">>, UpstreamName}
            ]),

          XName = <<"fed_ex_on_upgrade_cluster">>,
          X = exchange_declare_method(XName, <<"direct">>),
          {Conn1, Ch1} = rabbit_ct_client_helpers:open_connection_and_channel(
                           Config1, OldNodeA),
          ?assertEqual({'exchange.declare_ok'}, declare_exchange(Ch1, X)),
          rabbit_ct_client_helpers:close_channel(Ch1),
          rabbit_ct_client_helpers:close_connection(Conn1),

          %% Verify the format of the child ID. In the main branch, the format was
          %% temporarily a size-2 tuple with a list as the first element. This was
          %% not kept later and the original ID format is used in old and new nodes.
          [{Id, _, _, _}] = rabbit_ct_broker_helpers:rpc(
                              Config1, OldNodeA,
                              mirrored_supervisor, which_children,
                              [rabbit_federation_exchange_link_sup_sup]),
          case Id of
              %% This is the format we expect everywhere.
              #exchange{name = #resource{name = XName}} ->
                  %% Verify that the supervisors exist on all nodes.
                  lists:foreach(
                    fun(Node) ->
                            ?assertMatch(
                               [{#exchange{name = #resource{name = XName}},
                                 _, _, _}],
                               rabbit_ct_broker_helpers:rpc(
                                 Config1, Node,
                                 mirrored_supervisor, which_children,
                                 [rabbit_federation_exchange_link_sup_sup]))
                    end, [OldNodeA, OldNodeC]),

                  %% Simulate a rolling upgrade by:
                  %% 1. adding new nodes to the old cluster
                  %% 2. stopping the old nodes
                  %%
                  %% After that, the supervisors run on the new code.
                  Config2 = rabbit_ct_broker_helpers:cluster_nodes(
                              Config1, OldNodeA, [NewNodeB, NewNodeD]),
                  ok = rabbit_ct_broker_helpers:stop_broker(Config2, OldNodeA),
                  ok = rabbit_ct_broker_helpers:reset_node(Config1, OldNodeA),
                  ok = rabbit_ct_broker_helpers:stop_broker(Config2, OldNodeC),
                  ok = rabbit_ct_broker_helpers:reset_node(Config2, OldNodeC),

                  %% Verify that the supervisors still use the same IDs.
                  lists:foreach(
                    fun(Node) ->
                            ?assertMatch(
                               [{#exchange{name = #resource{name = XName}},
                                 _, _, _}],
                               rabbit_ct_broker_helpers:rpc(
                                 Config2, Node,
                                 mirrored_supervisor, which_children,
                                 [rabbit_federation_exchange_link_sup_sup]))
                    end, [NewNodeB, NewNodeD]),

                  %% Delete the exchange: it should work because the ID format is the
                  %% one expected.
                  %%
                  %% During the transient period where the ID format was changed,
                  %% this would crash with a badmatch because the running
                  %% supervisor's ID would not match the content of the database.
                  {Conn2, Ch2} = rabbit_ct_client_helpers:open_connection_and_channel(
                                   Config2, NewNodeB),
                  ?assertEqual({'exchange.delete_ok'}, delete_exchange(Ch2, XName)),
                  rabbit_ct_client_helpers:close_channel(Ch2),
                  rabbit_ct_client_helpers:close_connection(Conn2);

              %% This is the transient format we are not interested in as it only
              %% lived in a development branch.
              {List, #exchange{name = #resource{name = XName}}}
                when is_list(List) ->
                  {skip, "Testcase skipped with the transiently changed ID format"}
          end;
      false ->
          {skip, "Should only run in mixed version environments"}
  end.

maintenance_mode_disconnect_reconnect(Config) ->
  LinkCount = 5,
  Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
  Uri = rabbit_ct_broker_helpers:node_uri(Config, 0),

  Exchanges = [list_to_binary(io_lib:format("maintenance.fed.x.~b", [N]))
               || N <- lists:seq(1, LinkCount)],
  UpstreamExchanges = [list_to_binary(io_lib:format("maintenance.upstream.x.~b", [N]))
                       || N <- lists:seq(1, LinkCount)],

  lists:foreach(
    fun({Idx, UpX}) ->
        Name = list_to_binary(io_lib:format("upstream-~b", [Idx])),
        rabbit_ct_broker_helpers:set_parameter(
          Config, 0, <<"federation-upstream">>, Name,
          [{<<"uri">>, Uri}, {<<"exchange">>, UpX}])
    end,
    lists:zip(lists:seq(1, LinkCount), UpstreamExchanges)),

  rabbit_ct_broker_helpers:set_policy(
    Config, 0,
    <<"maintenance-fed-policy">>, <<"^maintenance\\.fed\\.x\\.">>, <<"exchanges">>,
    [{<<"federation-upstream-pattern">>, <<"upstream-">>}]),

  Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
  [declare_exchange(Ch, exchange_declare_method(X)) || X <- Exchanges],

  rabbit_ct_helpers:await_condition(
    fun() -> count_running_links(Config, Server) >= LinkCount end,
    15000),

  ok = rabbit_ct_broker_helpers:drain_node(Config, Server),

  rabbit_ct_helpers:await_condition(
    fun() -> count_running_links(Config, Server) =:= 0 end,
    10000),

  ok = rabbit_ct_broker_helpers:revive_node(Config, Server),

  rabbit_ct_helpers:await_condition(
    fun() -> count_running_links(Config, Server) >= LinkCount end,
    15000),

  rabbit_ct_client_helpers:close_channel(Ch),
  clean_up_federation_related_bits(Config).

count_running_links(Config, Server) ->
  Status = rabbit_ct_broker_helpers:rpc(Config, Server,
                                        rabbit_federation_status, status, []),
  case Status of
    {badrpc, _} -> 0;
    List ->
      length([S || S <- List,
                   proplists:get_value(status, S) =:= running])
  end.

%%
%% Test helpers
%%

clean_up_federation_related_bits(Config) ->
    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    NodeIndices = lists:seq(0, length(Nodes) - 1),
    [begin
         delete_all_policies_on(Config, N),
         delete_all_runtime_parameters_on(Config, N)
     end || N <- NodeIndices],
    [rabbit_ct_helpers:await_condition(
       fun() ->
               delete_all_queues_on(Config, N),
               all_queues_on(Config, N) =:= []
       end,
       30000) || N <- NodeIndices],
    [delete_all_exchanges_on(Config, N) || N <- NodeIndices],
    ok.

set_up_upstream(Config) ->
  rabbit_ct_broker_helpers:set_parameter(
    Config, 0, <<"federation-upstream">>, <<"localhost">>,
    [
      {<<"uri">>,      rabbit_ct_broker_helpers:node_uri(Config, 0)},
      {<<"exchange">>, <<"upstream">>}
    ]).

set_up_upstreams(Config) ->
  rabbit_ct_broker_helpers:set_parameter(
    Config, 0, <<"federation-upstream">>, <<"localhost1">>,
    [
      {<<"uri">>,      rabbit_ct_broker_helpers:node_uri(Config, 0)},
      {<<"exchange">>, <<"upstream.x.1">>}
    ]),
  rabbit_ct_broker_helpers:set_parameter(
    Config, 0, <<"federation-upstream">>, <<"localhost2">>,
    [
      {<<"uri">>,      rabbit_ct_broker_helpers:node_uri(Config, 0)},
      {<<"exchange">>, <<"upstream.x.2">>}
    ]).

set_up_upstreams_including_unavailable(Config) ->
  rabbit_ct_broker_helpers:set_parameter(
    Config, 0, <<"federation-upstream">>, <<"unavailable-node">>,
    [
      {<<"uri">>, <<"amqp://unavailable-node">>},
      {<<"reconnect-delay">>, 600000}
    ]),

  rabbit_ct_broker_helpers:set_parameter(
    Config, 0, <<"federation-upstream">>, <<"localhost">>,
    [
      {<<"uri">>, rabbit_ct_broker_helpers:node_uri(Config, 0)}
    ]).

declare_exchanges(Ch, Frames) ->
  [declare_exchange(Ch, F) || F <- Frames].
delete_exchanges(Ch, Frames) ->
    [delete_exchange(Ch, X) || #'exchange.declare'{exchange = X} <- Frames].

declare_exchange(Ch, X) ->
    #'exchange.declare_ok'{} = amqp_channel:call(Ch, X).

declare_queue(Ch) ->
  #'queue.declare_ok'{queue = Q} =
      amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
  Q.

declare_queue(Ch, Q) ->
  amqp_channel:call(Ch, Q).

bind_queue(Ch, Q, X, Key) ->
    amqp_channel:call(Ch, #'queue.bind'{queue       = Q,
                                        exchange    = X,
                                        routing_key = Key}).

unbind_queue(Ch, Q, X, Key) ->
    amqp_channel:call(Ch, #'queue.unbind'{queue       = Q,
                                          exchange    = X,
                                          routing_key = Key}).

bind_exchange(Ch, D, S, Key) ->
    amqp_channel:call(Ch, #'exchange.bind'{destination = D,
                                           source      = S,
                                           routing_key = Key}).

declare_and_bind_queue(Ch, X, Key) ->
    Q = declare_queue(Ch),
    bind_queue(Ch, Q, X, Key),
    Q.


delete_exchange(Ch, XName) ->
  amqp_channel:call(Ch, #'exchange.delete'{exchange = XName}).

delete_queue(Ch, QName) ->
  amqp_channel:call(Ch, #'queue.delete'{queue = QName}).

exchange_declare_method(Name) ->
  exchange_declare_method(Name, <<"topic">>).

exchange_declare_method(Name, Type) ->
  #'exchange.declare'{exchange = Name,
                      type     = Type,
                      durable  = true}.

delete_all_queues_on(Config, Node) ->
  [rabbit_ct_broker_helpers:rpc(
     Config, Node, rabbit_amqqueue, delete, [Q, false, false,
                                             <<"acting-user">>]) ||
      Q <- all_queues_on(Config, Node)].

delete_all_exchanges_on(Config, Node) ->
  [rabbit_ct_broker_helpers:rpc(
    Config, Node, rabbit_exchange, delete, [X, false,
                                            <<"acting-user">>]) ||
     #exchange{name = X} <- all_exchanges_on(Config, Node)].

delete_all_policies_on(Config, Node) ->
  [rabbit_ct_broker_helpers:rpc(
    Config, Node, rabbit_policy, delete, [V, Name, <<"acting-user">>]) ||
      #{name := Name, vhost := V} <- all_policies_on(Config, Node)].

delete_all_runtime_parameters_on(Config, Node) ->
  [rabbit_ct_broker_helpers:rpc(
    Config, Node, rabbit_runtime_parameters, clear, [V, Component, Name, <<"acting-user">>]) ||
      #{component := Component, name := Name, vhost := V} <- all_runtime_parameters_on(Config, Node)].


all_queues_on(Config, Node) ->
  Ret = rabbit_ct_broker_helpers:rpc(Config, Node,
    rabbit_amqqueue, list, [<<"/">>]),
  case Ret of
      {badrpc, _} -> [];
      Qs          -> Qs
  end.

all_exchanges_on(Config, Node) ->
  Ret = rabbit_ct_broker_helpers:rpc(Config, Node,
    rabbit_exchange, list, [<<"/">>]),
  case Ret of
      {badrpc, _} -> [];
      Xs          -> Xs
  end.

all_policies_on(Config, Node) ->
  Ret = rabbit_ct_broker_helpers:rpc(Config, Node,
    rabbit_policy, list, [<<"/">>]),
  case Ret of
      {badrpc, _} -> [];
      Xs          -> [maps:from_list(PList) || PList <- Xs]
  end.

all_runtime_parameters_on(Config, Node) ->
  Ret = rabbit_ct_broker_helpers:rpc(Config, Node,
    rabbit_runtime_parameters, list, [<<"/">>]),
  case Ret of
      {badrpc, _} -> [];
      Xs          -> [maps:from_list(PList) || PList <- Xs]
  end.

await_binding(Config, Node, X, Key) ->
  await_binding(Config, Node, X, Key, 1).

await_binding(Config, Node, X, Key, ExpectedBindingCount) when is_integer(ExpectedBindingCount) ->
  await_binding(Config, Node, <<"/">>, X, Key, ExpectedBindingCount).

await_binding(Config, Node, Vhost, X, Key, ExpectedBindingCount) when is_integer(ExpectedBindingCount) ->
  Attempts = 100,
  await_binding(Config, Node, Vhost, X, Key, ExpectedBindingCount, Attempts).

await_binding(Config, Node, Vhost, X, Key, ExpectedBindingCount, _AttemptsLeft) when is_integer(ExpectedBindingCount) ->
    rabbit_ct_helpers:await_condition(
      fun() ->
              Bs = bound_keys_from(Config, Node, Vhost, X, Key),
              length(Bs) >= ExpectedBindingCount
      end, 30000).

await_bindings(Config, Node, X, Keys) ->
  [await_binding(Config, Node, X, Key) || Key <- Keys].

await_binding_absent(Config, Node, X, Key) ->
  case bound_keys_from(Config, Node, <<"/">>, X, Key) of
      [] -> ok;
      _  -> timer:sleep(100),
            await_binding_absent(Config, Node, X, Key)
  end.

bound_keys_from(Config, Node, Vhost, X, Key) ->
  Res = rabbit_misc:r(Vhost, exchange, X),
  List = rabbit_ct_broker_helpers:rpc(Config, Node,
                                      rabbit_binding, list_for_source, [Res]),
  [K || #binding{key = K} <- List, K =:= Key].

publish_expect(Ch, X, Key, Q, Payload) ->
  publish(Ch, X, Key, Payload),
  expect(Ch, Q, [Payload]).

publish(Ch, X, Key, Payload) when is_binary(Payload) ->
  publish(Ch, X, Key, #amqp_msg{payload = Payload});

publish(Ch, X, Key, Msg = #amqp_msg{}) ->
  amqp_channel:call(Ch, #'basic.publish'{exchange    = X,
                                         routing_key = Key}, Msg).

await_credentials_obfuscation_seeding_on_two_nodes(Config) ->
  rabbit_ct_helpers:await_condition(fun() ->
    rabbit_ct_broker_helpers:rpc(Config, 0, credentials_obfuscation, enabled, []) and
    rabbit_ct_broker_helpers:rpc(Config, 1, credentials_obfuscation, enabled, [])
  end, 10000).

assert_federation_internal_queue_type(Config, Server, Expected) ->
    Qs = all_queues_on(Config, Server),
    FedQs = lists:filter(
              fun(Q) ->
                      lists:member(
                        {<<"x-internal-purpose">>, longstr, <<"federation">>}, amqqueue:get_arguments(Q))
              end,
              Qs),
    FedQTypes = lists:map(fun(Q) -> amqqueue:get_type(Q) end, FedQs),
    ?assertEqual([Expected], lists:uniq(FedQTypes)).
