%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(exchange_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-include("rabbit_federation.hrl").

-compile(export_all).

-import(rabbit_federation_test_util,
        [expect/3, expect/4, expect_empty/2,
         set_upstream/4, set_upstream/5, set_upstream_in_vhost/5, set_upstream_in_vhost/6,
         clear_upstream/3, set_upstream_set/4,
         set_policy/5, set_policy_pattern/5, clear_policy/3,
         set_policy_upstream/5, set_policy_upstreams/4,
         all_federation_links/2, federation_links_in_vhost/3, status_fields/2]).

-import(rabbit_ct_broker_helpers,
        [set_policy_in_vhost/7]).

all() ->
    [
      {group, essential},
      {group, cluster_size_3},
      {group, rolling_upgrade}
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
      lookup_exchange_status
    ].

suite() ->
    [{timetrap, {minutes, 3}}].

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
          rabbit_federation_test_util:setup_federation_with_upstream_params(Config1, [
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
                                            rabbit_federation_status, status, []),
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

child_id_format(Config) ->
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
                        Config1, [OldNodeA, NewNodeB, NewNodeD]),
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
    end.

%%
%% Test helpers
%%

clean_up_federation_related_bits(Config) ->
  delete_all_queues_on(Config, 0),
  delete_all_exchanges_on(Config, 0),
  delete_all_policies_on(Config, 0),
  delete_all_runtime_parameters_on(Config, 0).

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
    amqp_channel:call(Ch, X).

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

await_binding(_Config, _Node, _Vhost, _X, _Key, ExpectedBindingCount, 0) ->
  {error, rabbit_misc:format("expected ~b bindings but they did not materialize in time", [ExpectedBindingCount])};
await_binding(Config, Node, Vhost, X, Key, ExpectedBindingCount, AttemptsLeft) when is_integer(ExpectedBindingCount) ->
    case bound_keys_from(Config, Node, Vhost, X, Key) of
        Bs when length(Bs) < ExpectedBindingCount ->
            timer:sleep(1000),
            await_binding(Config, Node, Vhost, X, Key, ExpectedBindingCount, AttemptsLeft - 1);
        Bs when length(Bs) =:= ExpectedBindingCount ->
            ok;
        Bs ->
            {error, rabbit_misc:format("expected ~b bindings, got ~b", [ExpectedBindingCount, length(Bs)])}
    end.

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
  %% give credentials_obfuscation a moment to start and be seeded
  rabbit_ct_helpers:await_condition(fun() ->
    rabbit_ct_broker_helpers:rpc(Config, 0, credentials_obfuscation, enabled, []) and
    rabbit_ct_broker_helpers:rpc(Config, 1, credentials_obfuscation, enabled, [])
  end),

  timer:sleep(1000).

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
