%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(exchange_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-include("rabbit_federation.hrl").

-compile(export_all).

-import(rabbit_federation_test_util,
        [wait_for_federation/2, expect/3, expect/4, expect_empty/2,
         set_upstream/4, set_upstream/5, set_upstream_in_vhost/5, set_upstream_in_vhost/6,
         clear_upstream/3, set_upstream_set/4,
         set_policy/5, set_policy_pattern/5, clear_policy/3,
         set_policy_upstream/5, set_policy_upstreams/4,
         all_federation_links/2, federation_links_in_vhost/3, status_fields/2]).

-import(rabbit_ct_broker_helpers,
        [set_policy_in_vhost/7]).

all() ->
    [
      {group, without_automatic_setup},
      {group, channel_use_mode_single},
      {group, without_disambiguate},
      {group, with_disambiguate}
    ].

groups() ->
    [
      {without_automatic_setup, [], [
          message_cycle_detection_case2
      ]},
      {without_disambiguate, [], [
          {cluster_size_1, [], [
              simple,
              multiple_upstreams,
              multiple_upstreams_pattern,
              multiple_uris,
              multiple_downstreams,
              e2e,
              unbind_on_delete,
              unbind_on_unbind,
              unbind_gets_transmitted,
              no_loop,
              dynamic_reconfiguration,
              dynamic_reconfiguration_integrity,
              federate_unfederate,
              dynamic_plugin_stop_start,
              dynamic_plugin_cleanup_stop_start,
              dynamic_policy_cleanup,
              delete_federated_exchange_upstream,
              delete_federated_queue_upstream
            ]}
        ]},
      {with_disambiguate, [], [
          {cluster_size_2, [], [
              user_id,
              message_cycle_detection_case1,
              restart_upstream
            ]},
          {cluster_size_3, [], [
              max_hops,
              binding_propagation
            ]},

          {without_plugins, [], [
              {cluster_size_2, [], [
                  upstream_has_no_federation
                ]}
            ]}
        ]},
        {channel_use_mode_single, [], [
            simple,
            single_channel_mode,
            multiple_upstreams,
            multiple_upstreams_pattern,
            multiple_uris,
            multiple_downstreams,
            e2e,
            unbind_on_delete,
            unbind_on_unbind,
            unbind_gets_transmitted,
            federate_unfederate
        ]}
    ].

suite() ->
    [{timetrap, {minutes, 5}}].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

%% Some of the "regular" tests but in the single channel mode.
init_per_group(channel_use_mode_single, Config) ->
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
        {rmq_nodes_clustered, false}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps() ++
      SetupFederation);
init_per_group(without_automatic_setup, Config) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Suffix},
        {rmq_nodes_clustered, false},
        {rmq_nodes_count, 1}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps());
init_per_group(without_disambiguate, Config) ->
    rabbit_ct_helpers:set_config(Config,
      {disambiguate_step, []});
init_per_group(with_disambiguate, Config) ->
    rabbit_ct_helpers:set_config(Config,
      {disambiguate_step, [fun rabbit_federation_test_util:disambiguate/1]});
init_per_group(without_plugins, Config) ->
    rabbit_ct_helpers:set_config(Config,
      {broker_with_plugins, [true, false]});
init_per_group(cluster_size_1 = Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodes_count, 1}
      ]),
    init_per_group1(Group, Config1);
init_per_group(cluster_size_2 = Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodes_count, 2}
      ]),
    init_per_group1(Group, Config1);
init_per_group(cluster_size_3 = Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodes_count, 3}
      ]),
    init_per_group1(Group, Config1).

init_per_group1(Group, Config) ->
    SetupFederation = case Group of
        cluster_size_1 -> [fun rabbit_federation_test_util:setup_federation/1];
        cluster_size_2 -> [];
        cluster_size_3 -> []
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
end_per_group(without_plugins, Config) ->
    Config;
end_per_group(_, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()
    ).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

simple(Config) ->
    with_ch(Config,
      fun (Ch) ->
              Q = bind_queue(Ch, <<"fed.downstream">>, <<"key">>),
              await_binding(Config, 0, <<"upstream">>, <<"key">>),
              publish_expect(Ch, <<"upstream">>, <<"key">>, Q, <<"HELLO">>)
      end, upstream_downstream()).

single_channel_mode(Config) ->
    with_conn_and_ch(Config,
      fun (Conn, Ch) ->
              Infos = amqp_connection:info(Conn, [num_channels]),
              N = proplists:get_value(num_channels, Infos),
              ?assertEqual(1, N),
              Q = bind_queue(Ch, <<"fed.downstream">>, <<"key">>),
              await_binding(Config, 0, <<"upstream">>, <<"key">>),
              publish_expect(Ch, <<"upstream">>, <<"key">>, Q, <<"HELLO">>)
      end, upstream_downstream()).

multiple_upstreams(Config) ->
    with_ch(Config,
      fun (Ch) ->
              Q = bind_queue(Ch, <<"fed12.downstream">>, <<"key">>),
              await_binding(Config, 0, <<"upstream">>, <<"key">>),
              await_binding(Config, 0, <<"upstream2">>, <<"key">>),
              publish_expect(Ch, <<"upstream">>, <<"key">>, Q, <<"HELLO1">>),
              publish_expect(Ch, <<"upstream2">>, <<"key">>, Q, <<"HELLO2">>)
      end, [x(<<"upstream">>),
            x(<<"upstream2">>),
            x(<<"fed12.downstream">>)]).

multiple_upstreams_pattern(Config) ->
    set_upstream(Config, 0, <<"local453x">>,
        rabbit_ct_broker_helpers:node_uri(Config, 0), [
        {<<"exchange">>, <<"upstream">>},
        {<<"queue">>, <<"upstream">>}]),

    set_upstream(Config, 0, <<"local3214x">>,
        rabbit_ct_broker_helpers:node_uri(Config, 0), [
        {<<"exchange">>, <<"upstream2">>},
        {<<"queue">>, <<"upstream2">>}]),

    set_policy_pattern(Config, 0, <<"pattern">>, <<"^pattern\.">>, <<"local\\d+x">>),

    with_ch(Config,
      fun (Ch) ->
              Q = bind_queue(Ch, <<"pattern.downstream">>, <<"key">>),
              await_binding(Config, 0, <<"upstream">>, <<"key">>),
              await_binding(Config, 0, <<"upstream2">>, <<"key">>),
              publish_expect(Ch, <<"upstream">>, <<"key">>, Q, <<"HELLO1">>),
              publish_expect(Ch, <<"upstream2">>, <<"key">>, Q, <<"HELLO2">>)
      end, [x(<<"upstream">>),
            x(<<"upstream2">>),
            x(<<"pattern.downstream">>)]),

    clear_upstream(Config, 0, <<"local453x">>),
    clear_upstream(Config, 0, <<"local3214x">>),
    clear_policy(Config, 0, <<"pattern">>).

multiple_uris(Config) ->
    %% We can't use a direct connection for Kill() to work.
    URIs = [
      rabbit_ct_broker_helpers:node_uri(Config, 0),
      rabbit_ct_broker_helpers:node_uri(Config, 0, [use_ipaddr])
    ],
    set_upstream(Config, 0, <<"localhost">>, URIs),
    WithCh = fun(F) ->
                     Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
                     F(Ch),
                     rabbit_ct_client_helpers:close_channels_and_connection(
                       Config, 0)
             end,
    WithCh(fun (Ch) -> declare_all(Ch, upstream_downstream()) end),
    expect_uris(Config, 0, URIs),
    WithCh(fun (Ch) -> delete_all(Ch, upstream_downstream()) end),
    %% Put back how it was
    rabbit_federation_test_util:setup_federation(Config),
    ok.

expect_uris(_, _, []) ->
    ok;
expect_uris(Config, Node, URIs) ->
    [Link] = rabbit_ct_broker_helpers:rpc(Config, Node,
      rabbit_federation_status, status, []),
    URI = rabbit_misc:pget(uri, Link),
    kill_only_connection(Config, Node),
    expect_uris(Config, Node, URIs -- [URI]).

kill_only_connection(Config, Node) ->
    case connection_pids(Config, Node) of
        [Pid] -> catch rabbit_ct_broker_helpers:rpc(Config, Node,
                   rabbit_networking, close_connection, [Pid, "boom"]), %% [1]
                 wait_for_pid_to_die(Config, Node, Pid);
        _     -> timer:sleep(100),
                 kill_only_connection(Config, Node)
    end.

%% [1] the catch is because we could still see a connection from a
%% previous time round. If so that's fine (we'll just loop around
%% again) but we don't want the test to fail because a connection
%% closed as we were trying to close it.

wait_for_pid_to_die(Config, Node, Pid) ->
    case connection_pids(Config, Node) of
        [Pid] -> timer:sleep(100),
                 wait_for_pid_to_die(Config, Node, Pid);
        _     -> ok
    end.


multiple_downstreams(Config) ->
    with_ch(Config,
      fun (Ch) ->
              Q1 = bind_queue(Ch, <<"fed.downstream">>, <<"key">>),
              Q12 = bind_queue(Ch, <<"fed12.downstream2">>, <<"key">>),
              await_binding(Config, 0, <<"upstream">>, <<"key">>, 2),
              await_binding(Config, 0, <<"upstream2">>, <<"key">>),
              publish(Ch, <<"upstream">>, <<"key">>, <<"HELLO1">>),
              publish(Ch, <<"upstream2">>, <<"key">>, <<"HELLO2">>),
              expect(Ch, Q1, [<<"HELLO1">>]),
              expect(Ch, Q12, [<<"HELLO1">>, <<"HELLO2">>])
      end, upstream_downstream() ++
          [x(<<"upstream2">>),
           x(<<"fed12.downstream2">>)]).

e2e(Config) ->
    with_ch(Config,
      fun (Ch) ->
              bind_exchange(Ch, <<"downstream2">>, <<"fed.downstream">>,
                            <<"key">>),
              await_binding(Config, 0, <<"upstream">>, <<"key">>),
              Q = bind_queue(Ch, <<"downstream2">>, <<"key">>),
              publish_expect(Ch, <<"upstream">>, <<"key">>, Q, <<"HELLO1">>)
      end, upstream_downstream() ++ [x(<<"downstream2">>)]).

unbind_on_delete(Config) ->
    with_ch(Config,
      fun (Ch) ->
              Q1 = bind_queue(Ch, <<"fed.downstream">>, <<"key">>),
              Q2 = bind_queue(Ch, <<"fed.downstream">>, <<"key">>),
              await_binding(Config, 0, <<"upstream">>, <<"key">>),
              delete_queue(Ch, Q2),
              publish_expect(Ch, <<"upstream">>, <<"key">>, Q1, <<"HELLO">>)
      end, upstream_downstream()).

unbind_on_unbind(Config) ->
    with_ch(Config,
      fun (Ch) ->
              Q1 = bind_queue(Ch, <<"fed.downstream">>, <<"key">>),
              Q2 = bind_queue(Ch, <<"fed.downstream">>, <<"key">>),
              await_binding(Config, 0, <<"upstream">>, <<"key">>),
              unbind_queue(Ch, Q2, <<"fed.downstream">>, <<"key">>),
              publish_expect(Ch, <<"upstream">>, <<"key">>, Q1, <<"HELLO">>),
              delete_queue(Ch, Q2)
      end, upstream_downstream()).

user_id(Config) ->
    [Rabbit, Hare] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    set_policy_upstream(Config, Rabbit, <<"^test$">>,
      rabbit_ct_broker_helpers:node_uri(Config, 1), []),
    Perm = fun (F, A) ->
                  ok = rpc:call(Hare,
                                rabbit_auth_backend_internal, F, A)
           end,
    Perm(add_user, [<<"hare-user">>, <<"hare-user">>, <<"acting-user">>]),
    Perm(set_permissions, [<<"hare-user">>,
                           <<"/">>, <<".*">>, <<".*">>, <<".*">>,
                           <<"acting-user">>]),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Rabbit),
    {ok, Conn2} = amqp_connection:start(
      #amqp_params_network{
        username = <<"hare-user">>,
        password = <<"hare-user">>,
        port     = rabbit_ct_broker_helpers:get_node_config(Config, Hare,
          tcp_port_amqp)}),
    {ok, Ch2} = amqp_connection:open_channel(Conn2),

    declare_exchange(Ch2, x(<<"test">>)),
    declare_exchange(Ch, x(<<"test">>)),
    Q = bind_queue(Ch, <<"test">>, <<"key">>),
    await_binding(Config, Hare, <<"test">>, <<"key">>),

    Msg = #amqp_msg{props   = #'P_basic'{user_id = <<"hare-user">>},
                    payload = <<"HELLO">>},

    SafeUri = fun (H) ->
                      {array, [{table, Recv}]} =
                          rabbit_misc:table_lookup(
                            H, <<"x-received-from">>),
                      URI = rabbit_ct_broker_helpers:node_uri(Config, 1),
                      {longstr, URI} =
                         rabbit_misc:table_lookup(Recv, <<"uri">>)
              end,
    ExpectUser =
        fun (ExpUser) ->
                fun () ->
                        receive
                            {#'basic.deliver'{},
                             #amqp_msg{props   = Props,
                                       payload = Payload}} ->
                                #'P_basic'{user_id = ActUser,
                                           headers = Headers} = Props,
                                SafeUri(Headers),
                                <<"HELLO">> = Payload,
                                ExpUser = ActUser
                        end
                end
        end,

    wait_for_federation(
      90,
      fun() ->
              VHost = <<"/">>,
              X1s = rabbit_ct_broker_helpers:rpc(
                      Config, Rabbit, rabbit_exchange, list, [VHost]),
              L1 =
              [X || X <- X1s,
               X#exchange.name =:= #resource{virtual_host = VHost,
                                             kind = exchange,
                                             name = <<"test">>},
               X#exchange.scratches =:= [{federation,
                                          [{{<<"upstream-2">>,
                                             <<"test">>},
                                            <<"B">>}]}]],
              X2s = rabbit_ct_broker_helpers:rpc(
                      Config, Hare, rabbit_exchange, list, [VHost]),
              L2 =
              [X || X <- X2s,
                    X#exchange.type =:= 'x-federation-upstream'],
              [] =/= L1 andalso [] =/= L2 andalso
              has_internal_federated_queue(Config, Hare, VHost)
      end),
    publish(Ch2, <<"test">>, <<"key">>, Msg),
    expect(Ch, Q, ExpectUser(undefined)),

    set_policy_upstream(Config, Rabbit, <<"^test$">>,
      rabbit_ct_broker_helpers:node_uri(Config, 1),
      [{<<"trust-user-id">>, true}]),
    wait_for_federation(
      90,
      fun() ->
              VHost = <<"/">>,
              X1s = rabbit_ct_broker_helpers:rpc(
                      Config, Rabbit, rabbit_exchange, list, [VHost]),
              L1 =
              [X || X <- X1s,
               X#exchange.name =:= #resource{virtual_host = VHost,
                                             kind = exchange,
                                             name = <<"test">>},
               X#exchange.scratches =:= [{federation,
                                          [{{<<"upstream-2">>,
                                             <<"test">>},
                                            <<"A">>}]}]],
              X2s = rabbit_ct_broker_helpers:rpc(
                      Config, Hare, rabbit_exchange, list, [VHost]),
              L2 =
              [X || X <- X2s,
                    X#exchange.type =:= 'x-federation-upstream'],
              [] =/= L1 andalso [] =/= L2 andalso
              has_internal_federated_queue(Config, Hare, VHost)
      end),
    publish(Ch2, <<"test">>, <<"key">>, Msg),
    expect(Ch, Q, ExpectUser(<<"hare-user">>)),

    amqp_channel:close(Ch2),
    amqp_connection:close(Conn2),

    ok.

%% In order to test that unbinds get sent we deliberately set up a
%% broken config - with topic upstream and fanout downstream. You
%% shouldn't really do this, but it lets us see "extra" messages that
%% get sent.
unbind_gets_transmitted(Config) ->
    with_ch(Config,
      fun (Ch) ->
              Q11 = bind_queue(Ch, <<"fed.downstream">>, <<"key1">>),
              Q12 = bind_queue(Ch, <<"fed.downstream">>, <<"key1">>),
              Q21 = bind_queue(Ch, <<"fed.downstream">>, <<"key2">>),
              Q22 = bind_queue(Ch, <<"fed.downstream">>, <<"key2">>),
              await_binding(Config, 0, <<"upstream">>, <<"key1">>),
              await_binding(Config, 0, <<"upstream">>, <<"key2">>),
              [delete_queue(Ch, Q) || Q <- [Q12, Q21, Q22]],
              publish(Ch, <<"upstream">>, <<"key1">>, <<"YES">>),
              publish(Ch, <<"upstream">>, <<"key2">>, <<"NO">>),
              expect(Ch, Q11, [<<"YES">>]),
              expect_empty(Ch, Q11)
      end, [x(<<"upstream">>),
            x(<<"fed.downstream">>)]).

no_loop(Config) ->
    with_ch(Config,
      fun (Ch) ->
              Q1 = bind_queue(Ch, <<"one">>, <<"key">>),
              Q2 = bind_queue(Ch, <<"two">>, <<"key">>),
              await_binding(Config, 0, <<"one">>, <<"key">>, 2),
              await_binding(Config, 0, <<"two">>, <<"key">>, 2),
              publish(Ch, <<"one">>, <<"key">>, <<"Hello from one">>),
              publish(Ch, <<"two">>, <<"key">>, <<"Hello from two">>),
              expect(Ch, Q1, [<<"Hello from one">>, <<"Hello from two">>]),
              expect(Ch, Q2, [<<"Hello from one">>, <<"Hello from two">>]),
              expect_empty(Ch, Q1),
              expect_empty(Ch, Q2)
      end, [x(<<"one">>),
            x(<<"two">>)]).

suffix(Config, Node, Name, XName) ->
    rabbit_ct_broker_helpers:rpc(Config, Node,
      rabbit_federation_db, get_active_suffix,
             [xr(<<"fed.downstream">>),
              #upstream{name          = Name,
                        exchange_name = list_to_binary(XName)}, none]).

restart_upstream(Config) ->
    [Rabbit, Hare] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Downstream = rabbit_ct_client_helpers:open_channel(Config, Rabbit),
    Upstream   = rabbit_ct_client_helpers:open_channel(Config, Hare),

    rabbit_federation_test_util:set_upstream(Config,
      Rabbit, <<"hare">>, rabbit_ct_broker_helpers:node_uri(Config, 1)),
    rabbit_federation_test_util:set_upstream_set(Config,
      Rabbit, <<"upstream">>,
      [{<<"hare">>, [{<<"exchange">>, <<"upstream">>}]}]),
    rabbit_federation_test_util:set_policy(Config,
      Rabbit, <<"hare">>, <<"^hare\\.">>, <<"upstream">>),

    declare_exchange(Upstream, x(<<"upstream">>)),
    declare_exchange(Downstream, x(<<"hare.downstream">>)),

    Qstays = bind_queue(Downstream, <<"hare.downstream">>, <<"stays">>),
    Qgoes = bind_queue(Downstream, <<"hare.downstream">>, <<"goes">>),

    rabbit_ct_client_helpers:close_channels_and_connection(Config, Hare),
    rabbit_ct_broker_helpers:stop_node(Config, Hare),

    Qcomes = bind_queue(Downstream, <<"hare.downstream">>, <<"comes">>),
    unbind_queue(Downstream, Qgoes, <<"hare.downstream">>, <<"goes">>),

    rabbit_ct_broker_helpers:start_node(Config, Hare),
    Upstream1 = rabbit_ct_client_helpers:open_channel(Config, Hare),

    %% Wait for the link to come up and for these bindings
    %% to be transferred
    await_binding(Config, Hare, <<"upstream">>, <<"comes">>, 1),
    await_binding_absent(Config, Hare, <<"upstream">>, <<"goes">>),
    await_binding(Config, Hare, <<"upstream">>, <<"stays">>, 1),

    publish(Upstream1, <<"upstream">>, <<"goes">>, <<"GOES">>),
    publish(Upstream1, <<"upstream">>, <<"stays">>, <<"STAYS">>),
    publish(Upstream1, <<"upstream">>, <<"comes">>, <<"COMES">>),

    expect(Downstream, Qstays, [<<"STAYS">>]),
    expect(Downstream, Qcomes, [<<"COMES">>]),
    expect_empty(Downstream, Qgoes),

    delete_exchange(Downstream, <<"hare.downstream">>),
    delete_exchange(Upstream1, <<"upstream">>),

    rabbit_federation_test_util:clear_policy(Config,
      Rabbit, <<"hare">>),
    rabbit_federation_test_util:clear_upstream_set(Config,
      Rabbit, <<"upstream">>),
    rabbit_federation_test_util:clear_upstream(Config,
      Rabbit, <<"hare">>),
    ok.

%% flopsy, mopsy and cottontail, connected in a ring with max_hops = 2
%% for each connection. We should not see any duplicates.

max_hops(Config) ->
    [Flopsy, Mopsy, Cottontail] = rabbit_ct_broker_helpers:get_node_configs(
      Config, nodename),
    [set_policy_upstream(Config, Downstream,
       <<"^ring$">>,
       rabbit_ct_broker_helpers:node_uri(Config, Upstream),
       [{<<"max-hops">>, 2}])
     || {Downstream, Upstream} <- [{Flopsy, Cottontail},
                                    {Mopsy, Flopsy},
                                    {Cottontail, Mopsy}]],

    FlopsyCh     = rabbit_ct_client_helpers:open_channel(Config, Flopsy),
    MopsyCh      = rabbit_ct_client_helpers:open_channel(Config, Mopsy),
    CottontailCh = rabbit_ct_client_helpers:open_channel(Config, Cottontail),

    declare_exchange(FlopsyCh,     x(<<"ring">>)),
    declare_exchange(MopsyCh,      x(<<"ring">>)),
    declare_exchange(CottontailCh, x(<<"ring">>)),

    Q1 = bind_queue(FlopsyCh,     <<"ring">>, <<"key">>),
    Q2 = bind_queue(MopsyCh,      <<"ring">>, <<"key">>),
    Q3 = bind_queue(CottontailCh, <<"ring">>, <<"key">>),

    await_binding(Config, Flopsy,     <<"ring">>, <<"key">>, 3),
    await_binding(Config, Mopsy,      <<"ring">>, <<"key">>, 3),
    await_binding(Config, Cottontail, <<"ring">>, <<"key">>, 3),

    publish(FlopsyCh,     <<"ring">>, <<"key">>, <<"HELLO flopsy">>),
    publish(MopsyCh,      <<"ring">>, <<"key">>, <<"HELLO mopsy">>),
    publish(CottontailCh, <<"ring">>, <<"key">>, <<"HELLO cottontail">>),

    Msgs = [<<"HELLO flopsy">>, <<"HELLO mopsy">>, <<"HELLO cottontail">>],
    expect(FlopsyCh,     Q1, Msgs),
    expect(MopsyCh,      Q2, Msgs),
    expect(CottontailCh, Q3, Msgs),
    expect_empty(FlopsyCh,     Q1),
    expect_empty(MopsyCh,      Q2),
    expect_empty(CottontailCh, Q3),
    ok.

%% Two nodes, federated two way with the same virtual hosts, and max_hops set to a
%% high value.
message_cycle_detection_case1(Config) ->
    [Cycle1, Cycle2] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    [set_policy_upstream(Config, Downstream,
       <<"^cycle$">>,
       rabbit_ct_broker_helpers:node_uri(Config, Upstream),
       [{<<"max-hops">>, 10}])
     || {Downstream, Upstream} <- [{Cycle1, Cycle2}, {Cycle2, Cycle1}]],

    Cycle1Ch = rabbit_ct_client_helpers:open_channel(Config, Cycle1),
    Cycle2Ch = rabbit_ct_client_helpers:open_channel(Config, Cycle2),

    declare_exchange(Cycle1Ch, x(<<"cycle">>)),
    declare_exchange(Cycle2Ch, x(<<"cycle">>)),

    Q1 = bind_queue(Cycle1Ch, <<"cycle">>, <<"cycle_detection-key">>),
    Q2 = bind_queue(Cycle2Ch, <<"cycle">>, <<"cycle_detection-key">>),

    %% "key" present twice because once for the local queue and once
    %% for federation in each case
    await_binding(Config, Cycle1, <<"cycle">>, <<"cycle_detection-key">>, 2),
    await_binding(Config, Cycle2, <<"cycle">>, <<"cycle_detection-key">>, 2),

    publish(Cycle1Ch, <<"cycle">>, <<"cycle_detection-key">>, <<"HELLO1">>),
    publish(Cycle2Ch, <<"cycle">>, <<"cycle_detection-key">>, <<"HELLO2">>),

    Msgs = [<<"HELLO1">>, <<"HELLO2">>],
    expect(Cycle1Ch, Q1, Msgs),
    expect(Cycle2Ch, Q2, Msgs),
    expect_empty(Cycle1Ch, Q1),
    expect_empty(Cycle2Ch, Q2),

    ok.

node_uri_with_virtual_host(Config, Vhost) ->
    node_uri_with_virtual_host(Config, 0, Vhost).

node_uri_with_virtual_host(Config, Node, Vhost) ->
    NodeURI = rabbit_ct_broker_helpers:node_uri(Config, Node),
    <<NodeURI/binary, "/", Vhost/binary>>.

upstream_policy_defs(Upstream) ->
    maps:to_list(#{<<"federation-upstream">> => Upstream}).

%% Exchange federation between three local virtual hosts, A -> B -> C,
%% propagates messages from A to C with a high enough max-hops value
message_cycle_detection_case2(Config) ->
    VH1 = <<"cycles.a">>,
    VH2 = <<"cycles.b">>,
    VH3 = <<"cycles.c">>,
    [begin
         rabbit_ct_broker_helpers:add_vhost(Config, V),
         rabbit_ct_broker_helpers:set_full_permissions(Config, V)
     end || V <- [VH1, VH2, VH3]],

    %% make sure that cycle detection does not drop messages because of a limit on hops
    UpstreamOpts = [{<<"max-hops">>, 5}],
    %% VH1 is an upstream for VH2
    %% VH2 is an upstream for VH3
    UpstreamA = <<"upstream_a">>,
    URI1 = node_uri_with_virtual_host(Config, VH1),
    set_upstream_in_vhost(Config, 0, VH2, UpstreamA, URI1, UpstreamOpts),
    UpstreamB = <<"upstream_b">>,
    URI2 = node_uri_with_virtual_host(Config, VH2),
    set_upstream_in_vhost(Config, 0, VH3, UpstreamB, URI2, UpstreamOpts),

    %% policies
    set_policy_in_vhost(Config, 0, VH2, <<"federate.x">>, <<"^federated">>, <<"exchanges">>, upstream_policy_defs(UpstreamA)),
    set_policy_in_vhost(Config, 0, VH3, <<"federate.x">>, <<"^federated">>, <<"exchanges">>, upstream_policy_defs(UpstreamB)),

    %% channels
    VH1Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0, VH1),
    VH2Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0, VH2),
    VH3Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0, VH3),

    {ok, VH1Ch} = amqp_connection:open_channel(VH1Conn),
    {ok, VH2Ch} = amqp_connection:open_channel(VH2Conn),
    {ok, VH3Ch} = amqp_connection:open_channel(VH3Conn),

    X = <<"federated.x">>,
    declare_exchange(VH3Ch, x(X, <<"fanout">>)),
    declare_exchange(VH2Ch, x(X, <<"fanout">>)),
    declare_exchange(VH1Ch, x(X, <<"fanout">>)),
    
    rabbit_ct_helpers:await_condition(
        fun () ->
            LinksInB = federation_links_in_vhost(Config, 0, VH2),
            LinksInC = federation_links_in_vhost(Config, 0, VH3),
            length(LinksInB) =:= 1 andalso
            length(LinksInC) =:= 1 andalso
            [running] =:= status_fields(status, LinksInB ++ LinksInC)
        end),
    
    Statuses = federation_links_in_vhost(Config, 0, VH2) ++ federation_links_in_vhost(Config, 0, VH3),
    
    ?assertEqual(lists:usort([URI1, URI2]),
                 status_fields(uri, Statuses)),
    ?assertEqual(lists:usort([<<"federated.x">>]),
                 status_fields(upstream_exchange, Statuses)),
    ?assertEqual(lists:usort([VH2, VH3]),
                 status_fields(vhost, Statuses)),
    ?assertEqual(lists:usort([exchange]),
                 status_fields(type, Statuses)),
    
    %% give links some time to set up their topology
    rabbit_ct_helpers:await_condition(
        fun () ->
            ExchangesInA = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_exchange, list, [VH1]),
            ExchangesInB = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_exchange, list, [VH2]),
            length(ExchangesInA) >= 1 andalso
            length(ExchangesInB) >= 1
        end),
    
    RK = <<"doesn't matter">>,
    Q  = bind_queue(VH3Ch, X, RK),
    ?assertEqual(ok, await_binding(Config, 0, VH2, X, RK, 1)),
    ?assertEqual(ok, await_binding(Config, 0, VH3, X, RK, 1)),
    timer:sleep(2000),

    rabbit_ct_helpers:await_condition(
    fun () ->
        ExchangesInA = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_exchange, list, [VH1]),
        lists:any(fun(#exchange{name = XName}) ->
                    XName =:= rabbit_misc:r(VH1, exchange, X)
                  end, ExchangesInA)
    end),
    
    Payload1 = <<"msg1">>,
    Payload2 = <<"msg2">>,
    publish(VH1Ch, X, RK, Payload1),
    publish(VH1Ch, X, RK, Payload2),

    Msgs = [Payload1, Payload2],
    %% payloads published to a federated exchange in A reach a queue in C
    expect(VH3Ch, Q, Msgs, 10000),

    [amqp_connection:close(Conn) || Conn <- [VH1Conn, VH2Conn, VH3Conn]],
    [rabbit_ct_broker_helpers:delete_vhost(Config, Vhost) || Vhost <- [VH1, VH2, VH3]],
    ok.

%% Arrows indicate message flow. Numbers indicate max_hops.
%%
%% Dylan ---1--> Bugs ---2--> Jessica
%% |^                              |^
%% |\--------------1---------------/|
%% \---------------1----------------/
%%
%%
%% We want to demonstrate that if we bind a queue locally at each
%% broker, (exactly) the following bindings propagate:
%%
%% Bugs binds to Dylan
%% Jessica binds to Bugs, which then propagates on to Dylan
%% Jessica binds to Dylan directly
%% Dylan binds to Jessica.
%%
%% i.e. Dylan has two bindings from Jessica and one from Bugs
%%      Bugs has one binding from Jessica
%%      Jessica has one binding from Dylan
%%
%% So we tag each binding with its original broker and see how far it gets
%%
%% Also we check that when we tear down the original bindings
%% that we get rid of everything again.

binding_propagation(Config) ->
    [Dylan, Bugs, Jessica] = rabbit_ct_broker_helpers:get_node_configs(Config,
      nodename),
    set_policy_upstream(Config, Dylan, <<"^x$">>,
      rabbit_ct_broker_helpers:node_uri(Config, Jessica), []),
    set_policy_upstream(Config, Bugs, <<"^x$">>,
      rabbit_ct_broker_helpers:node_uri(Config, Dylan), []),
    set_policy_upstreams(Config, Jessica, <<"^x$">>, [
        {rabbit_ct_broker_helpers:node_uri(Config, Dylan), []},
        {rabbit_ct_broker_helpers:node_uri(Config, Bugs),
          [{<<"max-hops">>, 2}]}
      ]),
    DylanCh   = rabbit_ct_client_helpers:open_channel(Config, Dylan),
    BugsCh    = rabbit_ct_client_helpers:open_channel(Config, Bugs),
    JessicaCh = rabbit_ct_client_helpers:open_channel(Config, Jessica),

    declare_exchange(DylanCh,   x(<<"x">>)),
    declare_exchange(BugsCh,    x(<<"x">>)),
    declare_exchange(JessicaCh, x(<<"x">>)),

    Q1 = bind_queue(DylanCh,   <<"x">>, <<"dylan">>),
    Q2 = bind_queue(BugsCh,    <<"x">>, <<"bugs">>),
    Q3 = bind_queue(JessicaCh, <<"x">>, <<"jessica">>),

    await_binding(Config,  Dylan,   <<"x">>, <<"jessica">>, 2),
    await_bindings(Config, Dylan,   <<"x">>, [<<"bugs">>, <<"dylan">>]),
    await_bindings(Config, Bugs,    <<"x">>, [<<"jessica">>, <<"bugs">>]),
    await_bindings(Config, Jessica, <<"x">>, [<<"dylan">>, <<"jessica">>]),

    delete_queue(DylanCh,   Q1),
    delete_queue(BugsCh,    Q2),
    delete_queue(JessicaCh, Q3),

    await_bindings(Config, Dylan,   <<"x">>, []),
    await_bindings(Config, Bugs,    <<"x">>, []),
    await_bindings(Config, Jessica, <<"x">>, []),

    ok.

upstream_has_no_federation(Config) ->
    [Rabbit, Hare] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    set_policy_upstream(Config, Rabbit, <<"^test$">>,
      rabbit_ct_broker_helpers:node_uri(Config, Hare), []),
    Downstream = rabbit_ct_client_helpers:open_channel(Config, Rabbit),
    Upstream   = rabbit_ct_client_helpers:open_channel(Config, Hare),
    declare_exchange(Upstream, x(<<"test">>)),
    declare_exchange(Downstream, x(<<"test">>)),
    Q = bind_queue(Downstream, <<"test">>, <<"routing">>),
    await_binding(Config, Hare, <<"test">>, <<"routing">>),
    publish(Upstream, <<"test">>, <<"routing">>, <<"HELLO">>),
    expect(Downstream, Q, [<<"HELLO">>]),
    ok.

dynamic_reconfiguration(Config) ->
    with_ch(Config,
      fun (_Ch) ->
              Xs = [<<"all.fed1">>, <<"all.fed2">>],
              %% Left from the conf we set up for previous tests
              assert_connections(Config, 0, Xs, [<<"localhost">>, <<"local5673">>]),

              %% Test that clearing connections works
              clear_upstream(Config, 0, <<"localhost">>),
              clear_upstream(Config, 0, <<"local5673">>),
              assert_connections(Config, 0, Xs, []),

              %% Test that readding them and changing them works
              set_upstream(Config, 0,
                <<"localhost">>, rabbit_ct_broker_helpers:node_uri(Config, 0)),
              %% Do it twice so we at least hit the no-restart optimisation
              URI = rabbit_ct_broker_helpers:node_uri(Config, 0, [use_ipaddr]),
              set_upstream(Config, 0, <<"localhost">>, URI),
              set_upstream(Config, 0, <<"localhost">>, URI),
              assert_connections(Config, 0, Xs, [<<"localhost">>]),

              %% And re-add the last - for next test
              rabbit_federation_test_util:setup_federation(Config)
      end, [x(<<"all.fed1">>), x(<<"all.fed2">>)]).

dynamic_reconfiguration_integrity(Config) ->
    with_ch(Config,
      fun (_Ch) ->
              Xs = [<<"new.fed1">>, <<"new.fed2">>],

              %% Declared exchanges with nonexistent set - no links
              assert_connections(Config, 0, Xs, []),

              %% Create the set - links appear
              set_upstream_set(Config, 0, <<"new-set">>, [{<<"localhost">>, []}]),
              assert_connections(Config, 0, Xs, [<<"localhost">>]),

              %% Add nonexistent connections to set - nothing breaks
              set_upstream_set(Config, 0,
                <<"new-set">>, [{<<"localhost">>, []},
                                {<<"does-not-exist">>, []}]),
              assert_connections(Config, 0, Xs, [<<"localhost">>]),

              %% Change connection in set - links change
              set_upstream_set(Config, 0, <<"new-set">>, [{<<"local5673">>, []}]),
              assert_connections(Config, 0, Xs, [<<"local5673">>])
      end, [x(<<"new.fed1">>), x(<<"new.fed2">>)]).

delete_federated_exchange_upstream(Config) ->
    %% If two exchanges in different virtual hosts have the same name, only one should be deleted.
    VH1 = <<"federation-downstream1">>,
    rabbit_ct_broker_helpers:delete_vhost(Config, VH1),
    rabbit_ct_broker_helpers:add_vhost(Config, VH1),
    rabbit_ct_broker_helpers:set_full_permissions(Config, <<"guest">>, VH1),
    VH2 = <<"federation-downstream2">>,
    rabbit_ct_broker_helpers:delete_vhost(Config, VH2),
    rabbit_ct_broker_helpers:add_vhost(Config, VH2),
    rabbit_ct_broker_helpers:set_full_permissions(Config, <<"guest">>, VH2),

    Conn1 = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0, VH1),
    Conn2 = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0, VH2),
    {ok, Ch1} = amqp_connection:open_channel(Conn1),
    {ok, Ch2} = amqp_connection:open_channel(Conn2),

    #'exchange.declare_ok'{} = declare_exchange(Ch1, #'exchange.declare'{exchange = <<"federated.topic">>,
                                                                         type     = <<"topic">>,
                                                                         durable  = true}),
    #'exchange.declare_ok'{} = declare_exchange(Ch2, #'exchange.declare'{exchange = <<"federated.topic">>,
                                                                         type     = <<"topic">>,
                                                                         durable  = true}),

    rabbit_ct_broker_helpers:rpc(Config, 0,
                                 rabbit_policy, set,
                                 [VH1,
                                  <<"federation">>, <<"^federated\.">>,
                                  [{<<"federation-upstream-set">>, <<"all">>}],
                                  0, <<"exchanges">>, <<"acting-user">>]),
    rabbit_ct_broker_helpers:rpc(Config, 0,
                                 rabbit_policy, set,
                                 [VH2,
                                  <<"federation">>, <<"^federated\.">>,
                                  [{<<"federation-upstream-set">>, <<"all">>}],
                                  0, <<"exchanges">>, <<"acting-user">>]),

    rabbit_ct_broker_helpers:set_parameter(Config, 0, VH1,
                                           <<"federation-upstream">>, <<"upstream">>,
                                           [{<<"uri">>, node_uri_with_virtual_host(Config, VH2)}]),
    rabbit_ct_broker_helpers:set_parameter(Config, 0, VH2,
                                           <<"federation-upstream">>, <<"upstream">>,
                                           [{<<"uri">>, node_uri_with_virtual_host(Config, VH1)}]),

    ?assertEqual(1, length(federation_links_in_vhost(Config, 0, VH1))),
    ?assertEqual(1, length(federation_links_in_vhost(Config, 0, VH2))),

    rabbit_ct_broker_helpers:clear_parameter(Config, 0, VH2,
                                             <<"federation-upstream">>, <<"upstream">>),

    %% one link is still around
    ?assertEqual(1, length(federation_links_in_vhost(Config, 0, VH1))),
    ?assertEqual(0, length(federation_links_in_vhost(Config, 0, VH2))),
    LinksInVH1 = federation_links_in_vhost(Config, 0, VH1),
    ?assertEqual(VH1, proplists:get_value(vhost, hd(LinksInVH1))),

    [rabbit_ct_broker_helpers:delete_vhost(Config, Val) || Val <- [VH1, VH2]].

delete_federated_queue_upstream(Config) ->
    %% If two queues in different virtual hosts have the same name, only one should be deleted.
    VH1 = <<"federation-downstream1">>,
    rabbit_ct_broker_helpers:delete_vhost(Config, VH1),
    rabbit_ct_broker_helpers:add_vhost(Config, VH1),
    rabbit_ct_broker_helpers:set_full_permissions(Config, <<"guest">>, VH1),
    VH2 = <<"federation-downstream2">>,
    rabbit_ct_broker_helpers:delete_vhost(Config, VH2),
    rabbit_ct_broker_helpers:add_vhost(Config, VH2),
    rabbit_ct_broker_helpers:set_full_permissions(Config, <<"guest">>, VH2),
    
    rabbit_ct_broker_helpers:rpc(Config, 0,
                                 rabbit_policy, set,
                                 [VH1,
                                  <<"federation">>, <<"^federated\.">>,
                                  [{<<"federation-upstream-set">>, <<"all">>}],
                                  0, <<"queues">>, <<"acting-user">>]),
    rabbit_ct_broker_helpers:rpc(Config, 0,
                                 rabbit_policy, set,
                                 [VH2,
                                  <<"federation">>, <<"^federated\.">>,
                                  [{<<"federation-upstream-set">>, <<"all">>}],
                                  0, <<"queues">>, <<"acting-user">>]),

    rabbit_ct_broker_helpers:set_parameter(Config, 0, VH1,
                                           <<"federation-upstream">>, <<"upstream">>,
                                           [{<<"uri">>, node_uri_with_virtual_host(Config, VH2)}]),
    rabbit_ct_broker_helpers:set_parameter(Config, 0, VH2,
                                           <<"federation-upstream">>, <<"upstream">>,
                                           [{<<"uri">>, node_uri_with_virtual_host(Config, VH1)}]),

    Conn1 = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0, VH1),
    Conn2 = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0, VH2),
    {ok, Ch1} = amqp_connection:open_channel(Conn1),
    {ok, Ch2} = amqp_connection:open_channel(Conn2),

    #'queue.declare_ok'{} = declare_queue(Ch1,
                                          #'queue.declare'{queue = <<"federated.queue">>,
                                                           durable = true}),
    #'queue.declare_ok'{} = declare_queue(Ch2,
                                          #'queue.declare'{queue = <<"federated.queue">>,
                                                           durable = true}),

    
    rabbit_ct_helpers:await_condition(
        fun () ->
            length(federation_links_in_vhost(Config, 0, VH1)) > 0 andalso
            length(federation_links_in_vhost(Config, 0, VH2)) > 0
        end),
    
    ?assertEqual(1, length(federation_links_in_vhost(Config, 0, VH1))),
    ?assertEqual(1, length(federation_links_in_vhost(Config, 0, VH2))),

    rabbit_ct_broker_helpers:clear_parameter(Config, 0, VH2,
                                             <<"federation-upstream">>, <<"upstream">>),

    ?assertEqual(1, length(federation_links_in_vhost(Config, 0, VH1))),
    ?assertEqual(0, length(federation_links_in_vhost(Config, 0, VH2))),
    LinksInVH1 = federation_links_in_vhost(Config, 0, VH1),
    ?assertEqual(VH1, proplists:get_value(vhost, hd(LinksInVH1))),

    [rabbit_ct_broker_helpers:delete_vhost(Config, Val) || Val <- [VH1, VH2]].

federate_unfederate(Config) ->
    with_ch(Config,
      fun (_Ch) ->
              Xs = [<<"dyn.exch1">>, <<"dyn.exch2">>],

              %% Declareda  non-federated exchanges - no links
              assert_connections(Config, 0, Xs, []),

              %% Federate them - links appear
              set_policy(Config, 0, <<"dyn">>, <<"^dyn\\.">>, <<"all">>),
              assert_connections(Config, 0, Xs, [<<"localhost">>, <<"local5673">>]),

              %% Change policy - links change
              set_policy(Config, 0, <<"dyn">>, <<"^dyn\\.">>, <<"localhost">>),
              assert_connections(Config, 0, Xs, [<<"localhost">>]),

              %% Unfederate them - links disappear
              clear_policy(Config, 0, <<"dyn">>),
              assert_connections(Config, 0, Xs, [])
      end, [x(<<"dyn.exch1">>), x(<<"dyn.exch2">>)]).

dynamic_plugin_stop_start(Config) ->
    X1 = <<"dyn.exch1">>,
    X2 = <<"dyn.exch2">>,
    with_ch(Config,
      fun (Ch) ->
              set_policy(Config, 0, <<"dyn">>, <<"^dyn\\.">>, <<"localhost">>),

              %% Declare federated exchange - get link
              assert_connections(Config, 0, [X1], [<<"localhost">>]),

              %% Disable plugin, link goes
              ok = rabbit_ct_broker_helpers:disable_plugin(Config, 0,
                "rabbitmq_federation"),
              %% We can't check with status for obvious reasons...
              undefined = rabbit_ct_broker_helpers:rpc(Config, 0,
                erlang, whereis, [rabbit_federation_sup]),
              {error, not_found} = rabbit_ct_broker_helpers:rpc(Config, 0,
                rabbit_registry, lookup_module,
                [exchange, 'x-federation-upstream']),

              %% Create exchange then re-enable plugin, links appear
              declare_exchange(Ch, x(X2)),
              ok = rabbit_ct_broker_helpers:enable_plugin(Config, 0,
                "rabbitmq_federation"),
              assert_connections(Config, 0, [X1, X2], [<<"localhost">>]),
              {ok, _} = rabbit_ct_broker_helpers:rpc(Config, 0,
                rabbit_registry, lookup_module,
                [exchange, 'x-federation-upstream']),

              wait_for_federation(
                90,
                fun() ->
                        VHost = <<"/">>,
                        Xs = rabbit_ct_broker_helpers:rpc(
                               Config, 0, rabbit_exchange, list, [VHost]),
                        L1 =
                        [X || X <- Xs,
                              X#exchange.type =:= 'x-federation-upstream'],
                        L2 =
                        [X || X <- Xs,
                              X#exchange.name =:= #resource{
                                                     virtual_host = VHost,
                                                     kind = exchange,
                                                     name = X1},
                              X#exchange.scratches =:= [{federation,
                                                         [{{<<"localhost">>,
                                                            X1},
                                                           <<"A">>}]}]],
                        L3 =
                        [X || X <- Xs,
                              X#exchange.name =:= #resource{
                                                     virtual_host = VHost,
                                                     kind = exchange,
                                                     name = X2},
                              X#exchange.scratches =:= [{federation,
                                                         [{{<<"localhost">>,
                                                            X2},
                                                           <<"B">>}]}]],
                        length(L1) =:= 2 andalso [] =/= L2 andalso [] =/= L3 andalso
                        has_internal_federated_queue(Config, 0, VHost)
                end),

              %% Test both exchanges work. They are just federated to
              %% themselves so should duplicate messages.
              [begin
                   Q = bind_queue(Ch, X, <<"key">>),
                   await_binding(Config, 0, X, <<"key">>, 2),
                   publish(Ch, X, <<"key">>, <<"HELLO">>),
                   expect(Ch, Q, [<<"HELLO">>, <<"HELLO">>]),
                   delete_queue(Ch, Q)
               end || X <- [X1, X2]],

              clear_policy(Config, 0, <<"dyn">>),
              assert_connections(Config, 0, [X1, X2], []),
              delete_exchange(Ch, X2)
      end, [x(X1)]).

dynamic_plugin_cleanup_stop_start(Config) ->
    X1 = <<"dyn.exch1">>,
    with_ch(Config,
      fun (_Ch) ->
              set_policy(Config, 0, <<"dyn">>, <<"^dyn\\.">>, <<"localhost">>),

              %% Declare a federated exchange, a link starts
              assert_connections(Config, 0, [X1], [<<"localhost">>]),
              wait_for_federation(
                90,
                fun() ->
                        VHost = <<"/">>,
                        Xs = rabbit_ct_broker_helpers:rpc(
                               Config, 0, rabbit_exchange, list, [VHost]),
                        L1 =
                        [X || X <- Xs,
                              X#exchange.type =:= 'x-federation-upstream'],
                        L2 =
                        [X || X <- Xs,
                              X#exchange.name =:= #resource{
                                                     virtual_host = VHost,
                                                     kind = exchange,
                                                     name = X1},
                              X#exchange.scratches =:= [{federation,
                                                         [{{<<"localhost">>,
                                                            X1},
                                                           <<"B">>}]}]],
                        [] =/= L1 andalso [] =/= L2 andalso
                        has_internal_federated_queue(Config, 0, VHost)
                end),

              ?assert(has_internal_federated_exchange(Config, 0, <<"/">>)),
              ?assert(has_internal_federated_queue(Config, 0, <<"/">>)),

              %% Disable plugin, link goes
              ok = rabbit_ct_broker_helpers:disable_plugin(Config, 0,
                "rabbitmq_federation"),

              %% Internal exchanges and queues need cleanup
              ?assert(not has_internal_federated_exchange(Config, 0, <<"/">>)),
              ?assert(not has_internal_federated_queue(Config, 0, <<"/">>)),

              ok = rabbit_ct_broker_helpers:enable_plugin(Config, 0,
                "rabbitmq_federation"),
              clear_policy(Config, 0, <<"dyn">>),
              assert_connections(Config, 0, [X1], [])
      end, [x(X1)]).

dynamic_policy_cleanup(Config) ->
    X1 = <<"dyn.exch1">>,
    with_ch(Config,
      fun (_Ch) ->
              set_policy(Config, 0, <<"dyn">>, <<"^dyn\\.">>, <<"localhost">>),

              %% Declare federated exchange - get link
              assert_connections(Config, 0, [X1], [<<"localhost">>]),
              wait_for_federation(
                90,
                fun() ->
                        VHost = <<"/">>,
                        Xs = rabbit_ct_broker_helpers:rpc(
                               Config, 0, rabbit_exchange, list, [VHost]),
                        L1 =
                        [X || X <- Xs,
                              X#exchange.type =:= 'x-federation-upstream'],
                        L2 =
                        [X || X <- Xs,
                              X#exchange.name =:= #resource{
                                                     virtual_host = VHost,
                                                     kind = exchange,
                                                     name = X1},
                              X#exchange.scratches =:= [{federation,
                                                         [{{<<"localhost">>,
                                                            X1},
                                                           <<"B">>}]}]],
                        [] =/= L1 andalso [] =/= L2 andalso
                        has_internal_federated_queue(Config, 0, VHost)
                end),

              ?assert(has_internal_federated_exchange(Config, 0, <<"/">>)),
              ?assert(has_internal_federated_queue(Config, 0, <<"/">>)),

              clear_policy(Config, 0, <<"dyn">>),
              timer:sleep(5000),

              %% Internal exchanges and queues need cleanup
              ?assert(not has_internal_federated_exchange(Config, 0, <<"/">>)),
              ?assert(not has_internal_federated_queue(Config, 0, <<"/">>)),

              clear_policy(Config, 0, <<"dyn">>),
              assert_connections(Config, 0, [X1], [])
      end, [x(X1)]).

has_internal_federated_exchange(Config, Node, VHost) ->
    lists:any(fun(X) ->
                      X#exchange.type == 'x-federation-upstream'
              end, rabbit_ct_broker_helpers:rpc(Config, Node,
                                                rabbit_exchange, list, [VHost])).

has_internal_federated_queue(Config, Node, VHost) ->
    lists:any(
      fun(Q) ->
              {'longstr', <<"federation">>} ==
                  rabbit_misc:table_lookup(amqqueue:get_arguments(Q), <<"x-internal-purpose">>)
      end, rabbit_ct_broker_helpers:rpc(Config, Node,
                                        rabbit_amqqueue, list, [VHost])).

%%----------------------------------------------------------------------------

with_ch(Config, Fun, Xs) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    declare_all(Ch, Xs),
    rabbit_federation_test_util:assert_status(Config, 0,
      Xs, {exchange, upstream_exchange}),
    Fun(Ch),
    delete_all(Ch, Xs),
    rabbit_ct_client_helpers:close_channel(Ch),
    cleanup(Config, 0),
    ok.

with_conn_and_ch(Config, Fun, Xs) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    declare_all(Ch, Xs),
    rabbit_federation_test_util:assert_status(Config, 0,
      Xs, {exchange, upstream_exchange}),
    Fun(Conn, Ch),
    delete_all(Ch, Xs),
    rabbit_ct_client_helpers:close_channel(Ch),
    cleanup(Config, 0),
    ok.

cleanup(Config, Node) ->
    [rabbit_ct_broker_helpers:rpc(
       Config, Node, rabbit_amqqueue, delete, [Q, false, false,
                                               <<"acting-user">>]) ||
        Q <- queues(Config, Node)].

queues(Config, Node) ->
    Ret = rabbit_ct_broker_helpers:rpc(Config, Node,
      rabbit_amqqueue, list, [<<"/">>]),
    case Ret of
        {badrpc, _} -> [];
        Qs          -> Qs
    end.

stop_other_node(Config, Node) ->
    cleanup(Config, Node),
    rabbit_federation_test_util:stop_other_node(Config, Node).

declare_all(Ch, Xs) -> [declare_exchange(Ch, X) || X <- Xs].
delete_all(Ch, Xs) ->
    [delete_exchange(Ch, X) || #'exchange.declare'{exchange = X} <- Xs].

declare_exchange(Ch, X) ->
    amqp_channel:call(Ch, X).

x(Name) -> x(Name, <<"topic">>).

x(Name, Type) ->
    #'exchange.declare'{exchange = Name,
                        type     = Type,
                        durable  = true}.

xr(Name) ->
    rabbit_misc:r(<<"/">>, exchange, Name).

xr(Vhost, Name) ->
    rabbit_misc:r(Vhost, exchange, Name).

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

bind_queue(Ch, X, Key) ->
    Q = declare_queue(Ch),
    bind_queue(Ch, Q, X, Key),
    Q.

delete_exchange(Ch, X) ->
    amqp_channel:call(Ch, #'exchange.delete'{exchange = X}).

delete_queue(Ch, Q) ->
    amqp_channel:call(Ch, #'queue.delete'{queue = Q}).

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
            timer:sleep(100),
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
    List = rabbit_ct_broker_helpers:rpc(Config, Node, rabbit_binding,
                                        list_for_source, [xr(Vhost, X)]),
    [K || #binding{key = K} <- List, K =:= Key].

publish(Ch, X, Key, Payload) when is_binary(Payload) ->
    publish(Ch, X, Key, #amqp_msg{payload = Payload});

publish(Ch, X, Key, Msg = #amqp_msg{}) ->
    amqp_channel:call(Ch, #'basic.publish'{exchange    = X,
                                           routing_key = Key}, Msg).

publish_expect(Ch, X, Key, Q, Payload) ->
    publish(Ch, X, Key, Payload),
    expect(Ch, Q, [Payload]).

%%----------------------------------------------------------------------------

assert_connections(Config, Node, Xs, Conns) ->
    rabbit_ct_broker_helpers:rpc(Config, Node,
      ?MODULE, assert_connections1, [Xs, Conns]).

assert_connections1(Xs, Conns) ->
    Links = [{X, C, X} ||
                X <- Xs,
                C <- Conns],
    Remaining = lists:foldl(
                  fun (Link, Status) ->
                          rabbit_federation_test_util:assert_link_status(
                            Link, Status, {exchange, upstream_exchange})
                  end, rabbit_federation_status:status(), Links),
    [] = Remaining,
    ok.

connection_pids(Config, Node) ->
    [P || [{pid, P}] <-
              rabbit_ct_broker_helpers:rpc(Config, Node,
                rabbit_networking, connection_info_all, [[pid]])].

upstream_downstream() ->
    [x(<<"upstream">>), x(<<"fed.downstream">>)].
