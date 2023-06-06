%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stream_manager_SUITE).

-include_lib("eunit/include/eunit.hrl").
% -include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(nowarn_export_all).
-compile(export_all).

all() ->
    [{group, non_parallel_tests}].

groups() ->
    [{non_parallel_tests, [],
      [manage_super_stream_exchange_type_direct,
       manage_super_stream_exchange_type_x_super_stream,
       route_direct_super_stream,
       lookup_leader,
       lookup_member,
       partition_index,
       partition_index_x_super_stream]}].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            {skip, "mixed version clusters are not supported"};
        _ ->
            rabbit_ct_helpers:log_environment(),
            Config
    end.

end_per_suite(Config) ->
    Config.

init_per_group(_, Config) ->
    Config1 =
        rabbit_ct_helpers:set_config(Config, [{rmq_nodes_clustered, false}]),
    Config2 =
        rabbit_ct_helpers:set_config(Config1,
                                     {rabbitmq_ct_tls_verify, verify_none}),
    Config3 =
        rabbit_ct_helpers:set_config(Config2, {rabbitmq_stream, verify_none}),
    rabbit_ct_helpers:run_setup_steps(Config3,
                                      [fun(StepConfig) ->
                                          rabbit_ct_helpers:merge_app_env(StepConfig,
                                                                          {rabbit,
                                                                           [{core_metrics_gc_interval,
                                                                             1000}]})
                                       end,
                                       fun(StepConfig) ->
                                          rabbit_ct_helpers:merge_app_env(StepConfig,
                                                                          {rabbitmq_stream,
                                                                           [{connection_negotiation_step_timeout,
                                                                             500}]})
                                       end]
                                      ++ rabbit_ct_broker_helpers:setup_steps()).

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

lookup_leader(Config) ->
    Stream = <<"stream_manager_lookup_leader_stream">>,
    ?assertMatch({ok, _}, create_stream(Config, Stream)),

    {ok, Pid} = lookup_leader(Config, Stream),
    ?assert(is_pid(Pid)),

    ?assertEqual({error, not_found}, lookup_leader(Config, <<"foo">>)),

    ?assertEqual({ok, deleted}, delete_stream(Config, Stream)).

lookup_member(Config) ->
    Stream = <<"stream_manager_lookup_member_stream">>,
    ?assertMatch({ok, _}, create_stream(Config, Stream)),

    {ok, Pid} = lookup_member(Config, Stream),
    ?assert(is_pid(Pid)),

    ?assertEqual({error, not_found}, lookup_member(Config, <<"foo">>)),

    ?assertEqual({ok, deleted}, delete_stream(Config, Stream)).

manage_super_stream_exchange_type_direct(Config) ->
    manage_super_stream(Config, <<"direct">>).

manage_super_stream_exchange_type_x_super_stream(Config) ->
    manage_super_stream(Config, <<"x-super-stream">>).

manage_super_stream(Config, Type) ->
    % create super stream
    ?assertEqual(ok,
                 create_super_stream(Config,
                                     #{name => <<"invoices">>,
                                       exchange_type => Type,
                                       partitions_source => {partitions, 3}})),
    % get the correct partitions
    ?assertEqual({ok,
                  [<<"invoices-0">>, <<"invoices-1">>, <<"invoices-2">>]},
                 partitions(Config, <<"invoices">>)),

    % get an error if trying to re-create it
    ?assertMatch({error, _},
                 create_super_stream(Config,
                                     #{name => <<"invoices">>,
                                       exchange_type => Type,
                                       partitions_source => {partitions, 3}})),

    % can delete it
    ?assertEqual(ok, delete_super_stream(Config, <<"invoices">>)),

    % create a stream with the same name as a potential partition
    ?assertMatch({ok, _}, create_stream(Config, <<"invoices-1">>)),

    % cannot create the super stream because a partition already exists
    ?assertMatch({error, _},
                 create_super_stream(Config,
                                     #{name => <<"invoices">>,
                                       exchange_type => Type,
                                       partitions_source => {partitions, 3}})),

    ?assertMatch({ok, _}, delete_stream(Config, <<"invoices-1">>)),
    ok.

route_direct_super_stream(Config) ->
    % create super stream
    ?assertEqual(ok,
                 create_super_stream(Config,
                                     #{name => <<"invoices">>,
                                       exchange_type => <<"direct">>,
                                       partitions_source => {partitions, 3}})),
    % get the correct partitions
    ?assertEqual({ok,
                  [<<"invoices-0">>, <<"invoices-1">>, <<"invoices-2">>]},
                 partitions(Config, <<"invoices">>)),

    [?assertEqual({ok, [Partition]},
                  route(Config, RoutingKey, <<"invoices">>))
     || {Partition, RoutingKey}
            <- [{<<"invoices-0">>, <<"0">>}, {<<"invoices-1">>, <<"1">>},
                {<<"invoices-2">>, <<"2">>}]],
    ?assertEqual(ok, delete_super_stream(Config, <<"invoices">>)),
    ok.

partition_index(Config) ->
    % create super stream
    ?assertEqual(ok,
                 create_super_stream(Config,
                                     #{name => <<"invoices">>,
                                       exchange_type => <<"direct">>,
                                       partitions_source => {partitions, 3}})),
    [?assertEqual({ok, Index},
                  partition_index(Config, <<"invoices">>, Stream))
     || {Index, Stream}
            <- [{0, <<"invoices-0">>}, {1, <<"invoices-1">>},
                {2, <<"invoices-2">>}]],

    ?assertEqual(ok, delete_super_stream(Config, <<"invoices">>)),

    C = start_amqp_connection(Config),
    {ok, Ch} = amqp_connection:open_channel(C),

    StreamsWithIndexes =
        [<<"invoices-0">>, <<"invoices-1">>, <<"invoices-2">>],
    create_super_stream_topology(<<"invoices">>, StreamsWithIndexes, Ch),

    [?assertEqual({ok, Index},
                  partition_index(Config, <<"invoices">>, Stream))
     || {Index, Stream}
            <- [{0, <<"invoices-0">>}, {1, <<"invoices-1">>},
                {2, <<"invoices-2">>}]],

    delete_super_stream_topology(<<"invoices">>, StreamsWithIndexes, Ch),

    StreamsWithNoIndexes =
        [<<"invoices-amer">>, <<"invoices-emea">>, <<"invoices-apac">>],
    create_super_stream_topology(<<"invoices">>, StreamsWithNoIndexes,
                                 Ch),

    [?assertEqual({ok, -1},
                  partition_index(Config, <<"invoices">>, Stream))
     || Stream
            <- [<<"invoices-amer">>, <<"invoices-emea">>, <<"invoices-apac">>]],

    delete_super_stream_topology(<<"invoices">>, StreamsWithNoIndexes,
                                 Ch),

    amqp_connection:close(C),
    ok.

partition_index_x_super_stream(Config) ->
    % create super stream
    ?assertEqual(ok,
                 create_super_stream(Config,
                                     #{name => <<"invoices">>,
                                       exchange_type => <<"x-super-stream">>,
                                       partitions_source => {partitions, 3}})),
    [?assertEqual({ok, Index},
                  partition_index(Config, <<"invoices">>, Stream))
     || {Index, Stream}
            <- [{0, <<"invoices-0">>}, {1, <<"invoices-1">>},
                {2, <<"invoices-2">>}]],

    ?assertEqual({error, stream_not_found},
                 partition_index(Config, <<"invoices">>,
                                 <<"bananas-gorilla">>)),

    ?assertEqual(ok, delete_super_stream(Config, <<"invoices">>)),

    ok.

create_super_stream(Config, Spec0) ->
    Spec = Spec0#{vhost => <<"/">>,
                  username => <<"guest">>},
    rabbit_ct_broker_helpers:rpc(Config,
                                 0,
                                 rabbit_stream_manager,
                                 create_super_stream,
                                 [Spec]).

delete_super_stream(Config, Name) ->
    rabbit_ct_broker_helpers:rpc(Config,
                                 0,
                                 rabbit_stream_manager,
                                 delete_super_stream,
                                 [<<"/">>, Name, <<"guest">>]).

create_stream(Config, Name) ->
    rabbit_ct_broker_helpers:rpc(Config,
                                 0,
                                 rabbit_stream_manager,
                                 create,
                                 [<<"/">>, Name, [], <<"guest">>]).

delete_stream(Config, Name) ->
    rabbit_ct_broker_helpers:rpc(Config,
                                 0,
                                 rabbit_stream_manager,
                                 delete,
                                 [<<"/">>, Name, <<"guest">>]).

lookup_leader(Config, Name) ->
    rabbit_ct_broker_helpers:rpc(Config,
                                 0,
                                 rabbit_stream_manager,
                                 lookup_leader,
                                 [<<"/">>, Name]).

lookup_member(Config, Name) ->
    rabbit_ct_broker_helpers:rpc(Config,
                                 0,
                                 rabbit_stream_manager,
                                 lookup_member,
                                 [<<"/">>, Name]).

partitions(Config, Name) ->
    rabbit_ct_broker_helpers:rpc(Config,
                                 0,
                                 rabbit_stream_manager,
                                 partitions,
                                 [<<"/">>, Name]).

route(Config, RoutingKey, SuperStream) ->
    rabbit_ct_broker_helpers:rpc(Config,
                                 0,
                                 rabbit_stream_manager,
                                 route,
                                 [RoutingKey, <<"/">>, SuperStream]).

partition_index(Config, SuperStream, Stream) ->
    rabbit_ct_broker_helpers:rpc(Config,
                                 0,
                                 rabbit_stream_manager,
                                 partition_index,
                                 [<<"/">>, SuperStream, Stream]).

start_amqp_connection(Config) ->
    Port =
        rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    {ok, Connection} =
        amqp_connection:start(#amqp_params_network{port = Port}),
    Connection.

create_super_stream_topology(SuperStream, Streams, Ch) ->
    ExchangeDeclare =
        #'exchange.declare'{exchange = SuperStream,
                            type = <<"direct">>,
                            passive = false,
                            durable = true,
                            auto_delete = false,
                            internal = false,
                            nowait = false,
                            arguments = []},
    #'exchange.declare_ok'{} = amqp_channel:call(Ch, ExchangeDeclare),

    [begin
         QueueDeclare =
             #'queue.declare'{queue = S,
                              durable = true,
                              exclusive = false,
                              auto_delete = false,
                              arguments =
                                  [{<<"x-queue-type">>, longstr,
                                    <<"stream">>}]},
         #'queue.declare_ok'{} = amqp_channel:call(Ch, QueueDeclare),
         Binding =
             #'queue.bind'{queue = S,
                           exchange = SuperStream,
                           routing_key = S},
         #'queue.bind_ok'{} = amqp_channel:call(Ch, Binding)
     end
     || S <- Streams],
    ok.

delete_super_stream_topology(SuperStream, Streams, Ch) ->
    DeleteExchange = #'exchange.delete'{exchange = SuperStream},
    #'exchange.delete_ok'{} = amqp_channel:call(Ch, DeleteExchange),

    [begin
         DeleteQueue = #'queue.delete'{queue = S},
         #'queue.delete_ok'{} = amqp_channel:call(Ch, DeleteQueue)
     end
     || S <- Streams],
    ok.
