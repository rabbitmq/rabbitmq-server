%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(commands_SUITE).

-compile(nowarn_export_all).
-compile([export_all]).

% -include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").
-include_lib("rabbitmq_stream_common/include/rabbit_stream.hrl").

-define(WAIT, 5000).
-define(COMMAND_LIST_CONNECTIONS,
        'Elixir.RabbitMQ.CLI.Ctl.Commands.ListStreamConnectionsCommand').
-define(COMMAND_LIST_CONSUMERS,
        'Elixir.RabbitMQ.CLI.Ctl.Commands.ListStreamConsumersCommand').
-define(COMMAND_LIST_PUBLISHERS,
        'Elixir.RabbitMQ.CLI.Ctl.Commands.ListStreamPublishersCommand').
-define(COMMAND_ADD_SUPER_STREAM,
        'Elixir.RabbitMQ.CLI.Ctl.Commands.AddSuperStreamCommand').
-define(COMMAND_DELETE_SUPER_STREAM_CLI,
        'Elixir.RabbitMQ.CLI.Ctl.Commands.DeleteSuperStreamCommand').
-define(COMMAND_LIST_CONSUMER_GROUPS,
        'Elixir.RabbitMQ.CLI.Ctl.Commands.ListStreamConsumerGroupsCommand').
-define(COMMAND_LIST_GROUP_CONSUMERS,
        'Elixir.RabbitMQ.CLI.Ctl.Commands.ListStreamGroupConsumersCommand').
-define(COMMAND_LIST_STREAM_TRACKING,
        'Elixir.RabbitMQ.CLI.Ctl.Commands.ListStreamTrackingCommand').
-define(COMMAND_ACTIVATE_STREAM_CONSUMER,
        'Elixir.RabbitMQ.CLI.Ctl.Commands.ActivateStreamConsumerCommand').
-define(COMMAND_RESET_OFFSET,
        'Elixir.RabbitMQ.CLI.Ctl.Commands.ResetOffsetCommand').

all() ->
    [{group, list_connections},
     {group, list_consumers},
     {group, list_publishers},
     {group, list_consumer_groups},
     {group, list_group_consumers},
     {group, activate_consumer},
     {group, list_stream_tracking},
     {group, reset_offset},
     {group, super_streams}].

groups() ->
    [{list_connections, [],
      [list_connections_merge_defaults, list_connections_run,
       list_tls_connections_run]},
     {list_consumers, [],
      [list_consumers_merge_defaults, list_consumers_run]},
     {list_publishers, [],
      [list_publishers_merge_defaults, list_publishers_run]},
     {list_consumer_groups, [],
      [list_consumer_groups_validate, list_consumer_groups_merge_defaults,
       list_consumer_groups_run]},
     {list_group_consumers, [],
      [list_group_consumers_validate, list_group_consumers_merge_defaults,
       list_group_consumers_run]},
     {activate_consumer, [],
      [activate_consumer_validate, activate_consumer_merge_defaults,
       activate_consumer_run]},
     {list_stream_tracking, [],
      [list_stream_tracking_validate, list_stream_tracking_merge_defaults,
       list_stream_tracking_run]},
     {reset_offset, [],
      [reset_offset_validate, reset_offset_merge_defaults,
       reset_offset_run]},
     {super_streams, [],
      [add_super_stream_merge_defaults,
       add_super_stream_validate,
       delete_super_stream_merge_defaults,
       delete_super_stream_validate,
       add_delete_super_stream_run]}].

init_per_suite(Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            {skip,
             "mixed version clusters are not supported for "
             "this suite"};
        _ ->
            Config1 =
                rabbit_ct_helpers:set_config(Config,
                                             [{rmq_nodename_suffix, ?MODULE}]),
            Config2 =
                rabbit_ct_helpers:set_config(Config1,
                                             {rabbitmq_ct_tls_verify,
                                              verify_none}),
            rabbit_ct_helpers:log_environment(),
            rabbit_ct_helpers:run_setup_steps(Config2,
                                              rabbit_ct_broker_helpers:setup_steps())
    end.

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
                                         rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

list_connections_merge_defaults(_Config) ->
    {[<<"node">>, <<"conn_name">>], #{verbose := false}} =
        ?COMMAND_LIST_CONNECTIONS:merge_defaults([], #{}),

    {[<<"other_key">>], #{verbose := true}} =
        ?COMMAND_LIST_CONNECTIONS:merge_defaults([<<"other_key">>],
                                                 #{verbose => true}),

    {[<<"other_key">>], #{verbose := false}} =
        ?COMMAND_LIST_CONNECTIONS:merge_defaults([<<"other_key">>],
                                                 #{verbose => false}).

list_connections_run(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Opts =
        #{node => Node,
          timeout => 10000,
          verbose => false},

    %% No connections
    [] = to_list(?COMMAND_LIST_CONNECTIONS:run([], Opts)),

    StreamPort = rabbit_stream_SUITE:get_stream_port(Config),

    {S1, C1} = start_stream_connection(StreamPort),
    ?awaitMatch(1, connection_count(Config), ?WAIT),

    [[{conn_name, _}]] =
        to_list(?COMMAND_LIST_CONNECTIONS:run([<<"conn_name">>], Opts)),
    [[{ssl, false}]] =
        to_list(?COMMAND_LIST_CONNECTIONS:run([<<"ssl">>], Opts)),

    {S2, C2} = start_stream_connection(StreamPort),
    ?awaitMatch(2, connection_count(Config), ?WAIT),

    [[{conn_name, _}], [{conn_name, _}]] =
        to_list(?COMMAND_LIST_CONNECTIONS:run([<<"conn_name">>], Opts)),

    Port =
        rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    start_amqp_connection(network, Node, Port),

    %% There are still just two connections
    [[{conn_name, _}], [{conn_name, _}]] =
        to_list(?COMMAND_LIST_CONNECTIONS:run([<<"conn_name">>], Opts)),

    start_amqp_connection(direct, Node, Port),

    %% Still two stream connections, one direct AMQP 0-9-1 connection
    [[{conn_name, _}], [{conn_name, _}]] =
        to_list(?COMMAND_LIST_CONNECTIONS:run([<<"conn_name">>], Opts)),

    %% Verbose returns all keys
    Infos =
        lists:map(fun(El) -> atom_to_binary(El, utf8) end, ?INFO_ITEMS),
    AllKeys = to_list(?COMMAND_LIST_CONNECTIONS:run(Infos, Opts)),
    Verbose =
        to_list(?COMMAND_LIST_CONNECTIONS:run([], Opts#{verbose => true})),
    ?assertEqual(AllKeys, Verbose),

    %% There are two connections
    [First, _Second] = AllKeys,

    %% Keys are INFO_ITEMS
    ?assertEqual(length(?INFO_ITEMS), length(First)),

    {Keys, _} = lists:unzip(First),

    ?assertEqual([], Keys -- ?INFO_ITEMS),
    ?assertEqual([], ?INFO_ITEMS -- Keys),

    rabbit_stream_SUITE:test_close(gen_tcp, S1, C1),
    rabbit_stream_SUITE:test_close(gen_tcp, S2, C2),
    ok.

list_tls_connections_run(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Opts =
        #{node => Node,
          timeout => 10000,
          verbose => false},

    %% No connections
    [] = to_list(?COMMAND_LIST_CONNECTIONS:run([], Opts)),

    StreamTlsPort = rabbit_stream_SUITE:get_stream_port_tls(Config),
    application:ensure_all_started(ssl),

    {S1, C1} = start_stream_tls_connection(StreamTlsPort),
    ?awaitMatch(1, connection_count(Config), ?WAIT),

    [[{conn_name, _}]] =
        to_list(?COMMAND_LIST_CONNECTIONS:run([<<"conn_name">>], Opts)),
    [[{ssl, true}]] =
        to_list(?COMMAND_LIST_CONNECTIONS:run([<<"ssl">>], Opts)),

    rabbit_stream_SUITE:test_close(ssl, S1, C1),
    ok.

list_consumers_merge_defaults(_Config) ->
    DefaultItems =
        [rabbit_data_coercion:to_binary(Item)
         || Item <- ?CONSUMER_INFO_ITEMS -- [connection_pid, node]],
    {DefaultItems, #{verbose := false}} =
        ?COMMAND_LIST_CONSUMERS:merge_defaults([], #{}),

    {[<<"other_key">>], #{verbose := true}} =
        ?COMMAND_LIST_CONSUMERS:merge_defaults([<<"other_key">>],
                                               #{verbose => true}),

    {[<<"other_key">>], #{verbose := false}} =
        ?COMMAND_LIST_CONSUMERS:merge_defaults([<<"other_key">>],
                                               #{verbose => false}).

list_consumers_run(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Opts =
        #{node => Node,
          timeout => 10000,
          verbose => false,
          vhost => <<"/">>},

    %% No connections, no consumers
    [] = to_list(?COMMAND_LIST_CONSUMERS:run([], Opts)),

    StreamPort = rabbit_stream_SUITE:get_stream_port(Config),
    {S1, C1} = start_stream_connection(StreamPort),
    ?awaitMatch(1, connection_count(Config), ?WAIT),

    Stream = <<"list_consumers_run">>,
    C1_1 = create_stream(S1, Stream, C1),
    SubId = 42,
    C1_2 = subscribe(S1, SubId, Stream, C1_1),

    ?awaitMatch(1, consumer_count(Config), ?WAIT),

    {S2, C2} = start_stream_connection(StreamPort),
    ?awaitMatch(2, connection_count(Config), ?WAIT),
    C2_1 = subscribe(S2, SubId, Stream, C2),

    ?awaitMatch(2, consumer_count(Config), ?WAIT),

    %% Verbose returns all keys
    InfoItems = ?CONSUMER_INFO_ITEMS,
    Infos = lists:map(fun(El) -> atom_to_binary(El, utf8) end, InfoItems),
    AllKeys = to_list(?COMMAND_LIST_CONSUMERS:run(Infos, Opts)),
    Verbose =
        to_list(?COMMAND_LIST_CONSUMERS:run([], Opts#{verbose => true})),
    ?assertEqual(AllKeys, Verbose),
    %% There are two consumers
    [First, _Second] = AllKeys,

    %% Keys are info items
    ?assertEqual(length(InfoItems), length(First)),

    {Keys, _} = lists:unzip(First),

    ?assertEqual([], Keys -- InfoItems),
    ?assertEqual([], InfoItems -- Keys),

    C1_3 = delete_stream(S1, Stream, C1_2),
    % metadata_update_stream_deleted(S1, Stream),
    metadata_update_stream_deleted(S2, Stream, C2_1),
    close(S1, C1_3),
    close(S2, C2_1),
    ?awaitMatch(0, consumer_count(Config), ?WAIT),
    ok.

list_publishers_merge_defaults(_Config) ->
    DefaultItems =
        [rabbit_data_coercion:to_binary(Item)
         || Item <- ?PUBLISHER_INFO_ITEMS -- [connection_pid, node]],
    {DefaultItems, #{verbose := false}} =
        ?COMMAND_LIST_PUBLISHERS:merge_defaults([], #{}),

    {[<<"other_key">>], #{verbose := true}} =
        ?COMMAND_LIST_PUBLISHERS:merge_defaults([<<"other_key">>],
                                                #{verbose => true}),

    {[<<"other_key">>], #{verbose := false}} =
        ?COMMAND_LIST_PUBLISHERS:merge_defaults([<<"other_key">>],
                                                #{verbose => false}).

list_publishers_run(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Opts =
        #{node => Node,
          timeout => 10000,
          verbose => false,
          vhost => <<"/">>},

    %% No connections, no publishers
    [] = to_list(?COMMAND_LIST_PUBLISHERS:run([], Opts)),

    StreamPort = rabbit_stream_SUITE:get_stream_port(Config),
    {S1, C1} = start_stream_connection(StreamPort),
    ?awaitMatch(1, connection_count(Config), ?WAIT),

    Stream = <<"list_publishers_run">>,
    C1_1 = create_stream(S1, Stream, C1),
    PubId = 42,
    C1_2 = declare_publisher(S1, PubId, Stream, C1_1),

    ?awaitMatch(1, publisher_count(Config), ?WAIT),

    {S2, C2} = start_stream_connection(StreamPort),
    ?awaitMatch(2, connection_count(Config), ?WAIT),
    C2_1 = declare_publisher(S2, PubId, Stream, C2),

    ?awaitMatch(2, publisher_count(Config), ?WAIT),

    %% Verbose returns all keys
    InfoItems = ?PUBLISHER_INFO_ITEMS,
    Infos = lists:map(fun(El) -> atom_to_binary(El, utf8) end, InfoItems),
    AllKeys = to_list(?COMMAND_LIST_PUBLISHERS:run(Infos, Opts)),
    Verbose =
        to_list(?COMMAND_LIST_PUBLISHERS:run([], Opts#{verbose => true})),
    ?assertEqual(AllKeys, Verbose),
    %% There are two publishers
    [First, _Second] = AllKeys,

    %% Keys are info items
    ?assertEqual(length(InfoItems), length(First)),

    {Keys, _} = lists:unzip(First),

    ?assertEqual([], Keys -- InfoItems),
    ?assertEqual([], InfoItems -- Keys),

    C1_3 = delete_stream(S1, Stream, C1_2),
    % metadata_update_stream_deleted(S1, Stream),
    C2_2 = metadata_update_stream_deleted(S2, Stream, C2_1),
    close(S1, C1_3),
    close(S2, C2_2),
    ?awaitMatch(0, publisher_count(Config), ?WAIT),
    ok.

list_consumer_groups_validate(_) ->
    ValidOpts = #{vhost => <<"/">>},
    ?assertMatch({validation_failure, {bad_info_key, [foo]}},
                 ?COMMAND_LIST_CONSUMER_GROUPS:validate([<<"foo">>],
                                                        ValidOpts)),
    ?assertMatch(ok,
                 ?COMMAND_LIST_CONSUMER_GROUPS:validate([<<"reference">>],
                                                        ValidOpts)),
    ?assertMatch(ok,
                 ?COMMAND_LIST_CONSUMER_GROUPS:validate([], ValidOpts)).


list_consumer_groups_merge_defaults(_Config) ->
    DefaultItems =
        [rabbit_data_coercion:to_binary(Item)
         || Item <- ?CONSUMER_GROUP_INFO_ITEMS],
    {DefaultItems, #{verbose := false, vhost := <<"/">>}} =
        ?COMMAND_LIST_CONSUMER_GROUPS:merge_defaults([], #{}),

    {[<<"other_key">>], #{verbose := true, vhost := <<"/">>}} =
        ?COMMAND_LIST_CONSUMER_GROUPS:merge_defaults([<<"other_key">>],
                                                     #{verbose => true}),

    {[<<"other_key">>], #{verbose := false, vhost := <<"/">>}} =
        ?COMMAND_LIST_CONSUMER_GROUPS:merge_defaults([<<"other_key">>],
                                                     #{verbose => false}).

list_consumer_groups_run(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Opts =
        #{node => Node,
          timeout => 10000,
          vhost => <<"/">>,
          verbose => true},

    %% No connections, no consumers
    {ok, []} = ?COMMAND_LIST_CONSUMER_GROUPS:run([], Opts),

    StreamPort = rabbit_stream_SUITE:get_stream_port(Config),
    {S, C0} = start_stream_connection(StreamPort),
    ?awaitMatch(1, connection_count(Config), ?WAIT),

    ConsumerReference = <<"foo">>,
    SubProperties =
        #{<<"single-active-consumer">> => <<"true">>,
          <<"name">> => ConsumerReference},

    Stream1 = <<"list_consumer_groups_run_1">>,
    C1 = create_stream(S, Stream1, C0),
    C2 = subscribe(S, 0, Stream1, SubProperties, C1),
    C3 = handle_consumer_update(S, C2, 0),
    C4 = subscribe(S, 1, Stream1, SubProperties, C3),
    C5 = subscribe(S, 2, Stream1, SubProperties, C4),

    ?awaitMatch(3, consumer_count(Config), ?WAIT),

    {ok, [CG1]} = ?COMMAND_LIST_CONSUMER_GROUPS:run([], Opts),
    assertConsumerGroup(Stream1, ConsumerReference, -1, 3, CG1),

    Stream2 = <<"list_consumer_groups_run_2">>,
    C6 = create_stream(S, Stream2, C5),
    C7 = subscribe(S, 3, Stream2, SubProperties, C6),
    C8 = handle_consumer_update(S, C7, 3),
    C9 = subscribe(S, 4, Stream2, SubProperties, C8),
    C10 = subscribe(S, 5, Stream2, SubProperties, C9),

    ?awaitMatch(3 + 3, consumer_count(Config), ?WAIT),

    {ok, [CG1, CG2]} = ?COMMAND_LIST_CONSUMER_GROUPS:run([], Opts),
    assertConsumerGroup(Stream1, ConsumerReference, -1, 3, CG1),
    assertConsumerGroup(Stream2, ConsumerReference, -1, 3, CG2),

    C11 = delete_stream(S, Stream1, C10),
    C12 = delete_stream(S, Stream2, C11),

    close(S, C12),
    {ok, []} = ?COMMAND_LIST_CONSUMER_GROUPS:run([], Opts),
    ok.

list_group_consumers_validate(_) ->
    ValidOpts =
        #{vhost => <<"/">>,
          stream => <<"s1">>,
          reference => <<"foo">>},
    ?assertMatch({validation_failure, not_enough_args},
                 ?COMMAND_LIST_GROUP_CONSUMERS:validate([], #{})),
    ?assertMatch({validation_failure, not_enough_args},
                 ?COMMAND_LIST_GROUP_CONSUMERS:validate([],
                                                        #{vhost =>
                                                              <<"test">>})),
    ?assertMatch({validation_failure, {bad_info_key, [foo]}},
                 ?COMMAND_LIST_GROUP_CONSUMERS:validate([<<"foo">>],
                                                        ValidOpts)),
    ?assertMatch(ok,
                 ?COMMAND_LIST_GROUP_CONSUMERS:validate([<<"subscription_id">>],
                                                        ValidOpts)),
    ?assertMatch(ok,
                 ?COMMAND_LIST_GROUP_CONSUMERS:validate([], ValidOpts)).

list_group_consumers_merge_defaults(_Config) ->
    DefaultItems =
        [rabbit_data_coercion:to_binary(Item)
         || Item <- ?GROUP_CONSUMER_INFO_ITEMS],
    {DefaultItems, #{verbose := false, vhost := <<"/">>}} =
        ?COMMAND_LIST_GROUP_CONSUMERS:merge_defaults([], #{}),

    {[<<"other_key">>], #{verbose := true, vhost := <<"/">>}} =
        ?COMMAND_LIST_GROUP_CONSUMERS:merge_defaults([<<"other_key">>],
                                                     #{verbose => true}),

    {[<<"other_key">>], #{verbose := false, vhost := <<"/">>}} =
        ?COMMAND_LIST_GROUP_CONSUMERS:merge_defaults([<<"other_key">>],
                                                     #{verbose => false}).

list_group_consumers_run(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Opts =
        #{node => Node,
          timeout => 10000,
          vhost => <<"/">>,
          verbose => false},
    Args = [<<"subscription_id">>, <<"state">>],

    Stream1 = <<"list_group_consumers_run_1">>,
    ConsumerReference = <<"foo">>,
    OptsGroup1 =
        maps:merge(#{stream => Stream1, reference => ConsumerReference},
                   Opts),

    %% the group does not exist yet
    {error, not_found} =
        ?COMMAND_LIST_GROUP_CONSUMERS:run(Args, OptsGroup1),

    StreamPort = rabbit_stream_SUITE:get_stream_port(Config),
    {S, C} = start_stream_connection(StreamPort),
    ?awaitMatch(1, connection_count(Config), ?WAIT),

    SubProperties =
        #{<<"single-active-consumer">> => <<"true">>,
          <<"name">> => ConsumerReference},

    create_stream(S, Stream1, C),
    subscribe(S, 0, Stream1, SubProperties, C),
    handle_consumer_update(S, C, 0),
    subscribe(S, 1, Stream1, SubProperties, C),
    subscribe(S, 2, Stream1, SubProperties, C),

    ?awaitMatch(3, consumer_count(Config), ?WAIT),

    {ok, Consumers1} =
        ?COMMAND_LIST_GROUP_CONSUMERS:run(Args, OptsGroup1),
    ?assertEqual([[{subscription_id, 0}, {state, "active (connected)"}],
                  [{subscription_id, 1}, {state, "waiting (connected)"}],
                  [{subscription_id, 2}, {state, "waiting (connected)"}]],
                 Consumers1),

    Stream2 = <<"list_group_consumers_run_2">>,
    OptsGroup2 =
        maps:merge(#{stream => Stream2, reference => ConsumerReference},
                   Opts),

    create_stream(S, Stream2, C),
    subscribe(S, 3, Stream2, SubProperties, C),
    handle_consumer_update(S, C, 3),
    subscribe(S, 4, Stream2, SubProperties, C),
    subscribe(S, 5, Stream2, SubProperties, C),

    ?awaitMatch(3 + 3, consumer_count(Config), ?WAIT),

    {ok, Consumers2} =
        ?COMMAND_LIST_GROUP_CONSUMERS:run(Args, OptsGroup2),
    ?assertEqual([[{subscription_id, 3}, {state, "active (connected)"}],
                  [{subscription_id, 4}, {state, "waiting (connected)"}],
                  [{subscription_id, 5}, {state, "waiting (connected)"}]],
                 Consumers2),

    delete_stream(S, Stream1, C),
    delete_stream(S, Stream2, C),

    {error, not_found} =
        ?COMMAND_LIST_GROUP_CONSUMERS:run(Args, OptsGroup2),

    close(S, C),
    ok.

activate_consumer_validate(_) ->
    Cmd = ?COMMAND_ACTIVATE_STREAM_CONSUMER,
    ValidOpts = #{vhost => <<"/">>,
                  stream => <<"s1">>,
                  reference => <<"foo">>},
    ?assertMatch({validation_failure, not_enough_args},
                 Cmd:validate([], #{})),
    ?assertMatch({validation_failure, not_enough_args},
                 Cmd:validate([], #{vhost => <<"test">>})),
    ?assertMatch({validation_failure, too_many_args},
                 Cmd:validate([<<"foo">>], ValidOpts)),
    ?assertMatch(ok, Cmd:validate([], ValidOpts)).

activate_consumer_merge_defaults(_Config) ->
    Cmd = ?COMMAND_ACTIVATE_STREAM_CONSUMER,
    Opts = #{vhost => <<"/">>,
             stream => <<"s1">>,
             reference => <<"foo">>},
    ?assertEqual({[], Opts},
                 Cmd:merge_defaults([], maps:without([vhost], Opts))),
    Merged = maps:merge(Opts, #{vhost => "vhost"}),
    ?assertEqual({[], Merged},
                 Cmd:merge_defaults([], Merged)).

activate_consumer_run(Config) ->
    Cmd = ?COMMAND_ACTIVATE_STREAM_CONSUMER,
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Opts =#{node => Node,
            timeout => 10000,
            vhost => <<"/">>},
    Args = [],

    St = atom_to_binary(?FUNCTION_NAME, utf8),
    ConsumerReference = <<"foo">>,
    OptsGroup = maps:merge(#{stream => St, reference => ConsumerReference},
                            Opts),

    %% the group does not exist yet
    ?assertEqual({error, not_found}, Cmd:run(Args, OptsGroup)),

    StreamPort = rabbit_stream_SUITE:get_stream_port(Config),
    {S, C} = start_stream_connection(StreamPort),
    ?awaitMatch(1, connection_count(Config), ?WAIT),

    SubProperties =#{<<"single-active-consumer">> => <<"true">>,
                     <<"name">> => ConsumerReference},

    create_stream(S, St, C),
    subscribe(S, 0, St, SubProperties, C),
    handle_consumer_update(S, C, 0),
    subscribe(S, 1, St, SubProperties, C),
    subscribe(S, 2, St, SubProperties, C),

    ?awaitMatch(3, consumer_count(Config), ?WAIT),

    ?assertEqual(ok, Cmd:run(Args, OptsGroup)),
    
    delete_stream(S, St, C),
    close(S, C),
    ok.

handle_consumer_update(S, C0, SubId) ->
    {{request, CorrId, {consumer_update, SubId, true}}, C1} =
        rabbit_stream_SUITE:receive_commands(gen_tcp, S, C0),
    ConsumerUpdateCmd =
        {response, CorrId, {consumer_update, ?RESPONSE_CODE_OK, next}},
    ConsumerUpdateFrame = rabbit_stream_core:frame(ConsumerUpdateCmd),
    ok = gen_tcp:send(S, ConsumerUpdateFrame),
    C1.

assertConsumerGroup(S, R, PI, Cs, Record) ->
    ?assertEqual(S, proplists:get_value(stream, Record)),
    ?assertEqual(R, proplists:get_value(reference, Record)),
    ?assertEqual(PI, proplists:get_value(partition_index, Record)),
    ?assertEqual(Cs, proplists:get_value(consumers, Record)),
    ok.

list_stream_tracking_validate(_) ->
    ValidOpts = #{vhost => <<"/">>, <<"writer">> => true},
    ?assertMatch({validation_failure, not_enough_args},
                 ?COMMAND_LIST_STREAM_TRACKING:validate([], #{})),
    ?assertMatch({validation_failure, not_enough_args},
                 ?COMMAND_LIST_STREAM_TRACKING:validate([],
                                                        #{vhost =>
                                                              <<"test">>})),
    ?assertMatch({validation_failure, "Specify only one of --all, --offset, --writer."},
                 ?COMMAND_LIST_STREAM_TRACKING:validate([<<"stream">>],
                                                        #{all => true, writer => true})),
    ?assertMatch({validation_failure, too_many_args},
                 ?COMMAND_LIST_STREAM_TRACKING:validate([<<"stream">>, <<"bad">>],
                                                        ValidOpts)),

    ?assertMatch(ok,
                 ?COMMAND_LIST_STREAM_TRACKING:validate([<<"stream">>],
                                                        ValidOpts)).
list_stream_tracking_merge_defaults(_Config) ->
    ?assertMatch({[<<"s">>], #{all := true, vhost := <<"/">>}},
      ?COMMAND_LIST_STREAM_TRACKING:merge_defaults([<<"s">>], #{})),

    ?assertMatch({[<<"s">>], #{writer := true, vhost := <<"/">>}},
      ?COMMAND_LIST_STREAM_TRACKING:merge_defaults([<<"s">>], #{writer => true})),

    ?assertMatch({[<<"s">>], #{all := true, vhost := <<"dev">>}},
      ?COMMAND_LIST_STREAM_TRACKING:merge_defaults([<<"s">>], #{vhost => <<"dev">>})),

    ?assertMatch({[<<"s">>], #{writer := true, vhost := <<"dev">>}},
      ?COMMAND_LIST_STREAM_TRACKING:merge_defaults([<<"s">>], #{writer => true, vhost => <<"dev">>})).

list_stream_tracking_run(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Stream = <<"list_stream_tracking_run">>,
    ConsumerReference = <<"foo">>,
    PublisherReference = <<"bar">>,
    Opts =
        #{node => Node,
          timeout => 10000,
          vhost => <<"/">>},
    Args = [Stream],

    %% the stream does not exist yet
    ?assertMatch({error, "The stream does not exist."},
                 ?COMMAND_LIST_STREAM_TRACKING:run(Args, Opts#{all => true})),

    StreamPort = rabbit_stream_SUITE:get_stream_port(Config),
    {S, C} = start_stream_connection(StreamPort),
    ?awaitMatch(1, connection_count(Config), ?WAIT),

    create_stream(S, Stream, C),

    ?assertMatch([],
                 ?COMMAND_LIST_STREAM_TRACKING:run(Args, Opts#{all => true})),

    store_offset(S, Stream, ConsumerReference, 42, C),

    ?assertMatch([[{type,offset}, {name, ConsumerReference}, {tracking_value, 42}]],
                ?COMMAND_LIST_STREAM_TRACKING:run(Args, Opts#{all => true})),

    ?assertMatch([[{type,offset}, {name, ConsumerReference}, {tracking_value, 42}]],
                ?COMMAND_LIST_STREAM_TRACKING:run(Args, Opts#{offset => true})),

    ok = store_offset(S, Stream, ConsumerReference, 55, C),
    ?assertMatch([[{type,offset}, {name, ConsumerReference}, {tracking_value, 55}]],
                ?COMMAND_LIST_STREAM_TRACKING:run(Args, Opts#{offset => true})),


    PublisherId = 1,
    rabbit_stream_SUITE:test_declare_publisher(gen_tcp, S, PublisherId,
                                               PublisherReference, Stream, C),
    rabbit_stream_SUITE:test_publish_confirm(gen_tcp, S, PublisherId, 42, <<"">>, C),

    ok = check_publisher_sequence(S, Stream, PublisherReference, 42, C),

    ?assertMatch([
                  [{type,writer},{name,<<"bar">>},{tracking_value, 42}],
                  [{type,offset},{name,<<"foo">>},{tracking_value, 55}]
                 ],
                 ?COMMAND_LIST_STREAM_TRACKING:run(Args, Opts#{all => true})),

    ?assertMatch([
                  [{type,writer},{name,<<"bar">>},{tracking_value, 42}]
                 ],
                 ?COMMAND_LIST_STREAM_TRACKING:run(Args, Opts#{writer => true})),

    rabbit_stream_SUITE:test_publish_confirm(gen_tcp, S, PublisherId, 66, <<"">>, C),

    ok = check_publisher_sequence(S, Stream, PublisherReference, 66, C),

    ?assertMatch([
                  [{type,writer},{name,<<"bar">>},{tracking_value, 66}]
                 ],
                 ?COMMAND_LIST_STREAM_TRACKING:run(Args, Opts#{writer => true})),

    delete_stream(S, Stream, C),

    close(S, C),
    ok.

reset_offset_validate(_) ->
    Cmd = ?COMMAND_RESET_OFFSET,
    ValidOpts = #{vhost => <<"/">>,
                  stream => <<"s1">>,
                  reference => <<"foo">>},
    ?assertMatch({validation_failure, not_enough_args},
                 Cmd:validate([], #{})),
    ?assertMatch({validation_failure, not_enough_args},
                 Cmd:validate([], #{vhost => <<"test">>})),
    ?assertMatch({validation_failure, too_many_args},
                 Cmd:validate([<<"foo">>], ValidOpts)),
    ?assertMatch({validation_failure, reference_too_long},
                 Cmd:validate([], ValidOpts#{reference => gen_bin(256)})),
    ?assertMatch(ok, Cmd:validate([], ValidOpts)),
    ?assertMatch(ok, Cmd:validate([], ValidOpts#{reference => gen_bin(255)})).

reset_offset_merge_defaults(_Config) ->
    Cmd = ?COMMAND_RESET_OFFSET,
    Opts = #{vhost => <<"/">>,
             stream => <<"s1">>,
             reference => <<"foo">>},
    ?assertEqual({[], Opts},
                 Cmd:merge_defaults([], maps:without([vhost], Opts))),
    Merged = maps:merge(Opts, #{vhost => "vhost"}),
    ?assertEqual({[], Merged},
                 Cmd:merge_defaults([], Merged)).

reset_offset_run(Config) ->
    Cmd = ?COMMAND_RESET_OFFSET,
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Opts =#{node => Node,
            timeout => 10000,
            vhost => <<"/">>},
    Args = [],

    St = atom_to_binary(?FUNCTION_NAME, utf8),
    Ref = <<"foo">>,
    OptsGroup = maps:merge(#{stream => St, reference => Ref},
                            Opts),

    %% the stream does not exist yet
    ?assertMatch({error, not_found},
                 Cmd:run(Args, OptsGroup)),

    Port = rabbit_stream_SUITE:get_stream_port(Config),
    {S, C} = start_stream_connection(Port),
    create_stream(S, St, C),

    ?assertEqual({error, no_reference}, Cmd:run(Args, OptsGroup)),
    store_offset(S, St, Ref, 42, C),

    check_stored_offset(S, St, Ref, 42, C),
    ?assertMatch(ok, Cmd:run(Args, OptsGroup)),
    check_stored_offset(S, St, Ref, 0, C),

    delete_stream(S, St, C),
    close(S, C),
    ok.

add_super_stream_merge_defaults(_Config) ->
    ?assertMatch({[<<"super-stream">>],
                  #{partitions := 3, vhost := <<"/">>}},
                 ?COMMAND_ADD_SUPER_STREAM:merge_defaults([<<"super-stream">>],
                                                          #{})),

    ?assertMatch({[<<"super-stream">>],
                  #{partitions := 5, vhost := <<"/">>}},
                 ?COMMAND_ADD_SUPER_STREAM:merge_defaults([<<"super-stream">>],
                                                          #{partitions => 5})),
    DefaultWithBindingKeys =
        ?COMMAND_ADD_SUPER_STREAM:merge_defaults([<<"super-stream">>],
                                                 #{binding_keys =>
                                                       <<"amer,emea,apac">>}),
    ?assertMatch({[<<"super-stream">>],
                  #{binding_keys := <<"amer,emea,apac">>, vhost := <<"/">>}},
                 DefaultWithBindingKeys),

    {_, OptsBks} = DefaultWithBindingKeys,
    ?assertEqual(false, maps:is_key(partitions, OptsBks)),

    DefaultWithRoutingKeys =
        ?COMMAND_ADD_SUPER_STREAM:merge_defaults([<<"super-stream">>],
                                                 #{routing_keys =>
                                                       <<"amer,emea,apac">>}),
    ?assertMatch({[<<"super-stream">>],
                  #{binding_keys := <<"amer,emea,apac">>, vhost := <<"/">>}},
                 DefaultWithRoutingKeys),

    {_, OptsRks} = DefaultWithRoutingKeys,
    ?assertEqual(false, maps:is_key(partitions, OptsRks)).

add_super_stream_validate(_Config) ->
    ?assertMatch({validation_failure, not_enough_args},
                 ?COMMAND_ADD_SUPER_STREAM:validate([], #{})),
    ?assertMatch({validation_failure, too_many_args},
                 ?COMMAND_ADD_SUPER_STREAM:validate([<<"a">>, <<"b">>], #{})),
    ?assertMatch({validation_failure, _},
                 ?COMMAND_ADD_SUPER_STREAM:validate([<<"a">>],
                                                    #{partitions => 1,
                                                      routing_keys =>
                                                          <<"a,b,c">>})),
    ?assertMatch({validation_failure, _},
                 ?COMMAND_ADD_SUPER_STREAM:validate([<<"a">>],
                                                    #{partitions => 1,
                                                      binding_keys => <<"a,b,c">>})),

    ?assertMatch({validation_failure, _},
                 ?COMMAND_ADD_SUPER_STREAM:validate([<<"a">>],
                                                    #{routing_keys => 1,
                                                      binding_keys => <<"a,b,c">>}
                                                    )),

    ?assertMatch({validation_failure, _},
                 ?COMMAND_ADD_SUPER_STREAM:validate([<<"a">>],
                                                    #{partitions => 0})),
    ?assertEqual(ok,
                 ?COMMAND_ADD_SUPER_STREAM:validate([<<"a">>],
                                                    #{partitions => 5})),
    ?assertEqual(ok,
                 ?COMMAND_ADD_SUPER_STREAM:validate([<<"a">>],
                                                    #{routing_keys =>
                                                          <<"a,b,c">>})),
    ?assertEqual(ok,
                 ?COMMAND_ADD_SUPER_STREAM:validate([<<"a">>],
                                                    #{binding_keys =>
                                                          <<"a,b,c">>})),

    [case Expected of
         ok ->
             ?assertEqual(ok,
                          ?COMMAND_ADD_SUPER_STREAM:validate([<<"a">>], Opts));
         error ->
             ?assertMatch({validation_failure, _},
                          ?COMMAND_ADD_SUPER_STREAM:validate([<<"a">>], Opts))
     end
     || {Opts, Expected}
            <- [{#{max_length_bytes => 1000}, ok},
                {#{max_length_bytes => <<"1000">>}, ok},
                {#{max_length_bytes => <<"100gb">>}, ok},
                {#{max_length_bytes => <<"50mb">>}, ok},
                {#{max_length_bytes => <<"50bm">>}, error},
                {#{max_age => <<"PT10M">>}, ok},
                {#{max_age => <<"P5DT8H">>}, ok},
                {#{max_age => <<"foo">>}, error},
                {#{stream_max_segment_size_bytes => 1000}, ok},
                {#{stream_max_segment_size_bytes => <<"1000">>}, ok},
                {#{stream_max_segment_size_bytes => <<"100gb">>}, ok},
                {#{stream_max_segment_size_bytes => <<"50mb">>}, ok},
                {#{stream_max_segment_size_bytes => <<"50bm">>}, error},
                {#{leader_locator => <<"client-local">>}, ok},
                {#{leader_locator => <<"least-leaders">>}, ok},
                {#{leader_locator => <<"random">>}, ok},
                {#{leader_locator => <<"foo">>}, error},
                {#{initial_cluster_size => <<"1">>}, ok},
                {#{initial_cluster_size => <<"2">>}, ok},
                {#{initial_cluster_size => <<"3">>}, ok},
                {#{initial_cluster_size => <<"0">>}, error},
                {#{initial_cluster_size => <<"-1">>}, error},
                {#{initial_cluster_size => <<"foo">>}, error}]],
    ok.

delete_super_stream_merge_defaults(_Config) ->
    ?assertMatch({[<<"super-stream">>], #{vhost := <<"/">>}},
                 ?COMMAND_DELETE_SUPER_STREAM_CLI:merge_defaults([<<"super-stream">>],
                                                             #{})),
    ok.

delete_super_stream_validate(_Config) ->
    ?assertMatch({validation_failure, not_enough_args},
                 ?COMMAND_DELETE_SUPER_STREAM_CLI:validate([], #{})),
    ?assertMatch({validation_failure, too_many_args},
                 ?COMMAND_DELETE_SUPER_STREAM_CLI:validate([<<"a">>, <<"b">>],
                                                       #{})),
    ?assertEqual(ok, ?COMMAND_ADD_SUPER_STREAM:validate([<<"a">>], #{})),
    ok.

add_delete_super_stream_run(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Opts =
        #{node => Node,
          timeout => 10000,
          vhost => <<"/">>},

    % with number of partitions
    ?assertMatch({ok, _},
                 ?COMMAND_ADD_SUPER_STREAM:run([<<"invoices">>],
                                               maps:merge(#{partitions => 3},
                                                          Opts))),
    ?assertEqual({ok,
                  [<<"invoices-0">>, <<"invoices-1">>, <<"invoices-2">>]},
                 partitions(Config, <<"invoices">>)),
    ?assertMatch({ok, _},
                 ?COMMAND_DELETE_SUPER_STREAM_CLI:run([<<"invoices">>], Opts)),
    ?assertEqual({error, stream_not_found},
                 partitions(Config, <<"invoices">>)),

    % with binding keys
    ?assertMatch({ok, _},
                 ?COMMAND_ADD_SUPER_STREAM:run([<<"invoices">>],
                                               maps:merge(#{binding_keys => <<" amer,emea , apac">>},
                                                          Opts))),
    ?assertEqual({ok,
                  [<<"invoices-amer">>, <<"invoices-emea">>,
                   <<"invoices-apac">>]},
                 partitions(Config, <<"invoices">>)),
    ?assertMatch({ok, _},
                 ?COMMAND_DELETE_SUPER_STREAM_CLI:run([<<"invoices">>], Opts)),
    ?assertEqual({error, stream_not_found},
                 partitions(Config, <<"invoices">>)),

    % with arguments
    ExtraOptions =
        #{partitions => 3,
          max_length_bytes => <<"50mb">>,
          max_age => <<"PT10M">>,
          stream_max_segment_size_bytes => <<"1mb">>,
          leader_locator => <<"random">>,
          initial_cluster_size => <<"1">>},

    ?assertMatch({ok, _},
                 ?COMMAND_ADD_SUPER_STREAM:run([<<"invoices">>],
                                               maps:merge(ExtraOptions, Opts))),

    {ok, Q} = queue_lookup(Config, <<"invoices-0">>),
    Args = amqqueue:get_arguments(Q),
    ?assertMatch({_, <<"random">>},
                 rabbit_misc:table_lookup(Args, <<"x-queue-leader-locator">>)),
    ?assertMatch({_, 1},
                 rabbit_misc:table_lookup(Args, <<"x-initial-cluster-size">>)),
    ?assertMatch({_, 1000000},
                 rabbit_misc:table_lookup(Args,
                                          <<"x-stream-max-segment-size-bytes">>)),
    ?assertMatch({_, <<"600s">>},
                 rabbit_misc:table_lookup(Args, <<"x-max-age">>)),
    ?assertMatch({_, 50000000},
                 rabbit_misc:table_lookup(Args, <<"x-max-length-bytes">>)),
    ?assertMatch({_, <<"stream">>},
                 rabbit_misc:table_lookup(Args, <<"x-queue-type">>)),

    ?assertMatch({ok, _},
                 ?COMMAND_DELETE_SUPER_STREAM_CLI:run([<<"invoices">>], Opts)),

    ok.

partitions(Config, Name) ->
    rabbit_ct_broker_helpers:rpc(Config,
                                 0,
                                 rabbit_stream_manager,
                                 partitions,
                                 [<<"/">>, Name]).

create_stream(S, Stream, C0) ->
    rabbit_stream_SUITE:test_create_stream(gen_tcp, S, Stream, C0).

subscribe(S, SubId, Stream, SubProperties, C) ->
    rabbit_stream_SUITE:test_subscribe(gen_tcp,
                                       S,
                                       SubId,
                                       Stream,
                                       SubProperties,
                                       ?RESPONSE_CODE_OK,
                                       C).

subscribe(S, SubId, Stream, C) ->
    rabbit_stream_SUITE:test_subscribe(gen_tcp, S, SubId, Stream, C).

declare_publisher(S, PubId, Stream, C) ->
    rabbit_stream_SUITE:test_declare_publisher(gen_tcp,
                                               S,
                                               PubId,
                                               Stream,
                                               C).

delete_stream(S, Stream, C) ->
    rabbit_stream_SUITE:test_delete_stream(gen_tcp, S, Stream, C).

delete_stream_no_metadata_update(S, Stream, C) ->
    rabbit_stream_SUITE:test_delete_stream(gen_tcp, S, Stream, C, false).

metadata_update_stream_deleted(S, Stream, C) ->
    rabbit_stream_SUITE:test_metadata_update_stream_deleted(gen_tcp,
                                                            S,
                                                            Stream,
                                                            C).

close(S, C) ->
    rabbit_stream_SUITE:test_close(gen_tcp, S, C).

options(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Opts =
        #{node => Node,
          timeout => 10000,
          verbose => false,
          vhost => <<"/">>}, %% just for list_consumers and list_publishers
    Opts.

flatten_command_result([], []) ->
    [];
flatten_command_result([], Acc) ->
    lists:reverse(Acc);
flatten_command_result([[{_K, _V} | _RecordRest] = Record | Rest],
                       Acc) ->
    flatten_command_result(Rest, [Record | Acc]);
flatten_command_result([H | T], Acc) ->
    Acc1 = flatten_command_result(H, Acc),
    flatten_command_result(T, Acc1).

to_list(CommandRun) ->
    Lists = 'Elixir.Enum':to_list(CommandRun),
    %% we can get results from different connections, so we flatten out
    flatten_command_result(Lists, []).

command_result_count(CommandRun) ->
    length(to_list(CommandRun)).

connection_count(Config) ->
    command_result_count(?COMMAND_LIST_CONNECTIONS:run([<<"conn_name">>],
                                                       options(Config))).

consumer_count(Config) ->
    command_result_count(?COMMAND_LIST_CONSUMERS:run([<<"stream">>],
                                                     options(Config))).

publisher_count(Config) ->
    command_result_count(?COMMAND_LIST_PUBLISHERS:run([<<"stream">>],
                                                      options(Config))).

start_stream_connection(Port) ->
    start_stream_connection(gen_tcp, Port).

start_stream_tls_connection(Port) ->
    start_stream_connection(ssl, Port).

start_stream_connection(Transport, Port) ->
    TlsOpts = case Transport of
        ssl -> [{verify, verify_none}];
        _   -> []
      end,
    {ok, S} =
        Transport:connect("localhost", Port,
                          [{active, false}, {mode, binary}] ++ TlsOpts),
    C0 = rabbit_stream_core:init(0),
    C1 = rabbit_stream_SUITE:test_peer_properties(Transport, S, C0),
    C = rabbit_stream_SUITE:test_authenticate(Transport, S, C1),
    {S, C}.

start_amqp_connection(Type, Node, Port) ->
    Params = amqp_params(Type, Node, Port),
    {ok, _Connection} = amqp_connection:start(Params).

amqp_params(network, _, Port) ->
    #amqp_params_network{port = Port};
amqp_params(direct, Node, _) ->
    #amqp_params_direct{node = Node}.

queue_lookup(Config, Q) ->
    QueueName = rabbit_misc:r(<<"/">>, queue, Q),
    rabbit_ct_broker_helpers:rpc(Config,
                                 0,
                                 rabbit_amqqueue,
                                 lookup,
                                 [QueueName]).

store_offset(S, Stream, Reference, Value, C) ->
    StoreOffsetFrame =
        rabbit_stream_core:frame({store_offset, Reference, Stream, Value}),
    ok = gen_tcp:send(S, StoreOffsetFrame),
    case check_stored_offset(S, Stream, Reference, Value, C, 20) of
        ok ->
            ok;
        _ ->
            {error, offset_not_stored}
    end.


check_stored_offset(S, Stream, Reference, Expected, C) ->
    check_stored_offset(S, Stream, Reference, Expected, C, 20).

check_stored_offset(_, _, _, _, _, 0) ->
    error;
check_stored_offset(S, Stream, Reference, Expected, C, Attempt) ->
    QueryOffsetFrame =
        rabbit_stream_core:frame({request, 1, {query_offset, Reference, Stream}}),
    ok = gen_tcp:send(S, QueryOffsetFrame),
    {Cmd, _} = rabbit_stream_SUITE:receive_commands(gen_tcp, S, C),
    ?assertMatch({response, 1, {query_offset, ?RESPONSE_CODE_OK, _}}, Cmd),
    {response, 1, {query_offset, ?RESPONSE_CODE_OK, StoredValue}} = Cmd,
    case StoredValue of
        Expected ->
            ok;
        _ ->
            timer:sleep(50),
            check_stored_offset(S, Stream, Reference, Expected, C, Attempt - 1)
    end.

check_publisher_sequence(S, Stream, Reference, Expected, C) ->
    check_publisher_sequence(S, Stream, Reference, Expected, C, 20).

check_publisher_sequence(_, _, _, _, _, 0) ->
    error;
check_publisher_sequence(S, Stream, Reference, Expected, C, Attempt) ->
    QueryFrame =
        rabbit_stream_core:frame({request, 1, {query_publisher_sequence, Reference, Stream}}),
    ok = gen_tcp:send(S, QueryFrame),
    {Cmd, _} = rabbit_stream_SUITE:receive_commands(gen_tcp, S, C),
    ?assertMatch({response, 1, {query_publisher_sequence, _, _}}, Cmd),
    {response, 1, {query_publisher_sequence, _, StoredValue}} = Cmd,
    case StoredValue of
        Expected ->
            ok;
        _ ->
            timer:sleep(50),
            check_publisher_sequence(S, Stream, Reference, Expected, C, Attempt - 1)
    end.

gen_bin(L) ->
    list_to_binary(lists:duplicate(L, "a")).
