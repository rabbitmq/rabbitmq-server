%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(amqp091_local_dynamic_SUITE).
%% Common test cases to amqp091 and local protocols
%% Both protocols behave very similar, so we can mostly join their
%% test suites and ensure a better coverage

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-compile(export_all).

-import(rabbit_ct_helpers, [eventually/3]).
-import(shovel_test_utils, [await_autodelete/2,
                            set_param/3,
                            set_param_nowait/3,
                            with_amqp10_session/2,
                            with_amqp10_session/3,
                            amqp10_publish_expect/5,
                            amqp10_declare_queue/3,
                            amqp10_publish/4,
                            amqp10_expect_one/2,
                            amqp10_expect_count/3,
                            amqp10_expect_empty/2,
                            make_uri/3,
                            await_shovel/3,
                            await_shovel/4,
                            await_no_shovel/2,
                            with_amqp091_ch/2,
                            amqp091_publish_expect/5,
                            amqp091_publish/4,
                            amqp091_expect_empty/2,
                            amqp091_publish_expect/5
                           ]).

-define(PARAM, <<"test">>).

all() ->
    [
     {group, amqp091},
     {group, local},
     {group, amqp091_to_local},
     {group, local_to_amqp091}
    ].

groups() ->
    [
     {amqp091, [], tests()},
     {local, [], tests()},
     {amqp091_to_local, [], tests()},
     {local_to_amqp091, [], tests()}
    ].

tests() ->
    [
     original_dest,
     exchange_dest,
     exchange_to_exchange,
     missing_exchange_dest,
     missing_create_exchange_dest,
     missing_src_queue_with_src_predeclared,
     missing_dest_queue_with_dest_predeclared,
     missing_src_queue_without_src_predeclared,
     missing_dest_queue_without_dest_predeclared,
     missing_src_and_dest_queue_with_false_src_and_dest_predeclared,
     predeclared_classic_src,
     predeclared_quorum_src,
     predeclared_stream_first_offset_src,
     predeclared_stream_last_offset_src,
     missing_predeclared_src,
     exchange_src,
     queue_args_src,
     queue_args_dest,
     predeclared_classic_dest,
     predeclared_quorum_dest,
     missing_predeclared_dest,
     exchange_status,
     queue_and_exchange_src_fails,
     queue_and_exchange_dest_fails,
     delete_after_queue_length,
     delete_after_queue_length_zero,
     autodelete_classic_on_confirm_queue_length,
     autodelete_quorum_on_confirm_queue_length,
     autodelete_classic_on_publish_queue_length,
     autodelete_quorum_on_publish_queue_length,
     autodelete_classic_no_ack_queue_length,
     autodelete_quorum_no_ack_queue_length
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------
init_per_suite(Config0) ->
    {ok, _} = application:ensure_all_started(amqp10_client),
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config0, [
      {rmq_nodename_suffix, ?MODULE},
      {ignored_crashes, [
          "server_initiated_close,404",
          "writer,send_failed,closed",
          "source_queue_down",
          "dest_queue_down",
          "inbound_link_detached",
          "not_found",
          "dependent process"
        ]}
      ]),
    rabbit_ct_helpers:run_setup_steps(
      Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
          rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    application:stop(amqp10_client),
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(amqp091, Config) ->
    rabbit_ct_helpers:set_config(
      Config,
      [
       {src_protocol, <<"amqp091">>},
       {dest_protocol, <<"amqp091">>}
      ]);
init_per_group(local, Config) ->
    rabbit_ct_helpers:set_config(
      Config,
      [
       {src_protocol, <<"local">>},
       {dest_protocol, <<"local">>}
      ]);
init_per_group(amqp091_to_local, Config) ->
    rabbit_ct_helpers:set_config(
      Config,
      [
       {src_protocol, <<"amqp091">>},
       {dest_protocol, <<"local">>}
      ]);
init_per_group(local_to_amqp091, Config) ->
    rabbit_ct_helpers:set_config(
      Config,
      [
       {src_protocol, <<"local">>},
       {dest_protocol, <<"amqp091">>}
      ]).

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config0) ->
    SrcQ = list_to_binary(atom_to_list(Testcase) ++ "_src"),
    DestQ = list_to_binary(atom_to_list(Testcase) ++ "_dest"),
    VHost = list_to_binary(atom_to_list(Testcase) ++ "_vhost"),
    ShovelArgs = [{<<"src-protocol">>, ?config(src_protocol, Config0)},
                  {<<"dest-protocol">>, ?config(dest_protocol, Config0)}],
    Config = rabbit_ct_helpers:set_config(
               Config0,
               [{srcq, SrcQ}, {destq, DestQ}, {shovel_args, ShovelArgs},
                {alt_vhost, VHost}]),
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    shovel_test_utils:clear_param(Config, ?PARAM),
    rabbit_ct_broker_helpers:rpc(Config, 0, shovel_test_utils, delete_all_queues, []),
    _ = rabbit_ct_broker_helpers:delete_vhost(Config, ?config(alt_vhost, Config)),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, unset_env,
                                      [rabbitmq_shovel, topology]),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------
original_dest(Config) ->
    %% Publish with the original routing keys, but use a different vhost
    %% to avoid a loop (this is a single-node test).
    Src = ?config(srcq, Config),
    Dest = Src,
    AltVHost = ?config(alt_vhost, Config),
    ok = rabbit_ct_broker_helpers:add_vhost(Config, AltVHost),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, <<"guest">>, AltVHost),
    with_amqp10_session(Config, AltVHost,
                        fun (Sess) ->
                                amqp10_declare_queue(Sess, Dest, #{})
                        end),
    with_amqp10_session(
      Config,
      fun (Sess) ->
              SrcUri = make_uri(Config, 0, <<"%2F">>),
              DestUri = make_uri(Config, 0, AltVHost),
              ShovelArgs = [{<<"src-uri">>,  SrcUri},
                            {<<"dest-uri">>, [DestUri]},
                            {<<"src-queue">>, Src}]
                  ++ ?config(shovel_args, Config),
              ok = rabbit_ct_broker_helpers:rpc(
                     Config, 0, rabbit_runtime_parameters, set,
                     [<<"/">>, <<"shovel">>, ?PARAM, ShovelArgs, none]),
              await_shovel(Config, 0, ?PARAM),
              _ = amqp10_publish(Sess, Src, <<"hello">>, 1)
      end),
    with_amqp10_session(Config, AltVHost,
                        fun (Sess) ->
                                amqp10_expect_one(Sess, Dest)
                        end).

exchange_dest(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    AltExchange = <<"alt-exchange">>,
    RoutingKey = <<"funky-routing-key">>,
    declare_exchange(Config, <<"/">>, AltExchange),
    declare_and_bind_queue(Config, <<"/">>, AltExchange, Dest, RoutingKey),
    with_amqp10_session(
      Config,
      fun (Sess) ->
              set_param(Config, ?PARAM,
                        ?config(shovel_args, Config) ++
                            [{<<"src-queue">>, Src},
                             {<<"dest-exchange">>, AltExchange},
                             {<<"dest-exchange-key">>, RoutingKey}
                            ]),
              _ = amqp10_publish_expect(Sess, Src, Dest, <<"hello">>, 1)
      end).

exchange_to_exchange(Config) ->
    with_amqp091_ch(Config,
      fun (Ch) ->
              amqp_channel:call(Ch, #'queue.declare'{queue   = <<"queue">>,
                                                     durable = true}),
              amqp_channel:call(
                Ch, #'queue.bind'{queue       = <<"queue">>,
                                  exchange    = <<"amq.topic">>,
                                  routing_key = <<"test-key">>}),
              set_param(Config,
                        ?PARAM, [{<<"src-exchange">>,    <<"amq.direct">>},
                                     {<<"src-exchange-key">>,<<"test-key">>},
                                     {<<"dest-exchange">>,   <<"amq.topic">>}]),
              amqp091_publish_expect(Ch, <<"amq.direct">>, <<"test-key">>,
                             <<"queue">>, <<"hello">>),
              set_param(Config,
                        ?PARAM, [{<<"src-exchange">>,     <<"amq.direct">>},
                                     {<<"src-exchange-key">>, <<"test-key">>},
                                     {<<"dest-exchange">>,    <<"amq.topic">>},
                                     {<<"dest-exchange-key">>,<<"new-key">>}]),
              amqp091_publish(Ch, <<"amq.direct">>, <<"test-key">>, <<"hello">>),
              amqp091_expect_empty(Ch, <<"queue">>),
              amqp_channel:call(
                Ch, #'queue.bind'{queue       = <<"queue">>,
                                  exchange    = <<"amq.topic">>,
                                  routing_key = <<"new-key">>}),
              amqp091_publish_expect(Ch, <<"amq.direct">>, <<"test-key">>,
                                     <<"queue">>, <<"hello">>)
      end).

missing_exchange_dest(Config) ->
    Src = ?config(srcq, Config),
    AltExchange = <<"alt-exchange">>,
    RoutingKey = <<"funky-routing-key">>,
    %% If the destination exchange doesn't exist, it succeeds to start
    %% the shovel. Just messages will not be routed
    shovel_test_utils:set_param(Config, ?PARAM,
                                ?config(shovel_args, Config) ++
                                    [{<<"src-queue">>, Src},
                                     {<<"dest-exchange">>, AltExchange},
                                     {<<"dest-exchange-key">>, RoutingKey}
                                    ]).

missing_create_exchange_dest(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    with_amqp091_ch(
      Config,
      fun (Ch) ->
              amqp_channel:call(
                Ch, #'queue.declare'{queue = Src,
                                     durable = true}),
              amqp_channel:call(
                Ch, #'queue.declare'{queue = Dest,
                                     durable = true}),
              amqp_channel:call(
                Ch, #'queue.bind'{queue = Src,
                                  exchange = <<"amq.direct">>,
                                  routing_key = <<"src-key">>}),
              set_param(Config,
                        ?PARAM, ?config(shovel_args, Config) ++
                            [{<<"src-queue">>, Src},
                             {<<"dest-exchange">>, <<"dest-ex">>},
                             {<<"dest-exchange-key">>, <<"dest-key">>}]),
              amqp091_publish(Ch, <<"amq.direct">>, <<"src-key">>, <<"hello">>),
              amqp091_expect_empty(Ch, Src),
              amqp_channel:call(
                Ch, #'exchange.declare'{exchange = <<"dest-ex">>}),
              amqp_channel:call(
                Ch, #'queue.bind'{queue = Dest,
                                  exchange = <<"dest-ex">>,
                                  routing_key = <<"dest-key">>}),
              amqp091_publish_expect(Ch, <<"amq.direct">>, <<"src-key">>, Dest, <<"hello!">>)
end).

missing_src_queue_with_src_predeclared(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    with_amqp091_ch(
      Config,
      fun (Ch) ->
              amqp_channel:call(
                Ch, #'queue.declare'{queue = Dest,
                                     durable = true}),
              amqp_channel:call(
                Ch, #'exchange.declare'{exchange = <<"dest-ex">>}),
              amqp_channel:call(
                Ch, #'queue.bind'{queue = Dest,
                                  exchange = <<"dest-ex">>,
                                  routing_key = <<"dest-key">>}),

              set_param_nowait(Config,
                               ?PARAM, ?config(shovel_args, Config) ++
                                   [{<<"src-queue">>, Src},
                                    {<<"src-predeclared">>, true},
                                    {<<"dest-exchange">>, <<"dest-ex">>},
                                    {<<"dest-exchange-key">>, <<"dest-key">>}]),
              await_shovel(Config, 0, ?PARAM, terminated),
              expect_missing_queue(Ch, Src),

              with_amqp091_ch(
                Config,
                fun(Ch2) ->
                        amqp_channel:call(
                          Ch2, #'queue.declare'{queue = Src,
                                                durable = true}),
                        amqp_channel:call(
                          Ch2, #'queue.bind'{queue = Src,
                                             exchange = <<"amq.direct">>,
                                             routing_key = <<"src-key">>}),
                        await_shovel(Config, 0, ?PARAM, running),
                        amqp091_publish_expect(Ch2, <<"amq.direct">>, <<"src-key">>, Dest, <<"hello!">>)
                end)
      end).

missing_dest_queue_with_dest_predeclared(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    with_amqp091_ch(
      Config,
      fun (Ch) ->
              amqp_channel:call(
                Ch, #'queue.declare'{queue = Src,
                                     durable = true}),
              amqp_channel:call(
                Ch, #'queue.bind'{queue = Src,
                                  exchange = <<"amq.direct">>,
                                  routing_key = <<"src-key">>}),

              set_param_nowait(Config,
                               ?PARAM, shovel_queue_args(Config) ++
                                   [{<<"dest-predeclared">>, true}]),
              await_shovel(Config, 0, ?PARAM, terminated),
              expect_missing_queue(Ch, Dest),

              with_amqp091_ch(
                Config,
                fun(Ch2) ->
                        amqp_channel:call(
                          Ch2, #'queue.declare'{queue = Dest,
                                                durable = true}),
                        await_shovel(Config, 0, ?PARAM, running),
                        amqp091_publish_expect(Ch2, <<"amq.direct">>, <<"src-key">>, Dest, <<"hello!">>)
                end)
      end).

missing_src_queue_without_src_predeclared(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, application, set_env,
           [rabbitmq_shovel, topology, [{predeclared, true}]]),
    with_amqp091_ch(
      Config,
      fun (Ch) ->
              amqp_channel:call(
                Ch, #'queue.declare'{queue = Dest,
                                     durable = true}),
              amqp_channel:call(
                Ch, #'exchange.declare'{exchange = <<"dest-ex">>}),
              amqp_channel:call(
                Ch, #'queue.bind'{queue = Dest,
                                  exchange = <<"dest-ex">>,
                                  routing_key = <<"dest-key">>}),

              set_param_nowait(Config, ?PARAM,
                               ?config(shovel_args, Config) ++
                                   [{<<"src-queue">>, Src},
                                    {<<"dest-exchange">>, <<"dest-ex">>},
                                    {<<"dest-exchange-key">>, <<"dest-key">>}]),
              await_shovel(Config, 0, ?PARAM, terminated),
              expect_missing_queue(Ch, Src),

              with_amqp091_ch(
                Config,
                fun(Ch2) ->
                        amqp_channel:call(
                          Ch2, #'queue.declare'{queue = Src,
                                                durable = true}),
                        amqp_channel:call(
                          Ch2, #'queue.bind'{queue = Src,
                                             exchange = <<"amq.direct">>,
                                             routing_key = <<"src-key">>}),
                        await_shovel(Config, 0, ?PARAM, running),

                        amqp091_publish_expect(Ch2, <<"amq.direct">>, <<"src-key">>, Dest, <<"hello!">>)
                end)
      end).


missing_dest_queue_without_dest_predeclared(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, application, set_env,
           [rabbitmq_shovel, topology, [{predeclared, true}]]),
    with_amqp091_ch(
      Config,
      fun (Ch) ->
              amqp_channel:call(
                Ch, #'queue.declare'{queue = Src,
                                     durable = true}),
              amqp_channel:call(
                Ch, #'queue.bind'{queue = Src,
                                  exchange = <<"amq.direct">>,
                                  routing_key = <<"src-key">>}),

              set_param_nowait(Config, ?PARAM,
                               shovel_queue_args(Config)),
              await_shovel(Config, 0, ?PARAM, terminated),
              expect_missing_queue(Ch, Dest),

              with_amqp091_ch(
                Config,
                fun(Ch2) ->
                        amqp_channel:call(
                          Ch2, #'queue.declare'{queue = Dest,
                                                durable = true}),
                        await_shovel(Config, 0, ?PARAM, running),
                        amqp091_publish_expect(Ch2, <<"amq.direct">>, <<"src-key">>, Dest, <<"hello!">>)
                end)
      end).

missing_src_and_dest_queue_with_false_src_and_dest_predeclared(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, application, set_env,
           [rabbitmq_shovel, topology, [{predeclared, true}]]),
    with_amqp10_session(
      Config,
      fun(Sess) ->
              shovel_test_utils:set_param(
                Config, ?PARAM, shovel_queue_args(Config) ++
                    [{<<"src-predeclared">>, false},
                     {<<"dest-predeclared">>, false}]),
              amqp10_publish_expect(Sess, Src, Dest, <<"hello">>, 1)
    end).

predeclared_classic_src(Config) ->
    predeclared_src(Config, <<"classic">>).

predeclared_quorum_src(Config) ->
    predeclared_src(Config, <<"quorum">>).

predeclared_src(Config, Type) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    with_amqp10_session(Config,
      fun (Sess) ->
              amqp10_declare_queue(Sess, Src, #{<<"x-queue-type">> => {utf8, Type}}),
              set_param(Config, ?PARAM,
                        shovel_queue_args(Config) ++
                            [{<<"src-predeclared">>, true}
                            ]),
              _ = amqp10_publish_expect(Sess, Src, Dest, <<"hello">>, 1)
      end).

predeclared_stream_first_offset_src(Config) ->
    predeclared_stream_offset_src(Config, <<"first">>, 20).

predeclared_stream_last_offset_src(Config) ->
    predeclared_stream_offset_src(Config, <<"last">>, 1).

predeclared_stream_offset_src(Config, Offset, ExpectedMsgs) ->
    %% TODO test this in static
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    with_amqp10_session(
      Config,
      fun (Sess) ->
              amqp10_declare_queue(Sess, Src, #{<<"x-queue-type">> => {utf8, <<"stream">>}}),
              amqp10_publish(Sess, Src, <<"tag1">>, 20),
              set_param(Config, ?PARAM,
                        shovel_queue_args(Config) ++
                            [{<<"src-predeclared">>, true},
                             {<<"src-consumer-args">>,  #{<<"x-stream-offset">> => Offset}}
                            ]),
              amqp10_expect_count(Sess, Dest, ExpectedMsgs),
              amqp10_expect_empty(Sess, Dest),
              _ = amqp10_publish_expect(Sess, Src, Dest, <<"hello">>, 1)
      end).

missing_predeclared_src(Config) ->
    set_param_nowait(Config, ?PARAM,
                     shovel_queue_args(Config) ++
                         [{<<"src-predeclared">>, true}]),
    await_no_shovel(Config, ?PARAM),
    %% The shovel parameter is only deleted when 'delete-after'
    %% is used. In any other failure, the shovel should
    %% remain and try to restart
    ?assertNotMatch(
       not_found,
       rabbit_ct_broker_helpers:rpc(
         Config, 0, rabbit_runtime_parameters, lookup,
         [<<"/">>, <<"shovel">>, ?PARAM])).

exchange_src(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    with_amqp10_session(Config,
      fun (Sess) ->
              set_param(Config, ?PARAM,
                        ?config(shovel_args, Config) ++
                        [{<<"src-exchange">>, <<"amq.direct">>},
                         {<<"src-exchange-key">>, Src},
                         {<<"dest-queue">>, Dest}
                        ]),
              Target = <<"/exchange/amq.direct/", Src/binary>>,
              _ = amqp10_publish_expect(Sess, Target, Dest, <<"hello">>, 1)
      end).

queue_args_src(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    shovel_test_utils:set_param(
      Config, ?PARAM,
      shovel_queue_args(Config) ++
          [{<<"src-queue-args">>, #{<<"x-queue-type">> => <<"quorum">>}}]),
    Expected = lists:sort([[Src, <<"quorum">>], [Dest, <<"classic">>]]),
    ?assertMatch(Expected,
                 lists:sort(rabbit_ct_broker_helpers:rabbitmqctl_list(
                              Config, 0,
                              ["list_queues", "name", "type", "--no-table-headers"]))).

queue_args_dest(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    shovel_test_utils:set_param(
      Config, ?PARAM,
      shovel_queue_args(Config) ++
          [{<<"dest-queue-args">>, #{<<"x-queue-type">> => <<"quorum">>}}]),
    Expected = lists:sort([[Dest, <<"quorum">>], [Src, <<"classic">>]]),
    ?assertMatch(Expected,
                 lists:sort(rabbit_ct_broker_helpers:rabbitmqctl_list(
                              Config, 0,
                              ["list_queues", "name", "type", "--no-table-headers"]))).

predeclared_classic_dest(Config) ->
    predeclared_dest(Config, <<"classic">>).

predeclared_quorum_dest(Config) ->
    predeclared_dest(Config, <<"quorum">>).

predeclared_dest(Config, Type) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    with_amqp10_session(Config,
      fun (Sess) ->
              amqp10_declare_queue(Sess, Dest, #{<<"x-queue-type">> => {utf8, Type}}),
              set_param(Config, ?PARAM,
                        shovel_queue_args(Config) ++
                            [{<<"dest-predeclared">>, true}]),
              _ = amqp10_publish_expect(Sess, Src, Dest, <<"hello">>, 1)
      end).

missing_predeclared_dest(Config) ->
    set_param_nowait(
      Config, ?PARAM, shovel_queue_args(Config) ++
          [{<<"dest-predeclared">>, true}]),
    await_no_shovel(Config, ?PARAM),
    %% The shovel parameter is only deleted when 'delete-after'
    %% is used. In any other failure, the shovel should
    %% remain and try to restart
    ?assertNotMatch(
       not_found,
       rabbit_ct_broker_helpers:rpc(
         Config, 0, rabbit_runtime_parameters, lookup,
         [<<"/">>, <<"shovel">>, ?PARAM])).

exchange_status(Config) ->
    DefExchange = <<"amq.direct">>,
    RK1 = <<"carrots">>,
    AltExchange = <<"amq.fanout">>,
    RK2 = <<"bunnies">>,
    SrcProtocol = ?config(src_protocol, Config),
    DestProtocol = ?config(dest_protocol, Config),
    set_param(Config, ?PARAM,
              ?config(shovel_args, Config) ++
                  [{<<"src-exchange">>, DefExchange},
                   {<<"src-exchange-key">>, RK1},
                   {<<"dest-exchange">>, AltExchange},
                   {<<"dest-exchange-key">>, RK2}
                  ]),
    Status = rabbit_ct_broker_helpers:rpc(Config, 0,
                                          rabbit_shovel_status, status, []),
    ?assertMatch([{_, dynamic, {running, _}, _, _}], Status),
    [{_, dynamic, {running, Info}, _, _}] = Status,
    ?assertMatch(SrcProtocol, proplists:get_value(src_protocol, Info)),
    ?assertMatch(DestProtocol, proplists:get_value(dest_protocol, Info)),
    ?assertMatch(undefined, proplists:get_value(src_queue, Info, undefined)),
    ?assertMatch(DefExchange, proplists:get_value(src_exchange, Info)),
    ?assertMatch(RK1, proplists:get_value(src_exchange_key, Info)),
    ?assertMatch(AltExchange, proplists:get_value(dest_exchange, Info)),
    ?assertMatch(RK2, proplists:get_value(dest_exchange_key, Info)),
    ok.

queue_and_exchange_src_fails(Config) ->
    %% Setting both queue and exchange for source fails
    try
        set_param(Config, ?PARAM,
                  shovel_queue_args(Config) ++
                      [{<<"src-exchange">>, <<"amq.direct">>},
                       {<<"src-exchange-key">>, <<"bunnies">>}
                      ]),
        throw(unexpected_success)
    catch
        _:{badmatch, {error_string, Reason}} ->
            ?assertMatch(match, re:run(Reason, "Validation failed", [{capture, none}]))
    end.

queue_and_exchange_dest_fails(Config) ->
    %% Setting both queue and exchange for dest fails
    try
        set_param(Config, ?PARAM,
                  shovel_queue_args(Config) ++
                      [{<<"dest-exchange">>, <<"amq.direct">>},
                       {<<"dest-exchange-key">>, <<"bunnies">>}
                      ]),
        throw(unexpected_success)
    catch
        _:{badmatch, {error_string, Reason}} ->
            ?assertMatch(match, re:run(Reason, "Validation failed", [{capture, none}]))
    end.

delete_after_queue_length_zero(Config) ->
    Src = ?config(srcq, Config),
    with_amqp10_session(
      Config,
      fun (Sess) ->
              amqp10_declare_queue(Sess, Src, #{}),
              set_param_nowait(Config, ?PARAM,
                               shovel_queue_args(Config) ++
                                   [{<<"src-predeclared">>, true},
                                    {<<"src-delete-after">>, <<"queue-length">>}
                                   ]),
              await_no_shovel(Config, ?PARAM),
              %% The shovel parameter is only deleted when 'delete-after'
              %% is used. In any other failure, the shovel should
              %% remain and try to restart
              ?assertMatch(not_found, rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_runtime_parameters, lookup, [<<"/">>, <<"shovel">>, ?PARAM]))
      end).

delete_after_queue_length(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    with_amqp10_session(
      Config,
      fun (Sess) ->
              amqp10_declare_queue(Sess, Src, #{}),
              amqp10_publish(Sess, Src, <<"tag1">>, 18),
              set_param_nowait(Config, ?PARAM,
                               shovel_queue_args(Config) ++
                                   [{<<"src-predeclared">>, true},
                                    {<<"src-delete-after">>, <<"queue-length">>}
                                   ]),
              %% The shovel parameter is only deleted when 'delete-after'
              %% is used. In any other failure, the shovel should
              %% remain and try to restart
              amqp10_expect_count(Sess, Dest, 18),
              await_autodelete(Config, ?PARAM),
              amqp10_publish(Sess, Src, <<"tag1">>, 5),
              amqp10_expect_empty(Sess, Dest)
      end).


autodelete_classic_on_confirm_queue_length(Config) ->
    autodelete(Config, <<"classic">>, <<"on-confirm">>).

autodelete_quorum_on_confirm_queue_length(Config) ->
    autodelete(Config, <<"quorum">>, <<"on-confirm">>).

autodelete_classic_on_publish_queue_length(Config) ->
    autodelete(Config, <<"classic">>, <<"on-publish">>).

autodelete_quorum_on_publish_queue_length(Config) ->
    autodelete(Config, <<"quorum">>, <<"on-publish">>).

autodelete_classic_no_ack_queue_length(Config) ->
    autodelete(Config, <<"classic">>, <<"no-ack">>).

autodelete_quorum_no_ack_queue_length(Config) ->
    autodelete(Config, <<"quorum">>, <<"no-ack">>).

autodelete(Config, Type, AckMode) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    with_amqp10_session(
      Config,
      fun (Sess) ->
              amqp10_declare_queue(Sess, Src, #{<<"x-queue-type">> => {utf8, Type}}),
              amqp10_declare_queue(Sess, Dest, #{<<"x-queue-type">> => {utf8, Type}}),
              amqp10_publish(Sess, Src, <<"hello">>, 100),
              Expected0 = lists:sort([[Dest, Type, <<"0">>], [Src, Type, <<"100">>]]),
              ?awaitMatch(Expected0,
                          lists:sort(rabbit_ct_broker_helpers:rabbitmqctl_list(
                                       Config, 0,
                                       ["list_queues", "name", "type", "messages", "--no-table-headers"])),
                          30_000),
              ExtraArgs = [{<<"ack-mode">>, AckMode},
                           {<<"src-delete-after">>, <<"queue-length">>}],
              ShovelArgs = shovel_queue_args(Config) ++ ExtraArgs,
              set_param_nowait(Config, ?PARAM, ShovelArgs),
              await_autodelete(Config, ?PARAM),
              amqp10_expect_count(Sess, Dest, 100)
      end).

%%----------------------------------------------------------------------------
shovel_queue_args(Config) ->
    ?config(shovel_args, Config) ++
        [{<<"src-queue">>, ?config(srcq, Config)},
         {<<"dest-queue">>, ?config(destq, Config)}].

declare_and_bind_queue(Config, VHost, Exchange, QName, RoutingKey) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0, VHost),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    ?assertEqual(
       {'queue.declare_ok', QName, 0, 0},
       amqp_channel:call(
         Ch, #'queue.declare'{queue = QName, durable = true,
                              arguments = [{<<"x-queue-type">>, longstr, <<"classic">>}]})),
    ?assertMatch(
       #'queue.bind_ok'{},
       amqp_channel:call(Ch, #'queue.bind'{
                                queue = QName,
                                exchange = Exchange,
                                routing_key = RoutingKey
                               })),
    rabbit_ct_client_helpers:close_channel(Ch),
    rabbit_ct_client_helpers:close_connection(Conn).

declare_exchange(Config, VHost, Exchange) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0, VHost),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    ?assertMatch(
       #'exchange.declare_ok'{},
       amqp_channel:call(Ch, #'exchange.declare'{exchange = Exchange})),
    rabbit_ct_client_helpers:close_channel(Ch),
    rabbit_ct_client_helpers:close_connection(Conn).

expect_missing_queue(Ch, Q) ->
    try
        amqp_channel:call(Ch, #'queue.declare'{queue   = Q,
                                               passive = true}),
        ct:fail(queue_still_exists)
    catch exit:{{shutdown, {server_initiated_close, ?NOT_FOUND, _Text}}, _} ->
        ok
    end.
