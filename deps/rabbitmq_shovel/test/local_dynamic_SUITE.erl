%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(local_dynamic_SUITE).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-compile(export_all).

-import(shovel_test_utils, [with_amqp10_session/2, with_amqp10_session/3,
                            amqp10_expect_empty/2,
                            amqp10_publish/4, amqp10_expect_one/2,
                            amqp10_expect_count/3, amqp10_expect/3,
                            amqp10_publish_expect/5, amqp10_subscribe/2,
                            await_autodelete/2]).

-define(PARAM, <<"test">>).

all() ->
    [
      {group, tests}
    ].

groups() ->
    [
     {tests, [], [
                  local_to_local_opt_headers,
                  local_to_local_original_dest,
                  local_to_local_exchange_dest,
                  local_to_local_missing_exchange_dest,
                  local_to_local_predeclared_src,
                  local_to_local_predeclared_quorum_src,
                  local_to_local_predeclared_stream_first_offset_src,
                  local_to_local_predeclared_stream_last_offset_src,
                  local_to_local_missing_predeclared_src,
                  local_to_local_exchange_src,
                  local_to_local_queue_args_src,
                  local_to_local_queue_args_dest,
                  local_to_local_predeclared_dest,
                  local_to_local_predeclared_quorum_dest,
                  local_to_local_missing_predeclared_dest,
                  local_to_local_queue_status,
                  local_to_local_exchange_status,
                  local_to_local_queue_and_exchange_src_fails,
                  local_to_local_queue_and_exchange_dest_fails,
                  local_to_local_delete_after_never,
                  local_to_local_delete_after_queue_length,
                  local_to_local_delete_after_queue_length_zero,
                  local_to_local_delete_after_with_rejections,
                  local_to_local_no_ack,
                  local_to_local_quorum_no_ack,
                  local_to_local_stream_no_ack,
                  local_to_local_on_publish,
                  local_to_local_quorum_on_publish,
                  local_to_local_stream_on_publish,
                  local_to_local_on_confirm,
                  local_to_local_quorum_on_confirm,
                  local_to_local_stream_on_confirm,
                  local_to_local_reject_publish,
                  local_to_amqp091,
                  local_to_amqp10,
                  amqp091_to_local,
                  amqp10_to_local,
                  local_to_local_delete_src_queue,
                  local_to_local_delete_dest_queue,
                  local_to_local_vhost_access,
                  local_to_local_user_access,
                  local_to_local_credit_flow_on_confirm,
                  local_to_local_credit_flow_on_publish,
                  local_to_local_credit_flow_no_ack,
                  local_to_local_quorum_credit_flow_on_confirm,
                  local_to_local_quorum_credit_flow_on_publish,
                  local_to_local_quorum_credit_flow_no_ack,
                  local_to_local_stream_credit_flow_on_confirm,
                  local_to_local_stream_credit_flow_on_publish,
                  local_to_local_stream_credit_flow_no_ack,
                  local_to_local_simple_uri,
                  local_to_local_counters
                 ]}
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
          "dest_queue_down"
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

init_per_group(_, Config) ->
    [Node] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    case rabbit_ct_broker_helpers:enable_feature_flag(
           Config, [Node], 'rabbitmq_4.0.0') of
        ok ->
            Config;
        _ ->
            {skip, "This suite requires rabbitmq_4.0.0 feature flag"}
    end.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config0) ->
    SrcQ = list_to_binary(atom_to_list(Testcase) ++ "_src"),
    DestQ = list_to_binary(atom_to_list(Testcase) ++ "_dest"),
    DestQ2 = list_to_binary(atom_to_list(Testcase) ++ "_dest2"),
    VHost = list_to_binary(atom_to_list(Testcase) ++ "_vhost"),
    Config = [{srcq, SrcQ}, {destq, DestQ}, {destq2, DestQ2},
              {alt_vhost, VHost} | Config0],

    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    shovel_test_utils:clear_param(Config, ?PARAM),
    rabbit_ct_broker_helpers:rpc(Config, 0, shovel_test_utils, delete_all_queues, []),
    _ = rabbit_ct_broker_helpers:delete_vhost(Config, ?config(alt_vhost, Config)),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

local_to_local_opt_headers(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    with_amqp10_session(Config,
      fun (Sess) ->
              shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-queue">>, Src},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-queue">>, Dest},
                                           {<<"dest-add-forward-headers">>, true},
                                           {<<"dest-add-timestamp-header">>, true}
                                          ]),
              [Msg] = amqp10_publish_expect(Sess, Src, Dest, <<"hello">>, 1),
              ?assertMatch(#{<<"x-opt-shovel-name">> := ?PARAM,
                             <<"x-opt-shovel-type">> := <<"dynamic">>,
                             <<"x-opt-shovelled-by">> := _,
                             <<"x-opt-shovelled-timestamp">> := _},
                           amqp10_msg:message_annotations(Msg))
      end).

local_to_local_original_dest(Config) ->
    %% Publish with the original routing keys, but use a different vhost
    %% to avoid a loop (this is a single-node test).
    Src = ?config(srcq, Config),
    Dest = Src,
    AltVHost = ?config(alt_vhost, Config),
    ok = rabbit_ct_broker_helpers:add_vhost(Config, AltVHost),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, <<"guest">>, AltVHost),
    declare_queue(Config, AltVHost, Dest),
    with_amqp10_session(
      Config,
      fun (Sess) ->
              SrcUri = shovel_test_utils:make_uri(Config, 0, <<"%2F">>),
              DestUri = shovel_test_utils:make_uri(Config, 0, AltVHost),
              ok = rabbit_ct_broker_helpers:rpc(
                     Config, 0, rabbit_runtime_parameters, set,
                     [<<"/">>, <<"shovel">>, ?PARAM, [{<<"src-uri">>,  SrcUri},
                                                      {<<"dest-uri">>, [DestUri]},
                                                      {<<"src-protocol">>, <<"local">>},
                                                      {<<"src-queue">>, Src},
                                                      {<<"dest-protocol">>, <<"local">>}],
                      none]),
              shovel_test_utils:await_shovel(Config, 0, ?PARAM),
              _ = amqp10_publish(Sess, Src, <<"hello">>, 1)
      end),
    with_amqp10_session(Config, AltVHost,
                 fun (Sess) ->
                         amqp10_expect_one(Sess, Dest)
                 end).

local_to_local_exchange_dest(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    AltExchange = <<"alt-exchange">>,
    RoutingKey = <<"funky-routing-key">>,
    declare_exchange(Config, <<"/">>, AltExchange),
    declare_and_bind_queue(Config, <<"/">>, AltExchange, Dest, RoutingKey),
    with_amqp10_session(Config,
      fun (Sess) ->
             shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-queue">>, Src},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-exchange">>, AltExchange},
                                           {<<"dest-exchange-key">>, RoutingKey}
                                          ]),
              _ = amqp10_publish_expect(Sess, Src, Dest, <<"hello">>, 1)
      end).

local_to_local_missing_exchange_dest(Config) ->
    Src = ?config(srcq, Config),
    AltExchange = <<"alt-exchange">>,
    RoutingKey = <<"funky-routing-key">>,
    %% If the destination exchange doesn't exist, it succeeds to start
    %% the shovel. Just messages will not be routed
    shovel_test_utils:set_param(Config, ?PARAM,
                                [{<<"src-protocol">>, <<"local">>},
                                 {<<"src-queue">>, Src},
                                 {<<"dest-protocol">>, <<"local">>},
                                 {<<"dest-exchange">>, AltExchange},
                                 {<<"dest-exchange-key">>, RoutingKey}
                                ]).

local_to_local_predeclared_src(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    declare_queue(Config, <<"/">>, Src),
    with_amqp10_session(Config,
      fun (Sess) ->
             shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-queue">>, Src},
                                           {<<"src-predeclared">>, true},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-queue">>, Dest}
                                          ]),
              _ = amqp10_publish_expect(Sess, Src, Dest, <<"hello">>, 1)
      end).

local_to_local_predeclared_quorum_src(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    declare_queue(Config, <<"/">>, Src, [{<<"x-queue-type">>, longstr, <<"quorum">>}]),
    with_amqp10_session(Config,
      fun (Sess) ->
             shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-queue">>, Src},
                                           {<<"src-predeclared">>, true},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-queue">>, Dest}
                                          ]),
              _ = amqp10_publish_expect(Sess, Src, Dest, <<"hello">>, 1)
      end).

local_to_local_predeclared_stream_first_offset_src(Config) ->
    %% TODO test this in static
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    declare_queue(Config, <<"/">>, Src, [{<<"x-queue-type">>, longstr, <<"stream">>}]),
    with_amqp10_session(Config,
      fun (Sess) ->
              amqp10_publish(Sess, Src, <<"tag1">>, 20),
              shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-queue">>, Src},
                                           {<<"src-predeclared">>, true},
                                           {<<"src-consumer-args">>,  #{<<"x-stream-offset">> => <<"first">>}},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-queue">>, Dest}
                                          ]),
              amqp10_expect_count(Sess, Dest, 20),
              amqp10_expect_empty(Sess, Dest),
              _ = amqp10_publish_expect(Sess, Src, Dest, <<"hello">>, 1)
      end).

local_to_local_predeclared_stream_last_offset_src(Config) ->
    %% TODO test this in static
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    declare_queue(Config, <<"/">>, Src, [{<<"x-queue-type">>, longstr, <<"stream">>}]),
    with_amqp10_session(Config,
      fun (Sess) ->
              amqp10_publish(Sess, Src, <<"tag1">>, 20),
              shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-queue">>, Src},
                                           {<<"src-predeclared">>, true},
                                           {<<"src-consumer-args">>,  #{<<"x-stream-offset">> => <<"last">>}},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-queue">>, Dest}
                                          ]),
              %% Deliver last
              amqp10_expect_count(Sess, Dest, 1),
              amqp10_expect_empty(Sess, Dest),
              _ = amqp10_publish_expect(Sess, Src, Dest, <<"hello">>, 1)
      end).

local_to_local_missing_predeclared_src(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    shovel_test_utils:set_param_nowait(Config, ?PARAM,
                                       [{<<"src-protocol">>, <<"local">>},
                                        {<<"src-queue">>, Src},
                                        {<<"src-predeclared">>, true},
                                        {<<"dest-protocol">>, <<"local">>},
                                        {<<"dest-queue">>, Dest}
                                       ]),
    shovel_test_utils:await_no_shovel(Config, ?PARAM),
    %% The shovel parameter is only deleted when 'delete-after'
    %% is used. In any other failure, the shovel should
    %% remain and try to restart
    ?assertNotMatch(
       not_found,
       rabbit_ct_broker_helpers:rpc(
         Config, 0, rabbit_runtime_parameters, lookup,
         [<<"/">>, <<"shovel">>, ?PARAM])).

local_to_local_exchange_src(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    with_amqp10_session(Config,
      fun (Sess) ->
             shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-exchange">>, <<"amq.direct">>},
                                           {<<"src-exchange-key">>, Src},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-queue">>, Dest}
                                          ]),
              Target = <<"/exchange/amq.direct/", Src/binary>>,
              _ = amqp10_publish_expect(Sess, Target, Dest, <<"hello">>, 1)
      end).

local_to_local_queue_args_src(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    shovel_test_utils:set_param(Config, ?PARAM,
                                [{<<"src-protocol">>, <<"local">>},
                                 {<<"src-queue">>, Src},
                                 {<<"src-queue-args">>, #{<<"x-queue-type">> => <<"quorum">>}},
                                 {<<"dest-protocol">>, <<"local">>},
                                 {<<"dest-queue">>, Dest}
                                ]),
    Expected = lists:sort([[Src, <<"quorum">>], [Dest, <<"classic">>]]),
    ?assertMatch(Expected,
                 lists:sort(rabbit_ct_broker_helpers:rabbitmqctl_list(
                              Config, 0,
                              ["list_queues", "name", "type", "--no-table-headers"]))).

local_to_local_queue_args_dest(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    shovel_test_utils:set_param(Config, ?PARAM,
                                [{<<"src-protocol">>, <<"local">>},
                                 {<<"src-queue">>, Src},
                                 {<<"dest-protocol">>, <<"local">>},
                                 {<<"dest-queue">>, Dest},
                                 {<<"dest-queue-args">>, #{<<"x-queue-type">> => <<"quorum">>}}
                                ]),
    Expected = lists:sort([[Dest, <<"quorum">>], [Src, <<"classic">>]]),
    ?assertMatch(Expected,
                 lists:sort(rabbit_ct_broker_helpers:rabbitmqctl_list(
                              Config, 0,
                              ["list_queues", "name", "type", "--no-table-headers"]))).

local_to_local_predeclared_dest(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    declare_queue(Config, <<"/">>, Dest),
    with_amqp10_session(Config,
      fun (Sess) ->
             shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-queue">>, Src},
                                           {<<"dest-predeclared">>, true},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-queue">>, Dest}
                                          ]),
              _ = amqp10_publish_expect(Sess, Src, Dest, <<"hello">>, 1)
      end).

local_to_local_predeclared_quorum_dest(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    declare_queue(Config, <<"/">>, Dest, [{<<"x-queue-type">>, longstr, <<"quorum">>}]),
    with_amqp10_session(Config,
      fun (Sess) ->
             shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-queue">>, Src},
                                           {<<"dest-predeclared">>, true},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-queue">>, Dest}
                                          ]),
              _ = amqp10_publish_expect(Sess, Src, Dest, <<"hello">>, 1)
      end).

local_to_local_missing_predeclared_dest(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    shovel_test_utils:set_param_nowait(
      Config, ?PARAM, [{<<"src-protocol">>, <<"local">>},
                       {<<"src-queue">>, Src},
                       {<<"dest-predeclared">>, true},
                       {<<"dest-protocol">>, <<"local">>},
                       {<<"dest-queue">>, Dest}
                      ]),
    shovel_test_utils:await_no_shovel(Config, ?PARAM),
    %% The shovel parameter is only deleted when 'delete-after'
    %% is used. In any other failure, the shovel should
    %% remain and try to restart
    ?assertNotMatch(
       not_found,
       rabbit_ct_broker_helpers:rpc(
         Config, 0, rabbit_runtime_parameters, lookup,
         [<<"/">>, <<"shovel">>, ?PARAM])).

local_to_local_queue_status(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    shovel_test_utils:set_param(Config, ?PARAM,
                                [{<<"src-protocol">>, <<"local">>},
                                 {<<"src-queue">>, Src},
                                 {<<"dest-protocol">>, <<"local">>},
                                 {<<"dest-queue">>, Dest}
                                ]),
    Status = rabbit_ct_broker_helpers:rpc(Config, 0,
                                          rabbit_shovel_status, status, []),
    ?assertMatch([{_, dynamic, {running, _}, _, _}], Status),
    [{_, dynamic, {running, Info}, _, _}] = Status,
    ?assertMatch(<<"local">>, proplists:get_value(src_protocol, Info)),
    ?assertMatch(<<"local">>, proplists:get_value(dest_protocol, Info)),
    ?assertMatch(Src, proplists:get_value(src_queue, Info)),
    ?assertMatch(Dest, proplists:get_value(dest_queue, Info)),
    ok.

local_to_local_exchange_status(Config) ->
    DefExchange = <<"amq.direct">>,
    RK1 = <<"carrots">>,
    AltExchange = <<"amq.fanout">>,
    RK2 = <<"bunnies">>,
    shovel_test_utils:set_param(Config, ?PARAM,
                                [{<<"src-protocol">>, <<"local">>},
                                 {<<"src-exchange">>, DefExchange},
                                 {<<"src-exchange-key">>, RK1},
                                 {<<"dest-protocol">>, <<"local">>},
                                 {<<"dest-exchange">>, AltExchange},
                                 {<<"dest-exchange-key">>, RK2}
                                ]),
    Status = rabbit_ct_broker_helpers:rpc(Config, 0,
                                          rabbit_shovel_status, status, []),
    ?assertMatch([{_, dynamic, {running, _}, _, _}], Status),
    [{_, dynamic, {running, Info}, _, _}] = Status,
    ?assertMatch(<<"local">>, proplists:get_value(src_protocol, Info)),
    ?assertMatch(<<"local">>, proplists:get_value(dest_protocol, Info)),
    ?assertMatch(match, re:run(proplists:get_value(src_queue, Info),
                               "amq\.gen.*", [{capture, none}])),
    ?assertMatch(DefExchange, proplists:get_value(src_exchange, Info)),
    ?assertMatch(RK1, proplists:get_value(src_exchange_key, Info)),
    ?assertMatch(AltExchange, proplists:get_value(dest_exchange, Info)),
    ?assertMatch(RK2, proplists:get_value(dest_exchange_key, Info)),
    ok.

local_to_local_queue_and_exchange_src_fails(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    %% Setting both queue and exchange for source fails
    try
        shovel_test_utils:set_param(Config, ?PARAM,
                                    [{<<"src-protocol">>, <<"local">>},
                                     {<<"src-queue">>, Src},
                                     {<<"src-exchange">>, <<"amq.direct">>},
                                     {<<"src-exchange-key">>, <<"bunnies">>},
                                     {<<"dest-protocol">>, <<"local">>},
                                     {<<"dest-queue">>, Dest}
                                    ]),
        throw(unexpected_success)
    catch
        _:{badmatch, {error_string, Reason}} ->
            ?assertMatch(match, re:run(Reason, "Validation failed", [{capture, none}]))
    end.

local_to_local_queue_and_exchange_dest_fails(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    %% Setting both queue and exchange for dest fails
    try
        shovel_test_utils:set_param(Config, ?PARAM,
                                    [{<<"src-protocol">>, <<"local">>},
                                     {<<"src-queue">>, Src},
                                     {<<"dest-protocol">>, <<"local">>},
                                     {<<"dest-queue">>, Dest},
                                     {<<"dest-exchange">>, <<"amq.direct">>},
                                     {<<"dest-exchange-key">>, <<"bunnies">>}
                                    ]),
        throw(unexpected_success)
    catch
        _:{badmatch, {error_string, Reason}} ->
            ?assertMatch(match, re:run(Reason, "Validation failed", [{capture, none}]))
    end.

local_to_local_delete_after_never(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    with_amqp10_session(Config,
      fun (Sess) ->
             shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-queue">>, Src},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-queue">>, Dest}
                                          ]),
              amqp10_publish(Sess, Src, <<"tag1">>, 20),
              amqp10_expect_count(Sess, Dest, 20)
      end).

local_to_local_delete_after_queue_length_zero(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    declare_queue(Config, <<"/">>, Src),
    shovel_test_utils:set_param_nowait(Config, ?PARAM,
                                       [{<<"src-protocol">>, <<"local">>},
                                        {<<"src-predeclared">>, true},
                                        {<<"src-queue">>, Src},
                                        {<<"src-delete-after">>, <<"queue-length">>},
                                        {<<"dest-protocol">>, <<"local">>},
                                        {<<"dest-queue">>, Dest}
                                       ]),
    shovel_test_utils:await_no_shovel(Config, ?PARAM),
    %% The shovel parameter is only deleted when 'delete-after'
    %% is used. In any other failure, the shovel should
    %% remain and try to restart
    ?assertMatch(not_found, rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_runtime_parameters, lookup, [<<"/">>, <<"shovel">>, ?PARAM])).

local_to_local_delete_after_queue_length(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    declare_queue(Config, <<"/">>, Src),
    with_amqp10_session(Config,
      fun (Sess) ->
              amqp10_publish(Sess, Src, <<"tag1">>, 18),
              shovel_test_utils:set_param_nowait(Config, ?PARAM,
                                                 [{<<"src-protocol">>, <<"local">>},
                                                  {<<"src-predeclared">>, true},
                                                  {<<"src-queue">>, Src},
                                                  {<<"src-delete-after">>, <<"queue-length">>},
                                                  {<<"dest-protocol">>, <<"local">>},
                                                  {<<"dest-queue">>, Dest}
                                                 ]),
              %% The shovel parameter is only deleted when 'delete-after'
              %% is used. In any other failure, the shovel should
              %% remain and try to restart
              amqp10_expect_count(Sess, Dest, 18),
              await_autodelete(Config, ?PARAM),
              amqp10_publish(Sess, Src, <<"tag1">>, 5),
              amqp10_expect_empty(Sess, Dest)
      end).

local_to_local_delete_after_with_rejections(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    VHost = <<"/">>,
    declare_queue(Config, VHost, Dest, [{<<"x-max-length">>, long, 5},
                                        {<<"x-overflow">>, longstr, <<"reject-publish">>}]),
    with_amqp10_session(Config,
      fun (Sess) ->
              shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-queue">>, Src},
                                           {<<"src-delete-after">>, 10},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-predeclared">>, true},
                                           {<<"dest-queue">>, Dest}
                                          ]),
              amqp10_publish(Sess, Src, <<"tag1">>, 10),
              ?awaitMatch(not_found, rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_runtime_parameters, lookup, [<<"/">>, <<"shovel">>, ?PARAM]), 30_000),
              Expected = lists:sort([[Src, <<"5">>], [Dest, <<"5">>]]),
              ?awaitMatch(
                 Expected,
                 lists:sort(rabbit_ct_broker_helpers:rabbitmqctl_list(
                              Config, 0,
                              ["list_queues", "name", "messages_ready", "--no-table-headers"])),
                 30_000)

      end).

local_to_local_no_ack(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    with_amqp10_session(Config,
      fun (Sess) ->
             shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-queue">>, Src},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-queue">>, Dest},
                                           {<<"ack-mode">>, <<"no-ack">>}
                                          ]),
              _ = amqp10_publish_expect(Sess, Src, Dest, <<"hello">>, 1)
      end).

local_to_local_quorum_no_ack(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    VHost = <<"/">>,
    declare_queue(Config, VHost, Src, [{<<"x-queue-type">>, longstr, <<"quorum">>}]),
    declare_queue(Config, VHost, Dest, [{<<"x-queue-type">>, longstr, <<"quorum">>}]),
    with_amqp10_session(Config,
      fun (Sess) ->
             shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-predeclared">>, true},
                                           {<<"src-queue">>, Src},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-predeclared">>, true},
                                           {<<"dest-queue">>, Dest},
                                           {<<"ack-mode">>, <<"no-ack">>}
                                          ]),
              _ = amqp10_publish_expect(Sess, Src, Dest, <<"hello">>, 1)
      end).

local_to_local_stream_no_ack(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    declare_queue(Config, <<"/">>, Src, [{<<"x-queue-type">>, longstr, <<"stream">>}]),
    declare_queue(Config, <<"/">>, Dest, [{<<"x-queue-type">>, longstr, <<"stream">>}]),
    with_amqp10_session(Config,
      fun (Sess) ->
              shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-queue">>, Src},
                                           {<<"src-predeclared">>, true},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-predeclared">>, true},
                                           {<<"dest-queue">>, Dest},
                                           {<<"ack-mode">>, <<"no-ack">>}
                                          ]),
              Receiver = amqp10_subscribe(Sess, Dest),
              amqp10_publish(Sess, Src, <<"tag1">>, 10),
              ?awaitMatch([{_Name, dynamic, {running, _}, #{forwarded := 10}, _}],
                          rabbit_ct_broker_helpers:rpc(Config, 0,
                                                       rabbit_shovel_status, status, []),
                          30000),
              _ = amqp10_expect(Receiver, 10, []),
              amqp10_client:detach_link(Receiver)
      end).

local_to_local_on_confirm(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    with_amqp10_session(Config,
      fun (Sess) ->
             shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-queue">>, Src},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-queue">>, Dest},
                                           {<<"ack-mode">>, <<"on-confirm">>}
                                          ]),
              _ = amqp10_publish_expect(Sess, Src, Dest, <<"hello">>, 1)
      end).

local_to_local_quorum_on_confirm(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    VHost = <<"/">>,
    declare_queue(Config, VHost, Src, [{<<"x-queue-type">>, longstr, <<"quorum">>}]),
    declare_queue(Config, VHost, Dest, [{<<"x-queue-type">>, longstr, <<"quorum">>}]),
    with_amqp10_session(Config,
      fun (Sess) ->
             shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-predeclared">>, true},
                                           {<<"src-queue">>, Src},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-predeclared">>, true},
                                           {<<"dest-queue">>, Dest},
                                           {<<"ack-mode">>, <<"on-confirm">>}
                                          ]),
              _ = amqp10_publish_expect(Sess, Src, Dest, <<"hello">>, 1)
      end).

local_to_local_stream_on_confirm(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    VHost = <<"/">>,
    declare_queue(Config, VHost, Src, [{<<"x-queue-type">>, longstr, <<"stream">>}]),
    declare_queue(Config, VHost, Dest, [{<<"x-queue-type">>, longstr, <<"stream">>}]),
    with_amqp10_session(Config,
      fun (Sess) ->
             shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-predeclared">>, true},
                                           {<<"src-queue">>, Src},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-predeclared">>, true},
                                           {<<"dest-queue">>, Dest},
                                           {<<"ack-mode">>, <<"on-confirm">>}
                                          ]),
              Receiver = amqp10_subscribe(Sess, Dest),
              amqp10_publish(Sess, Src, <<"tag1">>, 10),
              ?awaitMatch([{_Name, dynamic, {running, _}, #{forwarded := 10}, _}],
                          rabbit_ct_broker_helpers:rpc(Config, 0,
                                                       rabbit_shovel_status, status, []),
                          30000),
              _ = amqp10_expect(Receiver, 10, []),
              amqp10_client:detach_link(Receiver)
      end).

local_to_local_on_publish(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    with_amqp10_session(Config,
      fun (Sess) ->
             shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-queue">>, Src},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-queue">>, Dest},
                                           {<<"ack-mode">>, <<"on-publish">>}
                                          ]),
              _ = amqp10_publish_expect(Sess, Src, Dest, <<"hello">>, 1)
      end).

local_to_local_quorum_on_publish(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    VHost = <<"/">>,
    declare_queue(Config, VHost, Src, [{<<"x-queue-type">>, longstr, <<"quorum">>}]),
    declare_queue(Config, VHost, Dest, [{<<"x-queue-type">>, longstr, <<"quorum">>}]),
    with_amqp10_session(Config,
      fun (Sess) ->
             shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-predeclared">>, true},
                                           {<<"src-queue">>, Src},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-predeclared">>, true},
                                           {<<"dest-queue">>, Dest},
                                           {<<"ack-mode">>, <<"on-publish">>}
                                          ]),
              _ = amqp10_publish_expect(Sess, Src, Dest, <<"hello">>, 1)
      end).

local_to_local_stream_on_publish(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    VHost = <<"/">>,
    declare_queue(Config, VHost, Src, [{<<"x-queue-type">>, longstr, <<"stream">>}]),
    declare_queue(Config, VHost, Dest, [{<<"x-queue-type">>, longstr, <<"stream">>}]),
    with_amqp10_session(Config,
      fun (Sess) ->
             shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-predeclared">>, true},
                                           {<<"src-queue">>, Src},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-predeclared">>, true},
                                           {<<"dest-queue">>, Dest},
                                           {<<"ack-mode">>, <<"on-publish">>}
                                          ]),
              Receiver = amqp10_subscribe(Sess, Dest),
              amqp10_publish(Sess, Src, <<"tag1">>, 10),
              ?awaitMatch([{_Name, dynamic, {running, _}, #{forwarded := 10}, _}],
                          rabbit_ct_broker_helpers:rpc(Config, 0,
                                                       rabbit_shovel_status, status, []),
                          30000),
              _ = amqp10_expect(Receiver, 10, []),
              amqp10_client:detach_link(Receiver)
      end).

local_to_local_reject_publish(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    declare_queue(Config, <<"/">>, Dest, [{<<"x-max-length">>, long, 1},
                                          {<<"x-overflow">>, longstr, <<"reject-publish">>}
                                         ]),
    with_amqp10_session(
      Config,
      fun (Sess) ->
              amqp10_publish(Sess, Src, <<"tag1">>, 5),
              shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-queue">>, Src},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-predeclared">>, true},
                                           {<<"dest-queue">>, Dest},
                                           {<<"ack-mode">>, <<"on-confirm">>}
                                          ]),
              amqp10_expect_count(Sess, Dest, 1)
      end).

local_to_amqp091(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    with_amqp10_session(Config,
      fun (Sess) ->
             shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-queue">>, Src},
                                           {<<"dest-protocol">>, <<"amqp091">>},
                                           {<<"dest-queue">>, Dest}
                                          ]),
              _ = amqp10_publish_expect(Sess, Src, Dest, <<"hello">>, 1)
      end).

local_to_amqp10(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    with_amqp10_session(Config,
      fun (Sess) ->
             shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-queue">>, Src},
                                           {<<"dest-protocol">>, <<"amqp10">>},
                                           {<<"dest-address">>, Dest}
                                          ]),
              _ = amqp10_publish_expect(Sess, Src, Dest, <<"hello">>, 1)
      end).

amqp091_to_local(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    with_amqp10_session(Config,
      fun (Sess) ->
             shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"amqp091">>},
                                           {<<"src-queue">>, Src},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-queue">>, Dest}
                                          ]),
              _ = amqp10_publish_expect(Sess, Src, Dest, <<"hello">>, 1)
      end).

amqp10_to_local(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    with_amqp10_session(Config,
      fun (Sess) ->
             shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"amqp10">>},
                                           {<<"src-address">>, Src},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-queue">>, Dest}
                                          ]),
              _ = amqp10_publish_expect(Sess, Src, Dest, <<"hello">>, 1)
      end).

local_to_local_delete_src_queue(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    with_amqp10_session(Config,
      fun (Sess) ->
             shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-queue">>, Src},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-queue">>, Dest}
                                          ]),
              _ = amqp10_publish_expect(Sess, Src, Dest, <<"hello">>, 1),
              ?awaitMatch([{_Name, dynamic, {running, _}, #{forwarded := 1}, _}],
                          rabbit_ct_broker_helpers:rpc(Config, 0,
                                                       rabbit_shovel_status, status, []),
                          30000),
              rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_queue,
                                           [Src, <<"/">>]),
              ?awaitMatch([{_Name, dynamic, {terminated,source_queue_down}, _, _}],
                          rabbit_ct_broker_helpers:rpc(Config, 0,
                                                       rabbit_shovel_status, status, []),
                          30000)
      end).

local_to_local_delete_dest_queue(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    with_amqp10_session(Config,
      fun (Sess) ->
             shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-queue">>, Src},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-queue">>, Dest}
                                          ]),
              _ = amqp10_publish_expect(Sess, Src, Dest, <<"hello">>, 1),
              ?awaitMatch([{_Name, dynamic, {running, _}, #{forwarded := 1}, _}],
                          rabbit_ct_broker_helpers:rpc(Config, 0,
                                                       rabbit_shovel_status, status, []),
                          30000),
              rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_queue,
                                           [Dest, <<"/">>]),
              ?awaitMatch([{_Name, dynamic, {terminated, dest_queue_down}, _, _}],
                          rabbit_ct_broker_helpers:rpc(Config, 0,
                                                       rabbit_shovel_status, status, []),
                          30000)
      end).

local_to_local_vhost_access(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    AltVHost = ?config(alt_vhost, Config),
    ok = rabbit_ct_broker_helpers:add_vhost(Config, AltVHost),
    Uri = shovel_test_utils:make_uri(Config, 0, AltVHost),
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, rabbit_runtime_parameters, set,
           [<<"/">>, <<"shovel">>, ?PARAM, [{<<"src-uri">>,  Uri},
                                            {<<"dest-uri">>, [Uri]},
                                            {<<"src-protocol">>, <<"local">>},
                                            {<<"src-queue">>, Src},
                                            {<<"dest-protocol">>, <<"local">>},
                                            {<<"dest-queue">>, Dest}],
            none]),
    shovel_test_utils:await_no_shovel(Config, ?PARAM).

local_to_local_user_access(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    Uri = shovel_test_utils:make_uri(
            Config, 0, <<"guest">>, <<"forgotmypassword">>, <<"%2F">>),
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, rabbit_runtime_parameters, set,
           [<<"/">>, <<"shovel">>, ?PARAM, [{<<"src-uri">>,  Uri},
                                            {<<"dest-uri">>, [Uri]},
                                            {<<"src-protocol">>, <<"local">>},
                                            {<<"src-queue">>, Src},
                                            {<<"dest-protocol">>, <<"local">>},
                                            {<<"dest-queue">>, Dest}],
            none]),
    shovel_test_utils:await_no_shovel(Config, ?PARAM).

local_to_local_credit_flow_on_confirm(Config) ->
    local_to_local_credit_flow(Config, <<"on-confirm">>).

local_to_local_credit_flow_on_publish(Config) ->
    local_to_local_credit_flow(Config, <<"on-publish">>).

local_to_local_credit_flow_no_ack(Config) ->
    local_to_local_credit_flow(Config, <<"no-ack">>).

local_to_local_credit_flow(Config, AckMode) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    with_amqp10_session(Config,
      fun (Sess) ->
             shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-queue">>, Src},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-queue">>, Dest},
                                           {<<"ack-mode">>, AckMode}
                                          ]),
              amqp10_publish(Sess, Src, <<"tag1">>, 1000),
              amqp10_expect_count(Sess, Dest, 1000)
      end).

local_to_local_quorum_credit_flow_on_confirm(Config) ->
    local_to_local_quorum_credit_flow(Config, <<"on-confirm">>).

local_to_local_quorum_credit_flow_on_publish(Config) ->
    local_to_local_quorum_credit_flow(Config, <<"on-publish">>).

local_to_local_quorum_credit_flow_no_ack(Config) ->
    local_to_local_quorum_credit_flow(Config, <<"no-ack">>).

local_to_local_quorum_credit_flow(Config, AckMode) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    VHost = <<"/">>,
    declare_queue(Config, VHost, Src, [{<<"x-queue-type">>, longstr, <<"quorum">>}]),
    declare_queue(Config, VHost, Dest, [{<<"x-queue-type">>, longstr, <<"quorum">>}]),
    with_amqp10_session(Config,
      fun (Sess) ->
             shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-queue">>, Src},
                                           {<<"src-predeclared">>, true},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-queue">>, Dest},
                                           {<<"dest-predeclared">>, true},
                                           {<<"ack-mode">>, AckMode}
                                          ]),
              amqp10_publish(Sess, Src, <<"tag1">>, 1000),
              amqp10_expect_count(Sess, Dest, 1000)
      end).

local_to_local_stream_credit_flow_on_confirm(Config) ->
    local_to_local_stream_credit_flow(Config, <<"on-confirm">>).

local_to_local_stream_credit_flow_on_publish(Config) ->
    local_to_local_stream_credit_flow(Config, <<"on-publish">>).

local_to_local_stream_credit_flow_no_ack(Config) ->
    local_to_local_stream_credit_flow(Config, <<"no-ack">>).

local_to_local_stream_credit_flow(Config, AckMode) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    VHost = <<"/">>,
    declare_queue(Config, VHost, Src, [{<<"x-queue-type">>, longstr, <<"stream">>}]),
    declare_queue(Config, VHost, Dest, [{<<"x-queue-type">>, longstr, <<"stream">>}]),
    with_amqp10_session(Config,
      fun (Sess) ->
             shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-queue">>, Src},
                                           {<<"src-predeclared">>, true},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-queue">>, Dest},
                                           {<<"dest-predeclared">>, true},
                                           {<<"ack-mode">>, AckMode}
                                          ]),

              Receiver = amqp10_subscribe(Sess, Dest),
              amqp10_publish(Sess, Src, <<"tag1">>, 1000),
              ?awaitMatch([{_Name, dynamic, {running, _}, #{forwarded := 1000}, _}],
                          rabbit_ct_broker_helpers:rpc(Config, 0,
                                                       rabbit_shovel_status, status, []),
                          30000),
              _ = amqp10_expect(Receiver, 1000, []),
              amqp10_client:detach_link(Receiver)
      end).

local_to_local_simple_uri(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    Uri = <<"amqp://">>,
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, rabbit_runtime_parameters, set,
           [<<"/">>, <<"shovel">>, ?PARAM, [{<<"src-uri">>,  Uri},
                                            {<<"dest-uri">>, [Uri]},
                                            {<<"src-protocol">>, <<"local">>},
                                            {<<"src-queue">>, Src},
                                            {<<"dest-protocol">>, <<"local">>},
                                            {<<"dest-queue">>, Dest}],
            none]),
    shovel_test_utils:await_shovel(Config, ?PARAM).

local_to_local_counters(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    %% Let's restart the node so the counters are reset
    ok = rabbit_ct_broker_helpers:stop_node(Config, 0),
    ok = rabbit_ct_broker_helpers:start_node(Config, 0),
    with_amqp10_session(
      Config,
      fun (Sess) ->
              ?awaitMatch(#{publishers := 0, consumers := 0},
                          get_global_counters(Config), 30_000),
              shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-queue">>, Src},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-queue">>, Dest}
                                          ]),
              ?awaitMatch(#{publishers := 1, consumers := 1},
                          get_global_counters(Config), 30_000),
              _ = amqp10_publish(Sess, Src, <<"tag1">>, 150),
              ?awaitMatch(#{consumers := 1, publishers := 1,
                            messages_received_total := 150,
                            messages_received_confirm_total := 150,
                            messages_routed_total := 150,
                            messages_unroutable_dropped_total := 0,
                            messages_unroutable_returned_total := 0,
                            messages_confirmed_total := 150},
                          get_global_counters(Config), 30_000)
      end).

%%----------------------------------------------------------------------------
declare_queue(Config, VHost, QName) ->
    declare_queue(Config, VHost, QName, []).

declare_queue(Config, VHost, QName, Args) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0, VHost),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    ?assertEqual(
       {'queue.declare_ok', QName, 0, 0},
       amqp_channel:call(
         Ch, #'queue.declare'{queue = QName, durable = true, arguments = Args})),
    rabbit_ct_client_helpers:close_channel(Ch),
    rabbit_ct_client_helpers:close_connection(Conn).

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

delete_queue(Name, VHost) ->
    QName = rabbit_misc:r(VHost, queue, Name),
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} ->
            {ok, _} = rabbit_amqqueue:delete(Q, false, false, <<"dummy">>);
        _ ->
            ok
    end.

get_global_counters(Config) ->
    get_global_counters0(Config, #{protocol => 'local-shovel'}).

get_global_counters0(Config, Key) ->
    Overview = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_global_counters, overview, []),
    maps:get(Key, Overview).
