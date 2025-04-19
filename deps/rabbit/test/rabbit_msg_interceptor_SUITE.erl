%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(rabbit_msg_interceptor_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile([nowarn_export_all, export_all]).

-import(rabbit_ct_helpers, [eventually/1]).

all() ->
    [
     {group, cluster_size_1}
    ].

groups() ->
    [
     {cluster_size_1, [shuffle],
      [incoming_overwrite,
       incoming_no_overwrite,
       outgoing]}
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(rabbitmq_amqp_client),
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_testcase(Testcase, Config0) ->
    Config1 = rabbit_ct_helpers:set_config(
                Config0, [{rmq_nodename_suffix, Testcase}]),
    Val = case Testcase of
              incoming_overwrite ->
                  [{rabbit_msg_interceptor_routing_node, #{overwrite => true}},
                   {rabbit_msg_interceptor_timestamp, #{incoming => true,
                                                        overwrite => true}}];
              incoming_no_overwrite ->
                  [{rabbit_msg_interceptor_routing_node, #{overwrite => false}},
                   {rabbit_msg_interceptor_timestamp, #{incoming => true,
                                                        overwrite => false}}];
              outgoing ->
                  [{rabbit_msg_interceptor_timestamp, #{outgoing => true}}]
          end,
    Config = rabbit_ct_helpers:merge_app_env(
               Config1, {rabbit, [{message_interceptors, Val}]}),
    rabbit_ct_helpers:run_steps(
      Config,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_testcase(Testcase, Config0) ->
    Config = rabbit_ct_helpers:testcase_finished(Config0, Testcase),
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

incoming_overwrite(Config) ->
    incoming(true, Config).

incoming_no_overwrite(Config) ->
    incoming(false, Config).

incoming(Overwrite, Config) ->
    Server = atom_to_binary(rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename)),
    Payload = QName = atom_to_binary(?FUNCTION_NAME),
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName}),
    NowSecs = os:system_time(second),
    NowMs = os:system_time(millisecond),
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName},
                      #amqp_msg{payload = Payload}),
    AssertHeaders =
    fun() ->
            eventually(
              ?_assertMatch(
                 {#'basic.get_ok'{},
                  #amqp_msg{payload = Payload,
                            props = #'P_basic'{
                                       timestamp = Secs,
                                       headers = [{<<"timestamp_in_ms">>, long, ReceivedMs},
                                                  {<<"x-routed-by">>, longstr, Server}]
                                      }}}
                   when ReceivedMs < NowMs + 5000 andalso
                        ReceivedMs > NowMs - 5000 andalso
                        Secs < NowSecs + 5 andalso
                        Secs > NowSecs - 5,
                 amqp_channel:call(Ch, #'basic.get'{queue = QName})))
    end,
    AssertHeaders(),

    Msg = #amqp_msg{payload = Payload,
                    props = #'P_basic'{
                               timestamp = 1,
                               headers = [{<<"timestamp_in_ms">>, long, 1000},
                                          {<<"x-routed-by">>, longstr, <<"rabbit@my-node">>}]
                              }},
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName}, Msg),
    case Overwrite of
        true ->
            AssertHeaders();
        false ->
            eventually(
              ?_assertMatch(
                 {#'basic.get_ok'{}, Msg},
                 amqp_channel:call(Ch, #'basic.get'{queue = QName})))
    end,

    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok.

outgoing(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Address = rabbitmq_amqp_address:queue(QName),
    {_, Session, LinkPair} = Init = amqp_utils:init(Config),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{}),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"receiver">>, Address, settled),
    {ok, Sender} = amqp10_client:attach_sender_link_sync(
                     Session, <<"sender">>, Address, settled),
    ok = amqp_utils:wait_for_credit(Sender),

    Now = os:system_time(millisecond),
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"tag">>, <<"msg">>, true)),

    {ok, Msg} = amqp10_client:get_msg(Receiver),
    #{<<"x-opt-rabbitmq-sent-time">> := Sent} = amqp10_msg:message_annotations(Msg),
    ct:pal("client sent message at ~b~nRabbitMQ sent message at ~b",
           [Now, Sent]),
    ?assert(Sent > Now - 5000),
    ?assert(Sent < Now + 5000),

    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:detach_link(Receiver),
    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = amqp_utils:close(Init).
