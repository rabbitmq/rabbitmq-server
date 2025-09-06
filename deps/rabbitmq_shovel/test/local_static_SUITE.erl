%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(local_static_SUITE).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-compile(export_all).

-define(EXCHANGE,    <<"test_exchange">>).
-define(TO_SHOVEL,   <<"to_the_shovel">>).
-define(FROM_SHOVEL, <<"from_the_shovel">>).
-define(UNSHOVELLED, <<"unshovelled">>).
-define(SHOVELLED,   <<"shovelled">>).
-define(TIMEOUT,     1000).

all() ->
    [
      {group, tests}
    ].

groups() ->
    [
      {tests, [], [
          local_destination_no_ack,
          local_destination_on_publish,
          local_destination_on_confirm,
          local_destination_forward_headers_amqp10,
          local_destination_forward_headers_amqp091,
          local_destination_no_forward_headers_amqp10,
          local_destination_timestamp_header_amqp10,
          local_destination_timestamp_header_amqp091,
          local_destination_no_timestamp_header_amqp10,
          local_source_no_ack,
          local_source_on_publish,
          local_source_on_confirm,
          local_source_anonymous_queue
        ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(amqp10_client),
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE},
        {ignored_crashes, [
            "server_initiated_close,404",
            "writer,send_failed,closed",
            "source_queue_down",
            "dest_queue_down"
          ]}
      ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
          rabbit_ct_client_helpers:setup_steps() ++
          [fun stop_shovel_plugin/1]).

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

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_queues, [[<<"source-queue">>, <<"dest-queue">>]]),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

stop_shovel_plugin(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
                                      application, stop, [rabbitmq_shovel]),
    Config.

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

%% TODO test errors when queue has not been predeclared, it just crashes
%% with a case_clause right now

local_destination_no_ack(Config) ->
    local_destination(Config, no_ack).

local_destination_on_publish(Config) ->
    local_destination(Config, on_publish).

local_destination_on_confirm(Config) ->
    local_destination(Config, on_confirm).

local_destination(Config, AckMode) ->
    TargetQ =  <<"dest-queue">>,
    ok = setup_local_destination_shovel(Config, TargetQ, AckMode, []),
    {Conn, Receiver} = attach_receiver(Config, TargetQ),
    Chan = rabbit_ct_client_helpers:open_channel(Config, 0),
    Timestamp = erlang:system_time(millisecond),
    Msg = #amqp_msg{payload = <<42>>,
                    props = #'P_basic'{delivery_mode = 2,
                                       headers = [{<<"header1">>, long, 1},
                                                  {<<"header2">>, longstr, <<"h2">>}],
                                       content_encoding = ?UNSHOVELLED,
                                       content_type = ?UNSHOVELLED,
                                       correlation_id = ?UNSHOVELLED,
                                       %% needs to be guest here
                                       user_id = <<"guest">>,
                                       message_id = ?UNSHOVELLED,
                                       reply_to = ?UNSHOVELLED,
                                       timestamp = Timestamp,
                                       type = ?UNSHOVELLED
                                      }},
    publish(Chan, Msg, ?EXCHANGE, ?TO_SHOVEL),

    receive
        {amqp10_msg, Receiver, InMsg} ->
            ReplyTo = <<"/queues/", ?UNSHOVELLED/binary>>,
            [<<42>>] = amqp10_msg:body(InMsg),
            #{content_type := ?UNSHOVELLED,
              content_encoding := ?UNSHOVELLED,
              correlation_id := ?UNSHOVELLED,
              user_id := <<"guest">>,
              message_id := ?UNSHOVELLED,
              reply_to := ReplyTo
             } = amqp10_msg:properties(InMsg),
            #{<<"header1">> := 1,
              <<"header2">> := <<"h2">>
             } = amqp10_msg:application_properties(InMsg),
            #{<<"x-basic-type">> := ?UNSHOVELLED
             } = amqp10_msg:message_annotations(InMsg),
            #{durable := true} = amqp10_msg:headers(InMsg),
            ok
    after ?TIMEOUT ->
              throw(timeout_waiting_for_deliver1)
    end,


    ?awaitMatch([[_, <<"1">>, <<"0">>],
                 [<<"dest-queue">>, <<"1">>, <<"0">>]],
                lists:sort(
                  rabbit_ct_broker_helpers:rabbitmqctl_list(
                    Config, 0,
                    ["list_queues", "name", "consumers", "messages", "--no-table-headers"])),
                30000),

    [{test_shovel, static, {running, _Info}, _Metrics, _Time}] =
        rabbit_ct_broker_helpers:rpc(Config, 0,
          rabbit_shovel_status, status, []),
    detach_receiver(Conn, Receiver),
    rabbit_ct_client_helpers:close_channel(Chan).

local_destination_forward_headers_amqp10(Config) ->
    TargetQ = <<"dest-queue">>,
    ok = setup_local_destination_shovel(Config, TargetQ, on_publish,
                                        [{add_forward_headers, true}]),
    {Conn, Receiver} = attach_receiver(Config, TargetQ),
    Chan = rabbit_ct_client_helpers:open_channel(Config, 0),

    Msg = #amqp_msg{props = #'P_basic'{}},
    publish(Chan, Msg, ?EXCHANGE, ?TO_SHOVEL),

    receive
        {amqp10_msg, Receiver, InMsg} ->
            ?assertMatch(#{<<"x-opt-shovelled-by">> := _,
                           <<"x-opt-shovel-type">> := <<"static">>,
                           <<"x-opt-shovel-name">> := <<"test_shovel">>},
                         amqp10_msg:message_annotations(InMsg))
    after ?TIMEOUT ->
              throw(timeout_waiting_for_deliver1)
    end,

    detach_receiver(Conn, Receiver),
    rabbit_ct_client_helpers:close_channel(Chan).

local_destination_forward_headers_amqp091(Config) ->
    %% Check that we can consume with 0.9.1 or 1.0 and no properties are
    %% lost in translation
    TargetQ = <<"dest-queue">>,
    ok = setup_local_destination_shovel(Config, TargetQ, on_publish,
                                        [{add_forward_headers, true}]),
    Chan = rabbit_ct_client_helpers:open_channel(Config, 0),
    CTag = consume(Chan, TargetQ, true),

    Msg = #amqp_msg{props = #'P_basic'{}},
    publish(Chan, Msg, ?EXCHANGE, ?TO_SHOVEL),

    receive
        {#'basic.deliver'{consumer_tag = CTag},
         #amqp_msg{props = #'P_basic'{headers = Headers}}} ->
            ?assertMatch([{<<"x-opt-shovel-name">>, longstr, <<"test_shovel">>},
                          {<<"x-opt-shovel-type">>, longstr, <<"static">>},
                          {<<"x-opt-shovelled-by">>, longstr, _}],
                         lists:sort(Headers))
    after ?TIMEOUT -> throw(timeout_waiting_for_deliver1)
    end,

    rabbit_ct_client_helpers:close_channel(Chan).

local_destination_no_forward_headers_amqp10(Config) ->
    TargetQ =  <<"dest-queue">>,
    ok = setup_local_destination_shovel(Config, TargetQ, on_publish,
                                        [{add_forward_headers, false}]),
    {Conn, Receiver} = attach_receiver(Config, TargetQ),
    Chan = rabbit_ct_client_helpers:open_channel(Config, 0),

    Msg = #amqp_msg{props = #'P_basic'{}},
    publish(Chan, Msg, ?EXCHANGE, ?TO_SHOVEL),

    receive
        {amqp10_msg, Receiver, InMsg} ->
            Anns = amqp10_msg:message_annotations(InMsg),
            ?assertNot(maps:is_key(<<"x-opt-shovelled-by">>, Anns)),
            ?assertNot(maps:is_key(<<"x-opt-shovel-type">>, Anns)),
            ?assertNot(maps:is_key(<<"x-opt-shovel-name">>, Anns)),
            ok
    after ?TIMEOUT ->
              throw(timeout_waiting_for_deliver1)
    end,

    detach_receiver(Conn, Receiver),
    rabbit_ct_client_helpers:close_channel(Chan).

local_destination_timestamp_header_amqp10(Config) ->
    TargetQ = <<"dest-queue">>,
    ok = setup_local_destination_shovel(Config, TargetQ, on_publish,
                                        [{add_timestamp_header, true}]),
    {Conn, Receiver} = attach_receiver(Config, TargetQ),
    Chan = rabbit_ct_client_helpers:open_channel(Config, 0),

    Msg = #amqp_msg{props = #'P_basic'{}},
    publish(Chan, Msg, ?EXCHANGE, ?TO_SHOVEL),

    receive
        {amqp10_msg, Receiver, InMsg} ->
            ?assertMatch(#{<<"x-opt-shovelled-timestamp">> := _},
                         amqp10_msg:message_annotations(InMsg))
    after ?TIMEOUT ->
              throw(timeout_waiting_for_deliver1)
    end,

    detach_receiver(Conn, Receiver),
    rabbit_ct_client_helpers:close_channel(Chan).

local_destination_timestamp_header_amqp091(Config) ->
    TargetQ = <<"dest-queue">>,
    ok = setup_local_destination_shovel(Config, TargetQ, on_publish,
                                        [{add_timestamp_header, true}]),
    Chan = rabbit_ct_client_helpers:open_channel(Config, 0),
    CTag = consume(Chan, TargetQ, true),

    Msg = #amqp_msg{props = #'P_basic'{}},
    publish(Chan, Msg, ?EXCHANGE, ?TO_SHOVEL),

    receive
        {#'basic.deliver'{consumer_tag = CTag},
         #amqp_msg{props = #'P_basic'{headers = Headers}}} ->
            ?assertMatch([{<<"x-opt-shovelled-timestamp">>, long, _}],
                         Headers)
    after ?TIMEOUT -> throw(timeout_waiting_for_deliver1)
    end,

    rabbit_ct_client_helpers:close_channel(Chan).

local_destination_no_timestamp_header_amqp10(Config) ->
    TargetQ =  <<"dest-queue">>,
    ok = setup_local_destination_shovel(Config, TargetQ, on_publish,
                                        [{add_timestamp_header, false}]),
    {Conn, Receiver} = attach_receiver(Config, TargetQ),
    Chan = rabbit_ct_client_helpers:open_channel(Config, 0),

    Msg = #amqp_msg{props = #'P_basic'{}},
    publish(Chan, Msg, ?EXCHANGE, ?TO_SHOVEL),

    receive
        {amqp10_msg, Receiver, InMsg} ->
            Anns = amqp10_msg:message_annotations(InMsg),
            ?assertNot(maps:is_key(<<"x-opt-shovelled-timestamp">>, Anns))
    after ?TIMEOUT ->
              throw(timeout_waiting_for_deliver1)
    end,

    detach_receiver(Conn, Receiver),
    rabbit_ct_client_helpers:close_channel(Chan).

local_source_no_ack(Config) ->
    local_source(Config, no_ack).

local_source_on_publish(Config) ->
    local_source(Config, on_publish).

local_source_on_confirm(Config) ->
    local_source(Config, on_confirm).

local_source(Config, AckMode) ->
    SourceQ =  <<"source-queue">>,
    DestQ =  <<"dest-queue">>,
    ok = setup_local_source_shovel(Config, SourceQ, DestQ, AckMode),
    Chan = rabbit_ct_client_helpers:open_channel(Config, 0),
    CTag = consume(Chan, DestQ, AckMode =:= no_ack),
    Msg = #amqp_msg{payload = <<42>>,
                    props = #'P_basic'{delivery_mode = 2,
                                       content_type = ?UNSHOVELLED}},
    % publish to source
    publish(Chan, Msg, <<>>, SourceQ),

    receive
        {#'basic.deliver'{consumer_tag = CTag, delivery_tag = AckTag},
         #amqp_msg{payload = <<42>>,
                   props = #'P_basic'{headers = [{<<"x-shovelled">>, _, _},
                                                 {<<"x-shovelled-timestamp">>,
                                                  long, _}]}}} ->
            case AckMode of
                no_ack -> ok;
                _      -> ok = amqp_channel:call(
                                 Chan, #'basic.ack'{delivery_tag = AckTag})
            end
    after ?TIMEOUT -> throw(timeout_waiting_for_deliver1)
    end,

    QueuesAndConsumers = lists:sort([[<<"source-queue">>,<<"1">>,<<"0">>],
                                     [<<"dest-queue">>,<<"1">>,<<"0">>]]),
    ?awaitMatch(QueuesAndConsumers,
                lists:sort(
                  rabbit_ct_broker_helpers:rabbitmqctl_list(
                    Config, 0,
                    ["list_queues", "name", "consumers", "messages", "--no-table-headers"])),
                30000),

    [{test_shovel, static, {running, _Info}, _Metrics, _Time}] =
        rabbit_ct_broker_helpers:rpc(Config, 0,
          rabbit_shovel_status, status, []),
    rabbit_ct_client_helpers:close_channel(Chan).

local_source_anonymous_queue(Config) ->
    DestQ =  <<"dest-queue">>,
    ok = setup_local_server_named_shovel(Config, DestQ, no_ack),
    Chan = rabbit_ct_client_helpers:open_channel(Config, 0),
    CTag = consume(Chan, DestQ, true),
    Msg = #amqp_msg{payload = <<42>>,
                    props = #'P_basic'{delivery_mode = 2,
                                       content_type = ?UNSHOVELLED}},
    % publish to source
    publish(Chan, Msg, <<"amq.fanout">>, <<>>),

    receive
        {#'basic.deliver'{consumer_tag = CTag},
         #amqp_msg{payload = <<42>>,
                   props = #'P_basic'{}}} ->
            ok
    after ?TIMEOUT -> throw(timeout_waiting_for_deliver1)
    end,

    rabbit_ct_client_helpers:close_channel(Chan).

%%
%% Internal
%%
setup_local_source_shovel(Config, SourceQueue, DestQueue, AckMode) ->
    Hostname = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    Shovel = [{test_shovel,
               [{source,
                 [{uris, [rabbit_misc:format("amqp://~ts:~b/%2f?heartbeat=5",
                                             [Hostname, Port])]},
                  {protocol, local},
                  {queue, SourceQueue},
                  {declarations,
                   [{'queue.declare', [{queue, SourceQueue}, auto_delete]}]}
                  ]
                },
                {destination,
                 [{uris, [rabbit_misc:format("amqp://~ts:~b/%2f?heartbeat=5",
                                             [Hostname, Port])]},
                  {declarations,
                   [{'queue.declare', [{queue, DestQueue}, auto_delete]}]},
                  {publish_fields, [{exchange, <<>>},
                                    {routing_key, DestQueue}]},
                  {publish_properties, [{delivery_mode, 2},
                                        {content_type,  ?SHOVELLED}]},
                  {add_forward_headers, true},
                  {add_timestamp_header, true}]},
                {queue, <<>>},
                {ack_mode, AckMode}
               ]}],
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, setup_shovel,
                                      [Shovel]).

setup_local_destination_shovel(Config, Queue, AckMode, Dest) ->
    Hostname = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    Shovel = [{test_shovel,
               [{source,
                 [{uris, [rabbit_misc:format("amqp://~ts:~b/%2f?heartbeat=5",
                                             [Hostname, Port])]},
                  {declarations,
                   [{'queue.declare', [exclusive, auto_delete]},
                    {'exchange.declare', [{exchange, ?EXCHANGE}, auto_delete]},
                    {'queue.bind', [{queue, <<>>}, {exchange, ?EXCHANGE},
                                    {routing_key, ?TO_SHOVEL}]}]},
                  {queue, <<>>}]},
                {destination,
                 [{protocol, local},
                  {declarations,
                   [{'queue.declare', [{queue, Queue}, auto_delete]}]},
                  {uris, [rabbit_misc:format("amqp://~ts:~b",
                                             [Hostname, Port])]},
                  {dest_exchange, <<>>},
                  {dest_routing_key, Queue}] ++ Dest
                },
                {ack_mode, AckMode}]}],
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, setup_shovel,
                                      [Shovel]).

setup_local_server_named_shovel(Config, DestQueue, AckMode) ->
    Hostname = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    Shovel = [{test_shovel,
               [{source,
                 [{uris, [rabbit_misc:format("amqp://~ts:~b/%2f?heartbeat=5",
                                             [Hostname, Port])]},
                  {protocol, local},
                  {queue, <<>>},
                  {declarations,
                   ['queue.declare',
                    {'queue.bind', [
                                    {exchange, <<"amq.fanout">>},
                                    {queue,    <<>>}
                                   ]}]}
                 ]
                },
                {destination,
                 [{protocol, local},
                  {declarations,
                   [{'queue.declare', [{queue, DestQueue}, auto_delete]}]},
                  {uris, [rabbit_misc:format("amqp://~ts:~b",
                                             [Hostname, Port])]},
                  {dest_exchange, <<>>},
                  {dest_routing_key, DestQueue}]},
                {ack_mode, AckMode}
               ]}],
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, setup_shovel,
                                      [Shovel]).

setup_shovel(ShovelConfig) ->
    _ = application:stop(rabbitmq_shovel),
    application:set_env(rabbitmq_shovel, shovels, ShovelConfig, infinity),
    ok = application:start(rabbitmq_shovel),
    await_running_shovel(test_shovel).

await_running_shovel(Name) ->
    case [N || {N, _, {running, _}, _, _}
                      <- rabbit_shovel_status:status(),
                         N =:= Name] of
        [_] -> ok;
        _   -> timer:sleep(100),
               await_running_shovel(Name)
    end.

consume(Chan, Queue, NoAck) ->
    #'basic.consume_ok'{consumer_tag = CTag} =
        amqp_channel:subscribe(Chan, #'basic.consume'{queue = Queue,
                                                      no_ack = NoAck,
                                                      exclusive = false},
                               self()),
    receive
        #'basic.consume_ok'{consumer_tag = CTag} -> ok
    after ?TIMEOUT -> throw(timeout_waiting_for_consume_ok)
    end,
    CTag.

publish(Chan, Msg, Exchange, RoutingKey) ->
    ok = amqp_channel:call(Chan, #'basic.publish'{exchange = Exchange,
                                                  routing_key = RoutingKey},
                           Msg).

delete_queues(Qs) when is_list(Qs) ->
    (catch lists:foreach(fun delete_testcase_queue/1, Qs)).

delete_testcase_queue(Name) ->
    QName = rabbit_misc:r(<<"/">>, queue, Name),
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} ->
            {ok, _} = rabbit_amqqueue:delete(Q, false, false, <<"dummy">>);
        _ ->
            ok
    end.

attach_receiver(Config, TargetQ) ->
    Hostname = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    {ok, Conn} = amqp10_client:open_connection(Hostname, Port),
    {ok, Sess} = amqp10_client:begin_session(Conn),
    {ok, Receiver} = amqp10_client:attach_receiver_link(Sess,
                                                        <<"amqp-destination-receiver">>,
                                                        TargetQ, settled, unsettled_state),
    ok = amqp10_client:flow_link_credit(Receiver, 5, never),
    {Conn, Receiver}.

detach_receiver(Conn, Receiver) ->
    amqp10_client:detach_link(Receiver),
    amqp10_client:close_connection(Conn).
