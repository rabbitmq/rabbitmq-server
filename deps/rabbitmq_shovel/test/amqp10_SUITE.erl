%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(amqp10_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

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
          amqp10_destination_no_ack,
          amqp10_destination_on_publish,
          amqp10_destination_on_confirm,
          amqp10_source_no_ack,
          amqp10_source_on_publish,
          amqp10_source_on_confirm
        ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(amqp10_client),
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE}
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
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

stop_shovel_plugin(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
                                      application, stop, [rabbitmq_shovel]),
    Config.

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

amqp10_destination_no_ack(Config) ->
    amqp10_destination(Config, no_ack).

amqp10_destination_on_publish(Config) ->
    amqp10_destination(Config, on_publish).

amqp10_destination_on_confirm(Config) ->
    amqp10_destination(Config, on_confirm).

amqp10_destination(Config, AckMode) ->
    TargetQ =  <<"a-queue">>,
    ok = setup_amqp10_destination_shovel(Config, TargetQ, AckMode),
    Hostname = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    {ok, Conn} = amqp10_client:open_connection(Hostname, Port),
    {ok, Sess} = amqp10_client:begin_session(Conn),
    {ok, Receiver} = amqp10_client:attach_receiver_link(Sess,
                                                        <<"amqp-destination-receiver">>,
                                                        TargetQ, settled, unsettled_state),
    ok = amqp10_client:flow_link_credit(Receiver, 5, never),
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
            [<<42>>] = amqp10_msg:body(InMsg),
            #{content_type := ?UNSHOVELLED,
              content_encoding := ?UNSHOVELLED,
              correlation_id := ?UNSHOVELLED,
              user_id := <<"guest">>,
              message_id := ?UNSHOVELLED,
              reply_to := ?UNSHOVELLED
              %% timestamp gets overwritten
              % creation_time := Timestamp
             } = amqp10_msg:properties(InMsg),
            #{<<"routing_key">> := ?TO_SHOVEL,
              <<"type">> := ?UNSHOVELLED,
              <<"header1">> := 1,
              <<"header2">> := <<"h2">>
             } = amqp10_msg:application_properties(InMsg),
            #{durable := true} = amqp10_msg:headers(InMsg),
            ok
    after ?TIMEOUT ->
              throw(timeout_waiting_for_deliver1)
    end,

    [{test_shovel, static, {running, _Info}, _Time}] =
        rabbit_ct_broker_helpers:rpc(Config, 0,
          rabbit_shovel_status, status, []),
    amqp10_client:detach_link(Receiver),
    amqp10_client:close_connection(Conn),
    rabbit_ct_client_helpers:close_channel(Chan).

amqp10_source_no_ack(Config) ->
    amqp10_source(Config, no_ack).

amqp10_source_on_publish(Config) ->
    amqp10_source(Config, on_publish).

amqp10_source_on_confirm(Config) ->
    amqp10_source(Config, on_confirm).

amqp10_source(Config, AckMode) ->
    SourceQ =  <<"source-queue">>,
    DestQ =  <<"dest-queue">>,
    ok = setup_amqp10_source_shovel(Config, SourceQ, DestQ, AckMode),
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
                   props = #'P_basic'{%delivery_mode = 2,
                                      %content_type = ?SHOVELLED,
                                      headers = [{<<"x-shovelled">>, _, _},
                                                 {<<"x-shovelled-timestamp">>,
                                                  long, _}]}}} ->
            case AckMode of
                no_ack -> ok;
                _      -> ok = amqp_channel:call(
                                 Chan, #'basic.ack'{delivery_tag = AckTag})
            end
    after ?TIMEOUT -> throw(timeout_waiting_for_deliver1)
    end,

    [{test_shovel, static, {running, _Info}, _Time}] =
        rabbit_ct_broker_helpers:rpc(Config, 0,
          rabbit_shovel_status, status, []),
    rabbit_ct_client_helpers:close_channel(Chan).

setup_amqp10_source_shovel(Config, SourceQueue, DestQueue, AckMode) ->
    Hostname = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    Shovel = [{test_shovel,
               [{source,
                 [{protocol, amqp10},
                  {uris, [rabbit_misc:format("amqp://~s:~b",
                                             [Hostname, Port])]},
                  {source_address, SourceQueue}]
                },
                {destination,
                 [{uris, [rabbit_misc:format("amqp://~s:~b/%2f?heartbeat=5",
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

setup_amqp10_destination_shovel(Config, Queue, AckMode) ->
    Hostname = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    Shovel = [{test_shovel,
               [{source,
                 [{uris, [rabbit_misc:format("amqp://~s:~b/%2f?heartbeat=5",
                                             [Hostname, Port])]},
                  {declarations,
                   [{'queue.declare', [exclusive, auto_delete]},
                    {'exchange.declare', [{exchange, ?EXCHANGE}, auto_delete]},
                    {'queue.bind', [{queue, <<>>}, {exchange, ?EXCHANGE},
                                    {routing_key, ?TO_SHOVEL}]}]},
                  {queue, <<>>}]},
                {destination,
                 [{protocol, amqp10},
                  {uris, [rabbit_misc:format("amqp://~s:~b",
                                             [Hostname, Port])]},
                  {add_forward_headers, true},
                  {add_timestamp_header, true},
                  {target_address, Queue}]
                },
                {ack_mode, AckMode}]}],
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, setup_shovel,
                                      [Shovel]).
setup_amqp10_shovel(Config, SourceQueue, DestQueue, AckMode) ->
    Hostname = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    Shovel = [{test_shovel,
               [{source,
                 [{protocol, amqp10},
                  {uris, [rabbit_misc:format("amqp://~s:~b",
                                             [Hostname, Port])]},
                  {source_address, SourceQueue}]},
                {destination,
                 [{protocol, amqp10},
                  {uris, [rabbit_misc:format("amqp://~s:~b",
                                             [Hostname, Port])]},
                  {add_forward_headers, true},
                  {add_timestamp_header, true},
                  {target_address, DestQueue}]
                },
                {ack_mode, AckMode}]}],
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, setup_shovel,
                                      [Shovel]).

setup_shovel(ShovelConfig) ->
    _ = application:stop(rabbitmq_shovel),
    application:set_env(rabbitmq_shovel, shovels, ShovelConfig, infinity),
    ok = application:start(rabbitmq_shovel),
    await_running_shovel(test_shovel).

await_running_shovel(Name) ->
    case [N || {N, _, {running, _}, _}
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
