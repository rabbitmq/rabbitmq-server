%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(system_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include_lib("amqp10_common/include/amqp10_framing.hrl").

-include("src/amqp10_client.hrl").

-compile([export_all, nowarn_export_all]).

suite() ->
    [{timetrap, {minutes, 4}}].

all() ->
    [
     {group, rabbitmq},
     {group, rabbitmq_strict},
     {group, activemq},
     {group, activemq_no_anon},
     {group, mock}
    ].

groups() ->
    [
     {rabbitmq, [], shared()},
     {activemq, [], shared()},
     {rabbitmq_strict, [], [
                            basic_roundtrip_tls,
                            roundtrip_tls_global_config,
                            open_connection_plain_sasl,
                            open_connection_plain_sasl_failure,
                            open_connection_plain_sasl_parse_uri
                           ]},
     {activemq_no_anon, [],
      [
       open_connection_plain_sasl,
       open_connection_plain_sasl_parse_uri,
       open_connection_plain_sasl_failure
      ]},
      {azure, [],
      [
       basic_roundtrip_service_bus,
       filtered_roundtrip_service_bus
      ]},
     {mock, [], [
                 insufficient_credit,
                 incoming_heartbeat,
                 multi_transfer_without_delivery_id
                ]}
    ].

shared() ->
    [
     open_close_connection,
     basic_roundtrip,
     early_transfer,
     split_transfer,
     transfer_unsettled,
     subscribe,
     subscribe_with_auto_flow_settled,
     subscribe_with_auto_flow_unsettled,
     outgoing_heartbeat,
     roundtrip_large_messages,
     transfer_id_vs_delivery_id
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config,
      [
       fun start_amqp10_client_app/1
      ]).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      [
       fun stop_amqp10_client_app/1
      ]).

start_amqp10_client_app(Config) ->
    ?assertMatch({ok, _}, application:ensure_all_started(amqp10_client)),
    Config.

stop_amqp10_client_app(Config) ->
    ok = application:stop(amqp10_client),
    Config.

%% -------------------------------------------------------------------
%% Groups.
%% -------------------------------------------------------------------

init_per_group(rabbitmq, Config0) ->
    Config = rabbit_ct_helpers:set_config(Config0, {sasl, anon}),
    Config1 = rabbit_ct_helpers:merge_app_env(Config,
                                              [{rabbit,
                                                [{max_message_size, 134217728}]}]),
    rabbit_ct_helpers:run_steps(Config1, rabbit_ct_broker_helpers:setup_steps());

init_per_group(rabbitmq_strict, Config0) ->
    Config = rabbit_ct_helpers:set_config(Config0,
                                          {sasl, {plain, <<"guest">>, <<"guest">>}}),
    Config1 = rabbit_ct_helpers:merge_app_env(Config,
                                              [{rabbit,
                                                [{anonymous_login_user, none},
                                                 {max_message_size, 134217728}]}]),
    rabbit_ct_helpers:run_steps(Config1, rabbit_ct_broker_helpers:setup_steps());

init_per_group(activemq, Config0) ->
    Config = rabbit_ct_helpers:set_config(Config0, {sasl, anon}),
    rabbit_ct_helpers:run_steps(Config,
                                activemq_ct_helpers:setup_steps("activemq.xml"));
init_per_group(activemq_no_anon, Config0) ->
    Config = rabbit_ct_helpers:set_config(
               Config0, {sasl, {plain, <<"user">>, <<"password">>}}),
    rabbit_ct_helpers:run_steps(Config,
                                activemq_ct_helpers:setup_steps("activemq_no_anon.xml"));
init_per_group(azure, Config) ->
    rabbit_ct_helpers:set_config(Config,
                                        [
                                         {sb_endpoint, os:getenv("SB_ENDPOINT")},
                                         {sb_keyname, to_bin(os:getenv("SB_KEYNAME"))},
                                         {sb_key, to_bin(os:getenv("SB_KEY"))},
                                         {sb_port, 5671}
                                        ]);
init_per_group(mock, Config) ->
    rabbit_ct_helpers:set_config(Config, [{mock_port, 25000},
                                          {mock_host, "localhost"},
                                          {sasl, none}
                                         ]).
end_per_group(rabbitmq, Config) ->
    rabbit_ct_helpers:run_steps(Config, rabbit_ct_broker_helpers:teardown_steps());
end_per_group(rabbitmq_strict, Config) ->
    rabbit_ct_helpers:run_steps(Config, rabbit_ct_broker_helpers:teardown_steps());
end_per_group(activemq, Config) ->
    rabbit_ct_helpers:run_steps(Config, activemq_ct_helpers:teardown_steps());
end_per_group(activemq_no_anon, Config) ->
    rabbit_ct_helpers:run_steps(Config, activemq_ct_helpers:teardown_steps());
end_per_group(_, Config) ->
    Config.

%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------

init_per_testcase(_Test, Config) ->
    case lists:keyfind(mock_port, 1, Config) of
        {_, Port} ->
            M = mock_server:start(Port),
            rabbit_ct_helpers:set_config(Config, {mock_server, M});
        _ -> Config
    end.

end_per_testcase(_Test, Config) ->
    case lists:keyfind(mock_server, 1, Config) of
        {_, M} -> mock_server:stop(M);
        _ -> Config
    end.

%% -------------------------------------------------------------------

open_close_connection(Config) ->
    Hostname = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    %% an address list
    OpnConf = #{addresses => [Hostname],
                port => Port,
                notify => self(),
                container_id => <<"open_close_connection_container">>,
                sasl => ?config(sasl, Config)},
    {ok, Connection} = amqp10_client:open_connection(Hostname, Port),
    {ok, Connection2} = amqp10_client:open_connection(OpnConf),
    receive
        {amqp10_event, {connection, Connection2, opened}} -> ok
    after 5000 -> exit(connection_timeout)
    end,
    ok = amqp10_client:close_connection(Connection2),
    ok = amqp10_client:close_connection(Connection).

open_connection_plain_sasl(Config) ->
    Hostname = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    %% single address
    OpnConf = #{address => Hostname,
                port => Port,
                notify => self(),
                container_id => <<"open_connection_plain_sasl_container">>,
                sasl =>  ?config(sasl, Config)},
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    receive
        {amqp10_event, {connection, Connection, opened}} -> ok
    after 5000 -> exit(connection_timeout)
    end,
    ok = amqp10_client:close_connection(Connection).

open_connection_plain_sasl_parse_uri(Config) ->
    Hostname = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    Uri = case ?config(sasl, Config) of
              anon ->
                  lists:flatten(
                    io_lib:format("amqp://~ts:~b", [Hostname, Port]));
              {plain, Usr, Pwd} ->
                  lists:flatten(
                    io_lib:format("amqp://~ts:~ts@~ts:~b?sasl=plain",
                                  [Usr, Pwd, Hostname, Port]))
          end,

    {ok, ParsedConf} = amqp10_client:parse_uri(Uri),
    OpnConf = maps:merge(#{notify => self(),
                           container_id => <<"open_connection_plain_sasl_parse_uri_container">>},
                         ParsedConf),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    receive
        {amqp10_event, {connection, Connection, opened}} -> ok
    after 5000 -> exit(connection_timeout)
    end,
    ok = amqp10_client:close_connection(Connection).

open_connection_plain_sasl_failure(Config) ->
    Hostname = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    OpnConf = #{address => Hostname,
                port => Port,
                notify => self(),
                container_id => <<"open_connection_plain_sasl_container">>,
                % anonymous access is not allowed
                sasl => {plain, <<"WORNG">>, <<"WARBLE">>}},
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    receive
        {amqp10_event, {connection, Connection,
                        {closed, sasl_auth_failure}}} -> ok;
        % some implementation may simply close the tcp_connection
        {amqp10_event, {connection, Connection, {closed, shutdown}}} -> ok

    after 5000 ->
              ct:pal("Connection process is alive? = ~tp~n",
                     [erlang:is_process_alive(Connection)]),
              exit(connection_timeout)
    end,
    ok = amqp10_client:close_connection(Connection).

basic_roundtrip(Config) ->
    application:start(sasl),
    Hostname = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    OpenConf = #{address => Hostname, port => Port, sasl => anon},
    roundtrip(OpenConf).

basic_roundtrip_tls(Config) ->
    Hostname = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp_tls),
    CACertFile = ?config(rmq_certsdir, Config) ++ "/testca/cacert.pem",
    CertFile = ?config(rmq_certsdir, Config) ++ "/client/cert.pem",
    KeyFile = ?config(rmq_certsdir, Config) ++ "/client/key.pem",
    OpnConf = #{address => Hostname,
                port => Port,
                tls_opts => {secure_port, [{cacertfile, CACertFile},
                                           {certfile, CertFile},
                                           {keyfile, KeyFile}]},
                notify => self(),
                container_id => <<"open_connection_tls_container">>,
                sasl => ?config(sasl, Config)},
    roundtrip(OpnConf).

%% ssl option validation fails if verify_peer is enabled without cacerts.
%% Test that cacertfile option takes effect taken from the application env.
roundtrip_tls_global_config(Config) ->
    Hostname = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp_tls),
    CACertFile = ?config(rmq_certsdir, Config) ++ "/testca/cacert.pem",
    CertFile = ?config(rmq_certsdir, Config) ++ "/client/cert.pem",
    KeyFile = ?config(rmq_certsdir, Config) ++ "/client/key.pem",
    ok = application:set_env(amqp10_client, ssl_options, [{cacertfile, CACertFile},
                                                          {certfile, CertFile},
                                                          {keyfile, KeyFile}]),
    OpnConf = #{address => Hostname,
                port => Port,
                tls_opts => {secure_port, [{verify, verify_peer}]},
                notify => self(),
                container_id => <<"open_connection_tls_container">>,
                sasl => ?config(sasl, Config)},
    roundtrip(OpnConf),
    application:unset_env(amqp10_client, ssl_options).

service_bus_config(Config, ContainerId) ->
    Hostname = ?config(sb_endpoint, Config),
    Port = ?config(sb_port, Config),
    User = ?config(sb_keyname, Config),
    Password = ?config(sb_key, Config),
    #{address => Hostname,
      hostname => to_bin(Hostname),
      port => Port,
      tls_opts => {secure_port, [{versions, ['tlsv1.1']}]},
      notify => self(),
      container_id => ContainerId,
      sasl => {plain, User, Password}}.

basic_roundtrip_service_bus(Config) ->
    roundtrip(service_bus_config(Config, <<"basic_roundtrip_service_bus">>)).

filtered_roundtrip_service_bus(Config) ->
    filtered_roundtrip(service_bus_config(Config, <<"filtered_roundtrip_service_bus">>)).

roundtrip_large_messages(Config) ->
    Hostname = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    OpenConf = #{address => Hostname, port => Port, sasl => anon},

    DataKb = rand:bytes(1024),
    DataMb = rand:bytes(1024 * 1024),
    Data8Mb = rand:bytes(8 * 1024 * 1024),
    Data64Mb = rand:bytes(64 * 1024 * 1024),
    ok = roundtrip(OpenConf, DataKb),
    ok = roundtrip(OpenConf, DataMb),
    ok = roundtrip(OpenConf, Data8Mb),
    ok = roundtrip(OpenConf, Data64Mb).

roundtrip(OpenConf) ->
    roundtrip(OpenConf, <<"banana">>).

roundtrip(OpenConf, Body) ->
    {ok, Connection} = amqp10_client:open_connection(OpenConf),
    {ok, Session} = amqp10_client:begin_session(Connection),
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"banana-sender">>, <<"test1">>, settled, unsettled_state),
    await_link(Sender, credited, link_credit_timeout),

    Now = os:system_time(millisecond),
    Props = #{creation_time => Now,
              message_id => <<"my message ID">>,
              correlation_id => <<"my correlation ID">>,
              content_type => <<"my content type">>,
              content_encoding => <<"my content encoding">>,
              group_id => <<"my group ID">>},
    Msg0 = amqp10_msg:new(<<"my-tag">>, Body, true),
    Msg1 = amqp10_msg:set_application_properties(#{"a_key" => "a_value"}, Msg0),
    Msg2 = amqp10_msg:set_properties(Props, Msg1),
    Msg = amqp10_msg:set_message_annotations(#{<<"x-key 1">> => "value 1",
                                               <<"x-key 2">> => "value 2"}, Msg2),
    ok = amqp10_client:send_msg(Sender, Msg),
    ok = amqp10_client:detach_link(Sender),
    await_link(Sender, {detached, normal}, link_detach_timeout),

    {error, link_not_found} = amqp10_client:detach_link(Sender),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"banana-receiver">>, <<"test1">>, settled, unsettled_state),
    {ok, OutMsg} = amqp10_client:get_msg(Receiver, 4 * 60_000),
    ok = amqp10_client:end_session(Session),
    ok = amqp10_client:close_connection(Connection),

    % ct:pal(?LOW_IMPORTANCE, "roundtrip message Out: ~tp~nIn: ~tp~n", [OutMsg, Msg]),
    ?assertMatch(Props, amqp10_msg:properties(OutMsg)),
    ?assertEqual(#{<<"a_key">> => <<"a_value">>}, amqp10_msg:application_properties(OutMsg)),
    ?assertMatch(#{<<"x-key 1">> := <<"value 1">>,
                   <<"x-key 2">> := <<"value 2">>}, amqp10_msg:message_annotations(OutMsg)),
    ?assertEqual([Body], amqp10_msg:body(OutMsg)),
    ok.

filtered_roundtrip(OpenConf) ->
    filtered_roundtrip(OpenConf, <<"banana">>).

filtered_roundtrip(OpenConf, Body) ->
    {ok, Connection} = amqp10_client:open_connection(OpenConf),
    {ok, Session} = amqp10_client:begin_session(Connection),
    {ok, Sender} = amqp10_client:attach_sender_link(Session,
                                                    <<"default-sender">>,
                                                    <<"test1">>,
                                                    settled,
                                                    unsettled_state),
    await_link(Sender, credited, link_credit_timeout),

    Now = os:system_time(millisecond),
    Props1 = #{creation_time => Now},
    Msg1 = amqp10_msg:set_properties(Props1, amqp10_msg:new(<<"msg-1-tag">>, Body, true)),
    ok = amqp10_client:send_msg(Sender, Msg1),

    {ok, DefaultReceiver} = amqp10_client:attach_receiver_link(Session,
                                                        <<"default-receiver">>,
                                                        <<"test1">>,
                                                        settled,
                                                        unsettled_state),
    ok = amqp10_client:send_msg(Sender, Msg1),
    {ok, OutMsg1} = amqp10_client:get_msg(DefaultReceiver, 60_000 * 4),
    ?assertEqual(<<"msg-1-tag">>, amqp10_msg:delivery_tag(OutMsg1)),

    timer:sleep(5 * 1000),

    Now2 = os:system_time(millisecond),
    Props2 = #{creation_time => Now2 - 1000},
    Msg2 = amqp10_msg:set_properties(Props2, amqp10_msg:new(<<"msg-2-tag">>, Body, true)),
    Now2Binary = integer_to_binary(Now2),

    ok = amqp10_client:send_msg(Sender, Msg2),

    {ok, FilteredReceiver} = amqp10_client:attach_receiver_link(Session,
                                                        <<"filtered-receiver">>,
                                                        <<"test1">>,
                                                        settled,
                                                        unsettled_state,
                                                        #{<<"apache.org:selector-filter:string">> => <<"amqp.annotation.x-opt-enqueuedtimeutc > ", Now2Binary/binary>>}),

    {ok, OutMsg2} = amqp10_client:get_msg(DefaultReceiver, 60_000 * 4),
    ?assertEqual(<<"msg-2-tag">>, amqp10_msg:delivery_tag(OutMsg2)),

    {ok, OutMsgFiltered} = amqp10_client:get_msg(FilteredReceiver, 60_000 * 4),
    ?assertEqual(<<"msg-2-tag">>, amqp10_msg:delivery_tag(OutMsgFiltered)),

    ok = amqp10_client:end_session(Session),
    ok = amqp10_client:close_connection(Connection),
    ok.

%% Assert that implementations respect the difference between transfer-id and delivery-id.
transfer_id_vs_delivery_id(Config) ->
    Hostname = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    OpenConf = #{address => Hostname, port => Port, sasl => anon},

    {ok, Connection} = amqp10_client:open_connection(OpenConf),
    {ok, Session} = amqp10_client:begin_session(Connection),
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"banana-sender">>, <<"test1">>, settled, unsettled_state),
    await_link(Sender, credited, link_credit_timeout),

    P0 = binary:copy(<<0>>, 8_000_000),
    P1 = <<P0/binary, 1>>,
    P2 = <<P0/binary, 2>>,
    Msg1 = amqp10_msg:new(<<"tag 1">>, P1, true),
    Msg2 = amqp10_msg:new(<<"tag 2">>, P2, true),
    ok = amqp10_client:send_msg(Sender, Msg1),
    ok = amqp10_client:send_msg(Sender, Msg2),
    ok = amqp10_client:detach_link(Sender),
    await_link(Sender, {detached, normal}, link_detach_timeout),

    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"banana-receiver">>, <<"test1">>, settled, unsettled_state),
    {ok, RcvMsg1} = amqp10_client:get_msg(Receiver, 60_000 * 4),
    {ok, RcvMsg2} = amqp10_client:get_msg(Receiver, 60_000 * 4),
    ok = amqp10_client:end_session(Session),
    ok = amqp10_client:close_connection(Connection),

    ?assertEqual([P1], amqp10_msg:body(RcvMsg1)),
    ?assertEqual([P2], amqp10_msg:body(RcvMsg2)),
    %% Despite many transfers, there were only 2 deliveries.
    %% Therefore, delivery-id should have been increased by just 1.
    ?assertEqual(serial_number:add(amqp10_msg:delivery_id(RcvMsg1), 1),
                 amqp10_msg:delivery_id(RcvMsg2)).

% a message is sent before the link attach is guaranteed to
% have completed and link credit granted
% also queue a link detached immediately after transfer
early_transfer(Config) ->
    Hostname = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    {ok, Connection} = amqp10_client:open_connection(Hostname, Port),
    {ok, Session} = amqp10_client:begin_session(Connection),
    {ok, Sender} = amqp10_client:attach_sender_link(Session,
                                                    <<"early-transfer">>,
                                                    <<"test">>),

    Msg = amqp10_msg:new(<<"my-tag">>, <<"banana">>, true),
    % TODO: this is a timing issue - should use mock here really
    {error, half_attached} = amqp10_client:send_msg(Sender, Msg),
    % wait for credit
    await_link(Sender, credited, credited_timeout),
    ok = amqp10_client:detach_link(Sender),
    % attach then immediately detach
    LinkName = <<"early-transfer2">>,
    {ok, Sender2} = amqp10_client:attach_sender_link(Session, LinkName,
                                                    <<"test">>),
    {error, half_attached} = amqp10_client:detach_link(Sender2),
    await_link(Sender2, credited, credited_timeout),
    ok = amqp10_client:end_session(Session),
    ok = amqp10_client:close_connection(Connection),
    ok.

split_transfer(Config) ->
    Hostname = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    Conf = #{address => Hostname,
             port => Port,
             max_frame_size => 512,
             sasl => ?config(sasl, Config)},
    {ok, Connection} = amqp10_client:open_connection(Conf),
    {ok, Session} = amqp10_client:begin_session(Connection),
    Data = list_to_binary(string:chars(64, 1000)),
    {ok, Sender} = amqp10_client:attach_sender_link_sync(Session,
                                                         <<"data-sender">>,
                                                         <<"test">>),
    Msg = amqp10_msg:new(<<"my-tag">>, Data, true),
    ok = amqp10_client:send_msg(Sender, Msg),
    {ok, Receiver} = amqp10_client:attach_receiver_link(Session,
                                                        <<"data-receiver">>,
                                                        <<"test">>),
    {ok, OutMsg} = amqp10_client:get_msg(Receiver),
    ok = amqp10_client:end_session(Session),
    ok = amqp10_client:close_connection(Connection),
    ?assertEqual([Data], amqp10_msg:body(OutMsg)).

transfer_unsettled(Config) ->
    Hostname = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    Conf = #{address => Hostname, port => Port,
             sasl => ?config(sasl, Config)},
    {ok, Connection} = amqp10_client:open_connection(Conf),
    {ok, Session} = amqp10_client:begin_session(Connection),
    Data = list_to_binary(string:chars(64, 1000)),
    {ok, Sender} = amqp10_client:attach_sender_link_sync(Session,
                                                         <<"data-sender">>,
                                                         <<"test">>, unsettled),
    await_link(Sender, credited, credited_timeout),
    DeliveryTag = <<"my-tag">>,
    Msg = amqp10_msg:new(DeliveryTag, Data, false),
    ok = amqp10_client:send_msg(Sender, Msg),
    ct:pal("test pid ~w", [self()]),
    ok = await_disposition(DeliveryTag),
    {ok, Receiver} = amqp10_client:attach_receiver_link(Session,
                                                        <<"data-receiver">>,
                                                        <<"test">>, unsettled),
    {ok, OutMsg} = amqp10_client:get_msg(Receiver),
    ok = amqp10_client:accept_msg(Receiver, OutMsg),
    {error, timeout} = amqp10_client:get_msg(Receiver, 1000),
    ok = amqp10_client:end_session(Session),
    ok = amqp10_client:close_connection(Connection),
    ?assertEqual([Data], amqp10_msg:body(OutMsg)).

subscribe(Config) ->
    Hostname = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    QueueName = atom_to_binary(?FUNCTION_NAME),
    {ok, Connection} = amqp10_client:open_connection(Hostname, Port),
    {ok, Session} = amqp10_client:begin_session(Connection),
    {ok, Sender} = amqp10_client:attach_sender_link_sync(Session,
                                                         <<"sub-sender">>,
                                                         QueueName),
    await_link(Sender, credited, link_credit_timeout),
    _ = publish_messages(Sender, <<"banana">>, 10),
    {ok, Receiver} = amqp10_client:attach_receiver_link(Session,
                                                        <<"sub-receiver">>,
                                                        QueueName, unsettled),
    ok = amqp10_client:flow_link_credit(Receiver, 10, never),
    [begin
         receive {amqp10_msg, Receiver, Msg} ->
                     ok = amqp10_client:accept_msg(Receiver, Msg)
         after 2000 -> ct:fail(timeout)
         end
     end || _ <- lists:seq(1, 10)],
    ok = assert_no_message(Receiver),

    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 5000 -> flush(),
                  exit(credit_exhausted_assert)
    end,

    ok = amqp10_client:end_session(Session),
    ok = amqp10_client:close_connection(Connection).

subscribe_with_auto_flow_settled(Config) ->
    SenderSettleMode = settled,
    Hostname = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    QueueName = atom_to_binary(?FUNCTION_NAME),
    {ok, Connection} = amqp10_client:open_connection(Hostname, Port),
    {ok, Session} = amqp10_client:begin_session(Connection),
    {ok, Sender} = amqp10_client:attach_sender_link_sync(Session,
                                                         <<"sub-sender">>,
                                                         QueueName),
    await_link(Sender, credited, link_credit_timeout),

    publish_messages(Sender, <<"banana">>, 20),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"sub-receiver">>, QueueName, SenderSettleMode),
    await_link(Receiver, attached, attached_timeout),

    ok = amqp10_client:flow_link_credit(Receiver, 5, 2),
    ?assertEqual(20, count_received_messages(Receiver)),

    ok = amqp10_client:detach_link(Receiver),
    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:end_session(Session),
    ok = amqp10_client:close_connection(Connection).

subscribe_with_auto_flow_unsettled(Config) ->
    SenderSettleMode = unsettled,
    Hostname = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    QueueName = atom_to_binary(?FUNCTION_NAME),
    {ok, Connection} = amqp10_client:open_connection(Hostname, Port),
    {ok, Session} = amqp10_client:begin_session(Connection),
    {ok, Sender} = amqp10_client:attach_sender_link_sync(Session,
                                                         <<"sub-sender">>,
                                                         QueueName),
    await_link(Sender, credited, link_credit_timeout),

    _ = publish_messages(Sender, <<"1-">>, 30),
    %% Use sender settle mode 'unsettled'.
    %% This should require us to manually settle message in order to receive more messages.
    {ok, Receiver} = amqp10_client:attach_receiver_link(Session, <<"sub-receiver-2">>, QueueName, SenderSettleMode),
    await_link(Receiver, attached, attached_timeout),
    ok = amqp10_client:flow_link_credit(Receiver, 5, 2),
    %% We should receive exactly 5 messages.
    [M1, _M2, M3, M4, M5] = receive_messages(Receiver, 5),
    ok = assert_no_message(Receiver),

    %% Even when we accept the first 3 messages, the number of unsettled messages has not yet fallen below 2.
    %% Therefore, the client should not yet grant more credits to the sender.
    ok = amqp10_client_session:disposition(
           Receiver, amqp10_msg:delivery_id(M1), amqp10_msg:delivery_id(M3), true, accepted),
    ok = assert_no_message(Receiver),

    %% When we accept 1 more message (the order in which we accept shouldn't matter, here we accept M5 before M4),
    %% the number of unsettled messages now falls below 2 (since only M4 is left unsettled).
    %% Therefore, the client should grant 5 credits to the sender.
    %% Therefore, we should receive 5 more messages.
    ok = amqp10_client:accept_msg(Receiver, M5),
    [_M6, _M7, _M8, _M9, M10] = receive_messages(Receiver, 5),
    ok = assert_no_message(Receiver),

    %% It shouldn't matter how we settle messages, therefore we use 'rejected' this time.
    %% Settling all in flight messages should cause us to receive exactly 5 more messages.
    ok = amqp10_client_session:disposition(
           Receiver, amqp10_msg:delivery_id(M4), amqp10_msg:delivery_id(M10), true, rejected),
    [M11, _M12, _M13, _M14, M15] = receive_messages(Receiver, 5),
    ok = assert_no_message(Receiver),

    %% Dynamically decrease link credit.
    %% Since we explicitly tell to grant 3 new credits now, we expect to receive 3 more messages.
    ok = amqp10_client:flow_link_credit(Receiver, 3, 3),
    [M16, _M17, M18] = receive_messages(Receiver, 3),
    ok = assert_no_message(Receiver),

    ok = amqp10_client_session:disposition(
           Receiver, amqp10_msg:delivery_id(M11), amqp10_msg:delivery_id(M15), true, accepted),
    %% However, the RenewWhenBelow=3 still refers to all unsettled messages.
    %% Right now we have 3 messages (M16, M17, M18) unsettled.
    ok = assert_no_message(Receiver),

    %% Settling 1 out of these 3 messages causes RenewWhenBelow to fall below 3 resulting
    %% in 3 new messages to be received.
    ok = amqp10_client:accept_msg(Receiver, M18),
    [_M19, _M20, _M21] = receive_messages(Receiver, 3),
    ok = assert_no_message(Receiver),

    ok = amqp10_client:flow_link_credit(Receiver, 3, never, true),
    [_M22, _M23, M24] = receive_messages(Receiver, 3),
    ok = assert_no_message(Receiver),

    %% Since RenewWhenBelow = never, we expect to receive no new messages despite settling.
    ok = amqp10_client_session:disposition(
           Receiver, amqp10_msg:delivery_id(M16), amqp10_msg:delivery_id(M24), true, rejected),
    ok = assert_no_message(Receiver),

    ok = amqp10_client:flow_link_credit(Receiver, 2, never, false),
    [M25, _M26] = receive_messages(Receiver, 2),
    ok = assert_no_message(Receiver),

    ok = amqp10_client:flow_link_credit(Receiver, 3, 3),
    [_M27, _M28, M29] = receive_messages(Receiver, 3),
    ok = assert_no_message(Receiver),

    ok = amqp10_client_session:disposition(
           Receiver, amqp10_msg:delivery_id(M25), amqp10_msg:delivery_id(M29), true, accepted),
    [M30] = receive_messages(Receiver, 1),
    ok = assert_no_message(Receiver),
    ok = amqp10_client:accept_msg(Receiver, M30),
    %% The sender queue is empty now.
    ok = assert_no_message(Receiver),

    ok = amqp10_client:flow_link_credit(Receiver, 3, 1),
    _ = publish_messages(Sender, <<"2-">>, 1),
    [M31] = receive_messages(Receiver, 1),
    ok = amqp10_client:accept_msg(Receiver, M31),

    %% Since function flow_link_credit/3 documents
    %%     "if RenewWhenBelow is an integer, the amqp10_client will automatically grant more
    %%     Credit to the sender when the sum of the remaining link credit and the number of
    %%     unsettled messages falls below the value of RenewWhenBelow."
    %% our expectation is that the amqp10_client has not renewed credit since the sum of
    %% remaining link credit (2) and unsettled messages (0) is 2.
    %%
    %% Therefore, when we publish another 3 messages, we expect to only receive only 2 messages!
    _ = publish_messages(Sender, <<"3-">>, 5),
    [M32, M33] = receive_messages(Receiver, 2),
    ok = assert_no_message(Receiver),

    %% When we accept both messages, the sum of the remaining link credit (0) and unsettled messages (0)
    %% falls below RenewWhenBelow=1 causing the amqp10_client to grant 3 new credits.
    ok = amqp10_client:accept_msg(Receiver, M32),
    ok = assert_no_message(Receiver),
    ok = amqp10_client:accept_msg(Receiver, M33),

    [M35, M36, M37] = receive_messages(Receiver, 3),
    ok = amqp10_client:accept_msg(Receiver, M35),
    ok = amqp10_client:accept_msg(Receiver, M36),
    ok = amqp10_client:accept_msg(Receiver, M37),
    %% The sender queue is empty now.
    ok = assert_no_message(Receiver),

    ok = amqp10_client:detach_link(Receiver),
    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:end_session(Session),
    ok = amqp10_client:close_connection(Connection).

insufficient_credit(Config) ->
    Hostname = ?config(mock_host, Config),
    Port = ?config(mock_port, Config),
    OpenStep = fun({0 = Ch, #'v1_0.open'{}, _Pay}) ->
                       {Ch, [#'v1_0.open'{container_id = {utf8, <<"mock">>}}]}
               end,
    BeginStep = fun({0 = Ch, #'v1_0.begin'{}, _Pay}) ->
                         {Ch, [#'v1_0.begin'{remote_channel = {ushort, Ch},
                                             next_outgoing_id = {uint, 1},
                                             incoming_window = {uint, 1000},
                                             outgoing_window = {uint, 1000}}
                                             ]}
                end,
    AttachStep = fun({0 = Ch, #'v1_0.attach'{role = false,
                                             name = Name}, <<>>}) ->
                         {Ch, [#'v1_0.attach'{name = Name,
                                              handle = {uint, 99},
                                              role = true}]}
                 end,
    Steps = [fun mock_server:recv_amqp_header_step/1,
             fun mock_server:send_amqp_header_step/1,
             mock_server:amqp_step(OpenStep),
             mock_server:amqp_step(BeginStep),
             mock_server:amqp_step(AttachStep)],

    ok = mock_server:set_steps(?config(mock_server, Config), Steps),

    Cfg = #{address => Hostname, port => Port, sasl => none, notify => self()},
    {ok, Connection} = amqp10_client:open_connection(Cfg),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"mock1-sender">>,
                                                    <<"test">>),
    await_link(Sender, attached, attached_timeout),
    Msg = amqp10_msg:new(<<"mock-tag">>, <<"banana">>, true),
    {error, insufficient_credit} = amqp10_client:send_msg(Sender, Msg),

    ok = amqp10_client:end_session(Session),
    ok = amqp10_client:close_connection(Connection),
    ok.

multi_transfer_without_delivery_id(Config) ->
    Hostname = ?config(mock_host, Config),
    Port = ?config(mock_port, Config),
    OpenStep = fun({0 = Ch, #'v1_0.open'{}, _Pay}) ->
                       {Ch, [#'v1_0.open'{container_id = {utf8, <<"mock">>}}]}
               end,
    BeginStep = fun({0 = Ch, #'v1_0.begin'{}, _Pay}) ->
                         {Ch, [#'v1_0.begin'{remote_channel = {ushort, Ch},
                                             next_outgoing_id = {uint, 1},
                                             incoming_window = {uint, 1000},
                                             outgoing_window = {uint, 1000}}
                                             ]}
                end,
    AttachStep = fun({0 = Ch, #'v1_0.attach'{role = true,
                                             name = Name}, <<>>}) ->
                         {Ch, [#'v1_0.attach'{name = Name,
                                              handle = {uint, 99},
                                              initial_delivery_count = {uint, 1},
                                              role = false}
                              ]}
                 end,

    LinkCreditStep = fun({0 = Ch, #'v1_0.flow'{}, <<>>}) ->
                             {Ch, {multi, [[#'v1_0.transfer'{handle = {uint, 99},
                                                             delivery_id = {uint, 12},
                                                             more = true},
                                            #'v1_0.data'{content = <<"hello ">>}],
                                           [#'v1_0.transfer'{handle = {uint, 99},
                                                             % delivery_id can be omitted
                                                             % for continuation frames
                                                             delivery_id = undefined,
                                                             settled = undefined,
                                                             more = false},
                                            #'v1_0.data'{content = <<"world">>}]
                                          ]}}
                     end,
    Steps = [fun mock_server:recv_amqp_header_step/1,
             fun mock_server:send_amqp_header_step/1,
             mock_server:amqp_step(OpenStep),
             mock_server:amqp_step(BeginStep),
             mock_server:amqp_step(AttachStep),
             mock_server:amqp_step(LinkCreditStep)
            ],

    ok = mock_server:set_steps(?config(mock_server, Config), Steps),

    Cfg = #{address => Hostname, port => Port, sasl => none, notify => self()},
    {ok, Connection} = amqp10_client:open_connection(Cfg),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, Receiver} = amqp10_client:attach_receiver_link(Session, <<"mock1-received">>,
                                                    <<"test">>),
    amqp10_client:flow_link_credit(Receiver, 100, 50),
    receive
        {amqp10_msg, Receiver, _InMsg} ->
            ok
    after 2000 ->
              exit(delivery_timeout)
    end,

    ok = amqp10_client:end_session(Session),
    ok = amqp10_client:close_connection(Connection),
    ok.

outgoing_heartbeat(Config) ->
    Hostname = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    CConf = #{address => Hostname, port => Port,
              idle_time_out => 5000, sasl => ?config(sasl, Config)},
    {ok, Connection} = amqp10_client:open_connection(CConf),
    timer:sleep(35 * 1000), % activemq defaults to 15s I believe
    % check we can still establish a session
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    ok = amqp10_client:end_session(Session),
    ok = amqp10_client:close_connection(Connection).

incoming_heartbeat(Config) ->
    Hostname = ?config(mock_host, Config),
    Port = ?config(mock_port, Config),
    OpenStep = fun({0 = Ch, #'v1_0.open'{}, _Pay}) ->
                       {Ch, [#'v1_0.open'{container_id = {utf8, <<"mock">>},
                                          idle_time_out = {uint, 0}}]}
               end,

    CloseStep = fun({0 = Ch, #'v1_0.close'{error = _TODO}, _Pay}) ->
                         {Ch, [#'v1_0.close'{}]}
                end,
    Steps = [fun mock_server:recv_amqp_header_step/1,
             fun mock_server:send_amqp_header_step/1,
             mock_server:amqp_step(OpenStep),
             mock_server:amqp_step(CloseStep)],
    Mock = {_, MockPid} = ?config(mock_server, Config),
    MockRef = monitor(process, MockPid),
    ok = mock_server:set_steps(Mock, Steps),
    CConf = #{address => Hostname, port => Port, sasl => ?config(sasl, Config),
              idle_time_out => 1000, notify => self()},
    {ok, Connection} = amqp10_client:open_connection(CConf),
    receive
        {amqp10_event,
         {connection, Connection0,
          {closed, {resource_limit_exceeded, <<"remote idle-time-out">>}}}}
          when Connection0 =:= Connection ->
            ok
    after 5000 ->
              exit(incoming_heartbeat_assert)
    end,
    demonitor(MockRef).


%%% HELPERS
%%%

await_link(Who, What, Err) ->
    receive
        {amqp10_event, {link, Who0, What0}}
          when Who0 =:= Who andalso
               What0 =:= What ->
            ok;
        {amqp10_event, {link, Who0, {detached, Why}}}
          when Who0 =:= Who ->
            ct:fail(Why)
    after 5000 ->
              flush(),
              ct:fail(Err)
    end.

publish_messages(Sender, BodyPrefix, Num) ->
    [begin
         Tag = integer_to_binary(T),
         Msg = amqp10_msg:new(Tag, <<BodyPrefix/binary, Tag/binary>>, false),
         ok = amqp10_client:send_msg(Sender, Msg),
         ok = await_disposition(Tag)
     end || T <- lists:seq(1, Num)].

await_disposition(DeliveryTag) ->
    receive
        {amqp10_disposition, {accepted, DeliveryTag0}}
          when DeliveryTag0 =:= DeliveryTag -> ok
    after 3000 ->
              flush(),
              ct:fail(dispostion_timeout)
    end.

count_received_messages(Receiver) ->
    count_received_messages0(Receiver, 0).

count_received_messages0(Receiver, Count) ->
    receive
        {amqp10_msg, Receiver, _Msg} ->
            count_received_messages0(Receiver, Count + 1)
    after 500 ->
              Count
    end.

receive_messages(Receiver, N) ->
    receive_messages0(Receiver, N, []).

receive_messages0(_Receiver, 0, Acc) ->
    lists:reverse(Acc);
receive_messages0(Receiver, N, Acc) ->
    receive
        {amqp10_msg, Receiver, Msg} ->
            receive_messages0(Receiver, N - 1, [Msg | Acc])
    after 5000  ->
              LastReceivedMsg = case Acc of
                                    [] -> none;
                                    [M | _] -> M
                                end,
              ct:fail({timeout,
                       {num_received, length(Acc)},
                       {num_missing, N},
                       {last_received_msg, LastReceivedMsg}
                      })
    end.

assert_no_message(Receiver) ->
    receive {amqp10_msg, Receiver, Msg} -> ct:fail({unexpected_message, Msg})
    after 50 -> ok
    end.

to_bin(X) when is_list(X) ->
    list_to_binary(X).

flush() ->
    receive
        Any ->
            ct:pal("flush ~tp", [Any]),
            flush()
    after 0 ->
              ok
    end.
