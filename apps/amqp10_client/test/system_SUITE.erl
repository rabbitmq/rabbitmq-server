%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(system_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include_lib("amqp10_common/include/amqp10_framing.hrl").

-include_lib("src/amqp10_client.hrl").

-compile(export_all).

-define(UNAUTHORIZED_USER, <<"test_user_no_perm">>).

%% The latch constant defines how many processes are spawned in order
%% to run certain functionality in parallel. It follows the standard
%% countdown latch pattern.
-define(LATCH, 100).

%% The wait constant defines how long a consumer waits before it
%% unsubscribes
-define(WAIT, 200).

%% How to long wait for a process to die after an expected failure
-define(PROCESS_EXIT_TIMEOUT, 5000).

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
     subscribe_with_auto_flow,
     outgoing_heartbeat,
     roundtrip_large_messages
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
    rabbit_ct_helpers:run_teardown_steps(Config,
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
    Config = rabbit_ct_helpers:set_config(Config0,
                                          {sasl, {plain, <<"guest">>, <<"guest">>}}),
    Config1 = rabbit_ct_helpers:merge_app_env(Config,
                                              [{rabbitmq_amqp1_0,
                                                [{protocol_strict_mode, true}]}]),
    rabbit_ct_helpers:run_steps(Config1, rabbit_ct_broker_helpers:setup_steps());
init_per_group(rabbitmq_strict, Config0) ->
    Config = rabbit_ct_helpers:set_config(Config0,
                                          {sasl, {plain, <<"guest">>, <<"guest">>}}),
    Config1 = rabbit_ct_helpers:merge_app_env(Config,
                                              [{rabbitmq_amqp1_0,
                                                [{default_user, none},
                                                 {protocol_strict_mode, true}]}]),
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
                    io_lib:format("amqp://~s:~b", [Hostname, Port]));
              {plain, Usr, Pwd} ->
                  lists:flatten(
                    io_lib:format("amqp://~s:~s@~s:~b?sasl=plain",
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
              ct:pal("Connection process is alive? = ~p~n",
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
    DataKb = crypto:strong_rand_bytes(1024),
    roundtrip(OpenConf, DataKb),
    Data1Mb = binary:copy(DataKb, 1024),
    roundtrip(OpenConf, Data1Mb),
    roundtrip(OpenConf, binary:copy(Data1Mb, 8)),
    roundtrip(OpenConf, binary:copy(Data1Mb, 64)),
    ok.


roundtrip(OpenConf) ->
    roundtrip(OpenConf, <<"banana">>).

roundtrip(OpenConf, Body) ->
    {ok, Connection} = amqp10_client:open_connection(OpenConf),
    {ok, Session} = amqp10_client:begin_session(Connection),
    {ok, Sender} = amqp10_client:attach_sender_link(Session,
                                                    <<"banana-sender">>,
                                                    <<"test1">>,
                                                    settled,
                                                    unsettled_state),
    await_link(Sender, credited, link_credit_timeout),

    Now = os:system_time(millisecond),
    Props = #{creation_time => Now},
    Msg0 =  amqp10_msg:set_properties(Props,
                                      amqp10_msg:new(<<"my-tag">>, Body, true)),
    Msg1 = amqp10_msg:set_application_properties(#{"a_key" => "a_value"}, Msg0),
    Msg = amqp10_msg:set_message_annotations(#{<<"x_key">> => "x_value"}, Msg1),
    % RabbitMQ AMQP 1.0 does not yet support delivery annotations
    % Msg = amqp10_msg:set_delivery_annotations(#{<<"x_key">> => "x_value"}, Msg2),
    ok = amqp10_client:send_msg(Sender, Msg),
    ok = amqp10_client:detach_link(Sender),
    await_link(Sender, {detached, normal}, link_detach_timeout),

    {error, link_not_found} = amqp10_client:detach_link(Sender),
    {ok, Receiver} = amqp10_client:attach_receiver_link(Session,
                                                        <<"banana-receiver">>,
                                                        <<"test1">>,
                                                        settled,
                                                        unsettled_state),
    {ok, OutMsg} = amqp10_client:get_msg(Receiver, 60000 * 5),
    ok = amqp10_client:end_session(Session),
    ok = amqp10_client:close_connection(Connection),
    % ct:pal(?LOW_IMPORTANCE, "roundtrip message Out: ~p~nIn: ~p~n", [OutMsg, Msg]),
    #{creation_time := Now} = amqp10_msg:properties(OutMsg),
    #{<<"a_key">> := <<"a_value">>} = amqp10_msg:application_properties(OutMsg),
    #{<<"x_key">> := <<"x_value">>} = amqp10_msg:message_annotations(OutMsg),
    % #{<<"x_key">> := <<"x_value">>} = amqp10_msg:delivery_annotations(OutMsg),
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
    {ok, OutMsg1} = amqp10_client:get_msg(DefaultReceiver, 60000 * 5),
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

    {ok, OutMsg2} = amqp10_client:get_msg(DefaultReceiver, 60000 * 5),
    ?assertEqual(<<"msg-2-tag">>, amqp10_msg:delivery_tag(OutMsg2)),

    {ok, OutMsgFiltered} = amqp10_client:get_msg(FilteredReceiver, 60000 * 5),
    ?assertEqual(<<"msg-2-tag">>, amqp10_msg:delivery_tag(OutMsgFiltered)),

    ok = amqp10_client:end_session(Session),
    ok = amqp10_client:close_connection(Connection),
    ok.

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
    QueueName = <<"test-sub">>,
    {ok, Connection} = amqp10_client:open_connection(Hostname, Port),
    {ok, Session} = amqp10_client:begin_session(Connection),
    {ok, Sender} = amqp10_client:attach_sender_link_sync(Session,
                                                         <<"sub-sender">>,
                                                         QueueName),
    _ = publish_messages(Sender, <<"banana">>, 10),
    {ok, Receiver} = amqp10_client:attach_receiver_link(Session,
                                                        <<"sub-receiver">>,
                                                        QueueName, unsettled),
    ok = amqp10_client:flow_link_credit(Receiver, 10, never),

    _ = receive_messages(Receiver, 10),
    % assert no further messages are delivered
    timeout = receive_one(Receiver),
    receive
        {amqp10_event, {link, Receiver, credit_exhausted}} ->
            ok
    after 5000 ->
              flush(),
              exit(credit_exhausted_assert)
    end,

    ok = amqp10_client:end_session(Session),
    ok = amqp10_client:close_connection(Connection).

subscribe_with_auto_flow(Config) ->
    Hostname = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    QueueName = <<"test-sub">>,
    {ok, Connection} = amqp10_client:open_connection(Hostname, Port),
    {ok, Session} = amqp10_client:begin_session(Connection),
    {ok, Sender} = amqp10_client:attach_sender_link_sync(Session,
                                                         <<"sub-sender">>,
                                                         QueueName),
    _ = publish_messages(Sender, <<"banana">>, 10),
    {ok, Receiver} = amqp10_client:attach_receiver_link(Session,
                                                        <<"sub-receiver">>,
                                                        QueueName, unsettled),
    ok = amqp10_client:flow_link_credit(Receiver, 5, 2),

    _ = receive_messages(Receiver, 10),

    % assert no further messages are delivered
    timeout = receive_one(Receiver),
    ok = amqp10_client:end_session(Session),
    ok = amqp10_client:close_connection(Connection).

insufficient_credit(Config) ->
    Hostname = ?config(mock_host, Config),
    Port = ?config(mock_port, Config),
    OpenStep = fun({0 = Ch, #'v1_0.open'{}, _Pay}) ->
                       {Ch, [#'v1_0.open'{container_id = {utf8, <<"mock">>}}]}
               end,
    BeginStep = fun({1 = Ch, #'v1_0.begin'{}, _Pay}) ->
                         {Ch, [#'v1_0.begin'{remote_channel = {ushort, 1},
                                             next_outgoing_id = {uint, 1},
                                             incoming_window = {uint, 1000},
                                             outgoing_window = {uint, 1000}}
                                             ]}
                end,
    AttachStep = fun({1 = Ch, #'v1_0.attach'{role = false,
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
    BeginStep = fun({1 = Ch, #'v1_0.begin'{}, _Pay}) ->
                         {Ch, [#'v1_0.begin'{remote_channel = {ushort, 1},
                                             next_outgoing_id = {uint, 1},
                                             incoming_window = {uint, 1000},
                                             outgoing_window = {uint, 1000}}
                                             ]}
                end,
    AttachStep = fun({1 = Ch, #'v1_0.attach'{role = true,
                                             name = Name}, <<>>}) ->
                         {Ch, [#'v1_0.attach'{name = Name,
                                              handle = {uint, 99},
                                              initial_delivery_count = {uint, 1},
                                              role = false}
                              ]}
                 end,

    LinkCreditStep = fun({1 = Ch, #'v1_0.flow'{}, <<>>}) ->
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
        {amqp10_event, {connection, Connection,
         {closed, {resource_limit_exceeded, <<"remote idle-time-out">>}}}} ->
            ok
    after 5000 ->
          exit(incoming_heartbeat_assert)
    end,
    demonitor(MockRef).


%%% HELPERS
%%%

receive_messages(Receiver, Num) ->
    [begin
         ct:pal("receive_messages ~p", [T]),
         ok = receive_one(Receiver)
     end || T <- lists:seq(1, Num)].

publish_messages(Sender, Data, Num) ->
    [begin
        Tag = integer_to_binary(T),
        Msg = amqp10_msg:new(Tag, Data, false),
        ok = amqp10_client:send_msg(Sender, Msg),
        ok = await_disposition(Tag)
     end || T <- lists:seq(1, Num)].

receive_one(Receiver) ->
    receive
        {amqp10_msg, Receiver, Msg} ->
            amqp10_client:accept_msg(Receiver, Msg)
    after 2000 ->
          timeout
    end.

await_disposition(DeliveryTag) ->
    receive
        {amqp10_disposition, {accepted, DeliveryTag}} -> ok
    after 3000 ->
              flush(),
              exit(dispostion_timeout)
    end.

await_link(Who, What, Err) ->
    receive
        {amqp10_event, {link, Who, What}} ->
            ok;
        {amqp10_event, {link, Who, {detached, Why}}} ->
            exit(Why)
    after 5000 ->
              flush(),
              exit(Err)
    end.

to_bin(X) when is_list(X) ->
    list_to_binary(X).

flush() ->
    receive
        Any ->
            ct:pal("flush ~p", [Any]),
            flush()
    after 0 ->
              ok
    end.
