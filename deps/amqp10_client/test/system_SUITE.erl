%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(system_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include_lib("amqp10_common/include/amqp10_framing.hrl").

-include("amqp10_client.hrl").

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
     {group, activemq}
    ].

groups() ->
    [
     {rabbitmq, [], [
                     open_close_connection,
                     basic_roundtrip,
                     split_transfer,
                     transfer_unsettled
                    ]},
     {activemq, [], [
                     open_close_connection,
                     basic_roundtrip,
                     split_transfer,
                     transfer_unsettled,
                     send_multiple
                    ]},
     {mock, [], [
                 insufficient_credit
                ]}
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

init_per_group(rabbitmq, Config) ->
      rabbit_ct_helpers:run_steps(
        Config,
        rabbit_ct_broker_helpers:setup_steps());
init_per_group(activemq, Config) ->
      rabbit_ct_helpers:run_steps(
        Config,
        activemq_ct_helpers:setup_steps());
init_per_group(mock, Config) ->
    rabbit_ct_helpers:set_config(Config, [{mock_port, 21000},
                                          {mock_host, "localhost"}
                                         ]).

end_per_group(rabbitmq, Config) ->
      rabbit_ct_helpers:run_steps(
        Config,
        rabbit_ct_broker_helpers:teardown_steps());
end_per_group(activemq, Config) ->
      rabbit_ct_helpers:run_steps(
        Config,
        activemq_ct_helpers:teardown_steps());
end_per_group(mock, Config) ->
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
    OpnConf = #{address => Hostname, port => Port,
                container_id => <<"open_close_connection_container">>},
    {ok, Connection} = amqp10_client_connection:open(Hostname, Port),
    {ok, Connection2} = amqp10_client_connection:open(OpnConf),
    {ok, Session} = amqp10_client_session:begin_sync(Connection),
    {ok, Session2} = amqp10_client_session:begin_sync(Connection2),
    ok = amqp10_client_session:'end'(Session),
    ok = amqp10_client_session:'end'(Session2),
    ok = amqp10_client_connection:close(Connection2),
    ok = amqp10_client_connection:close(Connection).

basic_roundtrip(Config) ->
    Hostname = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    ct:pal("Opening connection to ~s:~b", [Hostname, Port]),
    {ok, Connection} = amqp10_client_connection:open(Hostname, Port),
    {ok, Session} = amqp10_client_session:'begin'(Connection),
    {ok, Sender} = amqp10_client_link:sender(Session, <<"banana-sender">>,
                                             <<"test">>),
    Msg = amqp10_msg:new(<<"my-tag">>, <<"banana">>, true),
    ok = amqp10_client_link:send(Sender, Msg),
    {ok, Receiver} = amqp10_client_link:receiver(Session, <<"banana-receiver">>,
                                                 <<"test">>),
    {amqp_msg, OutMsg} = amqp10_client_link:get_one(Receiver),
    ok = amqp10_client_session:'end'(Session),
    ok = amqp10_client_connection:close(Connection),
    ?assertEqual([<<"banana">>], amqp10_msg:body(OutMsg)).

split_transfer(Config) ->
    Hostname = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    ct:pal("Opening connection to ~s:~b", [Hostname, Port]),
    Conf = #{address => Hostname, port => Port, max_frame_size => 512},
    {ok, Connection} = amqp10_client_connection:open(Conf),
    {ok, Session} = amqp10_client_session:'begin'(Connection),
    Data = list_to_binary(string:chars(64, 1000)),
    {ok, Sender} = amqp10_client_link:sender(Session, <<"data-sender">>,
                                             <<"test">>),
    Msg = amqp10_msg:new(<<"my-tag">>, Data, true),
    ok = amqp10_client_link:send(Sender, Msg),
    {ok, Receiver} = amqp10_client_link:receiver(Session, <<"data-receiver">>,
                                                 <<"test">>),
    {amqp_msg, OutMsg} = amqp10_client_link:get_one(Receiver),
    ok = amqp10_client_session:'end'(Session),
    ok = amqp10_client_connection:close(Connection),
    ?assertEqual([Data], amqp10_msg:body(OutMsg)).

transfer_unsettled(Config) ->
    Hostname = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    Conf = #{address => Hostname, port => Port},
    {ok, Connection} = amqp10_client_connection:open(Conf),
    {ok, Session} = amqp10_client_session:'begin'(Connection),
    Data = list_to_binary(string:chars(64, 1000)),
    {ok, Sender} = amqp10_client_link:sender(Session, <<"data-sender">>,
                                             <<"test">>, unsettled),
    Msg = amqp10_msg:new(<<"my-tag">>, Data, false),
    accepted = amqp10_client_link:send(Sender, Msg),
    {ok, Receiver} = amqp10_client_link:receiver(Session, <<"data-receiver">>,
                                                 <<"test">>, unsettled),
    {amqp_msg, OutMsg} = amqp10_client_link:get_one(Receiver),
    ok = amqp10_client_link:accept(Receiver, OutMsg),
    {error, timeout} = amqp10_client_link:get_one(Receiver, 1000),
    ok = amqp10_client_session:'end'(Session),
    ok = amqp10_client_connection:close(Connection),
    ?assertEqual([Data], amqp10_msg:body(OutMsg)).

send_multiple(Config) ->
    Hostname = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    {ok, Connection} = amqp10_client_connection:open(Hostname, Port),
    {ok, Session} = amqp10_client_session:'begin'(Connection),
    {ok, Sender} = amqp10_client_link:sender(Session, <<"multi-sender">>,
                                             <<"test">>),
    Msg1 = amqp10_msg:new(<<"my-tag">>, <<"banana">>, true),
    Msg2 = amqp10_msg:new(<<"my-tag2">>, <<"banana">>, true),
    ok = amqp10_client_link:send(Sender, Msg1),
    ok = amqp10_client_link:send(Sender, Msg2),

    ok = amqp10_client_session:'end'(Session),
    ok = amqp10_client_connection:close(Connection).


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
    Steps = [
             fun mock_server:recv_amqp_header_step/1,
             fun mock_server:send_amqp_header_step/1,
             mock_server:amqp_step(OpenStep),
             mock_server:amqp_step(BeginStep),
             mock_server:amqp_step(AttachStep)
            ],

    ok = mock_server:set_steps(?config(mock_server, Config), Steps),

    Cfg = #{address => Hostname, port => Port, sasl => none},
    {ok, Connection} = amqp10_client_connection:open(Cfg),
    {ok, Session} = amqp10_client_session:begin_sync(Connection),
    {ok, Sender} = amqp10_client_link:sender(Session, <<"mock1-sender">>,
                                             <<"test">>),
    Msg = amqp10_msg:new(<<"mock-tag">>, <<"banana">>, true),
    insufficient_credit = amqp10_client_link:send(Sender, Msg),

    ok = amqp10_client_session:'end'(Session),
    ok = amqp10_client_connection:close(Connection),
    ok.
