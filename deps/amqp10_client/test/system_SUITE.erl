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
                     basic_roundtrip,
                     split_transfer,
                     transfer_unsettled
                    ]},
     {activemq, [], [
                     basic_roundtrip,
                     split_transfer,
                     transfer_unsettled
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
        activemq_ct_helpers:setup_steps()).

end_per_group(rabbitmq, Config) ->
      rabbit_ct_helpers:run_steps(
        Config,
        rabbit_ct_broker_helpers:teardown_steps());
end_per_group(activemq, Config) ->
      rabbit_ct_helpers:run_steps(
        Config,
        activemq_ct_helpers:teardown_steps()).

%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, Config) ->
    Config.

%% -------------------------------------------------------------------

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
