%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(msg_size_metrics_SUITE).

-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-import(rabbit_ct_broker_helpers,
        [rpc/4]).

all() ->
    [
     {group, tests}
    ].

groups() ->
    [
     {tests, [shuffle],
      [message_size,
       over_max_message_size]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(amqp10_client),
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(
      Config,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Test cases
%% -------------------------------------------------------------------

message_size(Config) ->
    AmqplBefore = get_msg_size_metrics(amqp091, Config),
    AmqpBefore = get_msg_size_metrics(amqp10, Config),

    Binary2B = <<"12">>,
    Binary200K  = binary:copy(<<"x">>, 200_000),
    Payloads = [Binary2B, Binary200K, Binary2B],

    {AmqplConn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    [amqp_channel:call(Ch,
                       #'basic.publish'{routing_key = <<"nowhere">>},
                       #amqp_msg{payload = Payload})
     || Payload <- Payloads],

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = rabbitmq_amqp_address:exchange(<<"amq.fanout">>),
    {ok, Sender} = amqp10_client:attach_sender_link_sync(Session, <<"sender">>, Address),
    receive {amqp10_event, {link, Sender, credited}} -> ok
    after 5000 -> ct:fail(credited_timeout)
    end,

    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"tag1">>, Binary2B)),
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"tag2">>, Binary200K)),
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"tag3">>, Binary2B)),

    ok = wait_for_settlement(released, <<"tag1">>),
    ok = wait_for_settlement(released, <<"tag2">>),
    ok = wait_for_settlement(released, <<"tag3">>),

    AmqplAfter = get_msg_size_metrics(amqp091, Config),
    AmqpAfter = get_msg_size_metrics(amqp10, Config),

    ExpectedDiff = [{100, 2},
                    {1_000_000, 1}],
    ?assertEqual(ExpectedDiff,
                 rabbit_msg_size_metrics:diff_raw_buckets(AmqplAfter, AmqplBefore)),
    ?assertEqual(ExpectedDiff,
                 rabbit_msg_size_metrics:diff_raw_buckets(AmqpAfter, AmqpBefore)),

    ok = amqp10_client:close_connection(Connection),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(AmqplConn, Ch).

over_max_message_size(Config) ->
    DefaultMaxMessageSize = rpc(Config, persistent_term, get, [max_message_size]),
    %% Limit the server to only accept messages up to 2KB.
    MaxMessageSize = 2_000,
    ok = rpc(Config, persistent_term, put, [max_message_size, MaxMessageSize]),

    Before = get_msg_size_metrics(amqp091, Config),
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    MonitorRef = erlang:monitor(process, Ch),
    MessageTooLarge = binary:copy(<<"x">>, MaxMessageSize + 1),
    amqp_channel:call(Ch,
                      #'basic.publish'{routing_key = <<"none">>},
                      #amqp_msg{payload = MessageTooLarge}),
    receive {'DOWN', MonitorRef, process, Ch, Info} ->
                ?assertEqual({shutdown,
                              {server_initiated_close,
                               406,
                               <<"PRECONDITION_FAILED - message size 2001 is larger than configured max size 2000">>}},
                             Info)
    after 2000 -> ct:fail(expected_channel_closed)
    end,

    After = get_msg_size_metrics(amqp091, Config),
    %% No metrics should be increased if client sent message that is too large.
    ?assertEqual(Before, After),

    ok = rabbit_ct_client_helpers:close_connection(Conn),
    ok = rpc(Config, persistent_term, put, [max_message_size, DefaultMaxMessageSize]).

get_msg_size_metrics(Protocol, Config) ->
    rpc(Config, rabbit_msg_size_metrics, raw_buckets, [Protocol]).

connection_config(Config) ->
    Host = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    #{address => Host,
      port => Port,
      container_id => <<"my container">>,
      sasl => anon}.

wait_for_settlement(State, Tag) ->
    receive
        {amqp10_disposition, {State, Tag}} ->
            ok
    after 5000 ->
              ct:fail({disposition_timeout, Tag})
    end.
