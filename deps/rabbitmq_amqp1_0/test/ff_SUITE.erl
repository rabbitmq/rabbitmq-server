%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2016-2023 VMware, Inc. or its affiliates.  All rights reserved.

-module(ff_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

all() ->
    [
     {group, cluster_size_1}
    ].

groups() ->
    [
     {cluster_size_1, [],
      [credit_api_v2]}
    ].

suite() ->
    [
     {timetrap, {minutes, 10}}
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(amqp10_client),
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config, []).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(_Group, Config0) ->
    Config = rabbit_ct_helpers:merge_app_env(
               Config0, {rabbit, [{forced_feature_flags_on_init, []}]}),
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_broker_helpers:setup_steps() ++
                                rabbit_ct_client_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_client_helpers:teardown_steps() ++
                                rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(TestCase, Config) ->
    case rabbit_ct_broker_helpers:is_feature_flag_supported(Config, TestCase) of
        true ->
            ?assertNot(rabbit_ct_broker_helpers:is_feature_flag_enabled(Config, TestCase)),
            Config;
        false ->
            {skip, io_lib:format("feature flag ~s is unsupported", [TestCase])}
    end.

end_per_testcase(_TestCase, Config) ->
    Config.

credit_api_v2(Config) ->
    CQ = <<"classic queue">>,
    QQ = <<"quorum queue">>,
    CQAddr = <<"/amq/queue/", CQ/binary>>,
    QQAddr = <<"/amq/queue/", QQ/binary>>,

    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = CQ}),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QQ,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, <<"quorum">>}]}),
    ok = rabbit_ct_client_helpers:close_channel(Ch),

    Host = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    OpnConf = #{address => Host,
                port => Port,
                container_id => <<"my container">>,
                sasl => {plain, <<"guest">>, <<"guest">>}},
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),

    {ok, CQSender} = amqp10_client:attach_sender_link(Session, <<"cq sender">>, CQAddr),
    {ok, QQSender} = amqp10_client:attach_sender_link(Session, <<"qq sender">>, QQAddr),
    receive {amqp10_event, {link, CQSender, credited}} -> ok
    after 5000 -> ct:fail(credited_timeout)
    end,
    receive {amqp10_event, {link, QQSender, credited}} -> ok
    after 5000 -> ct:fail(credited_timeout)
    end,

    %% Send 40 messages to each queue.
    NumMsgs = 40,
    [begin
         Bin = integer_to_binary(N),
         ok = amqp10_client:send_msg(CQSender, amqp10_msg:new(Bin, Bin, true)),
         ok = amqp10_client:send_msg(QQSender, amqp10_msg:new(Bin, Bin, true))
     end || N <- lists:seq(1, NumMsgs)],
    ok = amqp10_client:detach_link(CQSender),
    ok = amqp10_client:detach_link(QQSender),

    %% Consume with credit API v1
    CQAttachArgs = #{handle => 300,
                     name => <<"cq receiver 1">>,
                     role => {receiver, #{address => CQAddr,
                                          durable => configuration}, self()},
                     snd_settle_mode => unsettled,
                     rcv_settle_mode => first,
                     filter => #{}},
    {ok, CQReceiver1} = amqp10_client:attach_link(Session, CQAttachArgs),
    QQAttachArgs = #{handle => 400,
                     name => <<"qq receiver 1">>,
                     role => {receiver, #{address => QQAddr,
                                          durable => configuration}, self()},
                     snd_settle_mode => unsettled,
                     rcv_settle_mode => first,
                     filter => #{}},
    {ok, QQReceiver1} = amqp10_client:attach_link(Session, QQAttachArgs),

    ok = consume_and_accept(10, CQReceiver1, Session),
    ok = consume_and_accept(10, QQReceiver1, Session),

    ?assertEqual(ok,
                 rabbit_ct_broker_helpers:enable_feature_flag(Config, ?FUNCTION_NAME)),
    flush(enabled_feature_flag),

    %% Consume with credit API v2
    {ok, CQReceiver2} = amqp10_client:attach_receiver_link(
                          Session, <<"cq receiver 2">>, CQAddr, unsettled),
    {ok, QQReceiver2} = amqp10_client:attach_receiver_link(
                          Session, <<"qq receiver 2">>, QQAddr, unsettled),
    ok = consume_and_accept(10, CQReceiver2, Session),
    ok = consume_and_accept(10, QQReceiver2, Session),

    %% Consume via with credit API v1
    ok = consume_and_accept(10, CQReceiver1, Session),
    ok = consume_and_accept(10, QQReceiver1, Session),

    %% Detach the credit API v1 links and attach with the same output handle.
    ok = detach_sync(CQReceiver1),
    ok = detach_sync(QQReceiver1),
    {ok, CQReceiver3} = amqp10_client:attach_link(Session, CQAttachArgs),
    {ok, QQReceiver3} = amqp10_client:attach_link(Session, QQAttachArgs),

    %% The new links should use credit API v2
    ok = consume_and_accept(10, CQReceiver3, Session),
    ok = consume_and_accept(10, QQReceiver3, Session),

    flush(pre_drain),
    %% Draining should also work.
    ok = amqp10_client:flow_link_credit(CQReceiver3, 10, never, true),
    receive {amqp10_event, {link, CQReceiver3, credit_exhausted}} -> ok
    after 5000 -> ct:fail({missing_credit_exhausted, ?LINE})
    end,
    receive Unexpected1 -> ct:fail({unexpected, ?LINE, Unexpected1})
    after 20 -> ok
    end,

    ok = amqp10_client:flow_link_credit(QQReceiver3, 10, never, true),
    receive {amqp10_event, {link, QQReceiver3, credit_exhausted}} -> ok
    after 5000 -> ct:fail({missing_credit_exhausted, ?LINE})
    end,
    receive Unexpected2 -> ct:fail({unexpected, ?LINE, Unexpected2})
    after 20 -> ok
    end,

    ok = detach_sync(CQReceiver2),
    ok = detach_sync(QQReceiver2),
    ok = detach_sync(CQReceiver3),
    ok = detach_sync(QQReceiver3),
    ok = amqp10_client:end_session(Session),
    receive {amqp10_event, {session, Session, {ended, _}}} -> ok
    after 5000 -> ct:fail(missing_ended)
    end,
    ok = amqp10_client:close_connection(Connection),
    receive {amqp10_event, {connection, Connection, {closed, normal}}} -> ok
    after 5000 -> ct:fail(missing_closed)
    end.

consume_and_accept(NumMsgs, Receiver, Session) ->
    ok = amqp10_client:flow_link_credit(Receiver, NumMsgs, never),
    Msgs = receive_messages(Receiver, NumMsgs),
    ok = amqp10_client_session:disposition(
           Session,
           receiver,
           amqp10_msg:delivery_id(hd(Msgs)),
           amqp10_msg:delivery_id(lists:last(Msgs)),
           true,
           accepted).

receive_messages(Receiver, N) ->
    receive_messages0(Receiver, N, []).

receive_messages0(_Receiver, 0, Acc) ->
    lists:reverse(Acc);
receive_messages0(Receiver, N, Acc) ->
    receive
        {amqp10_msg, Receiver, Msg} ->
            receive_messages0(Receiver, N - 1, [Msg | Acc])
    after 5000 ->
              exit({timeout, {num_received, length(Acc)}, {num_missing, N}})
    end.

detach_sync(Receiver) ->
    ok = amqp10_client:detach_link(Receiver),
    receive {amqp10_event, {link, Receiver, {detached, normal}}} -> ok
    after 5000 -> ct:fail({missing_detached, Receiver})
    end.

flush(Prefix) ->
    receive
        Msg ->
            ct:pal("~ts flushed: ~p~n", [Prefix, Msg]),
            flush(Prefix)
    after 1 ->
              ok
    end.
