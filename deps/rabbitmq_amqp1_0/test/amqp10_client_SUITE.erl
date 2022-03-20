%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(amqp10_client_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").

-compile(export_all).

all() ->
    [
      {group, tests},
      {group, metrics}
    ].

groups() ->
    [
     {tests, [], [
                  roundtrip_quorum_queue_with_drain
                 ]},
     {metrics, [], [
                    auth_attempt_metrics
                   ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    application:ensure_all_started(amqp10_client),
    rabbit_ct_helpers:log_environment(),
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(Group, Config) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(
                Config, [
                         {rmq_nodename_suffix, Suffix},
                         {amqp10_client_library, Group}
                        ]),
    Config2 = rabbit_ct_helpers:run_setup_steps(
                Config1,
                rabbit_ct_broker_helpers:setup_steps() ++
                rabbit_ct_client_helpers:setup_steps()),
    Nodes = rabbit_ct_broker_helpers:get_node_configs(
              Config2, nodename),
    Ret = rabbit_ct_broker_helpers:rpc(
            Config2, 0,
            rabbit_feature_flags,
            is_supported_remotely,
            [Nodes, [quorum_queue], 60000]),
    case Ret of
        true ->
            ok = rabbit_ct_broker_helpers:rpc(
                    Config2, 0, rabbit_feature_flags, enable, [quorum_queue]),
            Config2;
        false ->
            end_per_group(Group, Config2),
            {skip, "Quorum queues are unsupported"}
    end.

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%%% TESTS
%%%

roundtrip_quorum_queue_with_drain(Config) ->
    Host = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    QName  = atom_to_binary(?FUNCTION_NAME, utf8),
    Address = <<"/amq/queue/", QName/binary>>,
    %% declare a quorum queue
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    amqp_channel:call(Ch, #'queue.declare'{queue = QName,
                                           durable = true,
                                           arguments = [{<<"x-queue-type">>, longstr, <<"quorum">>}]}),
    % create a configuration map
    OpnConf = #{address => Host,
                port => Port,
                container_id => atom_to_binary(?FUNCTION_NAME, utf8),
                sasl => {plain, <<"guest">>, <<"guest">>}},
    % ct:pal("opening connectoin with ~p", [OpnConf]),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session(Connection),
    SenderLinkName = <<"test-sender">>,
    {ok, Sender} = amqp10_client:attach_sender_link(Session,
                                                    SenderLinkName,
                                                    Address),

    % wait for credit to be received
    receive
        {amqp10_event, {link, Sender, credited}} -> ok
    after 2000 ->
              exit(credited_timeout)
    end,

    % create a new message using a delivery-tag, body and indicate
    % it's settlement status (true meaning no disposition confirmation
    % will be sent by the receiver).
    OutMsg = amqp10_msg:new(<<"my-tag">>, <<"my-body">>, true),
    ok = amqp10_client:send_msg(Sender, OutMsg),

    flush("pre-receive"),
    % create a receiver link
    {ok, Receiver} = amqp10_client:attach_receiver_link(Session,
                                                        <<"test-receiver">>,
                                                        Address),

    % grant credit and drain
    ok = amqp10_client:flow_link_credit(Receiver, 1, never, true),

    % wait for a delivery
    receive
        {amqp10_msg, Receiver, _InMsg} -> ok
    after 2000 ->
              exit(delivery_timeout)
    end,
    OutMsg2 = amqp10_msg:new(<<"my-tag">>, <<"my-body2">>, true),
    ok = amqp10_client:send_msg(Sender, OutMsg2),

    %% no delivery should be made at this point
    receive
        {amqp10_msg, _, _} ->
            exit(unexpected_delivery)
    after 500 ->
              ok
    end,

    flush("final"),
    ok = amqp10_client:detach_link(Sender),

    ok = amqp10_client:close_connection(Connection),
    ok.

auth_attempt_metrics(Config) ->
    Host = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    % create a configuration map
    OpnConf = #{address => Host,
                port => Port,
                container_id => atom_to_binary(?FUNCTION_NAME, utf8),
                sasl => {plain, <<"guest">>, <<"guest">>}},
    open_and_close_connection(OpnConf),
    [Attempt] =
        rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_core_metrics, get_auth_attempts, []),
    ?assertEqual(false, proplists:is_defined(remote_address, Attempt)),
    ?assertEqual(false, proplists:is_defined(username, Attempt)),
    ?assertEqual(proplists:get_value(protocol, Attempt), <<"amqp10">>),
    ?assertEqual(proplists:get_value(auth_attempts, Attempt), 1),
    ?assertEqual(proplists:get_value(auth_attempts_failed, Attempt), 0),
    ?assertEqual(proplists:get_value(auth_attempts_succeeded, Attempt), 1),
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_core_metrics, reset_auth_attempt_metrics, []),
    rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                 [rabbit, track_auth_attempt_source, true]),
    open_and_close_connection(OpnConf),
    Attempts =
        rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_core_metrics, get_auth_attempts_by_source, []),
    [Attempt1] = lists:filter(fun(Props) ->
                                      proplists:is_defined(remote_address, Props)
                              end, Attempts),
    ?assertEqual(proplists:get_value(remote_address, Attempt1), <<>>),
    ?assertEqual(proplists:get_value(username, Attempt1), <<"guest">>),
    ?assertEqual(proplists:get_value(protocol, Attempt), <<"amqp10">>),
    ?assertEqual(proplists:get_value(auth_attempts, Attempt1), 1),
    ?assertEqual(proplists:get_value(auth_attempts_failed, Attempt1), 0),
    ?assertEqual(proplists:get_value(auth_attempts_succeeded, Attempt1), 1),
    ok.

%% internal
%%

flush(Prefix) ->
    receive
        Msg ->
            ct:pal("~s flushed: ~w~n", [Prefix, Msg]),
            flush(Prefix)
    after 1 ->
              ok
    end.

open_and_close_connection(OpnConf) ->
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, _} = amqp10_client:begin_session(Connection),
    ok = amqp10_client:close_connection(Connection).
