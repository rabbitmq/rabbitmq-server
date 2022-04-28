%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(amqp10_client_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").


-compile(export_all).

all() ->
    [
      {group, tests},
      {group, metrics}
    ].

groups() ->
    [
     {tests, [], [
                 roundtrip_quorum_queue_with_drain,
                 incoming_credit_accounting_with_confirm,
                 unsupported_settled_link_with_unsettled_message,
                 message_headers_conversion,
                 additional_queue_arguments
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

    
    wait_for_credit(Sender),

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

% These  tests represent the different scenarios where confirm is used in the 
% amqp 1.0 plugin. 
incoming_credit_accounting_with_confirm(Config) -> 
    SettleModes = [unsettled, mixed],
    EndpointDurabilities = [configuration, unsettled_state],
    Permutations = [{A, B} || A <- SettleModes, B <- EndpointDurabilities],
    lists:foreach(fun({EndpointSettleMode, EndpointDurability}) -> 
        incoming_credit_accounting_with_confirm(Config, 5, on_publish, false, EndpointSettleMode, EndpointDurability),
        incoming_credit_accounting_with_confirm(Config, 5, on_publish, true, EndpointSettleMode, EndpointDurability),
        incoming_credit_accounting_with_confirm(Config, 5, on_confirm, false, EndpointSettleMode, EndpointDurability),
        incoming_credit_accounting_with_confirm(Config, 5, on_confirm, true, EndpointSettleMode, EndpointDurability)
    end, Permutations).
incoming_credit_accounting_with_confirm(Config, IncomingCredit, AckOn, MsgSettle, EdpointSettleMode, EndpointDurability) ->
    flush("Before test"),
    ct:pal("Ack: ~p, MsgSettle: ~p, Endpoint: ~p, Durability: ~p ", 
           [AckOn, MsgSettle, EdpointSettleMode, EndpointDurability]),
    Host = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env, [rabbitmq_amqp1_0, maximum_incoming_credit, IncomingCredit]),
    rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env, [rabbitmq_amqp1_0, grant_credit, AckOn]),
    
    QName  = atom_to_binary(?FUNCTION_NAME, utf8),
    Address = <<"/amq/queue/", QName/binary>>,
    
    delete_queue(Config, QName),
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    amqp_channel:call(Ch, #'queue.declare'{queue = QName,
                                           durable = true,
                                           arguments = [{<<"x-queue-type">>, longstr, <<"quorum">>}]}),
    % create a configuration map
    
    OpnConf = #{address => Host,
                port => Port,
                container_id => atom_to_binary(?FUNCTION_NAME, utf8),
                sasl => {plain, <<"guest">>, <<"guest">>}},
    
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session(Connection),
    SenderLinkName = <<"test-sender">>,
    {ok, Sender} = amqp10_client:attach_sender_link(Session,
                                                    SenderLinkName,
                                                    Address,
                                                    EdpointSettleMode,
                                                    EndpointDurability),

    
    wait_for_credit(Sender),
    
    
    [send_message(Sender, MsgSettle, N) || N <- lists:seq(0,15)],
    wait_for_accepts(16),

    ok = amqp10_client:detach_link(Sender),

    {ok, Msgs} = drain_queue(Session, Address, 16),
    
    ?assertEqual(16, length(Msgs)),

    ok = amqp10_client:close_connection(Connection),
    delete_queue(Config, QName),
    ok.

unsupported_settled_link_with_unsettled_message(Config) ->
    Host = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    QName  = atom_to_binary(?FUNCTION_NAME, utf8),
    Address = <<"/amq/queue/", QName/binary>>,

    OpnConf = #{address => Host,
    port => Port,
    container_id => atom_to_binary(?FUNCTION_NAME, utf8),
    sasl => {plain, <<"guest">>, <<"guest">>}},
    
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session(Connection),
    SenderLinkName = <<"test-sender">>,
    {ok, Sender} = amqp10_client:attach_sender_link(Session,
                                                    SenderLinkName,
                                                    Address,
                                                    settled),

    
    wait_for_credit(Sender),
    send_message(Sender, false, 1),
    
    receive 
        {amqp10_event,
            {session,Session,{ended,{'v1_0.error',
                {symbol,<<"amqp:not-implemented">>},
                {utf8,Error},undefined}}}}
        -> 
            ?assertEqual(
                <<"Publishing an unsettled message on a link set as 'settled' is not supported ">>,
                Error
            ),
            ok
    after  5000 -> 
        flush("Unsupported failed"),
        exit(expected_error)
    end.

message_headers_conversion(Config) ->
    Host = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    QName  = atom_to_binary(?FUNCTION_NAME, utf8),
    Address = <<"/amq/queue/", QName/binary>>,
    %% declare a quorum queue
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    amqp_channel:call(Ch, #'queue.declare'{queue = QName,
                                            durable = true,
                                            arguments = [{<<"x-queue-type">>, longstr, <<"quorum">>}]}),
    
    rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,[rabbitmq_amqp1_0, convert_amqp091_headers_to_app_props, true]),
    rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,[rabbitmq_amqp1_0, convert_app_props_to_amqp091_headers, true]),
    
    OpnConf = #{address => Host,
                port => Port,
                container_id => atom_to_binary(?FUNCTION_NAME, utf8),
                sasl => {plain, <<"guest">>, <<"guest">>}},
    % ct:pal("opening connectoin with ~p", [OpnConf]),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session(Connection),
    
    amqp10_to_amqp091_header_conversion(Session, Ch, QName, Address),

    amqp091_to_amqp10_header_conversion(Session, Ch, QName, Address),
    
    ok = amqp10_client:close_connection(Connection),
    ok.

amqp10_to_amqp091_header_conversion(Session,Ch, QName, Address) -> 
    {ok, Sender} = create_amqp10_sender(Session, Address),

    OutMsg = amqp10_msg:new(<<"my-tag">>, <<"my-body">>, true),
    OutMsg2 = amqp10_msg:set_application_properties(#{
        "x-string" => "string-value",
        "x-int" => 3,
        "x-bool" => true
    }, OutMsg),
    ok = amqp10_client:send_msg(Sender, OutMsg2),
    wait_for_accepts(1),

    {ok, Headers} = amqp091_get_msg_headers(Ch, QName),

    ?assertEqual({bool, true}, rabbit_misc:table_lookup(Headers, <<"x-bool">>)),
    ?assertEqual({long, 3}, rabbit_misc:table_lookup(Headers, <<"x-int">>)),
    ?assertEqual({longstr, <<"string-value">>}, rabbit_misc:table_lookup(Headers, <<"x-string">>)).    


amqp091_to_amqp10_header_conversion(Session, Ch, QName, Address) -> 
    Amqp091Headers = [
    {<<"x-forwarding">>, array, [{table,
                                    [{<<"uri">>, longstr,
                                    <<"amqp://localhost/%2F/upstream">>}]}]},
    {<<"x-string">>, longstr, "my-string"},
    {<<"x-int">>, long, 92},
    {<<"x-bool">>, bool, true}],

    amqp_channel:cast(Ch, 
        #'basic.publish'{exchange = <<"">>, routing_key = QName},
        #amqp_msg{props = #'P_basic'{
            headers = Amqp091Headers}, 
            payload = <<"foobar">> }
        ),

    {ok, [Msg]} = drain_queue(Session, Address, 1),
    Amqp10Props = amqp10_msg:application_properties(Msg),
    ?assertEqual(true, maps:get(<<"x-bool">>, Amqp10Props, undefined)),
    ?assertEqual(92, maps:get(<<"x-int">>, Amqp10Props, undefined)),    
    ?assertEqual(<<"my-string">>, maps:get(<<"x-string">>, Amqp10Props, undefined)).

additional_queue_arguments(Config) ->
    Host = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    QName  = atom_to_binary(?FUNCTION_NAME, utf8),
    rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,[rabbitmq_amqp1_0, additional_queue_arguments, [
        {'x-queue-type', <<"quorum">>}
    ]]),
    Address = QName,

    OpnConf = #{address => Host,
        port => Port,
        container_id => atom_to_binary(?FUNCTION_NAME, utf8),
        sasl => {plain, <<"guest">>, <<"guest">>}},
    
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session(Connection),
    SenderLinkName = <<"test-sender">>,
    {ok, Sender} = amqp10_client:attach_sender_link(Session,
                                                    SenderLinkName,
                                                    Address,
                                                    unsettled,
                                                    configuration),

    
    wait_for_credit(Sender),
    send_message(Sender, true, 1),
    {ok, [_]} = drain_queue(Session, Address, 1),
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName,
                                           durable = true,
                                           arguments = [{<<"x-queue-type">>, longstr, <<"quorum">>}]}).

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

wait_for_message(Receiver) -> 
    receive
        {amqp10_msg, Receiver, _InMsg} -> ok
    after 2000 ->
              exit(delivery_timeout)
    end.

% before we can send messages we have to wait for credit from the server
wait_for_credit(Sender) -> 
    receive
        {amqp10_event, {link, Sender, credited}} -> 
            ok
    after 5000 ->
        flush("Credit timed out"),
        exit(credited_timeout)
    end.

send_message(Sender, MsgSettle, N) -> 
    ct:pal("Publish message ~p", [N]),
    Tag = crypto:strong_rand_bytes(12),
    OutMsg = amqp10_msg:new(Tag, erlang:integer_to_binary(N), MsgSettle),
    ok = perform_send_message(Sender, OutMsg).

perform_send_message(Sender, OutMsg) -> 
    case amqp10_client:send_msg(Sender, OutMsg) of
        ok ->
            ok;
        {error, insufficient_credit} ->
            ct:pal("Insuf credit"),
            wait_for_credit(Sender),
            perform_send_message(Sender, OutMsg)
    end.

drain_queue(Session, Address, N) -> 
    flush("Before drain_queue"),
    {ok, Receiver} = amqp10_client:attach_receiver_link(Session,
                    <<"test-receiver">>,
                    Address, 
                    settled,
                    configuration),

    ok = amqp10_client:flow_link_credit(Receiver, 1000, never, true),
    Msgs = receive_message(Receiver, N, []),
    flush("after drain"),
    ok = amqp10_client:detach_link(Receiver),
    {ok, Msgs}.

receive_message(_Receiver, 0, Acc) -> lists:reverse(Acc);
receive_message(Receiver, N, Acc) ->
    receive
        {amqp10_msg, Receiver, Msg} -> 
            receive_message(Receiver, N-1, [Msg | Acc])
    after 5000  ->
            exit(receive_timed_out)
    end.

delete_queue(Config, QName) -> 
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    _ = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    rabbit_ct_client_helpers:close_channel(Ch).

wait_for_accepts(0) -> ok;
wait_for_accepts(N) -> 
    receive 
        {amqp10_disposition,{accepted,_}} -> wait_for_accepts(N -1)
    after 250 -> 
        ok
    end.
amqp091_get_msg_headers(Channel, QName) -> 
    {#'basic.get_ok'{}, #amqp_msg{props = #'P_basic'{ headers= Headers}}}
        = amqp_channel:call(Channel, #'basic.get'{queue = QName, no_ack = true}),
    {ok, Headers}.

create_amqp10_sender(Session, Address) -> 
    SenderLinkName = <<"test-sender">>,
    {ok, Sender} = amqp10_client:attach_sender_link(Session,
                                                    SenderLinkName,
                                                    Address),
    wait_for_credit(Sender),
    {ok, Sender}.
