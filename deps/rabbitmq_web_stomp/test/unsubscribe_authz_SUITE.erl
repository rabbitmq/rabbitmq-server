%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%

%% The durable UNSUBSCRIBE cleanup lives in rabbit_stomp_processor, which this
%% plugin drives directly: the same authorization must hold over a WebSocket.
-module(unsubscribe_authz_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-define(TOPIC_DESTINATION, "/topic/WsUnsubscribeAuthzTopic").
-define(SUBSCRIPTION_ID, "ws-authz-sub").
-define(SUB_QUEUE, <<"stomp-subscription-ws-sub">>).
%% Stands in for another connection's durable subscription queue: it matches
%% the same configure pattern, so a configure check alone would not protect it.
-define(BYSTANDER_QUEUE, <<"stomp-subscription-ws-bystander">>).
-define(USER, <<"ws-authz-user">>).
-define(PASSWORD, <<"pass">>).
%% Least privilege for a durable topic subscriber: its own subscription queues.
-define(CONFIGURE, <<"^stomp-subscription-.*">>).

all() ->
    [
     durable_unsubscribe_ignores_frame_queue_name,
     durable_unsubscribe_requires_configure_permission
    ].

init_per_suite(Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_nodename_suffix, ?MODULE},
                                            {protocol, "ws"}]),
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:testcase_started(Config, Testcase),
    {ok, Connection} = amqp_connection:start(#amqp_params_direct{
        node = rabbit_ct_broker_helpers:get_node_config(Config1, 0, nodename)
    }),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    %% a queue this connection never subscribes to
    #'queue.declare_ok'{} =
        amqp_channel:call(Channel,
                          #'queue.declare'{queue       = ?BYSTANDER_QUEUE,
                                           durable     = true,
                                           auto_delete = false}),
    rabbit_ct_broker_helpers:add_user(Config1, ?USER, ?PASSWORD),
    ok = rabbit_ct_broker_helpers:set_permissions(
           Config1, ?USER, ?config(rmq_vhost, Config1),
           ?CONFIGURE, <<".*">>, <<".*">>),
    rabbit_ct_helpers:set_config(Config1, [
        {amqp_connection, Connection},
        {amqp_channel, Channel}
    ]).

end_per_testcase(Testcase, Config) ->
    Connection = ?config(amqp_connection, Config),
    Channel = ?config(amqp_channel, Config),
    amqp_channel:close(Channel),
    amqp_connection:close(Connection),
    ok = rabbit_ct_broker_helpers:delete_user(Config, ?USER),
    _ = [delete_queue_if_present(QRes, Config)
         || QRes <- [queue_resource(?BYSTANDER_QUEUE, Config),
                     queue_resource(?SUB_QUEUE, Config)]],
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% A durable UNSUBSCRIBE must delete the queue this connection subscribed to,
%% not the one x-queue-name names on the UNSUBSCRIBE frame.
durable_unsubscribe_ignores_frame_queue_name(Config) ->
    Bystander = queue_resource(?BYSTANDER_QUEUE, Config),
    SubQueue = queue_resource(?SUB_QUEUE, Config),
    WS = authz_subscribe(Config),
    ?assertMatch({ok, _}, lookup_queue(SubQueue, Config)),
    ?assertMatch({ok, _}, lookup_queue(Bystander, Config)),
    %% the id alone resolves the subscription the delete targets
    ok = raw_send(WS, "UNSUBSCRIBE",
                  [{"destination", ?TOPIC_DESTINATION},
                   {"id", ?SUBSCRIPTION_ID},
                   {"durable", "true"},
                   {"x-queue-name", ?BYSTANDER_QUEUE},
                   {"receipt", "rcpt2"}]),
    {<<"RECEIPT">>, ReceiptHeaders, <<>>} = raw_recv(WS),
    ?assertEqual(<<"rcpt2">>,
                 proplists:get_value(<<"receipt-id">>, ReceiptHeaders)),
    %% a misdirected delete would land before the RECEIPT, so check it first
    ?assertMatch({ok, _}, lookup_queue(Bystander, Config)),
    ?awaitMatch({error, not_found}, lookup_queue(SubQueue, Config), 30_000),
    {close, _} = rfc6455_client:close(WS),
    ok.

%% Deleting the subscription's queue requires configure permission on it, so a
%% permission revoked after the subscription was created must take effect.
durable_unsubscribe_requires_configure_permission(Config) ->
    Bystander = queue_resource(?BYSTANDER_QUEUE, Config),
    SubQueue = queue_resource(?SUB_QUEUE, Config),
    WS = authz_subscribe(Config),
    ?assertMatch({ok, _}, lookup_queue(SubQueue, Config)),
    ok = rabbit_ct_broker_helpers:set_permissions(
           Config, ?USER, ?config(rmq_vhost, Config),
           <<"^$">>, <<".*">>, <<".*">>),
    ok = raw_send(WS, "UNSUBSCRIBE",
                  [{"destination", ?TOPIC_DESTINATION},
                   {"id", ?SUBSCRIPTION_ID},
                   {"durable", "true"},
                   {"x-queue-name", ?BYSTANDER_QUEUE},
                   {"receipt", "rcpt2"}]),
    {<<"ERROR">>, ErrorHeaders, _} = raw_recv(WS),
    ?assertEqual(<<"access_refused">>,
                 proplists:get_value(<<"message">>, ErrorHeaders)),
    ?assertMatch({ok, _}, lookup_queue(SubQueue, Config)),
    ?assertMatch({ok, _}, lookup_queue(Bystander, Config)),
    %% the receipt for the refused frame still follows the ERROR, because
    %% process_request/3 runs the success continuation on the stop path
    {<<"RECEIPT">>, _, <<>>} = raw_recv(WS),
    {close, _} = raw_recv(WS),
    ok.

%% auto-delete:false, so the queue outlives its consumer. x-queue-name is
%% legitimate here: on SUBSCRIBE it is checked against configure permission.
authz_subscribe(Config) ->
    PortStr = rabbit_ws_test_util:get_web_stomp_port_str(Config),
    Protocol = ?config(protocol, Config),
    WS = rfc6455_client:new(
           Protocol ++ "://127.0.0.1:" ++ PortStr ++ "/ws", self()),
    {ok, _} = rfc6455_client:open(WS),
    ok = raw_send(WS, "CONNECT", [{"login", ?USER},
                                  {"passcode", ?PASSWORD}]),
    {<<"CONNECTED">>, _, <<>>} = raw_recv(WS),
    ok = raw_send(WS, "SUBSCRIBE",
                  [{"destination", ?TOPIC_DESTINATION},
                   {"id", ?SUBSCRIPTION_ID},
                   {"durable", "true"},
                   {"auto-delete", "false"},
                   {"x-queue-name", ?SUB_QUEUE},
                   {"receipt", "rcpt1"}]),
    {<<"RECEIPT">>, ReceiptHeaders, <<>>} = raw_recv(WS),
    ?assertEqual(<<"rcpt1">>,
                 proplists:get_value(<<"receipt-id">>, ReceiptHeaders)),
    WS.

queue_resource(QNameBin, Config) ->
    rabbit_misc:r(?config(rmq_vhost, Config), queue, QNameBin).

lookup_queue(QRes, Config) ->
    rabbit_ct_broker_helpers:rpc(
      Config, 0, rabbit_amqqueue, lookup, [QRes]).

delete_queue_if_present(QRes, Config) ->
    case lookup_queue(QRes, Config) of
        {ok, _} ->
            rabbit_ct_broker_helpers:rpc(
              Config, 0, rabbit_amqqueue, delete_with,
              [QRes, false, false, <<"acting-user">>]);
        _ ->
            ok
    end.

raw_send(WS, Command, Headers) ->
    raw_send(WS, Command, Headers, <<>>).

raw_send(WS, Command, Headers, Body) ->
    Frame = stomp:marshal(Command, Headers, Body),
    rfc6455_client:send(WS, Frame).

raw_recv(WS) ->
    case rfc6455_client:recv(WS) of
        {ok, P} -> stomp:unmarshal(P);
        Other   -> Other
    end.
