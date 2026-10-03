%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(queue_type_enabled_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

all() ->
    [{group, cluster_size_1}].

groups() ->
    [{cluster_size_1, [], [
                           declare_allowed_when_type_enabled,
                           declare_refused_when_type_disabled,
                           direct_declare_refused_when_type_disabled,
                           direct_redeclare_existing_when_type_disabled,
                           redeclare_deleted_concurrently_when_type_disabled,
                           amqp_v1_attach_refused_when_type_disabled,
                           server_named_exclusive_queue_refused_when_classic_disabled,
                           direct_reply_to_works_when_classic_disabled,
                           amqp_v1_forged_reply_to_refused_when_classic_disabled,
                           amqp_dynamic_queue_refused_when_classic_disabled
                          ]}].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(amqp10_client),
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group, Config) ->
    Config1 = rabbit_ct_helpers:merge_app_env(
                rabbit_ct_helpers:set_config(
                  Config, [{rmq_nodes_count, 1},
                           {rmq_nodename_suffix, Group}]),
                {rabbit, [{permit_deprecated_features,
                           #{amqp_address_v1 => true}}]}),
    rabbit_ct_helpers:run_steps(
      Config1,
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
    ok = set_stream_queues_enabled(Config, true),
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, application, unset_env, [rabbit, classic_queues_enabled]),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

declare_allowed_when_type_enabled(Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    Q = <<"queue_type_enabled_SUITE.allowed">>,
    ?assertMatch(#'queue.declare_ok'{},
                 amqp_channel:call(Ch, declare(Q))),
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = Q}),
    rabbit_ct_client_helpers:close_channel(Ch).

declare_refused_when_type_disabled(Config) ->
    ok = set_stream_queues_enabled(Config, false),
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    Q = <<"queue_type_enabled_SUITE.refused">>,
    ?assertExit(
       {{shutdown, {connection_closing,
                    {server_initiated_close, 541,
                     <<"INTERNAL_ERROR - cannot declare queue ", _/binary>>}}}, _},
       amqp_channel:call(Ch, declare(Q))),
    QName = rabbit_misc:r(<<"/">>, queue, Q),
    ?assertEqual({error, not_found},
                 rabbit_ct_broker_helpers:rpc(
                   Config, 0, rabbit_amqqueue, lookup, [QName])).

direct_declare_refused_when_type_disabled(Config) ->
    ok = set_stream_queues_enabled(Config, false),
    QName = rabbit_misc:r(<<"/">>, queue, <<"queue_type_enabled_SUITE.direct">>),
    ?assertMatch({protocol_error, internal_error, _, _},
                 direct_declare(Config, QName)),
    ?assertEqual({error, not_found},
                 rabbit_ct_broker_helpers:rpc(
                   Config, 0, rabbit_amqqueue, lookup, [QName])).

direct_redeclare_existing_when_type_disabled(Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    Q = <<"queue_type_enabled_SUITE.existing">>,
    #'queue.declare_ok'{} = amqp_channel:call(Ch, declare(Q)),
    ok = set_stream_queues_enabled(Config, false),
    QName = rabbit_misc:r(<<"/">>, queue, Q),
    ?assertMatch({existing, _}, direct_declare(Config, QName)),
    ok = set_stream_queues_enabled(Config, true),
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = Q}),
    rabbit_ct_client_helpers:close_channel(Ch).

redeclare_deleted_concurrently_when_type_disabled(Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    Q = <<"queue_type_enabled_SUITE.deleted_concurrently">>,
    #'queue.declare_ok'{} = amqp_channel:call(Ch, declare(Q)),
    ok = set_stream_queues_enabled(Config, false),
    QName = rabbit_misc:r(<<"/">>, queue, Q),
    rabbit_ct_broker_helpers:setup_meck(Config, [?MODULE]),
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, meck, new, [rabbit_policy, [no_link, passthrough]]),
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, meck, expect,
           [rabbit_policy, set,
            fun(Q1) ->
                    case amqqueue:get_name(Q1) of
                        QName ->
                            _ = rabbit_amqqueue:delete_with(
                                  QName, false, false, <<"acting-user">>);
                        _ ->
                            ok
                    end,
                    meck:passthrough([Q1])
            end]),
    try
        ?assertMatch({existing, _}, direct_declare(Config, QName))
    after
        ok = rabbit_ct_broker_helpers:rpc(Config, 0, meck, unload, [rabbit_policy])
    end,
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = Q}),
    rabbit_ct_client_helpers:close_channel(Ch).

amqp_v1_attach_refused_when_type_disabled(Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    Existing = <<"queue_type_enabled_SUITE.amqp_v1">>,
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{queue = Existing, durable = true}),
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, application, set_env, [rabbit, classic_queues_enabled, false]),
    {ok, Connection} = amqp10_client:open_connection(amqp_utils:connection_config(Config)),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, Refused} = amqp10_client:attach_sender_link(
                      Session, <<"refused">>, <<"/queue/🎈"/utf8>>),
    receive
        {amqp10_event,
         {link, Refused,
          {detached, #'v1_0.error'{condition = ?V_1_0_AMQP_ERROR_INTERNAL_ERROR,
                                   description = {utf8, Description}}}}} ->
            Expected = <<"cannot declare queue '🎈' in vhost '/': "
                         "queue type 'classic' is not enabled"/utf8>>,
            ?assertEqual(Expected, binary:part(Description, 0, byte_size(Expected)))
    after 30_000 ->
              ct:fail({missing_event, ?LINE})
    end,
    {ok, Accepted} = amqp10_client:attach_sender_link_sync(
                       Session, <<"accepted">>, <<"/queue/", Existing/binary>>),
    ok = amqp_utils:wait_for_credit(Accepted),
    ok = amqp10_client:detach_link(Accepted),
    ok = amqp10_client:end_session(Session),
    ok = amqp10_client:close_connection(Connection),
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, application, unset_env, [rabbit, classic_queues_enabled]),
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = Existing}),
    rabbit_ct_client_helpers:close_channel(Ch).

server_named_exclusive_queue_refused_when_classic_disabled(Config) ->
    Before = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, count, []),
    ok = set_classic_queues_enabled(Config, false),
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    ?assertExit(
       {{shutdown, {connection_closing,
                    {server_initiated_close, 541,
                     <<"INTERNAL_ERROR - cannot declare queue ", _/binary>>}}}, _},
       amqp_channel:call(Ch, #'queue.declare'{queue = <<>>, exclusive = true})),
    ?assertEqual(Before,
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, count, [])).

direct_reply_to_works_when_classic_disabled(Config) ->
    ok = set_classic_queues_enabled(Config, false),
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    {ReplyTo, CTag} = direct_reply_to_name(Ch),
    #'queue.declare_ok'{} =
        amqp_channel:call(Ch, #'queue.declare'{queue = <<"amq.rabbitmq.reply-to">>}),
    amqp_channel:cast(Ch, #'basic.publish'{routing_key = ReplyTo},
                      #amqp_msg{payload = <<"reply">>}),
    receive
        {#'basic.deliver'{consumer_tag = CTag}, #amqp_msg{payload = <<"reply">>}} -> ok
    after 30_000 ->
              ct:fail({missing_reply, ?LINE})
    end,
    rabbit_ct_client_helpers:close_channel(Ch).

amqp_v1_forged_reply_to_refused_when_classic_disabled(Config) ->
    ok = set_classic_queues_enabled(Config, false),
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    {ReplyTo, _CTag} = direct_reply_to_name(Ch),
    [Prefix, _Key] = string:split(ReplyTo, <<".">>, trailing),
    Forged = <<Prefix/binary, ".forged">>,
    {ok, Connection} = amqp10_client:open_connection(amqp_utils:connection_config(Config)),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, Genuine} = amqp10_client:attach_sender_link(
                      Session, <<"genuine">>, v1_queue_address(ReplyTo)),
    ok = amqp_utils:wait_for_credit(Genuine),
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"forged">>, v1_queue_address(Forged)),
    receive
        {amqp10_event,
         {link, Sender,
          {detached, #'v1_0.error'{condition = ?V_1_0_AMQP_ERROR_INTERNAL_ERROR,
                                   description = {utf8, Description}}}}} ->
            ?assertNotEqual(nomatch,
                            binary:match(Description,
                                         <<"queue type 'classic' is not enabled">>));
        {amqp10_event, {link, Sender, credited}} ->
            ct:fail({forged_reply_to_accepted, Forged})
    after 30_000 ->
              ct:fail({missing_event, ?LINE})
    end,
    ok = amqp10_client:end_session(Session),
    ok = amqp10_client:close_connection(Connection),
    rabbit_ct_client_helpers:close_channel(Ch).

amqp_dynamic_queue_refused_when_classic_disabled(Config) ->
    ok = set_classic_queues_enabled(Config, false),
    {ok, Connection} = amqp10_client:open_connection(amqp_utils:connection_config(Config)),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, Receiver} = amqp10_client:attach_link(
                       Session,
                       #{name => <<"dynamic">>,
                         role => {receiver, #{address => undefined,
                                              dynamic => true,
                                              capabilities => [<<"temporary-queue">>]},
                                  self()},
                         snd_settle_mode => settled,
                         rcv_settle_mode => first}),
    receive
        {amqp10_event,
         {link, Receiver,
          {detached, #'v1_0.error'{condition = ?V_1_0_AMQP_ERROR_INTERNAL_ERROR,
                                   description = {utf8, Description}}}}} ->
            ?assertNotEqual(nomatch,
                            binary:match(Description,
                                         <<"queue type 'classic' is not enabled">>))
    after 30_000 ->
              ct:fail({missing_event, ?LINE})
    end,
    ok = amqp10_client:end_session(Session),
    ok = amqp10_client:close_connection(Connection).

%%----------------------------------------------------------------------------

direct_declare(Config, QName) ->
    Q = amqqueue:new(QName, none, true, false, none,
                     [{<<"x-queue-type">>, longstr, <<"stream">>}],
                     <<"/">>, #{user => <<"acting-user">>},
                     rabbit_stream_queue),
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    rabbit_ct_broker_helpers:rpc(
      Config, 0, rabbit_queue_type, declare, [Q, Node]).

declare(Q) ->
    #'queue.declare'{queue = Q,
                     durable = true,
                     arguments = [{<<"x-queue-type">>, longstr, <<"stream">>}]}.

direct_reply_to_name(Ch) ->
    #'basic.consume_ok'{consumer_tag = CTag} =
        amqp_channel:subscribe(Ch, #'basic.consume'{queue = <<"amq.rabbitmq.reply-to">>,
                                                    no_ack = true},
                               self()),
    receive #'basic.consume_ok'{consumer_tag = CTag} -> ok end,
    Requests = <<"queue_type_enabled_SUITE.requests">>,
    #'queue.declare_ok'{} =
        amqp_channel:call(Ch, #'queue.declare'{
                                 queue = Requests,
                                 durable = true,
                                 arguments = [{<<"x-queue-type">>, longstr, <<"quorum">>}]}),
    amqp_channel:cast(Ch, #'basic.publish'{routing_key = Requests},
                      #amqp_msg{props = #'P_basic'{reply_to = <<"amq.rabbitmq.reply-to">>},
                                payload = <<"request">>}),
    {_, #amqp_msg{props = #'P_basic'{reply_to = ReplyTo}}} =
        ?awaitMatch(
           {#'basic.get_ok'{},
            #amqp_msg{props = #'P_basic'{reply_to = <<"amq.rabbitmq.reply-to.", _/binary>>}}},
           amqp_channel:call(Ch, #'basic.get'{queue = Requests, no_ack = true}),
           10_000),
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = Requests}),
    {ReplyTo, CTag}.

%% Reply-to names contain base64, which can include `/`.
v1_queue_address(Name) ->
    <<"/queue/", (binary:replace(Name, <<"/">>, <<"%2F">>, [global]))/binary>>.

set_classic_queues_enabled(Config, Value) ->
    rabbit_ct_broker_helpers:rpc(
      Config, 0, application, set_env, [rabbit, classic_queues_enabled, Value]).

set_stream_queues_enabled(Config, Value) ->
    rabbit_ct_broker_helpers:rpc(
      Config, 0, application, set_env, [rabbit, stream_queues_enabled, Value]).
