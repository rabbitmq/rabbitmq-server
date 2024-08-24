%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(management_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include_lib("rabbitmq_amqp_client.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-compile([export_all,
          nowarn_export_all]).

-import(rabbit_ct_helpers,
        [eventually/1,
         eventually/3
        ]).

-import(rabbit_ct_broker_helpers,
        [rpc/4,
         rpc/5,
         get_node_config/3
        ]).

-define(DEFAULT_EXCHANGE, <<>>).

suite() ->
    [{timetrap, {minutes, 3}}].


all() ->
    [{group, cluster_size_1},
     {group, cluster_size_3}
    ].

groups() ->
    [
     {cluster_size_1, [shuffle],
      [all_management_operations,
       queue_binding_args,
       queue_defaults,
       queue_properties,
       exchange_defaults,
       bad_uri,
       bad_queue_property,
       bad_exchange_property,
       bad_exchange_type,
       get_queue_not_found,
       declare_queue_default_queue_type,
       declare_queue_empty_name,
       declare_queue_line_feed,
       declare_queue_amq_prefix,
       declare_queue_inequivalent_fields,
       declare_queue_inequivalent_exclusive,
       declare_queue_invalid_field,
       declare_queue_invalid_arg,
       declare_default_exchange,
       declare_exchange_amq_prefix,
       declare_exchange_line_feed,
       declare_exchange_inequivalent_fields,
       delete_default_exchange,
       delete_exchange_amq_prefix,
       delete_exchange_carriage_return,
       bind_source_default_exchange,
       bind_destination_default_exchange,
       bind_source_line_feed,
       bind_destination_line_feed,
       bind_missing_queue,
       unbind_bad_binding_path_segment,
       exclusive_queue,
       purge_stream,
       pipeline,
       multiple_link_pairs,
       link_attach_order,
       drain,
       session_flow_control
      ]},
     {cluster_size_3, [shuffle],
      [classic_queue_stopped,
       queue_topology
      ]}
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(amqp10_client),
    rabbit_ct_helpers:log_environment(),
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(Group, Config) ->
    Nodes = case Group of
                cluster_size_1 -> 1;
                cluster_size_3 -> 3
            end,
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(
                Config, [{rmq_nodes_count, Nodes},
                         {rmq_nodename_suffix, Suffix}]),
    rabbit_ct_helpers:run_setup_steps(
      Config1,
      rabbit_ct_broker_helpers:setup_steps()).

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    %% Ensure that all queues were cleaned up
    eventually(?_assertEqual([], rpc(Config, rabbit_amqqueue, list, []))),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

all_management_operations(Config) ->
    NodeName = get_node_config(Config, 0, nodename),
    Node = atom_to_binary(NodeName),
    Init = {_, LinkPair = #link_pair{session = Session}} = init(Config),

    QName = <<"my 🐇"/utf8>>,
    QProps = #{durable => true,
               exclusive => false,
               auto_delete => false,
               arguments => #{<<"x-queue-type">> => {utf8, <<"quorum">>}}},
    {ok, QInfo} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, QProps),
    ?assertEqual(
       #{name => QName,
         vhost => <<"/">>,
         durable => true,
         exclusive => false,
         auto_delete => false,
         arguments => #{<<"x-queue-type">> => {utf8, <<"quorum">>}},
         type => <<"quorum">>,
         message_count => 0,
         consumer_count => 0,
         leader => Node,
         replicas => [Node]},
       QInfo),

    %% This operation should be idempotent.
    %% Also, exactly the same queue infos should be returned.
    ?assertEqual({ok, QInfo},
                 rabbitmq_amqp_client:declare_queue(LinkPair, QName, QProps)),

    %% get_queue/2 should also return the exact the same queue infos.
    ?assertEqual({ok, QInfo},
                 rabbitmq_amqp_client:get_queue(LinkPair, QName)),

    [Q] = rpc(Config, rabbit_amqqueue, list, []),
    ?assert(rpc(Config, amqqueue, is_durable, [Q])),
    ?assertNot(rpc(Config, amqqueue, is_exclusive, [Q])),
    ?assertNot(rpc(Config, amqqueue, is_auto_delete, [Q])),
    ?assertEqual(rabbit_quorum_queue, rpc(Config, amqqueue, get_type, [Q])),

    TargetAddr1 = <<"/amq/queue/", QName/binary>>,
    {ok, Sender1} = amqp10_client:attach_sender_link(Session, <<"sender 1">>, TargetAddr1),
    ok = wait_for_credit(Sender1),
    flush(credited),
    DTag1 = <<"tag 1">>,
    Msg1 = amqp10_msg:new(DTag1, <<"m1">>, false),
    ok = amqp10_client:send_msg(Sender1, Msg1),
    ok = wait_for_accepted(DTag1),

    RoutingKey1 = BindingKey1 = <<"🗝️ 1"/utf8>>,
    SourceExchange = <<"amq.direct">>,
    ?assertEqual(ok, rabbitmq_amqp_client:bind_queue(LinkPair, QName, SourceExchange, BindingKey1, #{})),
    %% This operation should be idempotent.
    ?assertEqual(ok, rabbitmq_amqp_client:bind_queue(LinkPair, QName, SourceExchange, BindingKey1, #{})),
    TargetAddr2 = <<"/exchange/", SourceExchange/binary, "/", RoutingKey1/binary>>,

    {ok, Sender2} = amqp10_client:attach_sender_link(Session, <<"sender 2">>, TargetAddr2),
    ok = wait_for_credit(Sender2),
    flush(credited),
    DTag2 = <<"tag 2">>,
    Msg2 = amqp10_msg:new(DTag2, <<"m2">>, false),
    ok = amqp10_client:send_msg(Sender2, Msg2),
    ok = wait_for_accepted(DTag2),

    ?assertEqual(ok, rabbitmq_amqp_client:unbind_queue(LinkPair, QName, SourceExchange, BindingKey1, #{})),
    ?assertEqual(ok, rabbitmq_amqp_client:unbind_queue(LinkPair, QName, SourceExchange, BindingKey1, #{})),
    DTag3 = <<"tag 3">>,
    ok = amqp10_client:send_msg(Sender2, amqp10_msg:new(DTag3, <<"not routed">>, false)),
    ok = wait_for_settlement(DTag3, released),

    XName = <<"my fanout exchange 🥳"/utf8>>,
    XProps = #{type => <<"fanout">>,
               durable => false,
               auto_delete => true,
               internal => false,
               arguments => #{<<"x-📥"/utf8>> => {utf8, <<"📮"/utf8>>}}},
    ?assertEqual(ok, rabbitmq_amqp_client:declare_exchange(LinkPair, XName, XProps)),
    ?assertEqual(ok, rabbitmq_amqp_client:declare_exchange(LinkPair, XName, XProps)),

    {ok, Exchange} = rpc(Config, rabbit_exchange, lookup, [rabbit_misc:r(<<"/">>, exchange, XName)]),
    ?assertMatch(#exchange{type = fanout,
                           durable = false,
                           auto_delete = true,
                           internal = false,
                           arguments = [{<<"x-📥"/utf8>>, longstr, <<"📮"/utf8>>}]},
                 Exchange),

    TargetAddr3 = <<"/exchange/", XName/binary>>,
    SourceExchange = <<"amq.direct">>,
    ?assertEqual(ok, rabbitmq_amqp_client:bind_queue(LinkPair, QName, XName, <<"ignored">>, #{})),
    ?assertEqual(ok, rabbitmq_amqp_client:bind_queue(LinkPair, QName, XName, <<"ignored">>, #{})),

    {ok, Sender3} = amqp10_client:attach_sender_link(Session, <<"sender 3">>, TargetAddr3),
    ok = wait_for_credit(Sender3),
    flush(credited),
    DTag4 = <<"tag 4">>,
    Msg3 = amqp10_msg:new(DTag4, <<"m3">>, false),
    ok = amqp10_client:send_msg(Sender3, Msg3),
    ok = wait_for_accepted(DTag4),

    RoutingKey2 = BindingKey2 = <<"key 2">>,
    BindingArgs = #{<<" 😬 "/utf8>> => {utf8, <<" 😬 "/utf8>>}},
    ?assertEqual(ok, rabbitmq_amqp_client:bind_exchange(LinkPair, XName, SourceExchange, BindingKey2, BindingArgs)),
    ?assertEqual(ok, rabbitmq_amqp_client:bind_exchange(LinkPair, XName, SourceExchange, BindingKey2, BindingArgs)),
    TargetAddr4 = <<"/exchange/", SourceExchange/binary, "/", RoutingKey2/binary>>,

    {ok, Sender4} = amqp10_client:attach_sender_link(Session, <<"sender 4">>, TargetAddr4),
    ok = wait_for_credit(Sender4),
    flush(credited),
    DTag5 = <<"tag 5">>,
    Msg4 = amqp10_msg:new(DTag5, <<"m4">>, false),
    ok = amqp10_client:send_msg(Sender4, Msg4),
    ok = wait_for_accepted(DTag5),

    ?assertEqual(ok, rabbitmq_amqp_client:unbind_exchange(LinkPair, XName, SourceExchange, BindingKey2, BindingArgs)),
    ?assertEqual(ok, rabbitmq_amqp_client:unbind_exchange(LinkPair, XName, SourceExchange, BindingKey2, BindingArgs)),
    DTag6 = <<"tag 6">>,
    ok = amqp10_client:send_msg(Sender4, amqp10_msg:new(DTag6, <<"not routed">>, false)),
    ok = wait_for_settlement(DTag6, released),

    ?assertEqual(ok, rabbitmq_amqp_client:delete_exchange(LinkPair, XName)),
    ?assertEqual(ok, rabbitmq_amqp_client:delete_exchange(LinkPair, XName)),
    %% When we publish the next message, we expect:
    %% 1. that the message is released because the exchange doesn't exist anymore, and
    DTag7 = <<"tag 7">>,
    ok = amqp10_client:send_msg(Sender3, amqp10_msg:new(DTag7, <<"not routed">>, false)),
    ok = wait_for_settlement(DTag7, released),
    %% 2. that the server closes the link, i.e. sends us a DETACH frame.
    receive {amqp10_event,
             {link, Sender3,
              {detached, #'v1_0.error'{condition = ?V_1_0_AMQP_ERROR_NOT_FOUND}}}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,

    ?assertEqual({ok, #{message_count => 4}},
                 rabbitmq_amqp_client:purge_queue(LinkPair, QName)),

    ?assertEqual({ok, #{message_count => 0}},
                 rabbitmq_amqp_client:delete_queue(LinkPair, QName)),
    ?assertEqual({ok, #{message_count => 0}},
                 rabbitmq_amqp_client:delete_queue(LinkPair, QName)),

    ok = cleanup(Init).

queue_defaults(Config) ->
    Init = {_, LinkPair} = init(Config),
    QName = atom_to_binary(?FUNCTION_NAME),
    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{}),
    {ok, Q} = rpc(Config, rabbit_amqqueue, lookup, [QName, <<"/">>]),
    ?assert(rpc(Config, amqqueue, is_durable, [Q])),
    ?assertNot(rpc(Config, amqqueue, is_exclusive, [Q])),
    ?assertNot(rpc(Config, amqqueue, is_auto_delete, [Q])),

    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = cleanup(Init).

queue_properties(Config) ->
    Init = {_, LinkPair} = init(Config),
    QName = atom_to_binary(?FUNCTION_NAME),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{durable => false,
                                                                    exclusive => true,
                                                                    auto_delete => true}),
    [Q] = rpc(Config, rabbit_amqqueue, list, []),
    ?assertNot(rpc(Config, amqqueue, is_durable, [Q])),
    ?assert(rpc(Config, amqqueue, is_exclusive, [Q])),
    ?assert(rpc(Config, amqqueue, is_auto_delete, [Q])),

    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = cleanup(Init).

exchange_defaults(Config) ->
    Init = {_, LinkPair} = init(Config),
    XName = atom_to_binary(?FUNCTION_NAME),
    ok = rabbitmq_amqp_client:declare_exchange(LinkPair, XName, #{}),
    {ok, Exchange} = rpc(Config, rabbit_exchange, lookup, [rabbit_misc:r(<<"/">>, exchange, XName)]),
    ?assertMatch(#exchange{type = direct,
                           durable = true,
                           auto_delete = false,
                           internal = false,
                           arguments = []},
                 Exchange),

    ok = rabbitmq_amqp_client:delete_exchange(LinkPair, XName),
    ok = cleanup(Init).

queue_binding_args(Config) ->
    Init = {_, LinkPair = #link_pair{session = Session}} = init(Config),
    QName = <<"my queue ~!@#$%^&*()_+🙈`-=[]\;',./"/utf8>>,
    Q = #{durable => false,
          exclusive => true,
          auto_delete => false,
          arguments => #{<<"x-queue-type">> => {utf8, <<"classic">>}}},
    {ok, #{}} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, Q),

    Exchange = <<"amq.headers">>,
    BindingKey = <<>>,
    BindingArgs = #{<<"key 1">> => {utf8, <<"👏"/utf8>>},
                    <<"key 2">> => {uint, 3},
                    <<"key 3">> => true,
                    <<"x-match">> => {utf8, <<"all">>}},
    ?assertEqual(ok, rabbitmq_amqp_client:bind_queue(LinkPair, QName, Exchange, BindingKey, BindingArgs)),

    TargetAddr = <<"/exchange/amq.headers">>,
    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, TargetAddr),
    ok = wait_for_credit(Sender),
    flush(credited),
    DTag1 = <<"tag 1">>,
    Msg1 = amqp10_msg:new(DTag1, <<"m1">>, false),
    AppProps = #{<<"key 1">> => <<"👏"/utf8>>,
                 <<"key 2">> => 3,
                 <<"key 3">> => true},
    ok = amqp10_client:send_msg(Sender, amqp10_msg:set_application_properties(AppProps, Msg1)),
    ok = wait_for_accepted(DTag1),

    DTag2 = <<"tag 2">>,
    Msg2 = amqp10_msg:new(DTag2, <<"m2">>, false),
    ok = amqp10_client:send_msg(Sender,
                                amqp10_msg:set_application_properties(
                                  maps:remove(<<"key 2">>, AppProps),
                                  Msg2)),
    ok = wait_for_settlement(DTag2, released),

    ?assertEqual(ok, rabbitmq_amqp_client:unbind_queue(LinkPair, QName, Exchange, BindingKey, BindingArgs)),

    DTag3 = <<"tag 3">>,
    Msg3 = amqp10_msg:new(DTag3, <<"m3">>, false),
    ok = amqp10_client:send_msg(Sender, amqp10_msg:set_application_properties(AppProps, Msg3)),
    ok = wait_for_settlement(DTag3, released),

    ?assertEqual({ok, #{message_count => 1}},
                 rabbitmq_amqp_client:delete_queue(LinkPair, QName)),

    ok = amqp10_client:detach_link(Sender),
    ok = cleanup(Init).

bad_uri(Config) ->
    Init = {_, #link_pair{outgoing_link = OutgoingLink,
                          incoming_link = IncomingLink}} = init(Config),
    BadUri = <<"👎"/utf8>>,
    Correlation = <<1, 2, 3>>,
    Properties = #{subject => <<"GET">>,
                   to => BadUri,
                   message_id => {binary, Correlation},
                   reply_to => <<"$me">>},
    Body = null,
    Request0 = amqp10_msg:new(<<>>, #'v1_0.amqp_value'{content = Body}, true),
    Request =  amqp10_msg:set_properties(Properties, Request0),
    ok = amqp10_client:flow_link_credit(IncomingLink, 1, never),
    ok =  amqp10_client:send_msg(OutgoingLink, Request),

    receive {amqp10_msg, IncomingLink, Response} ->
                ?assertEqual(
                   #{subject => <<"400">>,
                     correlation_id => Correlation},
                   amqp10_msg:properties(Response)),
                ?assertEqual(
                   #'v1_0.amqp_value'{content = {utf8, <<"failed to normalize URI '👎': invalid_uri \"👎\""/utf8>>}},
                   amqp10_msg:body(Response))
    after 5000 -> ct:fail({missing_message, ?LINE})
    end,
    ok = cleanup(Init).

bad_queue_property(Config) ->
    bad_property(<<"queue">>, Config).

bad_exchange_property(Config) ->
    bad_property(<<"exchange">>, Config).

bad_property(Kind, Config) ->
    Init = {_, #link_pair{outgoing_link = OutgoingLink,
                          incoming_link = IncomingLink}} = init(Config),
    Correlation = <<1>>,
    Properties = #{subject => <<"PUT">>,
                   to => <<$/, Kind/binary, "s/my-object">>,
                   message_id => {binary, Correlation},
                   reply_to => <<"$me">>},
    Body = {map, [{{utf8, <<"unknown">>}, {utf8, <<"bla">>}}]},
    Request0 = amqp10_msg:new(<<>>, #'v1_0.amqp_value'{content = Body}, true),
    Request =  amqp10_msg:set_properties(Properties, Request0),
    ok = amqp10_client:flow_link_credit(IncomingLink, 1, never),
    ok =  amqp10_client:send_msg(OutgoingLink, Request),

    receive {amqp10_msg, IncomingLink, Response} ->
                ?assertEqual(
                   #{subject => <<"400">>,
                     correlation_id => Correlation},
                   amqp10_msg:properties(Response)),
                ?assertEqual(
                   #'v1_0.amqp_value'{
                      content = {utf8, <<"bad ", Kind/binary, " property {{utf8,<<\"unknown\">>},{utf8,<<\"bla\">>}}">>}},
                   amqp10_msg:body(Response))
    after 5000 -> ct:fail({missing_message, ?LINE})
    end,
    ok = cleanup(Init).

bad_exchange_type(Config) ->
    Init = {_, LinkPair} = init(Config),
    UnknownXType = <<"🤷"/utf8>>,
    {error, Resp} = rabbitmq_amqp_client:declare_exchange(LinkPair, <<"e1">>, #{type => UnknownXType}),
    ?assertMatch(#{subject := <<"400">>}, amqp10_msg:properties(Resp)),
    ?assertEqual(#'v1_0.amqp_value'{content = {utf8, <<"unknown exchange type '", UnknownXType/binary, "'">>}},
                 amqp10_msg:body(Resp)),
    ok = cleanup(Init).

get_queue_not_found(Config) ->
    Init = {_, LinkPair} = init(Config),
    QName = <<"🤷"/utf8>>,
    {error, Resp} = rabbitmq_amqp_client:get_queue(LinkPair, QName),
    ?assertMatch(#{subject := <<"404">>}, amqp10_msg:properties(Resp)),
    ?assertEqual(#'v1_0.amqp_value'{content = {utf8, <<"queue '", QName/binary, "' in vhost '/' not found">>}},
                 amqp10_msg:body(Resp)),
    ok = cleanup(Init).

declare_queue_default_queue_type(Config) ->
    Node = get_node_config(Config, 0, nodename),
    Vhost = QName = atom_to_binary(?FUNCTION_NAME),
    ok = erpc:call(Node, rabbit_vhost, add,
                   [Vhost,
                    #{default_queue_type => <<"quorum">>},
                    <<"acting-user">>]),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, <<"guest">>, Vhost),
    OpnConf = connection_config(Config, 0, Vhost),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, LinkPair} = rabbitmq_amqp_client:attach_management_link_pair_sync(Session, <<"my link pair">>),

    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ?assertMatch({ok, #{type := <<"quorum">>}},
                 rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{})),

    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair),
    ok = amqp10_client:end_session(Session),
    ok = amqp10_client:close_connection(Connection),
    ok = rabbit_ct_broker_helpers:delete_vhost(Config, Vhost).

declare_queue_empty_name(Config) ->
    Init = {_, LinkPair} = init(Config),
    QName = <<"">>,
    {error, Resp} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{}),
    ?assertMatch(#{subject := <<"400">>}, amqp10_msg:properties(Resp)),
    ?assertEqual(#'v1_0.amqp_value'{content = {utf8, <<"declare queue with empty name not allowed">>}},
                 amqp10_msg:body(Resp)),
    ok = cleanup(Init).

declare_queue_line_feed(Config) ->
    Init = {_, LinkPair} = init(Config),
    QName = <<"🤠\n😱"/utf8>>,
    {error, Resp} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{}),
    ?assertMatch(#{subject := <<"400">>}, amqp10_msg:properties(Resp)),
    ?assertEqual(#'v1_0.amqp_value'{
                    content = {utf8, <<"Bad name '", QName/binary,
                                       "': line feed and carriage return characters not allowed">>}},
                 amqp10_msg:body(Resp)),
    ok = cleanup(Init).

declare_queue_amq_prefix(Config) ->
    Init = {_, LinkPair} = init(Config),
    QName = <<"amq.🎇"/utf8>>,
    {error, Resp} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{}),
    ?assertMatch(#{subject := <<"403">>}, amqp10_msg:properties(Resp)),
    ?assertEqual(#'v1_0.amqp_value'{
                    content = {utf8, <<"queue '", QName/binary, "' in vhost '/' "
                                       "starts with reserved prefix 'amq.'">>}},
                 amqp10_msg:body(Resp)),
    ok = cleanup(Init).

declare_queue_inequivalent_fields(Config) ->
    Init = {_, LinkPair} = init(Config),
    QName = <<"👌"/utf8>>,
    {ok, #{auto_delete := false}} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{auto_delete => false}),
    {error, Resp} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{auto_delete => true}),
    ?assertMatch(#{subject := <<"409">>}, amqp10_msg:properties(Resp)),
    ?assertEqual(#'v1_0.amqp_value'{
                    content = {utf8, <<"inequivalent arg 'auto_delete' for queue '", QName/binary,
                                       "' in vhost '/': received 'true' but current is 'false'">>}},
                 amqp10_msg:body(Resp)),
    {ok, #{}} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = cleanup(Init).

declare_queue_inequivalent_exclusive(Config) ->
    Init = {_, LinkPair} = init(Config),
    QName = <<"👌"/utf8>>,
    {ok, #{exclusive := true}} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{exclusive => true}),
    {error, Resp} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{exclusive => false}),
    ?assertMatch(#{subject := <<"400">>}, amqp10_msg:properties(Resp)),
    ?assertEqual(
       #'v1_0.amqp_value'{
          content = {utf8,
                     <<"cannot obtain exclusive access to locked queue '", QName/binary, "' in vhost '/'. ",
                       "It could be originally declared on another connection or the exclusive property ",
                       "value does not match that of the original declaration.">>}},
       amqp10_msg:body(Resp)),
    ok = cleanup(Init).

declare_queue_invalid_field(Config) ->
    Init = {_, LinkPair} = init(Config),
    QName = <<"👌"/utf8>>,
    QProps = #{auto_delete => true,
               arguments => #{<<"x-queue-type">> => {utf8, <<"stream">>}}},
    {error, Resp} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, QProps),
    ?assertMatch(#{subject := <<"400">>}, amqp10_msg:properties(Resp)),
    ?assertEqual(
       #'v1_0.amqp_value'{
          content = {utf8, <<"invalid property 'auto-delete' for queue '", QName/binary, "' in vhost '/'">>}},
       amqp10_msg:body(Resp)),
    ok = cleanup(Init).

declare_queue_invalid_arg(Config) ->
    Init = {_, LinkPair} = init(Config),
    QName = <<"👌"/utf8>>,
    QProps = #{arguments => #{<<"x-queue-type">> => {utf8, <<"stream">>},
                              <<"x-dead-letter-exchange">> => {utf8, <<"dlx is invalid for stream">>}}},
    {error, Resp} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, QProps),
    ?assertMatch(#{subject := <<"409">>}, amqp10_msg:properties(Resp)),
    ?assertEqual(
       #'v1_0.amqp_value'{
          content = {utf8, <<"invalid arg 'x-dead-letter-exchange' for queue '", QName/binary,
                             "' in vhost '/' of queue type rabbit_stream_queue">>}},
       amqp10_msg:body(Resp)),
    ok = cleanup(Init).

declare_default_exchange(Config) ->
    Init = {_, LinkPair} = init(Config),
    {error, Resp} = rabbitmq_amqp_client:declare_exchange(LinkPair, ?DEFAULT_EXCHANGE, #{}),
    ?assertMatch(#{subject := <<"403">>}, amqp10_msg:properties(Resp)),
    ?assertEqual(#'v1_0.amqp_value'{content = {utf8, <<"operation not permitted on the default exchange">>}},
                 amqp10_msg:body(Resp)),
    ok = cleanup(Init).

declare_exchange_amq_prefix(Config) ->
    Init = {_, LinkPair} = init(Config),
    XName = <<"amq.🎇"/utf8>>,
    {error, Resp} = rabbitmq_amqp_client:declare_exchange(LinkPair, XName, #{}),
    ?assertMatch(#{subject := <<"403">>}, amqp10_msg:properties(Resp)),
    ?assertEqual(#'v1_0.amqp_value'{
                    content = {utf8, <<"exchange '", XName/binary, "' in vhost '/' "
                                       "starts with reserved prefix 'amq.'">>}},
                 amqp10_msg:body(Resp)),
    ok = cleanup(Init).

declare_exchange_line_feed(Config) ->
    Init = {_, LinkPair} = init(Config),
    XName = <<"🤠\n😱"/utf8>>,
    {error, Resp} = rabbitmq_amqp_client:declare_exchange(LinkPair, XName, #{}),
    ?assertMatch(#{subject := <<"400">>}, amqp10_msg:properties(Resp)),
    ?assertEqual(#'v1_0.amqp_value'{
                    content = {utf8, <<"Bad name '", XName/binary,
                                       "': line feed and carriage return characters not allowed">>}},
                 amqp10_msg:body(Resp)),
    ok = cleanup(Init).

declare_exchange_inequivalent_fields(Config) ->
    Init = {_, LinkPair} = init(Config),
    XName = <<"👌"/utf8>>,
    ok = rabbitmq_amqp_client:declare_exchange(LinkPair, XName, #{type => <<"direct">>}),
    {error, Resp} = rabbitmq_amqp_client:declare_exchange(LinkPair, XName, #{type => <<"fanout">>}),
    ?assertMatch(#{subject := <<"409">>}, amqp10_msg:properties(Resp)),
    ?assertEqual(#'v1_0.amqp_value'{
                    content = {utf8, <<"inequivalent arg 'type' for exchange '", XName/binary,
                                       "' in vhost '/': received 'fanout' but current is 'direct'">>}},
                 amqp10_msg:body(Resp)),
    ok = cleanup(Init).

classic_queue_stopped(Config) ->
    Init2 = {_, LinkPair2} = init(Config, 2),
    QName = <<"👌"/utf8>>,
    {ok, #{durable := true,
           type := <<"classic">>}} = rabbitmq_amqp_client:declare_queue(LinkPair2, QName, #{}),
    ok = cleanup(Init2),
    ok = rabbit_ct_broker_helpers:stop_node(Config, 2),
    %% Classic queue is now stopped.

    Init0 = {_, LinkPair0} = init(Config),
    {error, Resp0} = rabbitmq_amqp_client:declare_queue(LinkPair0, QName, #{}),
    ?assertMatch(#{subject := <<"400">>}, amqp10_msg:properties(Resp0)),
    ExpectedResponseBody = #'v1_0.amqp_value'{
                              content = {utf8, <<"queue '", QName/binary,
                                                 "' in vhost '/' process is stopped by supervisor">>}},
    ?assertEqual(ExpectedResponseBody,
                 amqp10_msg:body(Resp0)),

    {error, Resp1} = rabbitmq_amqp_client:get_queue(LinkPair0, QName),
    ?assertMatch(#{subject := <<"400">>}, amqp10_msg:properties(Resp1)),
    ?assertEqual(ExpectedResponseBody,
                 amqp10_msg:body(Resp1)),

    ok = rabbit_ct_broker_helpers:start_node(Config, 2),
    {ok, #{}} = rabbitmq_amqp_client:delete_queue(LinkPair0, QName),
    ok = cleanup(Init0).

delete_default_exchange(Config) ->
    Init = {_, LinkPair} = init(Config),
    {error, Resp} = rabbitmq_amqp_client:delete_exchange(LinkPair, ?DEFAULT_EXCHANGE),
    ?assertMatch(#{subject := <<"403">>}, amqp10_msg:properties(Resp)),
    ?assertEqual(#'v1_0.amqp_value'{content = {utf8, <<"operation not permitted on the default exchange">>}},
                 amqp10_msg:body(Resp)),
    ok = cleanup(Init).

delete_exchange_amq_prefix(Config) ->
    Init = {_, LinkPair} = init(Config),
    XName = <<"amq.fanout">>,
    {error, Resp} = rabbitmq_amqp_client:delete_exchange(LinkPair, XName),
    ?assertMatch(#{subject := <<"403">>}, amqp10_msg:properties(Resp)),
    ?assertEqual(#'v1_0.amqp_value'{
                    content = {utf8, <<"exchange '", XName/binary, "' in vhost '/' "
                                       "starts with reserved prefix 'amq.'">>}},
                 amqp10_msg:body(Resp)),
    ok = cleanup(Init).

delete_exchange_carriage_return(Config) ->
    Init = {_, LinkPair} = init(Config),
    XName = <<"x\rx">>,
    {error, Resp} = rabbitmq_amqp_client:delete_exchange(LinkPair, XName),
    ?assertMatch(#{subject := <<"400">>}, amqp10_msg:properties(Resp)),
    ?assertEqual(#'v1_0.amqp_value'{
                    content = {utf8, <<"Bad name '", XName/binary,
                                       "': line feed and carriage return characters not allowed">>}},
                 amqp10_msg:body(Resp)),
    ok = cleanup(Init).

bind_source_default_exchange(Config) ->
    Init = {_, LinkPair} = init(Config),
    QName = <<"👀"/utf8>>,
    {ok, #{}} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{}),

    {error, Resp} = rabbitmq_amqp_client:bind_queue(
                      LinkPair, QName, ?DEFAULT_EXCHANGE, <<"my binding key">>, #{}),
    ?assertMatch(#{subject := <<"403">>}, amqp10_msg:properties(Resp)),
    ?assertEqual(#'v1_0.amqp_value'{content = {utf8, <<"operation not permitted on the default exchange">>}},
                 amqp10_msg:body(Resp)),

    {ok, #{}} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = cleanup(Init).

bind_destination_default_exchange(Config) ->
    Init = {_, LinkPair} = init(Config),
    {error, Resp} = rabbitmq_amqp_client:bind_exchange(
                      LinkPair, ?DEFAULT_EXCHANGE, <<"amq.fanout">>, <<"my binding key">>, #{}),
    ?assertMatch(#{subject := <<"403">>}, amqp10_msg:properties(Resp)),
    ?assertEqual(#'v1_0.amqp_value'{content = {utf8, <<"operation not permitted on the default exchange">>}},
                 amqp10_msg:body(Resp)),
    ok = cleanup(Init).

bind_source_line_feed(Config) ->
    Init = {_, LinkPair} = init(Config),
    XName = <<"🤠\n😱"/utf8>>,
    {error, Resp} = rabbitmq_amqp_client:bind_exchange(
                      LinkPair, <<"amq.fanout">>, XName, <<"my binding key">>, #{}),
    ?assertMatch(#{subject := <<"400">>}, amqp10_msg:properties(Resp)),
    ?assertEqual(#'v1_0.amqp_value'{
                    content = {utf8, <<"Bad name '", XName/binary,
                                       "': line feed and carriage return characters not allowed">>}},
                 amqp10_msg:body(Resp)),
    ok = cleanup(Init).

bind_destination_line_feed(Config) ->
    Init = {_, LinkPair} = init(Config),
    XName = <<"🤠\n😱"/utf8>>,
    {error, Resp} = rabbitmq_amqp_client:bind_exchange(
                      LinkPair, XName, <<"amq.fanout">>, <<"my binding key">>, #{}),
    ?assertMatch(#{subject := <<"400">>}, amqp10_msg:properties(Resp)),
    ?assertEqual(#'v1_0.amqp_value'{
                    content = {utf8, <<"Bad name '", XName/binary,
                                       "': line feed and carriage return characters not allowed">>}},
                 amqp10_msg:body(Resp)),
    ok = cleanup(Init).

bind_missing_queue(Config) ->
    Init = {_, LinkPair} = init(Config),
    QName = <<"👀"/utf8>>,
    {error, Resp} = rabbitmq_amqp_client:bind_queue(
                      LinkPair, QName, <<"amq.direct">>, <<"my binding key">>, #{}),
    ?assertMatch(#{subject := <<"400">>}, amqp10_msg:properties(Resp)),
    ?assertEqual(#'v1_0.amqp_value'{content = {utf8, <<"no queue '", QName/binary, "' in vhost '/'">>}},
                 amqp10_msg:body(Resp)),
    ok = cleanup(Init).

unbind_bad_binding_path_segment(Config) ->
    Init = {_, #link_pair{outgoing_link = OutgoingLink,
                          incoming_link = IncomingLink}} = init(Config),
    Correlation = <<1>>,
    BadBindingPathSegment = <<"src=e1;dstq=q1;invalidkey=k1;args=">>,
    Properties = #{subject => <<"DELETE">>,
                   to => <<"/bindings/", BadBindingPathSegment/binary>>,
                   message_id => {binary, Correlation},
                   reply_to => <<"$me">>},
    Request0 = amqp10_msg:new(<<>>, #'v1_0.amqp_value'{content = null}, true),
    Request =  amqp10_msg:set_properties(Properties, Request0),
    ok = amqp10_client:flow_link_credit(IncomingLink, 1, never),
    ok =  amqp10_client:send_msg(OutgoingLink, Request),
    receive {amqp10_msg, IncomingLink, Response} ->
                ?assertEqual(
                   #{subject => <<"400">>,
                     correlation_id => Correlation},
                   amqp10_msg:properties(Response)),
                ?assertEqual(
                   #'v1_0.amqp_value'{
                      content = {utf8, <<"bad binding path segment '",
                                         BadBindingPathSegment/binary, "'">>}},
                   amqp10_msg:body(Response))
    after 5000 -> ct:fail({missing_message, ?LINE})
    end,
    ok = cleanup(Init).

exclusive_queue(Config) ->
    Init1 = {_, LinkPair1} = init(Config),
    BindingKey = <<"🗝️"/utf8>>,
    XName = <<"amq.direct">>,
    QName = <<"🙌"/utf8>>,
    QProps = #{exclusive => true},
    {ok, #{}} = rabbitmq_amqp_client:declare_queue(LinkPair1, QName, QProps),
    ok = rabbitmq_amqp_client:bind_queue(LinkPair1, QName, XName, BindingKey, #{}),

    {Conn2, LinkPair2} = init(Config),
    {error, Resp1} = rabbitmq_amqp_client:bind_queue(LinkPair2, QName, XName, BindingKey, #{}),
    ?assertMatch(#{subject := <<"400">>}, amqp10_msg:properties(Resp1)),
    Body = #'v1_0.amqp_value'{content = {utf8, Reason}} = amqp10_msg:body(Resp1),
    ?assertMatch(<<"cannot obtain exclusive access to locked queue '",
                   QName:(byte_size(QName))/binary, "' in vhost '/'.", _/binary >>,
                 Reason),
    ok = amqp10_client:close_connection(Conn2),

    {Conn3, LinkPair3} = init(Config),
    {error, Resp2} = rabbitmq_amqp_client:delete_queue(LinkPair3, QName),
    ?assertMatch(#{subject := <<"400">>}, amqp10_msg:properties(Resp2)),
    %% We expect the same error message as previously.
    ?assertEqual(Body, amqp10_msg:body(Resp2)),
    ok = amqp10_client:close_connection(Conn3),

    {Conn4, LinkPair4} = init(Config),
    {error, Resp3} = rabbitmq_amqp_client:purge_queue(LinkPair4, QName),
    ?assertMatch(#{subject := <<"400">>}, amqp10_msg:properties(Resp3)),
    %% We expect the same error message as previously.
    ?assertEqual(Body, amqp10_msg:body(Resp3)),
    ok = amqp10_client:close_connection(Conn4),

    ok = rabbitmq_amqp_client:unbind_queue(LinkPair1, QName, XName, BindingKey, #{}),
    {ok, #{}} = rabbitmq_amqp_client:delete_queue(LinkPair1, QName),
    ok = cleanup(Init1).

purge_stream(Config) ->
    Init = {_, LinkPair} = init(Config),
    QName = <<"🚀"/utf8>>,
    QProps = #{arguments => #{<<"x-queue-type">> => {utf8, <<"stream">>}}},
    {ok, #{}} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, QProps),

    {error, Resp} = rabbitmq_amqp_client:purge_queue(LinkPair, QName),
    ?assertMatch(#{subject := <<"400">>}, amqp10_msg:properties(Resp)),
    #'v1_0.amqp_value'{content = {utf8, Reason}} = amqp10_msg:body(Resp),
    ?assertEqual(<<"purge not supported by queue '", QName/binary, "' in vhost '/'">>,
                 Reason),

    {ok, #{}} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = cleanup(Init).

queue_topology(Config) ->
    NodeNames = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Nodes = [N0, N1, N2] = lists:map(fun erlang:atom_to_binary/1, NodeNames),
    Init0 = {_, LinkPair0} = init(Config, 0),

    CQName = <<"my classic queue">>,
    QQName = <<"my quorum queue">>,
    SQName = <<"my stream queue">>,

    CQProps = #{arguments => #{<<"x-queue-type">> => {utf8, <<"classic">>}}},
    QQProps = #{arguments => #{<<"x-queue-type">> => {utf8, <<"quorum">>}}},
    SQProps = #{arguments => #{<<"x-queue-type">> => {utf8, <<"stream">>}}},

    {ok, CQInfo0} = rabbitmq_amqp_client:declare_queue(LinkPair0, CQName, CQProps),
    {ok, QQInfo0} = rabbitmq_amqp_client:declare_queue(LinkPair0, QQName, QQProps),
    {ok, SQInfo0} = rabbitmq_amqp_client:declare_queue(LinkPair0, SQName, SQProps),

    %% The default queue leader strategy is client-local.
    ?assertEqual({ok, N0}, maps:find(leader, CQInfo0)),
    ?assertEqual({ok, N0}, maps:find(leader, QQInfo0)),
    ?assertEqual({ok, N0}, maps:find(leader, SQInfo0)),

    ?assertEqual({ok, [N0]}, maps:find(replicas, CQInfo0)),
    {ok, QQReplicas0} = maps:find(replicas, QQInfo0),
    ?assertEqual(Nodes, lists:usort(QQReplicas0)),
    {ok, SQReplicas0} = maps:find(replicas, SQInfo0),
    ?assertEqual(Nodes, lists:usort(SQReplicas0)),

    ok = cleanup(Init0),
    ok = rabbit_ct_broker_helpers:stop_node(Config, 0),

    Init2 = {_, LinkPair2} = init(Config, 2),
    {ok, QQInfo2} = rabbitmq_amqp_client:get_queue(LinkPair2, QQName),
    {ok, SQInfo2} = rabbitmq_amqp_client:get_queue(LinkPair2, SQName),

    case maps:get(leader, QQInfo2) of
        N1 -> ok;
        N2 -> ok;
        Other0 -> ct:fail({?LINE, Other0})
    end,
    case maps:get(leader, SQInfo2) of
        N1 -> ok;
        N2 -> ok;
        Other1 -> ct:fail({?LINE, Other1})
    end,

    %% Replicas should include both online and offline replicas.
    {ok, QQReplicas2} = maps:find(replicas, QQInfo2),
    ?assertEqual(Nodes, lists:usort(QQReplicas2)),
    {ok, SQReplicas2} = maps:find(replicas, SQInfo2),
    ?assertEqual(Nodes, lists:usort(SQReplicas2)),

    ok = rabbit_ct_broker_helpers:start_node(Config, 0),
    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair2, CQName),
    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair2, QQName),
    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair2, SQName),
    ok = cleanup(Init2).

%% Even though RabbitMQ processes management requests synchronously (one at a time),
%% the client should be able to send multiple requests at once before receiving a response.
pipeline(Config) ->
    Init = {_, LinkPair} = init(Config),
    flush(attached),

    %% We should be able to send 8 management requests at once
    %% because RabbitMQ grants us 8 link credits initially.
    Num = 8,
    pipeline0(Num, LinkPair, <<"PUT">>, {map, []}),
    eventually(?_assertEqual(Num, rpc(Config, rabbit_amqqueue, count, [])), 200, 20),
    flush(queues_created),

    pipeline0(Num, LinkPair, <<"DELETE">>, null),
    eventually(?_assertEqual(0, rpc(Config, rabbit_amqqueue, count, [])), 200, 20),
    flush(queues_deleted),

    ok = cleanup(Init).

pipeline0(Num,
          #link_pair{outgoing_link = OutgoingLink,
                     incoming_link = IncomingLink},
          HttpMethod,
          Body) ->
    ok = amqp10_client:flow_link_credit(IncomingLink, Num, never),
    [begin
         Request0 = amqp10_msg:new(<<>>, #'v1_0.amqp_value'{content = Body}, true),
         Bin = integer_to_binary(N),
         Props = #{subject => HttpMethod,
                   to => <<"/queues/q-", Bin/binary>>,
                   message_id => {binary, Bin},
                   reply_to => <<"$me">>},
         Request =  amqp10_msg:set_properties(Props, Request0),
         ok = amqp10_client:send_msg(OutgoingLink, Request)
     end || N <- lists:seq(1, Num)].

%% RabbitMQ allows attaching multiple link pairs.
multiple_link_pairs(Config) ->
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, LinkPair1} = rabbitmq_amqp_client:attach_management_link_pair_sync(Session, <<"link pair 1">>),
    {ok, LinkPair2} = rabbitmq_amqp_client:attach_management_link_pair_sync(Session, <<"link pair 2">>),

    [SessionPid] = rpc(Config, rabbit_amqp_session, list_local, []),
    #{management_link_pairs := Pairs0,
      incoming_management_links := Incoming0,
      outgoing_management_links := Outgoing0} = gen_server_state(SessionPid),
    ?assertEqual(2, maps:size(Pairs0)),
    ?assertEqual(2, maps:size(Incoming0)),
    ?assertEqual(2, maps:size(Outgoing0)),

    QName = <<"q">>,
    {ok, #{}} = rabbitmq_amqp_client:declare_queue(LinkPair1, QName, #{}),
    {ok, #{}} = rabbitmq_amqp_client:delete_queue(LinkPair2, QName),

    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair1),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair2),

    %% Assert that the server cleaned up its state.
    #{management_link_pairs := Pairs,
      incoming_management_links := Incoming,
      outgoing_management_links := Outgoing} = gen_server_state(SessionPid),
    ?assertEqual(0, maps:size(Pairs)),
    ?assertEqual(0, maps:size(Incoming)),
    ?assertEqual(0, maps:size(Outgoing)),

    ok = amqp10_client:end_session(Session),
    ok = amqp10_client:close_connection(Connection).

%% Attaching (and detaching) either the sender or the receiver link first should both work.
link_attach_order(Config) ->
    PairName1 = <<"link pair 1">>,
    PairName2 = <<"link pair 2">>,

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),

    Terminus = #{address => <<"/management">>,
                 durable => none},
    OutgoingAttachArgs1 = #{name => PairName1,
                            role => {sender, Terminus},
                            snd_settle_mode => settled,
                            rcv_settle_mode => first,
                            properties => #{<<"paired">> => true}},
    IncomingAttachArgs1 = OutgoingAttachArgs1#{role := {receiver, Terminus, self()},
                                               filter => #{}},
    OutgoingAttachArgs2 = OutgoingAttachArgs1#{name := PairName2},
    IncomingAttachArgs2 = IncomingAttachArgs1#{name := PairName2},

    %% Attach sender before receiver.
    {ok, OutgoingRef1} = amqp10_client:attach_link(Session, OutgoingAttachArgs1),
    {ok, IncomingRef1} = amqp10_client:attach_link(Session, IncomingAttachArgs1),
    %% Attach receiver before sender.
    {ok, IncomingRef2} = amqp10_client:attach_link(Session, IncomingAttachArgs2),
    {ok, OutgoingRef2} = amqp10_client:attach_link(Session, OutgoingAttachArgs2),

    Refs = [OutgoingRef1,
            OutgoingRef2,
            IncomingRef1,
            IncomingRef2],

    [ok = wait_for_event(Ref, attached) || Ref <- Refs],
    flush(attached),

    LinkPair1 = #link_pair{session = Session,
                           outgoing_link = OutgoingRef1,
                           incoming_link = IncomingRef1},
    LinkPair2 = #link_pair{session = Session,
                           outgoing_link = OutgoingRef2,
                           incoming_link = IncomingRef2},

    QName = <<"test queue">>,
    {ok, #{}} = rabbitmq_amqp_client:declare_queue(LinkPair1, QName, #{}),
    {ok, #{}} = rabbitmq_amqp_client:delete_queue(LinkPair2, QName),

    %% Detach sender before receiver.
    ok = amqp10_client:detach_link(OutgoingRef1),
    ok = amqp10_client:detach_link(IncomingRef1),
    %% Detach receiver before sender.
    ok = amqp10_client:detach_link(IncomingRef2),
    ok = amqp10_client:detach_link(OutgoingRef2),

    [ok = wait_for_event(Ref, {detached, normal}) || Ref <- Refs],
    flush(detached),
    ok = amqp10_client:end_session(Session),
    ok = amqp10_client:close_connection(Connection).

drain(Config) ->
    {Conn, #link_pair{session = Session,
                      outgoing_link = OutgoingLink,
                      incoming_link = IncomingLink}} = init(Config),

    ok = amqp10_client:flow_link_credit(IncomingLink, 2, never),
    ok = amqp10_client:flow_link_credit(IncomingLink, 3, never, _Drain = true),
    %% After draining, link credit on our incoming link should be 0.

    Request0 = amqp10_msg:new(<<>>, #'v1_0.amqp_value'{content = null}, true),
    Props = #{subject => <<"DELETE">>,
              to => <<"/queues/q1">>,
              message_id => {binary, <<1>>},
              reply_to => <<"$me">>},
    Request =  amqp10_msg:set_properties(Props, Request0),
    ok = amqp10_client:send_msg(OutgoingLink, Request),
    receive
        {amqp10_event,
         {session, Session,
          {ended,
           #'v1_0.error'{
              condition = ?V_1_0_AMQP_ERROR_PRECONDITION_FAILED,
              description = {utf8, <<"insufficient credit (0) for management link from RabbitMQ to client">>}}}}} -> ok
    after 5000 -> flush(missing_ended),
                  ct:fail({missing_event, ?LINE})
    end,
    ok = amqp10_client:close_connection(Conn).

%% Test that RabbitMQ respects session flow control.
session_flow_control(Config) ->
    Init = {_, #link_pair{session = Session,
                          outgoing_link = OutgoingLink,
                          incoming_link = IncomingLink}} = init(Config),
    flush(attached),

    ok = amqp10_client:flow_link_credit(IncomingLink, 1, never),
    %% Close our incoming window.
    gen_statem:cast(Session, {flow_session, #'v1_0.flow'{incoming_window = {uint, 0}}}),

    Request0 = amqp10_msg:new(<<>>, #'v1_0.amqp_value'{content = null}, true),
    MessageId = <<1>>,
    Props = #{subject => <<"DELETE">>,
              to => <<"/queues/q1">>,
              message_id => {binary, MessageId},
              reply_to => <<"$me">>},
    Request =  amqp10_msg:set_properties(Props, Request0),
    ok = amqp10_client:send_msg(OutgoingLink, Request),

    receive Unexpected -> ct:fail({unexpected, Unexpected})
    after 100 -> ok
    end,

    %% Open our incoming window
    gen_statem:cast(Session, {flow_session, #'v1_0.flow'{incoming_window = {uint, 5}}}),

    receive {amqp10_msg, IncomingLink, Response} ->
                ?assertMatch(#{correlation_id := MessageId,
                               subject := <<"200">>},
                             amqp10_msg:properties(Response))
    after 5000 -> flush(missing_msg),
                  ct:fail({missing_msg, ?LINE})
    end,
    ok = cleanup(Init).

init(Config) ->
    init(Config, 0).

init(Config, Node) ->
    OpnConf = connection_config(Config, Node),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, LinkPair} = rabbitmq_amqp_client:attach_management_link_pair_sync(Session, <<"my link pair">>),
    {Connection, LinkPair}.

cleanup({Connection, LinkPair = #link_pair{session = Session}}) ->
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair),
    ok = amqp10_client:end_session(Session),
    ok = amqp10_client:close_connection(Connection).

connection_config(Config) ->
    connection_config(Config, 0).

connection_config(Config, Node) ->
    connection_config(Config, Node, <<"/">>).

connection_config(Config, Node, Vhost) ->
    Host = ?config(rmq_hostname, Config),
    Port = get_node_config(Config, Node, tcp_port_amqp),
    #{address => Host,
      port => Port,
      container_id => <<"my container">>,
      sasl => {plain, <<"guest">>, <<"guest">>},
      hostname => <<"vhost:", Vhost/binary>>}.

wait_for_credit(Sender) ->
    receive
        {amqp10_event, {link, Sender, credited}} ->
            ok
    after 5000 ->
              flush(?FUNCTION_NAME),
              ct:fail(?FUNCTION_NAME)
    end.

flush(Prefix) ->
    receive
        Msg ->
            ct:pal("~p flushed: ~p~n", [Prefix, Msg]),
            flush(Prefix)
    after 1 ->
              ok
    end.

wait_for_accepted(Tag) ->
    wait_for_settlement(Tag, accepted).

wait_for_settlement(Tag, State) ->
    receive
        {amqp10_disposition, {State, Tag}} ->
            ok
    after 5000 ->
              Reason = {?FUNCTION_NAME, Tag},
              flush(Reason),
              ct:fail(Reason)
    end.

wait_for_event(Ref, Event) ->
    receive {amqp10_event, {link, Ref, Event}} -> ok
    after 5000 -> ct:fail({missing_event, Ref, Event})
    end.

%% Return the formatted state of a gen_server via sys:get_status/1.
%% (sys:get_state/1 is unformatted)
gen_server_state(Pid) ->
    {status, _, _, L0} = sys:get_status(Pid, 20_000),
    L1 = lists:last(L0),
    {data, L2} = lists:last(L1),
    proplists:get_value("State", L2).
