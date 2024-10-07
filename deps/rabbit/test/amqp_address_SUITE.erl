%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 VMware, Inc. or its affiliates.  All rights reserved.

-module(amqp_address_SUITE).

-compile([export_all,
          nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").
-include_lib("rabbitmq_amqp_client/include/rabbitmq_amqp_client.hrl").

-import(rabbit_ct_broker_helpers,
        [rpc/4]).
-import(rabbit_ct_helpers,
        [eventually/1]).
-import(amqp_utils,
        [flush/1,
         wait_for_credit/1]).

all() ->
    [
     {group, v1_permitted},
     {group, v1_denied}
    ].

groups() ->
    [
     {v1_permitted, [shuffle],
      common_tests()
     },
     {v1_denied, [shuffle],
      [
       target_queue_absent,
       source_queue_absent,
       target_bad_v2_address,
       source_bad_v2_address
      ] ++ common_tests()
     }
    ].

common_tests() ->
    [
     target_exchange_routing_key,
     target_exchange_routing_key_with_slash,
     target_exchange_routing_key_empty,
     target_exchange,
     target_exchange_absent,
     queue,
     queue_with_slash,
     target_per_message_exchange_routing_key,
     target_per_message_exchange,
     target_per_message_queue,
     target_per_message_unset_to_address,
     target_per_message_bad_to_address,
     target_per_message_exchange_absent_settled,
     target_per_message_exchange_absent_unsettled,
     target_bad_address,
     source_bad_address
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(amqp10_client),
    rabbit_ct_helpers:log_environment(),
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(Group, Config0) ->
    PermitV1 = case Group of
                   v1_permitted -> true;
                   v1_denied -> false
               end,
    Config = rabbit_ct_helpers:merge_app_env(
               Config0,
               {rabbit,
                [{permit_deprecated_features,
                  #{amqp_address_v1 => PermitV1}
                 }]
               }),
    rabbit_ct_helpers:run_setup_steps(
      Config,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% Test v2 target address
%% /exchanges/:exchange/:routing-key
target_exchange_routing_key(Config) ->
    XName = <<"👉"/utf8>>,
    RKey = <<"🗝️"/utf8>>,
    target_exchange_routing_key0(XName, RKey, Config).

%% Test v2 target address
%% /exchanges/:exchange/:routing-key
%% where both :exchange and :routing-key contains a "/" character.
target_exchange_routing_key_with_slash(Config) ->
    XName = <<"my/exchange">>,
    RKey = <<"my/key">>,
    target_exchange_routing_key0(XName, RKey, Config).

target_exchange_routing_key0(XName, RKey, Config) ->
    TargetAddr = rabbitmq_amqp_address:exchange(XName, RKey),
    QName = atom_to_binary(?FUNCTION_NAME),

    Init = {_, LinkPair = #link_pair{session = Session}} = init(Config),
    ok = rabbitmq_amqp_client:declare_exchange(LinkPair, XName, #{}),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{}),
    ok = rabbitmq_amqp_client:bind_queue(LinkPair, QName, XName, RKey, #{}),
    SrcAddr = rabbitmq_amqp_address:queue(QName),
    {ok, Receiver} = amqp10_client:attach_receiver_link(Session, <<"receiver">>, SrcAddr),

    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, TargetAddr),
    ok = wait_for_credit(Sender),
    Body = <<"body">>,
    Msg0 = amqp10_msg:new(<<"tag">>, Body, true),
    %% Although mc_amqp:essential_properties/1 parses these annotations, they should be ignored.
    Msg1 = amqp10_msg:set_message_annotations(
             #{<<"x-exchange">> => <<"ignored">>,
               <<"x-routing-key">> => <<"ignored">>},
             Msg0),
    ok = amqp10_client:send_msg(Sender, Msg1),

    {ok, Msg} = amqp10_client:get_msg(Receiver),
    ?assertEqual([Body], amqp10_msg:body(Msg)),

    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = rabbitmq_amqp_client:delete_exchange(LinkPair, XName),
    ok = cleanup(Init).

%% Test v2 target address
%% /exchanges/:exchange/
%% Routing key is empty.
target_exchange_routing_key_empty(Config) ->
    XName = <<"amq.fanout">>,
    TargetAddr = rabbitmq_amqp_address:exchange(XName, <<>>),
    QName = atom_to_binary(?FUNCTION_NAME),

    Init = {_, LinkPair = #link_pair{session = Session}} = init(Config),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{}),
    ok = rabbitmq_amqp_client:bind_queue(LinkPair, QName, XName, <<"ignored">>, #{}),
    SrcAddr = rabbitmq_amqp_address:queue(QName),
    {ok, Receiver} = amqp10_client:attach_receiver_link(Session, <<"receiver">>, SrcAddr),

    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, TargetAddr),
    ok = wait_for_credit(Sender),
    Body = <<"body">>,
    Msg0 = amqp10_msg:new(<<"tag">>, Body, true),
    ok = amqp10_client:send_msg(Sender, Msg0),

    {ok, Msg} = amqp10_client:get_msg(Receiver),
    ?assertEqual([Body], amqp10_msg:body(Msg)),

    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = cleanup(Init).

%% Test v2 target address
%% /exchanges/:exchange
%% Routing key is empty.
target_exchange(Config) ->
    XName = <<"amq.fanout">>,
    TargetAddr = rabbitmq_amqp_address:exchange(XName),
    QName = atom_to_binary(?FUNCTION_NAME),

    Init = {_, LinkPair = #link_pair{session = Session}} = init(Config),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{}),
    ok = rabbitmq_amqp_client:bind_queue(LinkPair, QName, XName, <<"ignored">>, #{}),
    SrcAddr = rabbitmq_amqp_address:queue(QName),
    {ok, Receiver} = amqp10_client:attach_receiver_link(Session, <<"receiver">>, SrcAddr),

    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, TargetAddr),
    ok = wait_for_credit(Sender),
    Body = <<"body">>,
    Msg0 = amqp10_msg:new(<<"tag">>, Body, true),
    ok = amqp10_client:send_msg(Sender, Msg0),

    {ok, Msg} = amqp10_client:get_msg(Receiver),
    ?assertEqual([Body], amqp10_msg:body(Msg)),

    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = cleanup(Init).

%% Test v2 target address
%% /exchanges/:exchange
%% where the target exchange does not exist.
target_exchange_absent(Config) ->
    XName = <<"🎈"/utf8>>,
    TargetAddr = rabbitmq_amqp_address:exchange(XName),

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),

    {ok, _Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, TargetAddr),
    receive
        {amqp10_event,
         {session, Session,
          {ended,
           #'v1_0.error'{
              condition = ?V_1_0_AMQP_ERROR_NOT_FOUND,
              description = {utf8, <<"no exchange '", XName:(byte_size(XName))/binary,
                                     "' in vhost '/'">>}}}}} -> ok
    after 5000 ->
              Reason = {missing_event, ?LINE},
              flush(Reason),
              ct:fail(Reason)
    end,
    ok = amqp10_client:close_connection(Connection).

%% Test v2 target and source address
%% /queues/:queue
queue(Config) ->
    QName = <<"🎈"/utf8>>,
    queue0(QName, Config).

%% Test v2 target and source address
%% /queues/:queue
%% where :queue contains a "/" character.
queue_with_slash(Config) ->
    QName = <<"my/queue">>,
    queue0(QName, Config).

queue0(QName, Config) ->
    Addr = rabbitmq_amqp_address:queue(QName),

    Init = {_, LinkPair = #link_pair{session = Session}} = init(Config),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{}),
    {ok, Receiver} = amqp10_client:attach_receiver_link(Session, <<"receiver">>, Addr),

    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, Addr),
    ok = wait_for_credit(Sender),
    Body = <<"body">>,
    Msg0 = amqp10_msg:new(<<"tag">>, Body, true),
    ok = amqp10_client:send_msg(Sender, Msg0),

    {ok, Msg} = amqp10_client:get_msg(Receiver),
    ?assertEqual([Body], amqp10_msg:body(Msg)),

    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = cleanup(Init).

%% Test v2 target address
%% /queues/:queue
%% where the target queue does not exist.
target_queue_absent(Config) ->
    QName = <<"🎈"/utf8>>,
    TargetAddr = rabbitmq_amqp_address:queue(QName),

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),

    {ok, _Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, TargetAddr),
    receive
        {amqp10_event,
         {session, Session,
          {ended,
           #'v1_0.error'{
              condition = ?V_1_0_AMQP_ERROR_NOT_FOUND,
              description = {utf8, <<"no queue '", QName:(byte_size(QName))/binary,
                                     "' in vhost '/'">>}}}}} -> ok
    after 5000 ->
              Reason = {missing_event, ?LINE},
              flush(Reason),
              ct:fail(Reason)
    end,
    ok = amqp10_client:close_connection(Connection).

%% Test v2 target address 'null' and 'to'
%% /exchanges/:exchange/:routing-key
%% with varying routing keys.
target_per_message_exchange_routing_key(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    DirectX = <<"amq.direct">>,
    RKey1 = <<"🗝️1"/utf8>>,
    RKey2 = <<"🗝️2"/utf8>>,
    To1 = rabbitmq_amqp_address:exchange(DirectX, RKey1),
    To2 = rabbitmq_amqp_address:exchange(DirectX, RKey2),

    Init = {_, LinkPair = #link_pair{session = Session}} = init(Config),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{}),
    ok = rabbitmq_amqp_client:bind_queue(LinkPair, QName, DirectX, RKey1, #{}),
    ok = rabbitmq_amqp_client:bind_queue(LinkPair, QName, DirectX, RKey2, #{}),

    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, null),
    ok = wait_for_credit(Sender),

    Tag1 = Body1 = <<1>>,
    Tag2 = Body2 = <<2>>,

    %% Although mc_amqp:essential_properties/1 parses these annotations, they should be ignored.
    Msg1 = amqp10_msg:set_message_annotations(
             #{<<"x-exchange">> => <<"ignored">>,
               <<"x-routing-key">> => <<"ignored">>},
             amqp10_msg:set_properties(#{to => To1}, amqp10_msg:new(Tag1, Body1))),
    Msg2 = amqp10_msg:set_properties(#{to => To2}, amqp10_msg:new(Tag2, Body2)),
    ok = amqp10_client:send_msg(Sender, Msg1),
    ok = amqp10_client:send_msg(Sender, Msg2),
    ok = wait_for_settled(accepted, Tag1),
    ok = wait_for_settled(accepted, Tag2),

    {ok, #{message_count := 2}} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = cleanup(Init).

%% Test v2 target address 'null' and 'to'
%% /exchanges/:exchange
%% with varying exchanges.
target_per_message_exchange(Config) ->
    XFanout = <<"amq.fanout">>,
    XHeaders = <<"amq.headers">>,
    To1 = rabbitmq_amqp_address:exchange(XFanout),
    To2 = rabbitmq_amqp_address:exchange(XHeaders),
    QName = atom_to_binary(?FUNCTION_NAME),

    Init = {_, LinkPair = #link_pair{session = Session}} = init(Config),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{}),
    ok = rabbitmq_amqp_client:bind_queue(LinkPair, QName, XFanout, <<>>, #{}),
    ok =  rabbitmq_amqp_client:bind_queue(LinkPair, QName, XHeaders, <<>>,
                                          #{<<"my key">> => true,
                                            <<"x-match">> => {utf8, <<"any">>}}),

    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, null),
    ok = wait_for_credit(Sender),

    Tag1 = Body1 = <<1>>,
    Tag2 = Body2 = <<2>>,
    Msg1 = amqp10_msg:set_properties(#{to => To1}, amqp10_msg:new(Tag1, Body1)),
    Msg2 = amqp10_msg:set_application_properties(
             #{<<"my key">> => true},
             amqp10_msg:set_properties(#{to => To2}, amqp10_msg:new(Tag2, Body2))),
    ok = amqp10_client:send_msg(Sender, Msg1),
    ok = amqp10_client:send_msg(Sender, Msg2),
    ok = wait_for_settled(accepted, Tag1),
    ok = wait_for_settled(accepted, Tag2),

    {ok, #{message_count := 2}} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = cleanup(Init).

%% Test v2 target address 'null' and 'to'
%% /queues/:queue
target_per_message_queue(Config) ->
    Q1 = <<"q1">>,
    Q2 = <<"q2">>,
    Q3 = <<"q3">>,
    To1 = rabbitmq_amqp_address:queue(Q1),
    To2 = rabbitmq_amqp_address:queue(Q2),
    To3 = rabbitmq_amqp_address:queue(Q3),

    Init = {_, LinkPair = #link_pair{session = Session}} = init(Config),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, Q1, #{}),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, Q2, #{}),

    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, null),
    ok = wait_for_credit(Sender),

    Tag1 = Body1 = <<1>>,
    Tag2 = Body2 = <<2>>,
    Tag3 = Body3 = <<3>>,
    Msg1 = amqp10_msg:set_properties(#{to => To1}, amqp10_msg:new(Tag1, Body1)),
    Msg2 = amqp10_msg:set_properties(#{to => To2}, amqp10_msg:new(Tag2, Body2)),
    Msg3 = amqp10_msg:set_properties(#{to => To3}, amqp10_msg:new(Tag3, Body3)),
    ok = amqp10_client:send_msg(Sender, Msg1),
    ok = amqp10_client:send_msg(Sender, Msg2),
    ok = amqp10_client:send_msg(Sender, Msg3),
    ok = wait_for_settled(accepted, Tag1),
    ok = wait_for_settled(accepted, Tag2),
    ok = wait_for_settled(released, Tag3),

    {ok, #{message_count := 1}} = rabbitmq_amqp_client:delete_queue(LinkPair, Q1),
    {ok, #{message_count := 1}} = rabbitmq_amqp_client:delete_queue(LinkPair, Q2),
    ok = cleanup(Init).

%% Test v2 target address 'null', but 'to' not set.
target_per_message_unset_to_address(Config) ->
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, null),
    ok = wait_for_credit(Sender),

    %% Send message with 'to' unset.
    DTag = <<1>>,
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(DTag, <<0>>)),
    ExpectedError = #'v1_0.error'{
                       condition = ?V_1_0_AMQP_ERROR_PRECONDITION_FAILED,
                       description = {utf8, <<"anonymous terminus requires 'to' address to be set">>}},
    ok = wait_for_settled({rejected, ExpectedError}, DTag),

    ok = amqp10_client:detach_link(Sender),
    receive {amqp10_event, {link, Sender, {detached, normal}}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,
    ok = amqp10_client:end_session(Session),
    ok = amqp10_client:close_connection(Connection).

bad_v2_addresses() ->
    [
     %% valid v1, but bad v2 target addresses
     <<"/topic/mytopic">>,
     <<"/amq/queue/myqueue">>,
     <<"myqueue">>,
     <<"/queue">>,
     %% bad v2 target addresses
     <<>>,
     <<0>>,
     <<"/">>,
     <<"//">>,
     <<"/queues">>,
     <<"/queues/">>,
     <<"/queue/">>,
     <<"/exchanges">>,
     %% default exchange in v2 target address is disallowed
     <<"/exchanges/">>,
     <<"/exchanges//">>,
     <<"/exchanges//mykey">>,
     <<"/exchanges/amq.default">>,
     <<"/exchanges/amq.default/">>,
     <<"/exchanges/amq.default/mykey">>,
     <<"/ex/✋"/utf8>>,
     <<"/exchange">>,
     <<"/exchange/">>,
     <<"/exchange/amq.default">>,
     <<"/exchange//key/">>,
     <<"/exchange//key/mykey">>,
     <<"/exchange/amq.default/key/">>,
     <<"/exchange/amq.default/key/mykey">>,
     %% The following addresses should be percent encoded, but aren't.
     <<"/queues/missing%encoding">>,
     <<"/queues/missing/encoding">>,
     <<"/queues/✋"/utf8>>,
     <<"/exchanges/missing%encoding">>,
     <<"/exchanges/missing/encoding/routingkey">>,
     <<"/exchanges/exchange/missing%encoding">>,
     <<"/exchanges/✋"/utf8>>
    ].

%% Test v2 target address 'null' with an invalid 'to' addresses.
target_per_message_bad_to_address(Config) ->
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, null),
    ok = wait_for_credit(Sender),

    lists:foreach(
      fun(Addr) ->
              DTag = <<"some delivery tag">>,
              Msg = amqp10_msg:set_properties(#{to => Addr}, amqp10_msg:new(DTag, <<0>>, false)),
              ok = amqp10_client:send_msg(Sender, Msg),
              receive
                  {amqp10_disposition, {{rejected, Error}, DTag}} ->
                      ?assertMatch(#'v1_0.error'{condition = ?V_1_0_AMQP_ERROR_PRECONDITION_FAILED,
                                                 description = {utf8, <<"bad 'to' address", _Rest/binary>>}},
                                   Error)
              after 5000 ->
                        flush(missing_disposition),
                        ct:fail(missing_disposition)
              end
      end, bad_v2_addresses()),

    ok = amqp10_client:end_session(Session),
    ok = amqp10_client:close_connection(Connection).

target_per_message_exchange_absent_settled(Config) ->
    Init = {_, LinkPair = #link_pair{session = Session}} = init(Config),
    XName = <<"🎈"/utf8>>,
    Address = rabbitmq_amqp_address:exchange(XName),
    ok = rabbitmq_amqp_client:declare_exchange(LinkPair, XName, #{}),
    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, null),
    ok = wait_for_credit(Sender),

    DTag1 = <<1>>,
    Msg1 = amqp10_msg:set_properties(#{to => Address}, amqp10_msg:new(DTag1, <<"m1">>)),
    ok = amqp10_client:send_msg(Sender, Msg1),
    ok = wait_for_settled(released, DTag1),

    ok = rabbitmq_amqp_client:delete_exchange(LinkPair, XName),

    DTag2 = <<2>>,
    Msg2 = amqp10_msg:set_properties(#{to => Address}, amqp10_msg:new(DTag2, <<"m2">>, true)),
    ok = amqp10_client:send_msg(Sender, Msg2),

    %% "the routing node MUST detach the link over which the message was sent with an error.
    %% [...] Additionally the info field of error MUST contain an entry with symbolic key delivery-tag
    %% and binary value of the delivery-tag of the message which caused the failure."
    %% https://docs.oasis-open.org/amqp/anonterm/v1.0/cs01/anonterm-v1.0-cs01.html#doc-routingerrors
    receive {amqp10_event, {link, Sender, {detached, Error}}} ->
                ?assertEqual(
                   #'v1_0.error'{
                      condition = ?V_1_0_AMQP_ERROR_NOT_FOUND,
                      description = {utf8, <<"no exchange '", XName/binary, "' in vhost '/'">>},
                      info = {map, [{{symbol, <<"delivery-tag">>}, {binary, DTag2}}]}
                     },
                   Error)
    after 5000 -> ct:fail("server did not close our outgoing link")
    end,

    ok = cleanup(Init).

target_per_message_exchange_absent_unsettled(Config) ->
    Init = {_, LinkPair = #link_pair{session = Session}} = init(Config),
    XName = <<"🎈"/utf8>>,
    Address = rabbitmq_amqp_address:exchange(XName),
    ok = rabbitmq_amqp_client:declare_exchange(LinkPair, XName, #{}),
    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, null),
    ok = wait_for_credit(Sender),

    DTag1 = <<"my tag">>,
    Msg1 = amqp10_msg:set_properties(#{to => Address}, amqp10_msg:new(DTag1, <<"hey">>)),
    ok = amqp10_client:send_msg(Sender, Msg1),
    ok = wait_for_settled(released, DTag1),

    ok = rabbitmq_amqp_client:delete_exchange(LinkPair, XName),

    %% "If the source of the link supports the rejected outcome, and the message has not
    %% already been settled by the sender, then the routing node MUST reject the message.
    %% In this case the error field of rejected MUST contain the error which would have been communicated
    %% in the detach which would have be sent if a link to the same address had been attempted."
    %% https://docs.oasis-open.org/amqp/anonterm/v1.0/cs01/anonterm-v1.0-cs01.html#doc-routingerrors
    %% We test here multiple rejections implicilty checking that link flow control works correctly.
    ExpectedError = #'v1_0.error'{
                       condition = ?V_1_0_AMQP_ERROR_NOT_FOUND,
                       description = {utf8, <<"no exchange '", XName/binary, "' in vhost '/'">>}},
    [begin
         DTag = Body = integer_to_binary(N),
         Msg = amqp10_msg:set_properties(#{to => Address}, amqp10_msg:new(DTag, Body, false)),
         ok = amqp10_client:send_msg(Sender, Msg),
         ok = wait_for_settled({rejected, ExpectedError}, DTag)
     end || N <- lists:seq(1, 300)],

    ok = cleanup(Init).

target_bad_address(Config) ->
    %% bad v1 and bad v2 target address
    TargetAddr = <<"/qqq/🎈"/utf8>>,
    target_bad_address0(TargetAddr, Config).

target_bad_v2_address(Config) ->
    lists:foreach(fun(Addr) ->
                          ok = target_bad_address0(Addr, Config)
                  end, bad_v2_addresses()).

target_bad_address0(TargetAddress, Config) ->
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),

    {ok, _Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, TargetAddress),
    receive
        {amqp10_event,
         {session, Session,
          {ended,
           #'v1_0.error'{condition = ?V_1_0_AMQP_ERROR_INVALID_FIELD}}}} -> ok
    after 5000 ->
              Reason = {missing_event, ?LINE, TargetAddress},
              flush(Reason),
              ct:fail(Reason)
    end,
    ok = amqp10_client:close_connection(Connection).

%% Test v2 source address
%% /queues/:queue
%% where the source queue does not exist.
source_queue_absent(Config) ->
    QName = <<"🎈"/utf8>>,
    SourceAddr = rabbitmq_amqp_address:queue(QName),

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),

    {ok, _Receiver} = amqp10_client:attach_receiver_link(Session, <<"receiver">>, SourceAddr),
    receive
        {amqp10_event,
         {session, Session,
          {ended,
           #'v1_0.error'{
              condition = ?V_1_0_AMQP_ERROR_NOT_FOUND,
              description = {utf8, <<"no queue '", QName:(byte_size(QName))/binary,
                                     "' in vhost '/'">>}}}}} -> ok
    after 5000 ->
              Reason = {missing_event, ?LINE},
              flush(Reason),
              ct:fail(Reason)
    end,
    ok = amqp10_client:close_connection(Connection).

source_bad_address(Config) ->
    %% bad v1 and bad v2 source address
    SourceAddr = <<"/qqq/🎈"/utf8>>,
    source_bad_address0(SourceAddr, Config).

source_bad_v2_address(Config) ->
    %% valid v1, but bad v2 source addresses
    SourceAddresses = [<<"/exchange/myroutingkey">>,
                       <<"/topic/mytopic">>,
                       <<"/amq/queue/myqueue">>,
                       <<"myqueue">>],
    lists:foreach(fun(Addr) ->
                          ok = source_bad_address0(Addr, Config)
                  end, SourceAddresses).

source_bad_address0(SourceAddress, Config) ->
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),

    {ok, _Receiver} = amqp10_client:attach_receiver_link(Session, <<"sender">>, SourceAddress),
    receive
        {amqp10_event,
         {session, Session,
          {ended,
           #'v1_0.error'{condition = ?V_1_0_AMQP_ERROR_INVALID_FIELD}}}} -> ok
    after 5000 ->
              Reason = {missing_event, ?LINE},
              flush(Reason),
              ct:fail(Reason)
    end,
    ok = amqp10_client:close_connection(Connection).

init(Config) ->
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, LinkPair} = rabbitmq_amqp_client:attach_management_link_pair_sync(Session, <<"mgmt link pair">>),
    {Connection, LinkPair}.

cleanup({Connection, LinkPair = #link_pair{session = Session}}) ->
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair),
    ok = amqp10_client:end_session(Session),
    ok = amqp10_client:close_connection(Connection).

connection_config(Config) ->
    Host = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    #{address => Host,
      port => Port,
      container_id => <<"my container">>,
      sasl => {plain, <<"guest">>, <<"guest">>}}.

wait_for_settled(State, Tag) ->
    receive
        {amqp10_disposition, {State, Tag}} ->
            ok
    after 5000 ->
              Reason = {?FUNCTION_NAME, State, Tag},
              flush(Reason),
              ct:fail(Reason)
    end.
