%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(protocol_interop_SUITE).

-compile([export_all,
          nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").
-include_lib("amqp10_common/include/amqp10_filter.hrl").

all() ->
    [{group, tests}].

groups() ->
    [{tests, [shuffle],
      [
       amqpl,
       amqp_credit_multiple_grants,
       amqp_credit_single_grant,
       amqp_attach_sub_batch,
       amqp_filter_expression
      ]
     }].

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
    %% Wait for exclusive or auto-delete queues being deleted.
    timer:sleep(800),
    rabbit_ct_broker_helpers:rpc(Config, ?MODULE, delete_queues, []),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testsuite cases
%% -------------------------------------------------------------------

amqpl(Config) ->
    [Server] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Ctag = Stream = atom_to_binary(?FUNCTION_NAME),
    publish_via_stream_protocol(Stream, Config),

    #'basic.qos_ok'{} = amqp_channel:call(Ch, #'basic.qos'{prefetch_count = 2}),
    amqp_channel:subscribe(Ch,
                           #'basic.consume'{queue = Stream,
                                            consumer_tag = Ctag,
                                            arguments = [{<<"x-stream-offset">>, long, 0}]},
                           self()),
    receive #'basic.consume_ok'{consumer_tag = Ctag} -> ok
    after 5000 -> ct:fail(consume_timeout)
    end,

    %% Since prefetch is 2, we expect to receive exactly 2 messages.
    %% Whenever we ack both messages, we should receive exactly 2 more messages.
    ExpectedPayloads = [{<<"m1">>, <<"m2">>},
                        {<<"m3">>, <<"m4">>},
                        {<<"m5">>, <<"m6">>},
                        %% The broker skips delivery of compressed sub batches to non Stream protocol
                        %% consumers, i.e. skips delivery of m7, m8, m9.
                        {<<"m10">>, <<"m11">>}],
    lists:foreach(
      fun({P1, P2}) ->
              ok = process_2_amqpl_messages(Ch, P1, P2)
      end, ExpectedPayloads),

    ok = amqp_channel:close(Ch).

process_2_amqpl_messages(Ch, P1, P2) ->
    %% We expect to receive exactly 2 messages.
    receive {#'basic.deliver'{},
             #amqp_msg{payload = P1}} -> ok
    after 5000 -> ct:fail({missing_delivery, P1})
    end,
    DTag = receive {#'basic.deliver'{delivery_tag = Tag},
                    #amqp_msg{payload = P2}} -> Tag
           after 5000 -> ct:fail({missing_delivery, P2})
           end,
    receive Msg -> ct:fail({unexpected_message, Msg})
    after 10 -> ok
    end,
    ok = amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DTag,
                                            multiple = true}).

amqp_credit_single_grant(Config) ->
    Stream = atom_to_binary(?FUNCTION_NAME),
    publish_via_stream_protocol(Stream, Config),

    %% Consume from the stream via AMQP 1.0.
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/queue/", Stream/binary>>,
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"test-receiver">>, Address, settled,
                       configuration, #{<<"rabbitmq:stream-offset-spec">> => <<"first">>}),

    %% There are 8 uncompressed messages in the stream.
    ok = amqp10_client:flow_link_credit(Receiver, 8, never),

    Msgs = receive_amqp_messages(Receiver, 8),
    ?assertEqual([<<"m1">>], amqp10_msg:body(hd(Msgs))),
    ?assertEqual([<<"m11">>], amqp10_msg:body(lists:last(Msgs))),
    ok = amqp10_client:close_connection(Connection).

amqp_credit_multiple_grants(Config) ->
    Stream = atom_to_binary(?FUNCTION_NAME),
    publish_via_stream_protocol(Stream, Config),

    %% Consume from the stream via AMQP 1.0.
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/queue/", Stream/binary>>,
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"test-receiver">>, Address, unsettled,
                       configuration, #{<<"rabbitmq:stream-offset-spec">> => <<"first">>}),

    %% Granting 1 credit should deliver us exactly 1 message.
    {ok, M1} = amqp10_client:get_msg(Receiver),
    ?assertEqual([<<"m1">>], amqp10_msg:body(M1)),
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 5000 -> ct:fail("expected credit_exhausted")
    end,
    receive {amqp10_msg, _, _} = Unexp1 -> ct:fail({unexpected_message, Unexp1})
    after 10 -> ok
    end,

    ok = amqp10_client:flow_link_credit(Receiver, 3, never),
    %% We expect to receive exactly 3 more messages
    receive {amqp10_msg, Receiver, Msg2} ->
                ?assertEqual([<<"m2">>], amqp10_msg:body(Msg2))
    after 5000 -> ct:fail("missing m2")
    end,
    receive {amqp10_msg, Receiver, Msg3} ->
                ?assertEqual([<<"m3">>], amqp10_msg:body(Msg3))
    after 5000 -> ct:fail("missing m3")
    end,
    %% Messages in an uncompressed subbatch should be delivered individually.
    M4 = receive {amqp10_msg, Receiver, Msg4} ->
                     ?assertEqual([<<"m4">>], amqp10_msg:body(Msg4)),
                     Msg4
         after 5000 -> ct:fail("missing m4")
         end,
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 5000 -> ct:fail("expected credit_exhausted")
    end,

    %% Let's ack all of them.
    ok = amqp10_client_session:disposition(
           Receiver,
           amqp10_msg:delivery_id(M1),
           amqp10_msg:delivery_id(M4),
           true,
           accepted),
    %% Acking shouldn't grant more credits.
    receive {amqp10_msg, _, _} = Unexp2 -> ct:fail({unexpected_message, Unexp2})
    after 10 -> ok
    end,

    ok = amqp10_client:flow_link_credit(Receiver, 3, never),
    M5 = receive {amqp10_msg, Receiver, Msg5} ->
                     ?assertEqual([<<"m5">>], amqp10_msg:body(Msg5)),
                     Msg5
         after 5000 -> ct:fail("missing m5")
         end,
    receive {amqp10_msg, Receiver, Msg6} ->
                ?assertEqual([<<"m6">>], amqp10_msg:body(Msg6))
    after 5000 -> ct:fail("missing m6")
    end,
    %% The broker skips delivery of compressed sub batches to non Stream protocol
    %% consumers, i.e. skips delivery of m7, m8, m9.
    receive {amqp10_msg, Receiver, Msg10} ->
                ?assertEqual([<<"m10">>], amqp10_msg:body(Msg10))
    after 5000 -> ct:fail("missing m10")
    end,
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 5000 -> ct:fail("expected credit_exhausted")
    end,
    receive {amqp10_msg, _, _} = Unexp3 -> ct:fail({unexpected_message, Unexp3})
    after 10 -> ok
    end,

    %% 1 message should be left in the stream.
    %% Let's drain the stream.
    ok = amqp10_client:flow_link_credit(Receiver, 1000, never, true),
    M11 = receive {amqp10_msg, Receiver, Msg11} ->
                      ?assertEqual([<<"m11">>], amqp10_msg:body(Msg11)),
                      Msg11
          after 5000 -> ct:fail("missing m11")
          end,
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 5000 -> ct:fail("expected credit_exhausted")
    end,

    %% Let's ack them all.
    ok = amqp10_client_session:disposition(
           Receiver,
           amqp10_msg:delivery_id(M5),
           amqp10_msg:delivery_id(M11),
           true,
           accepted),

    receive {amqp10_msg, _, _} = Unexp4 -> ct:fail({unexpected_message, Unexp4})
    after 10 -> ok
    end,

    ok = amqp10_client:detach_link(Receiver),
    ok = amqp10_client:close_connection(Connection).

amqp_attach_sub_batch(Config) ->
    Stream = atom_to_binary(?FUNCTION_NAME),
    publish_via_stream_protocol(Stream, Config),

    %% Consume from the stream via AMQP 1.0.
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/queue/", Stream/binary>>,
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"test-receiver">>, Address, settled, configuration,
                       %% Attach in the middle of an uncompresssed sub batch.
                       #{<<"rabbitmq:stream-offset-spec">> => 4}),

    {ok, M5} = amqp10_client:get_msg(Receiver),
    ?assertEqual([<<"m5">>], amqp10_msg:body(M5)),

    {ok, M6} = amqp10_client:get_msg(Receiver),
    ?assertEqual([<<"m6">>], amqp10_msg:body(M6)),

    %% The broker skips delivery of compressed sub batches to non Stream protocol
    %% consumers, i.e. skips delivery of m7, m8, m9.

    {ok, M10} = amqp10_client:get_msg(Receiver),
    ?assertEqual([<<"m10">>], amqp10_msg:body(M10)),

    {ok, M11} = amqp10_client:get_msg(Receiver),
    ?assertEqual([<<"m11">>], amqp10_msg:body(M11)),

    ok = amqp10_client:detach_link(Receiver),
    ok = amqp10_client:close_connection(Connection).

%% Test that AMQP filter expressions work when messages
%% are published via the stream protocol and consumed via AMQP.
amqp_filter_expression(Config) ->
    Stream = atom_to_binary(?FUNCTION_NAME),
    publish_via_stream_protocol(Stream, Config),

    %% Consume from the stream via AMQP 1.0.
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/queue/", Stream/binary>>,

    AppPropsFilter = [{{utf8, <<"my key">>},
                       {utf8, <<"my value">>}}],
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"test-receiver">>, Address, settled, configuration,
                       #{<<"rabbitmq:stream-offset-spec">> => <<"first">>,
                         ?DESCRIPTOR_NAME_APPLICATION_PROPERTIES_FILTER => {map, AppPropsFilter}
                        }),

    ok = amqp10_client:flow_link_credit(Receiver, 100, never),
    receive {amqp10_msg, Receiver, M2} ->
                ?assertEqual([<<"m2">>], amqp10_msg:body(M2))
    after 5000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_msg, Receiver, M4} ->
                ?assertEqual([<<"m4">>], amqp10_msg:body(M4))
    after 5000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_msg, Receiver, M5} ->
                ?assertEqual([<<"m5">>], amqp10_msg:body(M5))
    after 5000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_msg, Receiver, M6} ->
                ?assertEqual([<<"m6">>], amqp10_msg:body(M6))
    after 5000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_msg, _, _} = Msg ->
                ct:fail({received_unexpected_msg, Msg})
    after 10 -> ok
    end,

    ok = amqp10_client:detach_link(Receiver),
    ok = amqp10_client:close_connection(Connection).

%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------

publish_via_stream_protocol(Stream, Config) ->
    {ok, S, C0} = stream_test_utils:connect(Config, 0),

    {ok, C1} = stream_test_utils:create_stream(S, C0, Stream),

    PublisherId = 99,
    {ok, C2} = stream_test_utils:declare_publisher(S, C1, Stream, PublisherId),

    M1 = stream_test_utils:simple_entry(1, <<"m1">>),
    M2 = stream_test_utils:simple_entry(2, <<"m2">>, #'v1_0.application_properties'{
                                                        content = [{{utf8, <<"my key">>},
                                                                    {utf8, <<"my value">>}}]}),
    M3 = stream_test_utils:simple_entry(3, <<"m3">>),
    Messages1 = [M1, M2, M3],

    {ok, _, C3} = stream_test_utils:publish_entries(S, C2, PublisherId, length(Messages1), Messages1),

    UncompressedSubbatch = stream_test_utils:sub_batch_entry_uncompressed(4, [<<"m4">>, <<"m5">>, <<"m6">>]),
    {ok, _, C4} = stream_test_utils:publish_entries(S, C3, PublisherId, 1, UncompressedSubbatch),

    CompressedSubbatch = stream_test_utils:sub_batch_entry_compressed(5, [<<"m7">>, <<"m8">>, <<"m9">>]),
    {ok, _, C5} = stream_test_utils:publish_entries(S, C4, PublisherId, 1, CompressedSubbatch),

    M10 = stream_test_utils:simple_entry(6, <<"m10">>),
    M11 = stream_test_utils:simple_entry(7, <<"m11">>),
    Messages2 = [M10, M11],
    {ok, _, C6} = stream_test_utils:publish_entries(S, C5, PublisherId, length(Messages2), Messages2),
    {ok, _} = stream_test_utils:close(S, C6).

connection_config(Config) ->
    Host = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    #{address => Host,
      port => Port,
      container_id => <<"my container">>,
      sasl => {plain, <<"guest">>, <<"guest">>}}.

receive_amqp_messages(Receiver, N) ->
    receive_amqp_messages0(Receiver, N, []).

receive_amqp_messages0(_Receiver, 0, Acc) ->
    lists:reverse(Acc);
receive_amqp_messages0(Receiver, N, Acc) ->
    receive
        {amqp10_msg, Receiver, Msg} ->
            receive_amqp_messages0(Receiver, N - 1, [Msg | Acc])
    after 5000  ->
              exit({timeout, {num_received, length(Acc)}, {num_missing, N}})
    end.

delete_queues() ->
    [{ok, 0} = rabbit_amqqueue:delete(Q, false, false, <<"dummy">>) || Q <- rabbit_amqqueue:list()].
