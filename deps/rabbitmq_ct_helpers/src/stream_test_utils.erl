%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% There is no open source Erlang RabbitMQ Stream client.
%% Therefore, we have to build the Stream protocol commands manually.

-module(stream_test_utils).

-compile([export_all, nowarn_export_all]).

-include_lib("amqp10_common/include/amqp10_framing.hrl").

-define(RESPONSE_CODE_OK, 1).

connect(Config, Node) ->
    StreamPort = rabbit_ct_broker_helpers:get_node_config(Config, Node, tcp_port_stream),
    connect(StreamPort).

connect(StreamPort) ->
    {ok, Sock} = gen_tcp:connect("localhost", StreamPort, [{active, false}, {mode, binary}]),

    C0 = rabbit_stream_core:init(0),
    PeerPropertiesFrame = rabbit_stream_core:frame({request, 1, {peer_properties, #{}}}),
    ok = gen_tcp:send(Sock, PeerPropertiesFrame),
    {{response, 1, {peer_properties, _, _}}, C1} = receive_stream_commands(Sock, C0),

    ok = gen_tcp:send(Sock, rabbit_stream_core:frame({request, 1, sasl_handshake})),
    {{response, _, {sasl_handshake, _, _}}, C2} = receive_stream_commands(Sock, C1),
    Username = <<"guest">>,
    Password = <<"guest">>,
    Null = 0,
    PlainSasl = <<Null:8, Username/binary, Null:8, Password/binary>>,
    ok = gen_tcp:send(Sock, rabbit_stream_core:frame({request, 2, {sasl_authenticate, <<"PLAIN">>, PlainSasl}})),
    {{response, 2, {sasl_authenticate, _}}, C3} = receive_stream_commands(Sock, C2),
    {{tune, DefaultFrameMax, _}, C4} = receive_stream_commands(Sock, C3),

    ok = gen_tcp:send(Sock, rabbit_stream_core:frame({response, 0, {tune, DefaultFrameMax, 0}})),
    ok = gen_tcp:send(Sock, rabbit_stream_core:frame({request, 3, {open, <<"/">>}})),
    {{response, 3, {open, _, _ConnectionProperties}}, C5} = receive_stream_commands(Sock, C4),
    {ok, Sock, C5}.

close(Sock, C0) ->
    CloseReason = <<"OK">>,
    CloseFrame = rabbit_stream_core:frame({request, 1, {close, ?RESPONSE_CODE_OK, CloseReason}}),
    ok = gen_tcp:send(Sock, CloseFrame),
    {{response, 1, {close, ?RESPONSE_CODE_OK}}, C1} = receive_stream_commands(Sock, C0),
    {ok, C1}.

create_stream(Sock, C0, Stream) ->
    CreateStreamFrame = rabbit_stream_core:frame({request, 1, {create_stream, Stream, #{}}}),
    ok = gen_tcp:send(Sock, CreateStreamFrame),
    {{response, 1, {create_stream, ?RESPONSE_CODE_OK}}, C1} = receive_stream_commands(Sock, C0),
    {ok, C1}.

delete_stream(Sock, C0, Stream) ->
    DeleteStreamFrame = rabbit_stream_core:frame({request, 1, {delete_stream, Stream}}),
    ok = gen_tcp:send(Sock, DeleteStreamFrame),
    {{response, 1, {delete_stream, ?RESPONSE_CODE_OK}}, C1} = receive_stream_commands(Sock, C0),
    {ok, C1}.

declare_publisher(Sock, C0, Stream, PublisherId) ->
    DeclarePublisherFrame = rabbit_stream_core:frame({request, 1, {declare_publisher, PublisherId, <<>>, Stream}}),
    ok = gen_tcp:send(Sock, DeclarePublisherFrame),
    {{response, 1, {declare_publisher, ?RESPONSE_CODE_OK}}, C1} = receive_stream_commands(Sock, C0),
    {ok, C1}.

delete_publisher(Sock, C0, PublisherId) ->
    DeletePublisherFrame = rabbit_stream_core:frame({request, 1, {delete_publisher, PublisherId}}),
    ok = gen_tcp:send(Sock, DeletePublisherFrame),
    {{response, 1, {delete_publisher, ?RESPONSE_CODE_OK}}, C1} = receive_stream_commands(Sock, C0),
    {ok, C1}.


subscribe(Sock, C0, Stream, SubscriptionId, InitialCredit) ->
    subscribe(Sock, C0, Stream, SubscriptionId, InitialCredit, #{}).

subscribe(Sock, C0, Stream, SubscriptionId, InitialCredit, Props) ->
    Cmd = {subscribe, SubscriptionId, Stream, _OffsetSpec = first,
           InitialCredit, Props},
    SubscribeFrame = rabbit_stream_core:frame({request, 1, Cmd}),
    ok = gen_tcp:send(Sock, SubscribeFrame),
    {{response, 1, {subscribe, ?RESPONSE_CODE_OK}}, C1} = receive_stream_commands(Sock, C0),
    {ok, C1}.

credit(Sock, Subscription, Credit) ->
    CreditFrame = rabbit_stream_core:frame({credit, Subscription, Credit}),
    ok = gen_tcp:send(Sock, CreditFrame),
    ok.

unsubscribe(Sock, C0, SubscriptionId) ->
    UnsubscribeFrame = rabbit_stream_core:frame({request, 1, {unsubscribe, SubscriptionId}}),
    ok = gen_tcp:send(Sock, UnsubscribeFrame),
    {{response, 1, {unsubscribe, ?RESPONSE_CODE_OK}}, C1} = receive_stream_commands(Sock, C0),
    {ok, C1}.

publish(Sock, C0, PublisherId, Sequence0, Payloads) ->
    SeqIds = lists:seq(Sequence0, Sequence0 + length(Payloads) - 1),
    Messages = [simple_entry(Seq, P)
                || {Seq, P} <- lists:zip(SeqIds, Payloads)],
    {ok, SeqIds, C1} = publish_entries(Sock, C0, PublisherId, length(Messages), Messages),
    {ok, C1}.

publish_entries(Sock, C0, PublisherId, MsgCount, Messages) ->
    PublishFrame1 = rabbit_stream_core:frame({publish, PublisherId, MsgCount, Messages}),
    ok = gen_tcp:send(Sock, PublishFrame1),
    wait_for_confirms(Sock, C0, PublisherId, [], MsgCount).

wait_for_confirms(_, C, _, Acc, 0) ->
    {ok, Acc, C};
wait_for_confirms(S, C0, PublisherId, Acc, Remaining) ->
    case receive_stream_commands(S, C0) of
        {{publish_confirm, PublisherId, SeqIds}, C1} ->
            wait_for_confirms(S, C1, PublisherId, Acc ++ SeqIds, Remaining - length(SeqIds));
        {Frame, C1} ->
            {unexpected_frame, Frame, C1}
    end.

%% Streams contain AMQP 1.0 encoded messages.
%% In this case, the AMQP 1.0 encoded message contains a single data section.
simple_entry(Sequence, Body)
  when is_binary(Body) ->
    DataSect = iolist_to_binary(amqp10_framing:encode_bin(#'v1_0.data'{content = Body})),
    DataSectSize = byte_size(DataSect),
    <<Sequence:64, 0:1, DataSectSize:31, DataSect:DataSectSize/binary>>.

%% Streams contain AMQP 1.0 encoded messages.
%% In this case, the AMQP 1.0 encoded message consists of an application-properties section and a data section.
simple_entry(Sequence, Body, AppProps)
  when is_binary(Body) ->
    AppPropsSect = iolist_to_binary(amqp10_framing:encode_bin(AppProps)),
    DataSect = iolist_to_binary(amqp10_framing:encode_bin(#'v1_0.data'{content = Body})),
    Sects = <<AppPropsSect/binary, DataSect/binary>>,
    SectSize = byte_size(Sects),
    <<Sequence:64, 0:1, SectSize:31, Sects:SectSize/binary>>.

%% Here, each AMQP 1.0 encoded message consists of an application-properties section and a data section.
%% All data sections are delivered uncompressed in 1 batch.
sub_batch_entry_uncompressed(Sequence, Bodies) ->
    Batch = lists:foldl(fun(Body, Acc) ->
                                AppProps = #'v1_0.application_properties'{
                                              content = [{{utf8, <<"my key">>}, {utf8, <<"my value">>}}]},
                                Sect0 = iolist_to_binary(amqp10_framing:encode_bin(AppProps)),
                                Sect1 = iolist_to_binary(amqp10_framing:encode_bin(#'v1_0.data'{content = Body})),
                                Sect = <<Sect0/binary, Sect1/binary>>,
                                <<Acc/binary, 0:1, (byte_size(Sect)):31, Sect/binary>>
                        end, <<>>, Bodies),
    Size = byte_size(Batch),
    <<Sequence:64, 1:1, 0:3, 0:4, (length(Bodies)):16, Size:32, Size:32, Batch:Size/binary>>.

%% Here, each AMQP 1.0 encoded message contains a single data section.
%% All data sections are delivered in 1 gzip compressed batch.
sub_batch_entry_compressed(Sequence, Bodies) ->
    Uncompressed = lists:foldl(fun(Body, Acc) ->
                                       Bin = iolist_to_binary(amqp10_framing:encode_bin(#'v1_0.data'{content = Body})),
                                       <<Acc/binary, Bin/binary>>
                               end, <<>>, Bodies),
    Compressed = zlib:gzip(Uncompressed),
    CompressedLen = byte_size(Compressed),
    <<Sequence:64, 1:1, 1:3, 0:4, (length(Bodies)):16, (byte_size(Uncompressed)):32,
      CompressedLen:32, Compressed:CompressedLen/binary>>.


receive_stream_commands(Sock, C0) ->
    receive_stream_commands(gen_tcp, Sock, C0).

receive_stream_commands(Transport, Sock, C0) ->
    receive_stream_commands(Transport, Sock, C0, 10).

receive_stream_commands(_Transport, _Sock, C0, 0) ->
    rabbit_stream_core:next_command(C0);
receive_stream_commands(Transport, Sock, C0, N) ->
    case rabbit_stream_core:next_command(C0) of
        empty ->
            case Transport:recv(Sock, 0, 5000) of
                {ok, Data} ->
                    C1 = rabbit_stream_core:incoming_data(Data, C0),
                    receive_stream_commands(Transport, Sock, C1, N - 1);
                {error, Err} ->
                    ct:fail("error receiving stream data ~w", [Err])
            end;
        Res ->
            Res
    end.
