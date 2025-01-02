%% The contents of this file are subject to the Mozilla Public License
%% Version 2.0 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/en-US/MPL/2.0/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is Pivotal Software, Inc.
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_stream_core).

-include("rabbit_stream.hrl").

-export([init/1,
         incoming_data/2,
         next_command/1,
         all_commands/1,
         frame/1,
         parse_command/1]).

%% holds static or rarely changing fields
-record(cfg, {}).
-record(?MODULE,
        {cfg :: #cfg{},
         frames = [] :: [iodata()],
         %% partial data
         data ::
             undefined |
             %% this is only if the binary is smaller than 4 bytes
             binary() |
             {RemainingBytes :: non_neg_integer(), iodata()},
         commands = queue:new() :: queue:queue(command())}).

-opaque state() :: #?MODULE{}.

%% for parsing
-define(STRING(Size, Str), Size:16, Str:Size / binary).
%% for pickling
-define(STRING(Str), (byte_size(Str)):16, Str / binary).
-define(DATASTR(Str), (byte_size(Str)):32, Str / binary).

-export_type([state/0, command_version/0]).

-type command_version() :: 0..65535.
-type correlation_id() :: non_neg_integer().
%% publishing sequence number
-type publishing_id() :: non_neg_integer().
-type publisher_id() :: 0..255.
-type subscription_id() :: 0..255.
-type writer_ref() :: binary().
-type stream_name() :: binary().
-type offset_spec() :: osiris:offset_spec().
-type response_code() ::
    ?RESPONSE_CODE_OK |
    ?RESPONSE_CODE_STREAM_DOES_NOT_EXIST |
    ?RESPONSE_CODE_SUBSCRIPTION_ID_ALREADY_EXISTS |
    ?RESPONSE_CODE_SUBSCRIPTION_ID_DOES_NOT_EXIST |
    ?RESPONSE_SASL_MECHANISM_NOT_SUPPORTED |
    ?RESPONSE_AUTHENTICATION_FAILURE |
    ?RESPONSE_SASL_ERROR |
    ?RESPONSE_SASL_CHALLENGE |
    ?RESPONSE_SASL_AUTHENTICATION_FAILURE_LOOPBACK |
    ?RESPONSE_VHOST_ACCESS_FAILURE |
    ?RESPONSE_CODE_UNKNOWN_FRAME |
    ?RESPONSE_CODE_FRAME_TOO_LARGE |
    ?RESPONSE_CODE_INTERNAL_ERROR |
    ?RESPONSE_CODE_ACCESS_REFUSED |
    ?RESPONSE_CODE_PRECONDITION_FAILED |
    ?RESPONSE_CODE_PUBLISHER_DOES_NOT_EXIST |
    ?RESPONSE_CODE_NO_OFFSET |
    ?RESPONSE_SASL_CANNOT_CHANGE_MECHANISM |
    ?RESPONSE_SASL_CANNOT_CHANGE_USERNAME .
-type error_code() :: response_code().
-type sequence() :: non_neg_integer().
-type credit() :: non_neg_integer().
-type offset_ref() :: binary().
-type endpoint() :: {Host :: binary(), Port :: non_neg_integer()}.
-type active() :: boolean().
-type command() ::
    {publish,
     publisher_id(),
     MessageCount :: non_neg_integer(),
     Payload :: binary() | iolist()} |
    {publish_v2,
     publisher_id(),
     MessageCount :: non_neg_integer(),
     Payload :: binary() | iolist()} |
    {publish_confirm, publisher_id(), [publishing_id()]} |
    {publish_error, publisher_id(), error_code(), [publishing_id()]} |
    {deliver, subscription_id(), Chunk :: binary()} |
    {deliver_v2,
     subscription_id(),
     CommittedChunkId :: osiris:offset(),
     Chunk :: binary()} |
    {credit, subscription_id(), Credit :: non_neg_integer()} |
    {metadata_update, stream_name(), response_code()} |
    {store_offset, offset_ref(), stream_name(), osiris:offset()} |
    heartbeat |
    {tune, FrameMax :: non_neg_integer(),
     HeartBeat :: non_neg_integer()} |
    {request, correlation_id(),
     {declare_publisher, publisher_id(), writer_ref(), stream_name()} |
     {query_publisher_sequence, writer_ref(), stream_name()} |
     {delete_publisher, publisher_id()} |
     {subscribe,
      subscription_id(),
      stream_name(),
      offset_spec(),
      credit(),
      Properties :: #{binary() => binary()}} |
     {query_offset, offset_ref(), stream_name()} |
     {unsubscribe, subscription_id()} |
     {create_stream, stream_name(), Args :: #{binary() => binary()}} |
     {delete_stream, stream_name()} |
     {metadata, [stream_name()]} |
     {peer_properties, #{binary() => binary()}} |
     sasl_handshake |
     {sasl_authenticate, Mechanism :: binary(), SaslFragment :: binary()} |
     {open, VirtualHost :: binary()} |
     {close, Code :: non_neg_integer(), Reason :: binary()} |
     {route, RoutingKey :: binary(), SuperStream :: binary()} |
     {partitions, SuperStream :: binary()} |
     {consumer_update, subscription_id(), active()} |
     {exchange_command_versions,
      [{Command :: atom(), MinVersion :: command_version(),
        MaxVersion :: command_version()}]} |
     {stream_stats, Stream :: binary()} |
     {create_super_stream, stream_name(), Partitions :: [binary()], BindingKeys :: [binary()], Args :: #{binary() => binary()}} |
     {delete_super_stream, stream_name()}} |
    {response, correlation_id(),
     {declare_publisher |
      delete_publisher |
      subscribe |
      unsubscribe |
      create_stream |
      delete_stream |
      close |
      sasl_authenticate |
      create_super_stream |
      delete_super_stream,
      response_code()} |
     {query_publisher_sequence, response_code(), sequence()} |
     {open, response_code(), #{binary() => binary()}} |
     {query_offset, response_code(), osiris:offset()} |
     {metadata, Endpoints :: [endpoint()],
      Metadata ::
          #{stream_name() =>
                stream_not_found | stream_not_available |
                {Leader :: endpoint() | undefined, Replicas :: [endpoint()]}}} |
     {peer_properties, response_code(), #{binary() => binary()}} |
     {sasl_handshake, response_code(), Mechanisms :: [binary()]} |
     {sasl_authenticate, response_code(), Challenge :: binary()} |
     {tune, FrameMax :: non_neg_integer(),
      HeartBeat :: non_neg_integer()} |
     {credit, response_code(), subscription_id()} |
     {route, response_code(), [stream_name()]} |
     {partitions, response_code(), [stream_name()]} |
     {consumer_update, response_code(), none | offset_spec()} |
     {exchange_command_versions, response_code(),
      [{Command :: atom(), MinVersion :: command_version(),
        MaxVersion :: command_version()}]} |
     {stream_stats, response_code(), Stats :: #{binary() => integer()}}} |
    {unknown, binary()}.

-spec init(term()) -> state().
init(_) ->
    #?MODULE{cfg = #cfg{}}.

-spec next_command(state()) -> {command(), state()} | empty.
next_command(#?MODULE{commands = Commands0} = State) ->
    case queue:out(Commands0) of
        {{value, Cmd}, Commands} ->
            {Cmd, State#?MODULE{commands = Commands}};
        {empty, _} ->
            empty
    end.

-spec all_commands(state()) -> {[command()], state()}.
all_commands(#?MODULE{commands = Commands0} = State) ->
    {queue:to_list(Commands0), State#?MODULE{commands = queue:new()}}.

%% returns frames
-spec incoming_data(binary(), state()) -> state().
%% TODO: check max frame size
incoming_data(<<>>,
              #?MODULE{frames = Frames, commands = Commands} = State) ->
    State#?MODULE{frames = [], commands = parse_frames(Frames, Commands)};
incoming_data(<<Size:32, Frame:Size/binary, Rem/binary>>,
              #?MODULE{frames = Frames, data = undefined} = State) ->
    incoming_data(Rem,
                  State#?MODULE{frames = [Frame | Frames], data = undefined});
incoming_data(<<Size:32, Rem/binary>>,
              #?MODULE{frames = Frames,
                       data = undefined,
                       commands = Commands} =
                  State) ->
    %% not enough data to complete frame, stash and await more data
    State#?MODULE{frames = [],
                  data = {Size - byte_size(Rem), Rem},
                  commands = parse_frames(Frames, Commands)};
incoming_data(Data,
              #?MODULE{frames = Frames,
                       data = undefined,
                       commands = Commands} =
                  State)
    when byte_size(Data) < 4 ->
    %% not enough data to even know the size required
    %% just stash binary and hit last clause next
    State#?MODULE{frames = [],
                  data = Data,
                  commands = parse_frames(Frames, Commands)};
incoming_data(Data,
              #?MODULE{frames = Frames,
                       data = {Size, Partial},
                       commands = Commands} =
                  State) ->
    case Data of
        <<Part:Size/binary, Rem/binary>> ->
            incoming_data(Rem,
                          State#?MODULE{frames =
                                            [append_data(Partial, Part)
                                             | Frames],
                                        data = undefined});
        Rem ->
            State#?MODULE{frames = [],
                          data =
                              {Size - byte_size(Rem),
                               append_data(Partial, Rem)},
                          commands = parse_frames(Frames, Commands)}
    end;
incoming_data(Data, #?MODULE{data = Partial} = State)
    when is_binary(Partial) ->
    incoming_data(<<Partial/binary, Data/binary>>,
                  State#?MODULE{data = undefined}).

parse_frames(Frames, Queue) ->
    lists:foldr(fun(Frame, Acc) -> queue:in(parse_command(Frame), Acc)
                end,
                Queue, Frames).

-spec frame(command()) -> iodata().
frame({publish_confirm, PublisherId, PublishingIds}) ->
    PubIds =
        lists:foldl(fun(PublishingId, Acc) -> <<Acc/binary, PublishingId:64>>
                    end,
                    <<>>, PublishingIds),
    PublishingIdCount = length(PublishingIds),
    wrap_in_frame([<<?REQUEST:1,
                     ?COMMAND_PUBLISH_CONFIRM:15,
                     ?VERSION_1:16,
                     PublisherId:8,
                     PublishingIdCount:32>>,
                   PubIds]);
frame({publish, PublisherId, MessageCount, Payload}) ->
    wrap_in_frame([<<?REQUEST:1,
                     ?COMMAND_PUBLISH:15,
                     ?VERSION_1:16,
                     PublisherId:8,
                     MessageCount:32>>,
                   Payload]);
frame({publish_v2, PublisherId, MessageCount, Payload}) ->
    wrap_in_frame([<<?REQUEST:1,
                     ?COMMAND_PUBLISH:15,
                     ?VERSION_2:16,
                     PublisherId:8,
                     MessageCount:32>>,
                   Payload]);
frame({deliver, SubscriptionId, Chunk}) ->
    wrap_in_frame([<<?REQUEST:1,
                     ?COMMAND_DELIVER:15,
                     ?VERSION_1:16,
                     SubscriptionId:8>>,
                   Chunk]);
frame({deliver_v2, SubscriptionId, CommittedChunkId, Chunk}) ->
    wrap_in_frame([<<?REQUEST:1,
                     ?COMMAND_DELIVER:15,
                     ?VERSION_2:16,
                     SubscriptionId:8,
                     CommittedChunkId:64>>,
                   Chunk]);
frame({metadata_update, Stream, ResponseCode}) ->
    StreamSize = byte_size(Stream),
    wrap_in_frame(<<?REQUEST:1,
                    ?COMMAND_METADATA_UPDATE:15,
                    ?VERSION_1:16,
                    ResponseCode:16,
                    StreamSize:16,
                    Stream/binary>>);
frame({store_offset, Reference, Stream, Offset}) ->
    ReferenceSize = byte_size(Reference),
    StreamSize = byte_size(Stream),
    wrap_in_frame(<<?REQUEST:1,
                    ?COMMAND_STORE_OFFSET:15,
                    ?VERSION_1:16,
                    ReferenceSize:16,
                    Reference/binary,
                    StreamSize:16,
                    Stream/binary,
                    Offset:64>>);
frame(heartbeat) ->
    wrap_in_frame(<<?REQUEST:1, ?COMMAND_HEARTBEAT:15, ?VERSION_1:16>>);
frame({credit, SubscriptionId, Credit}) ->
    wrap_in_frame(<<?REQUEST:1,
                    ?COMMAND_CREDIT:15,
                    ?VERSION_1:16,
                    SubscriptionId:8,
                    Credit:16/signed>>);
frame({tune, FrameMax, Heartbeat}) ->
    %% tune can also be a response, which is weird
    wrap_in_frame(<<?REQUEST:1,
                    ?COMMAND_TUNE:15,
                    ?VERSION_1:16,
                    FrameMax:32,
                    Heartbeat:32>>);
frame({publish_error, PublisherId, ErrCode, PublishingIds}) ->
    Details =
        iolist_to_binary(lists:foldr(fun(PubId, Acc) ->
                                        [<<PubId:64, ErrCode:16>> | Acc]
                                     end,
                                     [], PublishingIds)),
    wrap_in_frame(<<?REQUEST:1,
                    ?COMMAND_PUBLISH_ERROR:15,
                    ?VERSION_1:16,
                    PublisherId:8,
                    (length(PublishingIds)):32,
                    Details/binary>>);
frame({request, CorrelationId, Body}) ->
    {CmdTag, BodyBin} = request_body(Body),
    CmdId = command_id(CmdTag),
    wrap_in_frame([<<?REQUEST:1,
                     CmdId:15,
                     ?VERSION_1:16,
                     CorrelationId:32>>,
                   BodyBin]);
frame({response, _CorrelationId, {credit, Code, SubscriptionId}}) ->
    %% special case as credit response does not write correlationid!
    wrap_in_frame(<<?RESPONSE:1,
                    ?COMMAND_CREDIT:15,
                    ?VERSION_1:16,
                    Code:16,
                    SubscriptionId:8>>);
frame({response, CorrelationId, {Tag, Code}})
    when is_integer(Code) andalso is_atom(Tag) ->
    %% standard response without payload
    CmdId = command_id(Tag),
    wrap_in_frame(<<?RESPONSE:1,
                    CmdId:15,
                    ?VERSION_1:16,
                    CorrelationId:32,
                    Code:16>>);
frame({response, _Corr, {tune, FrameMax, Heartbeat}}) ->
    wrap_in_frame(<<?RESPONSE:1,
                    ?COMMAND_TUNE:15,
                    ?VERSION_1:16,
                    FrameMax:32,
                    Heartbeat:32>>);
frame({response, Corr, {open, ResponseCode, ConnectionProperties}}) ->
    ConnPropsCount = map_size(ConnectionProperties),
    ConnectionPropertiesBin =
        case ConnPropsCount of
            0 ->
                <<>>;
            _ ->
                PropsBin = generate_map(ConnectionProperties),
                [<<ConnPropsCount:32>>, PropsBin]
        end,
    wrap_in_frame([<<?RESPONSE:1,
                     ?COMMAND_OPEN:15,
                     ?VERSION_1:16,
                     Corr:32,
                     ResponseCode:16>>,
                   ConnectionPropertiesBin]);
frame({response, CorrelationId, Body}) ->
    {CommandId, BodyBin} = response_body(Body),
    wrap_in_frame([<<?RESPONSE:1,
                     CommandId:15,
                     ?VERSION_1:16,
                     CorrelationId:32>>,
                   BodyBin]);
frame(Command) ->
    exit({not_impl, Command}).

response_body({peer_properties, Code, Props}) ->
    Init = <<Code:16, (maps:size(Props)):32>>,
    {command_id(peer_properties),
     maps:fold(fun(Key, Value, Acc) ->
                  KeySize = byte_size(Key),
                  ValueSize = byte_size(Value),
                  <<Acc/binary,
                    KeySize:16,
                    Key:KeySize/binary,
                    ValueSize:16,
                    Value:ValueSize/binary>>
               end,
               Init, Props)};
response_body({sasl_handshake, Code, Mechanisms}) ->
    MechanismsBin =
        lists:foldl(fun(M, Acc) ->
                       Size = byte_size(M),
                       <<Acc/binary, Size:16, M:Size/binary>>
                    end,
                    <<>>, Mechanisms),
    MechanismsCount = length(Mechanisms),
    {command_id(sasl_handshake),
     <<Code:16, MechanismsCount:32, MechanismsBin/binary>>};
response_body({sasl_authenticate = Tag, Code, Challenge}) ->
    {command_id(Tag),
     case Challenge of
         <<>> ->
             <<Code:16>>;
         _ ->
             <<Code:16, ?STRING(Challenge)>>
     end};
response_body({query_publisher_sequence = Tag, Code, Sequence}) ->
    {command_id(Tag), <<Code:16, Sequence:64>>};
response_body({query_offset = Tag, Code, Offset}) ->
    {command_id(Tag), <<Code:16, Offset:64>>};
response_body({metadata = Tag, Endpoints, Metadata}) ->
    NumEps = length(Endpoints),
    {_, EndpointsBin} =
        lists:foldl(fun({Host, Port}, {Index, Acc}) ->
                       HostLength = byte_size(Host),
                       {Index + 1,
                        <<Acc/binary,
                          Index:16,
                          HostLength:16,
                          Host:HostLength/binary,
                          Port:32>>}
                    end,
                    {0, <<NumEps:32>>}, Endpoints),
    MetadataBin =
        maps:fold(fun (Stream, Info, Acc) when is_atom(Info) ->
                          Code =
                              case Info of
                                  stream_not_found ->
                                      ?RESPONSE_CODE_STREAM_DOES_NOT_EXIST;
                                  stream_not_available ->
                                      ?RESPONSE_CODE_STREAM_NOT_AVAILABLE
                              end,
                          StreamLength = byte_size(Stream),
                          [<<StreamLength:16,
                             Stream/binary,
                             Code:16,
                             (-1):16,
                             0:32>>
                           | Acc];
                      (Stream, {LeaderEp, ReplicaEps}, Acc) ->
                          LeaderIndex = element_index(LeaderEp, Endpoints),
                          ReplicasBin =
                              iolist_to_binary(lists:foldr(fun(Repl, A) ->
                                                              I =
                                                                  element_index(Repl,
                                                                                Endpoints),
                                                              [<<I:16>> | A]
                                                           end,
                                                           [], ReplicaEps)),
                          StreamLength = byte_size(Stream),
                          ReplicasCount = length(ReplicaEps),
                          [<<StreamLength:16,
                             Stream/binary,
                             ?RESPONSE_CODE_OK:16,
                             LeaderIndex:16,
                             ReplicasCount:32,
                             ReplicasBin/binary>>
                           | Acc]
                  end,
                  [], Metadata),

    NumStreams = map_size(Metadata),
    {command_id(Tag), [EndpointsBin, <<NumStreams:32>>, MetadataBin]};
response_body({route = Tag, Code, Streams}) ->
    StreamsBin = [<<?STRING(Stream)>> || Stream <- Streams],
    {command_id(Tag), [<<Code:16, (length(Streams)):32>>, StreamsBin]};
response_body({partitions = Tag, Code, Streams}) ->
    StreamsBin = [<<?STRING(Stream)>> || Stream <- Streams],
    {command_id(Tag), [<<Code:16, (length(Streams)):32>>, StreamsBin]};
response_body({consumer_update = Tag, Code, OffsetSpec}) ->
    OffsetSpecBin =
        case OffsetSpec of
            none ->
                <<?OFFSET_TYPE_NONE:16>>;
            first ->
                <<?OFFSET_TYPE_FIRST:16>>;
            last ->
                <<?OFFSET_TYPE_LAST:16>>;
            next ->
                <<?OFFSET_TYPE_NEXT:16>>;
            Offset when is_integer(Offset) ->
                <<?OFFSET_TYPE_OFFSET, Offset:64/unsigned>>;
            {timestamp, Ts} ->
                <<?OFFSET_TYPE_TIMESTAMP, Ts:64/signed>>
        end,
    {command_id(Tag), [<<Code:16, OffsetSpecBin/binary>>]};
response_body({exchange_command_versions = Tag, Code,
               CommandVersions}) ->
    CommandVersionsBin =
        lists:foldr(fun({Command, MinVersion, MaxVersion}, Acc) ->
                       CommandId = command_id(Command),
                       [<<CommandId:16, MinVersion:16, MaxVersion:16>> | Acc]
                    end,
                    [], CommandVersions),
    {command_id(Tag),
     [<<Code:16, (length(CommandVersions)):32>>, CommandVersionsBin]};
response_body({stream_stats = Tag, Code, Stats}) ->
    Init = <<Code:16, (maps:size(Stats)):32>>,
    {command_id(Tag),
     maps:fold(fun(Key, Value, Acc) ->
                  KeySize = byte_size(Key),
                  <<Acc/binary,
                    KeySize:16,
                    Key:KeySize/binary,
                    Value:64/signed>>
               end,
               Init, Stats)}.

request_body({declare_publisher = Tag,
              PublisherId,
              WriterRef,
              Stream}) ->
    {Tag, <<PublisherId:8, ?STRING(WriterRef), ?STRING(Stream)>>};
request_body({query_publisher_sequence = Tag, WriterRef, Stream}) ->
    {Tag, <<?STRING(WriterRef), ?STRING(Stream)>>};
request_body({delete_publisher = Tag, PublisherId}) ->
    {Tag, <<PublisherId:8>>};
request_body({subscribe,
              SubscriptionId,
              Stream,
              OffsetSpec,
              Credit}) ->
    request_body({subscribe,
                  SubscriptionId,
                  Stream,
                  OffsetSpec,
                  Credit,
                  #{}});
request_body({subscribe = Tag,
              SubscriptionId,
              Stream,
              OffsetSpec,
              Credit,
              Properties}) ->
    Data =
        case OffsetSpec of
            first ->
                <<?OFFSET_TYPE_FIRST:16, Credit:16>>;
            last ->
                <<?OFFSET_TYPE_LAST:16, Credit:16>>;
            next ->
                <<?OFFSET_TYPE_NEXT:16, Credit:16>>;
            Offset when is_integer(Offset) ->
                <<?OFFSET_TYPE_OFFSET:16, Offset:64/unsigned, Credit:16>>;
            {timestamp, Timestamp} ->
                <<?OFFSET_TYPE_TIMESTAMP:16, Timestamp:64/signed, Credit:16>>
        end,
    PropertiesBin =
        case map_size(Properties) of
            0 ->
                <<>>;
            _ ->
                PropsBin = generate_map(Properties),
                [<<(map_size(Properties)):32>>, PropsBin]
        end,
    {Tag,
     [<<SubscriptionId:8, ?STRING(Stream), Data/binary>> | PropertiesBin]};
request_body({store_offset = Tag, OffsetRef, Stream, Offset}) ->
    {Tag, <<?STRING(OffsetRef), ?STRING(Stream), Offset:64>>};
request_body({query_offset = Tag, OffsetRef, Stream}) ->
    {Tag, <<?STRING(OffsetRef), ?STRING(Stream)>>};
request_body({unsubscribe = Tag, SubscriptionId}) ->
    {Tag, <<SubscriptionId:8>>};
request_body({create_stream = Tag, Stream, Args}) ->
    ArgsBin = generate_map(Args),
    {Tag, [<<?STRING(Stream), (map_size(Args)):32>>, ArgsBin]};
request_body({delete_stream = Tag, Stream}) ->
    {Tag, <<?STRING(Stream)>>};
request_body({metadata = Tag, Streams}) ->
    StreamsBin = generate_list(Streams),
    {Tag, [<<(length(Streams)):32>>, StreamsBin]};
request_body({peer_properties = Tag, Props}) ->
    PropsBin = generate_map(Props),
    {Tag, [<<(map_size(Props)):32>>, PropsBin]};
request_body(sasl_handshake = Tag) ->
    {Tag, <<>>};
request_body({sasl_authenticate = Tag, Mechanism, SaslBin}) ->
    {Tag,
     case SaslBin of
         <<>> ->
             <<?STRING(Mechanism), (-1):32/signed>>;
         _ ->
             <<?STRING(Mechanism), ?DATASTR(SaslBin)>>
     end};
request_body({open = Tag, Vhost}) ->
    {Tag, <<?STRING(Vhost)>>};
request_body({close = Tag, Code, Reason}) ->
    {Tag, <<Code:16, ?STRING(Reason)>>};
request_body({route = Tag, RoutingKey, SuperStream}) ->
    {Tag, <<?STRING(RoutingKey), ?STRING(SuperStream)>>};
request_body({partitions = Tag, SuperStream}) ->
    {Tag, <<?STRING(SuperStream)>>};
request_body({consumer_update = Tag, SubscriptionId, Active}) ->
    ActiveBin =
        case Active of
            true ->
                1;
            false ->
                0
        end,
    {Tag, <<SubscriptionId:8, ActiveBin:8>>};
request_body({exchange_command_versions = Tag, CommandVersions}) ->
    CommandVersionsBin =
        lists:foldl(fun({Command, MinVersion, MaxVersion}, Acc) ->
                       CommandId = command_id(Command),
                       [<<CommandId:16, MinVersion:16, MaxVersion:16>> | Acc]
                    end,
                    [], CommandVersions),
    CommandVersionsLength = length(CommandVersions),
    {Tag, [<<CommandVersionsLength:32>>, CommandVersionsBin]};
request_body({stream_stats = Tag, Stream}) ->
    {Tag, <<?STRING(Stream)>>};
request_body({create_super_stream = Tag, SuperStream, Partitions, BindingKeys, Args}) ->
    PartitionsBin = generate_list(Partitions),
    BindingKeysBin = generate_list(BindingKeys),
    ArgsBin = generate_map(Args),
    {Tag, [<<?STRING(SuperStream), (length(Partitions)):32>>, PartitionsBin,
           <<(length(BindingKeys)):32>>, BindingKeysBin,
           <<(map_size(Args)):32>>, ArgsBin]};
request_body({delete_super_stream = Tag, SuperStream}) ->
    {Tag, <<?STRING(SuperStream)>>}.

append_data(Prev, Data) when is_binary(Prev) ->
    [Prev, Data];
append_data(Prev, Data) when is_list(Prev) ->
    Prev ++ [Data].

wrap_in_frame(IOData) ->
    Size = iolist_size(IOData),
    [<<Size:32>> | IOData].

parse_command(<<?REQUEST:1, _:15, _/binary>> = Bin) ->
    parse_request(Bin);
parse_command(<<?RESPONSE:1, _:15, _/binary>> = Bin) ->
    parse_response(Bin);
parse_command(Data) when is_list(Data) ->
    %% TODO: most commands are rare or small and likely to be a single
    %% binary, however publish and delivery should be parsed from the
    %% iodata rather than turned into a binary
    parse_command(iolist_to_binary(Data)).

-spec parse_request(binary()) -> command().
parse_request(<<?REQUEST:1,
                ?COMMAND_PUBLISH:15,
                ?VERSION_1:16,
                PublisherId:8/unsigned,
                MessageCount:32,
                Messages/binary>>) ->
    {publish, PublisherId, MessageCount, Messages};
parse_request(<<?REQUEST:1,
                ?COMMAND_PUBLISH:15,
                ?VERSION_2:16,
                PublisherId:8/unsigned,
                MessageCount:32,
                Messages/binary>>) ->
    {publish_v2, PublisherId, MessageCount, Messages};
parse_request(<<?REQUEST:1,
                ?COMMAND_PUBLISH_CONFIRM:15,
                ?VERSION_1:16,
                PublisherId:8,
                _Count:32,
                PublishingIds/binary>>) ->
    {publish_confirm, PublisherId, list_of_longs(PublishingIds)};
parse_request(<<?REQUEST:1,
                ?COMMAND_DELIVER:15,
                ?VERSION_1:16,
                SubscriptionId:8,
                Chunk/binary>>) ->
    {deliver, SubscriptionId, Chunk};
parse_request(<<?REQUEST:1,
                ?COMMAND_DELIVER:15,
                ?VERSION_2:16,
                SubscriptionId:8,
                CommittedChunkId:64,
                Chunk/binary>>) ->
    {deliver_v2, SubscriptionId, CommittedChunkId, Chunk};
parse_request(<<?REQUEST:1,
                ?COMMAND_CREDIT:15,
                ?VERSION_1:16,
                SubscriptionId:8,
                Credit:16/signed>>) ->
    {credit, SubscriptionId, Credit};
parse_request(<<?REQUEST:1,
                ?COMMAND_PUBLISH_ERROR:15,
                ?VERSION_1:16,
                PublisherId:8,
                _Count:32,
                DetailsBin/binary>>) ->
    %% TODO: change protocol to match
    [{_, ErrCode} | _] = Details = list_of_longcodes(DetailsBin),
    {PublishingIds, _} = lists:unzip(Details),
    {publish_error, PublisherId, ErrCode, PublishingIds};
parse_request(<<?REQUEST:1,
                ?COMMAND_METADATA_UPDATE:15,
                ?VERSION_1:16,
                ResponseCode:16,
                StreamSize:16,
                Stream:StreamSize/binary>>) ->
    {metadata_update, Stream, ResponseCode};
parse_request(<<?REQUEST:1,
                ?COMMAND_STORE_OFFSET:15,
                ?VERSION_1:16,
                ?STRING(RefSize, OffsetRef),
                ?STRING(SSize, Stream),
                Offset:64>>) ->
    {store_offset, OffsetRef, Stream, Offset};
parse_request(<<?REQUEST:1, ?COMMAND_HEARTBEAT:15, ?VERSION_1:16>>) ->
    heartbeat;
parse_request(<<?REQUEST:1,
                ?COMMAND_DECLARE_PUBLISHER:15,
                ?VERSION_1:16,
                CorrelationId:32,
                PublisherId:8,
                ?STRING(WriterRefSize, WriterRef),
                ?STRING(StreamSize, Stream)>>) ->
    request(CorrelationId,
            {declare_publisher, PublisherId, WriterRef, Stream});
parse_request(<<?REQUEST:1,
                ?COMMAND_QUERY_PUBLISHER_SEQUENCE:15,
                ?VERSION_1:16,
                CorrelationId:32,
                ?STRING(WSize, WriterReference),
                ?STRING(SSize, Stream)>>) ->
    request(CorrelationId,
            {query_publisher_sequence, WriterReference, Stream});
parse_request(<<?REQUEST:1,
                ?COMMAND_DELETE_PUBLISHER:15,
                ?VERSION_1:16,
                CorrelationId:32,
                PublisherId:8>>) ->
    request(CorrelationId, {delete_publisher, PublisherId});
parse_request(<<?REQUEST:1,
                ?COMMAND_SUBSCRIBE:15,
                ?VERSION_1:16,
                CorrelationId:32,
                SubscriptionId:8,
                ?STRING(StreamSize, Stream),
                OffsetType:16/signed,
                OffsetCreditProperties/binary>>) ->
    {OffsetSpec, Credit, PropsBin} =
        case OffsetType of
            ?OFFSET_TYPE_FIRST ->
                <<Crdt:16, PropertiesBin/binary>> = OffsetCreditProperties,
                {first, Crdt, PropertiesBin};
            ?OFFSET_TYPE_LAST ->
                <<Crdt:16, PropertiesBin/binary>> = OffsetCreditProperties,
                {last, Crdt, PropertiesBin};
            ?OFFSET_TYPE_NEXT ->
                <<Crdt:16, PropertiesBin/binary>> = OffsetCreditProperties,
                {next, Crdt, PropertiesBin};
            ?OFFSET_TYPE_OFFSET ->
                <<Offset:64/unsigned, Crdt:16, PropertiesBin/binary>> =
                    OffsetCreditProperties,
                {Offset, Crdt, PropertiesBin};
            ?OFFSET_TYPE_TIMESTAMP ->
                <<Timestamp:64/signed, Crdt:16, PropertiesBin/binary>> =
                    OffsetCreditProperties,
                {{timestamp, Timestamp}, Crdt, PropertiesBin}
        end,
    Properties =
        case PropsBin of
            <<>> ->
                #{};
            <<_Count:32, Bin/binary>> ->
                parse_map(Bin, #{});
            _ ->
                logger:warning("Incorrect binary for subscription properties: ~w",
                               [PropsBin]),
                #{}
        end,
    request(CorrelationId,
            {subscribe,
             SubscriptionId,
             Stream,
             OffsetSpec,
             Credit,
             Properties});
parse_request(<<?REQUEST:1,
                ?COMMAND_QUERY_OFFSET:15,
                ?VERSION_1:16,
                CorrelationId:32,
                ?STRING(RefSize, OffsetRef),
                ?STRING(SSize, Stream)>>) ->
    request(CorrelationId, {query_offset, OffsetRef, Stream});
parse_request(<<?REQUEST:1,
                ?COMMAND_UNSUBSCRIBE:15,
                ?VERSION_1:16,
                CorrelationId:32,
                SubscriptionId:8>>) ->
    request(CorrelationId, {unsubscribe, SubscriptionId});
parse_request(<<?REQUEST:1,
                ?COMMAND_CREATE_STREAM:15,
                ?VERSION_1:16,
                CorrelationId:32,
                ?STRING(StreamSize, Stream),
                _ArgumentsCount:32,
                ArgumentsBinary/binary>>) ->
    Args = parse_map(ArgumentsBinary, #{}),
    request(CorrelationId, {create_stream, Stream, Args});
parse_request(<<?REQUEST:1,
                ?COMMAND_DELETE_STREAM:15,
                ?VERSION_1:16,
                CorrelationId:32,
                ?STRING(StreamSize, Stream)>>) ->
    request(CorrelationId, {delete_stream, Stream});
parse_request(<<?REQUEST:1,
                ?COMMAND_METADATA:15,
                ?VERSION_1:16,
                CorrelationId:32,
                _StreamCount:32,
                BinaryStreams/binary>>) ->
    Streams = list_of_strings(BinaryStreams),
    request(CorrelationId, {metadata, Streams});
parse_request(<<?REQUEST:1,
                ?COMMAND_PEER_PROPERTIES:15,
                ?VERSION_1:16,
                CorrelationId:32,
                _PropertiesCount:32,
                PropertiesBinary/binary>>) ->
    Props = parse_map(PropertiesBinary, #{}),
    request(CorrelationId, {peer_properties, Props});
parse_request(<<?REQUEST:1,
                ?COMMAND_SASL_HANDSHAKE:15,
                ?VERSION_1:16,
                CorrelationId:32>>) ->
    request(CorrelationId, sasl_handshake);
parse_request(<<?REQUEST:1,
                ?COMMAND_SASL_AUTHENTICATE:15,
                ?VERSION_1:16,
                CorrelationId:32,
                ?STRING(MechanismSize, Mechanism),
                SaslFragment/binary>>) ->
    SaslBin =
        case SaslFragment of
            <<(-1):32/signed>> ->
                <<>>;
            <<SaslBinaryLength:32, SaslBinary:SaslBinaryLength/binary>> ->
                SaslBinary
        end,
    request(CorrelationId, {sasl_authenticate, Mechanism, SaslBin});
parse_request(<<?REQUEST:1,
                ?COMMAND_TUNE:15,
                ?VERSION_1:16,
                FrameMax:32,
                Heartbeat:32>>) ->
    {tune, FrameMax, Heartbeat};
parse_request(<<?REQUEST:1,
                ?COMMAND_OPEN:15,
                ?VERSION_1:16,
                CorrelationId:32,
                ?STRING(VhostSize, VirtualHost)>>) ->
    request(CorrelationId, {open, VirtualHost});
parse_request(<<?REQUEST:1,
                ?COMMAND_CLOSE:15,
                ?VERSION_1:16,
                CorrelationId:32,
                CloseCode:16,
                ?STRING(ReasonSize, Reason)>>) ->
    request(CorrelationId, {close, CloseCode, Reason});
parse_request(<<?REQUEST:1,
                ?COMMAND_ROUTE:15,
                ?VERSION_1:16,
                CorrelationId:32,
                ?STRING(RKeySize, RoutingKey),
                ?STRING(StreamSize, SuperStream)>>) ->
    request(CorrelationId, {route, RoutingKey, SuperStream});
parse_request(<<?REQUEST:1,
                ?COMMAND_PARTITIONS:15,
                ?VERSION_1:16,
                CorrelationId:32,
                ?STRING(StreamSize, SuperStream)>>) ->
    request(CorrelationId, {partitions, SuperStream});
parse_request(<<?REQUEST:1,
                ?COMMAND_CONSUMER_UPDATE:15,
                ?VERSION_1:16,
                CorrelationId:32,
                SubscriptionId:8,
                ActiveBin:8>>) ->
    Active =
        case ActiveBin of
            0 ->
                false;
            1 ->
                true
        end,
    request(CorrelationId, {consumer_update, SubscriptionId, Active});
parse_request(<<?REQUEST:1,
                ?COMMAND_EXCHANGE_COMMAND_VERSIONS:15,
                ?VERSION_1:16,
                CorrelationId:32,
                _CommandVersionsCount:32,
                CommandVersionsBin/binary>>) ->
    CommandVersions = parse_command_versions(CommandVersionsBin),
    request(CorrelationId, {exchange_command_versions, CommandVersions});
parse_request(<<?REQUEST:1,
                ?COMMAND_STREAM_STATS:15,
                ?VERSION_1:16,
                CorrelationId:32,
                ?STRING(StreamSize, Stream)>>) ->
    request(CorrelationId, {stream_stats, Stream});
parse_request(<<?REQUEST:1,
                ?COMMAND_CREATE_SUPER_STREAM:15,
                ?VERSION_1:16,
                CorrelationId:32,
                ?STRING(StreamSize, Stream),
                PartitionsCount:32,
                Rest0/binary>>) ->
    {Partitions, <<BindingKeysCount:32, Rest1/binary>>} = list_of_strings(PartitionsCount, Rest0),
    {BindingKeys, <<_ArgumentsCount:32, Rest2/binary>>} = list_of_strings(BindingKeysCount, Rest1),
    Args = parse_map(Rest2, #{}),
    request(CorrelationId, {create_super_stream, Stream, Partitions, BindingKeys, Args});
parse_request(<<?REQUEST:1,
                ?COMMAND_DELETE_SUPER_STREAM:15,
                ?VERSION_1:16,
                CorrelationId:32,
                ?STRING(SuperStreamSize, SuperStream)>>) ->
    request(CorrelationId, {delete_super_stream, SuperStream});
parse_request(Bin) ->
    {unknown, Bin}.

parse_response(<<?RESPONSE:1,
                 CommandId:15,
                 ?VERSION_1:16,
                 CorrelationId:32,
                 ResponseCode:16>>) ->
    {response, CorrelationId,
     {parse_command_id(CommandId), ResponseCode}};
parse_response(<<?RESPONSE:1,
                 ?COMMAND_TUNE:15,
                 ?VERSION_1:16,
                 FrameMax:32,
                 Heartbeat:32>>) ->
    %% fake correlation id
    {response, 0, {tune, FrameMax, Heartbeat}};
parse_response(<<?RESPONSE:1,
                 CommandId:15,
                 ?VERSION_1:16,
                 CorrelationId:32,
                 Data/binary>>) ->
    {response, CorrelationId, parse_response_body(CommandId, Data)};
parse_response(<<?RESPONSE:1,
                 ?COMMAND_CREDIT:15,
                 ?VERSION_1:16,
                 ResponseCode:16,
                 SubscriptionId:8>>) ->
    {response, 0, {credit, ResponseCode, SubscriptionId}};
parse_response(Bin) ->
    {unknown, Bin}.

parse_response_body(?COMMAND_OPEN, <<ResponseCode:16>>) ->
    {open, ResponseCode, #{}};
parse_response_body(?COMMAND_OPEN,
                    <<ResponseCode:16, _ConnectionPropertiesCount:32,
                      ConnectionProperties/binary>>) ->
    {open, ResponseCode, parse_map(ConnectionProperties, #{})};
parse_response_body(?COMMAND_QUERY_PUBLISHER_SEQUENCE,
                    <<ResponseCode:16, Sequence:64>>) ->
    {query_publisher_sequence, ResponseCode, Sequence};
parse_response_body(?COMMAND_QUERY_OFFSET,
                    <<ResponseCode:16, Offset:64>>) ->
    {query_offset, ResponseCode, Offset};
parse_response_body(?COMMAND_METADATA,
                    <<NumNodes:32, Data/binary>>) ->
    {NodesLookup, <<_:32, MetadataBin/binary>>} =
        parse_nodes(Data, NumNodes, #{}),
    Nodes = maps:values(NodesLookup),
    Metadata = parse_meta(MetadataBin, NodesLookup, #{}),
    {metadata, Nodes, Metadata};
parse_response_body(?COMMAND_PEER_PROPERTIES,
                    <<ResponseCode:16, _Count:32, PropertiesBin/binary>>) ->
    Props = parse_map(PropertiesBin, #{}),
    {peer_properties, ResponseCode, Props};
parse_response_body(?COMMAND_SASL_HANDSHAKE,
                    <<ResponseCode:16, _Count:32, MechanismsBin/binary>>) ->
    Props = list_of_strings(MechanismsBin),
    {sasl_handshake, ResponseCode, Props};
parse_response_body(?COMMAND_SASL_AUTHENTICATE,
                    <<ResponseCode:16, ChallengeBin/binary>>) ->
    Challenge =
        case ChallengeBin of
            <<?STRING(CSize, Chall)>> ->
                Chall;
            <<>> ->
                <<>>
        end,
    {sasl_authenticate, ResponseCode, Challenge};
parse_response_body(?COMMAND_ROUTE,
                    <<ResponseCode:16, _Count:32, StreamsBin/binary>>) ->
    Streams = list_of_strings(StreamsBin),
    {route, ResponseCode, Streams};
parse_response_body(?COMMAND_PARTITIONS,
                    <<ResponseCode:16, _Count:32, PartitionsBin/binary>>) ->
    Partitions = list_of_strings(PartitionsBin),
    {partitions, ResponseCode, Partitions};
parse_response_body(?COMMAND_CONSUMER_UPDATE,
                    <<ResponseCode:16, OffsetType:16/signed,
                      OffsetValue/binary>>) ->
    OffsetSpec = offset_spec(OffsetType, OffsetValue),
    {consumer_update, ResponseCode, OffsetSpec};
parse_response_body(?COMMAND_EXCHANGE_COMMAND_VERSIONS,
                    <<ResponseCode:16, _CommandVersionsCount:32,
                      CommandVersionsBin/binary>>) ->
    CommandVersions = parse_command_versions(CommandVersionsBin),
    {exchange_command_versions, ResponseCode, CommandVersions};
parse_response_body(?COMMAND_STREAM_STATS,
                    <<ResponseCode:16, _Count:32, StatsBin/binary>>) ->
    Info = parse_int_map(StatsBin, #{}),
    {stream_stats, ResponseCode, Info}.

offset_spec(OffsetType, OffsetValueBin) ->
    case OffsetType of
        ?OFFSET_TYPE_NONE ->
            none;
        ?OFFSET_TYPE_FIRST ->
            first;
        ?OFFSET_TYPE_LAST ->
            last;
        ?OFFSET_TYPE_NEXT ->
            next;
        ?OFFSET_TYPE_OFFSET ->
            <<Offset:64/unsigned>> = OffsetValueBin,
            Offset;
        ?OFFSET_TYPE_TIMESTAMP ->
            <<Timestamp:64/signed>> = OffsetValueBin,
            {timestamp, Timestamp}
    end.

request(Corr, Cmd) ->
    {request, Corr, Cmd}.

parse_meta(<<>>, _Nodes, Acc) ->
    Acc;
parse_meta(<<?STRING(StreamSize, Stream),
             Code:16,
             LeaderIndex:16,
             ReplicaCount:32,
             ReplicaIndexBin:(ReplicaCount * 2)/binary,
             Rem/binary>>,
           Nodes, Acc) ->
    StreamDetail =
        case Code of
            ?RESPONSE_CODE_OK ->
                %% TODO: 65535 is the magic value for a leader
                %% that is not found everything else should crash
                Leader = maps:get(LeaderIndex, Nodes, undefined),
                Replicas = maps:with(list_of_shorts(ReplicaIndexBin), Nodes),
                {Leader, maps:values(Replicas)};
            ?RESPONSE_CODE_STREAM_DOES_NOT_EXIST ->
                stream_not_found;
            ?RESPONSE_CODE_STREAM_NOT_AVAILABLE ->
                stream_not_available
        end,
    parse_meta(Rem, Nodes, Acc#{Stream => StreamDetail}).

parse_nodes(Rem, 0, Acc) ->
    {Acc, Rem};
parse_nodes(<<Index:16,
              ?STRING(HostSize, Host),
              Port:32,
              Rem/binary>>,
            C, Acc) ->
    parse_nodes(Rem, C - 1, Acc#{Index => {Host, Port}}).

parse_command_versions(<<>>) ->
    [];
parse_command_versions(<<Key:16,
                         MinVersion:16,
                         MaxVersion:16,
                         Rem/binary>>) ->
    [{parse_command_id(Key), MinVersion, MaxVersion}
     | parse_command_versions(Rem)].

parse_map(<<>>, Acc) ->
    Acc;
parse_map(<<?STRING(KeySize, Key), ?STRING(ValSize, Value),
            Rem/binary>>,
          Acc) ->
    parse_map(Rem, Acc#{Key => Value}).

parse_int_map(<<>>, Acc) ->
    Acc;
parse_int_map(<<?STRING(KeySize, Key), Value:64, Rem/binary>>, Acc) ->
    parse_int_map(Rem, Acc#{Key => Value}).

generate_list(List) ->
    lists:foldr(fun(E, Acc) -> [<<?STRING(E)>> | Acc] end, [],
                List).

generate_map(Map) ->
    maps:fold(fun(K, V, Acc) -> [<<?STRING(K), ?STRING(V)>> | Acc] end,
              [], Map).

list_of_strings(Count, Bin) ->
    list_of_strings(Count, [], Bin).

list_of_strings(_, Acc, <<>>) ->
    {lists:reverse(Acc), <<>>};
list_of_strings(0, Acc, Rest) ->
    {lists:reverse(Acc), Rest};
list_of_strings(Count, Acc, <<?STRING(Size, String), Rem/binary>>) ->
    list_of_strings(Count - 1, [String | Acc], Rem).

list_of_strings(<<>>) ->
    [];
list_of_strings(<<?STRING(Size, String), Rem/binary>>) ->
    [String | list_of_strings(Rem)].

list_of_longs(<<>>) ->
    [];
list_of_longs(<<I:64, Rem/binary>>) ->
    [I | list_of_longs(Rem)].

list_of_shorts(<<>>) ->
    [];
list_of_shorts(<<I:16, Rem/binary>>) ->
    [I | list_of_shorts(Rem)].

list_of_longcodes(<<>>) ->
    [];
list_of_longcodes(<<I:64, C:16, Rem/binary>>) ->
    [{I, C} | list_of_longcodes(Rem)].

command_id(declare_publisher) ->
    ?COMMAND_DECLARE_PUBLISHER;
command_id(publish) ->
    ?COMMAND_PUBLISH;
command_id(publish_v2) ->
    ?COMMAND_PUBLISH;
command_id(publish_confirm) ->
    ?COMMAND_PUBLISH_CONFIRM;
command_id(publish_error) ->
    ?COMMAND_PUBLISH_ERROR;
command_id(query_publisher_sequence) ->
    ?COMMAND_QUERY_PUBLISHER_SEQUENCE;
command_id(delete_publisher) ->
    ?COMMAND_DELETE_PUBLISHER;
command_id(subscribe) ->
    ?COMMAND_SUBSCRIBE;
command_id(deliver) ->
    ?COMMAND_DELIVER;
command_id(deliver_v2) ->
    ?COMMAND_DELIVER;
command_id(credit) ->
    ?COMMAND_CREDIT;
command_id(store_offset) ->
    ?COMMAND_STORE_OFFSET;
command_id(query_offset) ->
    ?COMMAND_QUERY_OFFSET;
command_id(unsubscribe) ->
    ?COMMAND_UNSUBSCRIBE;
command_id(create_stream) ->
    ?COMMAND_CREATE_STREAM;
command_id(delete_stream) ->
    ?COMMAND_DELETE_STREAM;
command_id(metadata) ->
    ?COMMAND_METADATA;
command_id(metadata_update) ->
    ?COMMAND_METADATA_UPDATE;
command_id(peer_properties) ->
    ?COMMAND_PEER_PROPERTIES;
command_id(sasl_handshake) ->
    ?COMMAND_SASL_HANDSHAKE;
command_id(sasl_authenticate) ->
    ?COMMAND_SASL_AUTHENTICATE;
command_id(tune) ->
    ?COMMAND_TUNE;
command_id(open) ->
    ?COMMAND_OPEN;
command_id(close) ->
    ?COMMAND_CLOSE;
command_id(heartbeat) ->
    ?COMMAND_HEARTBEAT;
command_id(route) ->
    ?COMMAND_ROUTE;
command_id(partitions) ->
    ?COMMAND_PARTITIONS;
command_id(consumer_update) ->
    ?COMMAND_CONSUMER_UPDATE;
command_id(exchange_command_versions) ->
    ?COMMAND_EXCHANGE_COMMAND_VERSIONS;
command_id(stream_stats) ->
    ?COMMAND_STREAM_STATS;
command_id(create_super_stream) ->
    ?COMMAND_CREATE_SUPER_STREAM;
command_id(delete_super_stream) ->
    ?COMMAND_DELETE_SUPER_STREAM.

parse_command_id(?COMMAND_DECLARE_PUBLISHER) ->
    declare_publisher;
parse_command_id(?COMMAND_PUBLISH) ->
    publish;
parse_command_id(?COMMAND_PUBLISH_CONFIRM) ->
    publish_confirm;
parse_command_id(?COMMAND_PUBLISH_ERROR) ->
    publish_error;
parse_command_id(?COMMAND_QUERY_PUBLISHER_SEQUENCE) ->
    query_publisher_sequence;
parse_command_id(?COMMAND_DELETE_PUBLISHER) ->
    delete_publisher;
parse_command_id(?COMMAND_SUBSCRIBE) ->
    subscribe;
parse_command_id(?COMMAND_DELIVER) ->
    deliver;
parse_command_id(?COMMAND_CREDIT) ->
    credit;
parse_command_id(?COMMAND_STORE_OFFSET) ->
    store_offset;
parse_command_id(?COMMAND_QUERY_OFFSET) ->
    query_offset;
parse_command_id(?COMMAND_UNSUBSCRIBE) ->
    unsubscribe;
parse_command_id(?COMMAND_CREATE_STREAM) ->
    create_stream;
parse_command_id(?COMMAND_DELETE_STREAM) ->
    delete_stream;
parse_command_id(?COMMAND_METADATA) ->
    metadata;
parse_command_id(?COMMAND_METADATA_UPDATE) ->
    metadata_update;
parse_command_id(?COMMAND_PEER_PROPERTIES) ->
    peer_properties;
parse_command_id(?COMMAND_SASL_HANDSHAKE) ->
    sasl_handshake;
parse_command_id(?COMMAND_SASL_AUTHENTICATE) ->
    sasl_authenticate;
parse_command_id(?COMMAND_TUNE) ->
    tune;
parse_command_id(?COMMAND_OPEN) ->
    open;
parse_command_id(?COMMAND_CLOSE) ->
    close;
parse_command_id(?COMMAND_HEARTBEAT) ->
    heartbeat;
parse_command_id(?COMMAND_ROUTE) ->
    route;
parse_command_id(?COMMAND_PARTITIONS) ->
    partitions;
parse_command_id(?COMMAND_CONSUMER_UPDATE) ->
    consumer_update;
parse_command_id(?COMMAND_EXCHANGE_COMMAND_VERSIONS) ->
    exchange_command_versions;
parse_command_id(?COMMAND_STREAM_STATS) ->
    stream_stats;
parse_command_id(?COMMAND_CREATE_SUPER_STREAM) ->
    create_super_stream;
parse_command_id(?COMMAND_DELETE_SUPER_STREAM) ->
    delete_super_stream.

element_index(Element, List) ->
    element_index(Element, List, 0).

element_index(Element, [Element | _], N) ->
    N;
element_index(Element, [_ | List], N) ->
    element_index(Element, List, N + 1);
element_index(_, _, _) ->
    -1.
