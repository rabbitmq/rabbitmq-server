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
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_stream_utils).

-define(MAX_SUPER_STREAM_PARTITIONS, 1000).

-define(ENDPOINT_TIMEOUT, 10_000).

%% Sub-entry compression types 0-4 are assigned (none, gzip, snappy, lz4, zstd);
%% 5-7 are not, and a batch declaring one of them can never be decoded by a consumer.
-define(MAX_KNOWN_COMPRESSION_TYPE, 4).

%% API
-export([enforce_correct_name/1,
         write_messages/8,
         parse_map/2,
         auth_mechanisms/1,
         auth_mechanism_to_module/2,
         check_configure_permitted/2,
         check_write_permitted/2,
         check_read_permitted/3,
         check_read_or_write_permitted/2,
         extract_stream_list/2,
         sort_partitions/1,
         strip_cr_lf/1,
         consumer_activity_status/2,
         filter_defined/1,
         filter_spec/1,
         command_versions/0,
         check_super_stream_management_permitted/4,
         check_super_stream_permitted/3,
         offset_lag/4,
         consumer_offset/3,
         validate_super_stream_max_partitions/1,
         max_super_stream_partitions/0,
         node_endpoints/2]).

%% super stream partition helpers
-export([streams_from_partitions/2,
         streams_from_binding_keys/2,
         binding_keys/1,
         routing_keys/1]).

%% for tests
-export([validate_super_stream_max_partitions/2,
         node_endpoints/3,
         classify_endpoint/3,
         transient_endpoint_error/1]).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbitmq_stream_common/include/rabbit_stream.hrl").
-include_lib("kernel/include/logger.hrl").

enforce_correct_name(Name) ->
    % from rabbit_channel
    StrippedName =
        binary:replace(Name, [<<"\n">>, <<"\r">>], <<"">>, [global]),
    case check_name(StrippedName) of
        ok ->
            {ok, StrippedName};
        error ->
            error
    end.

check_name(<<"amq.", _/binary>>) ->
    error;
check_name(<<"">>) ->
    error;
check_name(_Name) ->
    ok.

-spec write_messages(rabbit_stream_core:command_version(), pid(),
                     undefined | binary(), byte(),
                     integer(), binary(), non_neg_integer(), binary()) ->
    [integer()].
write_messages(_Version, _ClusterLeader, _PublisherRef, _PublisherId, _InternalId, <<>>,
               _MaxUncompressedSize, _Stream) ->
    [];
write_messages(?VERSION_1 = V, ClusterLeader,
               PublisherRef,
               PublisherId,
               InternalId,
               <<PublishingId:64,
                 0:1,
                 MessageSize:31,
                 Message:MessageSize/binary,
                 Rest/binary>>,
               MaxUncompressedSize, Stream) ->
    write_messages0(V, ClusterLeader, PublisherRef, PublisherId, InternalId,
                    PublishingId, Message, Rest, MaxUncompressedSize, Stream);
write_messages(?VERSION_1 = V, ClusterLeader,
               PublisherRef,
               PublisherId,
               InternalId,
               <<PublishingId:64,
                 1:1,
                 CompressionType:3,
                 _Unused:4,
                 MessageCount:16,
                 UncompressedSize:32,
                 BatchSize:32,
                 Batch:BatchSize/binary,
                 Rest/binary>>,
               MaxUncompressedSize, Stream) ->
    case validate_compressed_sub_batch(CompressionType, MessageCount,
                                       UncompressedSize, BatchSize,
                                       MaxUncompressedSize) of
        ok ->
            Data = {batch, MessageCount, CompressionType, UncompressedSize, Batch},
            write_messages0(V, ClusterLeader, PublisherRef, PublisherId, InternalId,
                            PublishingId, Data, Rest, MaxUncompressedSize, Stream);
        {error, Reason} ->
            ?LOG_WARNING("Rejecting sub-entry batch published to stream '~ts' "
                         "by publisher ~tp: ~tp. The rest of the publish "
                         "frame is rejected as well to avoid a gap in the "
                         "publishing ID sequence.",
                         [Stream, PublisherId, Reason]),
            [PublishingId | reject_remaining(V, Rest)]
    end;
write_messages(?VERSION_2 = V, ClusterLeader,
               PublisherRef,
               PublisherId,
               InternalId,
               <<PublishingId:64,
                 -1:16/signed,
                 0:1,
                 MessageSize:31,
                 Message:MessageSize/binary,
                 Rest/binary>>,
               MaxUncompressedSize, Stream) ->
    write_messages0(V, ClusterLeader, PublisherRef, PublisherId, InternalId,
                    PublishingId, Message, Rest, MaxUncompressedSize, Stream);
write_messages(?VERSION_2 = V, ClusterLeader,
               PublisherRef,
               PublisherId,
               InternalId,
               <<PublishingId:64,
                 FilterValueLength:16, FilterValue:FilterValueLength/binary,
                 0:1,
                 MessageSize:31,
                 Message:MessageSize/binary,
                 Rest/binary>>,
               MaxUncompressedSize, Stream) ->
    write_messages0(V, ClusterLeader, PublisherRef, PublisherId, InternalId,
                    PublishingId, {FilterValue, Message}, Rest, MaxUncompressedSize, Stream).

write_messages0(Vsn, ClusterLeader, PublisherRef, PublisherId, InternalId, PublishingId, Data,
                Rest, MaxUncompressedSize, Stream) ->
    Corr = case PublisherRef of
               undefined ->
                   %% we add the internal ID to detect late confirms from a stale publisher
                   {PublisherId, InternalId, PublishingId};
               _ ->
                   %% we cannot add the internal ID because the correlation ID must be an integer
                   %% when deduplication is activated.
                   PublishingId
           end,
    ok = osiris:write(ClusterLeader, PublisherRef, Corr, Data),
    write_messages(Vsn, ClusterLeader, PublisherRef, PublisherId, InternalId, Rest,
                   MaxUncompressedSize, Stream).

%% Only the ?VERSION_1 compressed sub-batch shape is validated, so only that shape
%% (and the plain ?VERSION_1 entry it can be interleaved with) can appear here.
reject_remaining(_V, <<>>) ->
    [];
reject_remaining(?VERSION_1 = V, <<PublishingId:64,
                                    0:1,
                                    MessageSize:31,
                                    _Message:MessageSize/binary,
                                    Rest/binary>>) ->
    [PublishingId | reject_remaining(V, Rest)];
reject_remaining(?VERSION_1 = V, <<PublishingId:64,
                                    1:1,
                                    _CompressionType:3,
                                    _Unused:4,
                                    _MessageCount:16,
                                    _UncompressedSize:32,
                                    BatchSize:32,
                                    _Batch:BatchSize/binary,
                                    Rest/binary>>) ->
    [PublishingId | reject_remaining(V, Rest)].

-spec validate_compressed_sub_batch(non_neg_integer(), non_neg_integer(),
                                    non_neg_integer(), non_neg_integer(),
                                    non_neg_integer()) -> ok | {error, term()}.
validate_compressed_sub_batch(CompressionType, MessageCount, UncompressedSize,
                              BatchSize, MaxUncompressedSize) ->
    maybe
        ok ?= check_uncompressed_size(UncompressedSize, MaxUncompressedSize),
        ok ?= check_message_count(MessageCount),
        ok ?= check_message_count_fits_uncompressed_size(MessageCount, UncompressedSize),
        ok ?= check_compression_type(CompressionType),
        check_batch_size(BatchSize, CompressionType)
    end.

check_uncompressed_size(UncompressedSize, MaxUncompressedSize)
  when UncompressedSize > MaxUncompressedSize ->
    {error, {uncompressed_size_exceeds_limit, UncompressedSize, MaxUncompressedSize}};
check_uncompressed_size(_UncompressedSize, _MaxUncompressedSize) ->
    ok.

check_message_count(0) ->
    {error, zero_message_count};
check_message_count(_MessageCount) ->
    ok.

%% every sub-entry carries at least its own 4-byte length prefix
check_message_count_fits_uncompressed_size(MessageCount, UncompressedSize)
  when MessageCount * 4 > UncompressedSize ->
    {error, {message_count_exceeds_uncompressed_size, MessageCount, UncompressedSize}};
check_message_count_fits_uncompressed_size(_MessageCount, _UncompressedSize) ->
    ok.

check_compression_type(CompressionType)
  when CompressionType > ?MAX_KNOWN_COMPRESSION_TYPE ->
    {error, {unknown_compression_type, CompressionType}};
check_compression_type(_CompressionType) ->
    ok.

check_batch_size(0, CompressionType) when CompressionType =/= 0 ->
    {error, empty_batch_with_compression};
check_batch_size(_BatchSize, _CompressionType) ->
    ok.

parse_map(<<>>, _Count) ->
    {#{}, <<>>};
parse_map(Content, 0) ->
    {#{}, Content};
parse_map(Arguments, Count) ->
    parse_map(#{}, Arguments, Count).

parse_map(Acc, <<>>, _Count) ->
    {Acc, <<>>};
parse_map(Acc, Content, 0) ->
    {Acc, Content};
parse_map(Acc,
          <<KeySize:16,
            Key:KeySize/binary,
            ValueSize:16,
            Value:ValueSize/binary,
            Rest/binary>>,
          Count) ->
    parse_map(maps:put(Key, Value, Acc), Rest, Count - 1).

auth_mechanisms(Sock) ->
    {ok, Configured} = application:get_env(rabbit, auth_mechanisms),
    [rabbit_data_coercion:to_binary(Name)
     || {Name, Module} <- rabbit_registry:lookup_all(auth_mechanism),
        Module:should_offer(Sock), lists:member(Name, Configured)].

auth_mechanism_to_module(TypeBin, Sock) ->
    case rabbit_registry:binary_to_type(TypeBin) of
        {error, not_found} ->
            ?LOG_WARNING("Unknown authentication mechanism '~tp'",
                               [TypeBin]),
            {error, not_found};
        T ->
            case {lists:member(TypeBin,
                               rabbit_stream_utils:auth_mechanisms(Sock)),
                  rabbit_registry:lookup_module(auth_mechanism, T)}
            of
                {true, {ok, Module}} ->
                    {ok, Module};
                _ ->
                    ?LOG_WARNING("Invalid authentication mechanism '~tp'",
                                       [T]),
                    {error, invalid}
            end
    end.

check_resource_access(User, Resource, Perm, Context) ->
    try
        rabbit_access_control:check_resource_access(User,
                                                    Resource,
                                                    Perm,
                                                    Context),
        ok
    catch
        exit:_ ->
            error
    end.

check_configure_permitted(Resource, User) ->
    check_resource_access(User, Resource, configure, #{}).

check_write_permitted(Resource, User) ->
    check_resource_access(User, Resource, write, #{}).

check_read_permitted(Resource, User, Context) ->
    check_resource_access(User, Resource, read, Context).

check_read_or_write_permitted(Resource, User) ->
    case check_read_permitted(Resource, User, #{}) of
        ok ->
            ok;
        _ ->
            check_write_permitted(Resource, User)
    end.

-spec check_super_stream_management_permitted(rabbit_types:vhost(), binary(),
                                              [binary()], rabbit_types:user()) ->
    ok | error.
check_super_stream_management_permitted(VirtualHost, SuperStream, Partitions, User) ->
    Exchange = e(VirtualHost, SuperStream),
    maybe
        %% exchange creation
        ok ?= check_super_stream_permitted(VirtualHost, SuperStream, User),
        %% stream creations
        ok ?= check_streams_permissions(fun check_configure_permitted/2,
                                        VirtualHost, Partitions,
                                        User),
        %% binding from exchange
        ok ?= check_read_permitted(Exchange, User, #{}),
        %% binding to streams
        check_streams_permissions(fun check_write_permitted/2,
                                  VirtualHost, Partitions,
                                  User)
    end.

-spec check_super_stream_permitted(rabbit_types:vhost(), binary(),
                                   rabbit_types:user()) ->
    ok | error.
check_super_stream_permitted(Vhost, SuperStream, User) ->
    Exchange = e(Vhost, SuperStream),
    check_configure_permitted(Exchange, User).

check_streams_permissions(Fun, VirtualHost, List, User) ->
    case lists:all(fun(S) ->
                      case Fun(q(VirtualHost, S), User) of
                          ok ->
                              true;
                          _ ->
                              false
                      end
              end, List) of
        true ->
            ok;
        _ ->
            error
    end.

extract_stream_list(<<>>, Streams) ->
    Streams;
extract_stream_list(<<Length:16, Stream:Length/binary, Rest/binary>>,
                    Streams) ->
    extract_stream_list(Rest, [Stream | Streams]).

-spec sort_partitions([#binding{}]) -> [#binding{}].
sort_partitions(Partitions) ->
    lists:sort(fun(#binding{args = Args1}, #binding{args = Args2}) ->
                  Arg1 =
                      rabbit_misc:table_lookup(Args1,
                                               <<"x-stream-partition-order">>),
                  Arg2 =
                      rabbit_misc:table_lookup(Args2,
                                               <<"x-stream-partition-order">>),
                  case {Arg1, Arg2} of
                      {{_, Order1}, {_, Order2}} ->
                          rabbit_data_coercion:to_integer(Order1)
                          =< rabbit_data_coercion:to_integer(Order2);
                      {undefined, {_, _Order2}} -> false;
                      {{_, _Order1}, undefined} -> true;
                      _ -> true
                  end
               end,
               Partitions).

strip_cr_lf(NameBin) ->
    binary:replace(NameBin, [<<"\n">>, <<"\r">>], <<"">>, [global]).

consumer_activity_status(Active, Properties) ->
    case {rabbit_stream_reader:single_active_consumer(Properties), Active}
    of
        {false, true} ->
            up;
        {true, true} ->
            single_active;
        {true, false} ->
            waiting
    end.

filter_defined(SubscriptionProperties) when is_map(SubscriptionProperties) ->
    lists:any(fun(<<"filter.",_/binary>>) ->
                      true;
                 (_) ->
                      false
              end, maps:keys(SubscriptionProperties));
filter_defined(_) ->
    false.

filter_spec(Properties) ->
    Filters = maps:fold(fun(<<"filter.",_/binary>>, V, Acc) ->
                                [V] ++ Acc;
                           (_, _, Acc) ->
                                Acc
                        end, [], Properties),
    case Filters of
        [] ->
            #{};
        _ ->
            MatchUnfiltered = case Properties of
                                  #{<<"match-unfiltered">> := <<"true">>} ->
                                      true;
                                  _ ->
                                      false
                              end,
            #{filter_spec =>
              #{filters => Filters, match_unfiltered => MatchUnfiltered}}
    end.

command_versions() ->
    [{declare_publisher, ?VERSION_1, ?VERSION_1},
     {publish, ?VERSION_1, ?VERSION_2},
     {query_publisher_sequence, ?VERSION_1, ?VERSION_1},
     {delete_publisher, ?VERSION_1, ?VERSION_1},
     {subscribe, ?VERSION_1, ?VERSION_2},
     {credit, ?VERSION_1, ?VERSION_2},
     {store_offset, ?VERSION_1, ?VERSION_1},
     {query_offset, ?VERSION_1, ?VERSION_1},
     {unsubscribe, ?VERSION_1, ?VERSION_1},
     {create_stream, ?VERSION_1, ?VERSION_1},
     {delete_stream, ?VERSION_1, ?VERSION_1},
     {metadata, ?VERSION_1, ?VERSION_1},
     {close, ?VERSION_1, ?VERSION_1},
     {heartbeat, ?VERSION_1, ?VERSION_1},
     {route, ?VERSION_1, ?VERSION_1},
     {partitions, ?VERSION_1, ?VERSION_1},
     {stream_stats, ?VERSION_1, ?VERSION_1},
     {create_super_stream, ?VERSION_1, ?VERSION_1},
     {delete_super_stream, ?VERSION_1, ?VERSION_1},
     {resolve_offset_spec, ?VERSION_1, ?VERSION_1}].

q(VirtualHost, Name) ->
    rabbit_misc:r(VirtualHost, queue, Name).

e(VirtualHost, Name) ->
    rabbit_misc:r(VirtualHost, exchange, Name).

-spec consumer_offset(ConsumerOffsetFromCounter :: integer(),
                      MessageConsumed :: non_neg_integer(),
                      LastListenerOffset :: integer() | undefined) -> integer().
consumer_offset(0, 0, undefined) ->
    0;
consumer_offset(0, 0, LastListenerOffset) when LastListenerOffset > 0 ->
    %% consumer at "next" waiting for messages most likely
    LastListenerOffset;
consumer_offset(ConsumerOffsetFromCounter, _, _) ->
    ConsumerOffsetFromCounter.

-spec offset_lag(CommittedOffset :: integer(),
                 ConsumerOffsetFromCounter :: integer(),
                 MessageConsumed :: non_neg_integer(),
                 LastListenerOffset :: integer() | undefined) -> integer().
offset_lag(-1, _, _, _) ->
    %% -1 is for an empty stream, so no lag
    0;
offset_lag(_, 0, 0, LastListenerOffset) when LastListenerOffset > 0 ->
    %% consumer waiting for messages at the end of the stream, most likely
    0;
offset_lag(CommittedOffset, ConsumerOffset, _, _) ->
    CommittedOffset - ConsumerOffset.

-spec validate_super_stream_max_partitions(list() | integer()) -> boolean().
validate_super_stream_max_partitions(Partitions) ->
    MaxPartitions = rabbit_stream_utils:max_super_stream_partitions(),
    validate_super_stream_max_partitions(Partitions, MaxPartitions).

-spec validate_super_stream_max_partitions(list() | integer(),
                                           infinity | non_neg_integer()) -> boolean().
validate_super_stream_max_partitions(_, infinity) ->
    true;
validate_super_stream_max_partitions(Partitions, Max) when is_list(Partitions) ->
    length(Partitions) =< Max;
validate_super_stream_max_partitions(Partitions, Max) when is_integer(Partitions) ->
    Partitions =< Max.

-spec max_super_stream_partitions() -> infinity | non_neg_integer().
max_super_stream_partitions() ->
    application:get_env(rabbitmq_stream, max_super_stream_partitions,
                        ?MAX_SUPER_STREAM_PARTITIONS).

%% super stream partition helpers
-spec streams_from_partitions(binary(), non_neg_integer()) -> [binary()].
streams_from_partitions(Name, Partitions) ->
    [<<Name/binary, "-", (integer_to_binary(K))/binary>> ||
     K <- lists:seq(0, Partitions - 1)].

-spec streams_from_binding_keys(binary(), [binary()]) -> [binary()].
streams_from_binding_keys(Name, BindingKeys) ->
    [<<Name/binary, "-", K/binary>> || K <- BindingKeys].

-spec routing_keys(non_neg_integer()) -> [binary()].
routing_keys(Partitions) ->
    [integer_to_binary(K) || K <- lists:seq(0, Partitions - 1)].

-spec binding_keys(unicode:chardata()) -> [binary()].
binding_keys(BindingKeysBin) ->
    Keys = binary:split(rabbit_data_coercion:to_binary(BindingKeysBin),
                        <<",">>, [global]),
    %% Trim first, then verify the token is not an empty binary
    [Trimmed || K <- Keys,
                Trimmed <- [string:trim(K)],
                Trimmed =/= <<>>].

-spec node_endpoints([node()], tcp | ssl) -> #{node() => {binary(), integer()}}.
node_endpoints(Nodes, Transport) ->
    node_endpoints(Nodes, Transport, advertised_endpoint).

node_endpoints([], _Transport, _Fun) ->
    #{};
node_endpoints(Nodes, Transport, Fun) ->
    %% Absolute deadline, so that both phases and the receive loop of the
    %% fallback share one budget.
    Deadline = {abs, erlang:monotonic_time(millisecond) + ?ENDPOINT_TIMEOUT},
    Results = erpc:multicall(Nodes, rabbit_stream, Fun, [Transport], Deadline),
    Classify = fun(NodeResult, Acc) ->
                       classify_endpoint(Fun, NodeResult, Acc)
               end,
    {Endpoints, Legacy} = lists:foldl(Classify, {#{}, []},
                                      lists:zip(Nodes, Results)),
    case Legacy of
        [] ->
            Endpoints;
        _ ->
            ?LOG_DEBUG("Nodes ~tp do not export rabbit_stream:~ts/1, "
                       "retrieving host and port separately", [Legacy, Fun]),
            Fallback = legacy_node_endpoints(Legacy, Transport, Deadline),
            maps:merge(Endpoints, Fallback)
    end.

classify_endpoint(_Fun, {Node, {ok, {Host, Port}}}, {Endpoints, Legacy})
  when is_binary(Host), is_integer(Port) ->
    {Endpoints#{Node => {Host, Port}}, Legacy};
%% The peer predates advertised_endpoint/1, or the stream plugin is not
%% running there.
classify_endpoint(Fun,
                  {Node,
                   {error, {exception, undef, [{rabbit_stream, Fun, _, _} | _]}}},
                  {Endpoints, Legacy}) ->
    {Endpoints, [Node | Legacy]};
classify_endpoint(_Fun, {Node, Result}, {Endpoints, Legacy}) ->
    log_unusable_endpoint(Node, Result),
    {Endpoints, Legacy}.

log_unusable_endpoint(Node, {ok, {Host, Port}}) ->
    ?LOG_WARNING("Invalid stream endpoint reported by node '~ts': ~tp ~tp",
                 [Node, Host, Port]);
log_unusable_endpoint(Node, Result) ->
    Level = case transient_endpoint_error(Result) of
                true -> debug;
                false -> warning
            end,
    ?LOG(Level, "Could not retrieve the endpoint of node '~ts': ~tp",
         [Node, Result]).

transient_endpoint_error({error, {erpc, timeout}}) ->
    true;
transient_endpoint_error({error, {erpc, noconnection}}) ->
    true;
%% The stream plugin is not running on that node.
transient_endpoint_error({error, {exception, undef,
                                  [{rabbit_stream, _, _, _} | _]}}) ->
    true;
transient_endpoint_error(_) ->
    false.

%% For peers that do not export rabbit_stream:advertised_endpoint/1. It can go
%% away once every version an upgrade can start from exports it.
%%
%% Both requests of a node are sent before any of them is collected, so that a
%% slow node does not use up the budget of the others.
legacy_node_endpoints(Nodes, Transport, Deadline) ->
    HostFun = legacy_host_fun(Transport),
    PortFun = legacy_port_fun(Transport),
    ReqIds = lists:foldl(
               fun(Node, Acc0) ->
                       Acc1 = erpc:send_request(Node, rabbit_stream, HostFun,
                                                [], {Node, host}, Acc0),
                       erpc:send_request(Node, rabbit_stream, PortFun,
                                         [], {Node, port}, Acc1)
               end, erpc:reqids_new(), Nodes),
    Parts = receive_endpoint_parts(ReqIds, Deadline, #{}),
    Endpoints = maps:filtermap(
                  fun(_Node, #{host := H, port := P})
                        when is_binary(H), is_integer(P) ->
                          {true, {H, P}};
                     (_Node, _Part) ->
                          false
                  end, Parts),
    case Nodes -- maps:keys(Endpoints) of
        [] ->
            ok;
        Missing ->
            ?LOG_DEBUG("Could not retrieve the endpoint of nodes ~tp: ~tp",
                       [Missing, maps:with(Missing, Parts)])
    end,
    Endpoints.

receive_endpoint_parts(ReqIds, Deadline, Acc) ->
    try erpc:receive_response(ReqIds, Deadline, true) of
        no_request ->
            Acc;
        {Value, {Node, Key}, ReqIds1} ->
            Acc1 = add_endpoint_part(Node, Key, Value, Acc),
            receive_endpoint_parts(ReqIds1, Deadline, Acc1)
    catch
        error:{erpc, timeout} ->
            %% receive_response/3 abandons the outstanding requests.
            Acc;
        _Class:{Reason, {Node, Key}, ReqIds1} ->
            Acc1 = add_endpoint_part(Node, Key, {error, Reason}, Acc),
            receive_endpoint_parts(ReqIds1, Deadline, Acc1)
    end.

add_endpoint_part(Node, Key, Value, Acc) ->
    Part = maps:get(Node, Acc, #{}),
    Acc#{Node => Part#{Key => Value}}.

%% Only functions old peers export, hence not advertised_host/1 and
%% advertised_port/1.
legacy_host_fun(tcp) -> host;
legacy_host_fun(ssl) -> tls_host.

legacy_port_fun(tcp) -> port;
legacy_port_fun(ssl) -> tls_port.
