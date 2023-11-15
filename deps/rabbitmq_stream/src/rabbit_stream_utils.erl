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
%% Copyright (c) 2020-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stream_utils).

-feature(maybe_expr, enable).

%% API
-export([enforce_correct_name/1,
         write_messages/5,
         parse_map/2,
         auth_mechanisms/1,
         auth_mechanism_to_module/2,
         check_configure_permitted/2,
         check_write_permitted/2,
         check_read_permitted/3,
         extract_stream_list/2,
         sort_partitions/1,
         strip_cr_lf/1,
         consumer_activity_status/2,
         filter_defined/1,
         filter_spec/1,
         command_versions/0,
         filtering_supported/0,
         check_super_stream_management_permitted/4]).

-define(MAX_PERMISSION_CACHE_SIZE, 12).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbitmq_stream_common/include/rabbit_stream.hrl").

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

write_messages(_Version, _ClusterLeader, _PublisherRef, _PublisherId, <<>>) ->
    ok;
write_messages(?VERSION_1 = V, ClusterLeader,
               PublisherRef,
               PublisherId,
               <<PublishingId:64,
                 0:1,
                 MessageSize:31,
                 Message:MessageSize/binary,
                 Rest/binary>>) ->
    write_messages0(V, ClusterLeader, PublisherRef, PublisherId,
                    PublishingId, Message, Rest);
write_messages(?VERSION_1 = V, ClusterLeader,
               PublisherRef,
               PublisherId,
               <<PublishingId:64,
                 1:1,
                 CompressionType:3,
                 _Unused:4,
                 MessageCount:16,
                 UncompressedSize:32,
                 BatchSize:32,
                 Batch:BatchSize/binary,
                 Rest/binary>>) ->
    Data = {batch, MessageCount, CompressionType, UncompressedSize, Batch},
    write_messages0(V, ClusterLeader, PublisherRef, PublisherId,
                    PublishingId, Data, Rest);
write_messages(?VERSION_2 = V, ClusterLeader,
               PublisherRef,
               PublisherId,
               <<PublishingId:64,
                 -1:16/signed,
                 0:1,
                 MessageSize:31,
                 Message:MessageSize/binary,
                 Rest/binary>>) ->
    write_messages0(V, ClusterLeader, PublisherRef, PublisherId,
                    PublishingId, Message, Rest);
write_messages(?VERSION_2 = V, ClusterLeader,
               PublisherRef,
               PublisherId,
               <<PublishingId:64,
                 FilterValueLength:16, FilterValue:FilterValueLength/binary,
                 0:1,
                 MessageSize:31,
                 Message:MessageSize/binary,
                 Rest/binary>>) ->
    write_messages0(V, ClusterLeader, PublisherRef, PublisherId,
                    PublishingId, {FilterValue, Message}, Rest).

write_messages0(Vsn, ClusterLeader, PublisherRef, PublisherId, PublishingId, Data, Rest) ->
    Corr = case PublisherRef of
               undefined -> {PublisherId, PublishingId};
               _ -> PublishingId
           end,
    ok = osiris:write(ClusterLeader, PublisherRef, Corr, Data),
    write_messages(Vsn, ClusterLeader, PublisherRef, PublisherId, Rest).

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
            rabbit_log:warning("Unknown authentication mechanism '~tp'",
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
                    rabbit_log:warning("Invalid authentication mechanism '~tp'",
                                       [T]),
                    {error, invalid}
            end
    end.

check_resource_access(User, Resource, Perm, Context) ->
    V = {Resource, Context, Perm},

    Cache =
        case get(permission_cache) of
            undefined ->
                [];
            Other ->
                Other
        end,
    case lists:member(V, Cache) of
        true ->
            ok;
        false ->
            try
                rabbit_access_control:check_resource_access(User,
                                                            Resource,
                                                            Perm,
                                                            Context),
                CacheTail =
                    lists:sublist(Cache, ?MAX_PERMISSION_CACHE_SIZE - 1),
                put(permission_cache, [V | CacheTail]),
                ok
            catch
                exit:_ ->
                    error
            end
    end.

check_configure_permitted(Resource, User) ->
    check_resource_access(User, Resource, configure, #{}).

check_write_permitted(Resource, User) ->
    check_resource_access(User, Resource, write, #{}).

check_read_permitted(Resource, User, Context) ->
    check_resource_access(User, Resource, read, Context).

-spec check_super_stream_management_permitted(rabbit_types:vhost(), binary(), [binary()], rabbit_types:user()) ->
    ok | error.
check_super_stream_management_permitted(VirtualHost, SuperStream, Partitions, User) ->
    Exchange = e(VirtualHost, SuperStream),
    maybe
        %% exchange creation
        ok ?= check_configure_permitted(Exchange, User),
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
    PublishMaxVersion = case filtering_supported() of
                            false ->
                                ?VERSION_1;
                            true ->
                                ?VERSION_2
                        end,
    [{declare_publisher, ?VERSION_1, ?VERSION_1},
     {publish, ?VERSION_1, PublishMaxVersion},
     {query_publisher_sequence, ?VERSION_1, ?VERSION_1},
     {delete_publisher, ?VERSION_1, ?VERSION_1},
     {subscribe, ?VERSION_1, ?VERSION_1},
     {credit, ?VERSION_1, ?VERSION_1},
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
     {delete_super_stream, ?VERSION_1, ?VERSION_1}].

filtering_supported() ->
    rabbit_feature_flags:is_enabled(stream_filtering).

q(VirtualHost, Name) ->
    rabbit_misc:r(VirtualHost, queue, Name).

e(VirtualHost, Name) ->
    rabbit_misc:r(VirtualHost, exchange, Name).
