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
%% Copyright (c) 2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stream_utils).

%% API
-export([enforce_correct_stream_name/1,
         write_messages/4,
         parse_map/2,
         auth_mechanisms/1,
         auth_mechanism_to_module/2,
         check_configure_permitted/3,
         check_write_permitted/3,
         check_read_permitted/3,
         extract_stream_list/2]).

-define(MAX_PERMISSION_CACHE_SIZE, 12).

enforce_correct_stream_name(Name) ->
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

write_messages(_ClusterLeader, undefined, _PublisherId, <<>>) ->
    ok;
write_messages(ClusterLeader,
               undefined,
               PublisherId,
               <<PublishingId:64,
                 0:1,
                 MessageSize:31,
                 Message:MessageSize/binary,
                 Rest/binary>>) ->
    % FIXME handle write error
    ok =
        osiris:write(ClusterLeader,
                     undefined,
                     {PublisherId, PublishingId},
                     Message),
    write_messages(ClusterLeader, undefined, PublisherId, Rest);
write_messages(ClusterLeader,
               undefined,
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
    % FIXME handle write error
    ok =
        osiris:write(ClusterLeader,
                     undefined,
                     {PublisherId, PublishingId},
                     {batch, MessageCount, CompressionType, UncompressedSize, Batch}),
    write_messages(ClusterLeader, undefined, PublisherId, Rest);
write_messages(_ClusterLeader, _PublisherRef, _PublisherId, <<>>) ->
    ok;
write_messages(ClusterLeader,
               PublisherRef,
               PublisherId,
               <<PublishingId:64,
                 0:1,
                 MessageSize:31,
                 Message:MessageSize/binary,
                 Rest/binary>>) ->
    % FIXME handle write error
    ok = osiris:write(ClusterLeader, PublisherRef, PublishingId, Message),
    write_messages(ClusterLeader, PublisherRef, PublisherId, Rest);
write_messages(ClusterLeader,
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
    % FIXME handle write error
    ok =
        osiris:write(ClusterLeader,
                     PublisherRef,
                     PublishingId,
                     {batch, MessageCount, CompressionType, UncompressedSize, Batch}),
    write_messages(ClusterLeader, PublisherRef, PublisherId, Rest).

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
            rabbit_log:warning("Unknown authentication mechanism '~p'",
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
                    rabbit_log:warning("Invalid authentication mechanism '~p'",
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

check_configure_permitted(Resource, User, Context) ->
    check_resource_access(User, Resource, configure, Context).

check_write_permitted(Resource, User, Context) ->
    check_resource_access(User, Resource, write, Context).

check_read_permitted(Resource, User, Context) ->
    check_resource_access(User, Resource, read, Context).

extract_stream_list(<<>>, Streams) ->
    Streams;
extract_stream_list(<<Length:16, Stream:Length/binary, Rest/binary>>,
                    Streams) ->
    extract_stream_list(Rest, [Stream | Streams]).
