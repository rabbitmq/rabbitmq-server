%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stream_mgmt_db).

-define(ENTITY_CONSUMER, consumer).
-define(ENTITY_PUBLISHER, publisher).

-include_lib("rabbitmq_stream/include/rabbit_stream_metrics.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-export([get_all_consumers/1,
         get_all_publishers/1]).
-export([entity_data/4]).
-export([get_connection_consumers/1,
         get_connection_publishers/1,
         get_stream_publishers/1]).

get_all_consumers(VHosts) ->
    rabbit_mgmt_db:submit(fun(_Interval) -> consumers_stats(VHosts) end).

get_all_publishers(VHosts) ->
    rabbit_mgmt_db:submit(fun(_Interval) -> publishers_stats(VHosts) end).

get_connection_consumers(ConnectionPid) when is_pid(ConnectionPid) ->
    rabbit_mgmt_db:submit(fun(_Interval) ->
                             connection_consumers_stats(ConnectionPid)
                          end).

get_connection_publishers(ConnectionPid) when is_pid(ConnectionPid) ->
    rabbit_mgmt_db:submit(fun(_Interval) ->
                             connection_publishers_stats(ConnectionPid)
                          end).

get_stream_publishers(QueueResource) ->
    rabbit_mgmt_db:submit(fun(_Interval) ->
                             stream_publishers_stats(QueueResource)
                          end).

consumers_stats(VHost) ->
    Data =
        rabbit_mgmt_db:get_data_from_nodes({rabbit_stream_mgmt_db,
                                            entity_data,
                                            [VHost, ?ENTITY_CONSUMER,
                                             consumers_by_vhost]}),
    [V || {_, V} <- maps:to_list(Data)].

publishers_stats(VHost) ->
    Data =
        rabbit_mgmt_db:get_data_from_nodes({rabbit_stream_mgmt_db,
                                            entity_data,
                                            [VHost, ?ENTITY_PUBLISHER,
                                             publishers_by_vhost]}),
    [V || {_, V} <- maps:to_list(Data)].

connection_consumers_stats(ConnectionPid) ->
    Data =
        rabbit_mgmt_db:get_data_from_nodes({rabbit_stream_mgmt_db,
                                            entity_data,
                                            [ConnectionPid, ?ENTITY_CONSUMER,
                                             consumers_by_connection]}),
    [V || {_, V} <- maps:to_list(Data)].

connection_publishers_stats(ConnectionPid) ->
    Data =
        rabbit_mgmt_db:get_data_from_nodes({rabbit_stream_mgmt_db,
                                            entity_data,
                                            [ConnectionPid, ?ENTITY_PUBLISHER,
                                             publishers_by_connection]}),
    [V || {_, V} <- maps:to_list(Data)].

stream_publishers_stats(Queue) ->
    Data =
        rabbit_mgmt_db:get_data_from_nodes({rabbit_stream_mgmt_db,
                                            entity_data,
                                            [Queue, ?ENTITY_PUBLISHER,
                                             publishers_by_stream]}),
    [V || {_, V} <- maps:to_list(Data)].

entity_data(_Pid, Param, EntityType, QueryFunName) ->
    QueryFun = map_function(QueryFunName),
    maps:from_list([begin
                        AugmentedPublisher = augment_entity(EntityType, P),
                        {P,
                         augment_connection_pid(AugmentedPublisher)
                         ++ AugmentedPublisher}
                    end
                    || P <- QueryFun(Param)]).

map_function(consumers_by_vhost) ->
    fun consumers_by_vhost/1;
map_function(publishers_by_vhost) ->
    fun publishers_by_vhost/1;
map_function(consumers_by_connection) ->
    fun consumers_by_connection/1;
map_function(publishers_by_connection) ->
    fun publishers_by_connection/1;
map_function(publishers_by_stream) ->
    fun publishers_by_stream/1.

augment_entity(?ENTITY_CONSUMER, {{Q, ConnPid, SubId}, Props}) ->
    [{queue, format_resource(Q)}, {connection, ConnPid},
     {subscription_id, SubId}
     | Props];
augment_entity(?ENTITY_PUBLISHER, {{Q, ConnPid, PubId}, Props}) ->
    [{queue, format_resource(Q)}, {connection, ConnPid},
     {publisher_id, PubId}
     | Props].

consumers_by_vhost(VHost) ->
    ets:select(?TABLE_CONSUMER,
               [{{{#resource{virtual_host = '$1', _ = '_'}, '_', '_'}, '_'},
                 [{'orelse', {'==', all, VHost}, {'==', VHost, '$1'}}],
                 ['$_']}]).

publishers_by_vhost(VHost) ->
    ets:select(?TABLE_PUBLISHER,
               [{{{#resource{virtual_host = '$1', _ = '_'}, '_', '_'}, '_'},
                 [{'orelse', {'==', all, VHost}, {'==', VHost, '$1'}}],
                 ['$_']}]).

consumers_by_connection(ConnectionPid) ->
    get_entity_stats(?TABLE_CONSUMER, ConnectionPid).

publishers_by_connection(ConnectionPid) ->
    get_entity_stats(?TABLE_PUBLISHER, ConnectionPid).

publishers_by_stream(QueueResource) ->
    get_entity_stats_by_resource(?TABLE_PUBLISHER, QueueResource).

get_entity_stats(Table, Id) ->
    ets:select(Table, match_entity_spec(Id)).

match_entity_spec(ConnectionId) ->
    [{{{'_', '$1', '_'}, '_'}, [{'==', ConnectionId, '$1'}], ['$_']}].

get_entity_stats_by_resource(Table, Resource) ->
    ets:select(Table, match_entity_spec_by_resource(Resource)).

match_entity_spec_by_resource(#resource{virtual_host = VHost,
                                        name = Name}) ->
    [{{{#resource{virtual_host = '$1',
                  name = '$2',
                  _ = '_'},
        '_', '_'},
       '_'},
      [{'andalso', {'==', '$1', VHost}, {'==', Name, '$2'}}], ['$_']}].

augment_connection_pid(Consumer) ->
    Pid = rabbit_misc:pget(connection, Consumer),
    Conn =
        rabbit_mgmt_data:lookup_element(connection_created_stats, Pid, 3),
    ConnDetails =
        case Conn of
            [] -> %% If the connection has just been opened, we might not yet have the data
                [];
            _ ->
                [{name, rabbit_misc:pget(name, Conn)},
                 {user, rabbit_misc:pget(user, Conn)},
                 {node, rabbit_misc:pget(node, Conn)},
                 {peer_port, rabbit_misc:pget(peer_port, Conn)},
                 {peer_host, rabbit_misc:pget(peer_host, Conn)}]
        end,
    [{connection_details, ConnDetails}].

format_resource(unknown) ->
    unknown;
format_resource(Res) ->
    format_resource(name, Res).

format_resource(_, unknown) ->
    unknown;
format_resource(NameAs,
                #resource{name = Name, virtual_host = VHost}) ->
    [{NameAs, Name}, {vhost, VHost}].
