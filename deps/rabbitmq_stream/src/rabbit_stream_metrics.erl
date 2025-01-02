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

-module(rabbit_stream_metrics).

-include("rabbit_stream_metrics.hrl").

%% API
-export([init/0]).
-export([consumer_created/9,
         consumer_updated/9,
         consumer_cancelled/3]).
-export([publisher_created/4,
         publisher_updated/7,
         publisher_deleted/3]).

-define(CTAG_PREFIX, <<"stream.subid-">>).

init() ->
    _ = rabbit_core_metrics:create_table({?TABLE_CONSUMER, set}),
    _ = rabbit_core_metrics:create_table({?TABLE_PUBLISHER, set}),
    ok.

consumer_created(Connection,
                 StreamResource,
                 SubscriptionId,
                 Credits,
                 MessageCount,
                 Offset,
                 OffsetLag,
                 Active,
                 Properties) ->
    Values =
        [{credits, Credits},
         {consumed, MessageCount},
         {offset, Offset},
         {offset_lag, OffsetLag},
         {active, Active},
         {activity_status,
          rabbit_stream_utils:consumer_activity_status(Active, Properties)},
         {properties, Properties}],
    ets:insert(?TABLE_CONSUMER,
               {{StreamResource, Connection, SubscriptionId}, Values}),
    rabbit_global_counters:consumer_created(stream),
    rabbit_core_metrics:consumer_created(Connection,
                                         consumer_tag(SubscriptionId),
                                         false,
                                         false,
                                         StreamResource,
                                         0,
                                         Active,
                                         rabbit_stream_utils:consumer_activity_status(Active,
                                                                                      Properties),
                                         rabbit_misc:to_amqp_table(Properties)),
    ok.

consumer_tag(SubscriptionId) ->
    SubIdBinary = rabbit_data_coercion:to_binary(SubscriptionId),
    <<?CTAG_PREFIX/binary, SubIdBinary/binary>>.

consumer_updated(Connection,
                 StreamResource,
                 SubscriptionId,
                 Credits,
                 MessageCount,
                 Offset,
                 OffsetLag,
                 Active,
                 Properties) ->
    Values =
        [{credits, Credits},
         {consumed, MessageCount},
         {offset, Offset},
         {offset_lag, OffsetLag},
         {active, Active},
         {activity_status,
          rabbit_stream_utils:consumer_activity_status(Active, Properties)},
         {properties, Properties}],
    ets:insert(?TABLE_CONSUMER,
               {{StreamResource, Connection, SubscriptionId}, Values}),
    rabbit_core_metrics:consumer_updated(Connection,
                                         consumer_tag(SubscriptionId),
                                         false,
                                         false,
                                         StreamResource,
                                         0,
                                         Active,
                                         rabbit_stream_utils:consumer_activity_status(Active,
                                                                                      Properties),
                                         rabbit_misc:to_amqp_table(Properties)),

    ok.

consumer_cancelled(Connection, StreamResource, SubscriptionId) ->
    ets:delete(?TABLE_CONSUMER,
               {StreamResource, Connection, SubscriptionId}),
    rabbit_global_counters:consumer_deleted(stream),
    rabbit_core_metrics:consumer_deleted(Connection,
                                         consumer_tag(SubscriptionId),
                                         StreamResource),
    rabbit_event:notify(consumer_deleted,
                        [{consumer_tag, consumer_tag(SubscriptionId)},
                         {channel, self()}, {queue, StreamResource}]),
    ok.

publisher_created(Connection,
                  StreamResource,
                  PublisherId,
                  Reference) ->
    Values =
        [{reference, format_publisher_reference(Reference)},
         {published, 0},
         {confirmed, 0},
         {errored, 0}],
    rabbit_global_counters:publisher_created(stream),
    ets:insert(?TABLE_PUBLISHER,
               {{StreamResource, Connection, PublisherId}, Values}),
    ok.

publisher_updated(Connection,
                  StreamResource,
                  PublisherId,
                  Reference,
                  Published,
                  Confirmed,
                  Errored) ->
    Values =
        [{reference, format_publisher_reference(Reference)},
         {published, Published},
         {confirmed, Confirmed},
         {errored, Errored}],
    ets:insert(?TABLE_PUBLISHER,
               {{StreamResource, Connection, PublisherId}, Values}),
    ok.

publisher_deleted(Connection, StreamResource, PublisherId) ->
    ets:delete(?TABLE_PUBLISHER,
               {StreamResource, Connection, PublisherId}),
    rabbit_global_counters:publisher_deleted(stream),
    ok.

format_publisher_reference(undefined) ->
    <<"">>;
format_publisher_reference(Ref) when is_binary(Ref) ->
    Ref.
