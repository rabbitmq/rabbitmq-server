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

-module(rabbit_stream_metrics).

-include("rabbit_stream_metrics.hrl").

%% API
-export([init/0]).
-export([consumer_counters/3,
         consumer_created/3,
         consumer_cancelled/3]).
-export([publisher_counters/4,
         publisher_deleted/4]).

-define(CTAG_PREFIX, <<"stream.subid-">>).

init() ->
    seshat_counters:new_group(?TABLE_CONSUMER),
    seshat_counters:new_group(?TABLE_PUBLISHER),
    ok.

consumer_counters(Connection, StreamResource, SubscriptionId) ->
    seshat_counters:new(?TABLE_CONSUMER,
                        {StreamResource, Connection, SubscriptionId},
                        ?CONSUMER_COUNTERS).

consumer_created(Connection,
                 StreamResource,
                 SubscriptionId) ->
    rabbit_core_metrics:consumer_created(Connection,
                                         consumer_tag(SubscriptionId),
                                         false,
                                         false,
                                         StreamResource,
                                         0,
                                         true,
                                         up,
                                         []),
    ok.

consumer_tag(SubscriptionId) ->
    SubIdBinary = rabbit_data_coercion:to_binary(SubscriptionId),
    <<?CTAG_PREFIX/binary, SubIdBinary/binary>>.

consumer_cancelled(Connection, StreamResource, SubscriptionId) ->
    seshat_counters:delete(?TABLE_CONSUMER,
                           {StreamResource, Connection, SubscriptionId}),
    rabbit_core_metrics:consumer_deleted(Connection,
                                         consumer_tag(SubscriptionId),
                                         StreamResource),
    ok.

publisher_counters(Connection, StreamResource, PublisherId, Reference) ->
    seshat_counters:new(?TABLE_PUBLISHER,
                        {StreamResource, Connection, PublisherId,
                         format_publisher_reference(Reference)},
                        ?PUBLISHER_COUNTERS).

publisher_deleted(Connection, StreamResource, PublisherId, Reference) ->
    seshat_counters:delete(?TABLE_PUBLISHER,
                           {StreamResource, Connection, PublisherId,
                            format_publisher_reference(Reference)}),
    ok.

format_publisher_reference(undefined) ->
    <<"">>;
format_publisher_reference(Ref) when is_binary(Ref) ->
    Ref.
