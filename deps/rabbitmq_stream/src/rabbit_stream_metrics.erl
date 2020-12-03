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
%% Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stream_metrics).

-include("rabbit_stream_metrics.hrl").

%% API
-export([init/0]).
-export([consumer_created/4, consumer_updated/4, consumer_cancelled/3]).

init() ->
    rabbit_core_metrics:create_table({?TABLE_CONSUMER, set}),
    ok.

consumer_created(Connection, StreamResource, SubscriptionId, Credits) ->
    Values = [{credits, Credits}],
    ets:insert(?TABLE_CONSUMER, {{StreamResource, Connection, SubscriptionId}, Values}),
    ok.

consumer_updated(Connection, StreamResource, SubscriptionId, Credits) ->
    Values = [{credits, Credits}],
    ets:insert(?TABLE_CONSUMER, {{StreamResource, Connection, SubscriptionId}, Values}),
    ok.

consumer_cancelled(Connection, StreamResource, SubscriptionId) ->
    ets:delete(?TABLE_CONSUMER, {StreamResource, Connection, SubscriptionId}),
    ok.

