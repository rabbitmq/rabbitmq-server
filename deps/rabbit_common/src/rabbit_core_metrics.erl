%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_core_metrics).

-include("rabbit_core_metrics.hrl").

-export([init/0]).

-export([connection_created/2,
         connection_closed/1,
         connection_stats/2,
         connection_stats/4]).

-export([channel_created/2,
	 channel_closed/1,
	 channel_stats/2,
	 channel_stats/3,
	 channel_queue_down/1,
	 channel_queue_exchange_down/1,
	 channel_exchange_down/1]).

-export([consumer_created/7,
	 consumer_deleted/3]).

%%----------------------------------------------------------------------------
%% Types
%%----------------------------------------------------------------------------
-type(channel_stats_id() :: pid() |
			    {pid(),
			     {rabbit_amqqueue:name(), rabbit_exchange:name()}} |
			    {pid(), rabbit_amqqueue:name()} |
			    {pid(), rabbit_exchange:name()}).

-type(channel_stats_type() :: queue_exchange_stats | queue_stats |
			      exchange_stats | reductions).
%%----------------------------------------------------------------------------
%% Specs
%%----------------------------------------------------------------------------
-spec init() -> ok.
-spec connection_created(pid(), rabbit_types:infos()) -> ok.
-spec connection_closed(pid()) -> ok.
-spec connection_stats(pid(), rabbit_types:infos()) -> ok.
-spec connection_stats(pid(), integer(), integer(), integer()) -> ok.
-spec channel_created(pid(), rabbit_types:infos()) -> ok.
-spec channel_closed(pid()) -> ok.
-spec channel_stats(pid(), rabbit_types:infos()) -> ok.
-spec channel_stats(channel_stats_type(), channel_stats_id(),
		    rabbit_types:infos() | integer()) -> ok.
-spec channel_queue_down({pid(), rabbit_amqqueue:name()}) -> ok.
-spec channel_queue_exchange_down({pid(), {rabbit_amqqueue:name(),
					   rabbit_exchange:name()}}) -> ok.
-spec channel_exchange_down({pid(), rabbit_exchange:name()}) -> ok.
-spec consumer_created(pid(), binary(), boolean(), boolean(),
		       rabbit_amqqueue:name(), integer(), list()) -> ok.
-spec consumer_deleted(pid(), binary(), rabbit_amqqueue:name()) -> ok.

%%----------------------------------------------------------------------------
%% Storage of the raw metrics in RabbitMQ core. All the processing of stats
%% is done by the management plugin.
%%----------------------------------------------------------------------------
%%----------------------------------------------------------------------------
%% API
%%----------------------------------------------------------------------------
init() ->
    [ets:new(Table, [set, public, named_table]) || Table <- ?CORE_TABLES],
    ok.

connection_created(Pid, Infos) ->
    ets:insert(connection_created, {Pid, Infos}),
    ok.

connection_closed(Pid) ->
    ets:delete(connection_created, Pid),
    ets:delete(connection_metrics, Pid),
    ets:delete(connection_coarse_metrics, Pid),
    ok.

connection_stats(Pid, Infos) ->
    ets:insert(connection_metrics, {Pid, Infos}),
    ok.

connection_stats(Pid, Recv_oct, Send_oct, Reductions) ->
    ets:insert(connection_coarse_metrics, {Pid, Recv_oct, Send_oct, Reductions}),
    ok.

channel_created(Pid, Infos) ->
    ets:insert(channel_created, {Pid, Infos}),
    ok.

channel_closed(Pid) ->
    ets:delete(channel_created, Pid),
    ets:delete(channel_metrics, Pid),
    ets:delete(channel_process_metrics, Pid),
    ok.

channel_stats(Pid, Infos) ->
    ets:insert(channel_metrics, {Pid, Infos}),
    ok.

channel_stats(queue_exchange_stats, Id, [{publish, Value}]) ->
    ets:insert(channel_queue_exchange_metrics, {Id, Value}),
    ok;
channel_stats(queue_stats, Id, Infos) ->
    ets:insert(channel_queue_metrics, {Id, Infos}),
    ok;
channel_stats(exchange_stats, Id, Infos) ->
    ets:insert(channel_exchange_metrics, {Id, Infos}),
    ok;
channel_stats(reductions, Id, Value) ->
    ets:insert(channel_process_metrics, {Id, Value}),
    ok.

channel_queue_down(Id) ->
    ets:delete(channel_queue_metrics, Id),
    ok.

channel_queue_exchange_down(Id) ->
    ets:delete(channel_queue_exchange_metrics, Id),
    ok.

channel_exchange_down(Id) ->
    ets:delete(channel_exchange_metrics, Id),
    ok.

consumer_created(ChPid, ConsumerTag, ExclusiveConsume, AckRequired, QName,
		 PrefetchCount, Args) ->
    ets:insert(consumer_created, {{QName, ChPid, ConsumerTag}, ExclusiveConsume,
				  AckRequired, PrefetchCount, Args}),
    ok.

consumer_deleted(ChPid, ConsumerTag, QName) ->
    ets:delete(consumer_created, {QName, ChPid, ConsumerTag}),
    ok.
