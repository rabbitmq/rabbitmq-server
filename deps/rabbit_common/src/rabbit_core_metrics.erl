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
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_core_metrics).

-include("rabbit_core_metrics.hrl").

-export([init/0]).
-export([terminate/0]).

-export([connection_created/2,
         connection_closed/1,
         connection_stats/2,
         connection_stats/4]).

-export([channel_created/2,
         channel_closed/1,
         channel_stats/2,
         channel_stats/3,
         channel_stats/4,
         channel_queue_down/1,
         channel_queue_exchange_down/1,
         channel_exchange_down/1]).

-export([consumer_created/7,
         consumer_deleted/3]).

-export([queue_stats/2,
         queue_stats/5,
         queue_deleted/1]).

-export([node_stats/2]).

-export([node_node_stats/2]).

-export([gen_server2_stats/2,
         gen_server2_deleted/1,
         get_gen_server2_stats/1]).

-export([delete/2]).

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
-spec queue_stats(rabbit_amqqueue:name(), rabbit_types:infos()) -> ok.
-spec queue_stats(rabbit_amqqueue:name(), integer(), integer(), integer(),
                  integer()) -> ok.
-spec node_stats(atom(), rabbit_types:infos()) -> ok.
-spec node_node_stats({node(), node()}, rabbit_types:infos()) -> ok.
-spec gen_server2_stats(pid(), integer()) -> ok.
-spec gen_server2_deleted(pid()) -> ok.
-spec get_gen_server2_stats(pid()) -> integer() | 'not_found'.
-spec delete(atom(), any()) -> ok.
%%----------------------------------------------------------------------------
%% Storage of the raw metrics in RabbitMQ core. All the processing of stats
%% is done by the management plugin.
%%----------------------------------------------------------------------------
%%----------------------------------------------------------------------------
%% API
%%----------------------------------------------------------------------------
init() ->
    [ets:new(Table, [Type, public, named_table, {write_concurrency, true}])
     || {Table, Type} <- ?CORE_TABLES ++ ?CORE_EXTRA_TABLES],
    ok.

terminate() ->
    [ets:delete(Table)
     || {Table, _Type} <- ?CORE_TABLES ++ ?CORE_EXTRA_TABLES],
    ok.

connection_created(Pid, Infos) ->
    ets:insert(connection_created, {Pid, Infos}),
    ok.

connection_closed(Pid) ->
    ets:delete(connection_created, Pid),
    ets:delete(connection_metrics, Pid),
    %% Delete marker
    ets_compat:update_element(connection_coarse_metrics, Pid, {5, 1}),
    ok.

connection_stats(Pid, Infos) ->
    ets:insert(connection_metrics, {Pid, Infos}),
    ok.

connection_stats(Pid, Recv_oct, Send_oct, Reductions) ->
    %% Includes delete marker
    ets:insert(connection_coarse_metrics, {Pid, Recv_oct, Send_oct, Reductions, 0}),
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

channel_stats(reductions, Id, Value) ->
    ets:insert(channel_process_metrics, {Id, Value}),
    ok.

channel_stats(exchange_stats, publish, Id, Value) ->
    %% Includes delete marker
    ets_compat:update_counter(channel_exchange_metrics, Id, {2, Value}, {Id, 0, 0, 0, 0}),
    ok;
channel_stats(exchange_stats, confirm, Id, Value) ->
    %% Includes delete marker
    ets_compat:update_counter(channel_exchange_metrics, Id, {3, Value}, {Id, 0, 0, 0, 0}),
    ok;
channel_stats(exchange_stats, return_unroutable, Id, Value) ->
    %% Includes delete marker
    ets_compat:update_counter(channel_exchange_metrics, Id, {4, Value}, {Id, 0, 0, 0, 0}),
    ok;
channel_stats(queue_exchange_stats, publish, Id, Value) ->
    %% Includes delete marker
    ets_compat:update_counter(channel_queue_exchange_metrics, Id, Value, {Id, 0, 0}),
    ok;
channel_stats(queue_stats, get, Id, Value) ->
    %% Includes delete marker
    ets_compat:update_counter(channel_queue_metrics, Id, {2, Value}, {Id, 0, 0, 0, 0, 0, 0, 0}),
    ok;
channel_stats(queue_stats, get_no_ack, Id, Value) ->
    %% Includes delete marker
    ets_compat:update_counter(channel_queue_metrics, Id, {3, Value}, {Id, 0, 0, 0, 0, 0, 0, 0}),
    ok;
channel_stats(queue_stats, deliver, Id, Value) ->
    %% Includes delete marker
    ets_compat:update_counter(channel_queue_metrics, Id, {4, Value}, {Id, 0, 0, 0, 0, 0, 0, 0}),
    ok;
channel_stats(queue_stats, deliver_no_ack, Id, Value) ->
    %% Includes delete marker
    ets_compat:update_counter(channel_queue_metrics, Id, {5, Value}, {Id, 0, 0, 0, 0, 0, 0, 0}),
    ok;
channel_stats(queue_stats, redeliver, Id, Value) ->
    %% Includes delete marker
    ets_compat:update_counter(channel_queue_metrics, Id, {6, Value}, {Id, 0, 0, 0, 0, 0, 0, 0}),
    ok;
channel_stats(queue_stats, ack, Id, Value) ->
    %% Includes delete marker
    ets_compat:update_counter(channel_queue_metrics, Id, {7, Value}, {Id, 0, 0, 0, 0, 0, 0, 0}),
    ok.

delete(Table, Key) ->
    ets:delete(Table, Key).

channel_queue_down(Id) ->
    %% Delete marker
    ets_compat:update_element(channel_queue_metrics, Id, {8, 1}),
    ok.

channel_queue_exchange_down(Id) ->
    %% Delete marker
    ets_compat:update_element(channel_queue_exchange_metrics, Id, {3, 1}),
    ok.

channel_exchange_down(Id) ->
    %% Delete marker
    ets_compat:update_element(channel_exchange_metrics, Id, {5, 1}),
    ok.

consumer_created(ChPid, ConsumerTag, ExclusiveConsume, AckRequired, QName,
                 PrefetchCount, Args) ->
    ets:insert(consumer_created, {{QName, ChPid, ConsumerTag}, ExclusiveConsume,
                                   AckRequired, PrefetchCount, Args}),
    ok.

consumer_deleted(ChPid, ConsumerTag, QName) ->
    ets:delete(consumer_created, {QName, ChPid, ConsumerTag}),
    ok.

queue_stats(Name, Infos) ->
    %% Includes delete marker
    ets:insert(queue_metrics, {Name, Infos, 0}),
    ok.

queue_stats(Name, MessagesReady, MessagesUnacknowledge, Messages, Reductions) ->
    ets:insert(queue_coarse_metrics, {Name, MessagesReady, MessagesUnacknowledge,
                                      Messages, Reductions}),
    ok.

queue_deleted(Name) ->
    ets:delete(queue_coarse_metrics, Name),
    %% Delete markers
    ets_compat:update_element(queue_metrics, Name, {3, 1}),
    CQX = ets:select(channel_queue_exchange_metrics, match_spec_cqx(Name)),
    lists:foreach(fun(Key) ->
                          ets_compat:update_element(channel_queue_exchange_metrics, Key, {3, 1})
                  end, CQX),
    CQ = ets:select(channel_queue_metrics, match_spec_cq(Name)),
    lists:foreach(fun(Key) ->
                          ets_compat:update_element(channel_queue_metrics, Key, {8, 1})
                  end, CQ).

node_stats(persister_metrics, Infos) ->
    ets:insert(node_persister_metrics, {node(), Infos});
node_stats(coarse_metrics, Infos) ->
    ets:insert(node_coarse_metrics, {node(), Infos});
node_stats(node_metrics, Infos) ->
    ets:insert(node_metrics, {node(), Infos}).

node_node_stats(Id, Infos) ->
    ets:insert(node_node_metrics, {Id, Infos}).

match_spec_cqx(Id) ->
    [{{{'$2', {'$1', '$3'}}, '_', '_'}, [{'==', {Id}, '$1'}], [{{'$2', {{'$1', '$3'}}}}]}].

match_spec_cq(Id) ->
    [{{{'$2', '$1'}, '_', '_', '_', '_', '_', '_', '_'}, [{'==', {Id}, '$1'}], [{{'$2', '$1'}}]}].

gen_server2_stats(Pid, BufferLength) ->
    ets:insert(gen_server2_metrics, {Pid, BufferLength}).

gen_server2_deleted(Pid) ->
    ets:delete(gen_server2_metrics, Pid).

get_gen_server2_stats(Pid) ->
    case ets:lookup(gen_server2_metrics, Pid) of
        [{Pid, BufferLength}] ->
            BufferLength;
        [] ->
            not_found
    end.
