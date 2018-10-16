%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_queue_location_min_masters).
-behaviour(rabbit_queue_master_locator).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("amqqueue.hrl").

-export([description/0, queue_master_location/1]).

-rabbit_boot_step({?MODULE,
                   [{description, "locate queue master min bound queues"},
                    {mfa,         {rabbit_registry, register,
                                   [queue_master_locator,
                                    <<"min-masters">>, ?MODULE]}},
                    {requires,    rabbit_registry},
                    {enables,     kernel_ready}]}).

%%---------------------------------------------------------------------------
%% Queue Master Location Callbacks
%%---------------------------------------------------------------------------

description() ->
    [{description,
      <<"Locate queue master node from cluster node with least bound queues">>}].

queue_master_location(Q) when ?is_amqqueue(Q) ->
    Cluster            = rabbit_queue_master_location_misc:all_nodes(Q),
    VHosts             = rabbit_vhost:list(),
    BoundQueueMasters  = get_bound_queue_masters_per_vhost(VHosts, []),
    {_Count, MinMaster}= get_min_master(Cluster, BoundQueueMasters),
    {ok, MinMaster}.

%%---------------------------------------------------------------------------
%% Private helper functions
%%---------------------------------------------------------------------------
get_min_master(Cluster, BoundQueueMasters) ->
    lists:min([ {count_masters(Node, BoundQueueMasters), Node} ||
                  Node <- Cluster ]).

count_masters(Node, Masters) ->
    length([ X || X <- Masters, X == Node ]).

get_bound_queue_masters_per_vhost([], Acc) ->
    lists:flatten(Acc);
get_bound_queue_masters_per_vhost([VHost|RemVHosts], Acc) ->
    BoundQueueNames =
        lists:filtermap(
            fun(#binding{destination =#resource{kind = queue,
                                                name = QueueName}}) ->
                    {true, QueueName};
               (_) ->
                    false
            end,
            rabbit_binding:list(VHost)),
    UniqQueueNames = lists:usort(BoundQueueNames),
    BoundQueueMasters = get_queue_masters(VHost, UniqQueueNames, []),
    get_bound_queue_masters_per_vhost(RemVHosts, [BoundQueueMasters|Acc]).


get_queue_masters(_VHost, [], BoundQueueNodes) -> BoundQueueNodes;
get_queue_masters(VHost, [QueueName | RemQueueNames], QueueMastersAcc) ->
    QueueMastersAcc0 = case rabbit_queue_master_location_misc:lookup_master(
                              QueueName, VHost) of
                           {ok, Master} when is_atom(Master) ->
                               [Master|QueueMastersAcc];
                           _ -> QueueMastersAcc
                       end,
    get_queue_masters(VHost, RemQueueNames, QueueMastersAcc0).
