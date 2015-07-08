%%% The contents of this file are subject to the Mozilla Public License
%%% Version 1.1 (the "License"); you may not use this file except in
%%% compliance with the License. You may obtain a copy of the License at
%%% http://www.mozilla.org/MPL/
%%%
%%% Software distributed under the License is distributed on an "AS IS"
%%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%% License for the specific language governing rights and limitations
%%% under the License.
%%%
%%% The Original Code is RabbitMQ.
%%%
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2015 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_queue_location_min_masters).
-behaviour(rabbit_queue_master_locator).

-include("rabbit.hrl").

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

queue_master_location(#amqqueue{}) ->
    Cluster            = rabbit_queue_master_location_misc:all_nodes(),
    VHosts             = rabbit_vhost:list(),
    BoundQueueMasters  = get_bound_queue_masters_per_vhost(VHosts, []),
    {_Count, MinMaster}= get_min_master(Cluster, BoundQueueMasters,
                                         {undefined, undefined}),
    {ok, MinMaster}.

%%---------------------------------------------------------------------------
%% Private helper functions
%%---------------------------------------------------------------------------

get_min_master([], _BoundQueueMasters, MinNode)   -> MinNode;
get_min_master([Node|Rem], BoundQueueMasters, {undefined, undefined}) ->
    Count     = count_masters(Node, BoundQueueMasters, 0),
    get_min_master(Rem, BoundQueueMasters, {Count, Node});
get_min_master([Node0|Rem], BoundQueueMasters, MinNode={Count, _Node}) ->
    Count0    = count_masters(Node0, BoundQueueMasters, 0),
    MinNode0  = if  Count0 < Count -> {Count0, Node0};
                    true           -> MinNode
                end,
    get_min_master(Rem, BoundQueueMasters, MinNode0).


get_bound_queue_masters_per_vhost([], Acc) ->
    lists:flatten(Acc);
get_bound_queue_masters_per_vhost([VHost|RemVHosts], Acc) ->
    Bindings          = rabbit_binding:list(VHost),
    BoundQueueMasters = get_queue_master_per_binding(VHost, Bindings, []),
    get_bound_queue_masters_per_vhost(RemVHosts, [BoundQueueMasters|Acc]).


get_queue_master_per_binding(_VHost, [], BoundQueueNodes) -> BoundQueueNodes;
get_queue_master_per_binding(VHost, [#binding{key=QueueName}|RemBindings],
                              QueueMastersAcc) ->
    QueueMastersAcc0 = case rabbit_queue_master_location_misc:lookup_master(
                              QueueName, VHost) of
                           {ok, Master} when is_atom(Master) ->
                               [Master|QueueMastersAcc];
                           _ -> QueueMastersAcc
                       end,
    get_queue_master_per_binding(VHost, RemBindings, QueueMastersAcc0).


count_masters(_Node, [], Count)      -> Count;
count_masters(Node, [Node|T], Count) -> count_masters(Node, T, Count+1);
count_masters(Node, [_|T], Count)    -> count_masters(Node, T, Count).
