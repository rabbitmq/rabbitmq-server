%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
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
    Cluster = rabbit_queue_master_location_misc:all_nodes(Q),
    QueueNames = rabbit_amqqueue:list_names(),
    MastersPerNode0 = lists:foldl(
        fun(#resource{virtual_host = VHost, name = QueueName}, NodeMasters) ->
            case rabbit_queue_master_location_misc:lookup_master(QueueName, VHost) of
                {ok, Master} when is_atom(Master) ->
                    case maps:is_key(Master, NodeMasters) of
                        true -> maps:update_with(Master,
                                                 fun(N) -> N + 1 end,
                                                 NodeMasters);
                        false -> NodeMasters
                    end;
                _ -> NodeMasters
            end
        end,
        maps:from_list([{N, 0} || N <- Cluster]),
        QueueNames),
    
    MastersPerNode = maps:filter(fun (Node, _N) ->
                                    not rabbit_maintenance:is_being_drained_local_read(Node)
                                 end, MastersPerNode0),

    case map_size(MastersPerNode) > 0 of
        true ->
            {MinNode, _NMasters} = maps:fold(
                fun(Node, NMasters, init) ->
                    {Node, NMasters};
                (Node, NMasters, {MinNode, MinMasters}) ->
                    case NMasters < MinMasters of
                        true  -> {Node, NMasters};
                        false -> {MinNode, MinMasters}
                    end
                end,
                init, MastersPerNode),
            {ok, MinNode};
        false ->
            undefined
    end.
