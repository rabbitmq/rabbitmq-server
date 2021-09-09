%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_queue_location_min_masters).

-behaviour(rabbit_queue_master_locator).

-include("amqqueue.hrl").

-export([description/0, queue_master_location/1]).

-rabbit_boot_step(
    {?MODULE, [
        {description, "locate queue master min bound queues"},
        {mfa,
            {rabbit_registry, register, [
                queue_master_locator,
                <<109, 105, 110, 45, 109, 97, 115, 116, 101, 114, 115>>,
                ?MODULE
            ]}},
        {requires, rabbit_registry},
        {enables, kernel_ready}
    ]}
).

%%---------------------------------------------------------------------------
%% Queue Master Location Callbacks
%%---------------------------------------------------------------------------

description() ->
    [
        {description, <<
            "Locate queue master node from cluster node with least bound "
            "queues"
        >>}
    ].

queue_master_location(Q) when ?is_amqqueue(Q) ->
    Nodes0 = rabbit_nodes:all_running(),
    Nodes = rabbit_maintenance:filter_out_drained_nodes_local_read(Nodes0),
    case Nodes of
        [] ->
            {ok, node()};
        _ ->
            MastersPerNode0 = erpc:multicall(Nodes, ets, info, [queue_metrics, size], 10),
            %% use {node(), infinity} whenever we don't get an answer;
            %% this way if everything goes wrong, we select node()
            MastersPerNode =
                lists:zipwith(
                    fun(N, Qs) ->
                        case Qs of
                            {ok, V} -> {N, V};
                            _ -> {node(), infinity}
                        end
                    end,
                    Nodes,
                    MastersPerNode0
                ),
            {MinNode, _} = hd(lists:keysort(2, MastersPerNode)),
            {ok, MinNode}
    end.
