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
%% Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
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
    MastersPerNode = lists:foldl(
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

    {MinNode, _NMasters} = maps:fold(
        fun(Node, NMasters, init) ->
            {Node, NMasters};
        (Node, NMasters, {MinNode, MinMasters}) ->
            case NMasters < MinMasters of
                true  -> {Node, NMasters};
                false -> {MinNode, MinMasters}
            end
        end,
        init,
        MastersPerNode),
    {ok, MinNode}.
