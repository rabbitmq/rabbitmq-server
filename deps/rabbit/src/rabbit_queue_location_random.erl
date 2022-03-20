%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_queue_location_random).
-behaviour(rabbit_queue_master_locator).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("amqqueue.hrl").

-export([description/0, queue_master_location/1]).

-rabbit_boot_step({?MODULE,
                   [{description, "locate queue master random"},
                    {mfa,         {rabbit_registry, register,
                                   [queue_master_locator,
                                    <<"random">>, ?MODULE]}},
                    {requires,    rabbit_registry},
                    {enables,     kernel_ready}]}).

%%---------------------------------------------------------------------------
%% Queue Master Location Callbacks
%%---------------------------------------------------------------------------

description() ->
    [{description,
      <<"Locate queue master node from cluster in a random manner">>}].

queue_master_location(Q) when ?is_amqqueue(Q) ->
    Cluster0   = rabbit_queue_master_location_misc:all_nodes(Q),
    Cluster    = rabbit_maintenance:filter_out_drained_nodes_local_read(Cluster0),
    case Cluster of
        [] ->
            undefined;
        Candidates when is_list(Candidates) ->
            RandomPos  = erlang:phash2(erlang:monotonic_time(), length(Candidates)),
            MasterNode = lists:nth(RandomPos + 1, Candidates),
            {ok, MasterNode}
    end.
