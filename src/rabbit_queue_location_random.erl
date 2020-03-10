%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
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
    Cluster    = rabbit_queue_master_location_misc:all_nodes(Q),
    RandomPos  = erlang:phash2(erlang:monotonic_time(), length(Cluster)),
    MasterNode = lists:nth(RandomPos + 1, Cluster),
    {ok, MasterNode}.
