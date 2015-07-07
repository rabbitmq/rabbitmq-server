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
%%% @author Ayanda Dube <ayanda.dube@erlang-solutions.com>
%%% @doc
%%% - Queue Master Location 'minimum bound queues' selection callback
%%%
%%% @end
%%% Created : 19. Jun 2015
%%%-------------------------------------------------------------------
-module(rabbit_queue_location_min_masters).
-behaviour(rabbit_queue_master_locator).

-include("rabbit.hrl").

-export([ description/0, queue_master_location/1, validate_policy/1 ]).

-rabbit_boot_step({?MODULE,
  [{description, "Locate queue master node from cluster node with least bound queues"},
    {mfa,         {rabbit_registry, register,
      [queue_master_locator, <<"min-masters">>, ?MODULE]}},
    {requires,    rabbit_registry},
    {enables,     kernel_ready}]}).

%%---------------------------------------------------------------------------
%% Queue Master Location Callbacks
%%---------------------------------------------------------------------------

description() ->
  [{description, <<"Locate queue master node from cluster node with least bound queues">>}].

queue_master_location(#amqqueue{}) ->
  Cluster          = rabbit_queue_master_location_misc:all_nodes(),
  {_Count, MinNode}= get_bound_queue_counts(Cluster, {undefined, undefined}),
  {ok, MinNode}.

validate_policy(_Args) -> ok.

%%---------------------------------------------------------------------------
%% Private helper functions
%%---------------------------------------------------------------------------

get_bound_queue_counts([], MinNode)   -> MinNode;
get_bound_queue_counts([Node|Rem], {undefined, undefined}) ->
  VHosts = rpc:call(Node, rabbit_vhost, list, []),
  Count  = get_total_vhost_bound_queues(Node, VHosts, 0),
  get_bound_queue_counts(Rem, {Count, Node});
get_bound_queue_counts([Node0|Rem], MinNode={Count, _Node}) ->
  VHosts = rpc:call(Node0, rabbit_vhost, list, []),
  Count0  = get_total_vhost_bound_queues(Node0, VHosts, 0),
  MinNode0 = if Count0 < Count -> {Count0, Node0};
                true           -> MinNode
             end,
  get_bound_queue_counts(Rem, MinNode0).

get_total_vhost_bound_queues(_Node, [], Count) -> Count;
get_total_vhost_bound_queues(Node, [VHostPath|Rem], Count) ->
  Count0 = length(rpc:call(Node, rabbit_binding, list, [VHostPath])),
  get_total_vhost_bound_queues(Node, Rem, Count+Count0).

