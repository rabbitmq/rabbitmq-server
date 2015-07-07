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
%%% - Queue Master Location callback implementation
%%%
%%% @end
%%% Created : 19. Jun 2015 15:34
%%%-------------------------------------------------------------------
-module(rabbit_queue_location_round_robin).
-behaviour(rabbit_queue_master_location).

-include("rabbit.hrl").

-export([ init/1,
          description/0,
          queue_master_location/3,
          validate_policy/1 ]).

-record(state, {rr_pointer=1, cluster_size=2}). % cluster must start at any value > pointer

-rabbit_boot_step({?MODULE,
  [{description, "Locate queue master node in a round robin manner"},
    {mfa,         {rabbit_registry, register,
      [queue_master_location_strategy, <<"round-robin">>, ?MODULE]}},
    {requires,    rabbit_registry},
    {enables,     kernel_ready}]}).


%%---------------------------------------------------------------------------
%% Queue Master Location Callbacks
%%---------------------------------------------------------------------------

description() ->
  [{description, <<"Locate queue master node in a round robin manner">>}].

init(_Args) ->
  {ok, #state{}}.

queue_master_location(#amqqueue{}, Cluster, CbState=#state{rr_pointer   = RR_Pointer,
                                                           cluster_size = RR_Pointer}) ->
  MasterNode = lists:nth(RR_Pointer, Cluster),
  {reply, MasterNode, CbState#state{rr_pointer=1, cluster_size=length(Cluster)}};

queue_master_location(#amqqueue{}, Cluster, CbState=#state{rr_pointer  = RR_Pointer,
                                                           cluster_size= ClusterSize}) when RR_Pointer =< ClusterSize,
                                                                                            length(Cluster) > 0 ->
  MasterNode = lists:nth(RR_Pointer,Cluster),
  {reply, MasterNode, CbState#state{rr_pointer=RR_Pointer+1,cluster_size=length(Cluster)}}.

validate_policy(_Args) -> ok.