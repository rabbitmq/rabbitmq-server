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
%%% - Queue Master Location 'random' selection callback implementation
%%%
%%% @end
%%% Created : 19. Jun 2015
%%%-------------------------------------------------------------------
-module(rabbit_queue_location_random).
-behaviour(rabbit_queue_master_locator).

-include("rabbit.hrl").

-export([ description/0, queue_master_location/1, validate_policy/1  ]).

-rabbit_boot_step({?MODULE,
  [{description, "Locate queue master node from cluster in a random manner"},
    {mfa,         {rabbit_registry, register,
      [queue_master_locator, <<"random">>, ?MODULE]}},
    {requires,    rabbit_registry},
    {enables,     kernel_ready}]}).

%%---------------------------------------------------------------------------
%% Queue Master Location Callbacks
%%---------------------------------------------------------------------------

description() ->
  [{description, <<"Locate queue master node from cluster in a random manner">>}].

queue_master_location(#amqqueue{}) ->
  Cluster    = rabbit_queue_master_location_misc:all_nodes(),
  RandomPos  = erlang:phash(now(), length(Cluster)),
  MasterNode = lists:nth(RandomPos, Cluster),
  {ok, MasterNode}.

validate_policy(_Args) -> ok.