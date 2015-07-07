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
%%% - Queue Master Location miscellaneous functions
%%%
%%% @end
%%% Created : 19. Jun 2015
%%%-------------------------------------------------------------------
-module(rabbit_queue_master_location_misc).
-behaviour(rabbit_policy_validator).

-include("rabbit.hrl").

%% API
-export([ lookup_master/2,
          lookup_queue/2,
          get_location/1,
          get_location_by_config/1,
          get_location_by_args/1,
          get_location_by_policy/1,
          all_nodes/0,
          delay_ms/1,
          policy/2,
          module/1 ]).

-export([ validate_policy/1 ]).

-rabbit_boot_step(
{?MODULE,
  [{description, "Queue location policy validation"},
    {mfa, {rabbit_registry, register,
      [policy_validator, << "queue-master-location" >>, ?MODULE]}}]}).

lookup_master(QueueNameBin, VHostPath) when is_binary(QueueNameBin),
                                            is_binary(VHostPath) ->
  Queue = rabbit_misc:r(VHostPath, queue, QueueNameBin),
  case rabbit_amqqueue:lookup(Queue) of
    {ok, #amqqueue{pid=Pid}} when is_pid(Pid) ->
      {ok, node(Pid)};
    Error -> Error
  end.

lookup_queue(QueueNameBin, VHostPath) when is_binary(QueueNameBin),
  is_binary(VHostPath) ->
  Queue = rabbit_misc:r(VHostPath, queue, QueueNameBin),
  case rabbit_amqqueue:lookup(Queue) of
    Reply = {ok, #amqqueue{}} ->
      Reply;
    Error -> Error
  end.

get_location(Queue=#amqqueue{})->
  case get_location_by_args(Queue) of
    _Err1={error, _} ->
      case get_location_by_policy(Queue) of
        _Err2={error, _} ->
          case get_location_by_config(Queue) of
            Err3={error, _}   -> Err3;
            Reply={ok, _Node} -> Reply
          end;
        Reply={ok, _Node} -> Reply
      end;
    Reply={ok, _Node} -> Reply
  end.

get_location_by_args(Queue=#amqqueue{arguments=Args}) ->
  case proplists:lookup( << "queue-master-location" >> , Args) of
    { << "queue-master-location" >> , Strategy}  ->
      case validate_strategy(Strategy) of
        {ok, CB} -> CB:queue_master_location(Queue);
        Error    -> Error
      end;
    _ -> {error, "queue-master-location undefined"}
  end.

get_location_by_policy(Queue=#amqqueue{}) ->
  case rabbit_policy:get( << "queue-master-location" >> , Queue) of
    undefined ->  {error, "queue-master-location policy undefined"};
    Strategy ->
      case validate_strategy(Strategy) of
        {ok, CB} -> CB:queue_master_location(Queue);
        Error    -> Error
      end
  end.

get_location_by_config(Queue=#amqqueue{}) ->
  case application:get_env(rabbit, queue_master_location) of
    {ok, Strategy} ->
      case validate_strategy(Strategy) of
        {ok, CB} -> CB:queue_master_location(Queue);
        Error    -> Error
      end;
    _ -> {error, "queue-master-location undefined"}
  end.


validate_policy(KeyList) ->
  case proplists:lookup( << "queue-master-location" >> , KeyList) of
    {_, Strategy} -> validate_strategy(Strategy);
    _ -> {error, "queue-master-location undefined"}
  end.

validate_strategy(Strategy) ->
  case module(Strategy) of
    R={ok, _M} -> R;
    _          -> {error, "~p is not a valid queue-master-location value", [Strategy]}
  end.


all_nodes()  -> rabbit_mnesia:cluster_nodes(running).

delay_ms(Ms) -> receive after Ms -> void end.

policy(Policy, Q) ->
  case rabbit_policy:get(Policy, Q) of
    undefined -> none;
    P         -> P
  end.

module(#amqqueue{} = Q) ->
  case rabbit_policy:get( << "queue-master-location" >> , Q) of
    undefined -> no_location_strategy;
    Mode      -> module(Mode)
  end;

module(Strategy) when is_binary(Strategy) ->
  case rabbit_registry:binary_to_type(Strategy) of
    {error, not_found} -> no_location_strategy;
    T                  -> case rabbit_registry:lookup_module(queue_master_locator, T) of
                            {ok, Module} -> {ok, Module};
                            _            -> no_location_strategy
                          end
  end.