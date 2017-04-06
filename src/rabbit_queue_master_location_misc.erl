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
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_queue_master_location_misc).

-include("rabbit.hrl").

-export([lookup_master/2,
         lookup_queue/2,
         get_location/1,
         get_location_mod_by_config/1,
         get_location_mod_by_args/1,
         get_location_mod_by_policy/1,
         all_nodes/0]).

lookup_master(QueueNameBin, VHostPath) when is_binary(QueueNameBin),
                                            is_binary(VHostPath) ->
    Queue = rabbit_misc:r(VHostPath, queue, QueueNameBin),
    case rabbit_amqqueue:lookup(Queue) of
        {ok, #amqqueue{pid = Pid}} when is_pid(Pid) ->
            {ok, node(Pid)};
        Error -> Error
    end.

lookup_queue(QueueNameBin, VHostPath) when is_binary(QueueNameBin),
                                           is_binary(VHostPath) ->
    Queue = rabbit_misc:r(VHostPath, queue, QueueNameBin),
    case rabbit_amqqueue:lookup(Queue) of
        Reply = {ok, #amqqueue{}} -> Reply;
        Error                     -> Error
    end.

get_location(Queue=#amqqueue{})->
    Reply1 = case get_location_mod_by_args(Queue) of
                 _Err1 = {error, _} ->
                     case get_location_mod_by_policy(Queue) of
                         _Err2 = {error, _} ->
                             case get_location_mod_by_config(Queue) of
                                 Err3 = {error, _}      -> Err3;
                                 Reply0 = {ok, _Module} -> Reply0
                             end;
                         Reply0 = {ok, _Module} -> Reply0
                     end;
                 Reply0 = {ok, _Module} -> Reply0
             end,

    case Reply1 of
        {ok, CB} -> CB:queue_master_location(Queue);
        Error    -> Error
    end.

get_location_mod_by_args(#amqqueue{arguments=Args}) ->
    case rabbit_misc:table_lookup(Args, <<"x-queue-master-locator">>) of
        {_Type, Strategy}  ->
            case rabbit_queue_location_validator:validate_strategy(Strategy) of
                Reply = {ok, _CB} -> Reply;
                Error             -> Error
            end;
        _ -> {error, "x-queue-master-locator undefined"}
    end.

get_location_mod_by_policy(Queue=#amqqueue{}) ->
    case rabbit_policy:get(<<"queue-master-locator">> , Queue) of
        undefined ->  {error, "queue-master-locator policy undefined"};
        Strategy  ->
            case rabbit_queue_location_validator:validate_strategy(Strategy) of
                Reply = {ok, _CB} -> Reply;
                Error             -> Error
            end
    end.

get_location_mod_by_config(#amqqueue{}) ->
    case application:get_env(rabbit, queue_master_locator) of
        {ok, Strategy} ->
            case rabbit_queue_location_validator:validate_strategy(Strategy) of
                Reply = {ok, _CB} -> Reply;
                Error             -> Error
            end;
        _ -> {error, "queue_master_locator undefined"}
    end.

all_nodes()  -> rabbit_mnesia:cluster_nodes(running).
