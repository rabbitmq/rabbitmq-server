%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_queue_master_location_misc).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("amqqueue.hrl").

-export([lookup_master/2,
         lookup_queue/2,
         get_location/1,
         get_location_mod_by_config/1,
         get_location_mod_by_args/1,
         get_location_mod_by_policy/1,
         all_nodes/1]).

-spec lookup_master(binary(), binary()) -> {ok, node()} | {error, not_found}.
lookup_master(QueueNameBin, VHostPath) when is_binary(QueueNameBin),
                                            is_binary(VHostPath) ->
    QueueR = rabbit_misc:r(VHostPath, queue, QueueNameBin),
    case rabbit_amqqueue:lookup(QueueR) of
        {ok, Queue} when ?amqqueue_has_valid_pid(Queue) ->
            Pid = amqqueue:get_pid(Queue),
            {ok, node(Pid)};
        Error -> Error
    end.

lookup_queue(QueueNameBin, VHostPath) when is_binary(QueueNameBin),
                                           is_binary(VHostPath) ->
    QueueR = rabbit_misc:r(VHostPath, queue, QueueNameBin),
    case rabbit_amqqueue:lookup(QueueR) of
        Reply = {ok, Queue} when ?is_amqqueue(Queue) ->
            Reply;
        Error ->
            Error
    end.

get_location(Queue) when ?is_amqqueue(Queue) ->
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

get_location_mod_by_args(Queue) when ?is_amqqueue(Queue) ->
    Args = amqqueue:get_arguments(Queue),
    case rabbit_misc:table_lookup(Args, <<"x-queue-master-locator">>) of
        {_Type, Strategy}  ->
            case rabbit_queue_location_validator:validate_strategy(Strategy) of
                Reply = {ok, _CB} -> Reply;
                Error             -> Error
            end;
        _ -> {error, "x-queue-master-locator undefined"}
    end.

get_location_mod_by_policy(Queue) when ?is_amqqueue(Queue) ->
    case rabbit_policy:get(<<"queue-master-locator">> , Queue) of
        undefined ->  {error, "queue-master-locator policy undefined"};
        Strategy  ->
            case rabbit_queue_location_validator:validate_strategy(Strategy) of
                Reply = {ok, _CB} -> Reply;
                Error             -> Error
            end
    end.

get_location_mod_by_config(Queue) when ?is_amqqueue(Queue) ->
    case application:get_env(rabbit, queue_master_locator) of
        {ok, Strategy} ->
            case rabbit_queue_location_validator:validate_strategy(Strategy) of
                Reply = {ok, _CB} -> Reply;
                Error             -> Error
            end;
        _ -> {error, "queue_master_locator undefined"}
    end.

all_nodes(Queue) when ?is_amqqueue(Queue) ->
    handle_is_mirrored_ha_nodes(rabbit_mirror_queue_misc:is_mirrored_ha_nodes(Queue), Queue).

handle_is_mirrored_ha_nodes(false, _Queue) ->
    % Note: ha-mode is NOT 'nodes' - it is either exactly or all, which means
    % that any node in the cluster is eligible to be the new queue master node
    rabbit_nodes:all_running();
handle_is_mirrored_ha_nodes(true, Queue) ->
    % Note: ha-mode is 'nodes', which explicitly specifies allowed nodes.
    % We must use suggested_queue_nodes to get that list of nodes as the
    % starting point for finding the queue master location
    handle_suggested_queue_nodes(rabbit_mirror_queue_misc:suggested_queue_nodes(Queue)).

handle_suggested_queue_nodes({_MNode, []}) ->
    rabbit_nodes:all_running();
handle_suggested_queue_nodes({MNode, SNodes}) ->
    [MNode | SNodes].
