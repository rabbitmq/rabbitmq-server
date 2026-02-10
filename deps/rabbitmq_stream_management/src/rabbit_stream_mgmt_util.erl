%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_stream_mgmt_util).

%% TODO sort all this out; maybe there's scope for rabbit_mgmt_request?

-export([find_super_stream/3,
         find_super_stream/1,
         find_super_stream_from_exchange/1, find_super_stream_from_exchange/3]).
-include_lib("kernel/include/logger.hrl").
-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------
find_super_stream(ReqData) ->
    case rabbit_mgmt_util:vhost(ReqData) of
        not_found -> {error, missing_vhost};
        VHost     -> find_super_stream(VHost, id(queue, ReqData), rabbit_mgmt_util:range_ceil(ReqData))
    
    end.
find_super_stream(Vhost, Name, Range) ->
    case exchange(Vhost, Name) of
        not_found -> {error, not_found};
        E -> find_super_stream_from_exchange(E, detailed, Range)
    end.

find_super_stream_from_exchange(Exchange) ->
    find_super_stream_from_exchange(Exchange, basic, undefined).

find_super_stream_from_exchange(Exchange, Mode, Range) ->
    case maps:get(<<"x-super-stream">>, proplists:get_value(arguments, Exchange, []), false) of 
        true -> 
            Vhost = proplists:get_value(vhost, Exchange),
            Name = proplists:get_value(name, Exchange),
            Partitions = super_stream_partitions(list_bindings(Vhost, Name), Mode, Range),
            [{name, Name},
            {vhost, Vhost},
            {arguments, extract_arguments_from_partitions(Partitions)},
            {partition_count, erlang:length(Partitions)},
            {partitions, drop_partition_arguments(Partitions)}];
        false -> 
             {error, not_super_stream}
    end.

%%--------------------------------------------------------------------
           
super_stream_partitions(Bindings, Mode, Range) ->
    [ format_partition(S, Mode, Range) || S <- Bindings ].

drop_partition_arguments(Partitions) ->
    lists:map(fun(P) -> proplists:delete(arguments, P) end, Partitions).

extract_arguments_from_partitions(Partitions) -> 
    case proplists:get_value(arguments, lists:nth(1, Partitions)) of
        List when is_list(List) -> rabbit_mgmt_format:amqp_table(List);
        Map when is_map(Map) -> Map
    end.

format_partition(#binding{source        = _S,
                            key         = Key,
                            destination = #resource{name = PartitionName} = D,
                            args        = Args}, Mode, Range) ->
    
    Format = [
       {routing_key,      Key},
       {name,             PartitionName},
       {order,            get_partition_order(Args)}],
    
    Format ++ case Mode of 
        basic -> 
            case rabbit_amqqueue:lookup(D) of 
                {ok, Q} -> rabbit_amqqueue:info(Q, [state, arguments]);
                {error, not_found} -> [{state, not_found}]
            end;
        detailed ->
            Q0 = [queue(D#resource.virtual_host, D#resource.name)],
            [Q] = rabbit_mgmt_db:augment_queues(Q0, Range, full),
            rabbit_mgmt_format:clean_consumer_details(
                rabbit_mgmt_format:strip_pids(Q))
    end.

queue(VHost, QName) ->
    Name = rabbit_misc:r(VHost, queue, QName),
    case rabbit_amqqueue:lookup(Name) of
        {ok, Q}            -> rabbit_mgmt_format:queue(Q);
        {error, not_found} -> not_found
    end.

get_partition_order(Args) ->
    case lists:search(fun({<<"x-stream-partition-order">>, long, _}) -> true;
                        (_) -> false
                    end, Args) of
        {value, {_, _, Order}} -> Order;
        false -> not_found
    end.

id(Type, ReqData) ->
    rabbit_mgmt_util:id(Type, ReqData).

exchange(VHost, XName) ->
    Name = rabbit_misc:r(VHost, exchange, XName),
    case rabbit_exchange:lookup(Name) of
        {ok, X}            -> rabbit_mgmt_format:exchange(
                                rabbit_exchange:info(X));
        {error, not_found} -> not_found
    end.

list_bindings(Vhost, Exchange) ->
    rabbit_binding:list_for_source(rabbit_misc:r(Vhost, exchange, Exchange)).

