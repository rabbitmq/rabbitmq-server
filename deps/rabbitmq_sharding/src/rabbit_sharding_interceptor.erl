%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_sharding_interceptor).

-include_lib("rabbit_common/include/rabbit_framing.hrl").

-behaviour(rabbit_channel_interceptor).

-export([description/0, intercept/3, applies_to/0, init/1]).

%% exported for tests
-export([consumer_count/1]).

-import(rabbit_sharding_util, [a2b/1, shards_per_node/1]).
-import(rabbit_misc, [r/3, format/2, protocol_error/3]).

-rabbit_boot_step({?MODULE,
                   [{description, "sharding interceptor"},
                    {mfa, {rabbit_registry, register,
                           [channel_interceptor,
                            <<"sharding interceptor">>, ?MODULE]}},
                    {cleanup, {rabbit_registry, unregister,
                               [channel_interceptor,
                                <<"sharding interceptor">>]}},
                    {requires, rabbit_registry},
                    {enables, recovery}]}).

init(Ch) ->
    rabbit_channel:get_vhost(Ch).

description() ->
    [{description, <<"Sharding interceptor for channel methods">>}].

intercept(#'basic.consume'{queue = QName} = Method, Content, VHost) ->
    case queue_name(VHost, QName) of
        {ok, QName2} ->
            {Method#'basic.consume'{queue = QName2}, Content};
        {error, QName} ->
            precondition_failed("Error finding sharded queue for: ~p", [QName])
    end;

intercept(#'basic.get'{queue = QName} = Method, Content, VHost) ->
    case queue_name(VHost, QName) of
        {ok, QName2} ->
            {Method#'basic.get'{queue = QName2}, Content};
        {error, QName} ->
            precondition_failed("Error finding sharded queue for: ~p", [QName])
    end;

intercept(#'queue.delete'{queue = QName} = Method, Content, VHost) ->
    case is_sharded(VHost, QName) of
        true ->
            precondition_failed("Can't delete sharded queue: ~p", [QName]);
        _    ->
            {Method, Content}
    end;

intercept(#'queue.declare'{queue = QName} = Method, Content, VHost) ->
    case is_sharded(VHost, QName) of
        true ->
            %% Since as an interceptor we can't modify what the channel
            %% will return, we then modify the queue name so the channel
            %% can at least return a queue.declare_ok for that particular
            %% queue. Picking the first queue over the others is totally
            %% arbitrary.
            QName2 = rabbit_sharding_util:make_queue_name(
                                      QName, a2b(node()), 0),
            {Method#'queue.declare'{queue = QName2}, Content};
        _    ->
            {Method, Content}
    end;

intercept(#'queue.bind'{queue = QName} = Method, Content, VHost) ->
    case is_sharded(VHost, QName) of
        true ->
            precondition_failed("Can't bind sharded queue: ~p", [QName]);
        _    ->
            {Method, Content}
    end;

intercept(#'queue.unbind'{queue = QName} = Method, Content, VHost) ->
    case is_sharded(VHost, QName) of
        true ->
            precondition_failed("Can't unbind sharded queue: ~p", [QName]);
        _    ->
            {Method, Content}
    end;

intercept(#'queue.purge'{queue = QName} = Method, Content, VHost) ->
    case is_sharded(VHost, QName) of
        true ->
            precondition_failed("Can't purge sharded queue: ~p", [QName]);
        _    ->
            {Method, Content}
    end;

intercept(Method, Content, _VHost) ->
    {Method, Content}.

applies_to() ->
    ['basic.consume', 'basic.get', 'queue.delete', 'queue.declare',
     'queue.bind', 'queue.unbind', 'queue.purge'].

%%----------------------------------------------------------------------------

%% If the queue is not part of a shard, return unmodified name
queue_name(VHost, QBin) ->
    case lookup_exchange(VHost, QBin) of
        {ok, X}  ->
            case rabbit_sharding_util:shard(X) of
                true ->
                    least_consumers(VHost, QBin, shards_per_node(X));
                _    ->
                    {ok, QBin}
            end;
        _Error ->
            {ok, QBin}
    end.

is_sharded(VHost, QBin) ->
    case lookup_exchange(VHost, QBin) of
        {ok, X} ->
            rabbit_sharding_util:shard(X);
        _Error ->
            false
    end.

lookup_exchange(VHost, QBin) ->
    rabbit_exchange:lookup(r(VHost, exchange, QBin)).

least_consumers(VHost, QBin, N) ->
    F = fun(QNum) ->
                QBin2 = rabbit_sharding_util:make_queue_name(
                          QBin, a2b(node()), QNum),
                case consumer_count(r(VHost, queue, QBin2)) of
                    {error, E}       -> {error, E};
                    [{consumers, C}] -> {C, QBin2}
                end

        end,
    case queues_with_count(F, N) of
        []     ->
            {error, QBin};
        Queues ->
            [{_, QBin3} | _ ] = lists:sort(Queues),
            {ok, QBin3}
    end.

queues_with_count(F, N) ->
    lists:foldl(fun (C, Acc) ->
                        case F(C) of
                            {error, _} -> Acc;
                            Ret        -> [Ret|Acc]
                        end
                end, [], lists:seq(0, N-1)).

consumer_count(QName) ->
    rabbit_amqqueue:with(
      QName,
      fun(Q) ->
              rabbit_amqqueue:info(Q, [consumers])
      end).

precondition_failed(Format, QName) ->
    protocol_error(precondition_failed, Format, QName).
