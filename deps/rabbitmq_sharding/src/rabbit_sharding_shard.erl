%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_sharding_shard).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([maybe_shard_exchanges/0,
         ensure_sharded_queues/1,
         maybe_update_shards/2,
         stop_sharding/1]).

-import(rabbit_misc, [r/3]).
-import(rabbit_sharding_util, [a2b/1, exchange_bin/1, make_queue_name/3,
                               routing_key/1, shards_per_node/1]).

-rabbit_boot_step({rabbit_sharding_maybe_shard,
                   [{description, "rabbit sharding maybe shard"},
                    {mfa,         {?MODULE, maybe_shard_exchanges, []}},
                    {requires,    recovery}]}).

-define(MAX_CONNECTION_CLOSE_TIMEOUT, 10000).
-define(SHARDING_USER, <<"rmq-sharding">>).

%% We make sure the sharded queues are created when
%% RabbitMQ starts.
maybe_shard_exchanges() ->
    [maybe_shard_exchanges(V) || V <- rabbit_vhost:list_names()],
    ok.

maybe_shard_exchanges(VHost) ->
    [ensure_sharded_queues(X) ||
        X <- rabbit_sharding_util:sharded_exchanges(VHost)].

%% queue needs to be declared on the respective node.
ensure_sharded_queues(X) ->
    add_queues(X),
    bind_queues(X).

maybe_update_shards(OldX, NewX) ->
    maybe_unbind_queues(routing_key(OldX), routing_key(NewX), OldX),
    add_queues(NewX),
    bind_queues(NewX).

stop_sharding(OldX) ->
    unbind_queues(shards_per_node(OldX), OldX).

%% routing key didn't change. Do nothing.
maybe_unbind_queues(RK, RK, _OldX) ->
    ok;
maybe_unbind_queues(_RK, _NewRK, OldX) ->
    unbind_queues(shards_per_node(OldX), OldX).

unbind_queues(undefined, _X) ->
    ok;
unbind_queues(OldSPN, #exchange{name = XName} = X) ->
    OldRKey = routing_key(X),
    foreach_node(fun(Node) ->
                         [unbind_queue(XName, OldRKey, N, Node)
                          || N <- lists:seq(0, OldSPN-1)]
                 end).

add_queues(#exchange{name = XName, durable = Durable} = X) ->
    SPN = shards_per_node(X),
    foreach_node(fun(Node) ->
                         [declare_queue(XName, Durable, N, Node)
                          || N <- lists:seq(0, SPN-1)]
                 end).

bind_queues(#exchange{name = XName} = X) ->
    RKey = routing_key(X),
    SPN = shards_per_node(X),
    foreach_node(fun(Node) ->
                         [bind_queue(XName, RKey, N, Node) ||
                             N <- lists:seq(0, SPN-1)]
                 end).

%%----------------------------------------------------------------------------

declare_queue(XName, Durable, N, Node) ->
    QBin = make_queue_name(exchange_bin(XName), a2b(Node), N),
    QueueName = rabbit_misc:r(v(XName), queue, QBin),
    try rabbit_amqqueue:declare(QueueName, Durable, false, [], none,
                                ?SHARDING_USER, {ignore_location, Node}) of
        {_Reply, _Q} ->
            ok
    catch
        _Error:Reason ->
            rabbit_log:error("sharding failed to declare queue for exchange ~p"
                             " - soft error:~n~p",
                             [exchange_bin(XName), Reason]),
            error
    end.

bind_queue(XName, RoutingKey, N, Node) ->
    binding_action(fun rabbit_binding:add/3,
                   XName, RoutingKey, N, Node,
                   "sharding failed to bind queue ~p to exchange ~p"
                   " - soft error:~n~p~n").

unbind_queue(XName, RoutingKey, N, Node) ->
    binding_action(fun rabbit_binding:remove/3,
                   XName, RoutingKey, N, Node,
                   "sharding failed to unbind queue ~p to exchange ~p"
                   " - soft error:~n~p~n").

binding_action(F, XName, RoutingKey, N, Node, ErrMsg) ->
    QBin = make_queue_name(exchange_bin(XName), a2b(Node), N),
    QueueName = rabbit_misc:r(v(XName), queue, QBin),
    case F(#binding{source      = XName,
                    destination = QueueName,
                    key         = RoutingKey,
                    args        = []},
           fun (_X, _Q) -> ok end,
           ?SHARDING_USER) of
        ok              -> ok;
        {error, Reason} ->
            rabbit_log:error(ErrMsg, [QBin, exchange_bin(XName), Reason]),
            error
    end.

v(#resource{virtual_host = VHost}) ->
    VHost.

foreach_node(F) ->
    [F(Node) || Node <- running_nodes()].

running_nodes() ->
    proplists:get_value(running_nodes, rabbit_mnesia:status(), []).
