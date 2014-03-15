-module(rabbit_sharding_shard).

-include_lib("amqp_client/include/amqp_client.hrl").

-export([maybe_shard_exchanges/0,
         ensure_sharded_queues/1,
         maybe_update_shards/2,
         maybe_stop_sharding/1]).

-import(rabbit_misc, [r/3]).
-import(rabbit_sharding_util, [a2b/1, exchange_bin/1,
                               routing_key/1, shards_per_node/1]).

-rabbit_boot_step({rabbit_sharding_maybe_shard,
                   [{description, "rabbit sharding maybe shard"},
                    {mfa,         {?MODULE, maybe_shard_exchanges, []}},
                    {requires,    direct_client},
                    {enables,     networking}]}).

-define(MAX_CONNECTION_CLOSE_TIMEOUT, 10000).

%% We make sure the sharded queues are created when
%% RabbitMQ starts.
maybe_shard_exchanges() ->
    [maybe_shard_exchanges(V) || V <- rabbit_vhost:list()],
    ok.

maybe_shard_exchanges(VHost) ->
    [rabbit_sharding_util:rpc_call(ensure_sharded_queues, [X]) ||
        X <- rabbit_sharding_util:sharded_exchanges(VHost)].

%% queue needs to be started on the respective node.
%% connection will be local.
%% each rabbit_sharding_shard will receive the event
%% and can declare the queue locally
ensure_sharded_queues(X) ->
    add_queues(X),
    bind_queues(X).

maybe_update_shards(OldX, NewX) ->
    maybe_unbind_queues(routing_key(OldX), routing_key(NewX), OldX),
    add_queues(NewX),
    bind_queues(NewX).

maybe_stop_sharding(OldX) ->
    unbind_queues(shards_per_node(OldX), OldX).

%% routing key didn't change. Do nothing.
maybe_unbind_queues(RK, RK, _OldX) ->
    ok;
maybe_unbind_queues(_RK, _NewRK, OldX) ->
    unbind_queues(shards_per_node(OldX), OldX).

unbind_queues(undefined, _X) ->
    ok;
unbind_queues(OldSPN, #exchange{name = XName} = X) ->
    XBin = exchange_bin(XName),
    OldRKey = routing_key(X),
    F = fun (N) ->
                QBin = rabbit_sharding_util:make_queue_name(
                         XBin, a2b(node()), N),
                [#'queue.unbind'{exchange    = XBin,
                                 queue       = QBin,
                                 routing_key = OldRKey}]
        end,
    ErrFun = fun(Code, Text) -> {error, Code, Text} end,
    amqp_calls(X, calls(F, OldSPN), ErrFun).

add_queues(#exchange{name = XName} = X) ->
    SPN = shards_per_node(X),
    XBin = exchange_bin(XName),
    F = fun (N) ->
                QBin = rabbit_sharding_util:make_queue_name(
                         XBin, a2b(node()), N),
                [#'queue.declare'{queue = QBin, durable = true}]
        end,
    ErrFun = fun(Code, Text) -> {error, Code, Text} end,
    amqp_calls(X, calls(F, SPN), ErrFun).

bind_queues(#exchange{name = XName} = X) ->
    RKey = routing_key(X),
    SPN = shards_per_node(X),
    XBin = exchange_bin(XName),
    F = fun (N) ->
                QBin = rabbit_sharding_util:make_queue_name(
                         XBin, a2b(node()), N),
                #'queue.bind'{exchange = XBin,
                              queue = QBin,
                              routing_key = RKey}
        end,
    ErrFun = fun(Code, Text) -> {error, Code, Text} end,
    amqp_calls(X, calls(F, SPN), ErrFun).

%%----------------------------------------------------------------------------

calls(F, SPN) ->
    lists:flatten([F(N) || N <- lists:seq(0, SPN-1)]).

amqp_calls(X, Methods, ErrFun) ->
    case open(X) of
        {ok, Conn, Ch} ->
            try
                [amqp_channel:call(Ch, Method) || Method <- Methods]
            catch exit:{{shutdown, {connection_closing,
                                    {server_initiated_close, Code, Txt}}}, _} ->
                    ErrFun(Code, Txt)
            after
                ensure_connection_closed(Conn)
            end;
        E ->
            E
    end.

ensure_connection_closed(Conn) ->
    catch amqp_connection:close(Conn, ?MAX_CONNECTION_CLOSE_TIMEOUT).

open(X) ->
    case amqp_connection:start(params(X)) of
        {ok, Conn} -> case amqp_connection:open_channel(Conn) of
                          {ok, Ch} -> {ok, Conn, Ch};
                          E        -> catch amqp_connection:close(Conn),
                                      E
                      end;
        E -> E
    end.

params(#exchange{name = #resource{virtual_host = VHost}}) ->
    #amqp_params_direct{virtual_host = VHost}.
