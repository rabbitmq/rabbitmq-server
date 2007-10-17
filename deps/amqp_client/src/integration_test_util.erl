-module(integration_test_util).

-include_lib("rabbitmq_server/include/rabbit_framing.hrl").
-include("amqp_client.hrl").

-export([rpc_client_test/1]).

rpc_client_test(Connection) ->
    io:format("Starting RPC client test......~n"),
    X = <<"x">>,
    BindKey = <<"a.b.c.*">>,
    RoutingKey = <<"a.b.c.d">>,
    Realm = <<"/data">>,
    BindKey = <<"a.b.c.*">>,
    RoutingKey = <<"a.b.c.d">>,
    Q = <<"a.b.c">>,
    Module = transport_agnostic_server,
    Function = add,
    Args = [2,2],
    ContentType = ?Hessian,
    {ChannelPid, Ticket} = test_util:setup_channel(Connection, Realm),
    BrokerConfig = #broker_config{channel_pid = ChannelPid, ticket = Ticket,
                                       exchange = X, routing_key = RoutingKey,
                                       queue = Q},
    RpcHandlerState = #rpc_handler_state{broker_config = BrokerConfig,
                                         server_name = Module},
    {ok, Consumer} = gen_event:start_link(),
    gen_event:add_handler(Consumer, amqp_rpc_handler , [RpcHandlerState] ),
    BasicConsume = #'basic.consume'{ticket = Ticket, queue = Q,
                                    consumer_tag = <<"">>,
                                    no_local = false, no_ack = true, exclusive = false, nowait = false},
    #'basic.consume_ok'{consumer_tag = ConsumerTag} = amqp_channel:call(ChannelPid, BasicConsume, Consumer),
    RpcClientPid = amqp_rpc_client:start(BrokerConfig),
    Reply = amqp_rpc_client:call(RpcClientPid, ContentType, Function, Args),
    io:format("Reply from RPC was ~p~n", [Reply]),
    test_util:teardown(Connection, ChannelPid).
