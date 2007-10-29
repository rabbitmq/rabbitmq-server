%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is the RabbitMQ Erlang Client.
%%
%%   The Initial Developers of the Original Code are LShift Ltd.,
%%   Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd., Cohesive Financial
%%   Technologies LLC., and Rabbit Technologies Ltd. are Copyright (C) 
%%   2007 LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit 
%%   Technologies Ltd.; 
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): Ben Hood <0x6e6562@gmail.com>.
%%

-module(integration_test_util).

-include_lib("rabbitmq_server/include/rabbit_framing.hrl").
-include("amqp_client.hrl").

-export([rpc_client_test/1]).
-export([rabbit_management_test/1]).
-export([start_rpc_handler/2]).
-export([start_rabbit_management/1, stop_rabbit_management/2]).

rpc_client_test(Connection) ->
    Module = transport_agnostic_server,
    Function = add,
    Args = [2,2],
    rpc_util(Connection, Module, Function, Args).

rabbit_management_test(Connection) ->
    Module = rabbit_management,
    {X,Y,Username} = now(),
    Password = <<"password">>,
    {ChannelPid,BrokerConfig} = setup_broker(Connection),
    RpcClientPid = amqp_rpc_client:start(BrokerConfig),
    ok = rpc(RpcClientPid, add_user, [Username, Password]),
    Users1 = rpc(RpcClientPid, list_users, []),
    ok = rpc(RpcClientPid, delete_user, [Username]),
    Users2 = rpc(RpcClientPid, list_users, []),
    test_util:teardown(Connection, ChannelPid).

start_rabbit_management(Connection) ->
    {ChannelPid,BrokerConfig} = setup_broker(Connection),
    start_rpc_handler(rabbit_management, BrokerConfig),
    ChannelPid.

stop_rabbit_management(Connection, ChannelPid) ->
    test_util:teardown(Connection, ChannelPid).

start_rpc_handler(Module, BrokerConfig = #broker_config{ticket = Ticket,
                                                        queue = Q,
                                                        channel_pid = ChannelPid}) ->
    RpcHandlerState = #rpc_handler_state{broker_config = BrokerConfig,
                                         server_name = Module},
    {ok, Consumer} = gen_event:start_link(),
    gen_event:add_handler(Consumer, amqp_rpc_handler , [RpcHandlerState] ),
    BasicConsume = #'basic.consume'{ticket = Ticket, queue = Q,
                                    consumer_tag = <<"">>,
                                    no_local = false, no_ack = true, exclusive = false, nowait = false},
    #'basic.consume_ok'{consumer_tag = ConsumerTag} = amqp_channel:call(ChannelPid, BasicConsume, Consumer).

rpc_util(Connection, Module, Function, Args) ->
    {ChannelPid,BrokerConfig} = setup_broker(Connection),
    start_rpc_handler(Module, BrokerConfig),
    RpcClientPid = amqp_rpc_client:start(BrokerConfig),
    Reply = rpc(RpcClientPid, Function, Args),
    test_util:teardown(Connection, ChannelPid),
    Reply.

rpc(RpcClientPid, Function, Args) ->
    ContentType = ?Hessian,
    amqp_rpc_client:call(RpcClientPid, ContentType, Function, Args).

setup_broker(Connection) ->
    X = <<"x">>,
    BindKey = <<"a.b.c.*">>,
    RoutingKey = <<"a.b.c.d">>,
    Realm = <<"/data">>,
    BindKey = <<"a.b.c.*">>,
    RoutingKey = <<"a.b.c.d">>,
    Q = <<"a.b.c">>,
    {ChannelPid, Ticket} = test_util:setup_channel(Connection, Realm),
    ok = test_util:setup_exchange(ChannelPid, Ticket, Q, X, BindKey),
    BrokerConfig = #broker_config{channel_pid = ChannelPid, ticket = Ticket,
                                       exchange = X, routing_key = RoutingKey,
                                       queue = Q},
    {ChannelPid,BrokerConfig}.
