-module(lib_amqp).

-include_lib("rabbitmq_server/include/rabbit.hrl").
-include_lib("rabbitmq_server/include/rabbit_framing.hrl").
-include("amqp_client.hrl").

-compile(export_all).

start_connection() ->
    amqp_connection:start("guest", "guest").

start_connection(Host) ->
    amqp_connection:start("guest", "guest", Host).    

start_channel(Connection) ->
    Realm = <<"/data">>,
    Channel = amqp_connection:open_channel(Connection),
    Access = #'access.request'{realm = Realm,
                               exclusive = false,
                               passive = true,
                               active = true,
                               write = true,
                               read = true},
    #'access.request_ok'{ticket = Ticket} = amqp_channel:call(Channel, Access),
    {Channel,Ticket}.
    
declare_exchange(Channel,Ticket,X) ->
    ExchangeDeclare = #'exchange.declare'{ticket = Ticket, exchange = X,
                                          type = <<"direct">>,
                                          passive = false, durable = false, 
                                          auto_delete = false, internal = false,
                                          nowait = false, arguments = []},
    amqp_channel:call(Channel, ExchangeDeclare).
    
publish(Channel,Ticket,X,RoutingKey,Payload) ->
    BasicPublish = #'basic.publish'{ticket = Ticket,
                                    exchange = X,
                                    routing_key = RoutingKey,
                                    mandatory = false,immediate = false},
    Content = #content{class_id = 60,
                   properties = amqp_util:basic_properties(), 
                   properties_bin = none,
                   payload_fragments_rev = [Payload]},
    amqp_channel:cast(Channel, BasicPublish, Content).
    
teardown(Connection,Channel) ->
    ChannelClose = #'channel.close'{reply_code = 200, reply_text = <<"Goodbye">>,
                                    class_id = 0, method_id = 0},
    amqp_channel:call(Channel, ChannelClose),
    ConnectionClose = #'connection.close'{reply_code = 200, reply_text = <<"Goodbye">>,
                                              class_id = 0, method_id = 0},
    amqp_connection:close(Connection, ConnectionClose).    