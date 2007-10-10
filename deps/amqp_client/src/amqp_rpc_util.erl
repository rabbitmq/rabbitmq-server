-module(amqp_rpc_util).

-include_lib("rabbitmq_server/include/rabbit_framing.hrl").
-include("amqp_client.hrl").

-export([register_consumer/2]).

% Registers a consumer in this channel
register_consumer(RpcClientState = #rpc_client_state{broker_config = BrokerConfig}, Consumer) ->
    #broker_config{channel_pid = ChannelPid, ticket = Ticket, queue = Q} = BrokerConfig,
    Tag = <<"">>,
    BasicConsume = #'basic.consume'{ticket = Ticket, queue = Q,
                                    consumer_tag = Tag,
                                    no_local = false, no_ack = true, exclusive = false, nowait = false},
    #'basic.consume_ok'{consumer_tag = ConsumerTag} = amqp_channel:call(ChannelPid, BasicConsume, Consumer),
    RpcClientState#rpc_client_state{consumer_tag = ConsumerTag}.
