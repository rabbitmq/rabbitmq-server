-module(amqp_method_util).

-include("amqp_client.hrl").
-include_lib("rabbit/include/rabbit_framing.hrl").

-export([register_consumer/2]).

% Registers a consumer in this channel
register_consumer(RpcClientState = #rpc_client{channel_pid = ChannelPid, ticket = Ticket, queue = Q}, Consumer) ->
    Tag = <<"">>,
    BasicConsume = #'basic.consume'{ticket = Ticket, queue = Q,
                                    consumer_tag = Tag,
                                    no_local = false, no_ack = true, exclusive = false, nowait = false},
    #'basic.consume_ok'{consumer_tag = ConsumerTag} = amqp_channel:rpc(ChannelPid, BasicConsume, Consumer),
    RpcClientState#rpc_client{consumer_tag = ConsumerTag}.
