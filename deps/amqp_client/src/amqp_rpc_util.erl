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

-module(amqp_rpc_util).

-include_lib("rabbitmq_server/include/rabbit_framing.hrl").
-include("amqp_client.hrl").

-export([register_consumer/2]).
-export([encode/3,decode/2]).

% Registers a consumer in this channel
register_consumer(RpcClientState = #rpc_client_state{broker_config = BrokerConfig}, Consumer) ->
    #broker_config{channel_pid = ChannelPid, ticket = Ticket, queue = Q} = BrokerConfig,
    Tag = <<"">>,
    BasicConsume = #'basic.consume'{ticket = Ticket, queue = Q,
                                    consumer_tag = Tag,
                                    no_local = false, no_ack = true, exclusive = false, nowait = false},
    #'basic.consume_ok'{consumer_tag = ConsumerTag} = amqp_channel:call(ChannelPid, BasicConsume, Consumer),
    RpcClientState#rpc_client_state{consumer_tag = ConsumerTag}.

%---------------------------------------------------------------------------
% Encoding and decoding
%---------------------------------------------------------------------------

decode(?Hessian, [H|T]) ->
    hessian:decode(H).

encode(reply, ?Hessian, Payload) ->
    hessian:encode(reply, Payload);

encode(call, ?Hessian, [Function|Args]) ->
    hessian:encode(call, Function, Args).
