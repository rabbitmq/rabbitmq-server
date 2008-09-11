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

-module(amqp_rpc_client).

-include_lib("rabbitmq_server/include/rabbit_framing.hrl").
-include_lib("rabbitmq_server/include/rabbit.hrl").
-include("amqp_client.hrl").

-behaviour(gen_server).

-export([start/2]).
-export([call/4]).
-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2, handle_info/2]).

%---------------------------------------------------------------------------
% API
%---------------------------------------------------------------------------

start(BrokerConfig, TypeMapping) ->
    {ok, RpcClientPid} = gen_server:start(?MODULE, [BrokerConfig, TypeMapping], []),
    RpcClientPid.

call(RpcClientPid, ContentType, Function, Args) ->
    gen_server:call(RpcClientPid, {ContentType, [Function|Args]} ).

%---------------------------------------------------------------------------
% Plumbing
%---------------------------------------------------------------------------

% Sets up a reply queue for this client to listen on
setup_reply_queue(State = #rpc_client_state{broker_config = BrokerConfig}) ->
    #broker_config{channel_pid = ChannelPid} = BrokerConfig,
    QueueDeclare = #'queue.declare'{queue = <<>>,
                                    passive = false, durable = false,
                                    exclusive = false, auto_delete = false,
                                    nowait = false, arguments = []},
    #'queue.declare_ok'{queue = Q,
                        message_count = MessageCount,
                        consumer_count = ConsumerCount}
                        = amqp_channel:call(ChannelPid, QueueDeclare),
    NewBrokerConfig = BrokerConfig#broker_config{queue = Q},
    State#rpc_client_state{broker_config = NewBrokerConfig}.

% Sets up a consumer to handle rpc responses
setup_consumer(State) ->
    ConsumerTag = amqp_rpc_util:register_consumer(State, self()),
    State#rpc_client_state{consumer_tag = ConsumerTag}.

% Publishes to the broker, stores the From address against
% the correlation id and increments the correlationid for
% the next request
publish({ContentType, [Function|Args] }, From,
        State = #rpc_client_state{broker_config = BrokerConfig,
                                  correlation_id = CorrelationId,
                                  continuations = Continuations,
                                  type_mapping = TypeMapping}) ->
    Payload = amqp_rpc_util:encode(call, ContentType, [Function|Args], TypeMapping ),
    #broker_config{channel_pid = ChannelPid, queue = Q,
                   exchange = X, routing_key = RoutingKey} = BrokerConfig,
    BasicPublish = #'basic.publish'{exchange = X,
                                    routing_key = RoutingKey,
                                    mandatory = false, immediate = false},
    _CorrelationId = integer_to_list(CorrelationId),
    Props = #'P_basic'{correlation_id = list_to_binary(_CorrelationId),
                       reply_to = Q, content_type = ContentType},
    Content = #content{class_id = 60, %% TODO HARDCODED VALUE
                       properties = Props, properties_bin = 'none',
                       payload_fragments_rev = [Payload]},
    amqp_channel:cast(ChannelPid, BasicPublish, Content),
    NewContinuations = dict:store(_CorrelationId, From , Continuations),
    State#rpc_client_state{correlation_id = CorrelationId + 1, continuations = NewContinuations}.

%---------------------------------------------------------------------------
% gen_server callbacks
%---------------------------------------------------------------------------

% Sets up a reply queue and consumer within an existing channel
init([BrokerConfig, TypeMapping]) ->
    InitialState = #rpc_client_state{broker_config = BrokerConfig,
                                     type_mapping = TypeMapping},
    State = setup_reply_queue(InitialState),
    NewState = setup_consumer(State),
    {ok, NewState}.

terminate(Reason, State) ->
    ok.

handle_call( Payload = {ContentType, [Function|Args] }, From, State) ->
    NewState = publish(Payload, From, State),
    {noreply, NewState}.

handle_cast(Msg, State) ->
    {noreply, State}.

handle_info(#'basic.consume_ok'{consumer_tag = ConsumerTag}, State) ->
    {noreply, State};

handle_info(#'basic.cancel_ok'{consumer_tag = ConsumerTag}, State) ->
    {noreply, State};

handle_info({content, ClassId, Properties, PropertiesBin, Payload},
            State = #rpc_client_state{continuations = Continuations,
                                      type_mapping = TypeMapping}) ->
    #'P_basic'{correlation_id = CorrelationId,
               content_type = ContentType} = rabbit_framing:decode_properties(ClassId, PropertiesBin),
    _CorrelationId = binary_to_list(CorrelationId),
    From = dict:fetch(_CorrelationId, Continuations),
    Reply = amqp_rpc_util:decode(ContentType, Payload, TypeMapping),
    gen_server:reply(From, Reply),
    NewContinuations = dict:erase(_CorrelationId, Continuations),
    {noreply, State#rpc_client_state{continuations = NewContinuations}}.

code_change(_OldVsn, State, _Extra) ->
    State.
