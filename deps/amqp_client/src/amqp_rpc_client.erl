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

-export([start/2, start/3]).
-export([call/2]).
-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2, handle_info/2]).

%---------------------------------------------------------------------------
% API
%---------------------------------------------------------------------------

start(Channel, Exchange) ->
    start(Channel, Exchange, <<>>).

start(Channel, Exchange, RoutingKey) ->
    {ok, RpcClientPid} = gen_server:start(?MODULE, [Channel, Exchange, RoutingKey], []),
    RpcClientPid.

call(RpcClientPid, Payload) ->
    gen_server:call(RpcClientPid, Payload).

%---------------------------------------------------------------------------
% Plumbing
%---------------------------------------------------------------------------

% Sets up a reply queue for this client to listen on
setup_reply_queue(State = #rpc_client_state{channel = Channel}) ->
    Q = lib_amqp:declare_queue(Channel, <<"">>),
    State#rpc_client_state{reply_queue = Q}.

% Registers this RPC client instance as a consumer to handle rpc responses
setup_consumer(State = #rpc_client_state{channel = Channel, reply_queue = Q}) ->
    ConsumerTag = lib_amqp:subscribe(Channel, Q, self()),
    State#rpc_client_state{consumer_tag = ConsumerTag}.

% Publishes to the broker, stores the From address against
% the correlation id and increments the correlationid for
% the next request
publish(Payload, From, State = #rpc_client_state{channel = Channel,
                                                 reply_queue = Q,
                                                 exchange = X,
                                                 routing_key = RoutingKey,
                                                 correlation_id = CorrelationId,
                                                 continuations = Continuations}) ->
    Props = #'P_basic'{correlation_id = <<CorrelationId:64>>, reply_to = Q},
    lib_amqp:publish(Channel, X, RoutingKey, Payload, Props),
    State#rpc_client_state{correlation_id = CorrelationId + 1,
                           continuations = dict:store(CorrelationId, From, Continuations)}.

%---------------------------------------------------------------------------
% gen_server callbacks
%---------------------------------------------------------------------------

% Sets up a reply queue and consumer within an existing channel
init([Channel, Exchange, RoutingKey]) ->
    InitialState = #rpc_client_state{channel = Channel,
                                     exchange = Exchange,
                                     routing_key = RoutingKey},
    State = setup_reply_queue(InitialState),
    NewState = setup_consumer(State),
    {ok, NewState}.

terminate(Reason, State) ->
    ok.

handle_call(stop, From, State = #rpc_client_state{channel = Channel,
                                                  consumer_tag = Tag}) ->
    Reply = lib_amqp:unsubscribe(Channel, Tag),
    {noreply, Reply, State};

handle_call(Payload, From, State) ->
    NewState = publish(Payload, From, State),
    {noreply, NewState}.

handle_cast(Msg, State) ->
    {noreply, State}.

handle_info(#'basic.consume_ok'{consumer_tag = ConsumerTag}, State) ->
    NewState = State#rpc_client_state{consumer_tag = ConsumerTag},
    {noreply, NewState};

handle_info(#'basic.cancel_ok'{consumer_tag = ConsumerTag}, State) ->
    {stop, normal, State};

handle_info({content, ClassId, Properties, PropertiesBin, Payload},
            State = #rpc_client_state{continuations = Continuations}) ->
    #'P_basic'{correlation_id = CorrelationId,
               content_type = ContentType} = rabbit_framing:decode_properties(ClassId, PropertiesBin),
    <<_CorrelationId:64>> = CorrelationId,
    From = dict:fetch(_CorrelationId, Continuations),
    gen_server:reply(From, Payload),
    {noreply, State#rpc_client_state{continuations = dict:erase(_CorrelationId, Continuations) }}.

code_change(_OldVsn, State, _Extra) ->
    State.
