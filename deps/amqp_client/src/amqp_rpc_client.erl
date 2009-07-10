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

-include_lib("rabbit_framing.hrl").
-include_lib("rabbit.hrl").
-include("amqp_client.hrl").

-behaviour(gen_server).

-export([start/2, stop/1]).
-export([call/2]).
-export([init/1, terminate/2, code_change/3, handle_call/3,
         handle_cast/2, handle_info/2]).

%---------------------------------------------------------------------------
% API
%---------------------------------------------------------------------------

start(Connection, Queue) ->
    {ok, Pid} = gen_server:start(?MODULE, [Connection, Queue], []),
    Pid.

stop(Pid) ->
    gen_server:call(Pid, stop, infinity).

call(RpcClientPid, Payload) ->
    gen_server:call(RpcClientPid, {call, Payload}, infinity).

%---------------------------------------------------------------------------
% Plumbing
%---------------------------------------------------------------------------

% Sets up a reply queue for this client to listen on
setup_reply_queue(State = #rpc_client_state{channel = Channel}) ->
    Q = lib_amqp:declare_private_queue(Channel),
    State#rpc_client_state{reply_queue = Q}.

% Registers this RPC client instance as a consumer to handle rpc responses
setup_consumer(#rpc_client_state{channel = Channel,
                                 reply_queue = Q}) ->
    lib_amqp:subscribe(Channel, Q, self()).

% Publishes to the broker, stores the From address against
% the correlation id and increments the correlationid for
% the next request
publish(Payload, From,
        State = #rpc_client_state{channel = Channel,
                                  reply_queue = Q,
                                  exchange = X,
                                  routing_key = RoutingKey,
                                  correlation_id = CorrelationId,
                                  continuations = Continuations}) ->
    Props = #'P_basic'{correlation_id = <<CorrelationId:64>>,
                       content_type = <<"application/octet-stream">>,
                       reply_to = Q},
    lib_amqp:publish(Channel, X, RoutingKey, Payload, Props),
    State#rpc_client_state{correlation_id = CorrelationId + 1,
                           continuations
                           = dict:store(CorrelationId, From, Continuations)}.

%---------------------------------------------------------------------------
% gen_server callbacks
%---------------------------------------------------------------------------

% Sets up a reply queue and consumer within an existing channel
init([Connection, RoutingKey]) ->
    Channel = lib_amqp:start_channel(Connection),
    InitialState = #rpc_client_state{channel = Channel,
                                     exchange = <<>>,
                                     routing_key = RoutingKey},
    State = setup_reply_queue(InitialState),
    setup_consumer(State),
    {ok, State}.

% Closes the channel this gen_server instance started
terminate(_Reason, #rpc_client_state{channel = Channel}) ->
    lib_amqp:close_channel(Channel),
    ok.

% Handle the application initiated stop by just stopping this gen server
handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call({call, Payload}, From, State) ->
    NewState = publish(Payload, From, State),
    {noreply, NewState}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State};

handle_info(#'basic.cancel_ok'{}, State) ->
    {stop, normal, State};

handle_info({#'basic.deliver'{},
            {content, ClassId, _Props, PropertiesBin, [Payload] }},
            State = #rpc_client_state{continuations = Conts}) ->
    #'P_basic'{correlation_id = CorrelationId}
               = rabbit_framing:decode_properties(ClassId, PropertiesBin),
    <<Id:64>> = CorrelationId,
    From = dict:fetch(Id, Conts),
    gen_server:reply(From, Payload),
    {noreply, State#rpc_client_state{continuations = dict:erase(Id, Conts) }}.

code_change(_OldVsn, State, _Extra) ->
    State.

