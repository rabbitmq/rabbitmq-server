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

%% @doc This module allows the simple execution of an asynchronous RPC over 
%% AMQP. It frees a client programmer of the necessary having to AMQP
%% plumbing. Note that the this module does not handle any data encoding,
%% so it is up to the caller to marshall and unmarshall message payloads 
%% accordingly.
-module(amqp_rpc_client).

-include("amqp_client.hrl").

-behaviour(gen_server).

-export([start/2, stop/1]).
-export([call/2]).
-export([init/1, terminate/2, code_change/3, handle_call/3,
         handle_cast/2, handle_info/2]).

%%--------------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------------

%% @spec (Connection, Queue) -> RpcClient
%% where
%%      Connection = pid()
%%      Queue = binary()
%%      RpcClient = pid()
%% @doc Starts a new RPC client instance that sends requests to a
%% specified queue. This function returns the pid of the RPC client process
%% that can be used to invoke RPCs and stop the client.
start(Connection, Queue) ->
    {ok, Pid} = gen_server:start(?MODULE, [Connection, Queue], []),
    Pid.

%% @spec (RpcClient) -> ok
%% where
%%      RpcClient = pid()
%% @doc Stops an exisiting RPC client.
stop(Pid) ->
    gen_server:call(Pid, stop, infinity).

%% @spec (RpcClient, Payload) -> ok
%% where
%%      RpcClient = pid()
%%      Payload = binary()
%% @doc Invokes an RPC. Note the caller of this function is responsible for
%% encoding the request and decoding the response.
call(RpcClient, Payload) ->
    gen_server:call(RpcClient, {call, Payload}, infinity).

%%--------------------------------------------------------------------------
%% Plumbing
%%--------------------------------------------------------------------------

%% Sets up a reply queue for this client to listen on
setup_reply_queue(State = #rpc_client_state{channel = Channel}) ->
    #'queue.declare_ok'{queue = Q} =
        amqp_channel:call(Channel, #'queue.declare'{}),
    State#rpc_client_state{reply_queue = Q}.

%% Registers this RPC client instance as a consumer to handle rpc responses
setup_consumer(#rpc_client_state{channel = Channel,
                                 reply_queue = Q}) ->
    amqp_channel:subscribe(Channel, #'basic.consume'{queue = Q}, self()).

%% Publishes to the broker, stores the From address against
%% the correlation id and increments the correlationid for
%% the next request
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
    Publish = #'basic.publish'{exchange = X,
                               routing_key = RoutingKey,
                               mandatory = true},
    amqp_channel:call(Channel, Publish, #amqp_msg{props = Props,
                                                  payload = Payload}),
    State#rpc_client_state{correlation_id = CorrelationId + 1,
                           continuations
                           = dict:store(CorrelationId, From, Continuations)}.

%%--------------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------------

%% Sets up a reply queue and consumer within an existing channel
%% @private
init([Connection, RoutingKey]) ->
    Channel = amqp_connection:open_channel(Connection),
    InitialState = #rpc_client_state{channel = Channel,
                                     exchange = <<>>,
                                     routing_key = RoutingKey},
    State = setup_reply_queue(InitialState),
    setup_consumer(State),
    {ok, State}.

%% Closes the channel this gen_server instance started
%% @private
terminate(_Reason, #rpc_client_state{channel = Channel}) ->
    amqp_channel:close(Channel),
    ok.

%% Handle the application initiated stop by just stopping this gen server
%% @private
handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

%% @private
handle_call({call, Payload}, From, State) ->
    NewState = publish(Payload, From, State),
    {noreply, NewState}.

%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State};

%% @private
handle_info(#'basic.cancel_ok'{}, State) ->
    {stop, normal, State};

%% @private
handle_info({#'basic.deliver'{},
             #amqp_msg{props = #'P_basic'{correlation_id = <<Id:64>>},
                       payload = Payload}},
            State = #rpc_client_state{continuations = Conts}) ->
    From = dict:fetch(Id, Conts),
    gen_server:reply(From, Payload),
    {noreply, State#rpc_client_state{continuations = dict:erase(Id, Conts) }}.

%% @private
code_change(_OldVsn, State, _Extra) ->
    State.

