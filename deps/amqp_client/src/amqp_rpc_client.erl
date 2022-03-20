%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @doc This module allows the simple execution of an asynchronous RPC over
%% AMQP. It frees a client programmer of the necessary having to AMQP
%% plumbing. Note that the this module does not handle any data encoding,
%% so it is up to the caller to marshall and unmarshall message payloads
%% accordingly.
-module(amqp_rpc_client).

-include("amqp_client.hrl").

-behaviour(gen_server).

-export([start/2, start_link/2, stop/1]).
-export([call/2]).
-export([init/1, terminate/2, code_change/3, handle_call/3,
         handle_cast/2, handle_info/2]).

-record(state, {channel,
                reply_queue,
                exchange,
                routing_key,
                continuations = #{},
                correlation_id = 0}).

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

%% @spec (Connection, Queue) -> RpcClient
%% where
%%      Connection = pid()
%%      Queue = binary()
%%      RpcClient = pid()
%% @doc Starts, and links to, a new RPC client instance that sends requests
%% to a specified queue. This function returns the pid of the RPC client
%% process that can be used to invoke RPCs and stop the client.
start_link(Connection, Queue) ->
    {ok, Pid} = gen_server:start_link(?MODULE, [Connection, Queue], []),
    Pid.

%% @spec (RpcClient) -> ok
%% where
%%      RpcClient = pid()
%% @doc Stops an existing RPC client.
stop(Pid) ->
    gen_server:call(Pid, stop, amqp_util:call_timeout()).

%% @spec (RpcClient, Payload) -> ok
%% where
%%      RpcClient = pid()
%%      Payload = binary()
%% @doc Invokes an RPC. Note the caller of this function is responsible for
%% encoding the request and decoding the response.
call(RpcClient, Payload) ->
    gen_server:call(RpcClient, {call, Payload}, amqp_util:call_timeout()).

%%--------------------------------------------------------------------------
%% Plumbing
%%--------------------------------------------------------------------------

%% Sets up a reply queue for this client to listen on
setup_reply_queue(State = #state{channel = Channel}) ->
    #'queue.declare_ok'{queue = Q} =
        amqp_channel:call(Channel, #'queue.declare'{exclusive   = true,
                                                    auto_delete = true}),
    State#state{reply_queue = Q}.

%% Registers this RPC client instance as a consumer to handle rpc responses
setup_consumer(#state{channel = Channel, reply_queue = Q}) ->
    amqp_channel:call(Channel, #'basic.consume'{queue = Q}).

%% Publishes to the broker, stores the From address against
%% the correlation id and increments the correlationid for
%% the next request
publish(Payload, From,
        State = #state{channel = Channel,
                       reply_queue = Q,
                       exchange = X,
                       routing_key = RoutingKey,
                       correlation_id = CorrelationId,
                       continuations = Continuations}) ->
    EncodedCorrelationId = base64:encode(<<CorrelationId:64>>),
    Props = #'P_basic'{correlation_id = EncodedCorrelationId,
                       content_type = <<"application/octet-stream">>,
                       reply_to = Q},
    Publish = #'basic.publish'{exchange = X,
                               routing_key = RoutingKey,
                               mandatory = true},
    amqp_channel:call(Channel, Publish, #amqp_msg{props = Props,
                                                  payload = Payload}),
    State#state{correlation_id = CorrelationId + 1,
                continuations = maps:put(EncodedCorrelationId, From, Continuations)}.

%%--------------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------------

%% Sets up a reply queue and consumer within an existing channel
%% @private
init([Connection, RoutingKey]) ->
    {ok, Channel} = amqp_connection:open_channel(
                        Connection, {amqp_direct_consumer, [self()]}),
    InitialState = #state{channel     = Channel,
                          exchange    = <<>>,
                          routing_key = RoutingKey},
    State = setup_reply_queue(InitialState),
    setup_consumer(State),
    {ok, State}.

%% Closes the channel this gen_server instance started
%% @private
terminate(_Reason, #state{channel = Channel}) ->
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
handle_info({#'basic.consume'{}, _Pid}, State) ->
    {noreply, State};

%% @private
handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State};

%% @private
handle_info(#'basic.cancel'{}, State) ->
    {noreply, State};

%% @private
handle_info(#'basic.cancel_ok'{}, State) ->
    {stop, normal, State};

%% @private
handle_info({#'basic.deliver'{delivery_tag = DeliveryTag},
             #amqp_msg{props = #'P_basic'{correlation_id = Id},
                       payload = Payload}},
            State = #state{continuations = Conts, channel = Channel}) ->
    From = maps:get(Id, Conts),
    gen_server:reply(From, Payload),
    amqp_channel:call(Channel, #'basic.ack'{delivery_tag = DeliveryTag}),
    {noreply, State#state{continuations = maps:remove(Id, Conts) }}.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
