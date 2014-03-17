%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
%%

%% @doc This is a utility module that is used to expose an arbitrary function
%% via an asynchronous RPC over AMQP mechanism. It frees the implementor of
%% a simple function from having to plumb this into AMQP. Note that the
%% RPC server does not handle any data encoding, so it is up to the callback
%% function to marshall and unmarshall message payloads accordingly.
-module(amqp_rpc_server).

-behaviour(gen_server).

-include("amqp_client.hrl").

-export([init/1, terminate/2, code_change/3, handle_call/3,
         handle_cast/2, handle_info/2]).
-export([start/3, start_link/3]).
-export([stop/1]).

-record(state, {channel,
                handler}).

%%--------------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------------

%% @spec (Connection, Queue, RpcHandler) -> RpcServer
%% where
%%      Connection = pid()
%%      Queue = binary()
%%      RpcHandler = function()
%%      RpcServer = pid()
%% @doc Starts a new RPC server instance that receives requests via a
%% specified queue and dispatches them to a specified handler function. This
%% function returns the pid of the RPC server that can be used to stop the
%% server.
start(Connection, Queue, Fun) ->
    {ok, Pid} = gen_server:start(?MODULE, [Connection, Queue, Fun], []),
    Pid.

%% @spec (Connection, Queue, RpcHandler) -> RpcServer
%% where
%%      Connection = pid()
%%      Queue = binary()
%%      RpcHandler = function()
%%      RpcServer = pid()
%% @doc Starts, and links to, a new RPC server instance that receives
%% requests via a specified queue and dispatches them to a specified
%% handler function. This function returns the pid of the RPC server that
%% can be used to stop the server.
start_link(Connection, Queue, Fun) ->
    {ok, Pid} = gen_server:start_link(?MODULE, [Connection, Queue, Fun], []),
    Pid.

%% @spec (RpcServer) -> ok
%% where
%%      RpcServer = pid()
%% @doc Stops an exisiting RPC server.
stop(Pid) ->
    gen_server:call(Pid, stop, infinity).

%%--------------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------------

%% @private
init([Connection, Q, Fun]) ->
    {ok, Channel} = amqp_connection:open_channel(
                        Connection, {amqp_direct_consumer, [self()]}),
    amqp_channel:call(Channel, #'queue.declare'{queue = Q}),
    amqp_channel:call(Channel, #'basic.consume'{queue = Q}),
    {ok, #state{channel = Channel, handler = Fun} }.

%% @private
handle_info(shutdown, State) ->
    {stop, normal, State};

%% @private
handle_info({#'basic.consume'{}, _}, State) ->
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
             #amqp_msg{props = Props, payload = Payload}},
            State = #state{handler = Fun, channel = Channel}) ->
    #'P_basic'{correlation_id = CorrelationId,
               reply_to = Q} = Props,
    Response = Fun(Payload),
    Properties = #'P_basic'{correlation_id = CorrelationId},
    Publish = #'basic.publish'{exchange = <<>>,
                               routing_key = Q,
                               mandatory = true},
    amqp_channel:call(Channel, Publish, #amqp_msg{props = Properties,
                                                  payload = Response}),
    amqp_channel:call(Channel, #'basic.ack'{delivery_tag = DeliveryTag}),
    {noreply, State};

%% @private
handle_info({'DOWN', _MRef, process, _Pid, _Info}, State) ->
    {noreply, State}.

%% @private
handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

%%--------------------------------------------------------------------------
%% Rest of the gen_server callbacks
%%--------------------------------------------------------------------------

%% @private
handle_cast(_Message, State) ->
    {noreply, State}.

%% Closes the channel this gen_server instance started
%% @private
terminate(_Reason, #state{channel = Channel}) ->
    amqp_channel:close(Channel),
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
