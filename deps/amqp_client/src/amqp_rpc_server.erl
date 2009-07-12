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

-module(amqp_rpc_server).

-behaviour(gen_server).

-include_lib("rabbit.hrl").
-include_lib("rabbit_framing.hrl").
-include("amqp_client.hrl").

-export([init/1, terminate/2, code_change/3, handle_call/3,
         handle_cast/2, handle_info/2]).
-export([start/3]).
-export([stop/1]).


%---------------------------------------------------------------------------
% API
%---------------------------------------------------------------------------

start(Connection, Queue, Fun) ->
    {ok, Pid} = gen_server:start(?MODULE, [Connection, Queue, Fun], []),
    Pid.

stop(Pid) ->
    gen_server:call(Pid, stop, infinity).

%---------------------------------------------------------------------------
% gen_server callbacks
%---------------------------------------------------------------------------

init([Connection, Queue, Fun]) ->
    Channel = lib_amqp:start_channel(Connection),
    lib_amqp:declare_private_queue(Channel, Queue),
    lib_amqp:subscribe(Channel, Queue, self()),
    {ok, #rpc_server_state{channel = Channel, handler = Fun} }.

handle_info(shutdown, State) ->
    {stop, normal, State};

handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State};

handle_info(#'basic.cancel_ok'{}, State) ->
    {stop, normal, State};

handle_info({#'basic.deliver'{},
            {content, ClassId, _Props, PropertiesBin, [Payload] }},
            State = #rpc_server_state{handler = Fun, channel = Channel}) ->
    #'P_basic'{correlation_id = CorrelationId,
               reply_to = Q} =
               rabbit_framing:decode_properties(ClassId, PropertiesBin),
    Response = Fun(Payload),
    Properties = #'P_basic'{correlation_id = CorrelationId},
    lib_amqp:publish(Channel, <<>>, Q, Response, Properties),
    {noreply, State}.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

%---------------------------------------------------------------------------
% Rest of the gen_server callbacks
%---------------------------------------------------------------------------

handle_cast(_Message, State) ->
    {noreply, State}.

% Closes the channel this gen_server instance started
terminate(_Reason, #rpc_server_state{channel = Channel}) ->
    lib_amqp:close_channel(Channel),
    ok.

code_change(_OldVsn, State, _Extra) ->
    State.

