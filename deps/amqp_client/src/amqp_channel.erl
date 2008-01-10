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

-module(amqp_channel).

-include_lib("rabbitmq_server/include/rabbit.hrl").
-include_lib("rabbitmq_server/include/rabbit_framing.hrl").
-include_lib("stdlib/include/qlc.hrl").
-include("amqp_client.hrl").

-behaviour(gen_server).

-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2, handle_info/2]).
-export([call/2, call/3, cast/2, cast/3]).
-export([register_direct_peer/2]).
-export([register_return_handler/2]).

%% This diagram shows the interaction between the different component processes
%% in an AMQP client scenario.
%%
%%                             message* / reply*        +-------+
%%                            +----------------------   | queue |
%%                            |                         +-------+
%%                            |
%%                            |                          +-----+
%%                            v                          |     |
%%           request                     reply*          |     v
%% +------+  -------+  +--------------+  <------+  +----------------+
%% | User |         |  | amqp_channel |         |  | direct_channel |
%% +------+  <------+  +--------------+  -------+  +----------------+
%%           response /        |          request
%% cast/call         /         |
%%                  /          | message
%%                 /           v
%% +-------------+/       +----------+
%% | Pending RPC |        | Consumer |
%% +-------------+        +----------+
%%       |
%% [consumer tag --> consumer pid]
%%
%% * These notifications are processed asynchronously via handle_info/2 callbacks

%---------------------------------------------------------------------------
% AMQP Channel API methods
%---------------------------------------------------------------------------

%% Generic AMQP RPC mechanism that expects a pseudo synchronous response
call(Channel, Method) ->
    gen_server:call(Channel, {call, Method}).

%% Allows a consumer to be registered with the channel when invoking a BasicConsume
call(Channel, Method = #'basic.consume'{}, Consumer) ->
    gen_server:call(Channel, {basic_consume, Method, Consumer}).

%% Generic AMQP send mechansim that doesn't expect a response
cast(Channel, Method) ->
    gen_server:cast(Channel, {cast, Method}).

%% Generic AMQP send mechansim that doesn't expect a response
cast(Channel, Method, Content) ->
    gen_server:cast(Channel, {cast, Method, Content}).

%---------------------------------------------------------------------------
% Direct peer registration
%---------------------------------------------------------------------------

%% Regsiters the direct channel peer with the state of this channel.
%% This registration occurs after the amqp_channel gen_server instance
%% because the pid of this amqp_channel needs to be passed into the
%% initialization of that direct channel process, hence the resulting
%% direct channel pid can only be post-registered.

%% Have another look at this: This is also being used to register a writer
%% pid in the network case as well.........code reuse ;-)
register_direct_peer(Channel, Peer) ->
    gen_server:cast(Channel, {register_direct_peer, Peer} ).

%% Registers a handler to deal with returned messages
register_return_handler(Channel, ReturnHandler) ->
    gen_server:cast(Channel, {register_return_handler, ReturnHandler} ).

%---------------------------------------------------------------------------
% Internal plumbing
%---------------------------------------------------------------------------

rpc_top_half(Method, From, State = #channel_state{writer_pid = Writer,
                                                  pending_rpc = PendingRpc,
                                                  do2 = Do2}) ->
    if
        is_pid(PendingRpc) ->
            exit(illegal_pending_rpc);
        true ->
            ok
    end,
    NewState = State#channel_state{pending_rpc = From},
    Do2(Writer,Method),
    {noreply, NewState}.

rpc_bottom_half(Reply, State = #channel_state{pending_rpc = From}) ->
    gen_server:reply(From, Reply),
    NewState = State#channel_state{pending_rpc = <<>>},
    {noreply, NewState}.

resolve_consumer(ConsumerTag, #channel_state{consumers = []}) ->
    exit(no_consumers_registered);

resolve_consumer(ConsumerTag, #channel_state{consumers = Consumers}) ->
    dict:fetch(ConsumerTag, Consumers).

register_consumer(ConsumerTag, Consumer, State = #channel_state{consumers = Consumers0}) ->
    Consumers1 = dict:store(ConsumerTag, Consumer, Consumers0),
    State#channel_state{consumers = Consumers1}.

unregister_consumer(ConsumerTag, State = #channel_state{consumers = Consumers0}) ->
    Consumers1 = dict:erase(ConsumerTag, Consumers0),
    State#channel_state{consumers = Consumers1}.

channel_cleanup(State = #channel_state{consumers = []}) ->
    State;

channel_cleanup(State = #channel_state{consumers = Consumers}) ->
    Terminator = fun(ConsumerTag, Consumer) -> Consumer ! shutdown end,
    dict:map(Terminator, Consumers),
    State#channel_state{closing = true, consumers = []}.

return_handler(State = #channel_state{return_handler_pid = undefined}) ->
    %% TODO what about trapping exits??
    {ok, ReturnHandler} = gen_event:start_link(),
    gen_event:add_handler(ReturnHandler, amqp_return_handler , [] ),
    {ReturnHandler, State#channel_state{return_handler_pid = ReturnHandler}};

return_handler(State = #channel_state{return_handler_pid = ReturnHandler}) ->
    {ReturnHandler, State}.

%% Saves a sucessful consumer regsitration into the channel state
%% using the pending_consumer field of the channel_state record.
%% This then executes the bottom half of the RPC and finally
%% nulls out the pending_consumer pid field that has been saved
handle_method(BasicConsumeOk = #'basic.consume_ok'{consumer_tag = ConsumerTag},
                        State = #channel_state{pending_consumer = Consumer}) ->
    Consumer ! BasicConsumeOk,
    NewState = register_consumer(ConsumerTag, Consumer, State),
    {noreply, NewState2} = rpc_bottom_half(BasicConsumeOk, NewState),
    {noreply, NewState2#channel_state{pending_consumer = <<>>} };

handle_method(BasicCancelOk = #'basic.cancel_ok'{consumer_tag = ConsumerTag}, State) ->
    Consumer = resolve_consumer(ConsumerTag, State),
    Consumer ! BasicCancelOk,
    NewState = unregister_consumer(ConsumerTag, State),
    rpc_bottom_half(BasicCancelOk, NewState);

handle_method(ChannelCloseOk = #'channel.close_ok'{}, State) ->
    {noreply, NewState} = rpc_bottom_half(ChannelCloseOk, State),
    {stop, shutdown, NewState};

handle_method(Method, State) ->
    rpc_bottom_half(Method, State).

handle_method(BasicDeliver = #'basic.deliver'{consumer_tag = ConsumerTag}, Content, State) ->
    Consumer = resolve_consumer(ConsumerTag, State),
    io:format("Sending 999 : ~p~n",[BasicDeliver]),
    Consumer ! {BasicDeliver, Content},
    {noreply, State};

%% Why is the consumer a handle_method/3 call with the network driver,
%% but this is a handle_method/2 call with the direct driver?
handle_method('basic.consume_ok', ConsumerTag, State) ->
    handle_method(#'basic.consume_ok'{consumer_tag = ConsumerTag}, State);

handle_method(BasicReturn = #'basic.return'{}, Content, State) ->
    %% TODO This is unimplemented because I don't know how to
    %% resolve the originator of the message
    {ReturnHandler, NewState} = return_handler(State),
    ReturnHandler ! {BasicReturn, Content},
    {noreply, NewState};

handle_method(Method, Content, State) ->
    rpc_bottom_half( {Method, Content} , State).

%---------------------------------------------------------------------------
% gen_server callbacks
%---------------------------------------------------------------------------

init([InitialState]) ->
    {ok, InitialState}.

%% Standard implementation of top half of the call/2 command
%% Do not accept any further RPCs when the channel is about to close
handle_call({call, Method}, From, State = #channel_state{closing = false}) ->
    rpc_top_half(Method, From, State);

%% Top half of the basic consume process.
%% Sets up the consumer for registration in the bottom half of this RPC.
handle_call({basic_consume, Method, Consumer}, From, State) ->
    NewState = State#channel_state{pending_consumer = Consumer},
    rpc_top_half(Method, From, NewState).

%% Standard implementation of the cast/2 command
handle_cast({cast, Method}, State = #channel_state{writer_pid = Writer, do2 = Do2}) ->
    Do2(Writer, Method),
    {noreply, State};

%% Standard implementation of the cast/3 command
handle_cast({cast, Method, Content}, State = #channel_state{writer_pid = Writer, do3 = Do3}) ->
    Do3(Writer, Method, Content),
    {noreply, State};

%% Registers the direct channel peer when using the direct client
handle_cast({register_direct_peer, Peer}, State) ->
    NewState = State#channel_state{writer_pid = Peer},
    {noreply, NewState};

%% Registers a handler to process return messages
handle_cast({register_return_handler, ReturnHandler}, State) ->
    NewState = State#channel_state{return_handler_pid = ReturnHandler},
    {noreply, NewState};

handle_cast({notify_sent, Peer}, State) ->
    {noreply, State}.

%---------------------------------------------------------------------------
% Network Writer methods (gen_server callbacks).
% These callbacks are invoked when a network channel sends messages
% to this gen_server instance.
%---------------------------------------------------------------------------

%% Handles the delivery of a message from the network channel
handle_info({frame, Channel, {method, Method, BinaryContent}, ReaderPid }, State) ->
    case amqp_util:decode_method(Method, BinaryContent) of
        {DecodedMethod, DecodedContent} ->
            handle_method(DecodedMethod, DecodedContent, State);
        DecodedMethod ->
            handle_method(DecodedMethod, State)
    end;

%---------------------------------------------------------------------------
% Rabbit Writer API methods (gen_server callbacks).
% These callbacks are invoked when a direct channel sends messages
% to this gen_server instance.
%------------------------------------------ ---------------------------------

%% Standard method handling in the direct case
handle_info( {send_command, Method}, State) -> handle_method(Method, State);
handle_info( {send_command, Method, Content}, State) -> handle_method(Method, Content, State);

%% Handles the rpc bottom half and shuts down the channel
handle_info( {send_command_and_shutdown, Method}, State) ->
    NewState = channel_cleanup(State),
    rpc_bottom_half(Method, NewState),
    {stop, shutdown, NewState};

%% Handles the delivery of a message from a direct channel
handle_info( {send_command_and_notify, Q, ChPid, Method, Content}, State) ->
    handle_method(Method, Content, State),
    rabbit_amqqueue:notify_sent(Q, ChPid),
    {noreply, State};

handle_info(shutdown, State ) ->
    NewState = channel_cleanup(State),
    {stop, shutdown, NewState};

%---------------------------------------------------------------------------
% This is for a race condition between a close.close_ok and a subsequent channel.open
%---------------------------------------------------------------------------

handle_info( {channel_close, Peer}, State ) ->
    NewState = channel_cleanup(State),
    Peer ! handshake,
    {noreply, NewState};

%---------------------------------------------------------------------------
% This is for a channel exception that can't be otherwise handled
%---------------------------------------------------------------------------

handle_info( {channel_exception, Channel, Reason}, State) ->
    io:format("Channel ~p is shutting down due to: ~p~n",[Channel, Reason]),
    NewState = channel_cleanup(State),
    {stop, shutdown, NewState}.

%---------------------------------------------------------------------------
% Rest of the gen_server callbacks
%---------------------------------------------------------------------------

terminate(Reason, State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    State.
