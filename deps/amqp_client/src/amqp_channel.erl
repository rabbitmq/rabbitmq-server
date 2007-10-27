-module(amqp_channel).

-include_lib("rabbitmq_server/include/rabbit.hrl").
-include_lib("rabbitmq_server/include/rabbit_framing.hrl").
-include_lib("mnesia/include/mnemosyne.hrl").
-include("amqp_client.hrl").

-behaviour(gen_server).

-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2, handle_info/2]).
-export([call/2, call/3, cast/2, cast/3]).
-export([register_direct_peer/2]).

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
register_direct_peer(Channel, Peer) ->
    gen_server:cast(Channel, {register_direct_peer, Peer} ).

%---------------------------------------------------------------------------
% Internal plumbing
%---------------------------------------------------------------------------

rpc_top_half(Method, From, State = #channel_state{writer_pid = Writer, pending_rpc = PendingRpc}) ->
    if
        is_pid(PendingRpc) ->
            exit(illegal_pending_rpc);
        true ->
            ok
    end,
    NewState = State#channel_state{pending_rpc = From},
    Writer ! { self(), Method },
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

acknowledge_reader(ReaderPid) ->
    ReaderPid ! ack.

%% Saves a sucessful consumer regsitration into the channel state
%% using the pending_consumer field of the channel_state record.
%% This then executes the bottom half of the RPC and finally
%% nulls out the pending_consumer pid field that has been saved
handle_basic_consume_ok(BasicConsumeOk = #'basic.consume_ok'{consumer_tag = ConsumerTag},
                        State = #channel_state{pending_consumer = Consumer}) ->
    Consumer ! BasicConsumeOk,
    NewState = register_consumer(ConsumerTag, Consumer, State),
    {noreply, NewState2} = rpc_bottom_half(BasicConsumeOk, NewState),
    {noreply, NewState2#channel_state{pending_consumer = <<>>} }.

handle_basic_deliver(ConsumerTag, Content, State) ->
    Consumer = resolve_consumer(ConsumerTag, State),
    Consumer ! Content,
    {noreply, State}.

handle_basic_cancel_ok(BasicCancelOk = #'basic.cancel_ok'{consumer_tag = ConsumerTag}, State) ->
    Consumer = resolve_consumer(ConsumerTag, State),
    Consumer ! BasicCancelOk,
    NewState = unregister_consumer(ConsumerTag, State),
    rpc_bottom_half(BasicCancelOk, NewState).

handle_channel_close_ok(ChannelCloseOk = #'channel.close_ok'{}, State) ->
    {noreply, NewState} = rpc_bottom_half(ChannelCloseOk, State),
    {stop, shutdown, NewState}.

%---------------------------------------------------------------------------
% gen_server callbacks
%---------------------------------------------------------------------------

init([InitialState]) ->
    {ok, InitialState}.

%% This allows the direct channel to pass a transaction into this channel
%% This should only happen during initialization, so therefore there shouldn't
%% be a pending RPC and hence doesn't use the rpc_bottom_half mechansim.
handle_call({set_transaction, Tx}, From, State) ->
    gen_server:reply(From, ok),
    {noreply, State#channel_state{tx = Tx}};

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
handle_cast({cast, Method}, State = #channel_state{writer_pid = Writer}) ->
    Writer ! { self(), Method },
    {noreply, State};

%% Standard implementation of the cast/3 command
handle_cast({cast, Method, Content}, State = #channel_state{writer_pid = Writer}) ->
    Writer ! { self(), Method, Content },
    {noreply, State};

%% Registers the direct channel peer when using the direct client
handle_cast({register_direct_peer, Peer}, State) ->
    NewState = State#channel_state{writer_pid = Peer},
    {noreply, NewState};

handle_cast({notify_sent, Peer}, State) ->
    %% TODO What should we do here?
    {noreply, State}.

%---------------------------------------------------------------------------
% Network Writer methods (gen_server callbacks).
% These callbacks are invoked when a network channel sends messages
% to this gen_server instance.
%---------------------------------------------------------------------------

%% Saves a sucessful consumer regsitration from the network channel into the channel state
handle_info({frame, Channel, {method, 'basic.consume_ok', BinaryContent}, ReaderPid }, State) ->
    acknowledge_reader(ReaderPid),
    BasicConsumeOk = amqp_util:decode_method('basic.consume_ok', BinaryContent),
    handle_basic_consume_ok(BasicConsumeOk, State);

%% Handles the delivery of a message from the network channel
handle_info({frame, Channel, {method, 'basic.deliver', BinaryContent}, ReaderPid }, State) ->
    acknowledge_reader(ReaderPid),
    {BasicDeliver, Content} = amqp_util:decode_method('basic.deliver', BinaryContent),
    #'basic.deliver'{consumer_tag = ConsumerTag} = BasicDeliver,
    handle_basic_deliver(ConsumerTag, Content, State);

%% Upon the cancellation of a consumer from the network channel,
%% this function deregisters the consumer in the channel state
handle_info({frame, Channel, {method, 'basic.cancel_ok', BinaryContent}, ReaderPid }, State) ->
    acknowledge_reader(ReaderPid),
    BasicCancelOk = amqp_util:decode_method('basic.cancel_ok', BinaryContent),
    handle_basic_cancel_ok(BasicCancelOk, State);

%% This deals with channel close request from a network channel
handle_info({frame, Channel, {method, 'channel.close_ok', BinaryContent}, ReaderPid }, State) ->
    acknowledge_reader(ReaderPid),
    ChannelCloseOk = amqp_util:decode_method('channel.close_ok', BinaryContent),
    handle_channel_close_ok(ChannelCloseOk, State);

%% Standard rpc bottom half handling in the network case
handle_info({frame, Channel, {method, Method, Content}, ReaderPid }, State) ->
    acknowledge_reader(ReaderPid),
    Reply = amqp_util:decode_method(Method, Content),
    rpc_bottom_half(Reply, State);

%---------------------------------------------------------------------------
% Rabbit Writer API methods (gen_server callbacks).
% These callbacks are invoked when a direct channel sends messages
% to this gen_server instance.
%------------------------------------------ ---------------------------------

%% Saves a sucessful consumer regsitration from the direct channel into the channel state
handle_info( {send_command, BasicConsumeOk = #'basic.consume_ok'{} }, State) ->
    handle_basic_consume_ok(BasicConsumeOk, State);

%% Upon the cancellation of a consumer from a direct channel,
%% this function deregisters the consumer in the channel state
handle_info( {send_command, BasicCancelOk = #'basic.cancel_ok'{} }, State) ->
    handle_basic_cancel_ok(BasicCancelOk, State);

%% This deals with channel close request from a direct channel
handle_info( {send_command, ChannelCloseOk = #'channel.close_ok'{} }, State) ->
    handle_channel_close_ok(ChannelCloseOk, State);

%% Standard rpc bottom half handling in the direct case
handle_info( {send_command, Method}, State) ->
    rpc_bottom_half(Method, State);

%% Standard rpc bottom half handling in the direct case
handle_info( {send_command, Method, Content}, State) ->
    rpc_bottom_half( {Method, Content} , State);

%% Handles the rpc bottom half and shuts down the channel
handle_info( {send_command_and_shutdown, Method}, State) ->
    NewState = channel_cleanup(State),
    rpc_bottom_half(Method, NewState),
    {stop, shutdown, NewState};

%% Handles the delivery of a message from a direct channel
handle_info( {deliver, ConsumerTag, AckRequired, QName, QPid, Message},
             State = #channel_state{next_delivery_tag = DeliveryTag, tx = Tx}) ->
    #basic_message{content = Content,
                   persistent_key = PersistentKey} = Message,
    amqp_direct_driver:acquire_lock(AckRequired, {Tx, DeliveryTag, ConsumerTag, QName, QPid, Message}),
    {noreply, _NewState} = handle_basic_deliver(ConsumerTag,Content, State),
    amqp_direct_driver:release_lock(AckRequired, {QName, QPid, PersistentKey}),
    NewState = _NewState#channel_state{next_delivery_tag = DeliveryTag + 1},
    {noreply, NewState};

handle_info( {get_ok, MessageCount, AckRequired, QName, QPid, Message},
             State = #channel_state{next_delivery_tag = DeliveryTag, tx = Tx}) ->
    #basic_message{content = Content,
                   exchange_name = X,
                   routing_key = RoutingKey,
                   persistent_key = PersistentKey,
                   redelivered = Redelivered}  = Message,
    Method = #'basic.get_ok'{delivery_tag = DeliveryTag,
                             redelivered = Redelivered,
                             exchange = X,
                             routing_key = RoutingKey,
                             message_count = MessageCount},
    {noreply, _NewState} = rpc_bottom_half( {Method, Content}, State),
    NewState = State#channel_state{next_delivery_tag = DeliveryTag + 1},
    {noreply, NewState};

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
