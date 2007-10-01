-module(amqp_rpc_client).

-include_lib("rabbit/include/rabbit_framing.hrl").
-include("amqp_client.hrl").

-behaviour(gen_server).

-export([start/1]).
-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2, handle_info/2]).

%---------------------------------------------------------------------------
% API
%---------------------------------------------------------------------------

start(BrokerConfig) ->
    gen_server:start(?MODULE, [BrokerConfig], []).

%---------------------------------------------------------------------------
% Plumbing
%---------------------------------------------------------------------------

% Sets up a reply queue for this client to listen on
setup_reply_queue(State = #rpc_client{channel_pid = ChannelPid, ticket = Ticket}) ->
    QueueDeclare = #'queue.declare'{ticket = Ticket, queue = [],
                                    passive = false, durable = false,
                                    exclusive = false, auto_delete = false,
                                    nowait = false, arguments = []},
    #'queue.declare_ok'{queue = Q,
                        message_count = MessageCount,
                        consumer_count = ConsumerCount}
                        = amqp_channel:call(ChannelPid, QueueDeclare),
    State#rpc_client{queue = Q}.

% Sets up a consumer to handle rpc responses
setup_consumer(State) ->
    ConsumerTag = amqp_method_util:register_consumer(State, self()),
    State#rpc_client{consumer_tag = ConsumerTag}.

%---------------------------------------------------------------------------
% gen_server callbacks
%---------------------------------------------------------------------------

% Starts a new connection to the broker and opens up a new channel
init([BrokerConfig = #rpc_client{channel_pid = ChannelPid, ticket = Ticket}]) ->
    State = setup_reply_queue(BrokerConfig),
    NewState = setup_consumer(State),
    {ok, NewState}.

% Closes the channel and the broker connection
terminate(Reason, State) ->
    amqp_aux:close_channel(State),
    amqp_aux:close_connection(State).

handle_call(manage, From, State = #rpc_client{channel_pid = ChannelPid}) ->
    Reply = amqp_channel:call(ChannelPid, []),
    {reply, Reply, State}.

handle_cast(Msg, State) ->
    {noreply, State}.

handle_info(Msg, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    State.
