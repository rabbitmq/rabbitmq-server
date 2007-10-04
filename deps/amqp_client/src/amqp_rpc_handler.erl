-module(amqp_rpc_handler).

-behaviour(gen_event).

-include_lib("rabbitmq_server/include/rabbit.hrl").
-include_lib("rabbitmq_server/include/rabbit_framing.hrl").
-include("amqp_client.hrl").
-export([init/1, handle_info/2, terminate/2]).

%---------------------------------------------------------------------------
% gen_event callbacks
%---------------------------------------------------------------------------

init([BrokerConfig]) ->
    {ok, BrokerConfig}.

handle_info(shutdown, State) ->
    {remove_handler, State};

handle_info(#'basic.consume_ok'{consumer_tag = ConsumerTag}, State) ->
    {ok, State};

handle_info(#'basic.cancel_ok'{consumer_tag = ConsumerTag}, State) ->
    {ok, State};

handle_info({content, ClassId, Properties, PropertiesBin, Payload},
            State = #rpc_client_state{channel_pid = ChannelPid, ticket = Ticket,
                            exchange = X}) ->
    Props = #'P_basic'{correlation_id = CorrelationId, reply_to = Q}
    = rabbit_framing:decode_properties(ClassId, PropertiesBin),
    io:format("------>RPC handler corr id: ~p~n", [CorrelationId]),

    BasicPublish = #'basic.publish'{ticket = Ticket, exchange = X,
                                    routing_key = Q,
                                    mandatory = false, immediate = false},
    ReplyProps = #'P_basic'{correlation_id = CorrelationId},
    Content = #content{class_id = 60, %% TODO HARDCODED VALUE
                       properties = ReplyProps, properties_bin = 'none',
                       payload_fragments_rev = [<<"f00bar">>]},
    amqp_channel:cast(ChannelPid, BasicPublish, Content),

    {ok, State}.

terminate(Args, State) ->
    ok.
