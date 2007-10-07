-module(amqp_rpc_handler).

-behaviour(gen_event).

-include_lib("rabbitmq_server/include/rabbit.hrl").
-include_lib("rabbitmq_server/include/rabbit_framing.hrl").
-include("amqp_client.hrl").
-export([init/1, handle_info/2, terminate/2]).

%---------------------------------------------------------------------------
% gen_event callbacks
%---------------------------------------------------------------------------

init([State]) ->
    {ok, State}.

handle_info(shutdown, State) ->
    {remove_handler, State};

handle_info(#'basic.consume_ok'{consumer_tag = ConsumerTag}, State) ->
    {ok, State};

handle_info(#'basic.cancel_ok'{consumer_tag = ConsumerTag}, State) ->
    {ok, State};

handle_info({content, ClassId, Properties, PropertiesBin, Payload},
            State = #rpc_handler_state{broker_config = BrokerConfig, server_name = ServerName}) ->
    #broker_config{channel_pid = ChannelPid, ticket = Ticket,
                   exchange = X, content_type = ContentType} = BrokerConfig,
    Props = #'P_basic'{correlation_id = CorrelationId,
                       reply_to = Q,
                       content_type = ContentType}
    = rabbit_framing:decode_properties(ClassId, PropertiesBin),
%%     [Function|Arguments] = decode(ContentType, Payload),
%%     _Reply = gen_server:call(ServerName, [Function|Arguments]),
%%     Reply = encode(ContentType, _Reply),
    Reply = <<"555">>,
    BasicPublish = #'basic.publish'{ticket = Ticket, exchange = <<"">>,
                                    routing_key = Q,
                                    mandatory = false, immediate = false},
    ReplyProps = #'P_basic'{correlation_id = CorrelationId},
    Content = #content{class_id = 60, %% TODO HARDCODED VALUE
                       properties = ReplyProps, properties_bin = 'none',
                       payload_fragments_rev = [Reply]},
    amqp_channel:cast(ChannelPid, BasicPublish, Content),
    {ok, State}.

terminate(Args, State) ->
    ok.

%---------------------------------------------------------------------------
% Encoding and decoding
%---------------------------------------------------------------------------

decode(<<"application/x-hessian">>, Payload) ->
    hessian:decode(Payload).

encode(<<"application/x-hessian">>, Payload) ->
    hessian:encode_reply(Payload).
