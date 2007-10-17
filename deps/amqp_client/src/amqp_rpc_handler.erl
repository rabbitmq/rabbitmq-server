-module(amqp_rpc_handler).

-behaviour(gen_event).

-include_lib("rabbitmq_server/include/rabbit.hrl").
-include_lib("rabbitmq_server/include/rabbit_framing.hrl").
-include("amqp_client.hrl").
-export([init/1, handle_info/2, terminate/2]).

%---------------------------------------------------------------------------
% gen_event callbacks
%---------------------------------------------------------------------------

init([State = #rpc_handler_state{server_name = ServerName}]) ->
    %% TODO Think about registering gen_servers and linking them to this....
    %% it's probably a bad idea because then the server is tied to the rpc handler
    io:format("Starting gen serv ~n"),
    {ok, Pid} = gen_server:start_link(ServerName, [], []),
    io:format("Started gen serv ~n"),
    {ok, State#rpc_handler_state{server_pid = Pid}}.

handle_info(shutdown, State) ->
    {remove_handler, State};

handle_info(#'basic.consume_ok'{consumer_tag = ConsumerTag}, State) ->
    {ok, State};

handle_info(#'basic.cancel_ok'{consumer_tag = ConsumerTag}, State) ->
    {ok, State};

handle_info({content, ClassId, Properties, PropertiesBin, Payload},
            State = #rpc_handler_state{broker_config = BrokerConfig, server_pid = ServerPid}) ->
    #broker_config{channel_pid = ChannelPid, ticket = Ticket, exchange = X} = BrokerConfig,
    Props = #'P_basic'{correlation_id = CorrelationId,
                       reply_to = Q,
                       content_type = ContentType}
    = rabbit_framing:decode_properties(ClassId, PropertiesBin),
    [Function,Arguments] = amqp_rpc_util:decode(ContentType, Payload),
    io:format("Before gen serv call~n"),
    _Reply = gen_server:call(ServerPid, [Function|Arguments]),
    io:format("After gen serv call ~p~n",[_Reply]),
    Reply = amqp_rpc_util:encode(reply, ContentType, _Reply),
    %%Reply = <<"555">>,
    BasicPublish = #'basic.publish'{ticket = Ticket, exchange = <<"">>,
                                    routing_key = Q,
                                    mandatory = false, immediate = false},
    ReplyProps = #'P_basic'{correlation_id = CorrelationId,
                            content_type = ContentType},
    Content = #content{class_id = 60, %% TODO HARDCODED VALUE
                       properties = ReplyProps, properties_bin = 'none',
                       payload_fragments_rev = [Reply]},
    amqp_channel:cast(ChannelPid, BasicPublish, Content),
    {ok, State}.

terminate(Args, State) ->
    ok.
