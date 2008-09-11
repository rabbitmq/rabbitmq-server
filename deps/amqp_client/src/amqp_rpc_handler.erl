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

-module(amqp_rpc_handler).

-behaviour(gen_server).

-include_lib("rabbitmq_server/include/rabbit.hrl").
-include_lib("rabbitmq_server/include/rabbit_framing.hrl").
-include("amqp_client.hrl").

-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2, handle_info/2]).

%---------------------------------------------------------------------------
% gen_server callbacks
%---------------------------------------------------------------------------
init([ServerName, TypeMapping, Username, Password,
      BC = #broker_config{exchange = X, routing_key = RoutingKey,
                          queue = Q, bind_key = BindKey}]) ->
    Connection = amqp_connection:start(Username, Password),
    ChannelPid = test_util:setup_channel(Connection),
    ok = test_util:setup_exchange(ChannelPid, Q, X, BindKey),
    BrokerConfig = BC#broker_config{channel_pid = ChannelPid},
    State = #rpc_handler_state{server_name = ServerName,
                               type_mapping = TypeMapping,
                               broker_config = BrokerConfig},
    BasicConsume = #'basic.consume'{queue = Q,
                                    consumer_tag = <<"">>,
                                    no_local = false, no_ack = true, exclusive = false, nowait = false},
    #'basic.consume_ok'{consumer_tag = ConsumerTag} = amqp_channel:call(ChannelPid, BasicConsume, self()),
    init([State]);

init([State = #rpc_handler_state{server_name = ServerName}]) ->
    %% TODO Think about registering gen_servers and linking them to this....
    %% it's probably a bad idea because then the server is tied to the rpc handler
    {ok, Pid} = gen_server:start_link(ServerName, [], []),
    {ok, State#rpc_handler_state{server_pid = Pid}}.

handle_info(shutdown, State) ->
    terminate(shutdown, State);

handle_info(#'basic.consume_ok'{consumer_tag = ConsumerTag}, State) ->
    {noreply, State};

handle_info(#'basic.cancel_ok'{consumer_tag = ConsumerTag}, State) ->
    {noreply, State};

handle_info({content, ClassId, Properties, PropertiesBin, Payload},
            State = #rpc_handler_state{broker_config = BrokerConfig,
                                       server_pid = ServerPid,
                                       type_mapping = TypeMapping}) ->
    #broker_config{channel_pid = ChannelPid, exchange = X} = BrokerConfig,
    Props = #'P_basic'{correlation_id = CorrelationId,
                       reply_to = Q,
                       content_type = ContentType}
    = rabbit_framing:decode_properties(ClassId, PropertiesBin),

    io:format("ABOUT 2---------> ~p / ~p ~n",[Payload,TypeMapping]),
    T = amqp_rpc_util:decode(ContentType, Payload, TypeMapping),
    io:format("---------> ~p~n",[T]),

    Response = case amqp_rpc_util:decode(ContentType, Payload, TypeMapping) of
                   {error, Encoded} ->
                        Encoded;
                   [Function,Arguments] ->
                        %% This doesn't seem to be the right way to do this dispatch
                        FunctionName = list_to_atom(binary_to_list(Function)),
                        case gen_server:call(ServerPid, [FunctionName|Arguments]) of
                            {'EXIT', Reason} ->
                                amqp_rpc_util:encode(fault, ContentType, Reason);
                            Reply ->
                                amqp_rpc_util:encode(reply, ContentType, Reply, TypeMapping)
                        end
               end,
    BasicPublish = #'basic.publish'{exchange = <<"">>,
                                    routing_key = Q,
                                    mandatory = false, immediate = false},
    ReplyProps = #'P_basic'{correlation_id = CorrelationId,
                            content_type = ContentType},
    Content = #content{class_id = 60, %% TODO HARDCODED VALUE
                       properties = ReplyProps, properties_bin = 'none',
                       payload_fragments_rev = [Response]},
    amqp_channel:cast(ChannelPid, BasicPublish, Content),
    {noreply, State}.

%---------------------------------------------------------------------------
% Rest of the gen_server callbacks
%---------------------------------------------------------------------------

handle_call(Message, From, State) ->
    {noreply, State}.

handle_cast(Message, State) ->
    {noreply, State}.

terminate(Reason, State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    State.
