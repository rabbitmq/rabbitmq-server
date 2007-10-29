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
    {ok, Pid} = gen_server:start_link(ServerName, [], []),
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
    %% This doesn't seem to be the right way to do this dispatch
    FunctionName = list_to_atom(binary_to_list(Function)),
    _Reply = gen_server:call(ServerPid, [FunctionName|Arguments]),
    Reply = amqp_rpc_util:encode(reply, ContentType, _Reply),
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
