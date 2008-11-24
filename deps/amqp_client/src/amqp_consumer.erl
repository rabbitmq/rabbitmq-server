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

-module(amqp_consumer).

-behaviour(gen_event).

-include_lib("rabbitmq_server/include/rabbit_framing.hrl").

-export([init/1, handle_info/2, terminate/2]).
-export([code_change/3, handle_call/2, handle_event/2]).

%---------------------------------------------------------------------------
% gen_event callbacks
%---------------------------------------------------------------------------

init(_Args) ->
    {ok, []}.

handle_info(shutdown, State) ->
    io:format("---------------------------~n"),
    io:format("AMQP Consumer SHUTDOWN~n"),
    io:format("---------------------------~n"),
    {remove_handler, State};

handle_info(#'basic.consume_ok'{consumer_tag = ConsumerTag}, State) ->
    io:format("---------------------------~n"),
    io:format("AMQP Consumer, rec'd consume ok, tag= ~p~n", [ConsumerTag] ),
    io:format("---------------------------~n"),
    {ok, State};

handle_info(#'basic.cancel_ok'{consumer_tag = ConsumerTag}, State) ->
    io:format("---------------------------~n"),
    io:format("AMQP Consumer, rec'd cancel ok, tag= ~p~n", [ConsumerTag] ),
    io:format("---------------------------~n"),
    {ok, State};

handle_info({#'basic.deliver'{},
              {content, _ClassId, _Properties, _PropertiesBin, Payload}},
              State) ->
     io:format("---------------------------~n"),
     io:format("AMQP Consumer, rec'd: ~p~n", [ Payload ] ),
     io:format("---------------------------~n"),
     {ok, State}.
     
% Just put these 3 calls to stop warning
% This whole event handler will be deleted in 19344 
handle_call(_, _) -> ok.
handle_event(_, _) -> ok.
code_change(_, _, _) -> ok.

terminate(_Args, _State) ->
    ok.
