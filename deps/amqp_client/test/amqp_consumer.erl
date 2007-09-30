-module(amqp_consumer).

-behaviour(gen_event).

-include_lib("rabbit/include/rabbit_framing.hrl").

-export([init/1, handle_info/2, terminate/2]).

%---------------------------------------------------------------------------
% gen_event callbacks
%---------------------------------------------------------------------------

init(Args) ->
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

handle_info({content, ClassId, Properties, PropertiesBin, Payload}, State) ->
     io:format("---------------------------~n"),
     io:format("AMQP Consumer, rec'd: ~p~n", [ Payload ] ),
     io:format("---------------------------~n"),
     {ok, State}.

terminate(Args, State) ->
    ok.
