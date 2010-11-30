-module(rabbit_amqp1_0).

-behaviour(application).

-export([start/0, start/2, stop/1]).

%% Interface

start() ->
    start(normal, []).

%% Callbacks

start(normal, []) ->
    {ok, Listeners} = application:get_env(tcp_listeners),
    {ok, SupPid} = rabbit_amqp1_0_sup:start_link(Listeners),
    %% TODO report starting up ...
    {ok, SupPid}.

stop(_State) ->
    ok.
