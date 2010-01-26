-module(rabbit_shovel_worker).
-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%---------------------------
% Gen Server Implementation
% --------------------------

init([]) ->
    io:format("~p alive!~n", [self()]),
    gen_server:cast(self(), die_soon),
    {ok, ok}.

handle_call(_Msg, _From, State) ->
    {noreply, State}.

handle_cast(die_soon, State) ->
    Sleep = 2000,
    io:format("~p dying in ~p~n", [self(), Sleep]),
    timer:sleep(Sleep),
    {stop, normal, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, _State) ->
    io:format("~p terminating with reason ~p~n", [self(), Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
