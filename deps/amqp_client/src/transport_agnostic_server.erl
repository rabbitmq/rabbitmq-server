-module(transport_agnostic_server).

-export([start/1]).

-behaviour(gen_server).

-export([start/1]).
-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2, handle_info/2]).

%---------------------------------------------------------------------------
% gen_server callbacks
%---------------------------------------------------------------------------

start(Args) ->
    {ok, Pid} = gen_server:start(?MODULE, [], []),
    Pid.

init(Args) ->
    {ok, []}.

terminate(Reason, State) ->
    ok.

handle_call(Payload, From, State) ->
    {reply, something, State}.

handle_cast(Msg, State) ->
    {noreply, State}.

handle_info(Msg, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    State.
