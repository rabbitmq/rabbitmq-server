-module(rabbit_management).

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

handle_call([Function|Arguments], From, State) ->
    io:format("Calling management function ~p with args ~p~n",[Function,Arguments]),
    case catch apply(rabbit_access_control, Function, Arguments) of
        {error, Reason} ->
            exit(do_something_about_this, Reason);
        {ok, Response} ->
            io:format("Return from management function -  ~p~n",[Response]),
            {reply, Response, State};
        ok ->
            {reply, ok, State}
    end.


handle_cast(Msg, State) ->
    {noreply, State}.

handle_info(Msg, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    State.

