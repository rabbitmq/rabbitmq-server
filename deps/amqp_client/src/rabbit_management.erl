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

