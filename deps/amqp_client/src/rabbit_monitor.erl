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
%%   The Original Code is RabbitMQ.
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
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_monitor).

-behaviour(gen_server).

-export([start_link/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([monitor/2, demonitor/2, fake_exit/2]).

-define(SERVER, ?MODULE).

-record(state, {callback, monitors}).

%%--------------------------------------------------------------------

start_link(Name, Callback) ->
    gen_server:start_link(Name, ?MODULE, Callback, []).

monitor(Name, Pid) ->
    gen_server:call(Name, {monitor, Pid}).

demonitor(Name, Pid) ->
    gen_server:call(Name, {demonitor, Pid}).

fake_exit(Name, Pid) ->
    gen_server:call(Name, {fake_exit, Pid}).

%%--------------------------------------------------------------------

init(Callback) ->
    process_flag(trap_exit, true),
    {ok, #state{callback = Callback, monitors = dict:new()}}.

handle_call({monitor, Pid}, _From, State = #state{monitors = Monitors}) ->
    case dict:is_key(Pid, Monitors) of
        true -> {reply, ok, State};
        false ->
            MonitorRef = erlang:monitor(process, Pid),
            {reply, ok, State#state{monitors =
                                    dict:store(Pid, MonitorRef, Monitors)}}
    end;
handle_call({demonitor, Pid}, _From, State = #state{monitors = Monitors}) ->
    case dict:is_key(Pid, Monitors) of
        true ->
            erlang:demonitor(dict:fetch(Pid, Monitors)),
            {reply, ok, State#state{monitors = dict:erase(Pid, Monitors)}};
        false -> {reply, ok, State}
    end;
handle_call({fake_exit, Pid}, _From,
            State = #state{callback = Callback, monitors = Monitors}) ->
    case dict:is_key(Pid, Monitors) of
        true ->
            invoke_callback(Callback, Pid),
            erlang:demonitor(dict:fetch(Pid, Monitors)),
            {reply, ok, State#state{monitors = dict:erase(Pid, Monitors)}};
        false -> {reply, ok, State}
    end;
handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _MonitorRef, process, Pid, _Reason},
            State = #state{callback = Callback, monitors = Monitors}) ->
    case dict:is_key(Pid, Monitors) of
        true ->
            invoke_callback(Callback, Pid),
            {noreply, State#state{monitors = dict:erase(Pid, Monitors)}};
        false -> {noreply, State}
    end;

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{callback = Callback, monitors = Monitors}) ->
    lists:foreach(fun ({Pid, _MonitorRef}) ->
                          invoke_callback(Callback, Pid)
                  end,
                  dict:to_list(Monitors)),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------

invoke_callback({Mod, Fun}, Pid) -> apply(Mod, Fun, [Pid]);
invoke_callback(Fun, Pid) -> Fun(Pid).
