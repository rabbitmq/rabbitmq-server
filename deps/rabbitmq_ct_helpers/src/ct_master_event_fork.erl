%%
%% %CopyrightBegin%
%% 
%% Copyright Ericsson AB 2006-2024. All Rights Reserved.
%% 
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%% 
%% %CopyrightEnd%
%%

%%% Common Test Framework Event Handler
%%%
%%% This module implements an event handler that the CT Master
%%% uses to handle status and progress notifications sent to the
%%% master node during test runs. This module may be used as a 
%%% template for other event handlers that can be plugged in to 
%%% handle logging and reporting on the master node.
-module(ct_master_event_fork).
-moduledoc false.

-behaviour(gen_event).

%% API
-export([start_link/0, add_handler/0, add_handler/1, stop/0]).
-export([notify/1, sync_notify/1]).

%% gen_event callbacks
-export([init/1, handle_event/2, handle_call/2, 
	 handle_info/2, terminate/2, code_change/3]).

-include_lib("common_test/include/ct_event.hrl").
-include_lib("common_test/src/ct_util.hrl").


-record(state, {}).

%%====================================================================
%% gen_event callbacks
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | {error,Error} 
%% Description: Creates an event manager.
%%--------------------------------------------------------------------
start_link() ->
    gen_event:start_link({local,?CT_MEVMGR}). 

%%--------------------------------------------------------------------
%% Function: add_handler() -> ok | {'EXIT',Reason} | term()
%% Description: Adds an event handler
%%--------------------------------------------------------------------
add_handler() ->
    gen_event:add_handler(?CT_MEVMGR_REF,?MODULE,[]).
add_handler(Args) ->
    gen_event:add_handler(?CT_MEVMGR_REF,?MODULE,Args).

%%--------------------------------------------------------------------
%% Function: stop() -> ok
%% Description: Stops the event manager
%%--------------------------------------------------------------------
stop() ->
    case flush() of
	{error,Reason} ->
	    ct_master_logs_fork:log("Error",
			       "No response from CT Master Event.\n"
			       "Reason = ~tp\n"
			       "Terminating now!\n",[Reason]),
	    %% communication with event manager fails, kill it
	    catch exit(whereis(?CT_MEVMGR_REF), kill);
	_ ->
	    gen_event:stop(?CT_MEVMGR_REF)
    end.

flush() ->
    try gen_event:call(?CT_MEVMGR_REF,?MODULE,flush,1800000) of
	flushing ->
	    timer:sleep(1),
	    flush();
	done ->
	    ok;
	Error = {error,_} ->
	    Error
    catch
	_:Reason ->
	    {error,Reason}
    end.

%%--------------------------------------------------------------------
%% Function: notify(Event) -> ok
%% Description: Asynchronous notification to event manager.
%%--------------------------------------------------------------------
notify(Event) ->
    gen_event:notify(?CT_MEVMGR_REF,Event).

%%--------------------------------------------------------------------
%% Function: sync_notify(Event) -> ok
%% Description: Synchronous notification to event manager.
%%--------------------------------------------------------------------
sync_notify(Event) ->
    gen_event:sync_notify(?CT_MEVMGR_REF,Event).

%%====================================================================
%% gen_event callbacks
%%====================================================================
%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State}
%% Description: Whenever a new event handler is added to an event manager,
%% this function is called to initialize the event handler.
%%--------------------------------------------------------------------
init(_) ->
    ct_util:mark_process(),
    ct_master_logs_fork:log("CT Master Event Handler started","",[]),
    {ok,#state{}}.

%%--------------------------------------------------------------------
%% Function:  
%% handle_event(Event, State) -> {ok, State} |
%%                               {swap_handler, Args1, State1, Mod2, Args2} |
%%                               remove_handler
%% Description:Whenever an event manager receives an event sent using
%% gen_event:notify/2 or gen_event:sync_notify/2, this function is called for
%% each installed event handler to handle the event. 
%%--------------------------------------------------------------------
handle_event(#event{name=start_logging,node=Node,data=RunDir},State) ->
    ct_master_logs_fork:log("CT Master Event Handler","Got ~ts from ~w",[RunDir,Node]),
    ct_master_logs_fork:nodedir(Node,RunDir),
    {ok,State};

handle_event(#event{name=Name,node=Node,data=Data},State) ->
    print("~n=== ~w ===~n", [?MODULE]),
    print("~tw on ~w: ~tp~n", [Name,Node,Data]),
    {ok,State}.

%%--------------------------------------------------------------------
%% Function: 
%% handle_call(Request, State) -> {ok, Reply, State} |
%%                                {swap_handler, Reply, Args1, State1, 
%%                                  Mod2, Args2} |
%%                                {remove_handler, Reply}
%% Description: Whenever an event manager receives a request sent using
%% gen_event:call/3,4, this function is called for the specified event 
%% handler to handle the request.
%%--------------------------------------------------------------------
handle_call(flush,State) ->
    case process_info(self(),message_queue_len) of
	{message_queue_len,0} ->
	    {ok,done,State};
	_ ->
	    {ok,flushing,State}
    end.

%%--------------------------------------------------------------------
%% Function: 
%% handle_info(Info, State) -> {ok, State} |
%%                             {swap_handler, Args1, State1, Mod2, Args2} |
%%                              remove_handler
%% Description: This function is called for each installed event handler when
%% an event manager receives any other message than an event or a synchronous
%% request (or a system message).
%%--------------------------------------------------------------------
handle_info(_Info,State) ->
    {ok,State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> ok
%% Description:Whenever an event handler is deleted from an event manager,
%% this function is called. It should be the opposite of Module:init/1 and 
%% do any necessary cleaning up. 
%%--------------------------------------------------------------------
terminate(_Reason,_State) ->
    ct_master_logs_fork:log("CT Master Event Handler stopping","",[]),
    ok.

%%--------------------------------------------------------------------
%% Function: code_change(OldVsn, State, Extra) -> {ok, NewState} 
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn,State,_Extra) ->
    {ok,State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

print(_Str,_Args) ->
%    io:format(_Str,_Args),
    ok.
