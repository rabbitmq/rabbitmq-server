%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(mirrored_supervisor_tests).

-compile([export_all]).

-export([init/1, handle_call/3, handle_info/2, terminate/2, code_change/3,
         handle_cast/2]).

-behaviour(gen_server).
-behaviour(mirrored_supervisor).

%% ---------------------------------------------------------------------------
%% Functional tests
%% ---------------------------------------------------------------------------

all_tests() ->
    passed = test_simple(),
    passed.

test_simple() ->
    {ok, A} = start_sup(a),
    {ok, B} = start_sup(b),
    mirrored_supervisor:start_child(a, childspec(worker)),
    Pid1 = pid_of(worker),
    kill(A),
    Pid2 = pid_of(worker),
    false = (Pid1 =:= Pid2),
    kill(B),
    passed.

%% ---------------------------------------------------------------------------

start_sup(Name) ->
    start_sup(Name, group).

start_sup(Name, Group) ->
    mirrored_supervisor:start_link({local, Name}, Group, ?MODULE, []).

childspec(Id) ->
    {Id, {?MODULE, start_gs, [Id]}, transient, 16#ffffffff, worker, [?MODULE]}.

start_gs(Id) ->
    gen_server:start_link({local, Id}, ?MODULE, [], []).

pid_of(Id) ->
    {received, Pid, ping} = call(Id, ping),
    {ok, Pid}.

call(Id, Msg) -> call(Id, Msg, 100, 10).

call(Id, Msg, 0, Decr) ->
    exit({timeout_waiting_for_server, Id, Msg});

call(Id, Msg, MaxDelay, Decr) ->
    try
        gen_server:call(Id, Msg, infinity)
    catch exit:{shutdown, _} -> timer:sleep(Decr),
                                call(Id, Msg, MaxDelay - Decr, Decr)
    end.

kill(Pid) -> exit(Pid, bang).

%% ---------------------------------------------------------------------------
%% Dumb gen_server we can supervise
%% ---------------------------------------------------------------------------

%% Cheeky: this is used for both the gen_server and supervisor
%% behaviours. So our gen server has a weird-looking state. But we
%% don't care.
init(ChildSpecs) ->
    {ok, {{one_for_one, 3, 10}, ChildSpecs}}.

handle_call(Msg, _From, State) ->
    {reply, {received, self(), Msg}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
