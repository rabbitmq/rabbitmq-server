%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(worker_pool).

%% Generic worker pool manager.
%%
%% Supports nested submission of jobs (nested jobs always run
%% immediately in current worker process).
%%
%% Possible future enhancements:
%%
%% 1. Allow priorities (basically, change the pending queue to a
%% priority_queue).

-behaviour(gen_server2).

-export([start_link/0, submit/1, submit_async/1, ready/1, idle/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(mfargs() :: {atom(), atom(), [any()]}).

-spec(start_link/0 :: () -> {'ok', pid()} | {'error', any()}).
-spec(submit/1 :: (fun (() -> A) | mfargs()) -> A).
-spec(submit_async/1 :: (fun (() -> any()) | mfargs()) -> 'ok').
-spec(ready/1 :: (pid()) -> 'ok').
-spec(idle/1 :: (pid()) -> 'ok').

-endif.

%%----------------------------------------------------------------------------

-define(SERVER, ?MODULE).
-define(HIBERNATE_AFTER_MIN, 1000).
-define(DESIRED_HIBERNATE, 10000).

-record(state, { available, pending }).

%%----------------------------------------------------------------------------

start_link() -> gen_server2:start_link({local, ?SERVER}, ?MODULE, [],
                                       [{timeout, infinity}]).

submit(Fun) ->
    case get(worker_pool_worker) of
        true -> worker_pool_worker:run(Fun);
        _    -> Pid = gen_server2:call(?SERVER, {next_free, self()}, infinity),
                worker_pool_worker:submit(Pid, Fun)
    end.

submit_async(Fun) -> gen_server2:cast(?SERVER, {run_async, Fun}).

ready(WPid) -> gen_server2:cast(?SERVER, {ready, WPid}).

idle(WPid) -> gen_server2:cast(?SERVER, {idle, WPid}).

%%----------------------------------------------------------------------------

init([]) ->
    {ok, #state { pending = queue:new(), available = ordsets:new() }, hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

handle_call({next_free, CPid}, From, State = #state { available = [],
                                                      pending   = Pending }) ->
    {noreply, State#state{pending = queue:in({next_free, From, CPid}, Pending)},
     hibernate};
handle_call({next_free, CPid}, _From, State = #state { available =
                                                           [WPid | Avail1] }) ->
    worker_pool_worker:next_job_from(WPid, CPid),
    {reply, WPid, State #state { available = Avail1 }, hibernate};

handle_call(Msg, _From, State) ->
    {stop, {unexpected_call, Msg}, State}.

handle_cast({ready, WPid}, State) ->
    erlang:monitor(process, WPid),
    handle_cast({idle, WPid}, State);

handle_cast({idle, WPid}, State = #state { available = Avail,
                                           pending   = Pending }) ->
    {noreply,
     case queue:out(Pending) of
         {empty, _Pending} ->
             State #state { available = ordsets:add_element(WPid, Avail) };
         {{value, {next_free, From, CPid}}, Pending1} ->
             worker_pool_worker:next_job_from(WPid, CPid),
             gen_server2:reply(From, WPid),
             State #state { pending = Pending1 };
         {{value, {run_async, Fun}}, Pending1} ->
             worker_pool_worker:submit_async(WPid, Fun),
             State #state { pending = Pending1 }
     end, hibernate};

handle_cast({run_async, Fun}, State = #state { available = [],
                                               pending   = Pending }) ->
    {noreply, State #state { pending = queue:in({run_async, Fun}, Pending)},
     hibernate};
handle_cast({run_async, Fun}, State = #state { available = [WPid | Avail1] }) ->
    worker_pool_worker:submit_async(WPid, Fun),
    {noreply, State #state { available = Avail1 }, hibernate};

handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.

handle_info({'DOWN', _MRef, process, WPid, _Reason},
            State = #state { available = Avail }) ->
    {noreply, State #state { available = ordsets:del_element(WPid, Avail) },
     hibernate};

handle_info(Msg, State) ->
    {stop, {unexpected_info, Msg}, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, State) ->
    State.
