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
%% Submitted jobs are functions. They can be executed asynchronously
%% (using worker_pool:submit/1, worker_pool:submit/2) or synchronously
%% (using worker_pool:submit_async/1).
%%
%% We typically use the worker pool if we want to limit the maximum
%% parallelism of some job. We are not trying to dodge the cost of
%% creating Erlang processes.
%%
%% Supports nested submission of jobs and two execution modes:
%% 'single' and 'reuse'. Jobs executed in 'single' mode are invoked in
%% a one-off process. Those executed in 'reuse' mode are invoked in a
%% worker process out of the pool. Nested jobs are always executed
%% immediately in current worker process.
%%
%% 'single' mode is offered to work around a bug in Mnesia: after
%% network partitions reply messages for prior failed requests can be
%% sent to Mnesia clients - a reused worker pool process can crash on
%% receiving one.
%%
%% Caller submissions are enqueued internally. When the next worker
%% process is available, it communicates it to the pool and is
%% assigned a job to execute. If job execution fails with an error, no
%% response is returned to the caller.
%%
%% Worker processes prioritise certain command-and-control messages
%% from the pool.
%%
%% Future improvement points: job prioritisation.

-behaviour(gen_server2).

-export([start_link/0, submit/1, submit/2, submit_async/1, ready/1,
         idle/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(mfargs() :: {atom(), atom(), [any()]}).

-spec(start_link/0 :: () -> {'ok', pid()} | {'error', any()}).
-spec(submit/1 :: (fun (() -> A) | mfargs()) -> A).
-spec(submit/2 :: (fun (() -> A) | mfargs(), 'reuse' | 'single') -> A).
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
    submit(Fun, reuse).

%% ProcessModel =:= single is for working around the mnesia_locker bug.
submit(Fun, ProcessModel) ->
    case get(worker_pool_worker) of
        true -> worker_pool_worker:run(Fun);
        _    -> Pid = gen_server2:call(?SERVER, {next_free, self()}, infinity),
                worker_pool_worker:submit(Pid, Fun, ProcessModel)
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
