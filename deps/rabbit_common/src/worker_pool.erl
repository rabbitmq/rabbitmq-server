%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(worker_pool).

%% Generic worker pool manager.
%%
%% Submitted jobs are functions. They can be executed synchronously
%% (using worker_pool:submit/1, worker_pool:submit/2) or asynchronously
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

-export([start_link/1,
         submit/1, submit/2, submit/3,
         submit_async/1, submit_async/2,
         dispatch_sync/1, dispatch_sync/2,
         ready/2,
         idle/2,
         default_pool/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%%----------------------------------------------------------------------------

-type mfargs() :: {atom(), atom(), [any()]}.

-spec start_link(atom()) -> {'ok', pid()} | {'error', any()}.
-spec submit(fun (() -> A) | mfargs()) -> A.
-spec submit(fun (() -> A) | mfargs(), 'reuse' | 'single') -> A.
-spec submit(atom(), fun (() -> A) | mfargs(), 'reuse' | 'single') -> A.
-spec submit_async(fun (() -> any()) | mfargs()) -> 'ok'.
-spec dispatch_sync(fun(() -> any()) | mfargs()) -> 'ok'.
-spec ready(atom(), pid()) -> 'ok'.
-spec idle(atom(), pid()) -> 'ok'.
-spec default_pool() -> atom().

%%----------------------------------------------------------------------------

-define(DEFAULT_POOL, ?MODULE).
-define(HIBERNATE_AFTER_MIN, 1000).
-define(DESIRED_HIBERNATE, 10000).

-record(state, { available, pending }).

%%----------------------------------------------------------------------------

start_link(Name) -> gen_server2:start_link({local, Name}, ?MODULE, [],
                                           [{timeout, infinity}]).

submit(Fun) ->
    submit(?DEFAULT_POOL, Fun, reuse).

%% ProcessModel =:= single is for working around the mnesia_locker bug.
submit(Fun, ProcessModel) ->
    submit(?DEFAULT_POOL, Fun, ProcessModel).

submit(Server, Fun, ProcessModel) ->
    case get(worker_pool_worker) of
        true -> worker_pool_worker:run(Fun);
        _    -> Pid = gen_server2:call(Server, {next_free, self()}, infinity),
                worker_pool_worker:submit(Pid, Fun, ProcessModel)
    end.

submit_async(Fun) -> submit_async(?DEFAULT_POOL, Fun).

submit_async(Server, Fun) -> gen_server2:cast(Server, {run_async, Fun}).

dispatch_sync(Fun) ->
    dispatch_sync(?DEFAULT_POOL, Fun).

dispatch_sync(Server, Fun) ->
    Pid = gen_server2:call(Server, {next_free, self()}, infinity),
    worker_pool_worker:submit_async(Pid, Fun).

ready(Server, WPid) -> gen_server2:cast(Server, {ready, WPid}).

idle(Server, WPid) -> gen_server2:cast(Server, {idle, WPid}).

default_pool() -> ?DEFAULT_POOL.

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
