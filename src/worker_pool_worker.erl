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
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(worker_pool_worker).

%% Executes jobs (functions) submitted to a worker pool with worker_pool:submit/1,
%% worker_pool:submit/2 or worker_pool:submit_async/1.
%%
%% See worker_pool for an overview.

-behaviour(gen_server2).

-export([start_link/1, next_job_from/2, submit/3, submit_async/2,
         run/1]).

-export([set_maximum_since_use/2]).
-export([set_timeout/2, set_timeout/3, clear_timeout/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, prioritise_cast/3]).

%%----------------------------------------------------------------------------

-type mfargs() :: {atom(), atom(), [any()]}.

-spec start_link(atom) -> {'ok', pid()} | {'error', any()}.
-spec next_job_from(pid(), pid()) -> 'ok'.
-spec submit(pid(), fun (() -> A) | mfargs(), 'reuse' | 'single') -> A.
-spec submit_async(pid(), fun (() -> any()) | mfargs()) -> 'ok'.
-spec run(fun (() -> A)) -> A; (mfargs()) -> any().
-spec set_maximum_since_use(pid(), non_neg_integer()) -> 'ok'.

%%----------------------------------------------------------------------------

-define(HIBERNATE_AFTER_MIN, 1000).
-define(DESIRED_HIBERNATE, 10000).

%%----------------------------------------------------------------------------

start_link(PoolName) ->
    gen_server2:start_link(?MODULE, [PoolName], [{timeout, infinity}]).

next_job_from(Pid, CPid) ->
    gen_server2:cast(Pid, {next_job_from, CPid}).

submit(Pid, Fun, ProcessModel) ->
    gen_server2:call(Pid, {submit, Fun, self(), ProcessModel}, infinity).

submit_async(Pid, Fun) ->
    gen_server2:cast(Pid, {submit_async, Fun}).

set_maximum_since_use(Pid, Age) ->
    gen_server2:cast(Pid, {set_maximum_since_use, Age}).

run({M, F, A}) -> apply(M, F, A);
run(Fun)       -> Fun().

run(Fun, reuse) ->
    run(Fun);
run(Fun, single) ->
    Self = self(),
    Ref = make_ref(),
    spawn_link(fun () ->
                       put(worker_pool_worker, true),
                       Self ! {Ref, run(Fun)},
                       unlink(Self)
               end),
    receive
        {Ref, Res} -> Res
    end.

%%----------------------------------------------------------------------------

init([PoolName]) ->
    ok = file_handle_cache:register_callback(?MODULE, set_maximum_since_use,
                                             [self()]),
    ok = worker_pool:ready(PoolName, self()),
    put(worker_pool_worker, true),
    put(worker_pool_name, PoolName),
    {ok, undefined, hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

prioritise_cast({set_maximum_since_use, _Age}, _Len, _State) -> 8;
prioritise_cast({next_job_from, _CPid},        _Len, _State) -> 7;
prioritise_cast(_Msg,                          _Len, _State) -> 0.

handle_call({submit, Fun, CPid, ProcessModel}, From, undefined) ->
    {noreply, {job, CPid, From, Fun, ProcessModel}, hibernate};

handle_call({submit, Fun, CPid, ProcessModel}, From, {from, CPid, MRef}) ->
    erlang:demonitor(MRef),
    gen_server2:reply(From, run(Fun, ProcessModel)),
    ok = worker_pool:idle(get(worker_pool_name), self()),
    {noreply, undefined, hibernate};

handle_call(Msg, _From, State) ->
    {stop, {unexpected_call, Msg}, State}.

handle_cast({next_job_from, CPid}, undefined) ->
    MRef = erlang:monitor(process, CPid),
    {noreply, {from, CPid, MRef}, hibernate};

handle_cast({next_job_from, CPid}, {job, CPid, From, Fun, ProcessModel}) ->
    gen_server2:reply(From, run(Fun, ProcessModel)),
    ok = worker_pool:idle(get(worker_pool_name), self()),
    {noreply, undefined, hibernate};

handle_cast({submit_async, Fun}, undefined) ->
    run(Fun),
    ok = worker_pool:idle(get(worker_pool_name), self()),
    {noreply, undefined, hibernate};

handle_cast({set_maximum_since_use, Age}, State) ->
    ok = file_handle_cache:set_maximum_since_use(Age),
    {noreply, State, hibernate};

handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.

handle_info({'DOWN', MRef, process, CPid, _Reason}, {from, CPid, MRef}) ->
    ok = worker_pool:idle(get(worker_pool_name), self()),
    {noreply, undefined, hibernate};

handle_info({'DOWN', _MRef, process, _Pid, _Reason}, State) ->
    {noreply, State, hibernate};

handle_info({timeout, Key, Fun}, State) ->
    clear_timeout(Key),
    Fun(),
    {notrply, State, hibernate};

handle_info(Msg, State) ->
    {stop, {unexpected_info, Msg}, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, State) ->
    State.

-spec set_timeout(integer(), fun(() -> any())) -> ref().
set_timeout(Time, Fun) ->
    Key = make_ref(),
    set_timeout(Key, Time, Fun).

-spec set_timeout(Key, integer(), fun(() -> any())) -> Key when Key :: any().
set_timeout(Key, Time, Fun) ->
    Timeouts = get_timeouts(),
    set_timeout(Key, Time, Fun, Timeouts).

-spec clear_timeout(any()) -> ok.
clear_timeout(Key) ->
    NewTimeouts = cancel_timeout(Key, get_timeouts()),
    put(timeouts, NewTimeouts),
    ok.

get_timeouts() ->
    case get(timeouts) of
        undefined -> dict:new();
        Dict      -> Dict
    end.

set_timeout(Key, Time, Fun, Timeouts) ->
    cancel_timeout(Key, Timeouts),
    {ok, TRef} = timer:send_after(Time, {timeout, Key, Fun}),
    NewTimeouts = dict:store(Key, TRef, Timeouts),
    put(timeouts, NewTimeouts),
    {ok, Key}.

cancel_timeout(Key, Timeouts) ->
    case dict:find(Key, Timeouts) of
        {ok, TRef} ->
            timer:cancel(TRef),
            receive {timeout, Key, _} -> ok
            after 0 -> ok
            end,
            dict:erase(Key, Timeouts);
        error ->
            Timeouts
    end.
