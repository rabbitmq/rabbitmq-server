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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(worker_pool_worker).

-behaviour(gen_server2).

-export([start_link/1, submit/2, submit_async/2, run/1]).

-export([set_maximum_since_use/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, prioritise_cast/2]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(mfargs() :: {atom(), atom(), [any()]}).

-spec(start_link/1 :: (any()) -> {'ok', pid()} | {'error', any()}).
-spec(submit/2 :: (pid(), fun (() -> A) | mfargs()) -> A).
-spec(submit_async/2 :: (pid(), fun (() -> any()) | mfargs()) -> 'ok').
-spec(run/1 :: (fun (() -> A)) -> A; (mfargs()) -> any()).
-spec(set_maximum_since_use/2 :: (pid(), non_neg_integer()) -> 'ok').

-endif.

%%----------------------------------------------------------------------------

-define(HIBERNATE_AFTER_MIN, 1000).
-define(DESIRED_HIBERNATE, 10000).

%%----------------------------------------------------------------------------

start_link(WId) ->
    gen_server2:start_link(?MODULE, [WId], [{timeout, infinity}]).

submit(Pid, Fun) ->
    gen_server2:call(Pid, {submit, Fun}, infinity).

submit_async(Pid, Fun) ->
    gen_server2:cast(Pid, {submit_async, Fun}).

set_maximum_since_use(Pid, Age) ->
    gen_server2:cast(Pid, {set_maximum_since_use, Age}).

run({M, F, A}) ->
    apply(M, F, A);
run(Fun) ->
    Fun().

%%----------------------------------------------------------------------------

init([WId]) ->
    ok = file_handle_cache:register_callback(?MODULE, set_maximum_since_use,
                                             [self()]),
    ok = worker_pool:idle(WId),
    put(worker_pool_worker, true),
    {ok, WId, hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

prioritise_cast({set_maximum_since_use, _Age}, _State) -> 8;
prioritise_cast(_Msg,                          _State) -> 0.

handle_call({submit, Fun}, From, WId) ->
    gen_server2:reply(From, run(Fun)),
    ok = worker_pool:idle(WId),
    {noreply, WId, hibernate};

handle_call(Msg, _From, State) ->
    {stop, {unexpected_call, Msg}, State}.

handle_cast({submit_async, Fun}, WId) ->
    run(Fun),
    ok = worker_pool:idle(WId),
    {noreply, WId, hibernate};

handle_cast({set_maximum_since_use, Age}, WId) ->
    ok = file_handle_cache:set_maximum_since_use(Age),
    {noreply, WId, hibernate};

handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.

handle_info(Msg, State) ->
    {stop, {unexpected_info, Msg}, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, State) ->
    State.
