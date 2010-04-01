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
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2010 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2010 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(worker_pool_worker).

-behaviour(gen_server2).

-export([start_link/1, submit/2, run/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/1 :: (any()) -> {'ok', pid()} | 'ignore' | {'error', any()}).
-spec(submit/2 :: (pid(), fun (() -> A) | {atom(), atom(), [any()]}) -> A).

-endif.

%%----------------------------------------------------------------------------

-define(HIBERNATE_AFTER_MIN, 1000).
-define(DESIRED_HIBERNATE, 10000).

%%----------------------------------------------------------------------------

start_link(WId) ->
    gen_server2:start_link(?MODULE, [WId], [{timeout, infinity}]).

submit(Pid, Fun) ->
    gen_server2:call(Pid, {submit, Fun}, infinity).

init([WId]) ->
    ok = worker_pool:idle(WId),
    put(worker_pool_worker, true),
    {ok, WId, hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

handle_call({submit, Fun}, From, WId) ->
    gen_server2:reply(From, run(Fun)),
    ok = worker_pool:idle(WId),
    {noreply, WId};

handle_call(Msg, _From, State) ->
    {stop, {unexpected_call, Msg}, State}.

handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.

handle_info(Msg, State) ->
    {stop, {unexpected_info, Msg}, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, State) ->
    State.

%%----------------------------------------------------------------------------

run({M, F, A}) ->
    apply(M, F, A);
run(Fun) ->
    Fun().
