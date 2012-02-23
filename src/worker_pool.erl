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

-export([start_link/0, submit/1, submit_async/1, idle/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(mfargs() :: {atom(), atom(), [any()]}).

-spec(start_link/0 :: () -> {'ok', pid()} | {'error', any()}).
-spec(submit/1 :: (fun (() -> A) | mfargs()) -> A).
-spec(submit_async/1 :: (fun (() -> any()) | mfargs()) -> 'ok').
-spec(idle/1 :: (any()) -> 'ok').

-endif.

%%----------------------------------------------------------------------------

-define(SERVER, ?MODULE).
-define(HIBERNATE_AFTER_MIN, 1000).
-define(DESIRED_HIBERNATE, 10000).

-record(state, { available, pending }).

%%----------------------------------------------------------------------------

start_link() ->
    gen_server2:start_link({local, ?SERVER}, ?MODULE, [],
                           [{timeout, infinity}]).

submit(Fun) ->
    case get(worker_pool_worker) of
        true -> worker_pool_worker:run(Fun);
        _    -> Pid = gen_server2:call(?SERVER, next_free, infinity),
                worker_pool_worker:submit(Pid, Fun)
    end.

submit_async(Fun) ->
    gen_server2:cast(?SERVER, {run_async, Fun}).

idle(WId) ->
    gen_server2:cast(?SERVER, {idle, WId}).

%%----------------------------------------------------------------------------

init([]) ->
    {ok, #state { pending = queue:new(), available = queue:new() }, hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

handle_call(next_free, From, State = #state { available = Avail,
                                              pending = Pending }) ->
    case queue:out(Avail) of
        {empty, _Avail} ->
            {noreply,
             State #state { pending = queue:in({next_free, From}, Pending) },
             hibernate};
        {{value, WId}, Avail1} ->
            {reply, get_worker_pid(WId), State #state { available = Avail1 },
             hibernate}
    end;

handle_call(Msg, _From, State) ->
    {stop, {unexpected_call, Msg}, State}.

handle_cast({idle, WId}, State = #state { available = Avail,
                                          pending = Pending }) ->
    {noreply, case queue:out(Pending) of
                  {empty, _Pending} ->
                      State #state { available = queue:in(WId, Avail) };
                  {{value, {next_free, From}}, Pending1} ->
                      gen_server2:reply(From, get_worker_pid(WId)),
                      State #state { pending = Pending1 };
                  {{value, {run_async, Fun}}, Pending1} ->
                      worker_pool_worker:submit_async(get_worker_pid(WId), Fun),
                      State #state { pending = Pending1 }
              end, hibernate};

handle_cast({run_async, Fun}, State = #state { available = Avail,
                                               pending = Pending }) ->
    {noreply,
     case queue:out(Avail) of
         {empty, _Avail} ->
             State #state { pending = queue:in({run_async, Fun}, Pending)};
         {{value, WId}, Avail1} ->
             worker_pool_worker:submit_async(get_worker_pid(WId), Fun),
             State #state { available = Avail1 }
     end, hibernate};

handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.

handle_info(Msg, State) ->
    {stop, {unexpected_info, Msg}, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, State) ->
    State.

%%----------------------------------------------------------------------------

get_worker_pid(WId) ->
    [{WId, Pid, _Type, _Modules} | _] =
        lists:dropwhile(fun ({Id, _Pid, _Type, _Modules})
                              when Id =:= WId -> false;
                            (_)               -> true
                        end,
                        supervisor:which_children(worker_pool_sup)),
    Pid.
