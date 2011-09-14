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
%% Copyright (c) 2011 VMware, Inc.  All rights reserved.
%%

-module(serialiser).

-behaviour(gen_server2).

-export([start_link/0, submit/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/0 :: () -> {'ok', pid()} | {'error', any()}).
-spec(submit/1 :: (fun (() -> A) | {atom(), atom(), [any()]}) -> A).

-endif.

%%----------------------------------------------------------------------------

-define(SERVER, ?MODULE).
-define(HIBERNATE_AFTER_MIN, 1000).
-define(DESIRED_HIBERNATE, 10000).

%%----------------------------------------------------------------------------

start_link() ->
    gen_server2:start_link({local, ?SERVER}, ?MODULE, [],
                           [{timeout, infinity}]).

submit(Fun) ->
    %% If the io_runner is not running, just run the Fun in the
    %% current process.
    case whereis(?SERVER) of
        undefined  -> run(Fun);
        _          -> gen_server2:call(?SERVER, {run, Fun}, infinity)
    end.

%%----------------------------------------------------------------------------

init([]) ->
    {ok, nostate, hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

handle_call({run, Fun}, _From, State) ->
    {reply, run(Fun), State, hibernate};
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
