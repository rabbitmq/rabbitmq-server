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

-module(mnesia_sync).

-behaviour(gen_server2).

-export([sync/0]).

-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {sync_pid}).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(sync/0 :: () -> 'ok').

-endif.

%%----------------------------------------------------------------------------

start_link() ->
    gen_server2:start_link({local, ?SERVER}, ?MODULE, [], [{timeout, infinity}]).

sync() ->
    gen_server2:call(?SERVER, sync).

%%----------------------------------------------------------------------------

sync_proc() ->
    receive
        {sync_request, From} -> sync_proc([From]);
        stop                 -> ok;
        Other                -> error({unexpected_non_sync, Other})
    end.

sync_proc(Waiting) ->
    receive
        {sync_request, From} -> sync_proc([From | Waiting])
    after 0 ->
        disk_log:sync(latest_log),
        [gen_server2:reply(From, ok) || From <- Waiting],
        sync_proc()
    end.

%%----------------------------------------------------------------------------

init([]) ->
    {ok, #state{sync_pid  = case rabbit_mnesia:is_disc_node() of
                                true  -> proc_lib:spawn_link(fun sync_proc/0);
                                false -> undefined
                            end}}.

handle_call(sync, _From, #state{sync_pid = undefined} = State) ->
    {reply, ok, State};
handle_call(sync, From, #state{sync_pid = SyncProcPid} = State) ->
    SyncProcPid ! {sync_request, From},
    {noreply, State};
handle_call(Request, _From, State) ->
    {stop, {unhandled_call, Request}, State}.

handle_cast(Request, State) ->
    {stop, {unhandled_cast, Request}, State}.

handle_info(Message, State) ->
    {stop, {unhandled_info, Message}, State}.

terminate(_Reason, #state{sync_pid = undefined}) ->
    ok;
terminate(_Reason, #state{sync_pid = SyncProcPid}) ->
    SyncProcPid ! stop,
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
