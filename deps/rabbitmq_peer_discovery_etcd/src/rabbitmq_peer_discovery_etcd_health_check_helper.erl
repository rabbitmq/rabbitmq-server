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
%%   The Original Code is RabbitMQ Management Console.
%%
%%   The Initial Developer of the Original Code is GoPivotal, Inc.
%%   Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

%% This gen_server starts a periodic timer on behalf of
%% a short lived process that kicks off peer discovery.
%% This is so that the timer is not automatically canceled
%% and cleaned up by the timer server when the short lived
%% process terminates.

-module(rabbitmq_peer_discovery_etcd_health_check_helper).

-behaviour(gen_server).

-export([start_link/0, start_timer/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-record(state, {timer_ref}).


%%
%% API
%%

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

start_timer(Interval) ->
    gen_server:call(?MODULE, {start_timer, Interval}, infinity).



init([]) ->
    {ok, #state{timer_ref = undefined}}.

handle_call({start_timer, Interval}, _From, #state{timer_ref = undefined} = State) ->
    rabbit_log:info("Starting etcd health check notifier (effective interval: ~p milliseconds)", [Interval]),
    {ok, TRef} = timer:apply_interval(Interval, rabbit_peer_discovery_etcd,
                                      update_node_key, []),
    {reply, ok, State#state{timer_ref = TRef}};

handle_call({start_timer, _Interval}, _From, State) ->
    {reply, ok, State};

handle_call(_Msg, _From, State) ->
    {reply, not_understood, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_MSg, State) ->
    {noreply, State}.

terminate(_Arg, #state{timer_ref = undefined}) ->
    ok;

terminate(_Arg, #state{timer_ref = TRef}) ->
    timer:cancel(TRef),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
