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
%%   The Initial Developer of the Original Code is Pivotal Software, Inc.
%%   Copyright (c) 2010-2015 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_queue_stats_collector).

-include("rabbit_mgmt.hrl").
-include("rabbit_mgmt_metrics.hrl").
-include("rabbit_mgmt_event_collector.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(gen_server2).

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3, handle_pre_hibernate/1]).

-export([prioritise_cast/3]).

-import(rabbit_misc, [pget/3]).
-import(rabbit_mgmt_db, [pget/2, id_name/1, id/2, lookup_element/2]).

prioritise_cast({event, #event{type = queue_stats}}, Len,
                #state{max_backlog = MaxBacklog} = _State)
  when Len > MaxBacklog ->
    drop;
prioritise_cast(_Msg, _Len, _State) ->
    0.

%% See the comment on rabbit_mgmt_db for the explanation of
%% events and stats.

%% Although this gen_server could process all types of events through the
%% handle_cast, rabbit_mgmt_db_handler (in the management agent) forwards
%% only the non-prioritiy events channel_stats
%%----------------------------------------------------------------------------
%% API
%%----------------------------------------------------------------------------

start_link() ->
    case gen_server2:start_link({global, ?MODULE}, ?MODULE, [], []) of
        {ok, Pid} -> register(?MODULE, Pid), %% [1]
                     {ok, Pid};
        Else      -> Else
    end.
%% [1] For debugging it's helpful to locally register the name too
%% since that shows up in places global names don't.

%%----------------------------------------------------------------------------
%% Internal, gen_server2 callbacks
%%----------------------------------------------------------------------------

init([]) ->
    {ok, Interval} = application:get_env(rabbit, collect_statistics_interval),
    {ok, RatesMode} = application:get_env(rabbitmq_management, rates_mode),
    {ok, MaxBacklog} = application:get_env(rabbitmq_management,
                                           stats_event_max_backlog),
    process_flag(priority, high),
    rabbit_log:info("Statistics queue stats collector started.~n"),
    {ok, reset_lookups(
           #state{interval               = Interval,
                  rates_mode             = RatesMode,
                  max_backlog            = MaxBacklog}), hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

%% Used in rabbit_mgmt_test_db where we need guarantees events have
%% been handled before querying
handle_call({event, Event = #event{reference = none}}, _From, State) ->
    rabbit_mgmt_event_collector_utils:handle_event(Event, State),
    reply(ok, State);

handle_call(_Request, _From, State) ->
    reply(not_understood, State).

%% Only handle events that are real.
handle_cast({event, Event = #event{reference = none}}, State) ->
    rabbit_mgmt_event_collector_utils:handle_event(Event, State),
    noreply(State);

handle_cast(_Request, State) ->
    noreply(State).

handle_info(_Info, State) ->
    noreply(State).

terminate(_Arg, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

reply(Reply, NewState) -> {reply, Reply, NewState, hibernate}.
noreply(NewState) -> {noreply, NewState, hibernate}.

reset_lookups(State) ->
    State#state{lookups = [{exchange, fun rabbit_exchange:lookup/1},
                           {queue,    fun rabbit_amqqueue:lookup/1}]}.

handle_pre_hibernate(State) ->
    %% rabbit_event can end up holding on to some memory after a busy
    %% workout, but it's not a gen_server so we can't make it
    %% hibernate. The best we can do is forcibly GC it here (if
    %% rabbit_mgmt_db is hibernating the odds are rabbit_event is
    %% quiescing in some way too).
    rpc:multicall(
      rabbit_mnesia:cluster_nodes(running), rabbit_mgmt_db_handler, gc, []),
    {hibernate, State}.
