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

-module(rabbit_mgmt_event_collector).

-include("rabbit_mgmt.hrl").
-include("rabbit_mgmt_metrics.hrl").
-include("rabbit_mgmt_event_collector.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(gen_server2).

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3, handle_pre_hibernate/1]).

%% For testing
-export([override_lookups/1, reset_lookups/0]).

-import(rabbit_mgmt_db, [pget/2]).

%% See the comment on rabbit_mgmt_db for the explanation of
%% events and stats.

%% Although this gen_server could process all types of events through the
%% handle_cast, rabbit_mgmt_db_handler (in the management agent) forwards
%% the non-prioritiy events channel_stats and queue_stats to their own gen_servers

%%----------------------------------------------------------------------------
%% API
%%----------------------------------------------------------------------------

start_link() ->
    Ref = make_ref(),
    case gen_server2:start_link({global, ?MODULE}, ?MODULE, [Ref], []) of
        {ok, Pid} -> register(?MODULE, Pid), %% [1]
                     rabbit:force_event_refresh(Ref),
                     {ok, Pid};
        Else      -> Else
    end.
%% [1] For debugging it's helpful to locally register the name too
%% since that shows up in places global names don't.

override_lookups(Lookups) ->
    gen_server2:call({global, ?MODULE}, {override_lookups, Lookups}, infinity).
reset_lookups() ->
    gen_server2:call({global, ?MODULE}, reset_lookups, infinity).

%%----------------------------------------------------------------------------
%% Internal, gen_server2 callbacks
%%----------------------------------------------------------------------------

init([Ref]) ->
    %% When Rabbit is overloaded, it's usually especially important
    %% that the management plugin work.
    process_flag(priority, high),
    {ok, Interval} = application:get_env(rabbit, collect_statistics_interval),
    {ok, RatesMode} = application:get_env(rabbitmq_management, rates_mode),
    rabbit_node_monitor:subscribe(self()),
    rabbit_log:info("Statistics event collector started.~n"),
    ?TABLES = [ets:new(Key, [public, set, named_table]) || Key <- ?TABLES],
    %% Index for cleaning up stats of abnormally terminated processes.
    [ets:new(rabbit_mgmt_stats_tables:key_index(Table),
             [ordered_set, public, named_table]) || Table <- ?PROC_STATS_TABLES],
    %% Index for the deleting of fine stats, reduces the number of reductions
    %% to 1/8 under heavy load.
    ets:new(old_stats_fine_index, [bag, public, named_table]),
    ?AGGR_TABLES = [rabbit_mgmt_stats:blank(Name) || Name <- ?AGGR_TABLES],
    {ok, reset_lookups(
           #state{interval               = Interval,
                  event_refresh_ref      = Ref,
                  rates_mode             = RatesMode}), hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

%% Used in rabbit_mgmt_test_db where we need guarantees events have
%% been handled before querying
handle_call({event, Event = #event{reference = none}}, _From, State) ->
    rabbit_mgmt_event_collector_utils:handle_event(Event, State),
    reply(ok, State);

handle_call({override_lookups, Lookups}, _From, State) ->
    reply(ok, State#state{lookups = Lookups});

handle_call(reset_lookups, _From, State) ->
    reply(ok, reset_lookups(State));

handle_call(_Request, _From, State) ->
    reply(not_understood, State).

%% Only handle events that are real, or pertain to a force-refresh
%% that we instigated.
handle_cast({event, Event = #event{reference = none}}, State) ->
    rabbit_mgmt_event_collector_utils:handle_event(Event, State),
    noreply(State);

handle_cast({event, Event = #event{reference = Ref}},
            State = #state{event_refresh_ref = Ref}) ->
    rabbit_mgmt_event_collector_utils:handle_event(Event, State),
    noreply(State);

handle_cast(_Request, State) ->
    noreply(State).

handle_info({node_down, Node}, State) ->
    Conns = created_events(connection_stats),
    Chs = created_events(channel_stats),
    delete_all_from_node(connection_closed, Node, Conns, State),
    delete_all_from_node(channel_closed, Node, Chs, State),
    noreply(State);

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

delete_all_from_node(Type, Node, [Item | Items], State) ->
    Pid = pget(pid, Item),
    case node(Pid) of
        Node ->
            rabbit_mgmt_event_collector_utils:handle_event(
              #event{type = Type, props = [{pid, Pid}]}, State);
        _    -> ok
    end,
    delete_all_from_node(Type, Node, Items, State);
delete_all_from_node(_Type, _Node, [], _State) ->
    ok.

created_events(Table) ->
    ets:select(Table, [{{{'_', '$1'}, '$2', '_'}, [{'==', 'create', '$1'}],
                        ['$2']}]).
