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

-module(rabbit_mgmt_stats_gc).

-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(gen_server2).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3, handle_pre_hibernate/1]).

-export([name/1]).

-import(rabbit_misc, [pget/3]).
-import(rabbit_mgmt_db, [pget/2, id_name/1, id/2, lookup_element/2]).

-record(state, {
          interval,
          gc_timer,
          gc_table,
          gc_index,
          gc_next_key
         }).

-define(GC_INTERVAL, 5000).
-define(GC_MIN_ROWS, 100).
-define(GC_MIN_RATIO, 0.01).

-define(DROP_LENGTH, 1000).

%%----------------------------------------------------------------------------
%% API
%%----------------------------------------------------------------------------

start_link(Table) ->
    Ref = make_ref(),
    case gen_server2:start_link({global, name(Table)}, ?MODULE, [Ref, Table], []) of
        {ok, Pid} -> register(name(Table), Pid), %% [1]
                     rabbit:force_event_refresh(Ref),
                     {ok, Pid};
        Else      -> Else
    end.
%% [1] For debugging it's helpful to locally register the name too
%% since that shows up in places global names don't.

%%----------------------------------------------------------------------------
%% Internal, gen_server2 callbacks
%%----------------------------------------------------------------------------

init([_, Table]) ->
    {ok, Interval} = application:get_env(rabbit, collect_statistics_interval),
    rabbit_log:info("Statistics garbage collector started for table ~p.~n", [Table]),
    {ok, set_gc_timer(#state{interval = Interval,
                             gc_table = Table,
                             gc_index = rabbit_mgmt_stats_tables:key_index(Table)}),
     hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

handle_call(_Request, _From, State) ->
    reply(not_understood, State).

handle_cast(_Request, State) ->
    noreply(State).

handle_info(gc, State) ->
    noreply(set_gc_timer(gc_batch(State)));

handle_info(_Info, State) ->
    noreply(State).

terminate(_Arg, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

reply(Reply, NewState) -> {reply, Reply, NewState, hibernate}.
noreply(NewState) -> {noreply, NewState, hibernate}.

set_gc_timer(State) ->
    TRef = erlang:send_after(?GC_INTERVAL, self(), gc),
    State#state{gc_timer = TRef}.

handle_pre_hibernate(State) ->
    {hibernate, State}.

%%----------------------------------------------------------------------------
%% Internal, utilities
%%----------------------------------------------------------------------------

floor(TS, #state{interval = Interval}) ->
    rabbit_mgmt_util:floor(TS, Interval).

%%----------------------------------------------------------------------------
%% Internal, event-GCing
%%----------------------------------------------------------------------------

gc_batch(#state{gc_index = Index} = State) ->
    {ok, Policies} = application:get_env(
                       rabbitmq_management, sample_retention_policies),
    Total = ets:info(Index, size),
    Rows = erlang:max(?GC_MIN_ROWS, round(?GC_MIN_RATIO * Total)),
    gc_batch(Rows, Policies, State).

gc_batch(0, _Policies, State) ->
    State;
gc_batch(Rows, Policies, State = #state{gc_next_key = Cont,
                                        gc_table = Table,
                                        gc_index = Index}) ->
    Select = case Cont of
                 undefined ->
                     ets:first(Index);
                 _ ->
                     ets:next(Index, Cont)
             end,
    NewCont = case Select of
                  '$end_of_table' ->
                      undefined;
                  Key ->
                      Now = floor(
                              time_compat:os_system_time(milli_seconds),
                              State),
                      gc(Key, Table, Policies, Now),
                      Key
              end,
    gc_batch(Rows - 1, Policies, State#state{gc_next_key = NewCont}).

gc(Key, Table, Policies, Now) ->
    Policy = pget(retention_policy(Table), Policies),
    rabbit_mgmt_stats:gc({Policy, Now}, Table, Key).

retention_policy(aggr_node_stats_deliver_get) -> global;
retention_policy(aggr_node_stats_fine_stats) -> global;
retention_policy(aggr_node_stats_queue_msg_rates) -> global;
retention_policy(aggr_node_stats_msg_rates_details) -> global;
retention_policy(aggr_node_stats_queue_msg_counts) -> global;
retention_policy(aggr_node_stats_coarse_node_stats) -> global;
retention_policy(aggr_node_stats_coarse_node_node_stats) -> global;
retention_policy(aggr_node_stats_coarse_conn_stats) -> global;
retention_policy(aggr_node_node_stats_deliver_get) -> global;
retention_policy(aggr_node_node_stats_fine_stats) -> global;
retention_policy(aggr_node_node_stats_queue_msg_rates) -> global;
retention_policy(aggr_node_node_stats_msg_rates_details) -> global;
retention_policy(aggr_node_node_stats_queue_msg_counts) -> global;
retention_policy(aggr_node_node_stats_coarse_node_stats) -> global;
retention_policy(aggr_node_node_stats_coarse_node_node_stats) -> global;
retention_policy(aggr_node_node_stats_coarse_conn_stats) -> global;
retention_policy(aggr_vhost_stats_deliver_get) -> global;
retention_policy(aggr_vhost_stats_fine_stats) -> global;
retention_policy(aggr_vhost_stats_queue_msg_rates) -> global;
retention_policy(aggr_vhost_stats_msg_rates_details) -> global;
retention_policy(aggr_vhost_stats_queue_msg_counts) -> global;
retention_policy(aggr_vhost_stats_coarse_node_stats) -> global;
retention_policy(aggr_vhost_stats_coarse_node_node_stats) -> global;
retention_policy(aggr_vhost_stats_coarse_conn_stats) -> global;
retention_policy(aggr_queue_stats_fine_stats) -> basic;
retention_policy(aggr_queue_stats_deliver_get) -> basic;
retention_policy(aggr_queue_stats_queue_msg_counts) -> basic;
retention_policy(aggr_exchange_stats_fine_stats) -> basic;
retention_policy(aggr_connection_stats_coarse_conn_stats) -> basic;
retention_policy(aggr_channel_stats_deliver_get) -> basic;
retention_policy(aggr_channel_stats_fine_stats) -> basic;
retention_policy(aggr_channel_stats_queue_msg_rates) -> basic;
retention_policy(aggr_channel_stats_msg_rates_details) -> basic;
retention_policy(aggr_channel_stats_queue_msg_counts) -> basic;
retention_policy(aggr_channel_stats_coarse_node_stats) -> basic;
retention_policy(aggr_channel_stats_coarse_node_node_stats) -> basic;
retention_policy(aggr_channel_stats_coarse_conn_stats) -> basic;
retention_policy(aggr_queue_exchange_stats_deliver_get)   -> detailed;
retention_policy(aggr_queue_exchange_stats_fine_stats)   -> detailed;
retention_policy(aggr_queue_exchange_stats_queue_msg_rates)   -> detailed;
retention_policy(aggr_queue_exchange_stats_msg_rates_details)   -> detailed;
retention_policy(aggr_queue_exchange_stats_queue_msg_counts)   -> detailed;
retention_policy(aggr_queue_exchange_stats_coarse_node_stats)   -> detailed;
retention_policy(aggr_queue_exchange_stats_coarse_node_node_stats)   -> detailed;
retention_policy(aggr_queue_exchange_stats_coarse_conn_stats)   -> detailed;
retention_policy(aggr_channel_exchange_stats_deliver_get) -> detailed;
retention_policy(aggr_channel_exchange_stats_fine_stats) -> detailed;
retention_policy(aggr_channel_exchange_stats_queue_msg_rates) -> detailed;
retention_policy(aggr_channel_exchange_stats_msg_rates_details) -> detailed;
retention_policy(aggr_channel_exchange_stats_queue_msg_counts) -> detailed;
retention_policy(aggr_channel_exchange_stats_coarse_node_stats) -> detailed;
retention_policy(aggr_channel_exchange_stats_coarse_node_node_stats) -> detailed;
retention_policy(aggr_channel_exchange_stats_coarse_conn_stats) -> detailed;
retention_policy(aggr_channel_queue_stats_deliver_get) -> detailed;
retention_policy(aggr_channel_queue_stats_fine_stats) -> detailed;
retention_policy(aggr_channel_queue_stats_queue_msg_rates) -> detailed;
retention_policy(aggr_channel_queue_stats_msg_rates_details) -> detailed;
retention_policy(aggr_channel_queue_stats_queue_msg_counts) -> detailed;
retention_policy(aggr_channel_queue_stats_coarse_node_stats) -> detailed;
retention_policy(aggr_channel_queue_stats_coarse_node_node_stats) -> detailed;
retention_policy(aggr_channel_queue_stats_coarse_conn_stats) -> detailed.

name(Atom) ->
    list_to_atom((atom_to_list(Atom) ++ "_gc")).
