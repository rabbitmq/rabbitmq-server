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

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3, handle_pre_hibernate/1]).

-import(rabbit_misc, [pget/3]).
-import(rabbit_mgmt_db, [pget/2, id_name/1, id/2, lookup_element/2]).

-record(state, {
          interval,
          gc_timer,
          gc_next_key
         }).

-define(GC_INTERVAL, 5000).
-define(GC_MIN_ROWS, 100).
-define(GC_MIN_RATIO, 0.01).

-define(DROP_LENGTH, 1000).

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

%%----------------------------------------------------------------------------
%% Internal, gen_server2 callbacks
%%----------------------------------------------------------------------------

init(_) ->
    {ok, Interval} = application:get_env(rabbit, collect_statistics_interval),
    rabbit_log:info("Statistics garbage collector started.~n"),
    {ok, set_gc_timer(#state{interval = Interval}), hibernate,
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

gc_batch(State) ->
    {ok, Policies} = application:get_env(
                       rabbitmq_management, sample_retention_policies),
    Rows = erlang:max(?GC_MIN_ROWS,
                      round(?GC_MIN_RATIO * ets:info(aggregated_stats, size))),
    gc_batch(Rows, Policies, State).

gc_batch(0, _Policies, State) ->
    State;
gc_batch(Rows, Policies, State = #state{gc_next_key = Key0}) ->
    Key = case Key0 of
              undefined -> ets:first(aggregated_stats);
              _         -> ets:next(aggregated_stats, Key0)
          end,
    Key1 = case Key of
               '$end_of_table' -> undefined;
               _               -> Now = floor(
                                    time_compat:os_system_time(milli_seconds),
                                    State),
                                  Stats = ets:lookup_element(aggregated_stats, Key, 2),
                                  gc(Key, Stats, Policies, Now),
                                  Key
           end,
    gc_batch(Rows - 1, Policies, State#state{gc_next_key = Key1}).

gc({{Type, _}, _}, Stats, Policies, Now) ->
    Policy = pget(retention_policy(Type), Policies),
    rabbit_mgmt_stats:gc({Policy, Now}, Stats).

retention_policy(node_stats)             -> global;
retention_policy(node_node_stats)        -> global;
retention_policy(vhost_stats)            -> global;
retention_policy(queue_stats)            -> basic;
retention_policy(exchange_stats)         -> basic;
retention_policy(connection_stats)       -> basic;
retention_policy(channel_stats)          -> basic;
retention_policy(queue_exchange_stats)   -> detailed;
retention_policy(channel_exchange_stats) -> detailed;
retention_policy(channel_queue_stats)    -> detailed.
