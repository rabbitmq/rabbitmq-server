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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%
-module(rabbit_mgmt_metrics_gc).

-record(state, {basic_i,
		detailed_i
	       }).

-include_lib("rabbit_common/include/rabbit.hrl").

-spec start_link(atom()) -> rabbit_types:ok_pid_or_error().

-export([name/1]).
-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-import(rabbit_mgmt_db, [pget/2]).

name(EventType) ->
    list_to_atom((atom_to_list(EventType) ++ "_metrics_gc")).

start_link(EventType) ->
    gen_server2:start_link({local, name(EventType)}, ?MODULE, [], []).

init(_) ->
    {ok, Policies} = application:get_env(
                       rabbitmq_management, sample_retention_policies),
    {ok, #state{basic_i = intervals(basic, Policies),
		detailed_i = intervals(detailed, Policies)}}.

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast({event, #event{type  = connection_closed, props = Props}},
	    State = #state{basic_i = Intervals}) ->
    Pid = pget(pid, Props),
    remove_connection(Pid, Intervals),
    {noreply, State};
handle_cast({event, #event{type  = channel_closed, props = Props}},
	    State = #state{basic_i = Intervals}) ->
    Pid = pget(pid, Props),
    remove_channel(Pid, Intervals),
    {noreply, State};
handle_cast({event, #event{type  = consumer_deleted, props = Props}}, State) ->
    remove_consumer(Props),
    {noreply, State};
handle_cast({event, #event{type  = exchange_deleted, props = Props}},
	    State = #state{detailed_i = Intervals}) ->
    Name = pget(name, Props),
    remove_exchange(Name, Intervals),
    {noreply, State};
handle_cast({event, #event{type  = queue_deleted, props = Props}},
	    State = #state{basic_i = BIntervals,
			   detailed_i = DIntervals}) ->
    Name = pget(name, Props),
    remove_queue(Name, BIntervals, DIntervals),
    {noreply, State};
handle_cast({event, #event{type  = vhost_deleted, props = Props}},
	    State = #state{basic_i = BIntervals,
			   detailed_i = DIntervals}) ->
    Name = pget(name, Props),
    remove_vhost(Name, BIntervals, DIntervals),
    {noreply, State};
handle_cast({event, #event{type  = node_node_deleted, props = Props}}, State) ->
    Name = pget(route, Props),
    remove_node_node(Name),
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

remove_connection(Id, Intervals) ->
    ets:delete(connection_created_stats, Id),
    ets:delete(connection_stats, Id),
    delete_samples(connection_stats_coarse_conn_stats, Id, Intervals),
    delete_samples(vhost_stats_coarse_conn_stats, Id, Intervals).

remove_channel(Id, Intervals) ->
    ets:delete(channel_created_stats, Id),
    ets:delete(channel_stats, Id),
    delete_samples(channel_process_stats, Id, Intervals),
    delete_samples(channel_stats_fine_stats, Id, Intervals),
    delete_samples(channel_stats_deliver_stats, Id, Intervals),
    ets:select_delete(consumer_stats, match_consumer_spec(Id)),
    ets:select_delete(old_aggr_stats, match_spec(Id)),
    ets:select_delete(channel_exchange_stats_fine_stats, match_interval_spec(Id)),
    ets:select_delete(channel_queue_stats_deliver_stats, match_interval_spec(Id)),
    ok.

remove_consumer(Props) ->
    Id = {pget(queue, Props), pget(channel, Props), pget(consumer_tag, Props)},
    ets:delete(consumer_stats, Id).

remove_exchange(Name, Intervals) ->
    delete_samples(exchange_stats_publish_out, Name, Intervals),
    delete_samples(exchange_stats_publish_in, Name, Intervals),
    ets:select_delete(queue_exchange_stats_publish, match_second_interval_spec(Name)),
    ets:select_delete(channel_exchange_stats_fine_stats, match_second_interval_spec(Name)).

remove_queue(Name, BIntervals, DIntervals) ->
    ets:delete(queue_stats, Name),
    delete_samples(queue_stats_publish, Name, DIntervals),
    delete_samples(queue_stats_deliver_stats, Name, DIntervals),
    ets:select_delete(channel_queue_stats_deliver_stats, match_second_interval_spec({Name})),
    ets:select_delete(queue_exchange_stats_publish, match_interval_spec({Name})),
    delete_samples(queue_process_stats, Name, BIntervals),
    delete_samples(queue_msg_stats, Name, BIntervals),
    ets:select_delete(old_aggr_stats, match_second_spec({Name})),
    ets:select_delete(consumer_stats, match_queue_consumer_spec({Name})),
    ok.

remove_vhost(Name, BIntervals, DIntervals) ->
    delete_samples(vhost_stats_coarse_conn_stats, Name, BIntervals),
    delete_samples(vhost_stats_fine_stats, Name, DIntervals),
    delete_samples(vhost_stats_deliver_stats, Name, DIntervals).

remove_node_node(Name) ->
    ets:select_delete(node_node_coarse_stats, match_second_interval_spec(Name)).

intervals(Type, Policies) ->
    [I || {_, I} <- proplists:get_value(Type, Policies)].

delete_samples(Table, Id, Intervals) ->
    [ets:delete(Table, {Id, I}) || I <- Intervals].

match_spec(Id) ->
    [{{{'$1', '_'}, '_'}, [{'==', Id, '$1'}], [true]}].

match_second_spec(Id) ->
    [{{{'_', '$1'}, '_'}, [{'==', Id, '$1'}], [true]}].

match_interval_spec(Id) ->
    [{{{{'$1', '_'}, '_'}, '_'}, [{'==', Id, '$1'}], [true]}].

match_second_interval_spec(Id) ->
    [{{{{'_', '$1'}, '_'}, '_'}, [{'==', Id, '$1'}], [true]}].

match_consumer_spec(Id) ->
    [{{{'_', '$1', '_'}, '_'}, [{'==', Id, '$1'}], [true]}].

match_queue_consumer_spec(Id) ->
    [{{{'$1', '_', '_'}, '_'}, [{'==', Id, '$1'}], [true]}].
