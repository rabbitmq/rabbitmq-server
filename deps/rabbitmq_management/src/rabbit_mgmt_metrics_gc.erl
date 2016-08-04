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

-record(state, {intervals}).

-include_lib("rabbit_common/include/rabbit.hrl").

-spec start_link(atom()) -> rabbit_types:ok_pid_or_error().

-export([name/1]).
-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

name(EventType) ->
    list_to_atom((atom_to_list(EventType) ++ "_metrics_gc")).

start_link(EventType) ->
    gen_server2:start_link({local, name(EventType)}, ?MODULE, [EventType], []).

init([EventType]) ->
    {ok, Policies} = application:get_env(
                       rabbitmq_management, sample_retention_policies),
    Policy = retention_policy(EventType),
    Intervals = [I || {_, I} <- proplists:get_value(Policy, Policies)],
    {ok, #state{intervals = Intervals}}.

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast({event, #event{type  = connection_closed, props = [{pid, Pid}]}},
	    State = #state{intervals = Intervals}) ->
    remove_connection(Pid, Intervals),
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

retention_policy(connection_closed) -> basic.

remove_connection(Id, Intervals) ->
    ets:delete(connection_created_stats, Id),
    ets:delete(connection_stats, Id),
    delete_samples(connection_stats_coarse_conn_stats, Id, Intervals),
    delete_samples(vhost_stats_coarse_conn_stats, Id, Intervals).

delete_samples(Table, Id, Intervals) ->
    [ets:delete(Table, {Id, I}) || I <- Intervals].
