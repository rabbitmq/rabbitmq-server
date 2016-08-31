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
-module(rabbit_mgmt_metrics_collector).

-record(state, {table, agent, policies}).

-spec start_link(atom()) -> rabbit_types:ok_pid_or_error().

-export([name/1]).
-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-import(rabbit_misc, [pget/3]).

name(Table) ->
    list_to_atom((atom_to_list(Table) ++ "_metrics_collector")).

start_link(Table) ->
    gen_server2:start_link({local, name(Table)}, ?MODULE, [Table], []).

init([Table]) ->    
    {ok, Policies} = application:get_env(
                       rabbitmq_management, sample_retention_policies),
    Policy = retention_policy(Table),
    TablePolicies = proplists:get_value(Policy, Policies),
    Interval = take_smaller(TablePolicies),
    {ok, Agent} = rabbit_mgmt_agent_collector_sup:start_child(self(), Table,
							      Interval * 1000),
    {ok, #state{table = Table, agent = Agent, policies = TablePolicies}}.

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast({metrics, Timestamp, Records}, State = #state{table = Table,
                                                          policies = TablePolicies}) ->
    aggregate_metrics(Timestamp, Table, TablePolicies, Records),
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

retention_policy(connection_created) -> basic; %% really nothing
retention_policy(connection_metrics) -> basic;
retention_policy(connection_metrics_simple_metrics) -> basic.

take_smaller(Policies) ->
    lists:min([I || {_, I} <- Policies]).

aggregate_metrics(Timestamp, Table, Policies, Records) ->
    [aggregate_entry(Timestamp, Table, Policies, R) || R <- Records].

aggregate_entry(_TS, connection_created, _, {Id, Metrics}) ->
    case ets:member(connection_created_stats, Id) of
        true -> 
            ok;
        false ->
            Ftd = rabbit_mgmt_format:format(
                    Metrics,
                    {fun rabbit_mgmt_format:format_connection_created/1, true}),
            ets:insert(connection_created_stats, {Id, pget(name, Ftd, unknown), Ftd})
    end;
aggregate_entry(_TS, connection_metrics, _, {Id, Metrics}) ->
    ets:insert(connection_stats, {Id, Metrics});
aggregate_entry(TS, connection_metrics_simple_metrics, Policies,
                {Id, RecvOct, SendOct, Reductions}) ->
    [begin
         insert_entry(connection_stats_coarse_conn_stats, Id, TS,
                      {RecvOct, SendOct, Reductions}, Size, Interval),
         insert_entry(vhost_stats_coarse_conn_stats, Id, TS,
                      {RecvOct, SendOct, Reductions}, Size, Interval)
     end || {Size, Interval} <- Policies].

insert_entry(Table, Id, TS, Entry, Size, Interval) ->
    Key = {Id, Interval},
    Slide = case ets:lookup(Table, Key) of
                [{Key, S}] ->
                    S;
                [] ->
                    exometer_slide:new(Size * 1000, [{interval, Interval * 1000}])
            end,
    ets:insert(Table, {Key, exometer_slide:add_element(TS, Entry, Slide)}).
