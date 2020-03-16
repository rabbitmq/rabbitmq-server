%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stream_manager).
-behaviour(gen_server).

%% API
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).
-export([start_link/1, create/1, register/0, delete/1, lookup/1, unregister/0, init_mnesia_tables/0]).

-record(state, {
    configuration, listeners, monitors
}).

-record(?MODULE, {name, leader_pid, leader, replicas}).

-define(TABLE, ?MODULE).

-rabbit_boot_step(
{rabbit_exchange_type_consistent_hash_mnesia,
    [{description, "rabbitmq stream: shared state"},
        {mfa, {?MODULE, init_mnesia_tables, []}},
        {requires, database},
        {enables, external_infrastructure}]}).

init_mnesia_tables() ->
    mnesia:create_table(?TABLE,
        [{attributes, record_info(fields, ?MODULE)},
            {type, set}]),
    mnesia:add_table_copy(?TABLE, node(), ram_copies),
    mnesia:wait_for_tables([?TABLE], 30000).

start_link(Conf) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Conf], []).

init([Conf]) ->
    GetAllRecords = fun() ->
        mnesia:foldl(fun(Record, Acc) ->
            [Record] ++ Acc
                     end,
            [], ?TABLE)
                    end,
    Records = mnesia:activity(sync_transaction, GetAllRecords),
    [begin
         #?MODULE{
             name = Name, leader = Leader, replicas = Replicas, leader_pid = LeaderPidInRecord
         } = Record,
         error_logger:info_msg("Loading from database ~p~n", [Record]),
         case node() of
             Leader ->
                 case is_process_alive(LeaderPidInRecord) of
                     true ->
                         error_logger:info_msg("Process still alive, doing nothing~n"),
                         ok;
                     false ->
                         error_logger:info_msg("Starting the Osiris cluster~n"),
                         Reference = binary_to_list(Name),

                         OsirisConf = #{leader_node => Leader,
                             reference => Reference, name => Reference,
                             replica_nodes => Replicas},

                         {ok, #{leader_pid := LeaderPid}} = osiris:start_cluster(OsirisConf),

                         error_logger:info_msg("New PID for ~p is ~p~n", [Name, LeaderPid]),

                         UpdateFunction = fun() ->
                             mnesia:write(Record#?MODULE{leader_pid = LeaderPid}) end,
                         mnesia:activity(sync_transaction, UpdateFunction)
                 end;
             _ ->
                 error_logger:info_msg("Node not leader, not starting it~n"),
                 ok
         end
     end || Record <- Records],

    {ok, #state{configuration = Conf, listeners = [], monitors = #{}}}.

create(Reference) ->
    gen_server:call(?MODULE, {create, Reference}).

delete(Reference) ->
    gen_server:call(?MODULE, {delete, Reference}).

register() ->
    gen_server:call(?MODULE, {register, self()}).

unregister() ->
    gen_server:call(?MODULE, {unregister, self()}).

lookup(Target) ->
    gen_server:call(?MODULE, {lookup, Target}).

replicas_for_current_node() ->
    rabbit_mnesia:cluster_nodes(all) -- [node()].

read(Name) ->
    mnesia:activity(sync_transaction, fun() -> mnesia:read({?TABLE, Name}) end).

handle_call({create, Reference}, _From, State) ->
    Key = list_to_binary(Reference),
    case read(Key) of
        [] ->
            LeaderNode = node(),
            Replicas = replicas_for_current_node(),
            error_logger:info_msg("Creating ~p cluster on ~p with replica(s) ~p~n", [Key, LeaderNode, Replicas]),
            OsirisConf = #{leader_node => node(),
                reference => Reference, name => Reference,
                replica_nodes => Replicas},
            {ok, #{leader_pid := LeaderPid}} = Res = osiris:start_cluster(OsirisConf),
            ClusterRecord = #?MODULE{name = Key, leader_pid = LeaderPid, leader = LeaderNode, replicas = Replicas},
            F = fun() ->
                mnesia:write(ClusterRecord)
                end,
            mnesia:activity(sync_transaction, F),
            {reply, Res, State};
        [_ClusterRecord] ->
            {reply, {error, reference_already_exists}, State}
    end;
handle_call({delete, Reference}, _From, #state{listeners = Listeners} = State) ->
    Key = list_to_binary(Reference),
    case read(Key) of
        [] ->
            {reply, {error, reference_not_found}, State};
        [ClusterRecord] ->
            Conf = #{
                name => Reference,
                reference => Reference,
                replica_nodes => ClusterRecord#?MODULE.replicas,
                leader_pid => ClusterRecord#?MODULE.leader_pid,
                leader_node => ClusterRecord#?MODULE.leader},
            ok = osiris:delete_cluster(Conf),
            [Pid ! {stream_manager, cluster_deleted, Reference} || Pid <- Listeners],
            F = fun() ->
                mnesia:delete({?TABLE, Key})
                end,
            mnesia:activity(sync_transaction, F),
            {reply, {ok, deleted}, State}
    end;
handle_call({register, Pid}, _From, #state{listeners = Listeners, monitors = Monitors} = State) ->
    case lists:member(Pid, Listeners) of
        false ->
            MonitorRef = erlang:monitor(process, Pid),
            {reply, ok, State#state{listeners = [Pid | Listeners], monitors = Monitors#{Pid => MonitorRef}}};
        true ->
            {reply, ok, State}
    end;
handle_call({unregister, Pid}, _From, #state{listeners = Listeners, monitors = Monitors} = State) ->
    Monitors1 = case maps:get(Pid, Monitors, undefined) of
                    undefined ->
                        Monitors;
                    MonitorRef ->
                        erlang:demonitor(MonitorRef, [flush]),
                        maps:remove(Pid, Monitors)
                end,
    {reply, ok, State#state{listeners = lists:delete(Pid, Listeners), monitors = Monitors1}};
handle_call({lookup, Target}, _From, State) ->
    Res = case read(Target) of
              [] ->
                  cluster_not_found;
              [#?MODULE{leader_pid = LeaderPid}] ->
                  LeaderPid
          end,
    {reply, Res, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info({'DOWN', _MonitorRef, process, Pid, _Info}, #state{listeners = Listeners, monitors = Monitors} = State) ->
    {noreply, State#state{listeners = lists:delete(Pid, Listeners), monitors = maps:remove(Pid, Monitors)}};
handle_info(Info, State) ->
    error_logger:info_msg("Received info ~p~n", [Info]),
    {noreply, State}.