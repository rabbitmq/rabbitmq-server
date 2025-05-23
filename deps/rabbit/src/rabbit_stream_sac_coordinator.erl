%% The contents of this file are subject to the Mozilla Public License
%% Version 2.0 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/en-US/MPL/2.0/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is Pivotal Software, Inc.
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_stream_sac_coordinator).

-include("rabbit_stream_sac_coordinator.hrl").

-opaque command() :: #command_register_consumer{} |
                     #command_unregister_consumer{} |
                     #command_activate_consumer{} |
                     #command_connection_reconnected{} |
                     #command_purge_nodes{} |
                     #command_update_conf{}.

-opaque state() :: #?MODULE{}.

-export_type([state/0,
              command/0]).

%% Single Active Consumer API
-export([register_consumer/7,
         unregister_consumer/5,
         activate_consumer/3,
         consumer_groups/2,
         group_consumers/4,
         connection_reconnected/1]).
-export([apply/2,
         init_state/0,
         send_message/2,
         ensure_monitors/4,
         handle_connection_down/2,
         handle_connection_node_disconnected/2,
         handle_node_reconnected/3,
         forget_connection/2,
         consumer_groups/3,
         group_consumers/5,
         overview/1,
         import_state/2,
         check_conf_change/1]).
-export([make_purge_nodes/1,
         make_update_conf/1]).

%% exported for unit tests only
-ifdef(TEST).
-export([compute_pid_group_dependencies/1]).
-endif.

-import(rabbit_stream_coordinator, [ra_local_query/1]).

-define(ACTIVE, active).
-define(WAITING, waiting).
-define(DEACTIVATING, deactivating).

-define(CONNECTED, connected).
-define(DISCONNECTED, disconnected).
-define(FORGOTTTEN, forgotten).

-define(CONN_ACT, {?CONNECTED, ?ACTIVE}).
-define(CONN_WAIT, {?CONNECTED, ?WAITING}).
-define(DISCONN_ACT, {?DISCONNECTED, ?ACTIVE}).
-define(FORG_ACT, {?FORGOTTTEN, ?ACTIVE}).

-define(DISCONNECTED_TIMEOUT_APP_KEY, stream_sac_disconnected_timeout).
-define(DISCONNECTED_TIMEOUT_CONF_KEY, disconnected_timeout).
-define(DISCONNECTED_TIMEOUT_MS, 60_000).

%% Single Active Consumer API
-spec register_consumer(binary(),
                        binary(),
                        integer(),
                        binary(),
                        pid(),
                        binary(),
                        integer()) ->
                           {ok, boolean()} | {error, term()}.
register_consumer(VirtualHost,
                  Stream,
                  PartitionIndex,
                  ConsumerName,
                  ConnectionPid,
                  Owner,
                  SubscriptionId) ->
    process_command(#command_register_consumer{vhost = VirtualHost,
                                               stream = Stream,
                                               partition_index = PartitionIndex,
                                               consumer_name = ConsumerName,
                                               connection_pid = ConnectionPid,
                                               owner = Owner,
                                               subscription_id = SubscriptionId}).

-spec unregister_consumer(binary(),
                          binary(),
                          binary(),
                          pid(),
                          integer()) ->
                             ok | {error, term()}.
unregister_consumer(VirtualHost,
                    Stream,
                    ConsumerName,
                    ConnectionPid,
                    SubscriptionId) ->
    process_command(#command_unregister_consumer{vhost = VirtualHost,
                                                 stream = Stream,
                                                 consumer_name = ConsumerName,
                                                 connection_pid = ConnectionPid,
                                                 subscription_id = SubscriptionId}).

-spec activate_consumer(binary(), binary(), binary()) -> ok.
activate_consumer(VH, Stream, Name) ->
    process_command(#command_activate_consumer{vhost =VH,
                                               stream = Stream,
                                               consumer_name= Name}).

-spec connection_reconnected(connection_pid()) -> ok.
connection_reconnected(Pid) ->
    process_command(#command_connection_reconnected{pid = Pid}).

process_command(Cmd) ->
    case rabbit_stream_coordinator:process_command(wrap_cmd(Cmd)) of
        {ok, Res, _} ->
            Res;
        {error, _} = Err ->
            rabbit_log:warning("SAC coordinator command ~tp returned error ~tp",
                               [Cmd, Err]),
            Err
    end.

-spec wrap_cmd(command()) -> {sac, command()}.
wrap_cmd(Cmd) ->
    {sac, Cmd}.

%% return the current groups for a given virtual host
-spec consumer_groups(binary(), [atom()]) ->
                         {ok,
                          [term()] | {error, atom()}}.
consumer_groups(VirtualHost, InfoKeys) ->
    case ra_local_query(fun(State) ->
                                SacState =
                                rabbit_stream_coordinator:sac_state(State),
                                consumer_groups(VirtualHost,
                                                InfoKeys,
                                                SacState)
                        end)
    of
        {ok, {_, Result}, _} -> Result;
        {error, noproc} ->
            %% not started yet, so no groups
            {ok, []};
        {error, _} = Err -> Err;
        {timeout, _} -> {error, timeout}
    end.

%% get the consumers of a given group in a given virtual host
-spec group_consumers(binary(), binary(), binary(), [atom()]) ->
                         {ok, [term()]} |
                         {error, atom()}.
group_consumers(VirtualHost, Stream, Reference, InfoKeys) ->
    case ra_local_query(fun(State) ->
                                SacState =
                                rabbit_stream_coordinator:sac_state(State),
                                group_consumers(VirtualHost,
                                                Stream,
                                                Reference,
                                                InfoKeys,
                                                SacState)
                        end)
    of
        {ok, {_, {ok, _} = Result}, _} -> Result;
        {ok, {_, {error, _} = Err}, _} -> Err;
        {error, noproc} ->
            %% not started yet, so the group cannot exist
            {error, not_found};
        {error, _} = Err -> Err;
        {timeout, _} -> {error, timeout}
    end.

-spec overview(state()) -> map().
overview(undefined) ->
    undefined;
overview(#?MODULE{groups = Groups}) ->
    GroupsOverview =
        maps:map(fun(_,
                     #group{consumers = Consumers, partition_index = Idx}) ->
                    #{num_consumers => length(Consumers),
                      partition_index => Idx}
                 end,
                 Groups),
    #{num_groups => map_size(Groups), groups => GroupsOverview}.

-spec init_state() -> state().
init_state() ->
    DisconTimeout = ?DISCONNECTED_TIMEOUT_MS,
    #?MODULE{groups = #{}, pids_groups = #{},
             conf = #{?DISCONNECTED_TIMEOUT_CONF_KEY => DisconTimeout}}.

-spec apply(command(), state()) ->
               {state(), term(), ra_machine:effects()}.
apply(#command_register_consumer{vhost = VirtualHost,
                                 stream = Stream,
                                 partition_index = PartitionIndex,
                                 consumer_name = ConsumerName,
                                 connection_pid = ConnectionPid,
                                 owner = Owner,
                                 subscription_id = SubscriptionId},
      #?MODULE{groups = StreamGroups0} = State) ->
    case maybe_create_group(VirtualHost,
                           Stream,
                           PartitionIndex,
                           ConsumerName,
                           StreamGroups0) of
        {ok, StreamGroups1} ->
            do_register_consumer(VirtualHost,
                                 Stream,
                                 PartitionIndex,
                                 ConsumerName,
                                 ConnectionPid,
                                 Owner,
                                 SubscriptionId,
                                 State#?MODULE{groups = StreamGroups1});
        {error, Error} ->
            {State, {error, Error}, []}
    end;
apply(#command_unregister_consumer{vhost = VirtualHost,
                                   stream = Stream,
                                   consumer_name = ConsumerName,
                                   connection_pid = ConnectionPid,
                                   subscription_id = SubscriptionId},
      #?MODULE{groups = StreamGroups0} = State0) ->
    {State1, Effects1} =
        case lookup_group(VirtualHost, Stream, ConsumerName, StreamGroups0) of
            undefined ->
                {State0, []};
            Group0 ->
                {Group1, Effects} =
                    case lookup_consumer(ConnectionPid, SubscriptionId, Group0)
                    of
                        {value, Consumer} ->
                            G1 = remove_from_group(Consumer, Group0),
                            handle_consumer_removal(
                              G1, Stream, ConsumerName,
                              is_active(Consumer#consumer.status));
                        false ->
                            {Group0, []}
                    end,
                SGS = update_groups(VirtualHost,
                                    Stream,
                                    ConsumerName,
                                    Group1,
                                    StreamGroups0),
                {State0#?MODULE{groups = SGS}, Effects}
        end,
    {State1, ok, Effects1};
apply(#command_activate_consumer{vhost = VirtualHost,
                                 stream = Stream,
                                 consumer_name = ConsumerName},
      #?MODULE{groups = StreamGroups0} = State0) ->
    {G, Eff} =
        case lookup_group(VirtualHost, Stream, ConsumerName, StreamGroups0) of
            undefined ->
                rabbit_log:warning("Trying to activate consumer in group ~tp, but "
                                   "the group does not longer exist",
                                   [{VirtualHost, Stream, ConsumerName}]),
                {undefined, []};
            G0 ->
                %% keep track of the former active, if any
                {ActPid, ActSubId} =
                case lookup_active_consumer(G0) of
                    {value, #consumer{pid = ActivePid,
                                      subscription_id = ActiveSubId}} ->
                        {ActivePid, ActiveSubId};
                    _ ->
                        {-1, -1}
                end,
                G1 = update_connected_consumers(G0, ?CONN_WAIT),
                case evaluate_active_consumer(G1) of
                    undefined ->
                        {G1, []};
                    #consumer{status = {?DISCONNECTED, _}} ->
                        %% we keep it this way, the consumer may come back
                        {G1, []};
                    #consumer{pid = Pid, subscription_id = SubId} ->
                        G2 = update_consumer_state_in_group(G1, Pid,
                                                            SubId,
                                                            ?CONN_ACT),
                        %% do we need effects or not?
                        Effects =
                        case {Pid, SubId} of
                            {ActPid, ActSubId} ->
                                %% it is the same active consumer as before
                                %% no need to notify it
                                [];
                            _ ->
                                %% new active consumer, need to notify it
                                [notify_consumer_effect(Pid, SubId, Stream,
                                                        ConsumerName, true)]
                        end,
                        {G2, Effects}
                end
        end,
    StreamGroups1 = update_groups(VirtualHost, Stream, ConsumerName,
                                  G, StreamGroups0),
    {State0#?MODULE{groups = StreamGroups1}, ok, Eff};
apply(#command_connection_reconnected{pid = Pid},
      #?MODULE{groups = Groups0} = State0) ->
    {State1, Eff} =
        maps:fold(fun(G, _, {St, Eff}) ->
                          handle_group_connection_reconnected(Pid, St, Eff, G)
                  end, {State0, []}, Groups0),

    {State1, ok, Eff};
apply(#command_purge_nodes{nodes = Nodes}, State0) ->
    {State1, Eff} = lists:foldl(fun(N, {S0, Eff0}) ->
                                        {S1, Eff1} = purge_node(N, S0),
                                        {S1, Eff1 ++ Eff0}
                                end, {State0, []}, Nodes),
    {State1, ok, Eff};
apply(#command_update_conf{conf = NewConf}, State) ->
    {State#?MODULE{conf = NewConf}, ok, []}.

purge_node(Node, #?MODULE{groups = Groups0} = State0) ->
    PidsGroups = compute_node_pid_group_dependencies(Node, Groups0),
    maps:fold(fun(Pid, Groups, {S0, Eff0}) ->
                      {S1, Eff1} = handle_connection_down0(Pid, S0, Groups),
                      {S1, Eff1 ++ Eff0}
              end, {State0, []}, PidsGroups).

handle_group_connection_reconnected(Pid, #?MODULE{groups = Groups0} = S0,
                                    Eff0, {VH, S, Name} = K) ->
    case lookup_group(VH, S, Name, Groups0) of
        undefined ->
            {S0, Eff0};
        Group ->
            case has_forgotten_active(Group, Pid) of
                true ->
                    %% a forgotten active is coming in the connection
                    %% we need to reconcile the group,
                    %% as there may have been 2 active consumers at a time
                    handle_forgotten_active_reconnected(Pid, S0, Eff0, K);
                false ->
                    do_handle_group_connection_reconnected(Pid, S0, Eff0, K)
            end
    end.

do_handle_group_connection_reconnected(Pid, #?MODULE{groups = Groups0} = S0,
                                       Eff0, {VH, S, Name} = K) ->
    G0 = #group{consumers = Consumers0} = lookup_group(VH, S, Name, Groups0),
    {Consumers1, Updated} =
    lists:foldr(
      fun(#consumer{pid = P, status = {_, St}} = C, {L, _})
            when P == Pid ->
              {[C#consumer{status = {?CONNECTED, St}} | L], true};
         (C, {L, UpdatedFlag}) ->
              {[C | L], UpdatedFlag or false}
      end, {[], false}, Consumers0),

    case Updated of
        true ->
            G1 = G0#group{consumers = Consumers1},
            {G2, Eff} = maybe_rebalance_group(G1, K),
            Groups1 = update_groups(VH, S, Name, G2, Groups0),
            {S0#?MODULE{groups = Groups1}, Eff ++ Eff0};
        false ->
            {S0, Eff0}
    end.

handle_forgotten_active_reconnected(Pid,
                                    #?MODULE{groups = Groups0} = S0,
                                    Eff0, {VH, S, Name}) ->
    G0 = #group{consumers = Consumers0} = lookup_group(VH, S, Name, Groups0),
    {Consumers1, Eff1} =
    case has_disconnected_active(G0) of
        true ->
            %% disconnected active consumer in the group, no rebalancing possible
            %% we update the disconnected active consumers
            %% and tell them to step down
            lists:foldr(fun(#consumer{status = St,
                                      pid = P,
                                      subscription_id = SID} = C, {Cs, Eff})
                              when P =:= Pid andalso St =:= ?FORG_ACT ->
                                {[C#consumer{status = ?CONN_WAIT} | Cs],
                                 [notify_consumer_effect(Pid, SID, S,
                                                         Name, false, true) | Eff]};
                           (C, {Cs, Eff}) ->
                                {[C | Cs], Eff}
                        end, {[], Eff0}, Consumers0);
        false ->
            lists:foldr(fun(#consumer{status = St,
                                      pid = P,
                                      subscription_id = SID} = C, {Cs, Eff})
                              when P =:= Pid andalso St =:= ?FORG_ACT ->
                                %% update forgotten active
                                %% tell it to step down
                                {[C#consumer{status = ?CONN_WAIT} | Cs],
                                 [notify_consumer_effect(P, SID, S,
                                                         Name, false, true) | Eff]};
                           (#consumer{status = {?FORGOTTTEN, _},
                                      pid = P} = C, {Cs, Eff})
                              when P =:= Pid ->
                                %% update forgotten
                                {[C#consumer{status = ?CONN_WAIT} | Cs], Eff};
                           (#consumer{status = ?CONN_ACT,
                                      pid = P,
                                      subscription_id = SID} = C, {Cs, Eff}) ->
                                %% update connected active
                                %% tell it to step down
                                {[C#consumer{status = ?CONN_WAIT} | Cs],
                                 [notify_consumer_effect(P, SID, S,
                                                         Name, false, true) | Eff]};
                           (C, {Cs, Eff}) ->
                                {[C | Cs], Eff}
                        end, {[], Eff0}, Consumers0)
    end,
    G1 = G0#group{consumers = Consumers1},
    Groups1 = update_groups(VH, S, Name, G1, Groups0),
    {S0#?MODULE{groups = Groups1}, Eff1}.

has_forgotten_active(#group{consumers = Consumers}, Pid) ->
    case lists:search(fun(#consumer{status = ?FORG_ACT,
                                    pid = P}) when P =:= Pid ->
                              true;
                         (_) -> false
                      end, Consumers) of
        false ->
            false;
        _ ->
            true
    end.

has_disconnected_active(Group) ->
    has_consumer_with_status(Group, ?DISCONN_ACT).

has_consumer_with_status(#group{consumers = Consumers}, Status) ->
    case lists:search(fun(#consumer{status = S}) when S =:= Status ->
                              true;
                         (_) -> false
                      end, Consumers) of
        false ->
            false;
        _ ->
            true
    end.



maybe_rebalance_group(#group{partition_index = -1, consumers = Consumers0} = G0,
                      {_VH, S, Name}) ->
    case lookup_active_consumer(G0) of
        {value, ActiveConsumer} ->
            %% there is already an active consumer, we just re-arrange
            %% the group to make sure the active consumer is the first in the array
            Consumers1 = lists:filter(fun(C) ->
                                              not same_consumer(C, ActiveConsumer)
                                      end, Consumers0),
            G1 = G0#group{consumers = [ActiveConsumer | Consumers1]},
            {G1, []};
        _ ->
            %% no active consumer
            G1 = compute_active_consumer(G0),
            case lookup_active_consumer(G1) of
                {value, #consumer{pid = Pid, subscription_id = SubId}} ->
                    %% creating the side effect to notify the new active consumer
                    {G1, [notify_consumer_effect(Pid, SubId, S, Name, true)]};
                _ ->
                    %% no active consumer found in the group, nothing to do
                    {G1, []}
            end
    end;
maybe_rebalance_group(#group{partition_index = _, consumers = Consumers} = G,
                      {_VH, S, Name}) ->
    case lookup_active_consumer(G) of
        {value, #consumer{pid = ActPid,
                          subscription_id = ActSubId} = CurrentActive} ->
            case evaluate_active_consumer(G) of
                undefined ->
                    %% no-one to select
                    {G, []};
                CurrentActive ->
                    %% the current active stays the same
                    {G, []};
                _ ->
                    %% there's a change, telling the active it's not longer active
                    {update_consumer_state_in_group(G,
                                                    ActPid,
                                                    ActSubId,
                                                    {?CONNECTED, ?DEACTIVATING}),
                     [notify_consumer_effect(ActPid,
                                             ActSubId,
                                             S,
                                             Name,
                                             false,
                                             true)]}
            end;
        false ->
            %% no active consumer in the (non-empty) group,
            case lists:search(fun(#consumer{status = Status}) ->
                                      Status =:= {?CONNECTED, ?DEACTIVATING}
                              end, Consumers) of
                {value, _Deactivating} ->
                    %%  we are waiting for the reply of a former active
                    %%  nothing to do
                    {G, []};
                _ ->
                    %% nothing going on in the group
                    %% a {disconnected, active} may have become {forgotten, active}
                    %% we must select a new active
                    case evaluate_active_consumer(G) of
                        undefined ->
                            %% no-one to select
                            {G, []};
                        #consumer{pid = ActPid, subscription_id = ActSubId} ->
                            {update_consumer_state_in_group(G,
                                                            ActPid,
                                                            ActSubId,
                                                            {?CONNECTED, ?ACTIVE}),
                             [notify_consumer_effect(ActPid,
                                                     ActSubId,
                                                     S,
                                                     Name,
                                                     true)]}
                    end
            end
    end.

-spec consumer_groups(binary(), [atom()], state()) -> {ok, [term()]}.
consumer_groups(VirtualHost, InfoKeys, #?MODULE{groups = Groups}) ->
    Res = maps:fold(fun ({VH, Stream, Reference},
                         #group{consumers = Consumers,
                                partition_index = PartitionIndex},
                         Acc)
                            when VH == VirtualHost ->
                            Record =
                                lists:foldr(fun (stream, RecAcc) ->
                                                    [{stream, Stream} | RecAcc];
                                                (reference, RecAcc) ->
                                                    [{reference, Reference}
                                                     | RecAcc];
                                                (partition_index, RecAcc) ->
                                                    [{partition_index,
                                                      PartitionIndex}
                                                     | RecAcc];
                                                (consumers, RecAcc) ->
                                                    [{consumers,
                                                      length(Consumers)}
                                                     | RecAcc];
                                                (Unknown, RecAcc) ->
                                                    [{Unknown, unknown_field}
                                                     | RecAcc]
                                            end,
                                            [], InfoKeys),
                            [Record | Acc];
                        (_GroupId, _Group, Acc) ->
                            Acc
                    end,
                    [], Groups),
    {ok, lists:reverse(Res)}.

-spec group_consumers(binary(),
                      binary(),
                      binary(),
                      [atom()],
                      state()) ->
                         {ok, [term()]} | {error, not_found}.
group_consumers(VirtualHost,
                Stream,
                Reference,
                InfoKeys,
                #?MODULE{groups = Groups}) ->
    GroupId = {VirtualHost, Stream, Reference},
    case Groups of
        #{GroupId := #group{consumers = Consumers}} ->
            Cs = lists:foldr(fun(#consumer{subscription_id = SubId,
                                           owner = Owner,
                                           status = Status},
                                 Acc) ->
                                Record =
                                    lists:foldr(fun (subscription_id, RecAcc) ->
                                                        [{subscription_id,
                                                          SubId}
                                                         | RecAcc];
                                                    (connection_name, RecAcc) ->
                                                        [{connection_name,
                                                          Owner}
                                                         | RecAcc];
                                                    (state, RecAcc) ->
                                                        [{state, cli_consumer_status_label(Status)}
                                                         | RecAcc];
                                                    (Unknown, RecAcc) ->
                                                        [{Unknown,
                                                          unknown_field}
                                                         | RecAcc]
                                                end,
                                                [], InfoKeys),
                                [Record | Acc]
                             end,
                             [], Consumers),
            {ok, Cs};
        _ ->
            {error, not_found}
    end.

cli_consumer_status_label({?FORGOTTTEN, _}) ->
   inactive;
cli_consumer_status_label({_, ?ACTIVE}) ->
   active;
cli_consumer_status_label(_) ->
   inactive.

-spec ensure_monitors(command(),
                      state(),
                      map(),
                      ra_machine:effects()) ->
                         {state(), map(), ra_machine:effects()}.
ensure_monitors(#command_register_consumer{vhost = VirtualHost,
                                           stream = Stream,
                                           consumer_name = ConsumerName,
                                           connection_pid = Pid},
                #?MODULE{pids_groups = PidsGroups0} = State0,
                Monitors0,
                Effects) ->
    GroupId = {VirtualHost, Stream, ConsumerName},
    Groups0 = maps:get(Pid, PidsGroups0, #{}),
    PidsGroups1 =
        maps:put(Pid, maps:put(GroupId, true, Groups0), PidsGroups0),
    {State0#?MODULE{pids_groups = PidsGroups1}, Monitors0#{Pid => sac},
     [{monitor, process, Pid}, {monitor, node, node(Pid)} | Effects]};
ensure_monitors(#command_unregister_consumer{vhost = VirtualHost,
                                             stream = Stream,
                                             consumer_name = ConsumerName,
                                             connection_pid = Pid},
                #?MODULE{groups = StreamGroups0, pids_groups = PidsGroups0} =
                    State0,
                Monitors,
                Effects)
    when is_map_key(Pid, PidsGroups0) ->
    GroupId = {VirtualHost, Stream, ConsumerName},
    #{Pid := PidGroup0} = PidsGroups0,
    PidGroup1 =
        case lookup_group(VirtualHost, Stream, ConsumerName, StreamGroups0) of
            undefined ->
                %% group is gone, can be removed from the PID map
                maps:remove(GroupId, PidGroup0);
            Group ->
                %% group still exists, check if other consumers are from this PID
                %% if yes, don't change the PID set
                %% if no, remove group from PID set
                case has_consumers_from_pid(Group, Pid) of
                    true ->
                        %% the group still depends on this PID, keep the group entry in the set
                        PidGroup0;
                    false ->
                        %% the group does not depend on the PID anymore, remove the group entry from the map
                        maps:remove(GroupId, PidGroup0)
                end
        end,
    case maps:size(PidGroup1) == 0 of
        true ->
            %% no more groups depend on the PID
            %% remove PID from data structure and demonitor it
            {State0#?MODULE{pids_groups = maps:remove(Pid, PidsGroups0)},
             maps:remove(Pid, Monitors), [{demonitor, process, Pid} | Effects]};
        false ->
            %% one or more groups still depend on the PID
            {State0#?MODULE{pids_groups =
                                maps:put(Pid, PidGroup1, PidsGroups0)},
             Monitors, Effects}
    end;
ensure_monitors(#command_connection_reconnected{pid = Pid},
                #?MODULE{pids_groups = PidsGroups,
                         groups = Groups} = State,
                Monitors,
                Effects)
  when not is_map_key(Pid, Monitors) orelse
       not is_map_key(Pid, PidsGroups) ->
    %% the connection PID should be monitored
    %% the inconsistency can happen when a forgotten connection comes back,
    %% we must re-compute the connection PID / group dependency mapping
    %% and re-issue the monitor
    AllPidsGroups = compute_pid_group_dependencies(Groups),
    {State#?MODULE{pids_groups = AllPidsGroups},
     Monitors#{Pid => sac},
     [{monitor, process, Pid}, {monitor, node, node(Pid)} | Effects]};
ensure_monitors(#command_purge_nodes{},
                #?MODULE{groups = Groups} = State,
                Monitors,
                Effects) ->
    AllPidsGroups = compute_pid_group_dependencies(Groups),
    {State#?MODULE{pids_groups = AllPidsGroups},
     Monitors,
     Effects};
ensure_monitors(_, #?MODULE{} = State0, Monitors, Effects) ->
    {State0, Monitors, Effects}.

-spec handle_connection_down(connection_pid(), state()) ->
                                {state(), ra_machine:effects()}.
handle_connection_down(Pid,
                       #?MODULE{pids_groups = PidsGroups0} = State0) ->
    case maps:take(Pid, PidsGroups0) of
        error ->
            {State0, []};
        {Groups, PidsGroups1} ->
            State1 = State0#?MODULE{pids_groups = PidsGroups1},
            handle_connection_down0(Pid, State1, Groups)
    end.

handle_connection_down0(Pid, State, Groups) ->
    maps:fold(fun(G, _, Acc) ->
                      handle_group_after_connection_down(Pid, Acc, G)
              end, {State, []}, Groups).

-spec handle_connection_node_disconnected(connection_pid(), state()) ->
    {state(), ra_machine:effects()}.
handle_connection_node_disconnected(ConnPid,
                                    #?MODULE{pids_groups = PidsGroups0} = State0) ->
    case maps:take(ConnPid, PidsGroups0) of
        error ->
            {State0, []};
        {Groups, PidsGroups1} ->
            State1 = State0#?MODULE{pids_groups = PidsGroups1},
            State2 =
                maps:fold(fun(G, _, Acc) ->
                                  handle_group_after_connection_node_disconnected(
                                    ConnPid, Acc, G)
                          end, State1, Groups),
            T = disconnected_timeout(State2),
            {State2, [{timer, {sac, node_disconnected,
                               #{connection_pid => ConnPid}}, T}]}
    end.
    
-spec handle_node_reconnected(node(), state(), ra_machine:effects()) ->
    {state(), ra_machine:effects()}.
handle_node_reconnected(Node,
                        #?MODULE{pids_groups = PidsGroups0,
                                 groups = Groups0} = State0,
                        Effects0) ->
    NodePidsGroups = compute_node_pid_group_dependencies(Node, Groups0),
    PidsGroups1 = maps:merge(PidsGroups0, NodePidsGroups),
    Effects1 =
        lists:foldr(fun(P, Acc) ->
                            [notify_connection_effect(P),
                             {monitor, process, P} | Acc]
                    end, Effects0, maps:keys(NodePidsGroups)),

    {State0#?MODULE{pids_groups = PidsGroups1}, Effects1}.

-spec forget_connection(connection_pid(), state()) ->
    {state(), ra_machine:effects()}.
forget_connection(Pid, #?MODULE{groups = Groups} = State0) ->
    {State1, Eff} =
        maps:fold(fun(G, _, {St, Eff}) ->
                          handle_group_forget_connection(Pid, St, Eff, G)
                  end, {State0, []}, Groups),
    {State1, Eff}.

handle_group_forget_connection(Pid, #?MODULE{groups = Groups0} = S0,
                               Eff0, {VH, S, Name} = K) ->
    case lookup_group(VH, S, Name, Groups0) of
        undefined ->
            {S0, Eff0};
        #group{consumers = Consumers0} = G0 ->
            {Consumers1, Updated} =
                lists:foldr(
                  fun(#consumer{pid = P, status = {?DISCONNECTED, St}} = C, {L, _})
                        when P == Pid ->
                          {[C#consumer{status = {?FORGOTTTEN, St}} | L], true};
                     (C, {L, UpdatedFlag}) ->
                          {[C | L], UpdatedFlag or false}
                  end, {[], false}, Consumers0),

            case Updated of
                true ->
                    G1 = G0#group{consumers = Consumers1},
                    {G2, Eff} = maybe_rebalance_group(G1, K),
                    Groups1 = update_groups(VH, S, Name, G2, Groups0),
                    {S0#?MODULE{groups = Groups1}, Eff ++ Eff0};
                false ->
                    {S0, Eff0}
            end
    end.

handle_group_after_connection_down(Pid,
                                   {#?MODULE{groups = Groups0} = S0, Eff0},
                                   {VirtualHost, Stream, ConsumerName}) ->
    case lookup_group(VirtualHost,
                      Stream,
                      ConsumerName,
                      Groups0) of
        undefined ->
            {S0, Eff0};
        #group{consumers = Consumers0} = G0 ->
            %% remove the connection consumers from the group state
            %% keep flags to know what happened
            {Consumers1, ActiveRemoved, AnyRemoved} =
                lists:foldl(
                  fun(#consumer{pid = P, status = S}, {L, ActiveFlag, _})
                        when P == Pid ->
                          {L, is_active(S) or ActiveFlag, true};
                     (C, {L, ActiveFlag, AnyFlag}) ->
                          {L ++ [C], ActiveFlag, AnyFlag}
                  end, {[], false, false}, Consumers0),

            case AnyRemoved of
                true ->
                    G1 = G0#group{consumers = Consumers1},
                    {G2, Effects} = handle_consumer_removal(G1, Stream,
                                                            ConsumerName,
                                                            ActiveRemoved),
                    Groups1 = update_groups(VirtualHost,
                                            Stream,
                                            ConsumerName,
                                            G2,
                                            Groups0),
                    {S0#?MODULE{groups = Groups1}, Effects ++ Eff0};
                false ->
                    {S0, Eff0}
            end
    end.

handle_group_after_connection_node_disconnected(ConnPid,
                                                #?MODULE{groups = Groups0} = S0,
                                                {VirtualHost, Stream, ConsumerName}) ->
    case lookup_group(VirtualHost,
                      Stream,
                      ConsumerName,
                      Groups0) of
        undefined ->
            S0;
        #group{consumers = Cs0} = G0 ->
            Cs1 = lists:foldr(fun(#consumer{status = {_, St},
                                            pid = Pid} = C0,
                                  Acc) when Pid =:= ConnPid ->
                                      C1 = C0#consumer{status = {?DISCONNECTED, St}},
                                      [C1 | Acc];
                                 (C, Acc) ->
                                      [C | Acc]
                              end, [], Cs0),
            G1 = G0#group{consumers = Cs1},
            Groups1 = update_groups(VirtualHost,
                                    Stream,
                                    ConsumerName,
                                    G1,
                                    Groups0),
            S0#?MODULE{groups = Groups1}
    end.

-spec import_state(ra_machine:version(), map()) -> state().
import_state(4, #{<<"groups">> := Groups, <<"pids_groups">> := PidsGroups}) ->
    #?MODULE{groups = map_to_groups(Groups),
             pids_groups = map_to_pids_groups(PidsGroups),
             conf = #{disconnected_timeout => ?DISCONNECTED_TIMEOUT_MS}}.

-spec check_conf_change(state()) -> {new, conf()} | unchanged.
check_conf_change(#?MODULE{conf = Conf}) ->
    DisconTimeout = lookup_disconnected_timeout(),
    case Conf of
        #{?DISCONNECTED_TIMEOUT_CONF_KEY := DT}
          when DT /= DisconTimeout ->
            {new, #{?DISCONNECTED_TIMEOUT_CONF_KEY => DisconTimeout}};
        C when is_map_key(?DISCONNECTED_TIMEOUT_CONF_KEY, C) == false ->
            {new, #{?DISCONNECTED_TIMEOUT_CONF_KEY => DisconTimeout}};
        _ ->
            unchanged
    end.

- spec make_purge_nodes([node()]) -> {sac, command()}.
make_purge_nodes(Nodes) ->
    wrap_cmd(#command_purge_nodes{nodes = Nodes}).

- spec make_update_conf(conf()) -> {sac, command()}.
make_update_conf(Conf) ->
    wrap_cmd(#command_update_conf{conf = Conf}).

lookup_disconnected_timeout() ->
    application:get_env(rabbit, ?DISCONNECTED_TIMEOUT_APP_KEY,
                        ?DISCONNECTED_TIMEOUT_MS).

disconnected_timeout(#?MODULE{conf = #{?DISCONNECTED_TIMEOUT_CONF_KEY := T}}) ->
    T;
disconnected_timeout(_) ->
    ?DISCONNECTED_TIMEOUT_MS.

map_to_groups(Groups) when is_map(Groups) ->
    maps:fold(fun(K, V, Acc) ->
                      Acc#{K => map_to_group(V)}
              end, #{}, Groups);
map_to_groups(_) ->
    #{}.

map_to_pids_groups(PidsGroups) when is_map(PidsGroups) ->
    PidsGroups;
map_to_pids_groups(_) ->
    #{}.

map_to_group(#{<<"consumers">> := Consumers, <<"partition_index">> := Index}) ->
    C = lists:foldl(fun(V, Acc) ->
                            Acc ++ [map_to_consumer(V)]
                    end, [], Consumers),
    #group{consumers = C,
           partition_index = Index}.

map_to_consumer(#{<<"pid">> := Pid, <<"subscription_id">> := SubId,
                  <<"owner">> := Owner, <<"active">> := Active}) ->
    #consumer{pid = Pid,
              subscription_id = SubId,
              owner = Owner,
              status = active_to_status(Active)}.

active_to_status(true) ->
    {?CONNECTED, ?ACTIVE};
active_to_status(false) ->
    {?CONNECTED, ?WAITING}.

is_active({?FORGOTTTEN, _}) ->
    false;
is_active({_, ?ACTIVE}) ->
    true;
is_active({_, ?DEACTIVATING}) ->
    true;
is_active(_) ->
    false.

do_register_consumer(VirtualHost,
                     Stream,
                     -1 = _PartitionIndex,
                     ConsumerName,
                     ConnectionPid,
                     Owner,
                     SubscriptionId,
                     #?MODULE{groups = StreamGroups0} = State) ->
    Group0 = lookup_group(VirtualHost, Stream, ConsumerName, StreamGroups0),

    Consumer =
        case lookup_active_consumer(Group0) of
            {value, _} ->
                #consumer{pid = ConnectionPid,
                          owner = Owner,
                          subscription_id = SubscriptionId,
                          status = {?CONNECTED, ?WAITING}};
            false ->
                #consumer{pid = ConnectionPid,
                          subscription_id = SubscriptionId,
                          owner = Owner,
                          status = {?CONNECTED, ?ACTIVE}}
        end,
    Group1 = add_to_group(Consumer, Group0),
    StreamGroups1 = update_groups(VirtualHost, Stream, ConsumerName,
                                  Group1,
                                  StreamGroups0),

    #consumer{status = Status} = Consumer,
    Effects =
        case Status of
            {_, ?ACTIVE} ->
                [notify_consumer_effect(ConnectionPid, SubscriptionId,
                                        Stream, ConsumerName, is_active(Status))];
            _ ->
                []
        end,

    {State#?MODULE{groups = StreamGroups1}, {ok, is_active(Status)}, Effects};
do_register_consumer(VirtualHost,
                     Stream,
                     _PartitionIndex,
                     ConsumerName,
                     ConnectionPid,
                     Owner,
                     SubscriptionId,
                     #?MODULE{groups = StreamGroups0} = State) ->
    Group0 = lookup_group(VirtualHost, Stream, ConsumerName, StreamGroups0),

    {Group1, Effects} =
        case Group0 of
            #group{consumers = []} ->
                %% first consumer in the group, it's the active one
                Consumer0 =
                    #consumer{pid = ConnectionPid,
                              owner = Owner,
                              subscription_id = SubscriptionId,
                              status = {?CONNECTED, ?ACTIVE}},
                G1 = add_to_group(Consumer0, Group0),
                {G1,
                 [notify_consumer_effect(ConnectionPid, SubscriptionId,
                                         Stream, ConsumerName, true)]};
            _G ->
                Consumer0 = #consumer{pid = ConnectionPid,
                                      owner = Owner,
                                      subscription_id = SubscriptionId,
                                      status = {?CONNECTED, ?WAITING}},
                G1 = add_to_group(Consumer0, Group0),
                maybe_rebalance_group(G1, {VirtualHost, Stream, ConsumerName})
        end,
        StreamGroups1 = update_groups(VirtualHost, Stream, ConsumerName,
                                      Group1,
                                      StreamGroups0),
    {value, #consumer{status = Status}} =
        lookup_consumer(ConnectionPid, SubscriptionId, Group1),
    {State#?MODULE{groups = StreamGroups1}, {ok, is_active(Status)}, Effects}.

handle_consumer_removal(#group{consumers = []} = G, _, _, _) ->
    {G, []};
handle_consumer_removal(#group{partition_index = -1} = Group0,
                        Stream, ConsumerName, ActiveRemoved) ->
    case ActiveRemoved of
        true ->
            %% this is the active consumer we remove, computing the new one
            Group1 = compute_active_consumer(Group0),
            case lookup_active_consumer(Group1) of
                {value, #consumer{pid = Pid, subscription_id = SubId}} ->
                    %% creating the side effect to notify the new active consumer
                    {Group1, [notify_consumer_effect(Pid, SubId, Stream, ConsumerName, true)]};
                _ ->
                    %% no active consumer found in the group, nothing to do
                    {Group1, []}
            end;
        false ->
            %% not the active consumer, nothing to do.
            {Group0, []}
    end;
handle_consumer_removal(Group0, Stream, ConsumerName, ActiveRemoved) ->
    case lookup_active_consumer(Group0) of
        {value, #consumer{pid = ActPid,
                          subscription_id = ActSubId} = CurrentActive} ->
            case evaluate_active_consumer(Group0) of
                undefined ->
                    {Group0, []};
                CurrentActive ->
                    %% the current active stays the same
                    {Group0, []};
                _ ->
                    %% there's a change, telling the active it's not longer active
                    {update_consumer_state_in_group(Group0,
                                                    ActPid,
                                                    ActSubId,
                                                    {?CONNECTED, ?DEACTIVATING}),
                     [notify_consumer_effect(ActPid, ActSubId,
                                             Stream, ConsumerName, false, true)]}
            end;
        false ->
            case ActiveRemoved of
                true ->
                    %% the active one is going away, picking a new one
                    case evaluate_active_consumer(Group0) of
                        undefined ->
                            {Group0, []};
                        #consumer{pid = P, subscription_id = SID} ->
                            {update_consumer_state_in_group(Group0, P, SID,
                                                            {?CONNECTED, ?ACTIVE}),
                             [notify_consumer_effect(P, SID,
                                                     Stream, ConsumerName, true)]}
                    end;
                false ->
                    %% no active consumer in the (non-empty) group,
                    %% we are waiting for the reply of a former active
                    {Group0, []}
            end
    end.

notify_connection_effect(Pid) ->
    mod_call_effect(Pid, {sac, check_connection, #{}}).

notify_consumer_effect(Pid, SubId, Stream, Name, Active) ->
    notify_consumer_effect(Pid, SubId, Stream, Name, Active, false).

notify_consumer_effect(Pid, SubId, Stream, Name, Active, false = _SteppingDown) ->
    mod_call_effect(Pid,
                    {sac, #{subscription_id => SubId,
                            stream => Stream,
                            consumer_name => Name,
                            active => Active}});
notify_consumer_effect(Pid, SubId, Stream, Name, Active, true = SteppingDown) ->
    mod_call_effect(Pid,
                    {sac, #{subscription_id => SubId,
                            stream => Stream,
                            consumer_name => Name,
                            active => Active,
                            stepping_down => SteppingDown}}).

maybe_create_group(VirtualHost,
                   Stream,
                   PartitionIndex,
                   ConsumerName,
                   StreamGroups) ->
    case StreamGroups of
        #{{VirtualHost, Stream, ConsumerName} := #group{partition_index = PI}}
            when PI =/= PartitionIndex ->
            {error, partition_index_conflict};
        #{{VirtualHost, Stream, ConsumerName} := _} ->
            {ok, StreamGroups};
        SGS ->
            {ok, maps:put({VirtualHost, Stream, ConsumerName},
                          #group{consumers = [], partition_index = PartitionIndex},
                          SGS)}
    end.

lookup_group(VirtualHost, Stream, ConsumerName, StreamGroups) ->
    maps:get({VirtualHost, Stream, ConsumerName}, StreamGroups,
             undefined).

add_to_group(Consumer, #group{consumers = Consumers} = Group) ->
    Group#group{consumers = Consumers ++ [Consumer]}.

remove_from_group(Consumer, #group{consumers = Consumers} = Group) ->
    Group#group{consumers = lists:delete(Consumer, Consumers)}.

has_consumers_from_pid(#group{consumers = Consumers}, Pid) ->
    lists:any(fun (#consumer{pid = P}) when P == Pid ->
                      true;
                  (_) ->
                      false
              end,
              Consumers).

compute_active_consumer(#group{partition_index = -1,
                               consumers = Crs} = Group)
    when length(Crs) == 0 ->
    Group;
compute_active_consumer(#group{partition_index = -1,
                               consumers = Consumers} = G) ->
    case lists:search(fun(#consumer{status = S}) ->
                              S =:= {?DISCONNECTED, ?ACTIVE}
                      end, Consumers) of
        {value, _DisconnectedActive} ->
            G;
        false ->
            case evaluate_active_consumer(G) of
                undefined ->
                    ok;
                #consumer{pid = Pid, subscription_id = SubId} ->
                    Consumers1 =
                        lists:foldr(
                          fun(#consumer{pid = P, subscription_id = SID} = C, L)
                                when P =:= Pid andalso SID =:= SubId ->
                                  %% change status of new active
                                  [C#consumer{status = ?CONN_ACT} | L];
                             (#consumer{status = {?CONNECTED, _}} = C, L) ->
                                  %% other connected consumers are set to "waiting"
                                  [C#consumer{status = ?CONN_WAIT} | L];
                             (C, L) ->
                                  %% other consumers stay the same
                                  [C | L]
                          end, [], Consumers),
                    G#group{consumers = Consumers1}
            end
    end.

evaluate_active_consumer(#group{consumers = Consumers})
    when length(Consumers) == 0 ->
    undefined;
evaluate_active_consumer(#group{consumers = Consumers} = G) ->
    case lists:search(fun(#consumer{status = S}) ->
                              S =:= ?DISCONN_ACT
                      end, Consumers) of
        {value, C} ->
            C;
        _ ->
            do_evaluate_active_consumer(G#group{consumers = eligible(Consumers)})
    end.

do_evaluate_active_consumer(#group{consumers = Consumers})
    when length(Consumers) == 0 ->
    undefined;
do_evaluate_active_consumer(#group{partition_index = -1,
                                consumers = [Consumer]}) ->
    Consumer;
do_evaluate_active_consumer(#group{partition_index = -1,
                                consumers = [Consumer | _]}) ->
    Consumer;
do_evaluate_active_consumer(#group{partition_index = PartitionIndex,
                                   consumers = Consumers})
    when PartitionIndex >= 0 ->
    ActiveConsumerIndex = PartitionIndex rem length(Consumers),
    lists:nth(ActiveConsumerIndex + 1, Consumers).

eligible(Consumers) ->
    lists:filter(fun(#consumer{status = {?CONNECTED, _}}) ->
                         true;
                    (_) ->
                         false
                 end, Consumers).

lookup_consumer(ConnectionPid, SubscriptionId,
                #group{consumers = Consumers}) ->
    lists:search(fun(#consumer{pid = ConnPid, subscription_id = SubId}) ->
                    ConnPid == ConnectionPid andalso SubId == SubscriptionId
                 end,
                 Consumers).

lookup_active_consumer(#group{consumers = Consumers}) ->
    lists:search(fun(#consumer{status = Status}) -> is_active(Status) end,
                 Consumers).

update_groups(_VirtualHost,
              _Stream,
              _ConsumerName,
              undefined,
              StreamGroups) ->
    StreamGroups;
update_groups(VirtualHost,
              Stream,
              ConsumerName,
              #group{consumers = []},
              StreamGroups) ->
    %% the group is now empty, removing the key
    maps:remove({VirtualHost, Stream, ConsumerName}, StreamGroups);
update_groups(VirtualHost,
              Stream,
              ConsumerName,
              Group,
              StreamGroups) ->
    maps:put({VirtualHost, Stream, ConsumerName}, Group, StreamGroups).

update_consumer_state_in_group(#group{consumers = Consumers0} = G,
                               Pid,
                               SubId,
                               NewStatus) ->
    CS1 = lists:map(fun(C0) ->
                       case C0 of
                           #consumer{pid = Pid, subscription_id = SubId} ->
                               C0#consumer{status = NewStatus};
                           C -> C
                       end
                    end,
                    Consumers0),
    G#group{consumers = CS1}.

update_connected_consumers(#group{consumers = Consumers0} = G, NewStatus) ->
    Consumers1 = lists:map(fun(#consumer{status = {?CONNECTED, _}} = C) ->
                                   C#consumer{status = NewStatus};
                              (C) ->
                                   C
                           end, Consumers0),
    G#group{consumers = Consumers1}.

mod_call_effect(Pid, Msg) ->
    {mod_call, rabbit_stream_sac_coordinator, send_message, [Pid, Msg]}.

-spec send_message(pid(), term()) -> ok.
send_message(ConnectionPid, Msg) ->
    ConnectionPid ! Msg,
    ok.

same_consumer(#consumer{pid = Pid, subscription_id = SubId},
              #consumer{pid = Pid, subscription_id = SubId}) ->
    true;
same_consumer(_, _) ->
    false.

-spec compute_pid_group_dependencies(groups()) -> pids_groups().
compute_pid_group_dependencies(Groups) ->
    maps:fold(fun(K, #group{consumers = Cs}, Acc) ->
                      lists:foldl(fun(#consumer{pid = Pid}, AccIn) ->
                                          PG0 = maps:get(Pid, AccIn, #{}),
                                          PG1 = PG0#{K => true},
                                          AccIn#{Pid => PG1}
                                  end, Acc, Cs)
              end, #{}, Groups).

-spec compute_node_pid_group_dependencies(node(), groups()) -> pids_groups().
compute_node_pid_group_dependencies(Node, Groups) ->
    maps:fold(fun(K, #group{consumers = Consumers}, Acc) ->
                      lists:foldl(fun(#consumer{pid = Pid}, AccIn)
                                        when node(Pid) =:= Node ->
                                          PG0 = maps:get(Pid, AccIn, #{}),
                                          PG1 = PG0#{K => true},
                                          AccIn#{Pid => PG1};
                                     (_, AccIn) ->
                                          AccIn
                                  end, Acc, Consumers)
              end, #{}, Groups).

