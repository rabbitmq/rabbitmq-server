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

-type sac_error() :: partition_index_conflict | not_found.

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
         handle_connection_down/3,
         handle_node_reconnected/3,
         presume_connection_down/2,
         consumer_groups/3,
         group_consumers/5,
         overview/1,
         import_state/2,
         check_conf_change/1,
         list_nodes/1,
         state_enter/2,
         is_sac_error/1
        ]).
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
-define(PDOWN, presumed_down).

-define(CONN_ACT, {?CONNECTED, ?ACTIVE}).
-define(CONN_WAIT, {?CONNECTED, ?WAITING}).
-define(DISCONN_ACT, {?DISCONNECTED, ?ACTIVE}).
-define(PDOWN_ACT, {?PDOWN, ?ACTIVE}).

-define(DISCONNECTED_TIMEOUT_APP_KEY, stream_sac_disconnected_timeout).
-define(DISCONNECTED_TIMEOUT_CONF_KEY, disconnected_timeout).
-define(DISCONNECTED_TIMEOUT_MS, 60_000).
-define(SAC_ERRORS, [partition_index_conflict, not_found]).
-define(IS_STATE_REC(T), is_record(T, ?MODULE)).
-define(IS_GROUP_REC(T), is_record(T, group)).
-define(SAME_CSR(C1, C2),
        (is_record(C1, consumer) andalso is_record(C2, consumer) andalso
         C1#consumer.pid =:= C2#consumer.pid andalso
         C1#consumer.subscription_id =:= C2#consumer.subscription_id)).

%% Single Active Consumer API
-spec register_consumer(binary(),
                        binary(),
                        integer(),
                        binary(),
                        pid(),
                        binary(),
                        integer()) ->
                           {ok, boolean()} | {error, sac_error() | term()}.
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
                             ok | {error, sac_error() | term()}.
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

-spec activate_consumer(binary(), binary(), binary()) ->
    ok | {error, sac_error() | term()}.
activate_consumer(VH, Stream, Name) ->
    process_command(#command_activate_consumer{vhost = VH,
                                               stream = Stream,
                                               consumer_name= Name}).

%% called by a stream connection to inform it is still alive
-spec connection_reconnected(connection_pid()) ->
    ok | {error, sac_error() | term()}.
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
%% (CLI command)
-spec consumer_groups(binary(), [atom()]) ->
                         {ok,
                          [term()]} | {error, sac_error() | term()}.
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
%% (CLI command)
-spec group_consumers(binary(), binary(), binary(), [atom()]) ->
                         {ok, [term()]} |
                         {error, sac_error() | term()}.
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

-spec overview(state() | undefined) -> map() | undefined.
overview(undefined) ->
    undefined;
overview(#?MODULE{groups = Groups} = S) when ?IS_STATE_REC(S) ->
    GroupsOverview =
        maps:map(fun(_,
                     #group{consumers = Consumers, partition_index = Idx}) ->
                    #{num_consumers => length(Consumers),
                      partition_index => Idx}
                 end,
                 Groups),
    #{num_groups => map_size(Groups), groups => GroupsOverview};
overview(S) ->
    rabbit_stream_sac_coordinator_v4:overview(S).

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
                    case lookup_consumer(ConnectionPid, SubscriptionId, Group0) of
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
apply(#command_activate_consumer{vhost = VH, stream = S, consumer_name = Name},
      #?MODULE{groups = StreamGroups0} = State0) ->
    {G, Eff} =
        case lookup_group(VH, S, Name, StreamGroups0) of
            undefined ->
                rabbit_log:warning("Trying to activate consumer in group ~tp, but "
                                   "the group does not longer exist",
                                   [{VH, S, Name}]),
                {undefined, []};
            G0 ->
                %% keep track of the former active, if any
                ActCsr = case lookup_active_consumer(G0) of
                             {value, Consumer} ->
                                 Consumer;
                             _ ->
                                 undefined
                         end,
                %% connected consumers are set to waiting status
                G1 = update_connected_consumers(G0, ?CONN_WAIT),
                case evaluate_active_consumer(G1) of
                    undefined ->
                        {G1, []};
                    #consumer{status = {?DISCONNECTED, _}} ->
                        %% we keep it this way, the consumer may come back
                        {G1, []};
                    Csr ->
                        G2 = update_consumer_state_in_group(G1, Csr, ?CONN_ACT),
                        %% do we need effects or not?
                        Effects =
                            case Csr of
                                Csr when ?SAME_CSR(Csr, ActCsr) ->
                                    %% it is the same active consumer as before
                                    %% no need to notify it
                                    [];
                                _ ->
                                    %% new active consumer, need to notify it
                                    [notify_csr_effect(Csr, S, Name, true)]
                            end,
                        {G2, Effects}
                end
        end,
    StreamGroups1 = update_groups(VH, S, Name,
                                  G, StreamGroups0),
    R = case G of
            undefined ->
                {error, not_found};
            _ ->
                ok
        end,
    {State0#?MODULE{groups = StreamGroups1}, R, Eff};
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
    {State#?MODULE{conf = NewConf}, ok, []};
apply(UnkCmd, State) ->
    rabbit_log:debug("~ts: unknown SAC command ~W", [?MODULE, UnkCmd, 10]),
    {State, {error, unknown_command}, []}.

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
            case has_pdown_active(Group, Pid) of
                true ->
                    %% a presumed-down active is coming back in the connection
                    %% we need to reconcile the group,
                    %% as there may have been 2 active consumers at a time
                    handle_pdown_active_reconnected(Pid, S0, Eff0, K);
                false ->
                    do_handle_group_connection_reconnected(Pid, S0, Eff0, K)
            end
    end.

do_handle_group_connection_reconnected(Pid, #?MODULE{groups = Groups0} = S0,
                                       Eff0, {VH, S, Name} = K)
  when is_map_key(K, Groups0) ->
    G0 = #group{consumers = Consumers0} = lookup_group(VH, S, Name, Groups0),
    %% update the status of the consumers from the connection
    {Consumers1, Updated} =
        lists:foldr(
          fun(#consumer{pid = P, status = {_, St}} = C, {L, _})
                when P == Pid ->
                  {[csr_status(C, {?CONNECTED, St}) | L], true};
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
    end;
do_handle_group_connection_reconnected(_, S0, Eff0, _) ->
    {S0, Eff0}.

handle_pdown_active_reconnected(Pid,
                                #?MODULE{groups = Groups0} = S0,
                                Eff0, {VH, S, Name} = K)
  when is_map_key(K, Groups0) ->
    G0 = #group{consumers = Consumers0} = lookup_group(VH, S, Name, Groups0),
    {Consumers1, Eff1} =
    case has_disconnected_active(G0) of
        true ->
            %% disconnected active consumer in the group, no rebalancing possible
            %% we update the presumed-down active consumers
            %% and tell them to step down
            lists:foldr(fun(#consumer{status = St, pid = P} = C, {Cs, Eff})
                              when P =:= Pid andalso St =:= ?PDOWN_ACT ->
                                {[csr_status(C, ?CONN_WAIT) | Cs],
                                 [notify_csr_effect(C, S,
                                                    Name, false, true) | Eff]};
                           (C, {Cs, Eff}) ->
                                {[C | Cs], Eff}
                        end, {[], Eff0}, Consumers0);
        false ->
            lists:foldr(fun(#consumer{status = St, pid = P} = C, {Cs, Eff})
                              when P =:= Pid andalso St =:= ?PDOWN_ACT ->
                                %% update presumed-down active
                                %% tell it to step down
                                {[csr_status(C, ?CONN_WAIT) | Cs],
                                 [notify_csr_effect(C, S,
                                                    Name, false, true) | Eff]};
                           (#consumer{status = {?PDOWN, _},
                                      pid = P} = C, {Cs, Eff})
                              when P =:= Pid ->
                                %% update presumed-down
                                {[csr_status(C, ?CONN_WAIT) | Cs], Eff};
                           (#consumer{status = ?CONN_ACT} = C, {Cs, Eff}) ->
                                %% update connected active
                                %% tell it to step down
                                {[csr_status(C, ?CONN_WAIT) | Cs],
                                 [notify_csr_effect(C, S,
                                                    Name, false, true) | Eff]};
                           (C, {Cs, Eff}) ->
                                {[C | Cs], Eff}
                        end, {[], Eff0}, Consumers0)
    end,
    G1 = G0#group{consumers = Consumers1},
    Groups1 = update_groups(VH, S, Name, G1, Groups0),
    {S0#?MODULE{groups = Groups1}, Eff1};
handle_pdown_active_reconnected(_, S0, Eff0, _) ->
    {S0, Eff0}.

has_pdown_active(#group{consumers = Consumers}, Pid) ->
    case lists:search(fun(#consumer{status = ?PDOWN_ACT,
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

maybe_rebalance_group(#group{partition_index = PI} = G0, _) when PI < -1 ->
    %% should not happen
    {G0, []};
maybe_rebalance_group(#group{consumers = CS} = G0, _) when length(CS) == 0 ->
    {G0, []};
maybe_rebalance_group(#group{partition_index = -1, consumers = Consumers0} = G0,
                      {_VH, S, Name}) ->
    case lookup_active_consumer(G0) of
        {value, ActiveCsr} ->
            %% there is already an active consumer, we just re-arrange
            %% the group to make sure the active consumer is the first in the array
            %% remove the active consumer from the list
            Consumers1 = lists:filter(fun(C) when ?SAME_CSR(C, ActiveCsr) ->
                                              false;
                                         (_) ->
                                              true
                                      end, Consumers0),
            %% add it back to the front
            G1 = G0#group{consumers = [ActiveCsr | Consumers1]},
            {G1, []};
        _ ->
            %% no active consumer
            G1 = compute_active_consumer(G0),
            case lookup_active_consumer(G1) of
                {value, Csr} ->
                    %% creating the side effect to notify the new active consumer
                    {G1, [notify_csr_effect(Csr, S, Name, true)]};
                _ ->
                    %% no active consumer found in the group, nothing to do
                    {G1, []}
            end
    end;
maybe_rebalance_group(#group{partition_index = _, consumers = Consumers} = G,
                      {_VH, S, Name}) ->
    case lookup_active_consumer(G) of
        {value, CurrentActive} ->
            case evaluate_active_consumer(G) of
                undefined ->
                    %% no-one to select
                    {G, []};
                CurrentActive ->
                    %% the current active stays the same
                    {G, []};
                _ ->
                    %% there's a change, telling the active it's not longer active
                    {update_consumer_state_in_group(G, CurrentActive,
                                                    {?CONNECTED, ?DEACTIVATING}),
                     [notify_csr_effect(CurrentActive, S, Name, false, true)]}
            end;
        false ->
            %% no active consumer in the group,
            case lists:search(fun(#consumer{status = Status}) ->
                                      Status =:= {?CONNECTED, ?DEACTIVATING}
                              end, Consumers) of
                {value, _Deactivating} ->
                    %%  we are waiting for the reply of a former active
                    %%  nothing to do
                    {G, []};
                _ ->
                    %% nothing going on in the group
                    %% a {disconnected, active} may have become {pdown, active}
                    %% we must select a new active
                    case evaluate_active_consumer(G) of
                        undefined ->
                            %% no-one to select
                            {G, []};
                        Csr ->
                            {update_consumer_state_in_group(G, Csr,
                                                            {?CONNECTED, ?ACTIVE}),
                             [notify_csr_effect(Csr, S, Name, true)]}
                    end
            end
    end.

%% used by CLI
-spec consumer_groups(binary(), [atom()], state()) -> {ok, [term()]}.
consumer_groups(VirtualHost, InfoKeys, #?MODULE{groups = Groups} = S)
  when ?IS_STATE_REC(S) ->
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
    {ok, lists:reverse(Res)};
consumer_groups(VirtualHost, InfoKeys, S) ->
    rabbit_stream_sac_coordinator_v4:consumer_groups(VirtualHost, InfoKeys, S).

-spec group_consumers(binary(),
                      binary(),
                      binary(),
                      [atom()],
                      state()) ->
                         {ok, [term()]} | {error, not_found}.
group_consumers(VH, St, Ref, InfoKeys,
                #?MODULE{groups = Groups} = S)
  when ?IS_STATE_REC(S) ->
    GroupId = {VH, St, Ref},
    case Groups of
        #{GroupId := #group{consumers = Consumers}} ->
            Cs = lists:foldr(fun(C, Acc) ->
                                     [csr_cli_record(C, InfoKeys) | Acc]
                             end,
                             [], Consumers),
            {ok, Cs};
        _ ->
            {error, not_found}
    end;
group_consumers(VirtualHost, Stream, Reference, InfoKeys, S) ->
    rabbit_stream_sac_coordinator_v4:group_consumers(VirtualHost, Stream,
                                                     Reference, InfoKeys, S).

csr_cli_record(#consumer{subscription_id = SubId, owner = Owner,
                         status = Status}, InfoKeys) ->
    lists:foldr(fun (subscription_id, Acc) ->
                        [{subscription_id, SubId} | Acc];
                    (connection_name, Acc) ->
                        [{connection_name, Owner} | Acc];
                    (state, Acc) ->
                        [{state, cli_csr_status_label(Status)} | Acc];
                    (Unknown, Acc) ->
                        [{Unknown, unknown_field} | Acc]
                end,
                [], InfoKeys).


cli_csr_status_label({Cnty, Acty}) ->
   rabbit_data_coercion:to_list(Acty) ++ " (" ++ connectivity_label(Cnty) ++ ")".

connectivity_label(?PDOWN) ->
    "presumed down";
connectivity_label(Cnty) ->
    rabbit_data_coercion:to_list(Cnty).

-spec ensure_monitors(command(),
                      state(),
                      map(),
                      ra_machine:effects()) ->
                         {state(), map(), ra_machine:effects()}.
ensure_monitors(#command_register_consumer{vhost = VH,
                                           stream = S,
                                           consumer_name = Name,
                                           connection_pid = Pid},
                #?MODULE{pids_groups = PidsGroups0} = State0,
                Monitors0,
                Effects) ->
    GroupId = {VH, S, Name},
    %% get the group IDs that depend on the PID
    Groups0 = maps:get(Pid, PidsGroups0, #{}),
    %% add the group ID
    Groups1 = Groups0#{GroupId => true},
    %% update the PID-to-group map
    PidsGroups1 = PidsGroups0#{Pid => Groups1},
    {State0#?MODULE{pids_groups = PidsGroups1}, Monitors0#{Pid => sac},
     [{monitor, process, Pid}, {monitor, node, node(Pid)} | Effects]};
ensure_monitors(#command_unregister_consumer{vhost = VH,
                                             stream = Stream,
                                             consumer_name = ConsumerName,
                                             connection_pid = Pid},
                #?MODULE{groups = StreamGroups0,
                         pids_groups = PidsGroups0} = State0,
                Monitors,
                Effects)
  when is_map_key(Pid, PidsGroups0) ->
    GroupId = {VH, Stream, ConsumerName},
    #{Pid := PidGroup0} = PidsGroups0,
    PidGroup1 =
        case lookup_group(VH, Stream, ConsumerName, StreamGroups0) of
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
            {State0#?MODULE{pids_groups = PidsGroups0#{Pid => PidGroup1}},
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

-spec handle_connection_down(connection_pid(), term(), state()) ->
    {state(), ra_machine:effects()}.
handle_connection_down(Pid, noconnection, State) ->
    handle_connection_node_disconnected(Pid, State);
handle_connection_down(Pid, _Reason,
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
            {State2, [node_disconnected_timer_effect(ConnPid, T)]}
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

-spec presume_connection_down(connection_pid(), state()) ->
    {state(), ra_machine:effects()}.
presume_connection_down(Pid, #?MODULE{groups = Groups} = State0) ->
    {State1, Eff} =
    maps:fold(fun(G, _, {St, Eff}) ->
                      handle_group_connection_presumed_down(Pid, St, Eff, G)
              end, {State0, []}, Groups),
    {State1, Eff}.

handle_group_connection_presumed_down(Pid, #?MODULE{groups = Groups0} = S0,
                                      Eff0, {VH, S, Name} = K)
  when is_map_key(K, Groups0) ->
    #group{consumers = Consumers0} = G0 = lookup_group(VH, S, Name, Groups0),
    {Consumers1, Updated} =
        lists:foldr(
          fun(#consumer{pid = P, status = {?DISCONNECTED, St}} = C, {L, _})
                when P == Pid ->
                  {[csr_status(C, {?PDOWN, St}) | L], true};
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
    end;
handle_group_connection_presumed_down(_, S0, Eff0, _) ->
    {S0, Eff0}.

handle_group_after_connection_down(Pid,
                                   {#?MODULE{groups = Groups0} = S0, Eff0},
                                   {VH, St, Name} = K)
  when is_map_key(K, Groups0) ->
    #group{consumers = Consumers0} = G0 = lookup_group(VH, St, Name, Groups0),
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
            {G2, Effects} = handle_consumer_removal(G1, St,
                                                    Name,
                                                    ActiveRemoved),
            Groups1 = update_groups(VH, St, Name, G2, Groups0),
            {S0#?MODULE{groups = Groups1}, Effects ++ Eff0};
        false ->
            {S0, Eff0}
    end;
handle_group_after_connection_down(_, {S0, Eff0}, _) ->
    {S0, Eff0}.

handle_group_after_connection_node_disconnected(ConnPid,
                                                #?MODULE{groups = Groups0} = S0,
                                                {VH, S, Name} = K)
  when is_map_key(K, Groups0) ->
    #group{consumers = Cs0} = G0 = lookup_group(VH, S, Name, Groups0),
    Cs1 = lists:foldr(fun(#consumer{status = {_, St},
                                    pid = Pid} = C0,
                          Acc) when Pid =:= ConnPid ->
                              C1 = csr_status(C0, {?DISCONNECTED, St}),
                              [C1 | Acc];
                         (C, Acc) ->
                              [C | Acc]
                      end, [], Cs0),
    G1 = G0#group{consumers = Cs1},
    Groups1 = update_groups(VH, S, Name, G1, Groups0),
    S0#?MODULE{groups = Groups1};
handle_group_after_connection_node_disconnected(_, S0, _) ->
    S0.

-spec import_state(ra_machine:version(), map()) -> state().
import_state(4, #{<<"groups">> := Groups, <<"pids_groups">> := PidsGroups}) ->
    #?MODULE{groups = map_to_groups(Groups),
             pids_groups = map_to_pids_groups(PidsGroups),
             conf = #{disconnected_timeout => ?DISCONNECTED_TIMEOUT_MS}}.

-spec check_conf_change(state() | term()) -> {new, conf()} | unchanged.
check_conf_change(State) when ?IS_STATE_REC(State) ->
    #?MODULE{conf = Conf} = State,
    DisconTimeout = lookup_disconnected_timeout(),
    case Conf of
        #{?DISCONNECTED_TIMEOUT_CONF_KEY := DT}
          when DT /= DisconTimeout ->
            {new, #{?DISCONNECTED_TIMEOUT_CONF_KEY => DisconTimeout}};
        C when is_map_key(?DISCONNECTED_TIMEOUT_CONF_KEY, C) == false ->
            {new, #{?DISCONNECTED_TIMEOUT_CONF_KEY => DisconTimeout}};
        _ ->
            unchanged
    end;
check_conf_change(_) ->
    unchanged.

-spec list_nodes(state()) -> [node()].
list_nodes(#?MODULE{groups = Groups}) ->
    Nodes = maps:fold(fun(_, G, Acc) ->
                              GNodes = nodes_from_group(G),
                              maps:merge(Acc, GNodes)
                      end, #{}, Groups),
    lists:sort(maps:keys(Nodes)).

-spec state_enter(ra_server:ra_state(), state() | term()) ->
    ra_machine:effects().
state_enter(leader, #?MODULE{groups = Groups} = State)
  when ?IS_STATE_REC(State) ->
    %% becoming leader, we re-issue monitors and timers for connections with
    %% disconnected consumers

    %% iterate over groups
    {Nodes, DisConns} =
    maps:fold(fun(_, #group{consumers = Cs}, Acc) ->
                      %% iterate over group consumers
                      lists:foldl(fun(#consumer{pid = P,
                                                status = {?DISCONNECTED, _},
                                                ts = Ts},
                                      {Nodes, DisConns}) ->
                                          %% disconnected consumer,
                                          %% store connection PID and node
                                          {Nodes#{node(P) => true},
                                           DisConns#{P => Ts}};
                                     (#consumer{pid = P}, {Nodes, DisConns}) ->
                                          %% store connection node only
                                          {Nodes#{node(P) => true}, DisConns}
                                  end, Acc, Cs)
              end, {#{}, #{}}, Groups),
    DisTimeout = disconnected_timeout(State),
    %% monitor involved nodes
    %% reset a timer for disconnected connections
    [{monitor, node, N} || N <- lists:sort(maps:keys(Nodes))] ++
    [begin
         Time = case ts() - Ts of
             T when T < 10_000 ->
                    %% 10 seconds is arbitrary, nothing specific about the value
                    10_000;
             T when T > DisTimeout ->
                 DisTimeout
         end,
         node_disconnected_timer_effect(P, Time)
     end || P := Ts <- maps:iterator(DisConns, ordered)];
state_enter(_, _) ->
    [].

-spec is_sac_error(term()) -> boolean().
is_sac_error(Reason) ->
    lists:member(Reason, ?SAC_ERRORS).

nodes_from_group(#group{consumers = Cs}) when is_list(Cs) ->
    lists:foldl(fun(#consumer{pid = Pid}, Acc) ->
                    Acc#{node(Pid) => true}
                end, #{}, Cs);
nodes_from_group(_) ->
    #{}.

-spec make_purge_nodes([node()]) -> {sac, command()}.
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
                      case map_to_group(V) of
                          G when ?IS_GROUP_REC(G) ->
                              Acc#{K => map_to_group(V)};
                          _ ->
                              Acc
                      end
              end, #{}, Groups);
map_to_groups(_) ->
    #{}.

map_to_pids_groups(PidsGroups) when is_map(PidsGroups) ->
    PidsGroups;
map_to_pids_groups(_) ->
    #{}.

map_to_group(#{<<"consumers">> := Consumers, <<"partition_index">> := Index}) ->
    {C, _} =
        lists:foldl(fun(V, {Cs, Dedup}) ->
                            case map_to_consumer(V) of
                                #consumer{pid = P, subscription_id = SubId} = C
                                  when not is_map_key({P, SubId}, Dedup) ->
                                    {[C | Cs], Dedup#{{P, SubId} => true}};
                                _ ->
                                    {Cs, Dedup}
                            end
                    end, {[], #{}}, Consumers),
    #group{consumers = lists:reverse(C),
           partition_index = Index};
map_to_group(_) ->
    undefined.

map_to_consumer(#{<<"pid">> := Pid, <<"subscription_id">> := SubId,
                  <<"owner">> := Owner, <<"active">> := Active}) ->
    csr(Pid, SubId, Owner, active_to_status(Active));
map_to_consumer(_) ->
    undefined.

active_to_status(true) ->
    {?CONNECTED, ?ACTIVE};
active_to_status(false) ->
    {?CONNECTED, ?WAITING}.

is_active({?PDOWN, _}) ->
    false;
is_active({_, ?ACTIVE}) ->
    true;
is_active({_, ?DEACTIVATING}) ->
    true;
is_active(_) ->
    false.

do_register_consumer(VH, S, -1 = _PI, Name, Pid, Owner, SubId,
                     #?MODULE{groups = StreamGroups0} = State)
  when is_map_key({VH, S, Name}, StreamGroups0) ->
    Group0 = lookup_group(VH, S, Name, StreamGroups0),

    Consumer = case lookup_active_consumer(Group0) of
                   {value, _} ->
                       csr(Pid, SubId, Owner, ?CONN_WAIT);
                   false ->
                       csr(Pid, SubId, Owner, ?CONN_ACT)
               end,
    Group1 = add_to_group(Consumer, Group0),
    StreamGroups1 = update_groups(VH, S, Name,
                                  Group1,
                                  StreamGroups0),

    #consumer{status = Status} = Consumer,
    Effects = case Status of
                  {_, ?ACTIVE} ->
                      [notify_csr_effect(Consumer, S, Name, is_active(Status))];
                  _ ->
                      []
              end,

    {State#?MODULE{groups = StreamGroups1}, {ok, is_active(Status)}, Effects};
do_register_consumer(VH, S, _PI, Name, Pid, Owner, SubId,
                     #?MODULE{groups = StreamGroups0} = State)
  when is_map_key({VH, S, Name}, StreamGroups0) ->
    Group0 = lookup_group(VH, S, Name, StreamGroups0),

    {Group1, Effects} =
        case Group0 of
            #group{consumers = []} ->
                %% first consumer in the group, it's the active one
                Consumer0 = csr(Pid, SubId, Owner, ?CONN_ACT),
                G1 = add_to_group(Consumer0, Group0),
                {G1,
                 [notify_csr_effect(Consumer0, S, Name, true)]};
            _G ->
                Consumer0 = csr(Pid, SubId, Owner, ?CONN_WAIT),
                G1 = add_to_group(Consumer0, Group0),
                maybe_rebalance_group(G1, {VH, S, Name})
        end,
        StreamGroups1 = update_groups(VH, S, Name,
                                      Group1,
                                      StreamGroups0),
    {value, #consumer{status = Status}} = lookup_consumer(Pid, SubId, Group1),
    {State#?MODULE{groups = StreamGroups1}, {ok, is_active(Status)}, Effects};
do_register_consumer(_, _, _, _, _, _, _, State) ->
    {State, {ok, false}, []}.

handle_consumer_removal(#group{consumers = []} = G, _, _, _) ->
    {G, []};
handle_consumer_removal(#group{partition_index = -1} = Group0,
                        S, Name, ActiveRemoved) ->
    case ActiveRemoved of
        true ->
            %% this is the active consumer we remove, computing the new one
            Group1 = compute_active_consumer(Group0),
            case lookup_active_consumer(Group1) of
                {value, Csr} ->
                    %% creating the side effect to notify the new active consumer
                    {Group1, [notify_csr_effect(Csr, S, Name, true)]};
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
        {value, CurrentActive} ->
            case evaluate_active_consumer(Group0) of
                undefined ->
                    {Group0, []};
                CurrentActive ->
                    %% the current active stays the same
                    {Group0, []};
                _ ->
                    %% there's a change, telling the active it's not longer active
                    {update_consumer_state_in_group(Group0, CurrentActive,
                                                    {?CONNECTED, ?DEACTIVATING}),
                     [notify_csr_effect(CurrentActive,
                                        Stream, ConsumerName, false, true)]}
            end;
        false ->
            case ActiveRemoved of
                true ->
                    %% the active one is going away, picking a new one
                    case evaluate_active_consumer(Group0) of
                        undefined ->
                            {Group0, []};
                        Csr ->
                            {update_consumer_state_in_group(Group0, Csr,
                                                            {?CONNECTED, ?ACTIVE}),
                             [notify_csr_effect(Csr, Stream, ConsumerName, true)]}
                    end;
                false ->
                    %% no active consumer in the (non-empty) group,
                    %% we are waiting for the reply of a former active
                    {Group0, []}
            end
    end.

notify_connection_effect(Pid) ->
    mod_call_effect(Pid, {sac, check_connection, #{}}).

notify_csr_effect(Csr, S, Name, Active) ->
    notify_csr_effect(Csr, S, Name, Active, false).

notify_csr_effect(#consumer{pid = P, subscription_id = SubId},
                  Stream, Name, Active, false = _SteppingDown) ->
    mod_call_effect(P,
                    {sac, #{subscription_id => SubId,
                            stream => Stream,
                            consumer_name => Name,
                            active => Active}});
notify_csr_effect(#consumer{pid = P, subscription_id = SubId},
                  Stream, Name, Active, true = SteppingDown) ->
    mod_call_effect(P,
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
            {ok, SGS#{{VirtualHost, Stream, ConsumerName} =>
                      #group{consumers = [], partition_index = PartitionIndex}}}
    end.

lookup_group(VirtualHost, Stream, ConsumerName, StreamGroups) ->
    maps:get({VirtualHost, Stream, ConsumerName}, StreamGroups,
             undefined).

add_to_group(#consumer{pid = Pid, subscription_id = SubId} = Consumer,
             #group{consumers = Consumers} = Group) ->
    case lookup_consumer(Pid, SubId, Group) of
        {value, _} ->
            %% the consumer is already in the group, nothing to do
            Group;
        false ->
            Group#group{consumers = Consumers ++ [Consumer]}
    end.

remove_from_group(Csr, #group{consumers = Consumers} = Group) ->
    CS = lists:filter(fun(C) when ?SAME_CSR(C, Csr) ->
                              false;
                         (_) ->
                              true
                      end, Consumers),
    Group#group{consumers = CS}.

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
                              S =:= ?DISCONN_ACT
                      end, Consumers) of
        {value, _DisconnectedActive} ->
            %% no rebalancing if there is a disconnected active
            G;
        false ->
            case evaluate_active_consumer(G) of
                undefined ->
                    G;
                AC ->
                    Consumers1 =
                        lists:foldr(
                          fun(C, L) when ?SAME_CSR(AC, C) ->
                                  %% change status of new active
                                  [csr_status(C, ?CONN_ACT) | L];
                             (#consumer{status = {?CONNECTED, _}} = C, L) ->
                                  %% other connected consumers are set to "waiting"
                                  [csr_status(C, ?CONN_WAIT) | L];
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
            %% no rebalancing if there is a disconnected active
            C;
        _ ->
            do_evaluate_active_consumer(G#group{consumers = eligible(Consumers)})
    end.

do_evaluate_active_consumer(#group{partition_index = PI}) when PI < -1 ->
    %% should not happen
    undefined;
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

update_groups(_VH, _S, _Name, undefined, Groups) ->
    Groups;
update_groups(VH, S, Name, #group{consumers = []}, Groups)
  when is_map_key({VH, S, Name}, Groups) ->
    %% the group is now empty, removing the key
    maps:remove({VH, S, Name}, Groups);
update_groups(_VH, _S, _Name, #group{consumers = []}, Groups) ->
    %% the group is now empty, but not in the group map
    %% just returning the map
    Groups;
update_groups(VH, S, Name, G, Groups) ->
    Groups#{{VH, S, Name} => G}.

update_consumer_state_in_group(#group{consumers = Consumers0} = G, Csr,
                               NewStatus) ->
    CS1 = lists:map(fun(C0) when ?SAME_CSR(C0, Csr) ->
                               csr_status(C0, NewStatus);
                       (C) ->
                            C
                    end,
                    Consumers0),
    G#group{consumers = CS1}.

update_connected_consumers(#group{consumers = Consumers0} = G, NewStatus) ->
    Consumers1 = lists:map(fun(#consumer{status = {?CONNECTED, _}} = C) ->
                                   csr_status(C, NewStatus);
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

-spec csr(pid(), subscription_id(), owner(), consumer_status()) ->
    consumer().
csr(Pid, Id, Owner, Status) ->
    #consumer{pid = Pid,
              subscription_id = Id,
              owner = Owner,
              status = Status,
              ts = ts()}.

-spec csr_status(consumer(), consumer_status()) -> consumer().
csr_status(C, Status) ->
    C#consumer{status = Status, ts = ts()}.

node_disconnected_timer_effect(Pid, T) ->
    {timer, {sac, node_disconnected,
             #{connection_pid => Pid}}, T}.

ts() ->
    erlang:system_time(millisecond).
