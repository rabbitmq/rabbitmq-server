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
%% Copyright (c) 2021-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stream_sac_coordinator).

-include("rabbit_stream_sac_coordinator.hrl").

-opaque command() ::
    #command_register_consumer{} | #command_unregister_consumer{} |
    #command_activate_consumer{}.
-opaque state() :: #?MODULE{}.

-export_type([state/0,
              command/0]).

%% Single Active Consumer API
-export([register_consumer/7,
         unregister_consumer/5,
         activate_consumer/3,
         consumer_groups/2,
         group_consumers/4]).
-export([apply/2,
         init_state/0,
         send_message/2,
         ensure_monitors/4,
         handle_connection_down/2,
         consumer_groups/3,
         group_consumers/5,
         overview/1]).

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
    process_command({sac,
                     #command_register_consumer{vhost =
                                                VirtualHost,
                                                stream =
                                                Stream,
                                                partition_index
                                                =
                                                PartitionIndex,
                                                consumer_name
                                                =
                                                ConsumerName,
                                                connection_pid
                                                =
                                                ConnectionPid,
                                                owner =
                                                Owner,
                                                subscription_id
                                                =
                                                SubscriptionId}}).

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
    process_command({sac,
                     #command_unregister_consumer{vhost =
                                                  VirtualHost,
                                                  stream =
                                                  Stream,
                                                  consumer_name
                                                  =
                                                  ConsumerName,
                                                  connection_pid
                                                  =
                                                  ConnectionPid,
                                                  subscription_id
                                                  =
                                                  SubscriptionId}}).

-spec activate_consumer(binary(), binary(), binary()) -> ok.
activate_consumer(VirtualHost, Stream, ConsumerName) ->
    process_command({sac,
                     #command_activate_consumer{vhost =
                                                VirtualHost,
                                                stream =
                                                Stream,
                                                consumer_name
                                                =
                                                ConsumerName}}).

process_command(Cmd) ->
    case rabbit_stream_coordinator:process_command(Cmd) of
        {ok, Res, _} ->
            Res;
        {error, _} = Err ->
            rabbit_log:warning("SAC coordinator command ~tp returned error ~tp",
                               [Cmd, Err]),
            Err
    end.

%% return the current groups for a given virtual host
-spec consumer_groups(binary(), [atom()]) ->
                         {ok,
                          [term()] | {error, atom()}}.
consumer_groups(VirtualHost, InfoKeys) ->
    case ra:local_query({rabbit_stream_coordinator,
                         node()},
                        fun(State) ->
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
    case ra:local_query({rabbit_stream_coordinator,
                         node()},
                        fun(State) ->
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
    #?MODULE{groups = #{}, pids_groups = #{}}.

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
    StreamGroups1 =
        maybe_create_group(VirtualHost,
                           Stream,
                           PartitionIndex,
                           ConsumerName,
                           StreamGroups0),

    do_register_consumer(VirtualHost,
                         Stream,
                         PartitionIndex,
                         ConsumerName,
                         ConnectionPid,
                         Owner,
                         SubscriptionId,
                         State#?MODULE{groups = StreamGroups1});
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
                            handle_consumer_removal(G1, Consumer, Stream, ConsumerName);
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
            Group ->
                #consumer{pid = Pid, subscription_id = SubId} =
                    evaluate_active_consumer(Group),
                    Group1 = update_consumer_state_in_group(Group, Pid, SubId, true),
                {Group1, [notify_consumer_effect(Pid, SubId, Stream, ConsumerName, true)]}
        end,
    StreamGroups1 =
        update_groups(VirtualHost, Stream, ConsumerName, G, StreamGroups0),
    {State0#?MODULE{groups = StreamGroups1}, ok, Eff}.

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
                                           active = Active},
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
                                                    (state, RecAcc)
                                                        when Active ->
                                                        [{state, active}
                                                         | RecAcc];
                                                    (state, RecAcc) ->
                                                        [{state, inactive}
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
            %% iterate other the groups that this PID affects
            maps:fold(fun({VirtualHost, Stream, ConsumerName}, _,
                          {#?MODULE{groups = ConsumerGroups} = S0, Eff0}) ->
                         case lookup_group(VirtualHost,
                                           Stream,
                                           ConsumerName,
                                           ConsumerGroups)
                         of
                             undefined -> {S0, Eff0};
                             #group{consumers = Consumers} ->
                                 %% iterate over the consumers of the group
                                 %% and unregister the ones from this PID.
                                 %% It may not be optimal, computing the new active consumer
                                 %% from the purged group and notifying the remaining consumers
                                 %% appropriately should avoid unwanted notifications and even rebalancing.
                                 lists:foldl(fun (#consumer{pid = P,
                                                            subscription_id =
                                                                SubId},
                                                  {StateSub0, EffSub0})
                                                     when P == Pid ->
                                                     {StateSub1, ok, E} =
                                                         ?MODULE:apply(#command_unregister_consumer{vhost
                                                                                                        =
                                                                                                        VirtualHost,
                                                                                                    stream
                                                                                                        =
                                                                                                        Stream,
                                                                                                    consumer_name
                                                                                                        =
                                                                                                        ConsumerName,
                                                                                                    connection_pid
                                                                                                        =
                                                                                                        Pid,
                                                                                                    subscription_id
                                                                                                        =
                                                                                                        SubId},
                                                                       StateSub0),
                                                     {StateSub1, EffSub0 ++ E};
                                                 (_Consumer, Acc) -> Acc
                                             end,
                                             {S0, Eff0}, Consumers)
                         end
                      end,
                      {State1, []}, Groups)
    end.

do_register_consumer(VirtualHost,
                     Stream,
                     -1 = _PartitionIndex,
                     ConsumerName,
                     ConnectionPid,
                     Owner,
                     SubscriptionId,
                     #?MODULE{groups = StreamGroups0} = State) ->
    Group0 =
        lookup_group(VirtualHost, Stream, ConsumerName, StreamGroups0),

    Consumer =
        case lookup_active_consumer(Group0) of
            {value, _} ->
                #consumer{pid = ConnectionPid,
                          owner = Owner,
                          subscription_id = SubscriptionId,
                          active = false};
            false ->
                #consumer{pid = ConnectionPid,
                          subscription_id = SubscriptionId,
                          owner = Owner,
                          active = true}
        end,
    Group1 = add_to_group(Consumer, Group0),
    StreamGroups1 =
        update_groups(VirtualHost,
                      Stream,
                      ConsumerName,
                      Group1,
                      StreamGroups0),

    #consumer{active = Active} = Consumer,
    Effects =
        case Active of
            true ->
                [notify_consumer_effect(ConnectionPid, SubscriptionId,
                                        Stream, ConsumerName, Active)];
            _ ->
                []
        end,

    {State#?MODULE{groups = StreamGroups1}, {ok, Active}, Effects};
do_register_consumer(VirtualHost,
                     Stream,
                     _PartitionIndex,
                     ConsumerName,
                     ConnectionPid,
                     Owner,
                     SubscriptionId,
                     #?MODULE{groups = StreamGroups0} = State) ->
    Group0 =
        lookup_group(VirtualHost, Stream, ConsumerName, StreamGroups0),

    {Group1, Effects} =
        case Group0 of
            #group{consumers = []} ->
                %% first consumer in the group, it's the active one
                Consumer0 =
                    #consumer{pid = ConnectionPid,
                              owner = Owner,
                              subscription_id = SubscriptionId,
                              active = true},
                G1 = add_to_group(Consumer0, Group0),
                {G1,
                 [notify_consumer_effect(ConnectionPid, SubscriptionId,
                                         Stream, ConsumerName, true)]};
            _G ->
                %% whatever the current state is, the newcomer will be passive
                Consumer0 =
                    #consumer{pid = ConnectionPid,
                              owner = Owner,
                              subscription_id = SubscriptionId,
                              active = false},
                G1 = add_to_group(Consumer0, Group0),

                case lookup_active_consumer(G1) of
                    {value,
                     #consumer{pid = ActPid, subscription_id = ActSubId} =
                         CurrentActive} ->
                        case evaluate_active_consumer(G1) of
                            CurrentActive ->
                                %% the current active stays the same
                                {G1, []};
                            _ ->
                                %% there's a change, telling the active it's not longer active
                                {update_consumer_state_in_group(G1,
                                                                ActPid,
                                                                ActSubId,
                                                                false),
                                 [notify_consumer_effect(ActPid,
                                                         ActSubId,
                                                         Stream,
                                                         ConsumerName,
                                                         false,
                                                         true)]}
                        end;
                    false ->
                        %% no active consumer in the (non-empty) group,
                        %% we are waiting for the reply of a former active
                        {G1, []}
                end
        end,
    StreamGroups1 =
        update_groups(VirtualHost,
                      Stream,
                      ConsumerName,
                      Group1,
                      StreamGroups0),
    {value, #consumer{active = Active}} =
        lookup_consumer(ConnectionPid, SubscriptionId, Group1),
    {State#?MODULE{groups = StreamGroups1}, {ok, Active}, Effects}.

handle_consumer_removal(#group{consumers = []} = G, _, _, _) ->
    {G, []};
handle_consumer_removal(#group{partition_index = -1} = Group0,
                        Consumer, Stream, ConsumerName) ->
    case Consumer of
        #consumer{active = true} ->
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
        #consumer{active = false} ->
            %% not the active consumer, nothing to do.
            {Group0, []}
    end;
handle_consumer_removal(Group0, Consumer, Stream, ConsumerName) ->
    case lookup_active_consumer(Group0) of
        {value,
         #consumer{pid = ActPid, subscription_id = ActSubId} =
             CurrentActive} ->
            case evaluate_active_consumer(Group0) of
                CurrentActive ->
                    %% the current active stays the same
                    {Group0, []};
                _ ->
                    %% there's a change, telling the active it's not longer active
                    {update_consumer_state_in_group(Group0,
                                                    ActPid,
                                                    ActSubId,
                                                    false),
                     [notify_consumer_effect(ActPid, ActSubId,
                                             Stream, ConsumerName, false, true)]}
            end;
        false ->
            case Consumer#consumer.active of
                true ->
                    %% the active one is going away, picking a new one
                    #consumer{pid = P, subscription_id = SID} =
                        evaluate_active_consumer(Group0),
                    {update_consumer_state_in_group(Group0, P, SID, true),
                     [notify_consumer_effect(P, SID,
                                             Stream, ConsumerName, true)]};
                false ->
                    %% no active consumer in the (non-empty) group,
                    %% we are waiting for the reply of a former active
                    {Group0, []}
            end
    end.

message_type() ->
    case has_unblock_group_support() of
        true ->
            map;
        false ->
            tuple
    end.

notify_consumer_effect(Pid, SubId, Stream, Name, Active) ->
    notify_consumer_effect(Pid, SubId, Stream, Name, Active, false).

notify_consumer_effect(Pid, SubId, Stream, Name, Active, SteppingDown) ->
    notify_consumer_effect(Pid, SubId, Stream, Name, Active, SteppingDown, message_type()).

notify_consumer_effect(Pid, SubId, _Stream, _Name, Active, false = _SteppingDown, tuple) ->
    mod_call_effect(Pid,
                    {sac,
                     {{subscription_id, SubId},
                      {active, Active},
                      {extra, []}}});
notify_consumer_effect(Pid, SubId, _Stream, _Name, Active, true = _SteppingDown, tuple) ->
    mod_call_effect(Pid,
                    {sac,
                     {{subscription_id, SubId},
                      {active, Active},
                      {extra, [{stepping_down, true}]}}});
notify_consumer_effect(Pid, SubId, Stream, Name, Active, false = _SteppingDown, map) ->
    mod_call_effect(Pid,
                    {sac, #{subscription_id => SubId,
                            stream => Stream,
                            consumer_name => Name,
                            active => Active}});
notify_consumer_effect(Pid, SubId, Stream, Name, Active, true = _SteppingDown, map) ->
    mod_call_effect(Pid,
                    {sac, #{subscription_id => SubId,
                            stream => Stream,
                            consumer_name => Name,
                            active => Active,
                            stepping_down => true}}).

maybe_create_group(VirtualHost,
                   Stream,
                   PartitionIndex,
                   ConsumerName,
                   StreamGroups) ->
    case StreamGroups of
        #{{VirtualHost, Stream, ConsumerName} := _Group} ->
            StreamGroups;
        SGS ->
            maps:put({VirtualHost, Stream, ConsumerName},
                     #group{consumers = [], partition_index = PartitionIndex},
                     SGS)
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

compute_active_consumer(#group{consumers = Crs,
                               partition_index = -1} =
                            Group)
    when length(Crs) == 0 ->
    Group;
compute_active_consumer(#group{partition_index = -1,
                               consumers = [Consumer0]} =
                            Group0) ->
    Consumer1 = Consumer0#consumer{active = true},
    Group0#group{consumers = [Consumer1]};
compute_active_consumer(#group{partition_index = -1,
                               consumers = [Consumer0 | T]} =
                            Group0) ->
    Consumer1 = Consumer0#consumer{active = true},
    Consumers = lists:map(fun(C) -> C#consumer{active = false} end, T),
    Group0#group{consumers = [Consumer1] ++ Consumers}.

evaluate_active_consumer(#group{partition_index = PartitionIndex,
                                consumers = Consumers})
    when PartitionIndex >= 0 ->
    ActiveConsumerIndex = PartitionIndex rem length(Consumers),
    lists:nth(ActiveConsumerIndex + 1, Consumers).

lookup_consumer(ConnectionPid, SubscriptionId,
                #group{consumers = Consumers}) ->
    lists:search(fun(#consumer{pid = ConnPid, subscription_id = SubId}) ->
                    ConnPid == ConnectionPid andalso SubId == SubscriptionId
                 end,
                 Consumers).

lookup_active_consumer(#group{consumers = Consumers}) ->
    lists:search(fun(#consumer{active = Active}) -> Active end,
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
                               NewState) ->
    CS1 = lists:map(fun(C0) ->
                       case C0 of
                           #consumer{pid = Pid, subscription_id = SubId} ->
                               C0#consumer{active = NewState};
                           C -> C
                       end
                    end,
                    Consumers0),
    G#group{consumers = CS1}.

mod_call_effect(Pid, Msg) ->
    {mod_call, rabbit_stream_sac_coordinator, send_message, [Pid, Msg]}.

-spec send_message(pid(), term()) -> ok.
send_message(ConnectionPid, Msg) ->
    ConnectionPid ! Msg,
    ok.

has_unblock_group_support() ->
    rabbit_feature_flags:is_enabled(stream_sac_coordinator_unblock_group).
