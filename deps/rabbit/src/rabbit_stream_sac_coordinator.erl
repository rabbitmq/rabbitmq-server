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
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stream_sac_coordinator).

-include("rabbit_stream_sac_coordinator.hrl").

-opaque command() ::
    {register_consumer,
     vhost(),
     stream(),
     partition_index(),
     consumer_name(),
     connection_pid(),
     subscription_id()} |
    {unregister_consumer,
     vhost(),
     stream(),
     consumer_name(),
     connection_pid(),
     subscription_id() |
     {activate_consumer, vhost(), stream(), consumer_name()}}.
-opaque state() :: #?MODULE{}.

-export_type([state/0,
              command/0]).

-export([apply/2,
         init_state/0,
         send_message/2,
         ensure_monitors/4,
         handle_connection_down/2]).

-spec init_state() -> state().
init_state() ->
    #?MODULE{groups = #{}, pids_groups = #{}}.

-spec apply(command(), state()) ->
               {state(), term(), ra_machine:effects()}.
apply({register_consumer,
       VirtualHost,
       Stream,
       PartitionIndex,
       ConsumerName,
       ConnectionPid,
       SubscriptionId},
      #?MODULE{groups = StreamGroups0} = State) ->
    %% TODO monitor connection PID to remove consumers when their connection dies
    %% this could require some index to avoid crawling the whole data structure
    %% this is necessary to fail over to another consumer when one dies abruptly
    %% also, check the liveliness of each consumer whenever there's a change in the group,
    %% to make sure to get rid of zombies
    %%
    %% TODO monitor streams and virtual hosts as well
    rabbit_log:debug("New consumer ~p ~p in group ~p, partition index "
                     "is ~p",
                     [ConnectionPid,
                      SubscriptionId,
                      {VirtualHost, Stream, ConsumerName},
                      PartitionIndex]),
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
                         SubscriptionId,
                         State#?MODULE{groups = StreamGroups1});
apply({unregister_consumer,
       VirtualHost,
       Stream,
       ConsumerName,
       ConnectionPid,
       SubscriptionId},
      #?MODULE{groups = StreamGroups0} = State0) ->
    {State1, Effects1} =
        case lookup_group(VirtualHost, Stream, ConsumerName, StreamGroups0) of
            error ->
                {State0, []};
            Group0 ->
                {Group1, Effects} =
                    case lookup_consumer(ConnectionPid, SubscriptionId, Group0)
                    of
                        {value, Consumer} ->
                            rabbit_log:debug("Unregistering consumer ~p from group",
                                             [Consumer]),
                            G1 = remove_from_group(Consumer, Group0),
                            rabbit_log:debug("Consumer removed from group: ~p",
                                             [G1]),
                            handle_consumer_removal(G1, Consumer);
                        false ->
                            rabbit_log:debug("Could not find consumer ~p ~p in group ~p ~p ~p",
                                             [ConnectionPid,
                                              SubscriptionId,
                                              VirtualHost,
                                              Stream,
                                              ConsumerName]),
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
apply({activate_consumer, VirtualHost, Stream, ConsumerName},
      #?MODULE{groups = StreamGroups0} = State0) ->
    {G, Eff} =
        case lookup_group(VirtualHost, Stream, ConsumerName, StreamGroups0) of
            undefined ->
                rabbit_log:warning("trying to activate consumer in group ~p, but "
                                   "the group does not longer exist",
                                   [{VirtualHost, Stream, ConsumerName}]);
            Group ->
                #consumer{pid = Pid, subscription_id = SubId} =
                    evaluate_active_consumer(Group),
                Group1 =
                    update_consumer_state_in_group(Group, Pid, SubId, true),
                {Group1, [notify_consumer_effect(Pid, SubId, true)]}
        end,
    StreamGroups1 =
        update_groups(VirtualHost, Stream, ConsumerName, G, StreamGroups0),
    {State0#?MODULE{groups = StreamGroups1}, ok, Eff}.

-spec ensure_monitors(command(), state(), map(), ra:effects()) ->
                         {state(), map(), ra:effects()}.
ensure_monitors({register_consumer,
                 VirtualHost,
                 Stream,
                 _PartitionIndex,
                 ConsumerName,
                 Pid,
                 _SubscriptionId},
                #?MODULE{pids_groups = PidsGroups0} = State0,
                Monitors0,
                Effects) ->
    GroupId = {VirtualHost, Stream, ConsumerName},
    PidsGroups1 =
        case PidsGroups0 of
            #{Pid := Groups} ->
                case sets:is_element(GroupId, Groups) of
                    true ->
                        PidsGroups0;
                    false ->
                        maps:put(Pid, sets:add_element(GroupId, Groups),
                                 PidsGroups0)
                end;
            _ ->
                Gs = sets:new(),
                maps:put(Pid, sets:add_element(GroupId, Gs), PidsGroups0)
        end,
    Monitors1 =
        case Monitors0 of
            #{Pid := {Pid, sac}} ->
                Monitors0;
            _ ->
                maps:put(Pid, {Pid, sac}, Monitors0)
        end,

    {State0#?MODULE{pids_groups = PidsGroups1}, Monitors1,
     [{monitor, process, Pid}, {monitor, node, node(Pid)} | Effects]};
ensure_monitors({unregister_consumer,
                 VirtualHost,
                 Stream,
                 ConsumerName,
                 _ConnectionPid,
                 _SubscriptionId},
                #?MODULE{groups = StreamGroups0, pids_groups = _PidsGroups0} =
                    _State0,
                _Monitors,
                _Effects) ->
    _GroupId = {VirtualHost, Stream, ConsumerName},
    case lookup_group(VirtualHost, Stream, ConsumerName, StreamGroups0) of
        undefined ->
            %% group is gone
            ok;
        _Group ->
            ok
    end;
ensure_monitors(_, #?MODULE{} = State0, Monitors, Effects) ->
    {State0, Monitors, Effects}.

-spec handle_connection_down(connection_pid(), state()) -> state().
handle_connection_down(_Pid, State) ->
    State.

do_register_consumer(VirtualHost,
                     Stream,
                     -1,
                     ConsumerName,
                     ConnectionPid,
                     SubscriptionId,
                     #?MODULE{groups = StreamGroups0} = State) ->
    Group0 =
        lookup_group(VirtualHost, Stream, ConsumerName, StreamGroups0),

    rabbit_log:debug("Group: ~p", [Group0]),
    Consumer =
        case lookup_active_consumer(Group0) of
            {value, _} ->
                #consumer{pid = ConnectionPid,
                          subscription_id = SubscriptionId,
                          active = false};
            false ->
                #consumer{pid = ConnectionPid,
                          subscription_id = SubscriptionId,
                          active = true}
        end,
    Group1 = add_to_group(Consumer, Group0),
    rabbit_log:debug("Consumer added to group: ~p", [Group1]),
    StreamGroups1 =
        update_groups(VirtualHost,
                      Stream,
                      ConsumerName,
                      Group1,
                      StreamGroups0),

    #consumer{active = Active} = Consumer,
    Effects =
        case Consumer of
            #consumer{active = true} ->
                [notify_consumer_effect(ConnectionPid, SubscriptionId, Active)];
            _ ->
                []
        end,

    {State#?MODULE{groups = StreamGroups1}, {ok, Active}, Effects};
do_register_consumer(VirtualHost,
                     Stream,
                     _,
                     ConsumerName,
                     ConnectionPid,
                     SubscriptionId,
                     #?MODULE{groups = StreamGroups0} = State) ->
    Group0 =
        lookup_group(VirtualHost, Stream, ConsumerName, StreamGroups0),

    rabbit_log:debug("Group: ~p", [Group0]),
    {Group1, Effects} =
        case Group0 of
            #group{consumers = []} ->
                %% first consumer in the group, it's the active one
                Consumer0 =
                    #consumer{pid = ConnectionPid,
                              subscription_id = SubscriptionId,
                              active = true},
                G1 = add_to_group(Consumer0, Group0),
                {G1,
                 [notify_consumer_effect(ConnectionPid, SubscriptionId, true)]};
            _G ->
                %% whatever the current state is, the newcomer will be passive
                Consumer0 =
                    #consumer{pid = ConnectionPid,
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
                                                         false,
                                                         true)]}
                        end;
                    undefined ->
                        %% no active consumer in the (non-empty) group, we are waiting for the reply of a former active
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

handle_consumer_removal(#group{consumers = []} = G, _) ->
    {G, []};
handle_consumer_removal(#group{partition_index = -1} = Group0,
                        Consumer) ->
    case Consumer of
        #consumer{active = true} ->
            Group1 = compute_active_consumer(Group0),
            rabbit_log:debug("This is the active consumer, group after active "
                             "consumer calculation: ~p",
                             [Group1]),
            case lookup_active_consumer(Group1) of
                {value, #consumer{pid = Pid, subscription_id = SubId} = C} ->
                    rabbit_log:debug("Creating side effect to notify new active consumer ~p",
                                     [C]),
                    {Group1, [notify_consumer_effect(Pid, SubId, true)]};
                _ ->
                    rabbit_log:debug("No active consumer found in the group, nothing "
                                     "to do"),
                    {Group1, []}
            end;
        #consumer{active = false} ->
            rabbit_log:debug("Not the active consumer, nothing to do."),
            {Group0, []}
    end;
handle_consumer_removal(Group0, Consumer) ->
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
                     [notify_consumer_effect(ActPid, ActSubId, false, true)]}
            end;
        false ->
            case Consumer#consumer.active of
                true ->
                    %% the active one is going away, picking a new one
                    #consumer{pid = P, subscription_id = SID} =
                        evaluate_active_consumer(Group0),
                    {update_consumer_state_in_group(Group0, P, SID, true),
                     [notify_consumer_effect(P, SID, true)]};
                false ->
                    %% no active consumer in the (non-empty) group, we are waiting for the reply of a former active
                    {Group0, []}
            end
    end.

notify_consumer_effect(Pid, SubId, Active) ->
    notify_consumer_effect(Pid, SubId, Active, false).

notify_consumer_effect(Pid, SubId, Active, false) ->
    mod_call_effect(Pid,
                    {sac,
                     {{subscription_id, SubId}, {active, Active},
                      {extra, []}}});
notify_consumer_effect(Pid, SubId, Active, true) ->
    mod_call_effect(Pid,
                    {sac,
                     {{subscription_id, SubId}, {active, Active},
                      {extra, [{stepping_down, true}]}}}).

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

compute_active_consumer(#group{consumers = []} = Group) ->
    Group;
compute_active_consumer(#group{partition_index = _,
                               consumers = [Consumer0]} =
                            Group0) ->
    Consumer1 = Consumer0#consumer{active = true},
    Group0#group{consumers = [Consumer1]};
compute_active_consumer(#group{partition_index = -1,
                               consumers = [Consumer0 | T]} =
                            Group0) ->
    Consumer1 = Consumer0#consumer{active = true},
    Consumers = lists:map(fun(C) -> C#consumer{active = false} end, T),
    Group0#group{consumers = [Consumer1] ++ Consumers};
compute_active_consumer(#group{partition_index = PartitionIndex,
                               consumers = Consumers0} =
                            Group) ->
    ActiveConsumerIndex = PartitionIndex rem length(Consumers0),
    rabbit_log:debug("Active consumer index = ~p rem ~p = ~p",
                     [PartitionIndex, length(Consumers0), ActiveConsumerIndex]),
    {_, Consumers1} =
        lists:foldr(fun (C0, {Index, Cs}) when Index == ActiveConsumerIndex ->
                            C1 = C0#consumer{active = true},
                            {Index - 1, [C1 | Cs]};
                        (C0, {Index, Cs}) ->
                            C1 = C0#consumer{active = false},
                            {Index - 1, [C1 | Cs]}
                    end,
                    {length(Consumers0) - 1, []}, Consumers0),
    Group#group{consumers = Consumers1}.

evaluate_active_consumer(#group{partition_index = PartitionIndex,
                                consumers = Consumers}) ->
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

update_groups(VirtualHost,
              Stream,
              ConsumerName,
              #group{consumers = []},
              StreamGroups) ->
    rabbit_log:debug("Group ~p ~p ~p is now empty, removing it",
                     [VirtualHost, Stream, ConsumerName]),
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
    CS1 = lists:foldr(fun(C0, Acc) ->
                         case C0 of
                             #consumer{pid = Pid, subscription_id = SubId} ->
                                 C1 = C0#consumer{active = NewState},
                                 [C1 | Acc];
                             C -> [C | Acc]
                         end
                      end,
                      [], Consumers0),
    G#group{consumers = CS1}.

mod_call_effect(Pid, Msg) ->
    {mod_call, rabbit_stream_sac_coordinator, send_message, [Pid, Msg]}.

-spec send_message(pid(), term()) -> ok.
send_message(ConnectionPid, Msg) ->
    ConnectionPid ! Msg,
    ok.
