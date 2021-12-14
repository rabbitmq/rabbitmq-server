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

-type vhost() :: binary().
-type stream() :: binary().
-type consumer_name() :: binary().
-type subscription_id() :: byte().

-opaque command() ::
    {register_consumer, vhost()} | {unregister_consumer, vhost()}.

-record(consumer,
        {pid :: pid(), subscription_id :: subscription_id(),
         active :: boolean()}).
-record(group,
        {consumers :: [#consumer{}], partition_index :: integer()}).
-record(?MODULE,
        {groups :: #{{vhost(), stream(), consumer_name()} => #group{}}}).

-opaque state() :: #?MODULE{}.

-export_type([state/0,
              command/0]).

-export([apply/2,
         init_state/0,
         send_message/2]).

-spec init_state() -> state().
init_state() ->
    #?MODULE{groups = #{}}.

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
    Group0 =
        lookup_group(VirtualHost, Stream, ConsumerName, StreamGroups1),

    rabbit_log:debug("Group: ~p", [Group0]),
    FormerActive =
        case lookup_active_consumer(Group0) of
            {value, FA} ->
                FA;
            false ->
                undefined
        end,
    Consumer0 =
        #consumer{pid = ConnectionPid,
                  subscription_id = SubscriptionId,
                  active = false},
    Group1 = add_to_group(Consumer0, Group0),
    rabbit_log:debug("Consumer added to group: ~p", [Group1]),
    Group2 = compute_active_consumer(Group1),
    rabbit_log:debug("Consumers in group after active consumer computation: ~p",
                     [Group2]),
    StreamGroups2 =
        update_groups(VirtualHost,
                      Stream,
                      ConsumerName,
                      Group2,
                      StreamGroups1),

    {value, Consumer1} =
        lookup_consumer(ConnectionPid, SubscriptionId, Group2),
    Effects = notify_consumers(FormerActive, Consumer1, Group2),
    #consumer{active = Active} = Consumer1,
    {State#?MODULE{groups = StreamGroups2}, {ok, Active}, Effects};
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
                            {value, ActiveInPreviousGroupInstance} =
                                lookup_active_consumer(Group0),
                            G1 = remove_from_group(Consumer, Group0),
                            rabbit_log:debug("Consumer removed from group: ~p",
                                             [G1]),
                            G2 = compute_active_consumer(G1),
                            rabbit_log:debug("Consumers in group after active consumer computation: ~p",
                                             [G2]),
                            NewActive =
                                case lookup_active_consumer(G2) of
                                    {value, AC} ->
                                        AC;
                                    false ->
                                        undefined
                                end,
                            AIPGI =
                                case ActiveInPreviousGroupInstance of
                                    Consumer ->
                                        undefined;
                                    _ ->
                                        ActiveInPreviousGroupInstance
                                end,
                            Effs = notify_consumers(AIPGI, NewActive, G2),
                            {G2, Effs};
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
    {State1, ok, Effects1}.

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
    maps:get({VirtualHost, Stream, ConsumerName}, StreamGroups).

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

lookup_consumer(ConnectionPid, SubscriptionId,
                #group{consumers = Consumers}) ->
    lists:search(fun(#consumer{pid = ConnPid, subscription_id = SubId}) ->
                    ConnPid == ConnectionPid andalso SubId == SubscriptionId
                 end,
                 Consumers).

lookup_active_consumer(#group{consumers = Consumers}) ->
    lists:search(fun(#consumer{active = Active}) -> Active end,
                 Consumers).

notify_consumers(_, _, #group{consumers = []}) ->
    [];
notify_consumers(_,
                 #consumer{pid = ConnectionPid,
                           subscription_id = SubscriptionId} =
                     NewConsumer,
                 #group{partition_index = -1, consumers = [NewConsumer]}) ->
    [mod_call_effect(ConnectionPid,
                     {sac,
                      {{subscription_id, SubscriptionId}, {active, true},
                       {side_effects, []}}})];
notify_consumers(_,
                 #consumer{pid = ConnectionPid,
                           subscription_id = SubscriptionId} =
                     NewConsumer,
                 #group{partition_index = -1, consumers = [NewConsumer | _]}) ->
    [mod_call_effect(ConnectionPid,
                     {sac,
                      {{subscription_id, SubscriptionId}, {active, true},
                       {side_effects, []}}})];
notify_consumers(_,
                 #consumer{pid = ConnectionPid,
                           subscription_id = SubscriptionId},
                 #group{partition_index = -1, consumers = _}) ->
    %% notifying a newcomer that it's inactive
    %% FIXME is consumer update always necessary for inactive newcomers?
    %% can't they assume they are inactive by default?
    [mod_call_effect(ConnectionPid,
                     {sac,
                      {{subscription_id, SubscriptionId}, {active, false},
                       {side_effects, []}}})];
notify_consumers(undefined,
                 #consumer{pid = ConnectionPid,
                           subscription_id = SubscriptionId},
                 _Group) ->
    [mod_call_effect(ConnectionPid,
                     {sac,
                      {{subscription_id, SubscriptionId}, {active, true},
                       {side_effects, []}}})];
notify_consumers(ActiveInPreviousGroupInstance, NewActive, _Group)
    when ActiveInPreviousGroupInstance == NewActive ->
    %% no changes (e.g. on unsubscription), nothing to do.
    [];
notify_consumers(#consumer{pid = FormerConnPid,
                           subscription_id = FormerSubId},
                 #consumer{pid = NewConnPid,
                           subscription_id = NewSubId,
                           active = true},
                 _Group) ->
    [mod_call_effect(FormerConnPid,
                     {sac,
                      {{subscription_id, FormerSubId}, {active, false},
                       {side_effects,
                        [{message, NewConnPid,
                          {sac,
                           {{subscription_id, NewSubId}, {active, true},
                            {side_effects, []}}}}]}}})];
notify_consumers(_StillActive,
                 #consumer{pid = NewConnPid,
                           subscription_id = NewSubId,
                           active = false},
                 _Group) ->
    %% notifying a newcomer that it's inactive
    %% FIXME is consumer update always necessary for inactive newcomers?
    %% can't they assume they are inactive by default?
    [mod_call_effect(NewConnPid,
                     {sac,
                      {{subscription_id, NewSubId}, {active, false},
                       {side_effects, []}}})].

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

mod_call_effect(Pid, Msg) ->
    {mod_call, rabbit_stream_sac_coordinator, send_message, [Pid, Msg]}.

-spec send_message(pid(), term()) -> ok.
send_message(ConnectionPid, Msg) ->
    ConnectionPid ! Msg,
    ok.
