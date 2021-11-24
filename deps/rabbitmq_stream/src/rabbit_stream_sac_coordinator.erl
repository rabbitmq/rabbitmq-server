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

-behaviour(gen_server).

%% API functions
-export([start_link/0]).
%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).
-export([register_consumer/6,
         unregister_consumer/5]).

-type vhost() :: binary().
-type stream() :: binary().
-type consumer_name() :: binary().
-type subscription_id() :: byte().

-record(consumer,
        {pid :: pid(), subscription_id :: subscription_id(),
         active :: boolean()}).
-record(group,
        {consumers :: [#consumer{}], partition_index :: integer()}).
-record(state,
        {groups :: #{{vhost(), stream(), consumer_name()} => #group{}}}).

register_consumer(VirtualHost,
                  Stream,
                  PartitionIndex,
                  ConsumerName,
                  ConnectionPid,
                  SubscriptionId) ->
    call({register_consumer,
          VirtualHost,
          Stream,
          PartitionIndex,
          ConsumerName,
          ConnectionPid,
          SubscriptionId}).

unregister_consumer(VirtualHost,
                    Stream,
                    ConsumerName,
                    ConnectionPid,
                    SubscriptionId) ->
    call({unregister_consumer,
          VirtualHost,
          Stream,
          ConsumerName,
          ConnectionPid,
          SubscriptionId}).

call(Request) ->
    gen_server:call({global, ?MODULE},
                    Request).%%%===================================================================
                             %%% API functions
                             %%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    case gen_server:start_link({global, ?MODULE}, ?MODULE, [], []) of
        {error, {already_started, _Pid}} ->
            ignore;
        R ->
            R
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    {ok, #state{groups = #{}}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({register_consumer,
             VirtualHost,
             Stream,
             PartitionIndex,
             ConsumerName,
             ConnectionPid,
             SubscriptionId},
            _From, #state{groups = StreamGroups0} = State) ->
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
    Group2 = compute_active_consumer(Group1),
    rabbit_log:debug("Consumers in group: ~p", [Group2]),
    StreamGroups2 =
        update_groups(VirtualHost,
                      Stream,
                      ConsumerName,
                      Group2,
                      StreamGroups1),

    {value, Consumer1} =
        lookup_consumer(ConnectionPid, SubscriptionId, Group2),
    notify_consumers(FormerActive, Consumer1, Group2),
    #consumer{active = Active} = Consumer1,
    {reply, {ok, Active}, State#state{groups = StreamGroups2}};
handle_call({unregister_consumer,
             VirtualHost,
             Stream,
             ConsumerName,
             ConnectionPid,
             SubscriptionId},
            _From, #state{groups = StreamGroups0} = State0) ->
    State1 =
        case lookup_group(VirtualHost, Stream, ConsumerName, StreamGroups0) of
            error ->
                State0;
            Group0 ->
                Group1 =
                    case lookup_consumer(ConnectionPid, SubscriptionId, Group0)
                    of
                        {value, Consumer} ->
                            rabbit_log:debug("Unregistering consumer ~p from group",
                                             [Consumer]),
                            G1 = remove_from_group(Consumer, Group0),
                            G2 = compute_active_consumer(G1),
                            NewActive =
                                case lookup_active_consumer(G2) of
                                    {value, AC} ->
                                        AC;
                                    false ->
                                        undefined
                                end,
                            notify_consumers(undefined, NewActive, G2),
                            G2;
                        false ->
                            rabbit_log:debug("Could not find consumer ~p ~p in group ~p ~p ~p",
                                             [ConnectionPid,
                                              SubscriptionId,
                                              VirtualHost,
                                              Stream,
                                              ConsumerName]),
                            Group0
                    end,
                SGS = update_groups(VirtualHost,
                                    Stream,
                                    ConsumerName,
                                    Group1,
                                    StreamGroups0),
                State0#state{groups = SGS}
        end,
    {reply, ok, State1};
handle_call(which_children, _From, State) ->
    {reply, [], State}.

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
    {_, Consumers1} =
        lists:foldr(fun (C0, {Index, Cs}) when Index == ActiveConsumerIndex ->
                            C1 = C0#consumer{active = true},
                            {Index + 1, [C1 | Cs]};
                        (C0, {Index, Cs}) ->
                            C1 = C0#consumer{active = false},
                            {Index + 1, [C1 | Cs]}
                    end,
                    {0, []}, Consumers0),
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
    ok;
notify_consumers(_FormerActive,
                 #consumer{pid = ConnectionPid,
                           subscription_id = SubscriptionId} =
                     NewConsumer,
                 #group{partition_index = -1, consumers = [NewConsumer]}) ->
    ConnectionPid
    ! {sac,
       {{subscription_id, SubscriptionId}, {active, true},
        {side_effects, []}}};
notify_consumers(_FormerActive,
                 #consumer{pid = ConnectionPid,
                           subscription_id = SubscriptionId} =
                     NewConsumer,
                 #group{partition_index = -1, consumers = [NewConsumer | _]}) ->
    ConnectionPid
    ! {sac,
       {{subscription_id, SubscriptionId}, {active, true},
        {side_effects, []}}};
notify_consumers(_FormerActive,
                 #consumer{pid = ConnectionPid,
                           subscription_id = SubscriptionId},
                 #group{partition_index = -1, consumers = _}) ->
    ConnectionPid
    ! {sac,
       {{subscription_id, SubscriptionId}, {active, false},
        {side_effects, []}}};
notify_consumers(undefined = _FormerActive,
                 #consumer{pid = ConnectionPid,
                           subscription_id = SubscriptionId},
                 _Group) ->
    ConnectionPid
    ! {sac,
       {{subscription_id, SubscriptionId}, {active, true},
        {side_effects, []}}};
notify_consumers(#consumer{pid = FormerConnPid,
                           subscription_id = FormerSubId},
                 #consumer{pid = NewConnPid, subscription_id = NewSubId},
                 _Group) ->
    FormerConnPid
    ! {sac,
       {{subscription_id, FormerSubId}, {active, false},
        {side_effects,
         [{message, NewConnPid,
           {sac,
            {{subscription_id, NewSubId}, {active, true},
             {side_effects, []}}}}]}}}.

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

handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
