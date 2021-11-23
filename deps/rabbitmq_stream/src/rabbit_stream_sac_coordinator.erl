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
    StreamGroups1 =
        maybe_create_group(VirtualHost,
                           Stream,
                           PartitionIndex,
                           ConsumerName,
                           StreamGroups0),
    Group0 =
        lookup_group(VirtualHost, Stream, ConsumerName, StreamGroups1),
    Consumer =
        #consumer{pid = ConnectionPid,
                  subscription_id = SubscriptionId,
                  active = false},
    Group1 = add_to_group(Consumer, Group0),
    Active = compute_active_flag(Consumer, Group1),
    #group{consumers = Consumers0} = Group1,
    Consumers1 = update_active_flag(Consumer, Active, Consumers0),
    StreamGroups2 =
        update_groups(VirtualHost,
                      Stream,
                      ConsumerName,
                      Group1#group{consumers = Consumers1},
                      StreamGroups1),
    ConnectionPid
    ! {sac,
       {{subscription_id, SubscriptionId}, {active, Active},
        {side_effects, []}}},

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
                #group{consumers = Consumers0} = Group0,
                Consumers1 =
                    case lists:search(fun(#consumer{pid = ConnPid,
                                                    subscription_id = SubId}) ->
                                         ConnPid == ConnectionPid
                                         andalso SubId == SubscriptionId
                                      end,
                                      Consumers0)
                    of
                        {value, Consumer} ->
                            rabbit_log:debug("Unregistering consumer ~p from group",
                                             [Consumer]),
                            case Consumer of
                                #consumer{active = true} ->
                                    rabbit_log:debug("Unregistering the active consumer"),
                                    %% this is active one, remove it and notify the new active one if group not empty
                                    Cs = lists:delete(Consumer, Consumers0),
                                    case Cs of
                                        [] ->
                                            %% group is empty now
                                            rabbit_log:debug("Group is now empty"),
                                            Cs;
                                        _ ->
                                            %% get new active one (the first) and notify it
                                            NewActive = lists:nth(1, Cs),
                                            #consumer{pid = Pid,
                                                      subscription_id = SubId} =
                                                NewActive,
                                            rabbit_log:debug("New active consumer is ~p ~p",
                                                             [Pid, SubId]),
                                            Pid
                                            ! {sac,
                                               {{subscription_id, SubId},
                                                {active, true},
                                                {side_effects, []}}},
                                            update_active_flag(NewActive, true,
                                                               Cs)
                                    end;
                                _ActiveConsumer ->
                                    rabbit_log:debug("Not the active consumer, just removing it from "
                                                     "the group"),
                                    lists:delete(Consumer, Consumers0)
                            end;
                        error ->
                            rabbit_log:debug("Could not find consumer ~p ~p in group ~p ~p ~p",
                                             [ConnectionPid,
                                              SubscriptionId,
                                              VirtualHost,
                                              Stream,
                                              ConsumerName]),
                            Consumers0
                    end,
                SGS = update_groups(VirtualHost,
                                    Stream,
                                    ConsumerName,
                                    Group0#group{consumers = Consumers1},
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

compute_active_flag(Consumer,
                    #group{partition_index = -1, consumers = [Consumer]}) ->
    true;
compute_active_flag(Consumer,
                    #group{partition_index = -1, consumers = [Consumer | _]}) ->
    true;
compute_active_flag(_, _) ->
    false.

update_active_flag(Consumer, Active, Consumers) ->
    lists:foldl(fun (C, Acc) when C == Consumer ->
                        Acc ++ [Consumer#consumer{active = Active}];
                    (C, Acc) ->
                        Acc ++ [C]
                end,
                [], Consumers).

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
