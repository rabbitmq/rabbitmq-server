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
-export([register_consumer/4]).

-type stream() :: binary().
-type consumer_name() :: binary().
-type subscription_id() :: byte().

-record(consumer,
        {pid :: pid(), subscription_id :: subscription_id()}).
-record(group, {consumers :: [#consumer{}]}).
-record(stream_groups, {groups :: #{consumer_name() => #group{}}}).
-record(state, {stream_groups :: #{stream() => #stream_groups{}}}).

register_consumer(Stream,
                  ConsumerName,
                  ConnectionPid,
                  SubscriptionId) ->
    call({register_consumer,
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
    {ok, #state{stream_groups = #{}}}.

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
             Stream,
             ConsumerName,
             ConnectionPid,
             SubscriptionId},
            _From, #state{stream_groups = StreamGroups0} = State) ->
    StreamGroups1 =
        maybe_create_group(Stream, ConsumerName, StreamGroups0),
    Group0 = lookup_group(Stream, ConsumerName, StreamGroups1),
    Consumer =
        #consumer{pid = ConnectionPid, subscription_id = SubscriptionId},
    Group = add_to_group(Consumer, Group0),
    Active = is_active(Consumer, Group),
    StreamGroups2 =
        update_groups(Stream, ConsumerName, Group, StreamGroups1),
    {reply, {ok, Active}, State#state{stream_groups = StreamGroups2}};
handle_call(which_children, _From, State) ->
    {reply, [], State}.

maybe_create_group(Stream, ConsumerName, StreamGroups) ->
    case StreamGroups of
        #{Stream := #stream_groups{groups = #{ConsumerName := _Consumers}}} ->
            %% the group already exists
            StreamGroups;
        #{Stream := #stream_groups{groups = GroupsForTheStream} = SG} ->
            %% there are groups for this streams, but not one for this consumer name
            GroupsForTheStream1 =
                maps:put(ConsumerName, #group{consumers = []},
                         GroupsForTheStream),
            StreamGroups#{Stream =>
                              SG#stream_groups{groups = GroupsForTheStream1}};
        SGS ->
            SG = maps:get(Stream, SGS, #stream_groups{groups = #{}}),
            #stream_groups{groups = Groups} = SG,
            Groups1 = maps:put(ConsumerName, #group{consumers = []}, Groups),
            SGS#{Stream => SG#stream_groups{groups = Groups1}}
    end.

lookup_group(Stream, ConsumerName, StreamGroups) ->
    case StreamGroups of
        #{Stream := #stream_groups{groups = #{ConsumerName := Group}}} ->
            Group;
        _ ->
            error
    end.

add_to_group(Consumer, #group{consumers = Consumers} = Group) ->
    Group#group{consumers = Consumers ++ [Consumer]}.

is_active(Consumer, #group{consumers = [Consumer]}) ->
    true;
is_active(Consumer, #group{consumers = [Consumer | _]}) ->
    true;
is_active(_, _) ->
    false.

update_groups(Stream, ConsumerName, Group, StreamGroups) ->
    #{Stream := #stream_groups{groups = Groups}} = StreamGroups,
    Groups1 = maps:put(ConsumerName, Group, Groups),
    StreamGroups#{Stream => #stream_groups{groups = Groups1}}.

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
