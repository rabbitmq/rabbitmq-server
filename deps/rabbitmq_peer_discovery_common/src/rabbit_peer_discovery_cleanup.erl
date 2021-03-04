%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% The Initial Developer of the Original Code is AWeber Communications.
%% Copyright (c) 2015-2016 AWeber Communications
%% Copyright (c) 2016-2021 VMware, Inc. or its affiliates. All rights reserved.
%%
-module(rabbit_peer_discovery_cleanup).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").
-include("include/rabbit_peer_discovery.hrl").

-export([start_link/0,
         check_cluster/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-ifdef(TEST).
-compile(export_all).
-endif.

-define(CONFIG_MODULE, rabbit_peer_discovery_config).
-define(CONFIG_KEY, node_cleanup).

-define(CONFIG_MAPPING,
        #{
          cleanup_interval   => #peer_discovery_config_entry_meta{
                                   type          = integer,
                                   env_variable  = "CLEANUP_INTERVAL",
                                   default_value = 60
                                  },
          cleanup_only_log_warning  => #peer_discovery_config_entry_meta{
                                          type          = atom,
                                          env_variable  = "CLEANUP_ONLY_LOG_WARNING",
                                          default_value = true
                                         }
         }).

-record(state, {interval, warn_only, timer}).

%%%===================================================================
%%% API
%%%===================================================================

-spec(start_link() ->
        {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


-spec(check_cluster() ->ok).
check_cluster() ->
  ok = gen_server:call(?MODULE, check_cluster).


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
-spec(init(Args :: term()) ->
        {ok, State :: #state{}} |
        {ok, State :: #state{}, timeout() | hibernate} |
        {stop, Reason :: term()} | ignore).
init([]) ->
    Map = ?CONFIG_MODULE:config_map(?CONFIG_KEY),
    case map_size(Map) of
        0 ->
            ?LOG_INFO(
               "Peer discovery: node cleanup is disabled",
               #{domain => ?RMQLOG_DOMAIN_PEER_DIS}),
            {ok, #state{}};
        _ ->
            Interval = ?CONFIG_MODULE:get(cleanup_interval, ?CONFIG_MAPPING, Map),
            WarnOnly = ?CONFIG_MODULE:get(cleanup_only_log_warning, ?CONFIG_MAPPING, Map),
            State = #state{interval = Interval,
                           warn_only = WarnOnly,
                           timer = apply_interval(Interval)},
            WarnMsg = case WarnOnly of
                          true -> "will only log warnings";
                          false -> "will remove nodes not known to the discovery backend"
                      end,
            ?LOG_INFO(
               "Peer discovery: enabling node cleanup (~s). Check interval: ~p seconds.",
               [WarnMsg, State#state.interval],
               #{domain => ?RMQLOG_DOMAIN_PEER_DIS}),
            {ok, State}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
      State :: #state{}) ->
        {reply, Reply :: term(), NewState :: #state{}} |
        {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
        {noreply, NewState :: #state{}} |
        {noreply, NewState :: #state{}, timeout() | hibernate} |
        {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
        {stop, Reason :: term(), NewState :: #state{}}).

handle_call(check_cluster, _From, State) ->
    ?LOG_DEBUG(
       "Peer discovery: checking for partitioned nodes to clean up.",
       #{domain => ?RMQLOG_DOMAIN_PEER_DIS}),
    maybe_cleanup(State),
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
        {noreply, NewState :: #state{}} |
        {noreply, NewState :: #state{}, timeout() | hibernate} |
        {stop, Reason :: term(), NewState :: #state{}}).
handle_cast(_Request, State) ->
    {noreply, State}.

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
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
             {noreply, NewState :: #state{}} |
             {noreply, NewState :: #state{}, timeout() | hibernate} |
             {stop, Reason :: term(), NewState :: #state{}}).
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
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
                State :: #state{}) -> term()).
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
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
                  Extra :: term()) ->
        {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc Create the timer that will invoke a gen_server cast for this
%%      module invoking maybe_cleanup/1 every N seconds.
%% @spec apply_interval(integer()) -> timer:tref()
%% @end
%%--------------------------------------------------------------------
-spec apply_interval(integer()) -> timer:tref().
apply_interval(Seconds) ->
    {ok, TRef} = timer:apply_interval(Seconds * 1000, ?MODULE,
                                      check_cluster, []),
    TRef.

%%--------------------------------------------------------------------
%% @private
%% @doc Fetch the list of nodes from service discovery and all of the
%%      partitioned nodes in RabbitMQ, removing any node from the
%%      partitioned list that exists in the service discovery list.
%% @spec maybe_cleanup(State :: #state{}) -> NewState :: #state{}
%% @end
%%--------------------------------------------------------------------
-spec maybe_cleanup(State :: #state{}) -> ok.
maybe_cleanup(State) ->
    maybe_cleanup(State, unreachable_nodes()).

%%--------------------------------------------------------------------
%% @private
%% @doc Fetch the list of nodes from service discovery and all of the
%%      unreachable nodes in RabbitMQ, removing any node from the
%%      unreachable list that exists in the service discovery list.
%% @spec maybe_cleanup(State :: #state{},
%%                     UnreachableNodes :: [node()]) -> ok
%% @end
%%--------------------------------------------------------------------
-spec maybe_cleanup(State :: #state{},
                    UnreachableNodes :: [node()]) -> ok.
maybe_cleanup(_, []) ->
    ?LOG_DEBUG(
       "Peer discovery: all known cluster nodes are up.",
       #{domain => ?RMQLOG_DOMAIN_PEER_DIS});
maybe_cleanup(State, UnreachableNodes) ->
    ?LOG_DEBUG(
       "Peer discovery: cleanup discovered unreachable nodes: ~p",
       [UnreachableNodes],
       #{domain => ?RMQLOG_DOMAIN_PEER_DIS}),
    case lists:subtract(UnreachableNodes, service_discovery_nodes()) of
        [] ->
            ?LOG_DEBUG(
               "Peer discovery: all unreachable nodes are still "
               "registered with the discovery backend ~p",
               [rabbit_peer_discovery:backend()],
               #{domain => ?RMQLOG_DOMAIN_PEER_DIS}),
            ok;
        Nodes ->
            ?LOG_DEBUG(
               "Peer discovery: unreachable nodes are not registered "
               "with the discovery backend ~p", [Nodes],
               #{domain => ?RMQLOG_DOMAIN_PEER_DIS}),
            maybe_remove_nodes(Nodes, State#state.warn_only)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc Iterate over the list of partitioned nodes, either logging the
%%      node that would be removed or actually removing it.
%% @spec maybe_remove_nodes(PartitionedNodes :: [node()],
%%                          WarnOnly :: true | false) -> ok
%% @end
%%--------------------------------------------------------------------
-spec maybe_remove_nodes(PartitionedNodes :: [node()],
                         WarnOnly :: true | false) -> ok.
maybe_remove_nodes([], _) -> ok;
maybe_remove_nodes([Node | Nodes], true) ->
    ?LOG_WARNING(
       "Peer discovery: node ~s is unreachable", [Node],
       #{domain => ?RMQLOG_DOMAIN_PEER_DIS}),
    maybe_remove_nodes(Nodes, true);
maybe_remove_nodes([Node | Nodes], false) ->
    ?LOG_WARNING(
       "Peer discovery: removing unknown node ~s from the cluster", [Node],
       #{domain => ?RMQLOG_DOMAIN_PEER_DIS}),
    rabbit_mnesia:forget_cluster_node(Node, false),
    maybe_remove_nodes(Nodes, false).

%%--------------------------------------------------------------------
%% @private
%% @doc Return nodes in the RabbitMQ cluster that are unhealthy.
%% @spec unreachable_nodes() -> [node()]
%% @end
%%--------------------------------------------------------------------
-spec unreachable_nodes() -> [node()].
unreachable_nodes() ->
    Status = rabbit_mnesia:status(),
    Nodes = proplists:get_value(nodes, Status, []),
    Running = proplists:get_value(running_nodes, Status, []),
    All = lists:merge(proplists:get_value(disc, Nodes, []),
                      proplists:get_value(ram, Nodes, [])),
    lists:subtract(All, Running).


%%--------------------------------------------------------------------
%% @private
%% @doc Return the nodes that the service discovery backend knows about
%% @spec service_discovery_nodes() -> [node()]
%% @end
%%--------------------------------------------------------------------
-spec service_discovery_nodes() -> [node()].
service_discovery_nodes() ->
    Module = rabbit_peer_discovery:backend(),
    case rabbit_peer_discovery:normalize(Module:list_nodes()) of
        {ok, {Nodes, _Type}} ->
            ?LOG_DEBUG(
               "Peer discovery cleanup: ~p returned ~p",
               [Module, Nodes],
               #{domain => ?RMQLOG_DOMAIN_PEER_DIS}),
            Nodes;
        {error, Reason} ->
            ?LOG_DEBUG(
               "Peer discovery cleanup: ~p returned error ~p",
               [Module, Reason],
               #{domain => ?RMQLOG_DOMAIN_PEER_DIS}),
            []
    end.
