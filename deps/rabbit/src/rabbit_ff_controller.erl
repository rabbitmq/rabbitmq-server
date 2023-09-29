%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @author The RabbitMQ team
%% @copyright 2023 VMware, Inc. or its affiliates.
%%
%% @doc
%% The feature flag controller is responsible for synchronization and managing
%% concurrency when feature flag states are changed.
%%
%% It makes sure only one node can enable feature flags or synchronize its
%% feature flag states with other nodes at a time. If another node wants to
%% enable a feature flag or if it joins a cluster and needs to synchronize its
%% feature flag states, it will wait for the current task to finish.
%%
%% The feature flag controller is used as soon as the `feature_flags_v2'
%% feature flag is enabled.
%%
%% Compared to the initial subsystem, it also introduces a new migration
%% function signature; See {@link rabbit_feature_flags:migration_fun_v2()}.

-module(rabbit_ff_controller).
-behaviour(gen_statem).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include_lib("rabbit_common/include/logging.hrl").

-include("src/rabbit_feature_flags.hrl").

-export([is_supported/1, is_supported/2,
         enable/1,
         enable_default/0,
         check_node_compatibility/1,
         sync_cluster/1,
         refresh_after_app_load/0,
         get_forced_feature_flag_names/0]).

%% Internal use only.
-export([start/0,
         start_link/0,
         rpc_call/5,
         all_nodes/0,
         running_nodes/0,
         collect_inventory_on_nodes/1, collect_inventory_on_nodes/2,
         mark_as_enabled_on_nodes/4,
         wait_for_task_and_stop/0]).

%% gen_statem callbacks.
-export([callback_mode/0,
         init/1,
         terminate/3,
         code_change/4,

         standing_by/3,
         waiting_for_end_of_controller_task/3,
         updating_feature_flag_states/3]).

-record(?MODULE, {from,
                  notify = #{}}).

-type run_callback_error() :: {error, any()}.

-define(LOCAL_NAME, ?MODULE).
-define(GLOBAL_NAME, {?MODULE, global}).

%% Default timeout for operations on remote nodes.
-define(TIMEOUT, 60000).

start() ->
    gen_statem:start({local, ?LOCAL_NAME}, ?MODULE, none, []).

start_link() ->
    gen_statem:start_link({local, ?LOCAL_NAME}, ?MODULE, none, []).

wait_for_task_and_stop() ->
    gen_statem:stop(?LOCAL_NAME).

is_supported(FeatureNames) ->
    is_supported(FeatureNames, ?TIMEOUT).

is_supported(FeatureName, Timeout) when is_atom(FeatureName) ->
    is_supported([FeatureName], Timeout);
is_supported(FeatureNames, Timeout) when is_list(FeatureNames) ->
    Nodes = running_nodes(),
    case collect_inventory_on_nodes(Nodes, Timeout) of
        {ok, Inventory} ->
            lists:all(
              fun(FeatureName) ->
                      is_known_and_supported(Inventory, FeatureName)
              end,
              FeatureNames);
        _Error ->
            false
    end.

enable(FeatureName) when is_atom(FeatureName) ->
    enable([FeatureName]);
enable(FeatureNames) when is_list(FeatureNames) ->
    ?LOG_DEBUG(
       "Feature flags: REQUEST TO ENABLE: ~tp",
       [FeatureNames],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    gen_statem:call(?LOCAL_NAME, {enable, FeatureNames}).

enable_default() ->
    ?LOG_DEBUG(
       "Feature flags: configure initial feature flags state",
       [],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    case erlang:whereis(?LOCAL_NAME) of
        Pid when is_pid(Pid) ->
            %% The function is called while `rabbit' is running.
            gen_statem:call(?LOCAL_NAME, enable_default);
        undefined ->
            %% The function is called while `rabbit' is stopped. We need to
            %% start a one-off controller, again to make sure concurrent
            %% changes are blocked.
            {ok, Pid} = start_link(),
            Ret = gen_statem:call(Pid, enable_default),
            gen_statem:stop(Pid),
            Ret
    end.

check_node_compatibility(RemoteNode) ->
    ThisNode = node(),
    ?LOG_DEBUG(
       "Feature flags: CHECKING COMPATIBILITY between nodes `~ts` and `~ts`",
       [ThisNode, RemoteNode],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    %% We don't go through the controller process to check nodes compatibility
    %% because this function is used while `rabbit' is stopped usually.
    %%
    %% There is no benefit in starting a controller just for this check
    %% because it would not guaranty that the compatibility remains true after
    %% this function finishes and before the node starts and synchronizes
    %% feature flags.
    check_node_compatibility_task(ThisNode, RemoteNode).

sync_cluster(Nodes) ->
    ?LOG_DEBUG(
       "Feature flags: SYNCING FEATURE FLAGS in cluster...",
       [],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    case erlang:whereis(?LOCAL_NAME) of
        Pid when is_pid(Pid) ->
            %% The function is called while `rabbit' is running.
            gen_statem:call(?LOCAL_NAME, {sync_cluster, Nodes});
        undefined ->
            %% The function is called while `rabbit' is stopped. We need to
            %% start a one-off controller, again to make sure concurrent
            %% changes are blocked.
            {ok, Pid} = start_link(),
            Ret = gen_statem:call(Pid, {sync_cluster, Nodes}),
            gen_statem:stop(Pid),
            Ret
    end.

refresh_after_app_load() ->
    ?LOG_DEBUG(
       "Feature flags: REFRESHING after applications load...",
       [],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    gen_statem:call(?LOCAL_NAME, refresh_after_app_load).

%% --------------------------------------------------------------------
%% gen_statem callbacks.
%% --------------------------------------------------------------------

callback_mode() ->
    state_functions.

init(_Args) ->
    {ok, standing_by, none}.

standing_by(
  {call, From} = EventType, EventContent, none)
  when EventContent =/= notify_when_done ->
    ?LOG_DEBUG(
       "Feature flags: registering controller globally before "
       "proceeding with task: ~tp",
       [EventContent],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),

    %% The first step is to register this process globally (it is already
    %% registered locally). The purpose is to make sure this one takes full
    %% control on feature flag changes among other controllers.
    %%
    %% This is useful for situations where a new node joins the cluster while
    %% a feature flag is being enabled. In this case, when that new node joins
    %% and its controller wants to synchronize feature flags, it will block
    %% and wait for this one to finish.
    case register_globally() of
        yes ->
            %% We would register the process globally. Therefore we can
            %% proceed with enabling/syncing feature flags.
            ?LOG_DEBUG(
               "Feature flags: controller globally registered; can proceed "
               "with task",
               [],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),

            Data = #?MODULE{from = From},
            {next_state, updating_feature_flag_states, Data,
             [{next_event, internal, EventContent}]};

        no ->
            %% Another controller is globally registered. We ask that global
            %% controller to notify us when it is done, and we wait for its
            %% response.
            ?LOG_DEBUG(
               "Feature flags: controller NOT globally registered; need to "
               "wait for the current global controller's task to finish",
               [],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),

            RequestId = notify_me_when_done(),
            {next_state, waiting_for_end_of_controller_task, RequestId,
             [{next_event, EventType, EventContent}]}
    end;
standing_by(
  {call, From}, notify_when_done, none) ->
    %% This state is entered when a globally-registered controller finished
    %% its task but had unhandled `notify_when_done` requests in its inbox. We
    %% just need to notify the caller that it can proceed.
    notify_waiting_controller(From),
    {keep_state_and_data, []}.

waiting_for_end_of_controller_task(
  {call, _From}, _EventContent, _RequestId) ->
    {keep_state_and_data, [postpone]};
waiting_for_end_of_controller_task(
  info, Msg, RequestId) ->
    case gen_statem:check_response(Msg, RequestId) of
        {reply, done} ->
            ?LOG_DEBUG(
               "Feature flags: current global controller's task finished; "
               "trying to take next turn",
               [],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            {next_state, standing_by, none, []};
        {error, Reason} ->
            ?LOG_DEBUG(
               "Feature flags: error while waiting for current global "
               "controller's task: ~0p; trying to take next turn",
               [Reason],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            {next_state, standing_by, none, []};
        no_reply ->
            ?LOG_DEBUG(
               "Feature flags: unknown message while waiting for current "
               "global controller's task: ~0p; still waiting",
               [Msg],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            keep_state_and_data
    end.

updating_feature_flag_states(
  internal, Task, #?MODULE{from = From} = Data) ->
    Reply = proceed_with_task(Task),

    ?LOG_DEBUG(
       "Feature flags: unregistering controller globally",
       [],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    unregister_globally(),
    notify_waiting_controllers(Data),
    {next_state, standing_by, none, [{reply, From, Reply}]};
updating_feature_flag_states(
  {call, From}, notify_when_done, #?MODULE{notify = Notify} = Data) ->
    Notify1 = Notify#{From => true},
    Data1 = Data#?MODULE{notify = Notify1},
    {keep_state, Data1}.

proceed_with_task({enable, FeatureNames}) ->
    enable_task(FeatureNames);
proceed_with_task(enable_default) ->
    enable_default_task();
proceed_with_task({sync_cluster, Nodes}) ->
    sync_cluster_task(Nodes);
proceed_with_task(refresh_after_app_load) ->
    refresh_after_app_load_task().

terminate(_Reason, _State, _Data) ->
    %% We block until a possible in-flight operation finishes. This increases
    %% the chance that an in-flight `enable' operation finishes and records the
    %% new feature flag state on this node before the `rabbit' application
    %% stops or this Erlang node exits.
    wait_for_in_flight_operations(),
    ok.

wait_for_in_flight_operations() ->
    case register_globally() of
        yes ->
            %% We don't unregister so the controller holds the lock until it
            %% exits.
            ?LOG_DEBUG(
               "Feature flags: controller exiting, no in-flight operations",
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            ok;
        no ->
            ?LOG_DEBUG(
               "Feature flags: can't stop local controller while there is "
               "an in-flight operation; waiting for it to finish",
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            RequestId = notify_me_when_done(),
            _ = gen_statem:wait_response(RequestId, infinity),
            wait_for_in_flight_operations()
    end.

code_change(_OldVsn, OldState, OldData, _Extra) ->
    {ok, OldState, OldData}.

%% --------------------------------------------------------------------
%% Global name registration.
%% --------------------------------------------------------------------

register_globally() ->
    ?LOG_DEBUG(
       "Feature flags: [global sync] @ ~ts",
       [node()],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    ok = rabbit_node_monitor:global_sync(),
    ?LOG_DEBUG(
       "Feature flags: [global register] @ ~ts",
       [node()],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    global:register_name(?GLOBAL_NAME, self()).

unregister_globally() ->
    ?LOG_DEBUG(
       "Feature flags: [global unregister] @ ~ts",
       [node()],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    _ = global:unregister_name(?GLOBAL_NAME),
    ok.

notify_me_when_done() ->
    gen_statem:send_request({global, ?GLOBAL_NAME}, notify_when_done).

notify_waiting_controllers(#?MODULE{notify = Notify}) ->
    maps:fold(
      fun(From, true, Acc) ->
              notify_waiting_controller(From),
              Acc
      end, ok, Notify).

notify_waiting_controller({ControlerPid, _} = From) ->
    ControlerNode = node(ControlerPid),
    ?LOG_DEBUG(
       "Feature flags: controller's task finished; notify waiting controller "
       "on node ~tp",
       [ControlerNode],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    gen_statem:reply(From, done).

%% --------------------------------------------------------------------
%% Code to check compatibility between nodes.
%% --------------------------------------------------------------------

-spec check_node_compatibility_task(Node, Node) -> Ret when
      Node :: node(),
      Ret :: ok | {error, Reason},
      Reason :: incompatible_feature_flags.

check_node_compatibility_task(NodeA, NodeB) ->
    ?LOG_NOTICE(
       "Feature flags: checking nodes `~ts` and `~ts` compatibility...",
       [NodeA, NodeB],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    NodesA = list_nodes_clustered_with(NodeA),
    case NodesA of
        _ when is_list(NodesA) ->
            NodesB = list_nodes_clustered_with(NodeB),
            case NodesB of
                _ when is_list(NodesB) ->
                    check_node_compatibility_task1(
                      NodeA, NodesA,
                      NodeB, NodesB);
                Error ->
                    ?LOG_WARNING(
                       "Feature flags: "
                       "error while querying cluster members from "
                       "node `~ts`:~n~tp",
                       [NodeB, Error],
                       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
                    {error, {aborted_feature_flags_compat_check, Error}}
            end;
        Error ->
            ?LOG_WARNING(
               "Feature flags: "
               "error while querying cluster members from node `~ts`:~n~tp",
               [NodeA, Error],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            {error, {aborted_feature_flags_compat_check, Error}}
    end.

check_node_compatibility_task1(NodeA, NodesA, NodeB, NodesB)
  when is_list(NodesA) andalso is_list(NodesB) ->
    case collect_inventory_on_nodes(NodesA) of
        {ok, InventoryA} ->
            ?LOG_DEBUG(
               "Feature flags: inventory of node `~ts`:~n~tp",
               [NodeA, InventoryA],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            case collect_inventory_on_nodes(NodesB) of
                {ok, InventoryB} ->
                    ?LOG_DEBUG(
                       "Feature flags: inventory of node "
                       "`~ts`:~n~tp",
                       [NodeB, InventoryB],
                       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
                    case are_compatible(InventoryA, InventoryB) of
                        true ->
                            ?LOG_NOTICE(
                               "Feature flags: "
                               "nodes `~ts` and `~ts` are compatible",
                               [NodeA, NodeB],
                               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
                            ok;
                        false ->
                            ?LOG_WARNING(
                               "Feature flags: "
                               "nodes `~ts` and `~ts` are incompatible",
                               [NodeA, NodeB],
                               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
                            {error, incompatible_feature_flags}
                    end;
                Error ->
                    ?LOG_WARNING(
                       "Feature flags: "
                       "error while collecting inventory from "
                       "nodes ~0tp:~n~tp",
                       [NodesB, Error],
                       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
                    {error, {aborted_feature_flags_compat_check, Error}}
            end;
        Error ->
            ?LOG_WARNING(
               "Feature flags: "
               "error while collecting inventory from nodes ~0tp:~n~tp",
               [NodesA, Error],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            {error, {aborted_feature_flags_compat_check, Error}}
    end.

-spec list_nodes_clustered_with(Node) -> Ret when
      Node :: node(),
      Ret :: Members | Error,
      Members :: [node()],
      Error :: {error, term()}.

list_nodes_clustered_with(Node) ->
    %% If `running_nodes()' returns an empty list, it means the `rabbit'
    %% application is not running on `Node'. In this case, we consider this
    %% node alone for now.
    %%
    %% It could be that RabbitMQ is starting on that node for instance;
    %% indeed, feature flags compatibility is checked as part of RabbitMQ
    %% booting. If that's not the case, collecting the feature flags inventory
    %% later will fail anyway.
    case rpc_call(Node, ?MODULE, running_nodes, [], ?TIMEOUT) of
        []          -> [Node];
        ListOrError -> ListOrError
    end.

-spec are_compatible(Inventory, Inventory) -> AreCompatible when
      Inventory :: rabbit_feature_flags:cluster_inventory(),
      AreCompatible :: boolean().

are_compatible(InventoryA, InventoryB) ->
    check_one_way_compatibility(InventoryA, InventoryB)
    andalso
    check_one_way_compatibility(InventoryB, InventoryA).

-spec check_one_way_compatibility(Inventory, Inventory) -> AreCompatible when
      Inventory :: rabbit_feature_flags:cluster_inventory(),
      AreCompatible :: boolean().

check_one_way_compatibility(InventoryA, InventoryB) ->
    %% This function checks the compatibility in one way, "inventory A" is
    %% compatible with "inventory B". This is true if all feature flags
    %% enabled in "inventory B" are supported by "inventory A".
    %%
    %% They don't need to be enabled on both side: the feature flags states
    %% will be synchronized by `sync_cluster()'.
    FeatureNames = list_feature_flags_enabled_somewhere(InventoryB, true),
    lists:all(
      fun(FeatureName) ->
              #{feature_flags := #{FeatureName := FeatureFlag}} = InventoryB,
              not is_known(InventoryA, FeatureFlag)
              orelse
              is_known_and_supported(InventoryA, FeatureName)
      end, FeatureNames).

%% --------------------------------------------------------------------
%% Code to enable and sync feature flags.
%% --------------------------------------------------------------------

-spec enable_task(FeatureNames) -> Ret when
      FeatureNames :: [rabbit_feature_flags:feature_name()],
      Ret :: ok | {error, Reason},
      Reason :: term().

enable_task(FeatureNames) ->
    %% We take a snapshot of clustered nodes, including stopped nodes, at the
    %% beginning of the process and use that list at all time.
    %%
    %% If there are some missing, unreachable or stopped nodes, we abort the
    %% task and refuse to enable the feature flag(s). This is to make sure
    %% that if there is a network partition, the other side of the partition
    %% doesn't "rejoin" the cluster with some feature flags enabled behind the
    %% scene.
    %%
    %% TODO: Should we remove the requirement above and call `sync_cluster()'
    %% when a network partition is repaired?
    %%
    %% If a node tries to join the cluster during the task, it will block
    %% because it will want to synchronize its feature flags state with the
    %% cluster. For that to happen, this controller `enable' task needs to
    %% finish before the synchronization task starts.
    %%
    %% If a node stops during the task, this will trigger an RPC error at some
    %% point and the task will abort. That's ok because migration functions
    %% are supposed to be idempotent.
    AllNodes = all_nodes(),
    RunningNodes = running_nodes(),
    case RunningNodes of
        AllNodes ->
            ?LOG_DEBUG(
               "Feature flags: nodes where the feature flags will be "
               "enabled: ~tp~n"
               "Feature flags: new nodes joining the cluster in between "
               "will wait and synchronize their feature flag states after.",
               [AllNodes],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),

            %% Likewise, we take a snapshot of the feature flags states on all
            %% running nodes right from the beginning. This is what we use
            %% during the entire task to determine if feature flags are
            %% supported, enabled somewhere, etc.
            case collect_inventory_on_nodes(AllNodes) of
                {ok, Inventory} -> enable_many(Inventory, FeatureNames);
                Error           -> Error
            end;
        _ ->
            ?LOG_ERROR(
               "Feature flags: refuse to enable feature flags while "
               "clustered nodes are missing, stopped or unreachable: ~tp",
               [AllNodes -- RunningNodes]),
            {error, missing_clustered_nodes}
    end.

enable_default_task() ->
    FeatureNames = get_forced_feature_flag_names(),
    case FeatureNames of
        undefined ->
            ?LOG_DEBUG(
              "Feature flags: starting an unclustered node for the first "
              "time: all stable feature flags will be enabled by default",
              #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            {ok, Inventory} = collect_inventory_on_nodes([node()]),
            #{feature_flags := FeatureFlags} = Inventory,
            StableFeatureNames =
            maps:fold(
              fun(FeatureName, FeatureProps, Acc) ->
                      Stability = rabbit_feature_flags:get_stability(
                                    FeatureProps),
                      case Stability of
                          stable -> [FeatureName | Acc];
                          _      -> Acc
                      end
              end, [], FeatureFlags),
            enable_many(Inventory, StableFeatureNames);
        [] ->
            ?LOG_DEBUG(
              "Feature flags: starting an unclustered node for the first "
              "time: all feature flags are forcibly left disabled from "
              "the $RABBITMQ_FEATURE_FLAGS environment variable",
              #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            ok;
        _ ->
            ?LOG_DEBUG(
               "Feature flags: starting an unclustered node for the first "
               "time: only the following feature flags specified in the "
               "$RABBITMQ_FEATURE_FLAGS environment variable will be enabled: "
               "~tp",
               [FeatureNames],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            {ok, Inventory} = collect_inventory_on_nodes([node()]),
            enable_many(Inventory, FeatureNames)
    end.

-spec get_forced_feature_flag_names() -> Ret when
      Ret :: FeatureNames | undefined,
      FeatureNames :: [rabbit_feature_flags:feature_name()].
%% @doc Returns the (possibly empty) list of feature flags the user wants to
%% enable out-of-the-box when starting a node for the first time.
%%
%% Without this, the default is to enable all the supported stable feature
%% flags.
%%
%% There are two ways to specify that list:
%% <ol>
%% <li>Using the `$RABBITMQ_FEATURE_FLAGS' environment variable; for
%%   instance `RABBITMQ_FEATURE_FLAGS=quorum_queue,mnevis'.</li>
%% <li>Using the `forced_feature_flags_on_init' configuration parameter;
%%   for instance
%%   `{rabbit, [{forced_feature_flags_on_init, [quorum_queue, mnevis]}]}'.</li>
%% </ol>
%%
%% The environment variable has precedence over the configuration parameter.
%%
%% @private

get_forced_feature_flag_names() ->
    Ret = case get_forced_feature_flag_names_from_env() of
              undefined -> get_forced_feature_flag_names_from_config();
              List      -> List
          end,
    case Ret of
        undefined ->
            ok;
        [] ->
            ?LOG_INFO(
               "Feature flags: automatic enablement of feature flags "
               "disabled (i.e. none will be enabled automatically)",
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS});
        _ ->
            ?LOG_INFO(
               "Feature flags: automatic enablement of feature flags "
               "limited to the following list: ~tp",
               [Ret],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS})
    end,
    Ret.

-spec get_forced_feature_flag_names_from_env() -> Ret when
      Ret :: FeatureNames | undefined,
      FeatureNames :: [rabbit_feature_flags:feature_name()].
%% @private

get_forced_feature_flag_names_from_env() ->
    case rabbit_prelaunch:get_context() of
        #{forced_feature_flags_on_init := ForcedFFs}
          when is_list(ForcedFFs) ->
            ForcedFFs;
        _ ->
            undefined
    end.

-spec get_forced_feature_flag_names_from_config() -> Ret when
      Ret :: FeatureNames | undefined,
      FeatureNames :: [rabbit_feature_flags:feature_name()].
%% @private

get_forced_feature_flag_names_from_config() ->
    Value = application:get_env(
              rabbit, forced_feature_flags_on_init, undefined),
    case Value of
        undefined ->
            Value;
        _ when is_list(Value) ->
            case lists:all(fun is_atom/1, Value) of
                true  -> Value;
                false -> undefined
            end;
        _ ->
            undefined
    end.

-spec sync_cluster_task() -> Ret when
      Ret :: ok | {error, Reason},
      Reason :: term().

sync_cluster_task() ->
    Nodes = running_nodes(),
    sync_cluster_task(Nodes).

-spec sync_cluster_task(Nodes) -> Ret when
      Nodes :: [node()],
      Ret :: ok | {error, Reason},
      Reason :: term().

sync_cluster_task(Nodes) ->
    %% We assume that a feature flag can only be enabled, not disabled.
    %% Therefore this synchronization searches for feature flags enabled on
    %% some nodes but not all, and make sure they are enabled everywhere.
    %%
    %% This happens when a node joins a cluster and that node has a different
    %% set of enabled feature flags.
    %%
    %% FIXME: `enable_task()' requires that all nodes in the cluster run to
    %% enable anything. Should we require the same here? On one hand, this
    %% would make sure a feature flag isn't enabled while there is a network
    %% partition. On the other hand, this would require that all nodes are
    %% running before we can expand the cluster...
    ?LOG_DEBUG(
       "Feature flags: synchronizing feature flags on nodes: ~tp",
       [Nodes],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),

    case collect_inventory_on_nodes(Nodes) of
        {ok, Inventory} ->
            CantEnable = list_deprecated_features_that_cant_be_denied(
                           Inventory),
            case CantEnable of
                [] ->
                    FeatureNames = list_feature_flags_enabled_somewhere(
                                     Inventory, false),
                    enable_many(Inventory, FeatureNames);
                _ ->
                    ?LOG_ERROR(
                       "Feature flags: the following deprecated features "
                       "can't be denied because their associated feature "
                       "is being actively used: ~0tp",
                       [CantEnable],
                       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
                    lists:foreach(
                      fun(FeatureName) ->
                              Warning =
                              rabbit_deprecated_features:get_warning(
                                FeatureName),
                              ?LOG_ERROR(
                                 "~ts", [Warning],
                                 #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS})
                      end, CantEnable),
                    {error,
                     {failed_to_deny_deprecated_features, CantEnable}}
            end;
        Error ->
            Error
    end.

-spec refresh_after_app_load_task() -> Ret when
      Ret :: ok | {error, Reason},
      Reason :: term().

refresh_after_app_load_task() ->
    case rabbit_ff_registry_factory:initialize_registry() of
        ok    -> sync_cluster_task();
        Error -> Error
    end.

-spec enable_many(Inventory, FeatureNames) -> Ret when
      Inventory :: rabbit_feature_flags:cluster_inventory(),
      FeatureNames :: [rabbit_feature_flags:feature_name()],
      Ret :: ok | {error, Reason},
      Reason :: term().

enable_many(#{states_per_node := _} = Inventory, [FeatureName | Rest]) ->
    case enable_if_supported(Inventory, FeatureName) of
        {ok, Inventory1} -> enable_many(Inventory1, Rest);
        Error            -> Error
    end;
enable_many(_Inventory, []) ->
    ok.

-spec enable_if_supported(Inventory, FeatureName) -> Ret when
      Inventory :: rabbit_feature_flags:cluster_inventory(),
      FeatureName :: rabbit_feature_flags:feature_name(),
      Ret :: {ok, NewInventory} | {error, Reason},
      NewInventory :: rabbit_feature_flags:cluster_inventory(),
      Reason :: term().

enable_if_supported(#{states_per_node := _} = Inventory, FeatureName) ->
    case is_known_and_supported(Inventory, FeatureName) of
        true ->
            ?LOG_DEBUG(
               "Feature flags: `~ts`: supported; continuing",
               [FeatureName],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            lock_registry_and_enable(Inventory, FeatureName);
        false ->
            ?LOG_DEBUG(
               "Feature flags: `~ts`: unsupported; aborting",
               [FeatureName],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            {error, unsupported}
    end.

-spec lock_registry_and_enable(Inventory, FeatureName) -> Ret when
      Inventory :: rabbit_feature_flags:cluster_inventory(),
      FeatureName :: rabbit_feature_flags:feature_name(),
      Ret :: {ok, Inventory} | {error, Reason},
      Reason :: term().

lock_registry_and_enable(#{states_per_node := _} = Inventory, FeatureName) ->
    %% We acquire a lock before making any change to the registry. This is not
    %% used by the controller (because it is already using a globally
    %% registered name to prevent concurrent runs). But this is used in
    %% `rabbit_feature_flags:is_enabled()' to block while the state is
    %% `state_changing'.
    rabbit_ff_registry_factory:acquire_state_change_lock(),
    Ret = enable_with_registry_locked(Inventory, FeatureName),
    rabbit_ff_registry_factory:release_state_change_lock(),
    Ret.

-spec enable_with_registry_locked(Inventory, FeatureName) -> Ret when
      Inventory :: rabbit_feature_flags:cluster_inventory(),
      FeatureName :: rabbit_feature_flags:feature_name(),
      Ret :: {ok, Inventory} | {error, Reason},
      Reason :: term().

enable_with_registry_locked(
  #{states_per_node := _} = Inventory, FeatureName) ->
    %% We verify if the feature flag needs to be enabled somewhere. This may
    %% have changed since the beginning, not because another controller
    %% enabled it (this is not possible because only one can run at a given
    %% time), but because this feature flag was already enabled as a
    %% consequence (for instance, it's a dependency of another feature flag we
    %% processed).
    Nodes = list_nodes_where_feature_flag_is_disabled(Inventory, FeatureName),
    case Nodes of
        [] ->
            ?LOG_DEBUG(
               "Feature flags: `~ts`: already enabled; skipping",
               [FeatureName],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            {ok, Inventory};
        _ ->
            ?LOG_NOTICE(
               "Feature flags: attempt to enable `~ts`...",
               [FeatureName],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),

            case check_required_and_enable(Inventory, FeatureName) of
                {ok, _Inventory} = Ok ->
                    ?LOG_NOTICE(
                       "Feature flags: `~ts` enabled",
                       [FeatureName],
                       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
                    Ok;
                Error ->
                    ?LOG_ERROR(
                       "Feature flags: failed to enable `~ts`: ~tp",
                       [FeatureName, Error],
                       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
                    Error
            end
    end.

-spec check_required_and_enable(Inventory, FeatureName) -> Ret when
      Inventory :: rabbit_feature_flags:cluster_inventory(),
      FeatureName :: rabbit_feature_flags:feature_name(),
      Ret :: {ok, Inventory} | {error, Reason},
      Reason :: term().

check_required_and_enable(
  #{feature_flags := FeatureFlags,
    states_per_node := _} = Inventory,
  FeatureName) ->
    %% Required feature flags vs. virgin nodes.
    FeatureProps = maps:get(FeatureName, FeatureFlags),
    Stability = rabbit_feature_flags:get_stability(FeatureProps),
    ProvidedBy = maps:get(provided_by, FeatureProps),
    NodesWhereDisabled = list_nodes_where_feature_flag_is_disabled(
                           Inventory, FeatureName),

    MarkDirectly = case Stability of
                       required when ProvidedBy =:= rabbit ->
                           ?LOG_DEBUG(
                              "Feature flags: `~s`: the feature flag is "
                              "required on some nodes; list virgin nodes "
                              "to determine if the feature flag can simply "
                              "be marked as enabled",
                              [FeatureName],
                              #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
                           VirginNodesWhereDisabled =
                           lists:filter(
                             fun(Node) ->
                                     case rabbit_db:is_virgin_node(Node) of
                                         IsVirgin when is_boolean(IsVirgin) ->
                                             IsVirgin;
                                         undefined ->
                                             false
                                     end
                             end, NodesWhereDisabled),
                           VirginNodesWhereDisabled =:= NodesWhereDisabled;
                       required when ProvidedBy =/= rabbit ->
                           %% A plugin can be enabled/disabled at runtime and
                           %% between restarts. Thus we have no way to
                           %% distinguish a newly enabled plugin from a plugin
                           %% which was enabled in the past.
                           %%
                           %% Therefore, we always mark required feature flags
                           %% from plugins directly as enabled. However, the
                           %% plugin is responsible for checking that its
                           %% possibly existing data is as it expects it or
                           %% perform any cleanup/conversion!
                           ?LOG_DEBUG(
                              "Feature flags: `~s`: the feature flag is "
                              "required on some nodes; it comes from a "
                              "plugin which can be enabled at runtime, "
                              "so it can be marked as enabled",
                              [FeatureName],
                              #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
                           true;
                       _ ->
                           false
                   end,

    case MarkDirectly of
        false ->
            case Stability of
                required ->
                    ?LOG_DEBUG(
                       "Feature flags: `~s`: some nodes where the feature "
                       "flag is disabled are not virgin, we need to perform "
                       "a regular sync",
                       [FeatureName],
                       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS});
                _ ->
                    ok
            end,
            update_feature_state_and_enable(Inventory, FeatureName);
        true ->
            ?LOG_DEBUG(
               "Feature flags: `~s`: all nodes where the feature flag is "
               "disabled are virgin, we can directly mark it as enabled "
               "there",
               [FeatureName],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            mark_as_enabled_on_nodes(
              NodesWhereDisabled, Inventory, FeatureName, true)
    end.

-spec update_feature_state_and_enable(Inventory, FeatureName) -> Ret when
      Inventory :: rabbit_feature_flags:cluster_inventory(),
      FeatureName :: rabbit_feature_flags:feature_name(),
      Ret :: {ok, Inventory} | {error, Reason},
      Reason :: term().

update_feature_state_and_enable(
  #{states_per_node := _} = Inventory, FeatureName) ->
    %% The feature flag is marked as `state_changing' on all running nodes who
    %% know it, including those where it's already enabled. The idea is that
    %% the feature flag is between two states on some nodes and the code
    %% running on the nodes where the feature flag enabled shouldn't assume
    %% all nodes in the cluster are in the same situation.
    Nodes = list_nodes_who_know_the_feature_flag(Inventory, FeatureName),

    %% We only consider nodes where the feature flag is disabled. The
    %% migration function is only executed on those nodes. I.e. we don't run
    %% it again where it has already been executed.
    NodesWhereDisabled = list_nodes_where_feature_flag_is_disabled(
                           Inventory, FeatureName),

    Ret1 = mark_as_enabled_on_nodes(
             Nodes, Inventory, FeatureName, state_changing),
    case Ret1 of
        %% We ignore the returned updated inventory because we don't need or
        %% even want to remember the `state_changing' state. This is only used
        %% for external queries of the registry.
        {ok, _Inventory0} ->
            case do_enable(Inventory, FeatureName, NodesWhereDisabled) of
                {ok, Inventory1} ->
                    Ret2 = mark_as_enabled_on_nodes(
                      Nodes, Inventory1, FeatureName, true),
                    case Ret2 of
                        {ok, Inventory2} ->
                            post_enable(
                              Inventory2, FeatureName, NodesWhereDisabled,
                              true),
                            Ret2;
                        Error ->
                            post_enable(
                              Inventory1, FeatureName, NodesWhereDisabled,
                              false),
                            restore_feature_flag_state(
                              Nodes, NodesWhereDisabled, Inventory1,
                              FeatureName),
                            Error
                    end;
                Error ->
                    post_enable(
                      Inventory, FeatureName, NodesWhereDisabled,
                      false),
                    restore_feature_flag_state(
                      Nodes, NodesWhereDisabled, Inventory, FeatureName),
                    Error
            end;
        Error ->
            restore_feature_flag_state(
              Nodes, NodesWhereDisabled, Inventory, FeatureName),
            Error
    end.

restore_feature_flag_state(
  Nodes, NodesWhereDisabled, Inventory, FeatureName) ->
    NodesWhereEnabled = Nodes -- NodesWhereDisabled,
    ?LOG_DEBUG(
       "Feature flags: `~ts`: restore initial state after failing to enable "
       "it:~n"
       "  enabled on:  ~0p~n"
       "  disabled on: ~0p",
       [FeatureName, NodesWhereEnabled, NodesWhereDisabled],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    _ = mark_as_enabled_on_nodes(
          NodesWhereEnabled, Inventory, FeatureName, true),
    _ = mark_as_enabled_on_nodes(
          NodesWhereDisabled, Inventory, FeatureName, false),
    ok.

-spec do_enable(Inventory, FeatureName, Nodes) -> Ret when
      Inventory :: rabbit_feature_flags:cluster_inventory(),
      FeatureName :: rabbit_feature_flags:feature_name(),
      Nodes :: [node()],
      Ret :: {ok, Inventory} | {error, Reason},
      Reason :: term().

do_enable(#{states_per_node := _} = Inventory, FeatureName, Nodes) ->
    %% After dependencies are enabled, we need to remember the updated
    %% inventory. This is useful later to skip feature flags enabled earlier
    %% in the process. For instance because a feature flag is dependency of
    %% several other feature flags.
    case enable_dependencies(Inventory, FeatureName) of
        {ok, Inventory1} ->
            Rets = run_callback(Nodes, FeatureName, enable, #{}, infinity),
            maps:fold(
              fun
                  (_Node, ok, {ok, _} = Ret) -> Ret;
                  (_Node, Error, {ok, _})    -> Error;
                  (_Node, _, Error)          -> Error
              end, {ok, Inventory1}, Rets);
        Error ->
            Error
    end.

-spec post_enable(Inventory, FeatureName, Nodes, Enabled) -> Ret when
      Inventory :: rabbit_feature_flags:cluster_inventory(),
      FeatureName :: rabbit_feature_flags:feature_name(),
      Nodes :: [node()],
      Enabled :: boolean(),
      Ret :: ok.

post_enable(#{states_per_node := _}, FeatureName, Nodes, Enabled) ->
    case rabbit_feature_flags:uses_callbacks(FeatureName) of
        true ->
            ?LOG_DEBUG(
               "Feature flags: `~ts`: run `post_enable` callback (if any) "
               "to ~ts",
               [FeatureName,
                case Enabled of
                    true  -> "perform any cleanups";
                    false -> "rollback any changes"
                end],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            _ = run_callback(
                  Nodes, FeatureName, post_enable, #{enabled => Enabled},
                  infinity),
            ok;
        false ->
            ok
    end.

%% --------------------------------------------------------------------
%% Cluster relationship and inventory.
%% --------------------------------------------------------------------

-ifndef(TEST).
all_nodes() ->
    lists:sort(rabbit_nodes:list_members()).

running_nodes() ->
    lists:usort([node() | rabbit_nodes:list_running()]).
-else.
all_nodes() ->
    AllNodes = case rabbit_feature_flags:get_overriden_nodes() of
                   undefined ->
                       rabbit_nodes:list_members();
                   Nodes ->
                       ?assert(lists:member(node(), Nodes)),
                       Nodes
               end,
    lists:sort(AllNodes).

running_nodes() ->
    RemoteNodes = case rabbit_feature_flags:get_overriden_running_nodes() of
                      undefined -> rabbit_nodes:list_running();
                      Nodes     -> Nodes
                  end,
    lists:usort([node() | RemoteNodes]).
-endif.

collect_inventory_on_nodes(Nodes) ->
    collect_inventory_on_nodes(Nodes, ?TIMEOUT).

-spec collect_inventory_on_nodes(Nodes, Timeout) -> Ret when
      Nodes :: [node()],
      Timeout :: timeout(),
      Ret :: {ok, Inventory} | {error, Reason},
      Inventory :: rabbit_feature_flags:cluster_inventory(),
      Reason :: term().

collect_inventory_on_nodes(Nodes, Timeout) ->
    ?LOG_DEBUG(
       "Feature flags: collecting inventory on nodes: ~tp",
       [Nodes],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    Inventory0 = #{feature_flags => #{},
                   applications_per_node => #{},
                   states_per_node => #{}},
    Rets = inventory_rpcs(Nodes, Timeout),
    maps:fold(
      fun
          (Node,
           #{feature_flags := FeatureFlags1,
             applications := ScannedApps,
             states := FeatureStates},
           {ok, #{feature_flags := FeatureFlags,
                  applications_per_node := ScannedAppsPerNode,
                  states_per_node := StatesPerNode} = Inventory}) ->
            FeatureFlags2 = merge_feature_flags(FeatureFlags, FeatureFlags1),
            ScannedAppsPerNode1 = ScannedAppsPerNode#{Node => ScannedApps},
            StatesPerNode1 = StatesPerNode#{Node => FeatureStates},
            Inventory1 = Inventory#{
                           feature_flags => FeatureFlags2,
                           applications_per_node => ScannedAppsPerNode1,
                           states_per_node => StatesPerNode1},
            {ok, Inventory1};
          (_Node, #{}, Error) ->
              Error;
          (_Node, Error, {ok, #{}}) ->
              Error;
          (_Node, _Error, Error) ->
              Error
      end, {ok, Inventory0}, Rets).

inventory_rpcs(Nodes, Timeout) ->
    %% We must use `rabbit_ff_registry_wrapper' if it is available to avoid
    %% any deadlock with the Code server. If it is unavailable, we fall back
    %% to `rabbit_ff_registry'.
    %%
    %% See commit aacfa1978e24bcacd8de7d06a7c3c5d9d8bd098e and pull request
    %% #8155.
    Rets0 = rpc_calls(
              Nodes,
              rabbit_ff_registry_wrapper, inventory, [], Timeout),
    OlderNodes = maps:fold(
                   fun
                       (Node,
                        {error,
                         {exception, undef,
                          [{rabbit_ff_registry_wrapper,_ , _, _} | _]}},
                        Acc) ->
                           [Node | Acc];
                       (_Node, _Ret, Acc) ->
                           Acc
                   end, [], Rets0),
    case OlderNodes of
        [] ->
            Rets0;
        _ ->
            ?LOG_INFO(
               "Feature flags: the following nodes run an older version of "
               "RabbitMQ causing the "
               "\"rabbit_ff_registry_wrapper:inventory[] undefined\" error "
               "above: ~2p~n"
               "Feature flags: falling back to another method for those "
               "nodes; this may trigger a bug causing them to hang",
               [lists:sort(OlderNodes)],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            Rets1 = rpc_calls(
                      OlderNodes,
                      rabbit_ff_registry, inventory, [], Timeout),
            maps:merge(Rets0, Rets1)
    end.

merge_feature_flags(FeatureFlagsA, FeatureFlagsB) ->
    FeatureFlags = maps:merge(FeatureFlagsA, FeatureFlagsB),
    maps:map(
      fun
          (FeatureName, FeatureProps) when ?IS_FEATURE_FLAG(FeatureProps) ->
              %% When we collect feature flag properties from all nodes, we
              %% start with an empty cluster inventory (a common Erlang
              %% recursion pattern). This means that all feature flags are
              %% unknown at first.
              %%
              %% Here we must compute a global stability level for each
              %% feature flag, in case all nodes are not on the same page
              %% (like one nodes considers a feature flag experimental, but
              %% another one marks it as stable). That's why we rank stability
              %% level: required > stable > experimental.
              %%
              %% We must distinguish an unknown feature flag (they are all
              %% unknown at the beginning of the collection) from a known
              %% feature flags with no explicit stability level. In the latter
              %% case, `rabbit_feature_flags:get_stability/1' will consider it
              %% stable. However in the former case, we must consider it
              %% experimental otherwise it would default to be stable would
              %% superceed an experimental level, even though all nodes agree
              %% on that level, because `rabbit_feature_flags:get_stability/1'
              %% would return stable as well.
              UnknownProps = #{stability => experimental},
              FeaturePropsA = maps:get(
                                FeatureName, FeatureFlagsA, UnknownProps),
              FeaturePropsB = maps:get(
                                FeatureName, FeatureFlagsB, UnknownProps),

              StabilityA = rabbit_feature_flags:get_stability(FeaturePropsA),
              StabilityB = rabbit_feature_flags:get_stability(FeaturePropsB),
              Stability = case {StabilityA, StabilityB} of
                              {required, _} -> required;
                              {_, required} -> required;
                              {stable, _}   -> stable;
                              {_, stable}   -> stable;
                              _             -> experimental
                          end,

              FeatureProps1 = FeatureProps#{stability => Stability},
              FeatureProps2 = maps:remove(callbacks, FeatureProps1),
              FeatureProps2;
          (FeatureName, FeatureProps) when ?IS_DEPRECATION(FeatureProps) ->
              UnknownProps = #{deprecation_phase => permitted_by_default},
              FeaturePropsA = maps:get(
                                FeatureName, FeatureFlagsA, UnknownProps),
              FeaturePropsB = maps:get(
                                FeatureName, FeatureFlagsB, UnknownProps),

              PhaseA = rabbit_deprecated_features:get_phase(FeaturePropsA),
              PhaseB = rabbit_deprecated_features:get_phase(FeaturePropsB),
              Phase = case {PhaseA, PhaseB} of
                          {removed, _}           -> removed;
                          {_, removed}           -> removed;
                          {disconnected, _}      -> disconnected;
                          {_, disconnected}      -> disconnected;
                          {denied_by_default, _} -> denied_by_default;
                          {_, denied_by_default} -> denied_by_default;
                          _                      -> permitted_by_default
                      end,

              FeatureProps1 = FeatureProps#{deprecation_phase => Phase},
              FeatureProps1
      end, FeatureFlags).

-spec list_feature_flags_enabled_somewhere(Inventory, HandleStateChanging) ->
    Ret when
      Inventory :: rabbit_feature_flags:cluster_inventory(),
      HandleStateChanging :: boolean(),
      Ret :: [FeatureName],
      FeatureName :: rabbit_feature_flags:feature_name().

list_feature_flags_enabled_somewhere(
  #{states_per_node := StatesPerNode},
  HandleStateChanging) ->
    %% We want to collect feature flags which are enabled on at least one
    %% node.
    MergedStates = maps:fold(
                     fun(_Node, FeatureStates, Acc1) ->
                             maps:fold(
                               fun
                                   (FeatureName, true, Acc2) ->
                                       Acc2#{FeatureName => true};
                                   (_FeatureName, false, Acc2) ->
                                       Acc2;
                                   (FeatureName, state_changing, Acc2)
                                     when HandleStateChanging ->
                                       Acc2#{FeatureName => true}
                               end, Acc1, FeatureStates)
                     end, #{}, StatesPerNode),
    lists:sort(maps:keys(MergedStates)).

-spec list_deprecated_features_that_cant_be_denied(Inventory) ->
    Ret when
      Inventory :: rabbit_feature_flags:cluster_inventory(),
      Ret :: [FeatureName],
      FeatureName :: rabbit_feature_flags:feature_name().

list_deprecated_features_that_cant_be_denied(
  #{states_per_node := StatesPerNode}) ->
    ThisNode = node(),
    States = maps:get(ThisNode, StatesPerNode),

    maps:fold(
      fun
          (FeatureName, true, Acc) ->
              #{ThisNode := IsUsed} = run_callback(
                                        [ThisNode], FeatureName,
                                        is_feature_used, #{}, infinity),
              case IsUsed of
                  true   -> [FeatureName | Acc];
                  false  -> Acc;
                  _Error -> Acc
              end;
          (_FeatureName, false, Acc) ->
              Acc
      end, [], States).

-spec list_nodes_who_know_the_feature_flag(Inventory, FeatureName) ->
    Ret when
      Inventory :: rabbit_feature_flags:cluster_inventory(),
      FeatureName :: rabbit_feature_flags:feature_name(),
      Ret :: [node()].

list_nodes_who_know_the_feature_flag(
  #{states_per_node := StatesPerNode},
  FeatureName) ->
    Nodes = lists:sort(
              maps:keys(
                maps:filter(
                  fun(_Node, FeatureStates) ->
                          maps:is_key(FeatureName, FeatureStates)
                  end, StatesPerNode))),
    this_node_first(Nodes).

-spec list_nodes_where_feature_flag_is_disabled(Inventory, FeatureName) ->
    Ret when
      Inventory :: rabbit_feature_flags:cluster_inventory(),
      FeatureName :: rabbit_feature_flags:feature_name(),
      Ret :: [node()].

list_nodes_where_feature_flag_is_disabled(
  #{states_per_node := StatesPerNode},
  FeatureName) ->
    Nodes = lists:sort(
              maps:keys(
                maps:filter(
                  fun(_Node, FeatureStates) ->
                          case FeatureStates of
                              #{FeatureName := Enabled} ->
                                  %% The feature flag is known on this node,
                                  %% run the migration function only if it is
                                  %% disabled.
                                  not Enabled;
                              _ ->
                                  %% The feature flags is unknown on this
                                  %% node, don't run the migration function.
                                  false
                          end
                  end, StatesPerNode))),
    this_node_first(Nodes).

this_node_first(Nodes) ->
    ThisNode = node(),
    case lists:member(ThisNode, Nodes) of
        true  -> [ThisNode | Nodes -- [ThisNode]];
        false -> Nodes
    end.

-spec rpc_call(Node, Module, Function, Args, Timeout) -> Ret when
      Node :: node(),
      Module :: module(),
      Function :: atom(),
      Args :: [term()],
      Timeout :: timeout(),
      Ret :: term() | {error, term()}.

rpc_call(Node, Module, Function, Args, Timeout) ->
    SleepBetweenRetries = 5000,
    T0 = erlang:monotonic_time(),
    try
        erpc:call(Node, Module, Function, Args, Timeout)
    catch
        %% In case of `noconnection' with `Timeout'=infinity, we don't retry
        %% at all. This is because the infinity "timeout" is used to run
        %% callbacks on remote node and they can last an indefinite amount of
        %% time, for instance, if there is a lot of data to migrate.
        error:{erpc, noconnection} = Reason
          when is_integer(Timeout) andalso Timeout > SleepBetweenRetries ->
            ?LOG_WARNING(
               "Feature flags: no connection to node `~ts`; "
               "retrying in ~b milliseconds",
               [Node, SleepBetweenRetries],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            timer:sleep(SleepBetweenRetries),
            T1 = erlang:monotonic_time(),
            TDiff = erlang:convert_time_unit(T1 - T0, native, millisecond),
            Remaining = Timeout - TDiff,
            Timeout1 = erlang:max(Remaining, 0),
            case Timeout1 of
                0 -> {error, Reason};
                _ -> rpc_call(Node, Module, Function, Args, Timeout1)
            end;

        Class:Reason:Stacktrace ->
            Message0 = erl_error:format_exception(Class, Reason, Stacktrace),
            Message1 = lists:flatten(Message0),
            Message2 = ["Feature flags:   " ++ Line ++ "~n"
                        || Line <- string:lexemes(Message1, [$\n])],
            Message3 = lists:flatten(Message2),
            ?LOG_ERROR(
               "Feature flags: error while running:~n"
               "Feature flags:   ~ts:~ts~tp~n"
               "Feature flags: on node `~ts`:~n" ++
               Message3,
               [Module, Function, Args, Node],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            {error, Reason}
    end.

-spec rpc_calls(Nodes, Module, Function, Args, Timeout) -> Rets when
      Nodes :: [node()],
      Module :: module(),
      Function :: atom(),
      Args :: [term()],
      Timeout :: timeout(),
      Rets :: #{node() => term()}.

rpc_calls(Nodes, Module, Function, Args, Timeout) when is_list(Nodes) ->
    Parent = self(),
    CallRef = {Module, Function, Args},
    Runners = [{Node,
                spawn_monitor(
                  fun() ->
                          Ret = rpc_call(
                                  Node, Module, Function, Args, Timeout),
                          Parent ! {internal_rpc_call, Node, CallRef, Ret}
                  end)}
               || Node <- Nodes],
    Rets = [receive
                {internal_rpc_call, Node, CallRef, Ret} ->
                    %% After we got the return value for that node, we still
                    %% need to consume the `DOWN' message emitted once the
                    %% spawn process exited.
                    receive
                        {'DOWN', MRef, process, Pid, Reason} ->
                            ?assertEqual(normal, Reason)
                    end,
                    {Node, Ret};
                {'DOWN', MRef, process, Pid, Reason} ->
                    {Node, {error, {'DOWN', Reason}}}
            end
            || {Node, {Pid, MRef}} <- Runners],
    maps:from_list(Rets).

%% --------------------------------------------------------------------
%% Feature flag support queries.
%% --------------------------------------------------------------------

-spec is_known(Inventory, FeatureProps) -> IsKnown when
      Inventory :: rabbit_feature_flags:cluster_inventory(),
      FeatureProps ::
      rabbit_feature_flags:feature_props_extended() |
      rabbit_deprecated_features:feature_props_extended(),
      IsKnown :: boolean().
%% @private

is_known(
  #{applications_per_node := ScannedAppsPerNode},
  #{provided_by := App} = _FeatureProps) ->
    maps:fold(
      fun
          (_Node, ScannedApps, false) -> lists:member(App, ScannedApps);
          (_Node, _ScannedApps, true) -> true
      end, false, ScannedAppsPerNode).

-spec is_known_and_supported(Inventory, FeatureName) ->
    IsKnownAndSupported when
      Inventory :: rabbit_feature_flags:cluster_inventory(),
      FeatureName :: rabbit_feature_flags:feature_name(),
      IsKnownAndSupported :: boolean().

is_known_and_supported(
  #{feature_flags := FeatureFlags,
    applications_per_node := ScannedAppsPerNode,
    states_per_node := StatesPerNode},
  FeatureName)
  when is_map_key(FeatureName, FeatureFlags) ->
    %% A feature flag is considered supported by a node if:
    %%   - the node knows the feature flag, or
    %%   - the node does not have the application providing it.
    %%
    %% Therefore, we first need to look up the application providing this
    %% feature flag.
    #{FeatureName := #{provided_by := App}} = FeatureFlags,

    maps:fold(
      fun
          (Node, FeatureStates, true) ->
              case FeatureStates of
                  #{FeatureName := _} ->
                      %% The node knows about the feature flag.
                      true;
                  _ ->
                      %% The node doesn't know about the feature flag, so does
                      %% it have the application providing it loaded?
                      #{Node := ScannedApps} = ScannedAppsPerNode,
                      not lists:member(App, ScannedApps)
              end;
          (_Node, _FeatureStates, false) ->
              false
      end, true, StatesPerNode);
is_known_and_supported(_Inventory, _FeatureName) ->
    %% None of the nodes know about this feature flag at all.
    false.

%% --------------------------------------------------------------------
%% Feature flag state changes.
%% --------------------------------------------------------------------

-spec mark_as_enabled_on_nodes(Nodes, Inventory, FeatureName, IsEnabled) ->
    Ret when
      Nodes :: [node()],
      Inventory :: rabbit_feature_flags:cluster_inventory(),
      FeatureName :: rabbit_feature_flags:feature_name(),
      IsEnabled :: rabbit_feature_flags:feature_state(),
      Ret :: {ok, Inventory} | {error, Reason},
      Reason :: term().

mark_as_enabled_on_nodes(
  Nodes,
  #{states_per_node := StatesPerNode} = Inventory,
  FeatureName, IsEnabled) ->
    ?LOG_DEBUG(
       "Feature flags: `~ts`: mark as enabled=~tp on nodes ~tp",
       [FeatureName, IsEnabled, Nodes],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    Rets = rpc_calls(
             Nodes, rabbit_feature_flags, mark_as_enabled_locally,
             [FeatureName, IsEnabled], ?TIMEOUT),
    Ret = maps:fold(
            fun
                (Node, ok, {ok, StatesPerNode1}) ->
                    FeatureStates1 = maps:get(Node, StatesPerNode1),
                    FeatureStates2 = FeatureStates1#{
                                       FeatureName => IsEnabled},
                    StatesPerNode2 = StatesPerNode1#{
                                       Node => FeatureStates2},
                    {ok, StatesPerNode2};
                (_Node, ok, Error) ->
                    Error;
                (_Node, Error, {ok, _StatesPerNode1}) ->
                    Error;
                (_Node, _Error, Error) ->
                    Error
            end, {ok, StatesPerNode}, Rets),
    case Ret of
        {ok, StatesPerNode3} ->
            Inventory1 = Inventory#{states_per_node => StatesPerNode3},
            {ok, Inventory1};
        Error ->
            Error
    end.

%% --------------------------------------------------------------------
%% Feature flag dependencies handling.
%% --------------------------------------------------------------------

-spec enable_dependencies(Inventory, FeatureName) -> Ret when
      Inventory :: rabbit_feature_flags:cluster_inventory(),
      FeatureName :: rabbit_feature_flags:feature_name(),
      Ret :: {ok, Inventory} | {error, Reason},
      Reason :: term().

enable_dependencies(
  #{feature_flags := FeatureFlags} = Inventory, FeatureName) ->
    #{FeatureName := FeatureProps} = FeatureFlags,
    DependsOn = maps:get(depends_on, FeatureProps, []),
    case DependsOn of
        [] ->
            {ok, Inventory};
        _ ->
            ?LOG_DEBUG(
               "Feature flags: `~ts`: enable dependencies: ~tp",
               [FeatureName, DependsOn],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            enable_dependencies1(Inventory, DependsOn)
    end.

enable_dependencies1(
  #{states_per_node := _} = Inventory, [FeatureName | Rest]) ->
    case enable_with_registry_locked(Inventory, FeatureName) of
        {ok, Inventory1} -> enable_dependencies1(Inventory1, Rest);
        Error            -> Error
    end;
enable_dependencies1(
  #{states_per_node := _} = Inventory, []) ->
    {ok, Inventory}.

%% --------------------------------------------------------------------
%% Migration function.
%% --------------------------------------------------------------------

-spec run_callback
(Nodes, FeatureName, Command, Extra, Timeout) -> Rets when
      Nodes :: [node()],
      FeatureName :: rabbit_feature_flags:feature_name(),
      Command :: enable,
      Extra :: map(),
      Timeout :: timeout(),
      Rets :: #{node() =>
                rabbit_feature_flags:enable_callback_ret() |
                run_callback_error()};
(Nodes, FeatureName, Command, Extra, Timeout) -> Rets when
      Nodes :: [node()],
      FeatureName :: rabbit_feature_flags:feature_name(),
      Command :: post_enable,
      Extra :: map(),
      Timeout :: timeout(),
      Rets :: #{node() =>
                rabbit_feature_flags:post_enable_callback_ret() |
                run_callback_error()};
(Nodes, FeatureName, Command, Extra, Timeout) -> Rets when
      Nodes :: [node()],
      FeatureName :: rabbit_feature_flags:feature_name(),
      Command :: is_feature_used,
      Extra :: map(),
      Timeout :: timeout(),
      Rets :: #{node() =>
                rabbit_deprecated_features:is_feature_used_callback_ret() |
                run_callback_error()}.

run_callback(Nodes, FeatureName, Command, Extra, Timeout) ->
    FeatureProps = rabbit_ff_registry_wrapper:get(FeatureName),
    Callbacks = maps:get(callbacks, FeatureProps, #{}),
    case Callbacks of
        #{Command := {CallbackMod, CallbackFun}}
          when is_atom(CallbackMod) andalso is_atom(CallbackFun) ->
            %% The migration fun API v2 is of the form:
            %%   CallbackMod:CallbackFun(...)
            %%
            %% Also, the function is executed on all nodes in parallel.
            Args = Extra#{feature_name => FeatureName,
                          feature_props => FeatureProps,
                          command => Command,
                          nodes => Nodes},
            do_run_callback(Nodes, CallbackMod, CallbackFun, Args, Timeout);
        #{Command := Invalid} ->
            ?LOG_ERROR(
               "Feature flags: `~ts`: invalid callback for `~ts`: ~tp",
               [FeatureName, Command, Invalid],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            #{node() =>
              {error, {invalid_callback, FeatureName, Command, Invalid}}};
        _ ->
            %% No callbacks defined for this feature flag. Consider it a
            %% success!
            Ret = case Command of
                      enable          -> ok;
                      post_enable     -> ok;
                      is_feature_used -> false
                  end,
            #{node() => Ret}
    end.

-spec do_run_callback(Nodes, CallbackMod, CallbackFun, Args, Timeout) ->
    Rets when
      Nodes :: [node()],
      CallbackMod :: module(),
      CallbackFun :: atom(),
      Args :: rabbit_feature_flags:callbacks_args() |
              rabbit_deprecated_features:callbacks_args(),
      Timeout :: timeout(),
      Rets :: #{node() => rabbit_feature_flags:callbacks_rets() |
                          rabbit_deprecated_features:callbacks_rets() |
                          run_callback_error()}.

do_run_callback(Nodes, CallbackMod, CallbackFun, Args, Timeout) ->
    #{feature_name := FeatureName,
      command := Command} = Args,
    ?LOG_DEBUG(
       "Feature flags: `~ts`: run callback ~ts:~ts (~ts callback)~n"
       "Feature flags:   with args = ~tp~n"
       "Feature flags:   on nodes ~tp",
       [FeatureName, CallbackMod, CallbackFun, Command, Args, Nodes],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    Rets = rpc_calls(Nodes, CallbackMod, CallbackFun, [Args], Timeout),
    ?LOG_DEBUG(
       "Feature flags: `~ts`: callback ~ts:~ts (~ts callback) returned:~n"
       "Feature flags:   ~tp",
       [FeatureName, CallbackMod, CallbackFun, Command, Rets],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    Rets.
