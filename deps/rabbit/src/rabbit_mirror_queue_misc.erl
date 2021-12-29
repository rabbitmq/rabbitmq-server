%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2010-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mirror_queue_misc).
-behaviour(rabbit_policy_validator).

-include("amqqueue.hrl").

-export([remove_from_queue/3, on_vhost_up/1, add_mirrors/3,
         report_deaths/4, store_updated_slaves/1,
         initial_queue_node/2, suggested_queue_nodes/1, actual_queue_nodes/1,
         is_mirrored/1, is_mirrored_ha_nodes/1,
         update_mirrors/2, update_mirrors/1, validate_policy/1,
         maybe_auto_sync/1, maybe_drop_master_after_sync/1,
         sync_batch_size/1, default_max_sync_throughput/0,
         log_info/3, log_warning/3]).
-export([stop_all_slaves/5]).

-export([sync_queue/1, cancel_sync_queue/1, queue_length/1]).

-export([get_replicas/1, transfer_leadership/2, migrate_leadership_to_existing_replica/2]).

%% for testing only
-export([module/1]).

-include_lib("rabbit_common/include/rabbit.hrl").

-define(HA_NODES_MODULE, rabbit_mirror_queue_mode_nodes).

-rabbit_boot_step(
   {?MODULE,
    [{description, "HA policy validation"},
     {mfa, {rabbit_registry, register,
            [policy_validator, <<"ha-mode">>, ?MODULE]}},
     {mfa, {rabbit_registry, register,
            [policy_validator, <<"ha-params">>, ?MODULE]}},
     {mfa, {rabbit_registry, register,
            [policy_validator, <<"ha-sync-mode">>, ?MODULE]}},
     {mfa, {rabbit_registry, register,
            [policy_validator, <<"ha-sync-batch-size">>, ?MODULE]}},
     {mfa, {rabbit_registry, register,
            [policy_validator, <<"ha-promote-on-shutdown">>, ?MODULE]}},
     {mfa, {rabbit_registry, register,
            [policy_validator, <<"ha-promote-on-failure">>, ?MODULE]}},
     {requires, rabbit_registry},
     {enables, recovery}]}).


%%----------------------------------------------------------------------------

%% Returns {ok, NewMPid, DeadPids, ExtraNodes}

-spec remove_from_queue
        (rabbit_amqqueue:name(), pid(), [pid()]) ->
            {'ok', pid(), [pid()], [node()]} | {'error', 'not_found'} |
            {'error', {'not_synced', [pid()]}}.

remove_from_queue(QueueName, Self, DeadGMPids) ->
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              %% Someone else could have deleted the queue before we
              %% get here. Or, gm group could've altered. see rabbitmq-server#914
              case mnesia:read({rabbit_queue, QueueName}) of
                  [] -> {error, not_found};
                  [Q0] when ?is_amqqueue(Q0) ->
                      QPid = amqqueue:get_pid(Q0),
                      SPids = amqqueue:get_slave_pids(Q0),
                      SyncSPids = amqqueue:get_sync_slave_pids(Q0),
                      GMPids = amqqueue:get_gm_pids(Q0),
                      {DeadGM, AliveGM} = lists:partition(
                                            fun ({GM, _}) ->
                                                    lists:member(GM, DeadGMPids)
                                            end, GMPids),
                      DeadPids  = [Pid || {_GM, Pid} <- DeadGM],
                      AlivePids = [Pid || {_GM, Pid} <- AliveGM],
                      Alive     = [Pid || Pid <- [QPid | SPids],
                                          lists:member(Pid, AlivePids)],
                      {QPid1, SPids1} = case Alive of
                                            [] ->
                                                %% GM altered, & if all pids are
                                                %% perceived as dead, rather do
                                                %% do nothing here, & trust the
                                                %% promoted mirror to have updated
                                                %% mnesia during the alteration.
                                                {QPid, SPids};
                                            _  -> promote_slave(Alive)
                                        end,
                      DoNotPromote = SyncSPids =:= [] andalso
                                     rabbit_policy:get(<<"ha-promote-on-failure">>, Q0) =:= <<"when-synced">>,
                      case {{QPid, SPids}, {QPid1, SPids1}} of
                          {Same, Same} ->
                              {ok, QPid1, DeadPids, []};
                          _ when QPid1 =/= QPid andalso QPid1 =:= Self andalso DoNotPromote =:= true ->
                              %% We have been promoted to master
                              %% but there are no synchronised mirrors
                              %% hence this node is not synchronised either
                              %% Bailing out.
                              {error, {not_synced, SPids1}};
                          _ when QPid =:= QPid1 orelse QPid1 =:= Self ->
                              %% Either master hasn't changed, so
                              %% we're ok to update mnesia; or we have
                              %% become the master. If gm altered,
                              %% we have no choice but to proceed.
                              Q1 = amqqueue:set_pid(Q0, QPid1),
                              Q2 = amqqueue:set_slave_pids(Q1, SPids1),
                              Q3 = amqqueue:set_gm_pids(Q2, AliveGM),
                              store_updated_slaves(Q3),
                              %% If we add and remove nodes at the
                              %% same time we might tell the old
                              %% master we need to sync and then
                              %% shut it down. So let's check if
                              %% the new master needs to sync.
                              maybe_auto_sync(Q3),
                              {ok, QPid1, DeadPids, slaves_to_start_on_failure(Q3, DeadGMPids)};
                      _ ->
                              %% Master has changed, and we're not it.
                              %% [1].
                              Q1 = amqqueue:set_slave_pids(Q0, Alive),
                              Q2 = amqqueue:set_gm_pids(Q1, AliveGM),
                              store_updated_slaves(Q2),
                              {ok, QPid1, DeadPids, []}
                      end
              end
      end).
%% [1] We still update mnesia here in case the mirror that is supposed
%% to become master dies before it does do so, in which case the dead
%% old master might otherwise never get removed, which in turn might
%% prevent promotion of another mirror (e.g. us).
%%
%% Note however that we do not update the master pid. Otherwise we can
%% have the situation where a mirror updates the mnesia record for a
%% queue, promoting another mirror before that mirror realises it has
%% become the new master, which is bad because it could then mean the
%% mirror (now master) receives messages it's not ready for (for
%% example, new consumers).
%%
%% We set slave_pids to Alive rather than SPids1 since otherwise we'd
%% be removing the pid of the candidate master, which in turn would
%% prevent it from promoting itself.
%%
%% We maintain gm_pids as our source of truth, i.e. it contains the
%% most up-to-date information about which GMs and associated
%% {M,S}Pids are alive. And all pids in slave_pids always have a
%% corresponding entry in gm_pids. By contrast, due to the
%% aforementioned restriction on updating the master pid, that pid may
%% not be present in gm_pids, but only if said master has died.

%% Sometimes a mirror dying means we need to start more on other
%% nodes - "exactly" mode can cause this to happen.
slaves_to_start_on_failure(Q, DeadGMPids) ->
    %% In case Mnesia has not caught up yet, filter out nodes we know
    %% to be dead..
    ClusterNodes = rabbit_nodes:all_running() --
        [node(P) || P <- DeadGMPids],
    {_, OldNodes, _} = actual_queue_nodes(Q),
    {_, NewNodes} = suggested_queue_nodes(Q, ClusterNodes),
    NewNodes -- OldNodes.

on_vhost_up(VHost) ->
    QNames =
        rabbit_misc:execute_mnesia_transaction(
          fun () ->
                  mnesia:foldl(
                    fun
                    (Q, QNames0) when not ?amqqueue_vhost_equals(Q, VHost) ->
                        QNames0;
                    (Q, QNames0) when ?amqqueue_is_classic(Q) ->
                            QName = amqqueue:get_name(Q),
                            Pid = amqqueue:get_pid(Q),
                            SPids = amqqueue:get_slave_pids(Q),
                            %% We don't want to pass in the whole
                            %% cluster - we don't want a situation
                            %% where starting one node causes us to
                            %% decide to start a mirror on another
                            PossibleNodes0 = [node(P) || P <- [Pid | SPids]],
                            PossibleNodes =
                                case lists:member(node(), PossibleNodes0) of
                                    true  -> PossibleNodes0;
                                    false -> [node() | PossibleNodes0]
                                end,
                            {_MNode, SNodes} = suggested_queue_nodes(
                                                 Q, PossibleNodes),
                            case lists:member(node(), SNodes) of
                                true  -> [QName | QNames0];
                                false -> QNames0
                            end;
                    (_, QNames0) ->
                            QNames0
                    end, [], rabbit_queue)
          end),
    [add_mirror(QName, node(), async) || QName <- QNames],
    ok.

drop_mirrors(QName, Nodes) ->
    [drop_mirror(QName, Node)  || Node <- Nodes],
    ok.

drop_mirror(QName, MirrorNode) ->
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} when ?is_amqqueue(Q) ->
            Name = amqqueue:get_name(Q),
            PrimaryPid = amqqueue:get_pid(Q),
            MirrorPids = amqqueue:get_slave_pids(Q),
            AllReplicaPids = [PrimaryPid | MirrorPids],
            case [Pid || Pid <- AllReplicaPids, node(Pid) =:= MirrorNode] of
                [] ->
                    {error, {queue_not_mirrored_on_node, MirrorNode}};
                [PrimaryPid] when MirrorPids =:= [] ->
                    {error, cannot_drop_only_mirror};
                [Pid] ->
                    log_info(Name, "Dropping queue mirror on node ~p",
                             [MirrorNode]),
                    exit(Pid, {shutdown, dropped}),
                    {ok, dropped}
            end;
        {error, not_found} = E ->
            E
    end.

-spec add_mirrors(rabbit_amqqueue:name(), [node()], 'sync' | 'async') ->
          'ok'.

add_mirrors(QName, Nodes, SyncMode) ->
    [add_mirror(QName, Node, SyncMode)  || Node <- Nodes],
    ok.

add_mirror(QName, MirrorNode, SyncMode) ->
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} ->
            rabbit_misc:with_exit_handler(
              rabbit_misc:const(ok),
              fun () ->
                    #resource{virtual_host = VHost} = amqqueue:get_name(Q),
                    case rabbit_vhost_sup_sup:get_vhost_sup(VHost, MirrorNode) of
                        {ok, _} ->
                            try
                                MirrorPid = rabbit_amqqueue_sup_sup:start_queue_process(MirrorNode, Q, slave),
                                log_info(QName, "Adding mirror on node ~p: ~p", [MirrorNode, MirrorPid]),
                                rabbit_mirror_queue_slave:go(MirrorPid, SyncMode)
                            of
                                _ -> ok
                            catch
                                error:QError ->
                                    log_warning(QName,
                                        "Unable to start queue mirror on node '~p'. "
                                        "Target queue supervisor is not running: ~p",
                                        [MirrorNode, QError])
                            end;
                        {error, Error} ->
                            log_warning(QName,
                                        "Unable to start queue mirror on node '~p'. "
                                        "Target virtual host is not running: ~p",
                                        [MirrorNode, Error]),
                            ok
                    end
              end);
        {error, not_found} = E ->
            E
    end.

report_deaths(_MirrorPid, _IsMaster, _QueueName, []) ->
    ok;
report_deaths(MirrorPid, IsMaster, QueueName, DeadPids) ->
    log_info(QueueName, "~s replica of queue ~s detected replica ~s to be down",
                    [case IsMaster of
                         true  -> "Primary";
                         false -> "Secondary"
                     end,
                     rabbit_misc:pid_to_string(MirrorPid),
                     [[$ , rabbit_misc:pid_to_string(P)] || P <- DeadPids]]).

-spec log_info(rabbit_amqqueue:name(), string(), [any()]) -> 'ok'.

log_info   (QName, Fmt, Args) ->
    rabbit_log_mirroring:info("Mirrored ~s: " ++ Fmt,
                              [rabbit_misc:rs(QName) | Args]).

-spec log_warning(rabbit_amqqueue:name(), string(), [any()]) -> 'ok'.

log_warning(QName, Fmt, Args) ->
    rabbit_log_mirroring:warning("Mirrored ~s: " ++ Fmt,
                                 [rabbit_misc:rs(QName) | Args]).

-spec store_updated_slaves(amqqueue:amqqueue()) ->
          amqqueue:amqqueue().

store_updated_slaves(Q0) when ?is_amqqueue(Q0) ->
    SPids = amqqueue:get_slave_pids(Q0),
    SSPids = amqqueue:get_sync_slave_pids(Q0),
    RS0 = amqqueue:get_recoverable_slaves(Q0),
    %% TODO now that we clear sync_slave_pids in rabbit_durable_queue,
    %% do we still need this filtering?
    SSPids1 = [SSPid || SSPid <- SSPids, lists:member(SSPid, SPids)],
    Q1 = amqqueue:set_sync_slave_pids(Q0, SSPids1),
    RS1 = update_recoverable(SPids, RS0),
    Q2 = amqqueue:set_recoverable_slaves(Q1, RS1),
    Q3 = amqqueue:set_state(Q2, live),
    %% amqqueue migration:
    %% The amqqueue was read from this transaction, no need to handle
    %% migration.
    ok = rabbit_amqqueue:store_queue(Q3),
    %% Wake it up so that we emit a stats event
    rabbit_amqqueue:notify_policy_changed(Q3),
    Q3.

%% Recoverable nodes are those which we could promote if the whole
%% cluster were to suddenly stop and we then lose the master; i.e. all
%% nodes with running mirrors , and all stopped nodes which had running
%% mirrors when they were up.
%%
%% Therefore we aim here to add new nodes with mirrors , and remove
%% running nodes without mirrors , We also try to keep the order
%% constant, and similar to the live SPids field (i.e. oldest
%% first). That's not necessarily optimal if nodes spend a long time
%% down, but we don't have a good way to predict what the optimal is
%% in that case anyway, and we assume nodes will not just be down for
%% a long time without being removed.
update_recoverable(SPids, RS) ->
    SNodes = [node(SPid) || SPid <- SPids],
    RunningNodes = rabbit_nodes:all_running(),
    AddNodes = SNodes -- RS,
    DelNodes = RunningNodes -- SNodes, %% i.e. running with no slave
    (RS -- DelNodes) ++ AddNodes.

stop_all_slaves(Reason, SPids, QName, GM, WaitTimeout) ->
    PidsMRefs = [{Pid, erlang:monitor(process, Pid)} || Pid <- [GM | SPids]],
    ok = gm:broadcast(GM, {delete_and_terminate, Reason}),
    %% It's possible that we could be partitioned from some mirrors
    %% between the lookup and the broadcast, in which case we could
    %% monitor them but they would not have received the GM
    %% message. So only wait for mirrors which are still
    %% not-partitioned.
    PendingSlavePids = lists:foldl(fun({Pid, MRef}, Acc) ->
        case rabbit_mnesia:on_running_node(Pid) of
            true ->
                receive
                    {'DOWN', MRef, process, _Pid, _Info} ->
                        Acc
                after WaitTimeout ->
                        rabbit_mirror_queue_misc:log_warning(
                          QName, "Missing 'DOWN' message from ~p in"
                          " node ~p", [Pid, node(Pid)]),
                        [Pid | Acc]
                end;
            false ->
                Acc
        end
    end, [], PidsMRefs),
    %% Normally when we remove a mirror another mirror or master will
    %% notice and update Mnesia. But we just removed them all, and
    %% have stopped listening ourselves. So manually clean up.
    rabbit_misc:execute_mnesia_transaction(fun () ->
        [Q0] = mnesia:read({rabbit_queue, QName}),
        Q1 = amqqueue:set_gm_pids(Q0, []),
        Q2 = amqqueue:set_slave_pids(Q1, []),
        %% Restarted mirrors on running nodes can
        %% ensure old incarnations are stopped using
        %% the pending mirror pids.
        Q3 = amqqueue:set_slave_pids_pending_shutdown(Q2, PendingSlavePids),
        rabbit_mirror_queue_misc:store_updated_slaves(Q3)
    end),
    ok = gm:forget_group(QName).

%%----------------------------------------------------------------------------

promote_slave([SPid | SPids]) ->
    %% The mirror pids are maintained in descending order of age, so
    %% the one to promote is the oldest.
    {SPid, SPids}.

-spec initial_queue_node(amqqueue:amqqueue(), node()) -> node().

initial_queue_node(Q, DefNode) ->
    {MNode, _SNodes} = suggested_queue_nodes(Q, DefNode, rabbit_nodes:all_running()),
    MNode.

-spec suggested_queue_nodes(amqqueue:amqqueue()) ->
          {node(), [node()]}.

suggested_queue_nodes(Q)      -> suggested_queue_nodes(Q, rabbit_nodes:all_running()).
suggested_queue_nodes(Q, All) -> suggested_queue_nodes(Q, node(), All).

%% The third argument exists so we can pull a call to
%% rabbit_nodes:all_running() out of a loop or transaction
%% or both.
suggested_queue_nodes(Q, DefNode, All) when ?is_amqqueue(Q) ->
    Owner = amqqueue:get_exclusive_owner(Q),
    {MNode0, SNodes, SSNodes} = actual_queue_nodes(Q),
    MNode = case MNode0 of
                none -> DefNode;
                _    -> MNode0
            end,
    case Owner of
        none -> Params = policy(<<"ha-params">>, Q),
                case module(Q) of
                    {ok, M} -> M:suggested_queue_nodes(
                                 Params, MNode, SNodes, SSNodes, All);
                    _       -> {MNode, []}
                end;
        _    -> {MNode, []}
    end.

policy(Policy, Q) ->
    case rabbit_policy:get(Policy, Q) of
        undefined -> none;
        P         -> P
    end.

module(Q) when ?is_amqqueue(Q) ->
    case rabbit_policy:get(<<"ha-mode">>, Q) of
        undefined -> not_mirrored;
        Mode      -> module(Mode)
    end;

module(Mode) when is_binary(Mode) ->
    case rabbit_registry:binary_to_type(Mode) of
        {error, not_found} -> not_mirrored;
        T                  -> case rabbit_registry:lookup_module(ha_mode, T) of
                                  {ok, Module} -> {ok, Module};
                                  _            -> not_mirrored
                              end
    end.

validate_mode(Mode) ->
    case module(Mode) of
        {ok, _Module} ->
            ok;
        not_mirrored ->
            {error, "~p is not a valid ha-mode value", [Mode]}
    end.

-spec is_mirrored(amqqueue:amqqueue()) -> boolean().

is_mirrored(Q) ->
    MatchedByPolicy = case module(Q) of
        {ok, _}  -> true;
        _        -> false
    end,
    MatchedByPolicy andalso (not rabbit_amqqueue:is_exclusive(Q)).

is_mirrored_ha_nodes(Q) ->
    MatchedByPolicy = case module(Q) of
        {ok, ?HA_NODES_MODULE} -> true;
        _ -> false
    end,
    MatchedByPolicy andalso (not rabbit_amqqueue:is_exclusive(Q)).

actual_queue_nodes(Q) when ?is_amqqueue(Q) ->
    PrimaryPid = amqqueue:get_pid(Q),
    MirrorPids = amqqueue:get_slave_pids(Q),
    InSyncMirrorPids = amqqueue:get_sync_slave_pids(Q),
    CollectNodes = fun (L) -> [node(Pid) || Pid <- L] end,
    NodeHostingPrimary = case PrimaryPid of
                         none -> none;
                         _ -> node(PrimaryPid)
                     end,
    {NodeHostingPrimary, CollectNodes(MirrorPids), CollectNodes(InSyncMirrorPids)}.

-spec maybe_auto_sync(amqqueue:amqqueue()) -> 'ok'.

maybe_auto_sync(Q) when ?is_amqqueue(Q) ->
    QPid = amqqueue:get_pid(Q),
    case policy(<<"ha-sync-mode">>, Q) of
        <<"automatic">> ->
            spawn(fun() -> rabbit_amqqueue:sync_mirrors(QPid) end);
        _ ->
            ok
    end.

sync_queue(Q0) ->
    F = fun
            (Q) when ?amqqueue_is_classic(Q) ->
                QPid = amqqueue:get_pid(Q),
                rabbit_amqqueue:sync_mirrors(QPid);
            (Q) when ?amqqueue_is_quorum(Q) ->
                {error, quorum_queue_not_supported}
        end,
    rabbit_amqqueue:with(Q0, F).

cancel_sync_queue(Q0) ->
    F = fun
            (Q) when ?amqqueue_is_classic(Q) ->
                QPid = amqqueue:get_pid(Q),
                rabbit_amqqueue:cancel_sync_mirrors(QPid);
            (Q) when ?amqqueue_is_quorum(Q) ->
               {error, quorum_queue_not_supported}
        end,
    rabbit_amqqueue:with(Q0, F).

sync_batch_size(Q) when ?is_amqqueue(Q) ->
    case policy(<<"ha-sync-batch-size">>, Q) of
        none -> %% we need this case because none > 1 == true
            default_batch_size();
        BatchSize when BatchSize > 1 ->
            BatchSize;
        _ ->
            default_batch_size()
    end.

-define(DEFAULT_BATCH_SIZE, 4096).

default_batch_size() ->
    rabbit_misc:get_env(rabbit, mirroring_sync_batch_size,
                        ?DEFAULT_BATCH_SIZE).

-define(DEFAULT_MAX_SYNC_THROUGHPUT, 0).

default_max_sync_throughput() ->
  case application:get_env(rabbit, mirroring_sync_max_throughput) of
    {ok, Value} ->
      case rabbit_resource_monitor_misc:parse_information_unit(Value) of
        {ok, ParsedThroughput} ->
          ParsedThroughput;
        {error, parse_error} ->
          rabbit_log:warning(
            "The configured value for the mirroring_sync_max_throughput is "
            "not a valid value: ~p. Disabled sync throughput control. ",
            [Value]),
          ?DEFAULT_MAX_SYNC_THROUGHPUT
      end;
    undefined ->
      ?DEFAULT_MAX_SYNC_THROUGHPUT
  end.

-spec update_mirrors
        (amqqueue:amqqueue(), amqqueue:amqqueue()) -> 'ok'.

update_mirrors(OldQ, NewQ) when ?amqqueue_pids_are_equal(OldQ, NewQ) ->
    % Note: we do want to ensure both queues have same pid
    QPid = amqqueue:get_pid(OldQ),
    QPid = amqqueue:get_pid(NewQ),
    case {is_mirrored(OldQ), is_mirrored(NewQ)} of
        {false, false} -> ok;
        _ -> rabbit_amqqueue:update_mirroring(QPid)
    end.

-spec update_mirrors
        (amqqueue:amqqueue()) -> 'ok'.

update_mirrors(Q) when ?is_amqqueue(Q) ->
    QName = amqqueue:get_name(Q),
    {PreTransferPrimaryNode, PreTransferMirrorNodes, __PreTransferInSyncMirrorNodes} = actual_queue_nodes(Q),
    {NewlySelectedPrimaryNode, NewlySelectedMirrorNodes} = suggested_queue_nodes(Q),
    PreTransferNodesWithReplicas = [PreTransferPrimaryNode | PreTransferMirrorNodes],
    NewlySelectedNodesWithReplicas = [NewlySelectedPrimaryNode | NewlySelectedMirrorNodes],
    %% When a mirror dies, remove_from_queue/2 might have to add new
    %% mirrors (in "exactly" mode). It will check the queue record to see which
    %% mirrors there currently are. If drop_mirror/2 is invoked first
    %% then when we end up in remove_from_queue/2 it will not see the
    %% mirrors that add_mirror/2 will add, and also want to add them
    %% (even though we are not responding to the death of a
    %% mirror). Breakage ensues.
    add_mirrors(QName, NewlySelectedNodesWithReplicas -- PreTransferNodesWithReplicas, async),
    drop_mirrors(QName, PreTransferNodesWithReplicas -- NewlySelectedNodesWithReplicas),
    %% This is for the case where no extra nodes were added but we changed to
    %% a policy requiring auto-sync.
    maybe_auto_sync(Q),
    ok.

queue_length(Q) ->
    [{messages, M}] = rabbit_amqqueue:info(Q, [messages]),
    M.

get_replicas(Q) ->
    {PrimaryNode, MirrorNodes} = suggested_queue_nodes(Q),
    [PrimaryNode] ++ MirrorNodes.

-spec transfer_leadership(amqqueue:amqqueue(), node()) -> {migrated, node()} | {not_migrated, atom()}.
%% Moves the primary replica (leader) of a classic mirrored queue to another node.
%% Target node can be any node in the cluster, and does not have to host a replica
%% of this queue.
transfer_leadership(Q, Destination) ->
    QName = amqqueue:get_name(Q),
    {PreTransferPrimaryNode, PreTransferMirrorNodes, _PreTransferInSyncMirrorNodes} = actual_queue_nodes(Q),
    PreTransferNodesWithReplicas = [PreTransferPrimaryNode | PreTransferMirrorNodes],

    NodesToAddMirrorsOn = [Destination] -- PreTransferNodesWithReplicas,
    %% This will wait for the transfer/eager sync to finish before we begin dropping
    %% mirrors on the next step. In this case we cannot add mirrors asynchronously
    %% as that will race with the dropping step.
    add_mirrors(QName, NodesToAddMirrorsOn, sync),

    NodesToDropMirrorsOn = PreTransferNodesWithReplicas -- [Destination],
    drop_mirrors(QName, NodesToDropMirrorsOn),

    case wait_for_new_master(QName, Destination) of
        not_migrated ->
            {not_migrated, undefined};
        {{not_migrated, Destination} = Result, _Q1} ->
            Result;
        {Result, NewQ} ->
            update_mirrors(NewQ),
            Result
    end.


-spec migrate_leadership_to_existing_replica(amqqueue:amqqueue(), atom()) -> {migrated, node()} | {not_migrated, atom()}.
%% Moves the primary replica (leader) of a classic mirrored queue to another node
%% which already hosts a replica of this queue. In this case we can stop
%% fewer replicas and reduce the load the operation has on the cluster.
migrate_leadership_to_existing_replica(Q, Destination) ->
    QName = amqqueue:get_name(Q),
    {PreTransferPrimaryNode, PreTransferMirrorNodes, _PreTransferInSyncMirrorNodes} = actual_queue_nodes(Q),
    PreTransferNodesWithReplicas = [PreTransferPrimaryNode | PreTransferMirrorNodes],

    NodesToAddMirrorsOn = [Destination] -- PreTransferNodesWithReplicas,
    %% This will wait for the transfer/eager sync to finish before we begin dropping
    %% mirrors on the next step. In this case we cannot add mirrors asynchronously
    %% as that will race with the dropping step.
    add_mirrors(QName, NodesToAddMirrorsOn, sync),

    NodesToDropMirrorsOn = [PreTransferPrimaryNode],
    drop_mirrors(QName, NodesToDropMirrorsOn),

    case wait_for_new_master(QName, Destination) of
        not_migrated ->
            {not_migrated, undefined};
        {{not_migrated, Destination} = Result, _Q1} ->
            Result;
        {Result, NewQ} ->
            update_mirrors(NewQ),
            Result
    end.

-spec wait_for_new_master(rabbit_amqqueue:name(), atom()) -> {{migrated, node()}, amqqueue:amqqueue()} | {{not_migrated, node()}, amqqueue:amqqueue()} | not_migrated.
wait_for_new_master(QName, Destination) ->
    wait_for_new_master(QName, Destination, 100).

wait_for_new_master(QName, _, 0) ->
    case rabbit_amqqueue:lookup(QName) of
        {error, not_found} -> not_migrated;
        {ok, Q}            -> {{not_migrated, undefined}, Q}
    end;
wait_for_new_master(QName, Destination, N) ->
    case rabbit_amqqueue:lookup(QName) of
        {error, not_found} ->
            not_migrated;
        {ok, Q} ->
            case amqqueue:get_pid(Q) of
                none ->
                    timer:sleep(100),
                    wait_for_new_master(QName, Destination, N - 1);
                Pid ->
                    case node(Pid) of
                        Destination ->
                            {{migrated, Destination}, Q};
                        _ ->
                            timer:sleep(100),
                            wait_for_new_master(QName, Destination, N - 1)
                    end
            end
    end.

%% The arrival of a newly synced mirror may cause the master to die if
%% the policy does not want the master but it has been kept alive
%% because there were no synced mirrors.
%%
%% We don't just call update_mirrors/2 here since that could decide to
%% start a mirror for some other reason, and since we are the mirror ATM
%% that allows complicated deadlocks.

-spec maybe_drop_master_after_sync(amqqueue:amqqueue()) -> 'ok'.

maybe_drop_master_after_sync(Q) when ?is_amqqueue(Q) ->
    QName = amqqueue:get_name(Q),
    MPid = amqqueue:get_pid(Q),
    {DesiredMNode, DesiredSNodes} = suggested_queue_nodes(Q),
    case node(MPid) of
        DesiredMNode -> ok;
        OldMNode     -> false = lists:member(OldMNode, DesiredSNodes), %% [0]
                        drop_mirror(QName, OldMNode)
    end,
    ok.
%% [0] ASSERTION - if the policy wants the master to change, it has
%% not just shuffled it into the mirrors. All our modes ensure this
%% does not happen, but we should guard against a misbehaving plugin.

%%----------------------------------------------------------------------------

validate_policy(KeyList) ->
    Mode = proplists:get_value(<<"ha-mode">>, KeyList, none),
    Params = proplists:get_value(<<"ha-params">>, KeyList, none),
    SyncMode = proplists:get_value(<<"ha-sync-mode">>, KeyList, none),
    SyncBatchSize = proplists:get_value(
                      <<"ha-sync-batch-size">>, KeyList, none),
    PromoteOnShutdown = proplists:get_value(
                          <<"ha-promote-on-shutdown">>, KeyList, none),
    PromoteOnFailure = proplists:get_value(
                          <<"ha-promote-on-failure">>, KeyList, none),
    case {Mode, Params, SyncMode, SyncBatchSize, PromoteOnShutdown, PromoteOnFailure} of
        {none, none, none, none, none, none} ->
            ok;
        {none, _, _, _, _, _} ->
            {error, "ha-mode must be specified to specify ha-params, "
             "ha-sync-mode or ha-promote-on-shutdown", []};
        _ ->
            validate_policies(
              [{Mode, fun validate_mode/1},
               {Params, ha_params_validator(Mode)},
               {SyncMode, fun validate_sync_mode/1},
               {SyncBatchSize, fun validate_sync_batch_size/1},
               {PromoteOnShutdown, fun validate_pos/1},
               {PromoteOnFailure, fun validate_pof/1}])
    end.

ha_params_validator(Mode) ->
    fun(Val) ->
            {ok, M} = module(Mode),
            M:validate_policy(Val)
    end.

validate_policies([]) ->
    ok;
validate_policies([{Val, Validator} | Rest]) ->
    case Validator(Val) of
        ok -> validate_policies(Rest);
        E  -> E
    end.

validate_sync_mode(SyncMode) ->
    case SyncMode of
        <<"automatic">> -> ok;
        <<"manual">>    -> ok;
        none            -> ok;
        Mode            -> {error, "ha-sync-mode must be \"manual\" "
                            "or \"automatic\", got ~p", [Mode]}
    end.

validate_sync_batch_size(none) ->
    ok;
validate_sync_batch_size(N) when is_integer(N) andalso N > 0 ->
    ok;
validate_sync_batch_size(N) ->
    {error, "ha-sync-batch-size takes an integer greater than 0, "
     "~p given", [N]}.

validate_pos(PromoteOnShutdown) ->
    case PromoteOnShutdown of
        <<"always">>      -> ok;
        <<"when-synced">> -> ok;
        none              -> ok;
        Mode              -> {error, "ha-promote-on-shutdown must be "
                              "\"always\" or \"when-synced\", got ~p", [Mode]}
    end.

validate_pof(PromoteOnShutdown) ->
    case PromoteOnShutdown of
        <<"always">>      -> ok;
        <<"when-synced">> -> ok;
        none              -> ok;
        Mode              -> {error, "ha-promote-on-failure must be "
                              "\"always\" or \"when-synced\", got ~p", [Mode]}
    end.
