%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2010-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mirror_queue_misc).
-behaviour(rabbit_policy_validator).

-export([remove_from_queue/3, on_vhost_up/1, add_mirrors/3,
         report_deaths/4, store_updated_slaves/1,
         initial_queue_node/2, suggested_queue_nodes/1, actual_queue_nodes/1,
         is_mirrored/1, is_mirrored_ha_nodes/1,
         update_mirrors/2, update_mirrors/1, validate_policy/1,
         maybe_auto_sync/1, maybe_drop_master_after_sync/1,
         sync_batch_size/1, log_info/3, log_warning/3]).
-export([stop_all_slaves/5]).

-export([sync_queue/1, cancel_sync_queue/1]).

-export([transfer_leadership/2, queue_length/1, get_replicas/1]).

%% for testing only
-export([module/1]).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("amqqueue.hrl").

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
                                                %% promoted slave to have updated
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
%% [1] We still update mnesia here in case the slave that is supposed
%% to become master dies before it does do so, in which case the dead
%% old master might otherwise never get removed, which in turn might
%% prevent promotion of another slave (e.g. us).
%%
%% Note however that we do not update the master pid. Otherwise we can
%% have the situation where a slave updates the mnesia record for a
%% queue, promoting another slave before that slave realises it has
%% become the new master, which is bad because it could then mean the
%% slave (now master) receives messages it's not ready for (for
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

%% Sometimes a slave dying means we need to start more on other
%% nodes - "exactly" mode can cause this to happen.
slaves_to_start_on_failure(Q, DeadGMPids) ->
    %% In case Mnesia has not caught up yet, filter out nodes we know
    %% to be dead..
    ClusterNodes = rabbit_mnesia:cluster_nodes(running) --
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
            QPid = amqqueue:get_pid(Q),
            SPids = amqqueue:get_slave_pids(Q),
            case [Pid || Pid <- [QPid | SPids], node(Pid) =:= MirrorNode] of
                [] ->
                    {error, {queue_not_mirrored_on_node, MirrorNode}};
                [QPid] when SPids =:= [] ->
                    {error, cannot_drop_only_mirror};
                [Pid] ->
                    log_info(Name, "Dropping queue mirror on node ~p~n",
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
                                SPid = rabbit_amqqueue_sup_sup:start_queue_process(
                                       MirrorNode, Q, slave),
                                log_info(QName, "Adding mirror on node ~p: ~p~n",
                                     [MirrorNode, SPid]),
                                rabbit_mirror_queue_slave:go(SPid, SyncMode)
                            of 
                                _ -> ok
                            catch
                                error:QError -> 
                                    log_warning(QName,
                                        "Unable to start queue mirror on node '~p'. "
                                        "Target queue supervisor is not running: ~p~n",
                                        [MirrorNode, QError])
                            end;
                        {error, Error} ->
                            log_warning(QName,
                                        "Unable to start queue mirror on node '~p'. "
                                        "Target virtual host is not running: ~p~n",
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
    log_info(QueueName, "~s ~s saw deaths of mirrors~s~n",
                    [case IsMaster of
                         true  -> "Master";
                         false -> "Slave"
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
%% nodes with running slaves, and all stopped nodes which had running
%% slaves when they were up.
%%
%% Therefore we aim here to add new nodes with slaves, and remove
%% running nodes without slaves, We also try to keep the order
%% constant, and similar to the live SPids field (i.e. oldest
%% first). That's not necessarily optimal if nodes spend a long time
%% down, but we don't have a good way to predict what the optimal is
%% in that case anyway, and we assume nodes will not just be down for
%% a long time without being removed.
update_recoverable(SPids, RS) ->
    SNodes = [node(SPid) || SPid <- SPids],
    RunningNodes = rabbit_mnesia:cluster_nodes(running),
    AddNodes = SNodes -- RS,
    DelNodes = RunningNodes -- SNodes, %% i.e. running with no slave
    (RS -- DelNodes) ++ AddNodes.

stop_all_slaves(Reason, SPids, QName, GM, WaitTimeout) ->
    PidsMRefs = [{Pid, erlang:monitor(process, Pid)} || Pid <- [GM | SPids]],
    ok = gm:broadcast(GM, {delete_and_terminate, Reason}),
    %% It's possible that we could be partitioned from some slaves
    %% between the lookup and the broadcast, in which case we could
    %% monitor them but they would not have received the GM
    %% message. So only wait for slaves which are still
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
                          " node ~p~n", [Pid, node(Pid)]),
                        [Pid | Acc]
                end;
            false ->
                Acc
        end
    end, [], PidsMRefs),
    %% Normally when we remove a slave another slave or master will
    %% notice and update Mnesia. But we just removed them all, and
    %% have stopped listening ourselves. So manually clean up.
    rabbit_misc:execute_mnesia_transaction(fun () ->
        [Q0] = mnesia:read({rabbit_queue, QName}),
        Q1 = amqqueue:set_gm_pids(Q0, []),
        Q2 = amqqueue:set_slave_pids(Q1, []),
        %% Restarted slaves on running nodes can
        %% ensure old incarnations are stopped using
        %% the pending slave pids.
        Q3 = amqqueue:set_slave_pids_pending_shutdown(Q2, PendingSlavePids),
        rabbit_mirror_queue_misc:store_updated_slaves(Q3)
    end),
    ok = gm:forget_group(QName).

%%----------------------------------------------------------------------------

promote_slave([SPid | SPids]) ->
    %% The slave pids are maintained in descending order of age, so
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
%% rabbit_mnesia:cluster_nodes(running) out of a loop or transaction
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
    case module(Q) of
        {ok, _}  -> true;
        _        -> false
    end.

is_mirrored_ha_nodes(Q) ->
    case module(Q) of
        {ok, ?HA_NODES_MODULE} -> true;
        _ -> false
    end.

actual_queue_nodes(Q) when ?is_amqqueue(Q) ->
    MPid = amqqueue:get_pid(Q),
    SPids = amqqueue:get_slave_pids(Q),
    SSPids = amqqueue:get_sync_slave_pids(Q),
    Nodes = fun (L) -> [node(Pid) || Pid <- L] end,
    {case MPid of
         none -> none;
         _    -> node(MPid)
     end, Nodes(SPids), Nodes(SSPids)}.

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
    {OldMNode, OldSNodes, _} = actual_queue_nodes(Q),
    {NewMNode, NewSNodes}    = suggested_queue_nodes(Q),
    OldNodes = [OldMNode | OldSNodes],
    NewNodes = [NewMNode | NewSNodes],
    %% When a mirror dies, remove_from_queue/2 might have to add new
    %% slaves (in "exactly" mode). It will check mnesia to see which
    %% slaves there currently are. If drop_mirror/2 is invoked first
    %% then when we end up in remove_from_queue/2 it will not see the
    %% slaves that add_mirror/2 will add, and also want to add them
    %% (even though we are not responding to the death of a
    %% mirror). Breakage ensues.
    add_mirrors (QName, NewNodes -- OldNodes, async),
    drop_mirrors(QName, OldNodes -- NewNodes),
    %% This is for the case where no extra nodes were added but we changed to
    %% a policy requiring auto-sync.
    maybe_auto_sync(Q),
    ok.

queue_length(Q) ->
    [{messages, M}] = rabbit_amqqueue:info(Q, [messages]),
    M.

get_replicas(Q) ->
    {MNode, SNodes} = suggested_queue_nodes(Q),
    [MNode] ++ SNodes.

transfer_leadership(Q, Destination) ->
    QName = amqqueue:get_name(Q),
    {OldMNode, OldSNodes, _} = actual_queue_nodes(Q),
    OldNodes = [OldMNode | OldSNodes],
    add_mirrors(QName, [Destination] -- OldNodes, async),
    drop_mirrors(QName, OldNodes -- [Destination]),
    {Result, NewQ} = wait_for_new_master(QName, Destination),
    update_mirrors(NewQ),
    Result.

wait_for_new_master(QName, Destination) ->
    wait_for_new_master(QName, Destination, 100).

wait_for_new_master(QName, _, 0) ->
    {ok, Q} = rabbit_amqqueue:lookup(QName),
    {{not_migrated, ""}, Q};
wait_for_new_master(QName, Destination, N) ->
    {ok, Q} = rabbit_amqqueue:lookup(QName),
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
    end.

%% The arrival of a newly synced slave may cause the master to die if
%% the policy does not want the master but it has been kept alive
%% because there were no synced slaves.
%%
%% We don't just call update_mirrors/2 here since that could decide to
%% start a slave for some other reason, and since we are the slave ATM
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
%% not just shuffled it into the slaves. All our modes ensure this
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
