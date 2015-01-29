%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2010-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_mirror_queue_misc).
-behaviour(rabbit_policy_validator).

-export([remove_from_queue/3, on_node_up/0, add_mirrors/3,
         report_deaths/4, store_updated_slaves/1,
         initial_queue_node/2, suggested_queue_nodes/1,
         is_mirrored/1, update_mirrors/2, validate_policy/1,
         maybe_auto_sync/1, maybe_drop_master_after_sync/1,
         log_info/3, log_warning/3]).

%% for testing only
-export([module/1]).

-include("rabbit.hrl").

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
            [policy_validator, <<"ha-promote-on-shutdown">>, ?MODULE]}},
     {requires, rabbit_registry},
     {enables, recovery}]}).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(remove_from_queue/3 ::
        (rabbit_amqqueue:name(), pid(), [pid()])
        -> {'ok', pid(), [pid()], [node()]} | {'error', 'not_found'}).
-spec(on_node_up/0 :: () -> 'ok').
-spec(add_mirrors/3 :: (rabbit_amqqueue:name(), [node()], 'sync' | 'async')
                       -> 'ok').
-spec(store_updated_slaves/1 :: (rabbit_types:amqqueue()) ->
                                     rabbit_types:amqqueue()).
-spec(initial_queue_node/2 :: (rabbit_types:amqqueue(), node()) -> node()).
-spec(suggested_queue_nodes/1 :: (rabbit_types:amqqueue()) ->
                                      {node(), [node()]}).
-spec(is_mirrored/1 :: (rabbit_types:amqqueue()) -> boolean()).
-spec(update_mirrors/2 ::
        (rabbit_types:amqqueue(), rabbit_types:amqqueue()) -> 'ok').
-spec(maybe_drop_master_after_sync/1 :: (rabbit_types:amqqueue()) -> 'ok').
-spec(maybe_auto_sync/1 :: (rabbit_types:amqqueue()) -> 'ok').
-spec(log_info/3 :: (rabbit_amqqueue:name(), string(), [any()]) -> 'ok').
-spec(log_warning/3 :: (rabbit_amqqueue:name(), string(), [any()]) -> 'ok').

-endif.

%%----------------------------------------------------------------------------

%% Returns {ok, NewMPid, DeadPids, ExtraNodes}
remove_from_queue(QueueName, Self, DeadGMPids) ->
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              %% Someone else could have deleted the queue before we
              %% get here.
              case mnesia:read({rabbit_queue, QueueName}) of
                  [] -> {error, not_found};
                  [Q = #amqqueue { pid        = QPid,
                                   slave_pids = SPids,
                                   gm_pids    = GMPids }] ->
                      {DeadGM, AliveGM} = lists:partition(
                                            fun ({GM, _}) ->
                                                    lists:member(GM, DeadGMPids)
                                            end, GMPids),
                      DeadPids  = [Pid || {_GM, Pid} <- DeadGM],
                      AlivePids = [Pid || {_GM, Pid} <- AliveGM],
                      Alive     = [Pid || Pid <- [QPid | SPids],
                                          lists:member(Pid, AlivePids)],
                      {QPid1, SPids1} = promote_slave(Alive),
                      Extra =
                          case {{QPid, SPids}, {QPid1, SPids1}} of
                              {Same, Same} ->
                                  [];
                              _ when QPid =:= QPid1 orelse QPid1 =:= Self ->
                                  %% Either master hasn't changed, so
                                  %% we're ok to update mnesia; or we have
                                  %% become the master.
                                  Q1 = Q#amqqueue{pid        = QPid1,
                                                  slave_pids = SPids1,
                                                  gm_pids    = AliveGM},
                                  store_updated_slaves(Q1),
                                  %% If we add and remove nodes at the
                                  %% same time we might tell the old
                                  %% master we need to sync and then
                                  %% shut it down. So let's check if
                                  %% the new master needs to sync.
                                  maybe_auto_sync(Q1),
                                  slaves_to_start_on_failure(Q1, DeadGMPids);
                          _ ->
                                  %% Master has changed, and we're not it.
                                  %% [1].
                                  Q1 = Q#amqqueue{slave_pids = Alive,
                                                  gm_pids    = AliveGM},
                                  store_updated_slaves(Q1),
                                  []
                          end,
                      {ok, QPid1, DeadPids, Extra}
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

on_node_up() ->
    QNames =
        rabbit_misc:execute_mnesia_transaction(
          fun () ->
                  mnesia:foldl(
                    fun (Q = #amqqueue{name       = QName,
                                       pid        = Pid,
                                       slave_pids = SPids}, QNames0) ->
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
                            end
                    end, [], rabbit_queue)
          end),
    [add_mirror(QName, node(), async) || QName <- QNames],
    ok.

drop_mirrors(QName, Nodes) ->
    [drop_mirror(QName, Node)  || Node <- Nodes],
    ok.

drop_mirror(QName, MirrorNode) ->
    case rabbit_amqqueue:lookup(QName) of
        {ok, #amqqueue { name = Name, pid = QPid, slave_pids = SPids }} ->
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

add_mirrors(QName, Nodes, SyncMode) ->
    [add_mirror(QName, Node, SyncMode)  || Node <- Nodes],
    ok.

add_mirror(QName, MirrorNode, SyncMode) ->
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} ->
            rabbit_misc:with_exit_handler(
              rabbit_misc:const(ok),
              fun () ->
                      SPid = rabbit_amqqueue_sup_sup:start_queue_process(
                               MirrorNode, Q, slave),
                      log_info(QName, "Adding mirror on node ~p: ~p~n",
                               [MirrorNode, SPid]),
                      rabbit_mirror_queue_slave:go(SPid, SyncMode)
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

log_info   (QName, Fmt, Args) -> log(info,    QName, Fmt, Args).
log_warning(QName, Fmt, Args) -> log(warning, QName, Fmt, Args).

log(Level, QName, Fmt, Args) ->
    rabbit_log:log(mirroring, Level, "Mirrored ~s: " ++ Fmt,
                   [rabbit_misc:rs(QName) | Args]).

store_updated_slaves(Q = #amqqueue{slave_pids         = SPids,
                                   sync_slave_pids    = SSPids,
                                   recoverable_slaves = RS}) ->
    %% TODO now that we clear sync_slave_pids in rabbit_durable_queue,
    %% do we still need this filtering?
    SSPids1 = [SSPid || SSPid <- SSPids, lists:member(SSPid, SPids)],
    Q1 = Q#amqqueue{sync_slave_pids    = SSPids1,
                    recoverable_slaves = update_recoverable(SPids, RS),
                    state              = live},
    ok = rabbit_amqqueue:store_queue(Q1),
    %% Wake it up so that we emit a stats event
    rabbit_amqqueue:notify_policy_changed(Q1),
    Q1.

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

%%----------------------------------------------------------------------------

promote_slave([SPid | SPids]) ->
    %% The slave pids are maintained in descending order of age, so
    %% the one to promote is the oldest.
    {SPid, SPids}.

initial_queue_node(Q, DefNode) ->
    {MNode, _SNodes} = suggested_queue_nodes(Q, DefNode, all_nodes()),
    MNode.

suggested_queue_nodes(Q)      -> suggested_queue_nodes(Q, all_nodes()).
suggested_queue_nodes(Q, All) -> suggested_queue_nodes(Q, node(), All).

%% The third argument exists so we can pull a call to
%% rabbit_mnesia:cluster_nodes(running) out of a loop or transaction
%% or both.
suggested_queue_nodes(Q = #amqqueue{exclusive_owner = Owner}, DefNode, All) ->
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

all_nodes() -> rabbit_mnesia:cluster_nodes(running).

policy(Policy, Q) ->
    case rabbit_policy:get(Policy, Q) of
        undefined -> none;
        P         -> P
    end.

module(#amqqueue{} = Q) ->
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

is_mirrored(Q) ->
    case module(Q) of
        {ok, _}  -> true;
        _        -> false
    end.

actual_queue_nodes(#amqqueue{pid             = MPid,
                             slave_pids      = SPids,
                             sync_slave_pids = SSPids}) ->
    Nodes = fun (L) -> [node(Pid) || Pid <- L] end,
    {case MPid of
         none -> none;
         _    -> node(MPid)
     end, Nodes(SPids), Nodes(SSPids)}.

maybe_auto_sync(Q = #amqqueue{pid = QPid}) ->
    case policy(<<"ha-sync-mode">>, Q) of
        <<"automatic">> ->
            spawn(fun() -> rabbit_amqqueue:sync_mirrors(QPid) end);
        _ ->
            ok
    end.

update_mirrors(OldQ = #amqqueue{pid = QPid},
               NewQ = #amqqueue{pid = QPid}) ->
    case {is_mirrored(OldQ), is_mirrored(NewQ)} of
        {false, false} -> ok;
        {true,  false} -> rabbit_amqqueue:stop_mirroring(QPid);
        {false,  true} -> rabbit_amqqueue:start_mirroring(QPid);
        {true,   true} -> update_mirrors0(OldQ, NewQ)
    end.

update_mirrors0(OldQ = #amqqueue{name = QName},
                NewQ = #amqqueue{name = QName}) ->
    {OldMNode, OldSNodes, _} = actual_queue_nodes(OldQ),
    {NewMNode, NewSNodes}    = suggested_queue_nodes(NewQ),
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
    maybe_auto_sync(NewQ),
    ok.

%% The arrival of a newly synced slave may cause the master to die if
%% the policy does not want the master but it has been kept alive
%% because there were no synced slaves.
%%
%% We don't just call update_mirrors/2 here since that could decide to
%% start a slave for some other reason, and since we are the slave ATM
%% that allows complicated deadlocks.
maybe_drop_master_after_sync(Q = #amqqueue{name = QName,
                                           pid  = MPid}) ->
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
    PromoteOnShutdown = proplists:get_value(
                          <<"ha-promote-on-shutdown">>, KeyList, none),
    case {Mode, Params, SyncMode, PromoteOnShutdown} of
        {none, none, none, none} ->
            ok;
        {none, _, _, _} ->
            {error, "ha-mode must be specified to specify ha-params, "
             "ha-sync-mode or ha-promote-on-shutdown", []};
        _ ->
            case module(Mode) of
                {ok, M} -> case M:validate_policy(Params) of
                               ok -> case validate_sync_mode(SyncMode) of
                                         ok -> validate_pos(PromoteOnShutdown);
                                         E  -> E
                                     end;
                               E  -> E
                           end;
                _       -> {error, "~p is not a valid ha-mode value", [Mode]}
            end
    end.

validate_sync_mode(SyncMode) ->
    case SyncMode of
        <<"automatic">> -> ok;
        <<"manual">>    -> ok;
        none            -> ok;
        Mode            -> {error, "ha-sync-mode must be \"manual\" "
                            "or \"automatic\", got ~p", [Mode]}
    end.

validate_pos(PromoteOnShutdown) ->
    case PromoteOnShutdown of
        <<"always">>      -> ok;
        <<"when-synced">> -> ok;
        none              -> ok;
        Mode              -> {error, "ha-promote-on-shutdown must be "
                              "\"always\" or \"when-synced\", got ~p", [Mode]}
    end.
