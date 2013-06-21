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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2010-2013 VMware, Inc.  All rights reserved.
%%

-module(rabbit_mirror_queue_misc).
-behaviour(rabbit_policy_validator).

-export([remove_from_queue/3, on_node_up/0, add_mirrors/2, add_mirror/2,
         report_deaths/4, store_updated_slaves/1, suggested_queue_nodes/1,
         is_mirrored/1, update_mirrors/2, validate_policy/1]).

%% for testing only
-export([module/1]).

-include("rabbit.hrl").

-rabbit_boot_step({?MODULE,
                   [{description, "HA policy validation"},
                    {mfa, {rabbit_registry, register,
                           [policy_validator, <<"ha-mode">>, ?MODULE]}},
                    {mfa, {rabbit_registry, register,
                           [policy_validator, <<"ha-params">>, ?MODULE]}},
                    {mfa, {rabbit_registry, register,
                           [policy_validator, <<"ha-sync-mode">>, ?MODULE]}},
                    {requires, rabbit_registry},
                    {enables, recovery}]}).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(remove_from_queue/3 ::
        (rabbit_amqqueue:name(), pid(), [pid()])
        -> {'ok', pid(), [pid()]} | {'error', 'not_found'}).
-spec(on_node_up/0 :: () -> 'ok').
-spec(add_mirrors/2 :: (rabbit_amqqueue:name(), [node()]) -> 'ok').
-spec(add_mirror/2 ::
        (rabbit_amqqueue:name(), node()) ->
                           {'ok', atom()} | rabbit_types:error(any())).
-spec(store_updated_slaves/1 :: (rabbit_types:amqqueue()) ->
                                     rabbit_types:amqqueue()).
-spec(suggested_queue_nodes/1 :: (rabbit_types:amqqueue()) ->
                                      {node(), [node()]}).
-spec(is_mirrored/1 :: (rabbit_types:amqqueue()) -> boolean()).
-spec(update_mirrors/2 ::
        (rabbit_types:amqqueue(), rabbit_types:amqqueue()) -> 'ok').

-endif.

%%----------------------------------------------------------------------------

%% If the dead pids include the queue pid (i.e. the master has died)
%% then only remove that if we are about to be promoted. Otherwise we
%% can have the situation where a slave updates the mnesia record for
%% a queue, promoting another slave before that slave realises it has
%% become the new master, which is bad because it could then mean the
%% slave (now master) receives messages it's not ready for (for
%% example, new consumers).
%% Returns {ok, NewMPid, DeadPids}
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
                      {Dead, GMPids1} = lists:partition(
                                          fun ({GM, _}) ->
                                                  lists:member(GM, DeadGMPids)
                                          end, GMPids),
                      DeadPids = [Pid || {_GM, Pid} <- Dead],
                      Alive = [QPid | SPids] -- DeadPids,
                      {QPid1, SPids1} = promote_slave(Alive),
                      case {{QPid, SPids}, {QPid1, SPids1}} of
                          {Same, Same} ->
                              GMPids = GMPids1, %% ASSERTION
                              {ok, QPid1, []};
                          _ when QPid =:= QPid1 orelse QPid1 =:= Self ->
                              %% Either master hasn't changed, so
                              %% we're ok to update mnesia; or we have
                              %% become the master.
                              store_updated_slaves(
                                Q #amqqueue { pid        = QPid1,
                                              slave_pids = SPids1,
                                              gm_pids    = GMPids1 }),
                              {ok, QPid1, [QPid | SPids] -- Alive};
                          _ ->
                              %% Master has changed, and we're not it,
                              %% so leave alone to allow the promoted
                              %% slave to find it and make its
                              %% promotion atomic.
                              {ok, QPid1, []}
                      end
              end
      end).

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
    [add_mirror(QName, node()) || QName <- QNames],
    ok.

drop_mirrors(QName, Nodes) ->
    [drop_mirror(QName, Node)  || Node <- Nodes],
    ok.

drop_mirror(QName, MirrorNode) ->
    rabbit_amqqueue:with(
      QName,
      fun (#amqqueue { name = Name, pid = QPid, slave_pids = SPids }) ->
              case [Pid || Pid <- [QPid | SPids], node(Pid) =:= MirrorNode] of
                  [] ->
                      {error, {queue_not_mirrored_on_node, MirrorNode}};
                  [QPid] when SPids =:= [] ->
                      {error, cannot_drop_only_mirror};
                  [Pid] ->
                      rabbit_log:info(
                        "Dropping queue mirror on node ~p for ~s~n",
                        [MirrorNode, rabbit_misc:rs(Name)]),
                      exit(Pid, {shutdown, dropped}),
                      {ok, dropped}
              end
      end).

add_mirrors(QName, Nodes) ->
    [add_mirror(QName, Node)  || Node <- Nodes],
    ok.

add_mirror(QName, MirrorNode) ->
    rabbit_amqqueue:with(
      QName,
      fun (#amqqueue { name = Name, pid = QPid, slave_pids = SPids } = Q) ->
              case [Pid || Pid <- [QPid | SPids], node(Pid) =:= MirrorNode] of
                  [] ->
                      start_child(Name, MirrorNode, Q);
                  [SPid] ->
                      case rabbit_misc:is_process_alive(SPid) of
                          true  -> {ok, already_mirrored};
                          false -> start_child(Name, MirrorNode, Q)
                      end
              end
      end).

start_child(Name, MirrorNode, Q) ->
    case rabbit_misc:with_exit_handler(
           rabbit_misc:const({ok, down}),
           fun () ->
                   rabbit_mirror_queue_slave_sup:start_child(MirrorNode, [Q])
           end) of
        {ok, SPid} when is_pid(SPid)  ->
            maybe_auto_sync(Q),
            rabbit_log:info("Adding mirror of ~s on node ~p: ~p~n",
                            [rabbit_misc:rs(Name), MirrorNode, SPid]),
            {ok, started};
        {error, {{stale_master_pid, StalePid}, _}} ->
            rabbit_log:warning("Detected stale HA master while adding "
                               "mirror of ~s on node ~p: ~p~n",
                               [rabbit_misc:rs(Name), MirrorNode, StalePid]),
            {ok, stale_master};
        {error, {{duplicate_live_master, _}=Err, _}} ->
            Err;
        Other ->
            Other
    end.

report_deaths(_MirrorPid, _IsMaster, _QueueName, []) ->
    ok;
report_deaths(MirrorPid, IsMaster, QueueName, DeadPids) ->
    rabbit_event:notify(queue_mirror_deaths, [{name, QueueName},
                                              {pids, DeadPids}]),
    rabbit_log:info("Mirrored-queue (~s): ~s ~s saw deaths of mirrors ~s~n",
                    [rabbit_misc:rs(QueueName),
                     case IsMaster of
                         true  -> "Master";
                         false -> "Slave"
                     end,
                     rabbit_misc:pid_to_string(MirrorPid),
                     [[rabbit_misc:pid_to_string(P), $ ] || P <- DeadPids]]).

store_updated_slaves(Q = #amqqueue{slave_pids      = SPids,
                                   sync_slave_pids = SSPids}) ->
    %% TODO now that we clear sync_slave_pids in rabbit_durable_queue,
    %% do we still need this filtering?
    SSPids1 = [SSPid || SSPid <- SSPids, lists:member(SSPid, SPids)],
    Q1 = Q#amqqueue{sync_slave_pids = SSPids1},
    ok = rabbit_amqqueue:store_queue(Q1),
    %% Wake it up so that we emit a stats event
    rabbit_amqqueue:wake_up(Q1),
    Q1.

%%----------------------------------------------------------------------------

promote_slave([SPid | SPids]) ->
    %% The slave pids are maintained in descending order of age, so
    %% the one to promote is the oldest.
    {SPid, SPids}.

suggested_queue_nodes(Q) ->
    suggested_queue_nodes(Q, rabbit_mnesia:cluster_nodes(running)).

%% This variant exists so we can pull a call to
%% rabbit_mnesia:cluster_nodes(running) out of a loop or
%% transaction or both.
suggested_queue_nodes(Q, All) ->
    {MNode0, SNodes, SSNodes} = actual_queue_nodes(Q),
    MNode = case MNode0 of
                none -> node();
                _    -> MNode0
            end,
    Params = policy(<<"ha-params">>, Q),
    case module(Q) of
        {ok, M} -> M:suggested_queue_nodes(Params, MNode, SNodes, SSNodes, All);
        _       -> {MNode, []}
    end.

policy(Policy, Q) ->
    case rabbit_policy:get(Policy, Q) of
        {ok, P} -> P;
        _       -> none
    end.

module(#amqqueue{} = Q) ->
    case rabbit_policy:get(<<"ha-mode">>, Q) of
        {ok, Mode} -> module(Mode);
        _          -> not_mirrored
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
    add_mirrors (QName, NewNodes -- OldNodes),
    drop_mirrors(QName, OldNodes -- NewNodes),
    maybe_auto_sync(NewQ),
    ok.

%%----------------------------------------------------------------------------

validate_policy(KeyList) ->
    Mode = proplists:get_value(<<"ha-mode">>, KeyList, none),
    Params = proplists:get_value(<<"ha-params">>, KeyList, none),
    SyncMode = proplists:get_value(<<"ha-sync-mode">>, KeyList, none),
    case {Mode, Params, SyncMode} of
        {none, none, none} ->
            ok;
        {none, _, _} ->
            {error, "ha-mode must be specified to specify ha-params or "
             "ha-sync-mode", []};
        _ ->
            case module(Mode) of
                {ok, M} -> case M:validate_policy(Params) of
                               ok -> validate_sync_mode(SyncMode);
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
