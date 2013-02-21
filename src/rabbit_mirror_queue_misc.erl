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
-export([suggested_queue_nodes/4]).

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
suggested_queue_nodes(Q, PossibleNodes) ->
    {MNode0, SNodes, SSNodes} = actual_queue_nodes(Q),
    MNode = case MNode0 of
                none -> node();
                _    -> MNode0
            end,
    suggested_queue_nodes(policy(<<"ha-mode">>, Q), policy(<<"ha-params">>, Q),
                          {MNode, SNodes, SSNodes}, PossibleNodes).

policy(Policy, Q) ->
    case rabbit_policy:get(Policy, Q) of
        {ok, P} -> P;
        _       -> none
    end.

suggested_queue_nodes(<<"all">>, _Params, {MNode, _SNodes, _SSNodes}, Poss) ->
    {MNode, Poss -- [MNode]};
suggested_queue_nodes(<<"nodes">>, Nodes0, {MNode, _SNodes, SSNodes}, Poss) ->
    Nodes1 = [list_to_atom(binary_to_list(Node)) || Node <- Nodes0],
    %% If the current master is not in the nodes specified, then what we want
    %% to do depends on whether there are any synchronised slaves. If there
    %% are then we can just kill the current master - the admin has asked for
    %% a migration and we should give it to them. If there are not however
    %% then we must keep the master around so as not to lose messages.
    Nodes = case SSNodes of
                [] -> lists:usort([MNode | Nodes1]);
                _  -> Nodes1
            end,
    Unavailable = Nodes -- Poss,
    Available = Nodes -- Unavailable,
    case Available of
        [] -> %% We have never heard of anything? Not much we can do but
              %% keep the master alive.
              {MNode, []};
        _  -> case lists:member(MNode, Available) of
                  true  -> {MNode, Available -- [MNode]};
                  false -> %% Make sure the new master is synced! In order to
                           %% get here SSNodes must not be empty.
                           [NewMNode | _] = SSNodes,
                           {NewMNode, Available -- [NewMNode]}
              end
    end;
%% When we need to add nodes, we randomise our candidate list as a
%% crude form of load-balancing. TODO it would also be nice to
%% randomise the list of ones to remove when we have too many - we
%% would have to take account of synchronisation though.
suggested_queue_nodes(<<"exactly">>, Count, {MNode, SNodes, _SSNodes}, Poss) ->
    SCount = Count - 1,
    {MNode, case SCount > length(SNodes) of
                true  -> Cand = shuffle((Poss -- [MNode]) -- SNodes),
                         SNodes ++ lists:sublist(Cand, SCount - length(SNodes));
                false -> lists:sublist(SNodes, SCount)
            end};
suggested_queue_nodes(_, _, {MNode, _, _}, _) ->
    {MNode, []}.

shuffle(L) ->
    {A1,A2,A3} = now(),
    random:seed(A1, A2, A3),
    {_, L1} = lists:unzip(lists:keysort(1, [{random:uniform(), N} || N <- L])),
    L1.

actual_queue_nodes(#amqqueue{pid             = MPid,
                             slave_pids      = SPids,
                             sync_slave_pids = SSPids}) ->
    Nodes = fun (L) -> [node(Pid) || Pid <- L] end,
    {case MPid of
         none -> none;
         _    -> node(MPid)
     end, Nodes(SPids), Nodes(SSPids)}.

is_mirrored(Q) ->
    case policy(<<"ha-mode">>, Q) of
        <<"all">>     -> true;
        <<"nodes">>   -> true;
        <<"exactly">> -> true;
        _             -> false
    end.

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
    case validate_policy(
           proplists:get_value(<<"ha-mode">>,   KeyList),
           proplists:get_value(<<"ha-params">>, KeyList, none)) of
        ok -> case proplists:get_value(
                     <<"ha-sync-mode">>, KeyList, <<"manual">>) of
                  <<"automatic">> -> ok;
                  <<"manual">>    -> ok;
                  Mode            -> {error, "ha-sync-mode must be \"manual\" "
                                      "or \"automatic\", got ~p", [Mode]}
              end;
        E  -> E
    end.

validate_policy(<<"all">>, none) ->
    ok;
validate_policy(<<"all">>, _Params) ->
    {error, "ha-mode=\"all\" does not take parameters", []};

validate_policy(<<"nodes">>, []) ->
    {error, "ha-mode=\"nodes\" list must be non-empty", []};
validate_policy(<<"nodes">>, Nodes) when is_list(Nodes) ->
    case [I || I <- Nodes, not is_binary(I)] of
        []      -> ok;
        Invalid -> {error, "ha-mode=\"nodes\" takes a list of strings, "
                    "~p was not a string", [Invalid]}
    end;
validate_policy(<<"nodes">>, Params) ->
    {error, "ha-mode=\"nodes\" takes a list, ~p given", [Params]};

validate_policy(<<"exactly">>, N) when is_integer(N) andalso N > 0 ->
    ok;
validate_policy(<<"exactly">>, Params) ->
    {error, "ha-mode=\"exactly\" takes an integer, ~p given", [Params]};

validate_policy(Mode, _Params) ->
    {error, "~p is not a valid ha-mode value", [Mode]}.

