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
%% Copyright (c) 2010-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_mirror_queue_misc).
-behaviour(rabbit_policy_validator).

-export([remove_from_queue/2, on_node_up/0, add_mirrors/2, add_mirror/2,
         report_deaths/4, store_updated_slaves/1, suggested_queue_nodes/1,
         is_mirrored/1, update_mirrors/2, validate_policy/1]).

%% for testing only
-export([suggested_queue_nodes/4]).

-include("rabbit.hrl").

-rabbit_boot_step({?MODULE,
                   [{description, "HA policy validation capability"},
                    {mfa, {rabbit_registry, register,
                           [policy_validator, <<"ha-mode">>, ?MODULE]}},
                    {mfa, {rabbit_registry, register,
                           [policy_validator, <<"ha-params">>, ?MODULE]}},
                    {requires, rabbit_registry},
                    {enables, recovery}]}).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(remove_from_queue/2 ::
        (rabbit_amqqueue:name(), [pid()])
        -> {'ok', pid(), [pid()], [node()]} | {'error', 'not_found'}).
-spec(on_node_up/0 :: () -> 'ok').
-spec(add_mirrors/2 :: (rabbit_amqqueue:name(), [node()]) -> 'ok').
-spec(add_mirror/2 ::
        (rabbit_amqqueue:name(), node()) -> rabbit_types:ok_or_error(any())).
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

remove_from_queue(QueueName, DeadGMPids) ->
    DeadNodes = [node(DeadGMPid) || DeadGMPid <- DeadGMPids],
    ClusterNodes = rabbit_mnesia:cluster_nodes(running) -- DeadNodes,
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              %% Someone else could have deleted the queue before we
              %% get here.
              case mnesia:read({rabbit_queue, QueueName}) of
                  [] -> {error, not_found};
                  [Q = #amqqueue { pid        = QPid,
                                   slave_pids = SPids }] ->
                      Alive = [Pid || Pid <- [QPid | SPids],
                                  not lists:member(node(Pid), DeadNodes)],
                      {QPid1, SPids1} = promote_slave(Alive),
                      case {{QPid, SPids}, {QPid1, SPids1}} of
                          {Same, Same} ->
                              {ok, QPid1, [], []};
                          _ when QPid =:= QPid1 orelse node(QPid1) =:= node() ->
                              %% Either master hasn't changed, so
                              %% we're ok to update mnesia; or we have
                              %% become the master.
                              Q1 = store_updated_slaves(
                                     Q #amqqueue { pid        = QPid1,
                                                   slave_pids = SPids1 }),
                              %% Sometimes a slave dying means we need
                              %% to start more on other nodes -
                              %% "exactly" mode can cause this to
                              %% happen.
                              {_, OldNodes} = actual_queue_nodes(Q1),
                              {_, NewNodes} = suggested_queue_nodes(
                                                Q1, ClusterNodes),
                              {ok, QPid1, [QPid | SPids] -- Alive,
                               NewNodes -- OldNodes};
                          _ ->
                              %% Master has changed, and we're not it,
                              %% so leave alone to allow the promoted
                              %% slave to find it and make its
                              %% promotion atomic.
                              {ok, QPid1, [], []}
                      end
              end
      end).

on_node_up() ->
    ClusterNodes = rabbit_mnesia:cluster_nodes(running),
    QNames =
        rabbit_misc:execute_mnesia_transaction(
          fun () ->
                  mnesia:foldl(
                    fun (Q = #amqqueue{name = QName}, QNames0) ->
                            {_MNode, SNodes} = suggested_queue_nodes(
                                                 Q, ClusterNodes),
                            case lists:member(node(), SNodes) of
                                true  -> [QName | QNames0];
                                false -> QNames0
                            end
                    end, [], rabbit_queue)
          end),
    [ok = add_mirror(QName, node()) || QName <- QNames],
    ok.

drop_mirrors(QName, Nodes) ->
    [ok = drop_mirror(QName, Node)  || Node <- Nodes],
    ok.

drop_mirror(QName, MirrorNode) ->
    if_mirrored_queue(
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
                      ok
              end
      end).

add_mirrors(QName, Nodes) ->
    [ok = add_mirror(QName, Node)  || Node <- Nodes],
    ok.

add_mirror(QName, MirrorNode) ->
    if_mirrored_queue(
      QName,
      fun (#amqqueue { name = Name, pid = QPid, slave_pids = SPids } = Q) ->
              case [Pid || Pid <- [QPid | SPids], node(Pid) =:= MirrorNode] of
                  [] ->
                      start_child(Name, MirrorNode, Q);
                  [SPid] ->
                      case rabbit_misc:is_process_alive(SPid) of
                          true ->
                              {error,{queue_already_mirrored_on_node,
                                      MirrorNode}};
                          false ->
                              start_child(Name, MirrorNode, Q)
                      end
              end
      end).

start_child(Name, MirrorNode, Q) ->
    case rabbit_misc:with_exit_handler(
           rabbit_misc:const({ok, down}),
           fun () ->
                   rabbit_mirror_queue_slave_sup:start_child(MirrorNode, [Q])
           end) of
        {ok, undefined} ->
            %% this means the mirror process was
            %% already running on the given node.
            ok;
        {ok, down} ->
            %% Node went down between us deciding to start a mirror
            %% and actually starting it. Which is fine.
            ok;
        {ok, SPid} ->
            rabbit_log:info("Adding mirror of ~s on node ~p: ~p~n",
                            [rabbit_misc:rs(Name), MirrorNode, SPid]),
            ok;
        {error, {{stale_master_pid, StalePid}, _}} ->
            rabbit_log:warning("Detected stale HA master while adding "
                               "mirror of ~s on node ~p: ~p~n",
                               [rabbit_misc:rs(Name), MirrorNode, StalePid]),
            ok;
        {error, {{duplicate_live_master, _}=Err, _}} ->
            Err;
        Other ->
            Other
    end.

if_mirrored_queue(QName, Fun) ->
    rabbit_amqqueue:with(QName, fun (Q) ->
                                        case is_mirrored(Q) of
                                            false -> ok;
                                            true  -> Fun(Q)
                                        end
                                end).

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
suggested_queue_nodes(Q, ClusterNodes) ->
    {MNode0, SNodes} = actual_queue_nodes(Q),
    MNode = case MNode0 of
                none -> node();
                _    -> MNode0
            end,
    suggested_queue_nodes(policy(<<"ha-mode">>, Q), policy(<<"ha-params">>, Q),
                          {MNode, SNodes}, ClusterNodes).

policy(Policy, Q) ->
    case rabbit_policy:get(Policy, Q) of
        {ok, P} -> P;
        _       -> none
    end.

suggested_queue_nodes(<<"all">>, _Params, {MNode, _SNodes}, All) ->
    {MNode, All -- [MNode]};
suggested_queue_nodes(<<"nodes">>, Nodes0, {MNode, _SNodes}, All) ->
    Nodes = [list_to_atom(binary_to_list(Node)) || Node <- Nodes0],
    Unavailable = Nodes -- All,
    Available = Nodes -- Unavailable,
    case Available of
        [] -> %% We have never heard of anything? Not much we can do but
              %% keep the master alive.
              {MNode, []};
        _  -> case lists:member(MNode, Available) of
                  true  -> {MNode, Available -- [MNode]};
                  false -> promote_slave(Available)
              end
    end;
suggested_queue_nodes(<<"exactly">>, Count, {MNode, SNodes}, All) ->
    SCount = Count - 1,
    {MNode, case SCount > length(SNodes) of
                true  -> Cand = (All -- [MNode]) -- SNodes,
                         SNodes ++ lists:sublist(Cand, SCount - length(SNodes));
                false -> lists:sublist(SNodes, SCount)
            end};
suggested_queue_nodes(_, _, {MNode, _}, _) ->
    {MNode, []}.

actual_queue_nodes(#amqqueue{pid = MPid, slave_pids = SPids}) ->
    {case MPid of
         none -> none;
         _    -> node(MPid)
     end, [node(Pid) || Pid <- SPids]}.

is_mirrored(Q) ->
    case policy(<<"ha-mode">>, Q) of
        <<"all">>     -> true;
        <<"nodes">>   -> true;
        <<"exactly">> -> true;
        _             -> false
    end.


%% [1] - rabbit_amqqueue:start_mirroring/1 will turn unmirrored to
%% master and start any needed slaves. However, if node(QPid) is not
%% in the nodes for the policy, it won't switch it. So this is for the
%% case where we kill the existing queue and restart elsewhere. TODO:
%% is this TRTTD? All alternatives seem ugly.
update_mirrors(OldQ = #amqqueue{pid = QPid},
               NewQ = #amqqueue{pid = QPid}) ->
    case {is_mirrored(OldQ), is_mirrored(NewQ)} of
        {false, false} -> ok;
        {true,  false} -> rabbit_amqqueue:stop_mirroring(QPid);
        {false, true}  -> rabbit_amqqueue:start_mirroring(QPid),
                          update_mirrors0(OldQ, NewQ); %% [1]
        {true, true}   -> update_mirrors0(OldQ, NewQ)
    end.

update_mirrors0(OldQ = #amqqueue{name = QName},
                NewQ = #amqqueue{name = QName}) ->
    All = fun ({A,B}) -> [A|B] end,
    OldNodes = All(actual_queue_nodes(OldQ)),
    NewNodes = All(suggested_queue_nodes(NewQ)),
    %% When a mirror dies, remove_from_queue/2 might have to add new
    %% slaves (in "exactly" mode). It will check mnesia to see which
    %% slaves there currently are. If drop_mirror/2 is invoked first
    %% then when we end up in remove_from_queue/2 it will not see the
    %% slaves that add_mirror/2 will add, and also want to add them
    %% (even though we are not responding to the death of a
    %% mirror). Breakage ensues.
    add_mirrors(QName, NewNodes -- OldNodes),
    drop_mirrors(QName, OldNodes -- NewNodes),
    ok.

%%----------------------------------------------------------------------------

validate_policy(TagList) ->
    Mode   = proplists:get_all_values(<<"ha-mode">>, TagList),
    Params = proplists:get_all_values(<<"ha-params">>, TagList),
    case Mode of
        [<<"all">>] ->
            ok;
        [<<"nodes">>] ->
            validate_params(lists:append(Params),
                            fun erlang:is_binary/1,
                            "~p has invalid node names when ha-mode=nodes",
                            fun (N) -> N > 0 end,
                            "at least one node expected when ha-mode=nodes");
        [<<"exactly">>] ->
            validate_params(Params,
                            fun (N) -> is_integer(N) andalso N > 0 end,
                            "~p must be a positive integer",
                            fun (N) -> N == 1 end,
                            "ha-params must be supplied with one number "
                            "when ha-mode=exactly");
        [_, _|_] ->
            {error, "ha-mode may appear at most once", []};
        [Other] ->
            {error, "~p is not a valid ha-mode value", [Other]}
    end.

validate_params(Params, FilterPred, FilterMsg, SizePred, SizeMsg)
  when is_list(Params) ->
    case SizePred(length(Params)) of
        true -> case lists:filter(fun (P) -> not FilterPred(P) end, Params) of
                    [] -> ok;
                    X  -> {error, FilterMsg, [X]}
                end;
        false -> {error, SizeMsg, []}
    end;
validate_params(Params, _, _, _, _) ->
    {error, "~p was expected to be a list", [Params]}.
