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

-export([remove_from_queue/2, on_node_up/0,
         drop_mirror/2, drop_mirror/3, add_mirror/2, add_mirror/3,
         report_deaths/4, store_updated_slaves/1]).

%% temp
-export([suggested_queue_nodes/1, is_mirrored/1, update_mirrors/2]).


-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(remove_from_queue/2 ::
        (rabbit_amqqueue:name(), [pid()])
        -> {'ok', pid(), [pid()]} | {'error', 'not_found'}).
-spec(on_node_up/0 :: () -> 'ok').
-spec(drop_mirror/2 ::
        (rabbit_amqqueue:name(), node()) -> rabbit_types:ok_or_error(any())).
-spec(add_mirror/2 ::
        (rabbit_amqqueue:name(), node()) -> rabbit_types:ok_or_error(any())).
-spec(add_mirror/3 ::
        (rabbit_types:vhost(), binary(), atom())
        -> rabbit_types:ok_or_error(any())).
-spec(store_updated_slaves/1 :: (rabbit_types:amqqueue()) ->
                                     rabbit_types:amqqueue()).

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
remove_from_queue(QueueName, DeadPids) ->
    DeadNodes = [node(DeadPid) || DeadPid <- DeadPids],
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              %% Someone else could have deleted the queue before we
              %% get here.
              case mnesia:read({rabbit_queue, QueueName}) of
                  [] -> {error, not_found};
                  [Q = #amqqueue { pid        = QPid,
                                   slave_pids = SPids }] ->
                      [QPid1 | SPids1] = Alive =
                          [Pid || Pid <- [QPid | SPids],
                                  not lists:member(node(Pid),
                                                   DeadNodes) orelse
                                  rabbit_misc:is_process_alive(Pid)],
                      case {{QPid, SPids}, {QPid1, SPids1}} of
                          {Same, Same} ->
                              {ok, QPid1, []};
                          _ when QPid =:= QPid1 orelse node(QPid1) =:= node() ->
                              %% Either master hasn't changed, so
                              %% we're ok to update mnesia; or we have
                              %% become the master.
                              store_updated_slaves(
                                Q #amqqueue { pid        = QPid1,
                                              slave_pids = SPids1 }),
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
                    fun (Q = #amqqueue{name = QName}, QNames0) ->
                            {_MNode, SNodes} = suggested_queue_nodes(Q),
                            case lists:member(node(), SNodes) of
                                true  -> [QName | QNames0];
                                false -> QNames0
                            end
                    end, [], rabbit_queue)
          end),
    [add_mirror(QName, node()) || QName <- QNames],
    ok.

drop_mirror(VHostPath, QueueName, MirrorNode) ->
    drop_mirror(rabbit_misc:r(VHostPath, queue, QueueName), MirrorNode).

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

add_mirror(VHostPath, QueueName, MirrorNode) ->
    add_mirror(rabbit_misc:r(VHostPath, queue, QueueName), MirrorNode).

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
    case rabbit_mirror_queue_slave_sup:start_child(MirrorNode, [Q]) of
        {ok, undefined} ->
            %% this means the mirror process was
            %% already running on the given node.
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
            throw(Err);
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

%% TODO this should take account of current nodes so we don't throw
%% away mirrors or change the master needlessly
suggested_queue_nodes(Q) ->
    case [rabbit_policy:get(P, Q) || P <- [<<"ha-mode">>, <<"ha-params">>]] of
        [{ok, <<"all">>}, _] ->
            {node(), rabbit_mnesia:all_clustered_nodes() -- [node()]};
        [{ok, <<"nodes">>}, {ok, Nodes}] ->
            case [list_to_atom(binary_to_list(Node)) || Node <- Nodes] of
                [Node]         -> {Node,   []};
                [First | Rest] -> {First,  Rest}
            end;
        [{ok, <<"at-least">>}, {ok, Count}] ->
            {node(), lists:sublist(
                       rabbit_mnesia:all_clustered_nodes(), Count) -- [node()]};
        _ ->
            {node(), []}
    end.

actual_queue_nodes(#amqqueue{pid = MPid, slave_pids = SPids}) ->
    MNode = case MPid of
                undefined -> undefined;
                _         -> node(MPid)
            end,
    SNodes = case SPids of
                 undefined -> undefined;
                 _         -> [node(Pid) || Pid <- SPids]
             end,
    {MNode, SNodes}.

is_mirrored(Q) ->
    case rabbit_policy:get(<<"ha-mode">>, Q) of
        {ok, <<"all">>}      -> true;
        {ok, <<"nodes">>}    -> true;
        {ok, <<"at-least">>} -> true;
        _                    -> false
    end.

update_mirrors(OldQ = #amqqueue{name = QName, pid = QPid},
               NewQ = #amqqueue{name = QName, pid = QPid}) ->
    case {is_mirrored(OldQ), is_mirrored(NewQ)} of
        {false, false} -> ok;
        {true,  false} -> rabbit_amqqueue:stop_mirroring(QPid);
        {false, true}  -> rabbit_amqqueue:start_mirroring(QPid);
        {true, true}   -> {OldMNode, OldSNodes} = actual_queue_nodes(OldQ),
                          {NewMNode, NewSNodes} = suggested_queue_nodes(NewQ),
                          case OldMNode of
                              NewMNode -> ok;
                              _        -> io:format("TODO: master needs to change for ~p~n", [NewQ])
                          end,
                          Add = NewSNodes -- OldSNodes,
                          Remove = OldSNodes -- NewSNodes,
                          [ok = drop_mirror(QName, SNode) || SNode <- Remove],
                          [ok = add_mirror(QName, SNode) || SNode <- Add]
    end.
