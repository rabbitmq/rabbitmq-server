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
%% Copyright (c) 2007-2010 VMware, Inc.  All rights reserved.
%%

-module(rabbit_mirror_queue_misc).

-export([remove_from_queue/2, on_node_up/0,
         drop_slave/2, drop_slave/3, add_slave/2, add_slave/3]).

-include("rabbit.hrl").

%% If the dead pids include the queue pid (i.e. the master has died)
%% then only remove that if we are about to be promoted. Otherwise we
%% can have the situation where a slave updates the mnesia record for
%% a queue, promoting another slave before that slave realises it has
%% become the new master.
remove_from_queue(QueueName, DeadPids) ->
    DeadNodes = [node(DeadPid) || DeadPid <- DeadPids],
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              %% Someone else could have deleted the queue before we
              %% get here.
              case mnesia:read({rabbit_queue, QueueName}) of
                  [] -> {error, not_found};
                  [Q = #amqqueue { pid         = QPid,
                                   mirror_pids = MPids }] ->
                      [QPid1 | MPids1] =
                          [Pid || Pid <- [QPid | MPids],
                                  not lists:member(node(Pid), DeadNodes)],
                      case {{QPid, MPids}, {QPid1, MPids1}} of
                          {Same, Same} ->
                              ok;
                          _ when QPid =:= QPid1 orelse node(QPid1) =:= node() ->
                              %% Either master hasn't changed, so
                              %% we're ok to update mnesia; or master
                              %% has changed to become us!
                              Q1 = Q #amqqueue { pid         = QPid1,
                                                 mirror_pids = MPids1 },
                              ok = rabbit_amqqueue:store_queue(Q1);
                          _ ->
                              %% Master has changed, and we're not it,
                              %% so leave alone to allow the promoted
                              %% slave to find it and make its
                              %% promotion atomic.
                              ok
                      end,
                      {ok, QPid1}
              end
      end).

on_node_up() ->
    Qs =
        rabbit_misc:execute_mnesia_transaction(
          fun () ->
                  mnesia:foldl(
                    fun (#amqqueue{ arguments = Args, name = QName }, QsN) ->
                            case rabbit_misc:table_lookup(
                                   Args, <<"x-mirror">>) of
                                {_Type, []} ->
                                    [QName | QsN];
                                {_Type, Nodes} ->
                                    Nodes1 = [list_to_atom(binary_to_list(Node))
                                              || {longstr, Node} <- Nodes],
                                    case lists:member(node(), Nodes1) of
                                        true  -> [QName | QsN];
                                        false -> QsN
                                    end;
                                _ ->
                                    QsN
                            end
                    end, [], rabbit_queue)
          end),
    [add_slave(Q, node()) || Q <- Qs],
    ok.

drop_slave(VHostPath, QueueName, MirrorNode) ->
    drop_slave(rabbit_misc:r(VHostPath, queue, QueueName), MirrorNode).

drop_slave(Queue, MirrorNode) ->
    if_mirrored_queue(
      Queue,
      fun (#amqqueue { name = Name, pid = QPid, mirror_pids = MPids }) ->
              case [Pid || Pid <- [QPid | MPids], node(Pid) =:= MirrorNode] of
                  [] ->
                      {error, {queue_not_mirrored_on_node, MirrorNode}};
                  [QPid | MPids] ->
                      {error, cannot_drop_only_mirror};
                  [Pid] ->
                      rabbit_log:info("Dropping slave node on ~p for ~s~n",
                                      [MirrorNode, rabbit_misc:rs(Name)]),
                      exit(Pid, {shutdown, dropped}),
                      ok
              end
      end).

add_slave(VHostPath, QueueName, MirrorNode) ->
    add_slave(rabbit_misc:r(VHostPath, queue, QueueName), MirrorNode).

add_slave(Queue, MirrorNode) ->
    if_mirrored_queue(
      Queue,
      fun (#amqqueue { name = Name, pid = QPid, mirror_pids = MPids } = Q) ->
              case [Pid || Pid <- [QPid | MPids], node(Pid) =:= MirrorNode] of
                  []  -> Result = rabbit_mirror_queue_slave_sup:start_child(
                                    MirrorNode, [Q]),
                         rabbit_log:info(
                           "Adding slave node for ~s on node ~p: ~p~n",
                           [rabbit_misc:rs(Name), MirrorNode, Result]),
                         case Result of
                             {ok, _Pid} -> ok;
                             _          -> Result
                         end;
                  [_] -> {error, {queue_already_mirrored_on_node, MirrorNode}}
              end
      end).

if_mirrored_queue(Queue, Fun) ->
    rabbit_amqqueue:with(
      Queue, fun (#amqqueue { arguments = Args } = Q) ->
                     case rabbit_misc:table_lookup(Args, <<"x-mirror">>) of
                         undefined -> ok;
                         _         -> Fun(Q)
                     end
             end).
