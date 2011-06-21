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
         drop_mirror/2, drop_mirror/3, add_mirror/2, add_mirror/3]).

-include("rabbit.hrl").

%% If the dead pids include the queue pid (i.e. the master has died)
%% then only remove that if we are about to be promoted. Otherwise we
%% can have the situation where a slave updates the mnesia record for
%% a queue, promoting another slave before that slave realises it has
%% become the new master, which is bad because it could then mean the
%% slave (now master) receives messages it's not ready for (for
%% example, new consumers).
remove_from_queue(QueueName, DeadPids) ->
    DeadNodes = [node(DeadPid) || DeadPid <- DeadPids],
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              %% Someone else could have deleted the queue before we
              %% get here.
              case mnesia:read({rabbit_queue, QueueName}) of
                  [] -> {error, not_found};
                  [Q = #amqqueue { pid          = QPid,
                                   slave_pids   = SPids }] ->
                      [QPid1 | SPids1] =
                          [Pid || Pid <- [QPid | SPids],
                                  not lists:member(node(Pid), DeadNodes)],
                      case {{QPid, SPids}, {QPid1, SPids1}} of
                          {Same, Same} ->
                              ok;
                          _ when QPid =:= QPid1 orelse node(QPid1) =:= node() ->
                              %% Either master hasn't changed, so
                              %% we're ok to update mnesia; or we have
                              %% become the master.
                              Q1 = Q #amqqueue { pid        = QPid1,
                                                 slave_pids = SPids1 },
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
                    fun (#amqqueue { mirror_nodes = [] }, QsN) ->
                            QsN;
                        (#amqqueue { name         = QName,
                                     mirror_nodes = MNodes }, QsN) ->
                            case lists:member(node(), MNodes) of
                                true  -> [QName | QsN];
                                false -> QsN
                            end
                    end, [], rabbit_queue)
          end),
    [add_mirror(Q, node()) || Q <- Qs],
    ok.

drop_mirror(VHostPath, QueueName, MirrorNode) ->
    drop_mirror(rabbit_misc:r(VHostPath, queue, QueueName), MirrorNode).

drop_mirror(Queue, MirrorNode) ->
    if_mirrored_queue(
      Queue,
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

add_mirror(Queue, MirrorNode) ->
    if_mirrored_queue(
      Queue,
      fun (#amqqueue { name = Name, pid = QPid, slave_pids = SPids } = Q) ->
              case [Pid || Pid <- [QPid | SPids], node(Pid) =:= MirrorNode] of
                  []  -> Result = rabbit_mirror_queue_slave_sup:start_child(
                                    MirrorNode, [Q]),
                         rabbit_log:info(
                           "Adding mirror of queue ~s on node ~p: ~p~n",
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
                     case rabbit_misc:table_lookup(Args, <<"x-ha-policy">>) of
                         undefined -> ok;
                         _         -> Fun(Q)
                     end
             end).
