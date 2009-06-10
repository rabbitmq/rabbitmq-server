%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_amqqueue).

-export([start/0, recover/0, declare/4, delete/3, purge/1]).
-export([internal_declare/2, internal_delete/1]).
-export([pseudo_queue/2]).
-export([lookup/1, with/2, with_or_die/2,
         stat/1, stat_all/0, deliver/2, redeliver/2, requeue/3, ack/4]).
-export([list/1, info/1, info/2, info_all/1, info_all/2]).
-export([claim_queue/2]).
-export([basic_get/3, basic_consume/8, basic_cancel/4]).
-export([notify_sent/2, unblock/2]).
-export([commit_all/2, rollback_all/2, notify_down_all/2, limit_all/3]).
-export([on_node_down/1]).

-import(mnesia).
-import(gen_server2).
-import(lists).
-import(queue).

-include("rabbit.hrl").
-include_lib("stdlib/include/qlc.hrl").

-define(CALL_TIMEOUT, 5000).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(qstats() :: {'ok', queue_name(), non_neg_integer(), non_neg_integer()}).
-type(qlen() :: {'ok', non_neg_integer()}).
-type(qfun(A) :: fun ((amqqueue()) -> A)).
-type(ok_or_errors() ::
      'ok' | {'error', [{'error' | 'exit' | 'throw', any()}]}).

-spec(start/0 :: () -> 'ok').
-spec(recover/0 :: () -> 'ok').
-spec(declare/4 :: (queue_name(), bool(), bool(), amqp_table()) ->
             amqqueue()).
-spec(lookup/1 :: (queue_name()) -> {'ok', amqqueue()} | not_found()).
-spec(with/2 :: (queue_name(), qfun(A)) -> A | not_found()).
-spec(with_or_die/2 :: (queue_name(), qfun(A)) -> A).
-spec(list/1 :: (vhost()) -> [amqqueue()]).
-spec(info/1 :: (amqqueue()) -> [info()]).
-spec(info/2 :: (amqqueue(), [info_key()]) -> [info()]).
-spec(info_all/1 :: (vhost()) -> [[info()]]).
-spec(info_all/2 :: (vhost(), [info_key()]) -> [[info()]]).
-spec(stat/1 :: (amqqueue()) -> qstats()).
-spec(stat_all/0 :: () -> [qstats()]).
-spec(delete/3 ::
      (amqqueue(), 'false', 'false') -> qlen();
      (amqqueue(), 'true' , 'false') -> qlen() | {'error', 'in_use'};
      (amqqueue(), 'false', 'true' ) -> qlen() | {'error', 'not_empty'};
      (amqqueue(), 'true' , 'true' ) -> qlen() |
                                            {'error', 'in_use'} |
                                            {'error', 'not_empty'}).
-spec(purge/1 :: (amqqueue()) -> qlen()).
-spec(deliver/2 :: (pid(), delivery()) -> bool()).
-spec(redeliver/2 :: (pid(), [{message(), bool()}]) -> 'ok').
-spec(requeue/3 :: (pid(), [msg_id()],  pid()) -> 'ok').
-spec(ack/4 :: (pid(), maybe(txn()), [msg_id()], pid()) -> 'ok').
-spec(commit_all/2 :: ([pid()], txn()) -> ok_or_errors()).
-spec(rollback_all/2 :: ([pid()], txn()) -> ok_or_errors()).
-spec(notify_down_all/2 :: ([pid()], pid()) -> ok_or_errors()).
-spec(limit_all/3 :: ([pid()], pid(), pid() | 'undefined') -> ok_or_errors()).
-spec(claim_queue/2 :: (amqqueue(), pid()) -> 'ok' | 'locked').
-spec(basic_get/3 :: (amqqueue(), pid(), bool()) ->
             {'ok', non_neg_integer(), msg()} | 'empty').
-spec(basic_consume/8 ::
      (amqqueue(), bool(), pid(), pid(), pid(), ctag(), bool(), any()) ->
             'ok' | {'error', 'queue_owned_by_another_connection' |
                     'exclusive_consume_unavailable'}).
-spec(basic_cancel/4 :: (amqqueue(), pid(), ctag(), any()) -> 'ok').
-spec(notify_sent/2 :: (pid(), pid()) -> 'ok').
-spec(unblock/2 :: (pid(), pid()) -> 'ok').
-spec(internal_declare/2 :: (amqqueue(), bool()) -> amqqueue()).
-spec(internal_delete/1 :: (queue_name()) -> 'ok' | not_found()).
-spec(on_node_down/1 :: (erlang_node()) -> 'ok').
-spec(pseudo_queue/2 :: (binary(), pid()) -> amqqueue()).

-endif.

%%----------------------------------------------------------------------------

start() ->
    {ok,_} = supervisor:start_child(
               rabbit_sup,
               {rabbit_amqqueue_sup,
                {rabbit_amqqueue_sup, start_link, []},
                transient, infinity, supervisor, [rabbit_amqqueue_sup]}),
    ok.

recover() ->
    ok = recover_durable_queues(),
    ok.

recover_durable_queues() ->
    Node = node(),
    lists:foreach(
      fun (RecoveredQ) ->
              Q = start_queue_process(RecoveredQ),
              %% We need to catch the case where a client connected to
              %% another node has deleted the queue (and possibly
              %% re-created it).
              case rabbit_misc:execute_mnesia_transaction(
                     fun () -> case mnesia:match_object(
                                      rabbit_durable_queue, RecoveredQ, read) of
                                   [_] -> ok = store_queue(Q),
                                          true;
                                   []  -> false
                               end
                     end) of
                  true  -> ok;
                  false -> exit(Q#amqqueue.pid, shutdown)
              end
      end,
      %% TODO: use dirty ops instead
      rabbit_misc:execute_mnesia_transaction(
        fun () ->
                qlc:e(qlc:q([Q || Q = #amqqueue{pid = Pid}
                                        <- mnesia:table(rabbit_durable_queue),
                                  node(Pid) == Node]))
        end)),
    ok.

declare(QueueName, Durable, AutoDelete, Args) ->
    Q = start_queue_process(#amqqueue{name = QueueName,
                                      durable = Durable,
                                      auto_delete = AutoDelete,
                                      arguments = Args,
                                      pid = none}),
    internal_declare(Q, true).

internal_declare(Q = #amqqueue{name = QueueName}, WantDefaultBinding) ->
    case rabbit_misc:execute_mnesia_transaction(
           fun () ->
                   case mnesia:wread({rabbit_queue, QueueName}) of
                       [] -> ok = store_queue(Q),
                             case WantDefaultBinding of
                                 true -> add_default_binding(Q);
                                 false -> ok
                             end,
                             Q;
                       [ExistingQ] -> ExistingQ
                   end
           end) of
        Q         -> Q;
        ExistingQ -> exit(Q#amqqueue.pid, shutdown),
                     ExistingQ
    end.

store_queue(Q = #amqqueue{durable = true}) ->
    ok = mnesia:write(rabbit_durable_queue, Q, write),
    ok = mnesia:write(rabbit_queue, Q, write),
    ok;
store_queue(Q = #amqqueue{durable = false}) ->
    ok = mnesia:write(rabbit_queue, Q, write),
    ok.

start_queue_process(Q) ->
    {ok, Pid} = supervisor:start_child(rabbit_amqqueue_sup, [Q]),
    Q#amqqueue{pid = Pid}.

add_default_binding(#amqqueue{name = QueueName}) ->
    Exchange = rabbit_misc:r(QueueName, exchange, <<>>),
    RoutingKey = QueueName#resource.name,
    rabbit_exchange:add_binding(Exchange, QueueName, RoutingKey, []),
    ok.

lookup(Name) ->
    rabbit_misc:dirty_read({rabbit_queue, Name}).

with(Name, F, E) ->
    case lookup(Name) of
        {ok, Q} -> rabbit_misc:with_exit_handler(E, fun () -> F(Q) end);
        {error, not_found} -> E()
    end.

with(Name, F) ->
    with(Name, F, fun () -> {error, not_found} end).
with_or_die(Name, F) ->
    with(Name, F, fun () -> rabbit_misc:not_found(Name) end).

list(VHostPath) ->
    mnesia:dirty_match_object(
      rabbit_queue,
      #amqqueue{name = rabbit_misc:r(VHostPath, queue), _ = '_'}).

map(VHostPath, F) -> rabbit_misc:filter_exit_map(F, list(VHostPath)).

info(#amqqueue{ pid = QPid }) ->
    gen_server2:pcall(QPid, 9, info, infinity).

info(#amqqueue{ pid = QPid }, Items) ->
    case gen_server2:pcall(QPid, 9, {info, Items}, infinity) of
        {ok, Res}      -> Res;
        {error, Error} -> throw(Error)
    end.

info_all(VHostPath) -> map(VHostPath, fun (Q) -> info(Q) end).

info_all(VHostPath, Items) -> map(VHostPath, fun (Q) -> info(Q, Items) end).

stat(#amqqueue{pid = QPid}) -> gen_server2:call(QPid, stat, infinity).

stat_all() ->
    lists:map(fun stat/1, rabbit_misc:dirty_read_all(rabbit_queue)).

delete(#amqqueue{ pid = QPid }, IfUnused, IfEmpty) ->
    gen_server2:call(QPid, {delete, IfUnused, IfEmpty}, infinity).

purge(#amqqueue{ pid = QPid }) -> gen_server2:call(QPid, purge, infinity).

deliver(QPid, #delivery{immediate = true,
                        txn = Txn, sender = ChPid, message = Message}) ->
    gen_server2:call(QPid, {deliver_immediately, Txn, Message, ChPid},
                     infinity);
deliver(QPid, #delivery{mandatory = true,
                        txn = Txn, sender = ChPid, message = Message}) ->
    gen_server2:call(QPid, {deliver, Txn, Message, ChPid}, infinity),
    true;
deliver(QPid, #delivery{txn = Txn, sender = ChPid, message = Message}) ->
    gen_server2:cast(QPid, {deliver, Txn, Message, ChPid}),
    true.

redeliver(QPid, Messages) ->
    gen_server2:cast(QPid, {redeliver, Messages}).

requeue(QPid, MsgIds, ChPid) ->
    gen_server2:cast(QPid, {requeue, MsgIds, ChPid}).

ack(QPid, Txn, MsgIds, ChPid) ->
    gen_server2:cast(QPid, {ack, Txn, MsgIds, ChPid}).

commit_all(QPids, Txn) ->
    safe_pmap_ok(
      fun (QPid) -> exit({queue_disappeared, QPid}) end,
      fun (QPid) -> gen_server2:call(QPid, {commit, Txn}, infinity) end,
      QPids).

rollback_all(QPids, Txn) ->
    safe_pmap_ok(
      fun (QPid) -> exit({queue_disappeared, QPid}) end,
      fun (QPid) -> gen_server2:cast(QPid, {rollback, Txn}) end,
      QPids).

notify_down_all(QPids, ChPid) ->
    safe_pmap_ok(
      %% we don't care if the queue process has terminated in the
      %% meantime
      fun (_)    -> ok end,
      fun (QPid) -> gen_server2:call(QPid, {notify_down, ChPid}, infinity) end,
      QPids).

limit_all(QPids, ChPid, LimiterPid) ->
    safe_pmap_ok(
      fun (_) -> ok end,
      fun (QPid) -> gen_server2:cast(QPid, {limit, ChPid, LimiterPid}) end,
      QPids).
    
claim_queue(#amqqueue{pid = QPid}, ReaderPid) ->
    gen_server2:call(QPid, {claim_queue, ReaderPid}, infinity).

basic_get(#amqqueue{pid = QPid}, ChPid, NoAck) ->
    gen_server2:call(QPid, {basic_get, ChPid, NoAck}, infinity).

basic_consume(#amqqueue{pid = QPid}, NoAck, ReaderPid, ChPid, LimiterPid,
              ConsumerTag, ExclusiveConsume, OkMsg) ->
    gen_server2:call(QPid, {basic_consume, NoAck, ReaderPid, ChPid, 
                            LimiterPid, ConsumerTag, ExclusiveConsume, OkMsg},
                     infinity).

basic_cancel(#amqqueue{pid = QPid}, ChPid, ConsumerTag, OkMsg) ->
    ok = gen_server2:call(QPid, {basic_cancel, ChPid, ConsumerTag, OkMsg},
                          infinity).

notify_sent(QPid, ChPid) ->
    gen_server2:cast(QPid, {notify_sent, ChPid}).

unblock(QPid, ChPid) ->
    gen_server2:cast(QPid, {unblock, ChPid}).

internal_delete(QueueName) ->
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              case mnesia:wread({rabbit_queue, QueueName}) of
                  []  -> {error, not_found};
                  [_] ->
                      ok = rabbit_exchange:delete_queue_bindings(QueueName),
                      ok = mnesia:delete({rabbit_queue, QueueName}),
                      ok = mnesia:delete({rabbit_durable_queue, QueueName}),
                      ok
              end
      end).

on_node_down(Node) ->
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              qlc:fold(
                fun (QueueName, Acc) ->
                        ok = rabbit_exchange:delete_transient_queue_bindings(
                               QueueName),
                        ok = mnesia:delete({rabbit_queue, QueueName}),
                        Acc
                end,
                ok,
                qlc:q([QueueName || #amqqueue{name = QueueName, pid = Pid}
                                        <- mnesia:table(rabbit_queue),
                                    node(Pid) == Node]))
      end).

pseudo_queue(QueueName, Pid) ->
    #amqqueue{name = QueueName,
              durable = false,
              auto_delete = false,
              arguments = [],
              pid = Pid}.

safe_pmap_ok(H, F, L) ->
    case [R || R <- rabbit_misc:upmap(
                      fun (V) ->
                              try
                                  rabbit_misc:with_exit_handler(
                                    fun () -> H(V) end,
                                    fun () -> F(V) end)
                              catch Class:Reason -> {Class, Reason}
                              end
                      end, L),
               R =/= ok] of
        []     -> ok;
        Errors -> {error, Errors}
    end.
